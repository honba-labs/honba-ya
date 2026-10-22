//! Trade execution persistence: idempotent fill ingestion keyed on
//! `trade_id` with atomic, exact-decimal aggregation of the owning order's
//! `filled_quantity` / `avg_fill_price` inside the same transaction.

use crate::orders::{parse_side, side_str};
use crate::{with_executor, Database};
use sqlx::Row;
use chrono::{DateTime, Utc};
use honba_core::{FillReport, OrderSide};
use rust_decimal::Decimal;
use thiserror::Error;

/// Fill store errors.
#[derive(Debug, Error)]
pub enum FillStoreError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// No fill matched the lookup key.
    #[error("fill not found: {0}")]
    NotFound(String),
}

/// Canonical SELECT projecting DECIMAL columns as TEXT (identical decoding
/// on PostgreSQL and SQLite).
const SELECT_FILLS: &str = "SELECT id, trade_id, client_order_id, broker_order_id, \
     instrument, side, \
     CAST(quantity AS TEXT) AS quantity, \
     CAST(price AS TEXT) AS price, \
     fee_breakdown_json, algo_id, client_id, executed_at, created_at \
     FROM fills";

/// Persisted fill row with typed accessors.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedFill {
    /// Surrogate row ID.
    pub id: i64,
    /// Unique execution (trade) ID — the domain `FillReport::fill_id`.
    pub trade_id: String,
    /// Client order UUID the fill belongs to (string form).
    pub client_order_id: String,
    /// Broker order ID at execution time.
    pub broker_order_id: Option<String>,
    /// Canonical instrument string `EXCHANGE:SYMBOL`.
    pub instrument: String,
    /// Canonical side: `buy` / `sell`.
    pub side: String,
    /// Executed quantity (exact decimal string).
    pub quantity: String,
    /// Execution price (exact decimal string).
    pub price: String,
    /// Statutory fee breakdown persisted as a JSON document.
    pub fee_breakdown_json: String,
    /// SEBI algo ID.
    pub algo_id: String,
    /// Client / account ID.
    pub client_id: String,
    /// Execution timestamp.
    pub executed_at: DateTime<Utc>,
    /// Insertion timestamp.
    pub created_at: DateTime<Utc>,
}

impl PersistedFill {
    /// Map a query row (any sqlx backend) into a `PersistedFill`.
    pub(crate) fn from_row<R>(row: &R) -> Result<Self, sqlx::Error>
    where
        R: sqlx::Row,
        for<'a> &'a str: sqlx::ColumnIndex<R>,
        i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        Option<String>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        DateTime<Utc>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    {
        Ok(Self {
            id: row.try_get("id")?,
            trade_id: row.try_get("trade_id")?,
            client_order_id: row.try_get("client_order_id")?,
            broker_order_id: row.try_get("broker_order_id")?,
            instrument: row.try_get("instrument")?,
            side: row.try_get("side")?,
            quantity: row.try_get("quantity")?,
            price: row.try_get("price")?,
            fee_breakdown_json: row.try_get("fee_breakdown_json")?,
            algo_id: row.try_get("algo_id")?,
            client_id: row.try_get("client_id")?,
            executed_at: row.try_get("executed_at")?,
            created_at: row.try_get("created_at")?,
        })
    }

    /// Executed quantity as an exact decimal.
    pub fn quantity(&self) -> Decimal {
        self.quantity.parse().unwrap_or(Decimal::ZERO)
    }

    /// Execution price as an exact decimal.
    pub fn price(&self) -> Decimal {
        self.price.parse().unwrap_or(Decimal::ZERO)
    }

    /// Typed fill side.
    pub fn side(&self) -> Option<OrderSide> {
        parse_side(&self.side)
    }
}

/// Fetch every fill persisted for an order, ordered by insertion.
pub(crate) async fn select_fills_for_order(
    db: &Database,
    client_order_id: &str,
) -> Result<Vec<PersistedFill>, FillStoreError> {
    with_executor!(db, |ex| {
        sqlx::query(&format!("{SELECT_FILLS} WHERE client_order_id = $1 ORDER BY id ASC"))
            .bind(client_order_id)
            .fetch_all(ex)
            .await?
            .iter()
            .map(PersistedFill::from_row)
            .collect::<Result<Vec<_>, sqlx::Error>>()
    })
    .map_err(Into::into)
}

/// Trade execution repository over either sqlx backend.
#[derive(Debug, Clone)]
pub struct FillStore {
    db: Database,
}

impl FillStore {
    /// Create a store bound to a database handle (pool is shared cheaply).
    pub fn new(db: &Database) -> Self {
        Self { db: db.clone() }
    }

    /// Idempotent fill ingestion keyed on `trade_id` (`FillReport::fill_id`).
    ///
    /// Inside one transaction: insert the fill (`ON CONFLICT DO NOTHING`,
    /// re-reading the winner on races), then recompute the owning order's
    /// `filled_quantity` and volume-weighted `avg_fill_price` exactly in
    /// `rust_decimal` (no floating-point), promoting the order to `filled`
    /// when the aggregate reaches the requested quantity.
    pub async fn insert(
        &self,
        fill: &FillReport,
        fee_breakdown_json: &str,
        ts: DateTime<Utc>,
    ) -> Result<PersistedFill, FillStoreError> {
        if let Some(existing) = self.get_by_trade_id(&fill.fill_id).await? {
            return Ok(existing);
        }
        let by_trade_id = format!("{SELECT_FILLS} WHERE trade_id = $1");
        with_executor!(&self.db, |ex| {
            let mut tx = ex.begin().await?;
            // Idempotency re-check inside the transaction absorbs racers.
            let existing = sqlx::query(&by_trade_id)
                .bind(&fill.fill_id)
                .fetch_optional(&mut *tx)
                .await?;
            if let Some(row) = existing {
                let persisted = PersistedFill::from_row(&row)?;
                tx.commit().await?;
                return Ok(persisted);
            }
            sqlx::query(
                "INSERT INTO fills (
                     trade_id, client_order_id, broker_order_id, instrument, side,
                     quantity, price, fee_breakdown_json, algo_id, client_id,
                     executed_at, created_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 ON CONFLICT (trade_id) DO NOTHING",
            )
            .bind(&fill.fill_id)
            .bind(fill.client_order_id.to_string())
            .bind(&fill.order_id)
            .bind(fill.instrument.to_string())
            .bind(side_str(fill.side))
            .bind(fill.quantity.0.to_string())
            .bind(fill.price.0.to_string())
            .bind(fee_breakdown_json)
            .bind(&fill.algo_id)
            .bind(fill.client_id_str())
            .bind(ts)
            .bind(ts)
            .execute(&mut *tx)
            .await?;

            // Recompute the owning order's aggregates exactly in Decimal.
            let cid = fill.client_order_id.to_string();
            let agg_rows = sqlx::query(
                "SELECT CAST(quantity AS TEXT) AS quantity, CAST(price AS TEXT) AS price
                 FROM fills WHERE client_order_id = $1",
            )
            .bind(&cid)
            .fetch_all(&mut *tx)
            .await?;
            let mut total_qty = Decimal::ZERO;
            let mut total_notional = Decimal::ZERO;
            for row in &agg_rows {
                let q: Decimal = row
                    .try_get::<String, _>("quantity")?
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let p: Decimal = row
                    .try_get::<String, _>("price")?
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                total_qty += q;
                total_notional += q * p;
            }
            let avg: Option<Decimal> = if total_qty > Decimal::ZERO {
                Some(total_notional / total_qty)
            } else {
                None
            };

            let ord = sqlx::query(
                "SELECT quantity, status FROM orders WHERE client_order_id = $1",
            )
            .bind(&cid)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(order) = ord {
                let order_qty: Decimal = order
                    .try_get::<String, _>("quantity")?
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let status: String = order.try_get("status")?;
                let promote =
                    total_qty >= order_qty && (status == "new" || status == "partially_filled");
                let new_status: &str = if promote { "filled" } else { &status };
                sqlx::query(
                    "UPDATE orders
                     SET filled_quantity = $1, avg_fill_price = $2, status = $3, updated_at = $4
                     WHERE client_order_id = $5",
                )
                .bind(total_qty.to_string())
                .bind(avg.map(|a| a.to_string()))
                .bind(new_status)
                .bind(ts)
                .bind(&cid)
                .execute(&mut *tx)
                .await?;
            }

            let row = sqlx::query(&format!("{SELECT_FILLS} WHERE trade_id = $1"))
                .bind(&fill.fill_id)
                .fetch_one(&mut *tx)
                .await?;
            let persisted = PersistedFill::from_row(&row)?;
            tx.commit().await?;
            Ok(persisted)
        })
    }

    /// Look up a fill by its unique trade ID.
    pub async fn get_by_trade_id(
        &self,
        trade_id: &str,
    ) -> Result<Option<PersistedFill>, FillStoreError> {
        let row = with_executor!(&self.db, |ex| {
            sqlx::query(&format!("{SELECT_FILLS} WHERE trade_id = $1"))
                .bind(trade_id)
                .fetch_optional(ex)
                .await?
                .as_ref()
                .map(PersistedFill::from_row)
                .transpose()
        })?;
        Ok(row)
    }
}
