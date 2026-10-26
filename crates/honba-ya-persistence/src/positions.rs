//! Portfolio position persistence: long-only FIFO tax-lot ledger.
//!
//! One `positions` row per `(client_id, exchange, symbol)`. Buys append
//! `position_lots` rows; sells consume the oldest lots first and realize
//! exact `rust_decimal` PnL per matched quantity. Open quantity and weighted
//! average cost (WAC) are always recomputed exactly from the lot ledger, so
//! partial FIFO consumption cannot drift.

use crate::orders::parse_side;
use crate::{Database, with_executor};
use chrono::{DateTime, Utc};
use honba_core::{InstrumentId, OrderSide};
use rust_decimal::Decimal;
use sqlx::Row;
use thiserror::Error;

/// Position store errors.
#[derive(Debug, Error)]
pub enum PositionStoreError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// No position matched the lookup key.
    #[error("position not found: {0}")]
    NotFound(String),
    /// A long-only position cannot satisfy the requested sell quantity.
    #[error(
        "insufficient open position for {instrument} (client {client_id}): requested {requested}, open {open}"
    )]
    InsufficientPosition {
        /// Canonical instrument string.
        instrument: String,
        /// Client / account ID.
        client_id: String,
        /// Requested sell quantity.
        requested: String,
        /// Currently open quantity.
        open: String,
    },
    /// Non-positive quantity supplied to [`PositionStore::apply_fill`].
    #[error("invalid fill quantity: {0}")]
    InvalidQuantity(String),
}

/// Canonical SELECT projecting DECIMAL columns as TEXT.
const SELECT_POSITIONS: &str = "SELECT id, client_id, exchange, symbol, instrument, \
     CAST(open_quantity AS TEXT) AS open_quantity, \
     CAST(weighted_average_cost AS TEXT) AS weighted_average_cost, \
     CAST(realized_pnl AS TEXT) AS realized_pnl, \
     last_updated \
     FROM positions";

/// Canonical lot SELECT projecting DECIMAL columns as TEXT.
const SELECT_LOTS: &str = "SELECT id, client_id, exchange, symbol, side, \
     CAST(open_quantity AS TEXT) AS open_quantity, \
     CAST(entry_price AS TEXT) AS entry_price, \
     opened_at \
     FROM position_lots";

/// Persisted position row (one per client + instrument).
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedPosition {
    /// Surrogate row ID.
    pub id: i64,
    /// Client / account ID.
    pub client_id: String,
    /// Exchange code (`NSE` / `BSE` / `MCX`).
    pub exchange: String,
    /// Trading symbol.
    pub symbol: String,
    /// Canonical instrument string `EXCHANGE:SYMBOL`.
    pub instrument: String,
    /// Currently open quantity (exact decimal string).
    pub open_quantity: String,
    /// Weighted average cost of open lots (exact decimal string).
    pub weighted_average_cost: String,
    /// Cumulative realized PnL (exact decimal string).
    pub realized_pnl: String,
    /// Last update timestamp.
    pub last_updated: DateTime<Utc>,
}

impl PersistedPosition {
    fn from_row<R>(row: &R) -> Result<Self, sqlx::Error>
    where
        R: sqlx::Row,
        for<'a> &'a str: sqlx::ColumnIndex<R>,
        i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        DateTime<Utc>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    {
        Ok(Self {
            id: row.try_get("id")?,
            client_id: row.try_get("client_id")?,
            exchange: row.try_get("exchange")?,
            symbol: row.try_get("symbol")?,
            instrument: row.try_get("instrument")?,
            open_quantity: row.try_get("open_quantity")?,
            weighted_average_cost: row.try_get("weighted_average_cost")?,
            realized_pnl: row.try_get("realized_pnl")?,
            last_updated: row.try_get("last_updated")?,
        })
    }

    /// Open quantity as an exact decimal.
    pub fn open_quantity(&self) -> Decimal {
        self.open_quantity.parse().unwrap_or(Decimal::ZERO)
    }

    /// Weighted average cost as an exact decimal.
    pub fn weighted_average_cost(&self) -> Decimal {
        self.weighted_average_cost.parse().unwrap_or(Decimal::ZERO)
    }

    /// Cumulative realized PnL as an exact decimal.
    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl.parse().unwrap_or(Decimal::ZERO)
    }

    /// Instrument reconstructed from the canonical string.
    pub fn instrument(&self) -> Option<InstrumentId> {
        crate::parse_instrument(&self.instrument)
    }
}

/// Persisted FIFO tax-lot row.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedPositionLot {
    /// Surrogate lot ID (monotonic insertion order = FIFO order).
    pub id: i64,
    /// Client / account ID.
    pub client_id: String,
    /// Exchange code.
    pub exchange: String,
    /// Trading symbol.
    pub symbol: String,
    /// Lot side: `buy` / `sell` (always `buy` for long-only openings).
    pub side: String,
    /// Remaining open quantity of this lot (exact decimal string).
    pub open_quantity: String,
    /// Entry price of this lot (exact decimal string).
    pub entry_price: String,
    /// Lot opening timestamp.
    pub opened_at: DateTime<Utc>,
}

impl PersistedPositionLot {
    fn from_row<R>(row: &R) -> Result<Self, sqlx::Error>
    where
        R: sqlx::Row,
        for<'a> &'a str: sqlx::ColumnIndex<R>,
        i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        DateTime<Utc>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    {
        Ok(Self {
            id: row.try_get("id")?,
            client_id: row.try_get("client_id")?,
            exchange: row.try_get("exchange")?,
            symbol: row.try_get("symbol")?,
            side: row.try_get("side")?,
            open_quantity: row.try_get("open_quantity")?,
            entry_price: row.try_get("entry_price")?,
            opened_at: row.try_get("opened_at")?,
        })
    }

    /// Remaining open quantity of this lot.
    pub fn open_quantity(&self) -> Decimal {
        self.open_quantity.parse().unwrap_or(Decimal::ZERO)
    }

    /// Entry price of this lot.
    pub fn entry_price(&self) -> Decimal {
        self.entry_price.parse().unwrap_or(Decimal::ZERO)
    }

    /// Typed lot side.
    pub fn side(&self) -> Option<OrderSide> {
        parse_side(&self.side)
    }
}

/// FIFO tax-lot position repository over either sqlx backend.
#[derive(Debug, Clone)]
pub struct PositionStore {
    db: Database,
}

impl PositionStore {
    /// Create a store bound to a database handle (pool is shared cheaply).
    pub fn new(db: &Database) -> Self {
        Self { db: db.clone() }
    }

    /// Apply one execution fill to the FIFO tax-lot ledger (long-only).
    ///
    /// - **Buy**: appends a `position_lots` row.
    /// - **Sell**: consumes the oldest lots first and realizes
    ///   `Σ (sell_price - lot_entry_price) * matched_qty` exactly in
    ///   `rust_decimal`; overselling fails with
    ///   [`PositionStoreError::InsufficientPosition`] and rolls back.
    ///
    /// Afterwards open quantity and WAC are recomputed exactly from the lot
    /// ledger and the position row is upserted atomically in one transaction.
    pub async fn apply_fill(
        &self,
        instrument: &InstrumentId,
        client_id: &str,
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        ts: DateTime<Utc>,
    ) -> Result<PersistedPosition, PositionStoreError> {
        if qty <= Decimal::ZERO {
            return Err(PositionStoreError::InvalidQuantity(qty.to_string()));
        }
        let exchange = instrument.exchange.to_string();
        let symbol = instrument.symbol.clone();
        let inst = instrument.to_string();
        with_executor!(&self.db, |ex| {
            let mut tx = ex.begin().await?;
            let pos_row = sqlx::query(
                "SELECT CAST(open_quantity AS TEXT) AS open_quantity, \
                        CAST(realized_pnl AS TEXT) AS realized_pnl \
                 FROM positions \
                 WHERE client_id = $1 AND exchange = $2 AND symbol = $3",
            )
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .fetch_optional(&mut *tx)
            .await?;
            let (cur_open, cur_realized) = match &pos_row {
                Some(row) => (
                    row.try_get::<String, _>("open_quantity")?
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    row.try_get::<String, _>("realized_pnl")?
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                ),
                None => (Decimal::ZERO, Decimal::ZERO),
            };

            let realized_delta = if side == OrderSide::Buy {
                // Buy: open a new lot.
                sqlx::query(
                    "INSERT INTO position_lots \
                         (client_id, exchange, symbol, side, open_quantity, entry_price, opened_at)
                     VALUES ($1, $2, $3, 'buy', $4, $5, $6)",
                )
                .bind(client_id)
                .bind(&exchange)
                .bind(&symbol)
                .bind(qty.to_string())
                .bind(price.to_string())
                .bind(ts)
                .execute(&mut *tx)
                .await?;
                Decimal::ZERO
            } else {
                // Sell: consume lots in FIFO insertion order.
                let lots = sqlx::query(&format!(
                    "{SELECT_LOTS} WHERE client_id = $1 AND exchange = $2 AND symbol = $3 \
                     ORDER BY id ASC"
                ))
                .bind(client_id)
                .bind(&exchange)
                .bind(&symbol)
                .fetch_all(&mut *tx)
                .await?;
                let mut remaining = qty;
                let mut realized = Decimal::ZERO;
                for lot in &lots {
                    let lot_open: Decimal = lot
                        .try_get::<String, _>("open_quantity")?
                        .parse()
                        .unwrap_or(Decimal::ZERO);
                    let lot_entry: Decimal = lot
                        .try_get::<String, _>("entry_price")?
                        .parse()
                        .unwrap_or(Decimal::ZERO);
                    let lot_id: i64 = lot.try_get("id")?;
                    if lot_open <= Decimal::ZERO {
                        // Defensive: fully-consumed lots are deleted, but skip
                        // any degenerate row without affecting the math.
                        continue;
                    }
                    if remaining <= Decimal::ZERO {
                        continue;
                    }
                    let matched = if remaining < lot_open {
                        remaining
                    } else {
                        lot_open
                    };
                    realized += (price - lot_entry) * matched;
                    remaining -= matched;
                    if matched >= lot_open {
                        sqlx::query("DELETE FROM position_lots WHERE id = $1")
                            .bind(lot_id)
                            .execute(&mut *tx)
                            .await?;
                    } else {
                        sqlx::query("UPDATE position_lots SET open_quantity = $1 WHERE id = $2")
                            .bind((lot_open - matched).to_string())
                            .bind(lot_id)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
                if remaining > Decimal::ZERO {
                    tx.rollback().await?;
                    return Err(PositionStoreError::InsufficientPosition {
                        instrument: inst,
                        client_id: client_id.to_string(),
                        requested: qty.to_string(),
                        open: cur_open.to_string(),
                    });
                }
                realized
            };

            // Recompute open quantity and WAC exactly from the lot ledger.
            let sum_rows = sqlx::query(&format!(
                "{SELECT_LOTS} WHERE client_id = $1 AND exchange = $2 AND symbol = $3 \
                 ORDER BY id ASC"
            ))
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .fetch_all(&mut *tx)
            .await?;
            let mut total_open = Decimal::ZERO;
            let mut total_cost = Decimal::ZERO;
            for row in &sum_rows {
                let open: Decimal = row
                    .try_get::<String, _>("open_quantity")?
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let entry: Decimal = row
                    .try_get::<String, _>("entry_price")?
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                total_open += open;
                total_cost += open * entry;
            }
            let new_open = total_open;
            let new_wac = if total_open > Decimal::ZERO {
                total_cost / total_open
            } else {
                Decimal::ZERO
            };
            let new_realized = cur_realized + realized_delta;

            // Upsert the position row: update first, insert on miss.
            let affected = sqlx::query(
                "UPDATE positions
                 SET open_quantity = $1, weighted_average_cost = $2, realized_pnl = $3,
                     last_updated = $4
                 WHERE client_id = $5 AND exchange = $6 AND symbol = $7",
            )
            .bind(new_open.to_string())
            .bind(new_wac.to_string())
            .bind(new_realized.to_string())
            .bind(ts)
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if affected == 0 {
                sqlx::query(
                    "INSERT INTO positions \
                         (client_id, exchange, symbol, instrument, open_quantity, \
                          weighted_average_cost, realized_pnl, last_updated)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(client_id)
                .bind(&exchange)
                .bind(&symbol)
                .bind(&inst)
                .bind(new_open.to_string())
                .bind(new_wac.to_string())
                .bind(new_realized.to_string())
                .bind(ts)
                .execute(&mut *tx)
                .await?;
            }

            let row = sqlx::query(&format!(
                "{SELECT_POSITIONS} WHERE client_id = $1 AND exchange = $2 AND symbol = $3"
            ))
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .fetch_one(&mut *tx)
            .await?;
            let persisted = PersistedPosition::from_row(&row)?;
            tx.commit().await?;
            Ok(persisted)
        })
    }

    /// Get the position for `(client_id, instrument)`, if any.
    pub async fn get_position(
        &self,
        client_id: &str,
        instrument: &InstrumentId,
    ) -> Result<Option<PersistedPosition>, PositionStoreError> {
        let exchange = instrument.exchange.to_string();
        let symbol = instrument.symbol.clone();
        let row = with_executor!(&self.db, |ex| {
            sqlx::query(&format!(
                "{SELECT_POSITIONS} WHERE client_id = $1 AND exchange = $2 AND symbol = $3"
            ))
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .fetch_optional(ex)
            .await?
            .as_ref()
            .map(PersistedPosition::from_row)
            .transpose()
        })?;
        Ok(row)
    }

    /// List open FIFO tax-lots for `(client_id, instrument)` in FIFO order.
    pub async fn get_lots(
        &self,
        client_id: &str,
        instrument: &InstrumentId,
    ) -> Result<Vec<PersistedPositionLot>, PositionStoreError> {
        let exchange = instrument.exchange.to_string();
        let symbol = instrument.symbol.clone();
        with_executor!(&self.db, |ex| {
            sqlx::query(&format!(
                "{SELECT_LOTS} WHERE client_id = $1 AND exchange = $2 AND symbol = $3 \
                 ORDER BY id ASC"
            ))
            .bind(client_id)
            .bind(&exchange)
            .bind(&symbol)
            .fetch_all(ex)
            .await?
            .iter()
            .map(PersistedPositionLot::from_row)
            .collect::<Result<Vec<_>, sqlx::Error>>()
        })
        .map_err(Into::into)
    }
}
