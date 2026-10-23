//! Order lifecycle persistence: idempotent upsert keyed on
//! `client_order_id`, strict state-machine status transitions, and broker
//! order-ID reconciliation lookups.

use crate::{with_executor, Database};
use chrono::{DateTime, Utc};
use honba_core::{InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, TimeInForce};
use rust_decimal::Decimal;
use thiserror::Error;
use uuid::Uuid;

/// Order store errors.
#[derive(Debug, Error)]
pub enum OrderStoreError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// No order matched the lookup key.
    #[error("order not found: {0}")]
    NotFound(String),
    /// The requested status transition violates the order state machine.
    #[error("invalid order state transition {from} -> {to} for order {client_order_id}")]
    InvalidStateTransition {
        /// Client order ID of the affected order.
        client_order_id: String,
        /// Current (or caller-declared `from`) status.
        from: String,
        /// Requested target status.
        to: String,
    },
}

/// Canonical SELECT projecting DECIMAL columns as TEXT so decoding is
/// identical on PostgreSQL (`CAST(numeric AS TEXT)`) and SQLite (no-op).
const SELECT_ORDERS: &str = "SELECT id, client_order_id, broker_order_id, instrument, side, \
     order_type, tif, quantity, status, algo_id, client_id, \
     CAST(filled_quantity AS TEXT) AS filled_quantity, \
     CAST(avg_fill_price AS TEXT) AS avg_fill_price, \
     created_at, updated_at \
     FROM orders";

/// Persisted order row with typed accessors.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedOrder {
    /// Surrogate row ID.
    pub id: i64,
    /// Client-generated order UUID (string form).
    pub client_order_id: String,
    /// Broker-assigned order ID once acknowledged.
    pub broker_order_id: Option<String>,
    /// Canonical instrument string `EXCHANGE:SYMBOL`.
    pub instrument: String,
    /// Canonical side: `buy` / `sell`.
    pub side: String,
    /// Canonical order type: `market`, `limit:<p>`, `stoploss:<p>`,
    /// `stoplimit:<trigger>:<limit>`.
    pub order_type: String,
    /// Canonical time in force: `day` / `ioc` / `gtc`.
    pub tif: String,
    /// Requested quantity (exact decimal string).
    pub quantity: String,
    /// Lifecycle status (snake_case, e.g. `pending_new`).
    pub status: String,
    /// SEBI algo ID.
    pub algo_id: String,
    /// Client / account ID.
    pub client_id: String,
    /// Aggregated filled quantity (exact decimal string).
    pub filled_quantity: String,
    /// Volume-weighted average fill price, if any fills exist.
    pub avg_fill_price: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl PersistedOrder {
    /// Map a query row (any sqlx backend) into a `PersistedOrder`.
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
            client_order_id: row.try_get("client_order_id")?,
            broker_order_id: row.try_get("broker_order_id")?,
            instrument: row.try_get("instrument")?,
            side: row.try_get("side")?,
            order_type: row.try_get("order_type")?,
            tif: row.try_get("tif")?,
            quantity: row.try_get("quantity")?,
            status: row.try_get("status")?,
            algo_id: row.try_get("algo_id")?,
            client_id: row.try_get("client_id")?,
            filled_quantity: row.try_get("filled_quantity")?,
            avg_fill_price: row.try_get("avg_fill_price")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    /// Client order ID parsed back into a `Uuid`.
    pub fn client_order_id_uuid(&self) -> Option<Uuid> {
        Uuid::parse_str(&self.client_order_id).ok()
    }

    /// Typed lifecycle status.
    pub fn status(&self) -> Option<OrderStatus> {
        parse_status(&self.status)
    }

    /// Typed order side.
    pub fn side(&self) -> Option<OrderSide> {
        parse_side(&self.side)
    }

    /// Typed order type (limit/trigger prices restored exactly).
    pub fn order_type(&self) -> Option<OrderType> {
        order_type_decode(&self.order_type)
    }

    /// Typed time in force.
    pub fn tif(&self) -> Option<TimeInForce> {
        tif_decode(&self.tif)
    }

    /// Requested quantity as an exact decimal.
    pub fn quantity(&self) -> Decimal {
        self.quantity.parse().unwrap_or(Decimal::ZERO)
    }

    /// Aggregated filled quantity as an exact decimal.
    pub fn filled_quantity(&self) -> Decimal {
        self.filled_quantity.parse().unwrap_or(Decimal::ZERO)
    }

    /// Average fill price as an exact decimal, if any fills exist.
    pub fn avg_fill_price(&self) -> Option<Decimal> {
        self.avg_fill_price.as_deref()?.parse().ok()
    }

    /// Instrument reconstructed from the canonical string.
    pub fn instrument(&self) -> Option<InstrumentId> {
        crate::parse_instrument(&self.instrument)
    }
}

/// Canonical snake_case status string for storage.
pub(crate) fn status_str(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::PendingNew => "pending_new",
        OrderStatus::New => "new",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Expired => "expired",
    }
}

/// Parse a stored status string back into an [`OrderStatus`].
pub(crate) fn parse_status(s: &str) -> Option<OrderStatus> {
    match s.to_ascii_lowercase().as_str() {
        "pending_new" => Some(OrderStatus::PendingNew),
        "new" => Some(OrderStatus::New),
        "partially_filled" => Some(OrderStatus::PartiallyFilled),
        "filled" => Some(OrderStatus::Filled),
        "cancelled" => Some(OrderStatus::Cancelled),
        "rejected" => Some(OrderStatus::Rejected),
        "expired" => Some(OrderStatus::Expired),
        _ => None,
    }
}

/// Canonical lowercase side string for storage.
pub(crate) fn side_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

/// Parse a stored side string back into an [`OrderSide`].
pub(crate) fn parse_side(s: &str) -> Option<OrderSide> {
    match s.to_ascii_lowercase().as_str() {
        "buy" => Some(OrderSide::Buy),
        "sell" => Some(OrderSide::Sell),
        _ => None,
    }
}

/// Canonical compact encoding of an [`OrderType`] preserving exact prices.
pub(crate) fn order_type_encode(ot: OrderType) -> String {
    match ot {
        OrderType::Market => "market".to_string(),
        OrderType::Limit(p) => format!("limit:{}", p.0),
        OrderType::StopLoss(p) => format!("stoploss:{}", p.0),
        OrderType::StopLimit { trigger, limit } => {
            format!("stoplimit:{}:{}", trigger.0, limit.0)
        }
    }
}

/// Decode the canonical compact order-type encoding.
pub(crate) fn order_type_decode(s: &str) -> Option<OrderType> {
    let (tag, rest) = s.split_once(':').unwrap_or((s, ""));
    match tag {
        "market" => Some(OrderType::Market),
        "limit" => Some(OrderType::Limit(Price(rest.parse().ok()?))),
        "stoploss" => Some(OrderType::StopLoss(Price(rest.parse().ok()?))),
        "stoplimit" => {
            let (trigger, limit) = rest.split_once(':')?;
            Some(OrderType::StopLimit {
                trigger: Price(trigger.parse().ok()?),
                limit: Price(limit.parse().ok()?),
            })
        }
        _ => None,
    }
}

/// Canonical lowercase time-in-force string for storage.
pub(crate) fn tif_str(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Day => "day",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Gtc => "gtc",
    }
}

/// Decode a stored time-in-force string.
pub(crate) fn tif_decode(s: &str) -> Option<TimeInForce> {
    match s.to_ascii_lowercase().as_str() {
        "day" => Some(TimeInForce::Day),
        "ioc" => Some(TimeInForce::Ioc),
        "gtc" => Some(TimeInForce::Gtc),
        _ => None,
    }
}

/// Returns `true` when `from -> to` is a legal order state-machine edge.
///
/// Legal edges:
/// - `PendingNew -> New | Cancelled | Rejected | Expired`
/// - `New -> PartiallyFilled | Filled | Cancelled | Rejected | Expired`
/// - `PartiallyFilled -> PartiallyFilled | Filled | Cancelled | Rejected | Expired`
///
/// Terminal states (`Filled`, `Cancelled`, `Rejected`, `Expired`) admit no
/// outgoing transitions.
pub fn is_legal_transition(from: OrderStatus, to: OrderStatus) -> bool {
    use OrderStatus::*;
    matches!(
        (from, to),
        (PendingNew, New | Cancelled | Rejected | Expired)
            | (New, PartiallyFilled | Filled | Cancelled | Rejected | Expired)
            | (PartiallyFilled, PartiallyFilled | Filled | Cancelled | Rejected | Expired)
    )
}

/// Order repository over either sqlx backend.
#[derive(Debug, Clone)]
pub struct OrderStore {
    db: Database,
}

impl OrderStore {
    /// Create a store bound to a database handle (pool is shared cheaply).
    pub fn new(db: &Database) -> Self {
        Self { db: db.clone() }
    }

    /// Idempotent upsert keyed on `client_order_id`.
    ///
    /// If the order already exists the existing row is returned unchanged;
    /// concurrent racers are absorbed via `ON CONFLICT DO NOTHING` followed
    /// by a re-read, so no duplicate rows can ever be produced.
    pub async fn upsert(
        &self,
        req: &OrderRequest,
        ts: DateTime<Utc>,
    ) -> Result<PersistedOrder, OrderStoreError> {
        let cid = req.client_order_id.to_string();
        if let Some(existing) = self.get_by_client_order_id(&cid).await? {
            return Ok(existing);
        }
        let instrument = req.instrument.to_string();
        let side = side_str(req.side).to_string();
        let order_type = order_type_encode(req.order_type);
        let tif = tif_str(req.tif).to_string();
        let quantity = req.quantity.0.to_string();
        let algo_id = req.algo_id.clone();
        let client_id = req.client_id_str().to_string();
        with_executor!(&self.db, |ex| {
            sqlx::query(
                "INSERT INTO orders (
                     client_order_id, broker_order_id, instrument, side, order_type, tif,
                     quantity, status, algo_id, client_id, filled_quantity, avg_fill_price,
                     created_at, updated_at
                 ) VALUES (
                     $1, NULL, $2, $3, $4, $5, $6, 'pending_new', $7, $8, '0', NULL, $9, $10
                 )
                 ON CONFLICT (client_order_id) DO NOTHING",
            )
            .bind(&cid)
            .bind(&instrument)
            .bind(&side)
            .bind(&order_type)
            .bind(&tif)
            .bind(&quantity)
            .bind(&algo_id)
            .bind(&client_id)
            .bind(ts)
            .bind(ts)
            .execute(ex)
            .await
            .map(|_| ())
        })?;
        self.get_by_client_order_id(&cid)
            .await?
            .ok_or_else(|| OrderStoreError::NotFound(cid))
    }

    /// Transition the order status from `from` to `to` with state-machine
    /// validation. The compare-and-set on the current status keeps the
    /// update atomic under concurrency.
    pub async fn update_status(
        &self,
        client_order_id: &str,
        from: OrderStatus,
        to: OrderStatus,
        ts: DateTime<Utc>,
    ) -> Result<PersistedOrder, OrderStoreError> {
        if !is_legal_transition(from, to) {
            return Err(OrderStoreError::InvalidStateTransition {
                client_order_id: client_order_id.to_string(),
                from: status_str(from).to_string(),
                to: status_str(to).to_string(),
            });
        }
        let affected = with_executor!(&self.db, |ex| {
            sqlx::query(
                "UPDATE orders SET status = $1, updated_at = $2
                 WHERE client_order_id = $3 AND status = $4",
            )
            .bind(status_str(to))
            .bind(ts)
            .bind(client_order_id)
            .bind(status_str(from))
            .execute(ex)
            .await
            .map(|r| r.rows_affected())
        })?;
        if affected == 0 {
            let current = self
                .get_by_client_order_id(client_order_id)
                .await?
                .ok_or_else(|| OrderStoreError::NotFound(client_order_id.to_string()))?;
            return Err(OrderStoreError::InvalidStateTransition {
                client_order_id: client_order_id.to_string(),
                from: current.status,
                to: status_str(to).to_string(),
            });
        }
        self.get_by_client_order_id(client_order_id)
            .await?
            .ok_or_else(|| OrderStoreError::NotFound(client_order_id.to_string()))
    }

    /// Attach (or overwrite) the broker-assigned order ID after an ack.
    pub async fn attach_broker_order_id(
        &self,
        client_order_id: &str,
        broker_order_id: &str,
        ts: DateTime<Utc>,
    ) -> Result<PersistedOrder, OrderStoreError> {
        let affected = with_executor!(&self.db, |ex| {
            sqlx::query(
                "UPDATE orders SET broker_order_id = $1, updated_at = $2
                 WHERE client_order_id = $3",
            )
            .bind(broker_order_id)
            .bind(ts)
            .bind(client_order_id)
            .execute(ex)
            .await
            .map(|r| r.rows_affected())
        })?;
        if affected == 0 {
            return Err(OrderStoreError::NotFound(client_order_id.to_string()));
        }
        self.get_by_client_order_id(client_order_id)
            .await?
            .ok_or_else(|| OrderStoreError::NotFound(client_order_id.to_string()))
    }

    /// Look up an order by its client-generated UUID.
    pub async fn get_by_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Result<Option<PersistedOrder>, OrderStoreError> {
        let row = with_executor!(&self.db, |ex| {
            sqlx::query(&format!("{SELECT_ORDERS} WHERE client_order_id = $1"))
                .bind(client_order_id)
                .fetch_optional(ex)
                .await?
                .as_ref()
                .map(PersistedOrder::from_row)
                .transpose()
        })?;
        Ok(row)
    }

    /// Look up an order by its broker-assigned order ID.
    pub async fn get_by_broker_order_id(
        &self,
        broker_order_id: &str,
    ) -> Result<Option<PersistedOrder>, OrderStoreError> {
        let row = with_executor!(&self.db, |ex| {
            sqlx::query(
                "SELECT id, client_order_id, broker_order_id, instrument, side, \
                     order_type, tif, quantity, status, algo_id, client_id, \
                     CAST(filled_quantity AS TEXT) AS filled_quantity, \
                     CAST(avg_fill_price AS TEXT) AS avg_fill_price, \
                     created_at, updated_at \
                 FROM orders WHERE broker_order_id = $1",
            )
            .bind(broker_order_id)
            .fetch_optional(ex)
            .await?
            .as_ref()
            .map(PersistedOrder::from_row)
            .transpose()
        })?;
        Ok(row)
    }

    /// All execution fills persisted for an order, ordered by insertion.
    pub async fn fills_for_order(
        &self,
        client_order_id: &str,
    ) -> Result<Vec<crate::fills::PersistedFill>, crate::fills::FillStoreError> {
        crate::fills::select_fills_for_order(&self.db, client_order_id).await
    }
}
