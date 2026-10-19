//! SEBI Regulatory Audit Trail persistence using an append-only SQLite audit ledger.
//!
//! Under SEBI's regulatory guidelines for algorithmic trading (including Retail Algo circular),
//! all order placement attempts, cancellations, modifications, risk refusals, broker acks,
//! and fills MUST be stored in an append-only immutable audit trail with millisecond precision
//! timestamps, algorithm identifiers, and correlation tracking.

use chrono::Utc;
use honba_core::{FillReport, OrderAck, OrderRequest, Price, Quantity};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

/// Audit errors.
#[derive(Debug, Error)]
pub enum AuditError {
    /// Database error from sqlx.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A persisted SEBI regulatory audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLedgerRecord {
    /// Unique auto-incrementing record ID.
    pub id: i64,
    /// Millisecond UNIX epoch timestamp.
    pub timestamp_ms: i64,
    /// ISO 8601 UTC timestamp string.
    pub timestamp_utc: String,
    /// Event type: `order_attempt`, `risk_refusal`, `broker_ack`, `cancellation`, `modification`, `fill`.
    pub event_type: String,
    /// SEBI-registered algorithm ID.
    pub algo_id: String,
    /// Client or investor account ID.
    pub client_id: String,
    /// Distributed trace correlation UUID.
    pub correlation_id: String,
    /// Client-generated order UUID.
    pub client_order_id: String,
    /// Broker-assigned order ID.
    pub broker_order_id: Option<String>,
    /// Exchange-assigned order ID.
    pub exchange_order_id: Option<String>,
    /// Traded instrument canonical string (e.g. `NSE:TCS`).
    pub instrument: String,
    /// Order side: `BUY`, `SELL`, or `NONE`.
    pub side: String,
    /// Order instructions: `market`, `limit`, etc.
    pub order_type: String,
    /// Quantity string.
    pub quantity: String,
    /// Price string (if applicable).
    pub price: Option<String>,
    /// State machine status: `pending_new`, `new`, `rejected`, `cancelled`, `filled`, etc.
    pub status: String,
    /// Detailed refusal or rejection reason if refused.
    pub refusal_reason: Option<String>,
    /// Latency marker in microseconds (evaluation or dispatch latency).
    pub latency_us: Option<i64>,
    /// Additional metadata in JSON.
    pub metadata_json: Option<String>,
}

/// An append-only SQLite regulatory audit ledger.
#[derive(Debug, Clone)]
pub struct SqliteAuditLedger {
    pool: SqlitePool,
}

impl SqliteAuditLedger {
    /// Create a new in-memory SQLite audit ledger (useful for backtests and integration tests).
    pub async fn new_in_memory() -> Result<Self, AuditError> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        let ledger = Self { pool };
        ledger.init_schema().await?;
        Ok(ledger)
    }

    /// Open or create an SQLite audit database file.
    pub async fn new_file(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path_str = path.as_ref().to_string_lossy();
        let conn_str = format!("sqlite://{}", path_str);
        let opts = SqliteConnectOptions::from_str(&conn_str)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(opts)
            .await?;
        let ledger = Self { pool };
        ledger.init_schema().await?;
        Ok(ledger)
    }

    /// Construct from an existing SQLite connection pool.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self, AuditError> {
        let ledger = Self { pool };
        ledger.init_schema().await?;
        Ok(ledger)
    }

    /// Get a reference to the underlying sqlx pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Ensure the audit table and indices are created.
    pub async fn init_schema(&self) -> Result<(), AuditError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sebi_audit_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                timestamp_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                algo_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                broker_order_id TEXT,
                exchange_order_id TEXT,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity TEXT NOT NULL,
                price TEXT,
                status TEXT NOT NULL,
                refusal_reason TEXT,
                latency_us INTEGER,
                metadata_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sebi_audit_algo ON sebi_audit_ledger(algo_id);
            CREATE INDEX IF NOT EXISTS idx_sebi_audit_correlation ON sebi_audit_ledger(correlation_id);
            CREATE INDEX IF NOT EXISTS idx_sebi_audit_ts ON sebi_audit_ledger(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_sebi_audit_event ON sebi_audit_ledger(event_type);
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Persist an order submission attempt (`order_attempt`).
    pub async fn record_order_attempt(&self, req: &OrderRequest) -> Result<i64, AuditError> {
        let now = Utc::now();
        let ts_ms = now.timestamp_millis();
        let ts_utc = now.to_rfc3339();
        let price_str = req.order_type.limit_price().map(|p| p.0.to_string());
        let order_type_str = format!("{:?}", req.order_type);
        let side_str = format!("{:?}", req.side);

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'order_attempt', ?3, ?4,
                ?5, ?6, NULL, NULL,
                ?7, ?8, ?9, ?10, ?11, 'pending_new', NULL,
                NULL, ?12
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(&req.algo_id)
        .bind(req.client_id_str())
        .bind(req.correlation_id().to_string())
        .bind(req.client_order_id.to_string())
        .bind(req.instrument.to_string())
        .bind(side_str)
        .bind(order_type_str)
        .bind(req.quantity.0.to_string())
        .bind(price_str)
        .bind(req.tag.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Persist a pre-trade risk refusal (`risk_refusal`).
    pub async fn record_risk_refusal(
        &self,
        req: &OrderRequest,
        refusal_reason: &str,
        eval_duration_us: u64,
    ) -> Result<i64, AuditError> {
        let now = Utc::now();
        let ts_ms = now.timestamp_millis();
        let ts_utc = now.to_rfc3339();
        let price_str = req.order_type.limit_price().map(|p| p.0.to_string());
        let order_type_str = format!("{:?}", req.order_type);
        let side_str = format!("{:?}", req.side);

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'risk_refusal', ?3, ?4,
                ?5, ?6, NULL, NULL,
                ?7, ?8, ?9, ?10, ?11, 'refused', ?12,
                ?13, NULL
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(&req.algo_id)
        .bind(req.client_id_str())
        .bind(req.correlation_id().to_string())
        .bind(req.client_order_id.to_string())
        .bind(req.instrument.to_string())
        .bind(side_str)
        .bind(order_type_str)
        .bind(req.quantity.0.to_string())
        .bind(price_str)
        .bind(refusal_reason)
        .bind(eval_duration_us as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Persist a broker acknowledgement (`broker_ack`).
    pub async fn record_broker_ack(
        &self,
        req: &OrderRequest,
        ack: &OrderAck,
        dispatch_duration_us: u64,
    ) -> Result<i64, AuditError> {
        let ts_ms = ack.timestamp.timestamp_millis();
        let ts_utc = ack.timestamp.to_rfc3339();
        let price_str = req.order_type.limit_price().map(|p| p.0.to_string());
        let order_type_str = format!("{:?}", req.order_type);
        let side_str = format!("{:?}", req.side);
        let status_str = format!("{:?}", ack.status).to_lowercase();

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'broker_ack', ?3, ?4,
                ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13, ?14, NULL,
                ?15, NULL
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(&req.algo_id)
        .bind(req.client_id_str())
        .bind(ack.correlation_id().to_string())
        .bind(ack.client_order_id.to_string())
        .bind(&ack.order_id)
        .bind(ack.exchange_order_id.as_deref())
        .bind(req.instrument.to_string())
        .bind(side_str)
        .bind(order_type_str)
        .bind(req.quantity.0.to_string())
        .bind(price_str)
        .bind(status_str)
        .bind(dispatch_duration_us as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Persist an order cancellation (`cancellation`).
    pub async fn record_cancellation(
        &self,
        algo_id: &str,
        client_id: &str,
        correlation_id: Uuid,
        order_id: &str,
        instrument: &str,
        reason: &str,
        latency_us: u64,
    ) -> Result<i64, AuditError> {
        let now = Utc::now();
        let ts_ms = now.timestamp_millis();
        let ts_utc = now.to_rfc3339();

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'cancellation', ?3, ?4,
                ?5, '', ?6, NULL,
                ?7, 'NONE', 'cancel', '0', NULL, 'cancelled', ?8,
                ?9, NULL
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(algo_id)
        .bind(client_id)
        .bind(correlation_id.to_string())
        .bind(order_id)
        .bind(instrument)
        .bind(reason)
        .bind(latency_us as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Persist an order modification (`modification`).
    pub async fn record_modification(
        &self,
        algo_id: &str,
        client_id: &str,
        correlation_id: Uuid,
        order_id: &str,
        instrument: &str,
        new_price: Option<Price>,
        new_qty: Quantity,
        latency_us: u64,
    ) -> Result<i64, AuditError> {
        let now = Utc::now();
        let ts_ms = now.timestamp_millis();
        let ts_utc = now.to_rfc3339();
        let price_str = new_price.map(|p| p.0.to_string());

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'modification', ?3, ?4,
                ?5, '', ?6, NULL,
                ?7, 'NONE', 'modification', ?8, ?9, 'modified', NULL,
                ?10, NULL
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(algo_id)
        .bind(client_id)
        .bind(correlation_id.to_string())
        .bind(order_id)
        .bind(instrument)
        .bind(new_qty.0.to_string())
        .bind(price_str)
        .bind(latency_us as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Persist an execution fill report (`fill`).
    pub async fn record_fill(&self, fill: &FillReport) -> Result<i64, AuditError> {
        let ts_ms = fill.timestamp.timestamp_millis();
        let ts_utc = fill.timestamp.to_rfc3339();
        let side_str = format!("{:?}", fill.side);

        let row = sqlx::query(
            r#"
            INSERT INTO sebi_audit_ledger (
                timestamp_ms, timestamp_utc, event_type, algo_id, client_id,
                correlation_id, client_order_id, broker_order_id, exchange_order_id,
                instrument, side, order_type, quantity, price, status, refusal_reason,
                latency_us, metadata_json
            ) VALUES (
                ?1, ?2, 'fill', ?3, ?4,
                ?5, ?6, ?7, NULL,
                ?8, ?9, 'fill', ?10, ?11, 'filled', NULL,
                NULL, ?12
            ) RETURNING id;
            "#,
        )
        .bind(ts_ms)
        .bind(ts_utc)
        .bind(&fill.algo_id)
        .bind(fill.client_id_str())
        .bind(fill.correlation_id().to_string())
        .bind(fill.client_order_id.to_string())
        .bind(&fill.order_id)
        .bind(fill.instrument.to_string())
        .bind(side_str)
        .bind(fill.quantity.0.to_string())
        .bind(fill.price.0.to_string())
        .bind(format!("{{\"fill_id\": \"{}\"}}", fill.fill_id))
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get(0))
    }

    /// Query audit records by algo ID.
    pub async fn query_by_algo_id(
        &self,
        algo_id: &str,
    ) -> Result<Vec<AuditLedgerRecord>, AuditError> {
        let rows = sqlx::query(
            "SELECT * FROM sebi_audit_ledger WHERE algo_id = ?1 ORDER BY timestamp_ms ASC, id ASC",
        )
        .bind(algo_id)
        .fetch_all(&self.pool)
        .await?;

        self.rows_to_records(rows)
    }

    /// Query audit records by correlation ID.
    pub async fn query_by_correlation_id(
        &self,
        correlation_id: &str,
    ) -> Result<Vec<AuditLedgerRecord>, AuditError> {
        let rows = sqlx::query(
            "SELECT * FROM sebi_audit_ledger WHERE correlation_id = ?1 ORDER BY timestamp_ms ASC, id ASC"
        )
        .bind(correlation_id)
        .fetch_all(&self.pool)
        .await?;

        self.rows_to_records(rows)
    }

    /// Query audit records by event type.
    pub async fn query_by_event_type(
        &self,
        event_type: &str,
    ) -> Result<Vec<AuditLedgerRecord>, AuditError> {
        let rows = sqlx::query(
            "SELECT * FROM sebi_audit_ledger WHERE event_type = ?1 ORDER BY timestamp_ms ASC, id ASC"
        )
        .bind(event_type)
        .fetch_all(&self.pool)
        .await?;

        self.rows_to_records(rows)
    }

    /// Query all records in the audit ledger.
    pub async fn query_all(&self) -> Result<Vec<AuditLedgerRecord>, AuditError> {
        let rows = sqlx::query("SELECT * FROM sebi_audit_ledger ORDER BY timestamp_ms ASC, id ASC")
            .fetch_all(&self.pool)
            .await?;

        self.rows_to_records(rows)
    }

    /// Count total records in the audit ledger.
    pub async fn count(&self) -> Result<i64, AuditError> {
        let row = sqlx::query("SELECT COUNT(*) FROM sebi_audit_ledger")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    /// Export all audit records to NDJSON.
    pub async fn export_ndjson(
        &self,
        writer: &mut (impl std::io::Write + Send),
    ) -> Result<usize, AuditError> {
        let records = self.query_all().await?;
        let count = records.len();
        for rec in &records {
            let line = serde_json::to_string(rec)?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(count)
    }

    fn rows_to_records(
        &self,
        rows: Vec<sqlx::sqlite::SqliteRow>,
    ) -> Result<Vec<AuditLedgerRecord>, AuditError> {
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            records.push(AuditLedgerRecord {
                id: row.get("id"),
                timestamp_ms: row.get("timestamp_ms"),
                timestamp_utc: row.get("timestamp_utc"),
                event_type: row.get("event_type"),
                algo_id: row.get("algo_id"),
                client_id: row.get("client_id"),
                correlation_id: row.get("correlation_id"),
                client_order_id: row.get("client_order_id"),
                broker_order_id: row.get("broker_order_id"),
                exchange_order_id: row.get("exchange_order_id"),
                instrument: row.get("instrument"),
                side: row.get("side"),
                order_type: row.get("order_type"),
                quantity: row.get("quantity"),
                price: row.get("price"),
                status: row.get("status"),
                refusal_reason: row.get("refusal_reason"),
                latency_us: row.get("latency_us"),
                metadata_json: row.get("metadata_json"),
            });
        }
        Ok(records)
    }
}
