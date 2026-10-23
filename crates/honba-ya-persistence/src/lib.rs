//! sqlx persistence for `honba-ya` (PostgreSQL / SQLite): relational
//! repositories, an embedded schema-migration runner, and the SEBI
//! append-only regulatory audit ledger.
//!
//! All monetary values are persisted as exact decimal strings
//! (`DECIMAL(30,16)` on PostgreSQL, `TEXT` on SQLite) and all timestamps are
//! passed explicitly as [`chrono::DateTime<Utc>`] parameters — the store layer
//! never reads a clock implicitly in domain arithmetic.

pub mod audit;
pub mod decisions;
pub mod fills;
pub mod messages;
pub mod migrations;
pub mod orders;
pub mod positions;
pub mod traders;

pub use audit::{AuditError, AuditLedgerRecord, SqliteAuditLedger};
pub use decisions::{sha256_hex, DecisionStore, DecisionStoreError, PersistedDecision};
pub use fills::{FillStore, FillStoreError, PersistedFill};
pub use messages::RiskBreachEvent;
pub use migrations::{migrate, rollback_last, MigrationError};
pub use orders::{OrderStore, OrderStoreError, PersistedOrder};
pub use positions::{PersistedPosition, PersistedPositionLot, PositionStore, PositionStoreError};
pub use traders::{PersistedTrader, TraderStore, TraderStoreError};

/// Explicit repository type aliases matching the system architecture.
pub type OrderRepository = OrderStore;
pub type FillRepository = FillStore;
pub type PositionRepository = PositionStore;
pub type DecisionRepository = DecisionStore;
pub type TraderRepository = TraderStore;

use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{PgPool, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

/// Execute a query body against whichever backend pool the [`Database`] wraps.
///
/// The body receives an `&PgPool` or `&SqlitePool` binding and must evaluate
/// to the same result type for both backends (e.g. `Result<T, sqlx::Error>`).
macro_rules! with_executor {
    ($db:expr, |$ex:ident| $body:expr) => {
        match $db {
            Database::Postgres($ex) => $body,
            Database::Sqlite($ex) => $body,
        }
    };
}
pub(crate) use with_executor;

/// A connected database handle wrapping exactly one sqlx backend pool.
#[derive(Debug, Clone)]
pub enum Database {
    /// PostgreSQL pool (live institutional deployments).
    Postgres(PgPool),
    /// SQLite pool (embedded edge nodes, backtests, and tests).
    Sqlite(SqlitePool),
}

impl Database {
    /// Connect using a `postgres://` / `postgresql://` or `sqlite:` URL.
    ///
    /// SQLite databases are created if missing, run in WAL mode (file-backed
    /// only) with a 5 s busy timeout; `sqlite::memory:` databases are pinned
    /// to a single connection so the in-memory image is shared and kept alive
    /// for the lifetime of the pool.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            let pool = PgPoolOptions::new().max_connections(16).connect(url).await?;
            Ok(Self::Postgres(pool))
        } else if url.starts_with("sqlite:") {
            let in_memory = url.contains(":memory:");
            let mut opts = SqliteConnectOptions::from_str(url)?
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(5));
            if !in_memory {
                opts = opts.journal_mode(SqliteJournalMode::Wal);
            }
            let max_connections = if in_memory { 1 } else { 10 };
            let pool = SqlitePoolOptions::new()
                .max_connections(max_connections)
                .connect_with(opts)
                .await?;
            Ok(Self::Sqlite(pool))
        } else {
            Err(sqlx::Error::Configuration(
                format!(
                    "unsupported database URL scheme (expected `postgres://`, `postgresql://` or `sqlite:`): {url}"
                )
                .into(),
            ))
        }
    }

    /// Returns `true` when backed by SQLite.
    pub fn is_sqlite(&self) -> bool {
        matches!(self, Self::Sqlite(_))
    }

    /// Health-check ping.
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        with_executor!(self, |ex| {
            sqlx::query("SELECT 1").execute(ex).await.map(|_| ())
        })
    }
}

/// Parse a canonical `"EXCHANGE:SYMBOL"` instrument string (as produced by
/// `InstrumentId`'s `Display` impl) back into an [`honba_core::InstrumentId`].
pub(crate) fn parse_instrument(s: &str) -> Option<honba_core::InstrumentId> {
    let (exchange, symbol) = s.split_once(':')?;
    if symbol.is_empty() {
        return None;
    }
    let exchange = honba_core::Exchange::from_str(exchange).ok()?;
    Some(honba_core::InstrumentId::new(exchange, symbol))
}
