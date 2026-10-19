//! sqlx repositories (PostgreSQL / SQLite).

pub mod audit;

pub use audit::{AuditError, AuditLedgerRecord, SqliteAuditLedger};

use sqlx::PgPool;

/// Unified database repositories.
pub struct Repos {
    /// PostgreSQL connection pool
    pub pg: PgPool,
}

impl Repos {
    /// Create repository from existing Postgres connection pool.
    pub fn from_pg(pg: PgPool) -> Self {
        Self { pg }
    }

    /// Health check ping.
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pg).await?;
        Ok(())
    }
}
