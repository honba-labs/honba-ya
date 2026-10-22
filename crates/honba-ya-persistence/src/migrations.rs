//! Embedded, backend-aware SQL migration runner.
//!
//! Migration files live in `migrations/postgres/` and `migrations/sqlite/` as
//! `NNN_name.up.sql` / `NNN_name.down.sql` pairs and are compiled into the
//! binary via `include_str!`. Applied versions are tracked in a
//! `schema_migrations` table; each pending version's up SQL is applied
//! transactionally, so a failure leaves the database untouched. Both
//! [`migrate`] and [`rollback_last`] are fully idempotent.

use crate::Database;
use thiserror::Error;

/// Migration runner errors.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// A single embedded migration version.
struct EmbeddedMigration {
    /// Monotonic version number (1-based).
    version: i64,
    /// Human-readable migration name.
    name: &'static str,
    /// Forward (up) SQL for this backend.
    up: &'static str,
    /// Rollback (down) SQL for this backend.
    down: &'static str,
}

/// PostgreSQL migration set (includes the postgres-only `audit_ledger`).
const POSTGRES_MIGRATIONS: &[EmbeddedMigration] = &[
    EmbeddedMigration {
        version: 1,
        name: "001_traders",
        up: include_str!("../migrations/postgres/001_traders.up.sql"),
        down: include_str!("../migrations/postgres/001_traders.down.sql"),
    },
    EmbeddedMigration {
        version: 2,
        name: "002_orders",
        up: include_str!("../migrations/postgres/002_orders.up.sql"),
        down: include_str!("../migrations/postgres/002_orders.down.sql"),
    },
    EmbeddedMigration {
        version: 3,
        name: "003_fills",
        up: include_str!("../migrations/postgres/003_fills.up.sql"),
        down: include_str!("../migrations/postgres/003_fills.down.sql"),
    },
    EmbeddedMigration {
        version: 4,
        name: "004_decisions",
        up: include_str!("../migrations/postgres/004_decisions.up.sql"),
        down: include_str!("../migrations/postgres/004_decisions.down.sql"),
    },
    EmbeddedMigration {
        version: 5,
        name: "005_positions",
        up: include_str!("../migrations/postgres/005_positions.up.sql"),
        down: include_str!("../migrations/postgres/005_positions.down.sql"),
    },
    EmbeddedMigration {
        version: 6,
        name: "006_audit_ledger",
        up: include_str!("../migrations/postgres/006_audit_ledger.up.sql"),
        down: include_str!("../migrations/postgres/006_audit_ledger.down.sql"),
    },
];

/// SQLite migration set. The audit ledger is intentionally absent: on SQLite
/// the `sebi_audit_ledger` runtime table is owned and created by
/// [`crate::audit::SqliteAuditLedger`] itself.
const SQLITE_MIGRATIONS: &[EmbeddedMigration] = &[
    EmbeddedMigration {
        version: 1,
        name: "001_traders",
        up: include_str!("../migrations/sqlite/001_traders.up.sql"),
        down: include_str!("../migrations/sqlite/001_traders.down.sql"),
    },
    EmbeddedMigration {
        version: 2,
        name: "002_orders",
        up: include_str!("../migrations/sqlite/002_orders.up.sql"),
        down: include_str!("../migrations/sqlite/002_orders.down.sql"),
    },
    EmbeddedMigration {
        version: 3,
        name: "003_fills",
        up: include_str!("../migrations/sqlite/003_fills.up.sql"),
        down: include_str!("../migrations/sqlite/003_fills.down.sql"),
    },
    EmbeddedMigration {
        version: 4,
        name: "004_decisions",
        up: include_str!("../migrations/sqlite/004_decisions.up.sql"),
        down: include_str!("../migrations/sqlite/004_decisions.down.sql"),
    },
    EmbeddedMigration {
        version: 5,
        name: "005_positions",
        up: include_str!("../migrations/sqlite/005_positions.up.sql"),
        down: include_str!("../migrations/sqlite/005_positions.down.sql"),
    },
];

/// Select the migration set matching the database backend.
fn migrations_for(db: &Database) -> &'static [EmbeddedMigration] {
    match db {
        Database::Postgres(_) => POSTGRES_MIGRATIONS,
        Database::Sqlite(_) => SQLITE_MIGRATIONS,
    }
}

/// Ensure the `schema_migrations` tracking table exists.
async fn ensure_schema_migrations(db: &Database) -> Result<(), MigrationError> {
    crate::with_executor!(db, |ex| {
        let sql = match db {
            Database::Postgres(_) => {
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL
                )"
            }
            Database::Sqlite(_) => {
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )"
            }
        };
        sqlx::query(sql).execute(ex).await.map(|_| ())
    })
    .map_err(Into::into)
}

/// Return all currently applied versions in ascending order.
async fn applied_versions(db: &Database) -> Result<Vec<i64>, MigrationError> {
    crate::with_executor!(db, |ex| {
        sqlx::query_scalar::<_, i64>("SELECT version FROM schema_migrations ORDER BY version ASC")
            .fetch_all(ex)
            .await
    })
    .map_err(Into::into)
}

/// Apply every pending migration version in order.
///
/// Returns the versions applied by this call (empty when already
/// up-to-date), making repeated invocations fully idempotent.
pub async fn migrate(db: &Database) -> Result<Vec<i64>, MigrationError> {
    ensure_schema_migrations(db).await?;
    let applied = applied_versions(db).await?;
    let mut newly_applied = Vec::new();
    for m in migrations_for(db) {
        if applied.contains(&m.version) {
            continue;
        }
        apply_one(db, m).await?;
        tracing::info!(version = m.version, name = m.name, "applied migration");
        newly_applied.push(m.version);
    }
    Ok(newly_applied)
}

/// Apply a single migration's up SQL inside a transaction.
async fn apply_one(db: &Database, m: &EmbeddedMigration) -> Result<(), MigrationError> {
    let now = chrono::Utc::now();
    crate::with_executor!(db, |ex| {
        let mut tx = ex.begin().await?;
        sqlx::raw_sql(m.up).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, $3)",
        )
        .bind(m.version)
        .bind(m.name)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok::<(), sqlx::Error>(())
    })
    .map_err(Into::into)
}

/// Roll back the highest applied migration version using its down SQL.
///
/// Returns the rolled-back version, or `None` when there is nothing to roll
/// back. Idempotent: repeated calls walk the applied versions backwards.
pub async fn rollback_last(db: &Database) -> Result<Option<i64>, MigrationError> {
    ensure_schema_migrations(db).await?;
    let last: Option<i64> = crate::with_executor!(db, |ex| {
        sqlx::query_scalar::<_, i64>(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
        )
        .fetch_optional(ex)
        .await
    })
    .map_err(MigrationError::from)?;
    let Some(version) = last else {
        return Ok(None);
    };
    let m = migrations_for(db)
        .iter()
        .find(|m| m.version == version)
        .ok_or_else(|| {
            MigrationError::Database(sqlx::Error::Configuration(
                format!("no embedded migration found for applied version {version}").into(),
            ))
        })?;
    crate::with_executor!(db, |ex| {
        let mut tx = ex.begin().await?;
        sqlx::raw_sql(m.down).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM schema_migrations WHERE version = $1")
            .bind(version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<(), sqlx::Error>(())
    })
    .map_err(MigrationError::from)?;
    tracing::info!(version, name = m.name, "rolled back migration");
    Ok(Some(version))
}
