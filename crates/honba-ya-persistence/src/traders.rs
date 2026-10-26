//! Trader identity, risk profile, and allocated capital persistence.

use crate::Database;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use thiserror::Error;

/// Trader store errors.
#[derive(Debug, Error)]
pub enum TraderStoreError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// No trader matched the lookup key.
    #[error("trader not found: {0}")]
    NotFound(String),
    /// Capital string could not be parsed as exact Decimal.
    #[error("invalid capital decimal string: {0}")]
    InvalidCapital(String),
}

/// Canonical SELECT projecting capital as TEXT.
const SELECT_TRADERS: &str = "SELECT id, client_id, algo_id, risk_profile, \
    CAST(capital AS TEXT) AS capital, created_at, updated_at FROM traders";

/// Persisted trader row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTrader {
    /// Row identifier.
    pub id: i64,
    /// Client / account identifier.
    pub client_id: String,
    /// Authoritative SEBI algo identifier.
    pub algo_id: String,
    /// Risk profile category (e.g. `retail`, `prop`, `institutional`).
    pub risk_profile: String,
    /// Allocated capital.
    pub capital: Decimal,
    /// Registration timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl PersistedTrader {
    fn from_row<R>(row: &R) -> Result<Self, TraderStoreError>
    where
        R: sqlx::Row,
        for<'a> &'a str: sqlx::ColumnIndex<R>,
        i64: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        String: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
        DateTime<Utc>: sqlx::Type<R::Database> + for<'r> sqlx::Decode<'r, R::Database>,
    {
        let id: i64 = row.try_get("id").map_err(TraderStoreError::Database)?;
        let client_id: String = row
            .try_get("client_id")
            .map_err(TraderStoreError::Database)?;
        let algo_id: String = row.try_get("algo_id").map_err(TraderStoreError::Database)?;
        let risk_profile: String = row
            .try_get("risk_profile")
            .map_err(TraderStoreError::Database)?;
        let cap_str: String = row.try_get("capital").map_err(TraderStoreError::Database)?;
        let capital =
            Decimal::from_str(&cap_str).map_err(|_| TraderStoreError::InvalidCapital(cap_str))?;
        let created_at: DateTime<Utc> = row
            .try_get("created_at")
            .map_err(TraderStoreError::Database)?;
        let updated_at: DateTime<Utc> = row
            .try_get("updated_at")
            .map_err(TraderStoreError::Database)?;

        Ok(Self {
            id,
            client_id,
            algo_id,
            risk_profile,
            capital,
            created_at,
            updated_at,
        })
    }
}

/// Repository for trader profile and capital allocation records.
#[derive(Debug, Clone)]
pub struct TraderStore {
    db: Database,
}

impl TraderStore {
    /// Bind this store to a database pool.
    pub fn new(db: &Database) -> Self {
        Self { db: db.clone() }
    }

    /// Idempotent upsert of a trader record keyed by `client_id`.
    pub async fn upsert(
        &self,
        client_id: &str,
        algo_id: &str,
        risk_profile: &str,
        capital: Decimal,
        timestamp: DateTime<Utc>,
    ) -> Result<PersistedTrader, TraderStoreError> {
        let cap_str = capital.to_string();
        match &self.db {
            Database::Postgres(pool) => {
                let sql = format!(
                    "INSERT INTO traders (client_id, algo_id, risk_profile, capital, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4::DECIMAL, $5, $6) \
                     ON CONFLICT (client_id) DO UPDATE SET \
                        algo_id = EXCLUDED.algo_id, \
                        risk_profile = EXCLUDED.risk_profile, \
                        capital = EXCLUDED.capital, \
                        updated_at = EXCLUDED.updated_at \
                     RETURNING id, client_id, algo_id, risk_profile, CAST(capital AS TEXT) AS capital, created_at, updated_at"
                );
                let row = sqlx::query(&sql)
                    .bind(client_id)
                    .bind(algo_id)
                    .bind(risk_profile)
                    .bind(&cap_str)
                    .bind(timestamp)
                    .bind(timestamp)
                    .fetch_one(pool)
                    .await?;
                PersistedTrader::from_row(&row)
            }
            Database::Sqlite(pool) => {
                let sql = "INSERT INTO traders (client_id, algo_id, risk_profile, capital, created_at, updated_at) \
                     VALUES (?, ?, ?, ?, ?, ?) \
                     ON CONFLICT(client_id) DO UPDATE SET \
                        algo_id = excluded.algo_id, \
                        risk_profile = excluded.risk_profile, \
                        capital = excluded.capital, \
                        updated_at = excluded.updated_at";
                let created_str = timestamp.to_rfc3339();
                sqlx::query(sql)
                    .bind(client_id)
                    .bind(algo_id)
                    .bind(risk_profile)
                    .bind(&cap_str)
                    .bind(&created_str)
                    .bind(&created_str)
                    .execute(pool)
                    .await?;

                self.get_by_client_id(client_id)
                    .await?
                    .ok_or_else(|| TraderStoreError::NotFound(client_id.to_string()))
            }
        }
    }

    /// Fetch a trader by `client_id`.
    pub async fn get_by_client_id(
        &self,
        client_id: &str,
    ) -> Result<Option<PersistedTrader>, TraderStoreError> {
        let sql = format!("{SELECT_TRADERS} WHERE client_id = $1");
        let sql_sqlite = format!("{SELECT_TRADERS} WHERE client_id = ?");
        match &self.db {
            Database::Postgres(pool) => {
                let maybe_row = sqlx::query(&sql)
                    .bind(client_id)
                    .fetch_optional(pool)
                    .await?;
                maybe_row.map(|r| PersistedTrader::from_row(&r)).transpose()
            }
            Database::Sqlite(pool) => {
                let maybe_row = sqlx::query(&sql_sqlite)
                    .bind(client_id)
                    .fetch_optional(pool)
                    .await?;
                maybe_row.map(|r| PersistedTrader::from_row(&r)).transpose()
            }
        }
    }

    /// Fetch a trader by `algo_id`.
    pub async fn get_by_algo_id(
        &self,
        algo_id: &str,
    ) -> Result<Option<PersistedTrader>, TraderStoreError> {
        let sql = format!("{SELECT_TRADERS} WHERE algo_id = $1");
        let sql_sqlite = format!("{SELECT_TRADERS} WHERE algo_id = ?");
        match &self.db {
            Database::Postgres(pool) => {
                let maybe_row = sqlx::query(&sql).bind(algo_id).fetch_optional(pool).await?;
                maybe_row.map(|r| PersistedTrader::from_row(&r)).transpose()
            }
            Database::Sqlite(pool) => {
                let maybe_row = sqlx::query(&sql_sqlite)
                    .bind(algo_id)
                    .fetch_optional(pool)
                    .await?;
                maybe_row.map(|r| PersistedTrader::from_row(&r)).transpose()
            }
        }
    }

    /// List all traders ordered by `id` ascending.
    pub async fn list(&self) -> Result<Vec<PersistedTrader>, TraderStoreError> {
        let sql = format!("{SELECT_TRADERS} ORDER BY id ASC");
        match &self.db {
            Database::Postgres(pool) => {
                let rows = sqlx::query(&sql).fetch_all(pool).await?;
                rows.iter().map(PersistedTrader::from_row).collect()
            }
            Database::Sqlite(pool) => {
                let rows = sqlx::query(&sql).fetch_all(pool).await?;
                rows.iter().map(PersistedTrader::from_row).collect()
            }
        }
    }
}
