//! LLM / strategy decision persistence with SHA-256 prompt checksums.
//!
//! Every decision row records the SHA-256 of the exact prompt payload so a
//! decision can be deterministically replayed against the same inputs.

use crate::{Database, with_executor};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use thiserror::Error;

/// Decision store errors.
#[derive(Debug, Error)]
pub enum DecisionStoreError {
    /// Database error from sqlx.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// No decision matched the lookup key.
    #[error("decision not found: {0}")]
    NotFound(String),
    /// Prompt checksum is not a 64-character lowercase hex SHA-256 digest.
    #[error("invalid prompt sha256 (expected 64 hex chars): {0}")]
    InvalidPromptSha256(String),
}

/// Canonical SELECT projecting DECIMAL columns as TEXT.
const SELECT_DECISIONS: &str = "SELECT id, decision_id, algo_id, prompt_sha256, model_name, \
     seed, CAST(temperature AS TEXT) AS temperature, decision_kind, spec_json, created_at \
     FROM decisions";

/// Compute the lowercase hex SHA-256 digest of a prompt payload.
///
/// Callers that hold the raw prompt text can pass it here instead of a
/// precomputed digest.
pub fn sha256_hex(prompt: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(prompt.as_bytes()))
}

/// Validate a prompt checksum: exactly 64 lowercase hex characters.
fn validate_prompt_sha256(s: &str) -> Result<(), DecisionStoreError> {
    let ok = s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
    if ok {
        Ok(())
    } else {
        Err(DecisionStoreError::InvalidPromptSha256(s.to_string()))
    }
}

/// Persisted decision row with typed accessors.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedDecision {
    /// Surrogate row ID.
    pub id: i64,
    /// Caller-assigned unique decision ID.
    pub decision_id: String,
    /// SEBI algo ID.
    pub algo_id: String,
    /// SHA-256 hex digest of the exact prompt payload (64 chars).
    pub prompt_sha256: String,
    /// Model identifier (e.g. `gpt-4o`, `claude-sonnet`).
    pub model_name: String,
    /// Sampling seed used for the decision (exact decimal string).
    pub seed: String,
    /// Sampling temperature (exact decimal string).
    pub temperature: String,
    /// Decision kind (e.g. `order_intent`, `rule`, `risk_override`).
    pub decision_kind: String,
    /// Canonical decision spec serialized as JSON.
    pub spec_json: String,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

impl PersistedDecision {
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
            decision_id: row.try_get("decision_id")?,
            algo_id: row.try_get("algo_id")?,
            prompt_sha256: row.try_get("prompt_sha256")?,
            model_name: row.try_get("model_name")?,
            seed: row.try_get("seed")?,
            temperature: row.try_get("temperature")?,
            decision_kind: row.try_get("decision_kind")?,
            spec_json: row.try_get("spec_json")?,
            created_at: row.try_get("created_at")?,
        })
    }

    /// Sampling seed parsed back to `u64`.
    pub fn seed_u64(&self) -> Option<u64> {
        self.seed.parse().ok()
    }

    /// Sampling temperature as an exact decimal.
    pub fn temperature_decimal(&self) -> Decimal {
        self.temperature.parse().unwrap_or(Decimal::ZERO)
    }
}

/// LLM / strategy decision repository over either sqlx backend.
#[derive(Debug, Clone)]
pub struct DecisionStore {
    db: Database,
}

impl DecisionStore {
    /// Create a store bound to a database handle (pool is shared cheaply).
    pub fn new(db: &Database) -> Self {
        Self { db: db.clone() }
    }

    /// Idempotent decision insert keyed on the unique `decision_id`.
    ///
    /// Re-inserting an existing `decision_id` returns the existing row
    /// unchanged (absorbed via `ON CONFLICT DO NOTHING` + re-read, so
    /// concurrent racers can never duplicate rows).
    pub async fn insert(
        &self,
        decision_id: &str,
        algo_id: &str,
        prompt_sha256: &str,
        model_name: &str,
        seed: u64,
        temperature: Decimal,
        decision_kind: &str,
        spec_json: &str,
        ts: DateTime<Utc>,
    ) -> Result<PersistedDecision, DecisionStoreError> {
        validate_prompt_sha256(prompt_sha256)?;
        if let Some(existing) = self.get_by_id(decision_id).await? {
            return Ok(existing);
        }
        let seed_str = seed.to_string();
        let temp_str = temperature.to_string();
        with_executor!(&self.db, |ex| {
            sqlx::query(
                "INSERT INTO decisions (
                     decision_id, algo_id, prompt_sha256, model_name, seed,
                     temperature, decision_kind, spec_json, created_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (decision_id) DO NOTHING",
            )
            .bind(decision_id)
            .bind(algo_id)
            .bind(prompt_sha256)
            .bind(model_name)
            .bind(&seed_str)
            .bind(&temp_str)
            .bind(decision_kind)
            .bind(spec_json)
            .bind(ts)
            .execute(ex)
            .await
            .map(|_| ())
        })?;
        self.get_by_id(decision_id)
            .await?
            .ok_or_else(|| DecisionStoreError::NotFound(decision_id.to_string()))
    }

    /// Look up a decision by its unique decision ID.
    pub async fn get_by_id(
        &self,
        decision_id: &str,
    ) -> Result<Option<PersistedDecision>, DecisionStoreError> {
        let row = with_executor!(&self.db, |ex| {
            sqlx::query(&format!("{SELECT_DECISIONS} WHERE decision_id = $1"))
                .bind(decision_id)
                .fetch_optional(ex)
                .await?
                .as_ref()
                .map(PersistedDecision::from_row)
                .transpose()
        })?;
        Ok(row)
    }

    /// List decisions for an algo, newest first, limited to `limit` rows.
    pub async fn list_by_algo(
        &self,
        algo_id: &str,
        limit: i64,
    ) -> Result<Vec<PersistedDecision>, DecisionStoreError> {
        with_executor!(&self.db, |ex| {
            sqlx::query(&format!(
                "{SELECT_DECISIONS} WHERE algo_id = $1 ORDER BY created_at DESC LIMIT $2"
            ))
            .bind(algo_id)
            .bind(limit)
            .fetch_all(ex)
            .await?
            .iter()
            .map(PersistedDecision::from_row)
            .collect::<Result<Vec<_>, sqlx::Error>>()
        })
        .map_err(Into::into)
    }
}
