//! Per-user portfolio store: one JSON file per user id in a directory.
//!
//! A portfolio is the money a user holds: accounts with cash, mode/source, margins and the raw
//! positions they carry, plus their trade history. It mirrors [`crate::ProfileStore`] exactly —
//! one JSON file per user id, a revision compare-and-swap, atomic temp+rename+fsync writes,
//! size/count caps and corrupt-file quarantine — because it is the same kind of thing: a small
//! per-user document owned by the request's `Caller`. User ids come from bearer `token_id`s and
//! are therefore untrusted: they are validated before they touch a path.
//!
//! This is the *input* to valuation, not a valued report: the read routes load a document and
//! turn it into a [`honba_portfolio::PortfolioSnapshot`] against the latest marks (ADR 0032 §3,
//! §5). The stored document carries no marks and no clock.
//!
//! The in-process mutex makes each read-check-write atomic within one process only. There is no
//! cross-process file lock, so run **one `honba serve` per portfolios directory**.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use honba_entities::{
    Currency, Money, PortfolioGreeks, PortfolioMode, PortfolioSource, Position, Trade,
};
use honba_messages::Product;
use serde::{Deserialize, Serialize};

use crate::user_doc;

/// Largest serialized portfolio a user may store, in bytes.
pub const MAX_PORTFOLIO_BYTES: usize = 2 * 1024 * 1024;

/// Most accounts one portfolio may hold.
pub const MAX_ACCOUNTS: usize = 100;

/// Most positions one account may hold.
pub const MAX_POSITIONS_PER_ACCOUNT: usize = 5_000;

/// Most trade-history records one portfolio may hold.
pub const MAX_TRADES: usize = 100_000;

/// Why a portfolio operation failed.
#[derive(Debug, thiserror::Error)]
pub enum PortfolioError {
    /// The user id is empty, too long, or has characters outside `[A-Za-z0-9_-]`.
    #[error("invalid user id")]
    InvalidUserId,
    /// The caller's expected revision is not the stored one.
    #[error("revision conflict: expected {expected}, current {current}")]
    RevisionConflict {
        /// Revision the caller sent.
        expected: u64,
        /// Revision currently stored.
        current: u64,
    },
    /// The portfolio exceeds a size or count limit; `what` names the limit.
    #[error("portfolio too large: {what}")]
    TooLarge {
        /// Which limit was exceeded.
        what: String,
    },
    /// Reading or writing the portfolio file failed.
    #[error("portfolio io: {0}")]
    Io(#[from] std::io::Error),
    /// A portfolio could not be serialized.
    #[error("portfolio json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result alias for portfolio operations.
pub type Result<T> = std::result::Result<T, PortfolioError>;

/// A position as the ledger holds it: the [`Position`] plus the product it is under.
///
/// The product decides whether valuation reports it as an open position or a delivery
/// [`honba_entities::PortfolioHolding`] (ADR 0032 §2); the pair is the valuation engine's
/// [`honba_portfolio::HeldPosition`] in its serializable form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortfolioHeldPosition {
    /// The position (quantity, average price, direction, realized PnL).
    pub position: Position,
    /// The product the position is held under.
    pub product: Product,
    /// Sub-basket / logical group (from QuantDinger design: e.g. "Core", "Hedge").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    /// Categorization tags (from QuantDinger design).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Associated strategy tag (from OpenAlgo design).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_id: Option<String>,
}

/// One account as the ledger holds it: cash, mode/source and the raw positions it carries.
///
/// The serializable input to valuation; [`honba_entities::PortfolioAccount`] is the valued
/// report it projects into (ADR 0032 §2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortfolioAccountRecord {
    /// Stable account id within the portfolio.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Live or sandbox money (ADR 0032 §4).
    pub mode: PortfolioMode,
    /// Where the positions and funds come from (ADR 0032 §6).
    pub source: PortfolioSource,
    /// The broker this account is synced from, when `source` is
    /// [`PortfolioSource::Broker`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
    /// The settlement currency of the account's amounts.
    pub currency: Currency,
    /// Free cash.
    pub cash: Money,
    /// Account leverage, `1.0` when unlevered.
    pub leverage: f64,
    /// Margin currently committed.
    pub margin_used: Money,
    /// Margin still available to trade.
    pub margin_available: Money,
    /// The positions the account holds.
    pub positions: Vec<PortfolioHeldPosition>,
    /// Net aggregate option Greeks across positions, when options are present (OpenAlgo design).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub greeks: Option<PortfolioGreeks>,
}

/// The durable per-user portfolio: the state valuation runs over (ADR 0032 §5).
///
/// One document per user, stored as `<dir>/<user_id>.json`. It is not a valued snapshot: no
/// marks and no `as_of` live here, so a mark refresh revalues without rewriting the document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PortfolioDocument {
    /// Monotonic revision, bumped by every successful write.
    pub revision: u64,
    /// The currency portfolio totals are reported in.
    pub base_currency: Currency,
    /// The accounts, in the order they were stored (valuation sorts them by id).
    pub accounts: Vec<PortfolioAccountRecord>,
    /// The user's trade history.
    pub trades: Vec<Trade>,
}

impl Default for PortfolioDocument {
    /// An empty portfolio: revision 0, INR base currency, no accounts and no trades.
    fn default() -> Self {
        Self {
            revision: 0,
            base_currency: Currency::Inr,
            accounts: Vec::new(),
            trades: Vec::new(),
        }
    }
}

/// Directory of per-user portfolio files.
#[derive(Debug)]
pub struct PortfolioStore {
    dir: PathBuf,
    /// Serializes writers so the read-check-write of the revision is atomic in-process.
    write_lock: Mutex<()>,
}

impl PortfolioStore {
    /// A store rooted at `dir`; the directory is created on first write.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            write_lock: Mutex::new(()),
        }
    }

    /// The user's portfolio, or a default one (revision 0) when none is stored.
    pub fn get(&self, user_id: &str) -> Result<PortfolioDocument> {
        let path = self.path_for(user_id)?;
        read_document(&path)
    }

    /// Replace the user's portfolio if `expected_revision` is the stored revision. The stored
    /// document gets `expected_revision + 1`, whatever revision `new_document` carries. If
    /// `new_document` equals the stored one apart from the revision, nothing is written and the
    /// stored document (same revision) is returned.
    pub fn put(
        &self,
        user_id: &str,
        expected_revision: u64,
        mut new_document: PortfolioDocument,
    ) -> Result<PortfolioDocument> {
        let path = self.path_for(user_id)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stored = read_document(&path)?;
        let current = stored.revision;
        if current != expected_revision {
            return Err(PortfolioError::RevisionConflict {
                expected: expected_revision,
                current,
            });
        }
        // Saving what is already stored (a debounced client re-sending) writes nothing.
        new_document.revision = current;
        if new_document == stored {
            return Ok(stored);
        }
        new_document.revision = current + 1;
        let bytes = encode_within_limits(&new_document)?;
        user_doc::create_dir_durable(&self.dir)?;
        user_doc::write_atomic(&path, &bytes)?;
        Ok(new_document)
    }

    /// Read-modify-write the user's portfolio under the store lock. `change` returns whether it
    /// altered the document; if so the revision is bumped once and the file rewritten atomically,
    /// otherwise nothing is written. Returns the document as stored afterwards.
    pub fn update(
        &self,
        user_id: &str,
        change: impl FnOnce(&mut PortfolioDocument) -> bool,
    ) -> Result<PortfolioDocument> {
        let path = self.path_for(user_id)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut document = read_document(&path)?;
        if !change(&mut document) {
            return Ok(document);
        }
        document.revision += 1;
        let bytes = encode_within_limits(&document)?;
        user_doc::create_dir_durable(&self.dir)?;
        user_doc::write_atomic(&path, &bytes)?;
        Ok(document)
    }

    fn path_for(&self, user_id: &str) -> Result<PathBuf> {
        user_doc::path_for(&self.dir, user_id).ok_or(PortfolioError::InvalidUserId)
    }
}

/// Check the count limits, then serialize and check [`MAX_PORTFOLIO_BYTES`].
fn encode_within_limits(document: &PortfolioDocument) -> Result<Vec<u8>> {
    let too_large = |what: String| Err(PortfolioError::TooLarge { what });
    if document.accounts.len() > MAX_ACCOUNTS {
        return too_large(format!("more than {MAX_ACCOUNTS} accounts"));
    }
    if document
        .accounts
        .iter()
        .any(|a| a.positions.len() > MAX_POSITIONS_PER_ACCOUNT)
    {
        return too_large(format!(
            "more than {MAX_POSITIONS_PER_ACCOUNT} positions in an account"
        ));
    }
    if document.trades.len() > MAX_TRADES {
        return too_large(format!("more than {MAX_TRADES} trades"));
    }
    let bytes = serde_json::to_vec_pretty(document)?;
    if bytes.len() > MAX_PORTFOLIO_BYTES {
        return too_large(format!("more than {MAX_PORTFOLIO_BYTES} bytes"));
    }
    Ok(bytes)
}

/// Read the document at `path`; a missing file is the default portfolio. A file that is not
/// valid JSON for a portfolio is moved aside to `<name>.corrupt-<unix_ts>` and the user starts
/// over at revision 0, so one bad file cannot make every request for that user fail forever.
fn read_document(path: &Path) -> Result<PortfolioDocument> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PortfolioDocument::default())
        }
        Err(e) => return Err(e.into()),
    };
    match serde_json::from_slice(&bytes) {
        Ok(document) => Ok(document),
        Err(e) => {
            user_doc::quarantine_corrupt(path, "portfolio", &e);
            Ok(PortfolioDocument::default())
        }
    }
}
