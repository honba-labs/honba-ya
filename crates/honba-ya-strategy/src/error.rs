//! The error type returned by [`crate::Strategy`] hooks and
//! [`crate::StrategyRunner`] operations.

use std::fmt;

use crate::intent::IntentError;

/// Error returned by a [`crate::Strategy`] hook, [`crate::StrategyAdapter`],
/// or [`crate::StrategyRunner`] operation.
///
/// This crate no longer depends on `honba-engine` (removed in the
/// `honba-core`/`honba-mon` migration), so there is no shared `AlgoError` to
/// reuse here; this is this crate's own, minimal error type.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum StrategyError {
    /// An [`crate::OrderIntent`] violated its invariants.
    Intent(IntentError),
    /// Any other strategy- or runner-level failure, with a human-readable message.
    Other(String),
}

impl StrategyError {
    /// Builds an [`StrategyError::Other`] from a displayable message.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

impl fmt::Display for StrategyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Intent(e) => write!(f, "{e}"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for StrategyError {}

impl From<IntentError> for StrategyError {
    fn from(e: IntentError) -> Self {
        Self::Intent(e)
    }
}

/// Result type for strategy hooks and runner operations.
pub type Result<T> = std::result::Result<T, StrategyError>;
