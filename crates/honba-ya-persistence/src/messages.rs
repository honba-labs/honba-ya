//! Local mirror of the actor message contracts that the persistence layer
//! consumes.
//!
//! [`RiskBreachEvent`] mirrors `honba-ya-actors`'
//! `messages::RiskBreachEvent` field-for-field so the audit ledger can record
//! structured pre-trade risk refusals without depending on the actor runtime
//! crate (which depends on this crate's audit ledger).

use chrono::{DateTime, Utc};
use honba_core::{InstrumentId, Price, Quantity};

/// Structured risk breach event emitted for persistence audit stream.
///
/// Emitted synchronously when `RiskEngine` refuses an order, providing an
/// immutable record for SEBI regulatory audit and post-trade analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskBreachEvent {
    /// Timestamp of the breach.
    pub ts: DateTime<Utc>,
    /// Algo ID associated with the breach.
    pub algo_id: String,
    /// Instrument being traded.
    pub instrument: InstrumentId,
    /// Refusal reason from the risk engine.
    pub refusal_reason: String,
    /// Regulatory rule code (e.g. `freeze_limit_exceeded`, `price_band_violation`).
    pub rule_code: &'static str,
    /// Evaluation latency in microseconds.
    pub eval_duration_us: u64,
    /// Requested order quantity.
    pub requested_qty: Option<Quantity>,
    /// Requested order price limit.
    pub requested_price: Option<Price>,
}
