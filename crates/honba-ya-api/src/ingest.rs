//! Validation of a `POST /bars` batch (E3-S7 follow-up).
//!
//! Individual bars are validated when they deserialize ([`honba_messages::Bar`] checks its own
//! invariants); this module bounds the batch.

use honba_messages::{ErrorCode, ErrorDetail};
use serde_json::json;

/// Most bars one `POST /bars` request may carry.
pub const MAX_INGEST_BARS: usize = 1_000;

/// Checks a batch of `len` bars: non-empty and at most [`MAX_INGEST_BARS`].
///
/// Both refusals are `validation_invalid_request` with a stable `context.reason`:
/// `bar_batch_empty` or `bar_batch_too_large`.
pub fn check_ingest_batch(len: usize) -> Result<(), ErrorDetail> {
    if len == 0 {
        return Err(ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            "bars must hold at least one bar",
        )
        .with_context(json!({"reason": "bar_batch_empty"})));
    }
    if len > MAX_INGEST_BARS {
        return Err(ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            format!("bars holds {len} bars; at most {MAX_INGEST_BARS} per request"),
        )
        .with_context(json!({
            "reason": "bar_batch_too_large",
            "max_bars": MAX_INGEST_BARS,
            "bars": len,
        })));
    }
    Ok(())
}
