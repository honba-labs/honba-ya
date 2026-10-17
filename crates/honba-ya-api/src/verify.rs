//! `POST /strategies/verify`: a strategy manifest in, the verified IR out.
//!
//! Verification is pure and idempotent (it compiles, it does not trade), so the
//! endpoint is read-only and not gated by the approval queue. This module is the
//! anti-corruption step between `honba-strategy`'s [`IrError`] and the API error
//! taxonomy, so every transport reports a failed verification the same way.

use honba_messages::{ErrorCode, ErrorDetail};
use honba_strategy::{IrError, StrategyIr, StrategyManifest};
use serde_json::json;

/// Request body of `POST /strategies/verify`: the manifest to verify.
///
/// The manifest is an *input* (ADR 0012), so unknown fields are rejected.
pub type VerifyStrategyRequest = StrategyManifest;

/// Response payload of `POST /strategies/verify`: the verified IR, a record.
pub type VerifyStrategyResponse = StrategyIr;

/// Verifies `manifest` and compiles it to its [`StrategyIr`].
///
/// A failure is a [`ErrorCode::ValidationInvalidRequest`] whose `context.reason`
/// is the stable [`IrError::code`] (e.g. `no_subscriptions`), so a caller can
/// react without parsing the message.
pub fn verify_strategy(manifest: VerifyStrategyRequest) -> Result<StrategyIr, ErrorDetail> {
    StrategyIr::compile(manifest).map_err(|e| to_error_detail(&e))
}

fn to_error_detail(e: &IrError) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, e.to_string())
        .with_context(json!({"reason": e.code()}))
}
