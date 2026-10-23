//! `/portfolio` routes: the caller's own valued portfolio and its parts.
//!
//! The user is the validated token's `token_id` when auth is on, else
//! [`DEFAULT_USER_ID`](crate::profile_store::DEFAULT_USER_ID); there is no user id in any path, so
//! a token can only ever read its own portfolio (ADR 0032 §1). `GET /portfolio/trades` reads the
//! stored trade history directly and needs no valuation.
//!
//! # Functionality gap: `GET /portfolio`, `/accounts`, `/positions`, `/holdings`
//!
//! These four used to call `honba_portfolio::value(state, marks, user, revision, as_of)` and
//! project its result into [`honba_core::PortfolioSnapshot`] (an `accounts: Vec<PortfolioAccount>`
//! shape, each account carrying its own valued `positions`/`holdings`/`totals`/`greeks`). That
//! function no longer exists with that signature or that output shape. The real
//! `honba_portfolio::value` (in `honba-kokoro/crates/honba-kokoro-portfolio/src/lib.rs`, a sibling
//! repo out of scope for this migration — see `AGENTS.md`/this task's scope limits) is
//! `fn value(state: &PortfolioState, marks: &Marks, as_of: &chrono::Utc, cash_flow: i64) ->
//! Result<PortfolioSnapshot, anyhow::Error>` where *that* `PortfolioSnapshot` is a flat
//! `{totals: Money, breakdown: BTreeMap<String, Money>, unrealized_pnl: Money, realized_pnl:
//! Money}` — no per-account breakdown, no positions/holdings list, no greeks. There is no
//! `PortfolioError`, `AccountState`, or `HeldPosition` matching the old call site's shape either
//! (`HeldPosition`/`AccountState` do exist in that crate, but with different field sets than this
//! file's old `to_state()` assumed, and `value()`'s errors are plain `anyhow::Error`, not a typed
//! enum with `MissingMark`/`Money` variants to branch on).
//!
//! Porting these four routes for real means re-deriving a per-account/per-position valued
//! snapshot (with marks, greeks, holdings) from that flat engine output — a real feature
//! redesign, not a rename — so they now answer `501 not_implemented` with a context explaining
//! why, rather than silently returning wrong or empty data. `GET /portfolio/trades` is unaffected
//! (it never called the valuation engine) and still works.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{ErrorCode, ErrorDetail};
use serde_json::json;
use tracing::error;

use crate::caller::Caller;
use crate::market::{failure, success};
use crate::{AppState, PortfolioDocument, PortfolioError};

/// The 503 for a `honba serve` with no portfolios directory configured.
fn no_store() -> Response {
    failure(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorDetail::new(
            ErrorCode::NotImplemented,
            "no portfolios directory is configured",
        )
        .with_context(json!({"reason": "no_portfolios_dir"})),
    )
}

/// Maps a store failure onto the standard envelope, mirroring `profile_failure`.
fn store_failure(error: PortfolioError) -> Response {
    match error {
        // The id comes from a token record, never from the request: refuse, don't build a path.
        PortfolioError::InvalidUserId => failure(
            StatusCode::FORBIDDEN,
            ErrorDetail::new(ErrorCode::Unauthorized, "this token id cannot own a portfolio")
                .with_context(json!({"reason": "invalid_user_id"})),
        ),
        PortfolioError::RevisionConflict { expected, current } => failure(
            StatusCode::CONFLICT,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("revision conflict: expected {expected}, current {current}"),
            )
            .with_context(json!({
                "reason": "revision_conflict",
                "expected": expected,
                "current": current,
            })),
        ),
        PortfolioError::TooLarge { what } => failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("portfolio too large: {what}"),
            )
            .with_context(json!({"reason": "portfolio_too_large"})),
        ),
        PortfolioError::Io(_) | PortfolioError::Json(_) => {
            error!(error = %error, "portfolio store failure");
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorDetail::new(ErrorCode::InternalError, "portfolio store failure"),
            )
        }
    }
}

/// Loads the caller's portfolio document, or a response describing the failure.
async fn document_for(state: &AppState, user: &str) -> Result<PortfolioDocument, Response> {
    let Some(store) = state.portfolios.clone() else {
        return Err(no_store());
    };
    let user = user.to_owned();
    match tokio::task::spawn_blocking(move || store.get(&user)).await {
        Ok(Ok(document)) => Ok(document),
        Ok(Err(error)) => Err(store_failure(error)),
        Err(_) => Err(failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorDetail::new(ErrorCode::InternalError, "portfolio task failed"),
        )),
    }
}

/// The 501 for the valuation gap documented in the module doc above.
fn valuation_not_implemented() -> Response {
    failure(
        StatusCode::NOT_IMPLEMENTED,
        ErrorDetail::new(
            ErrorCode::NotImplemented,
            "portfolio valuation is unavailable: honba_portfolio::value()'s shape no longer \
             matches what this route needs (see portfolio.rs's module doc)",
        )
        .with_context(json!({"reason": "valuation_engine_mismatch"})),
    )
}

/// `GET /portfolio`: the caller's valued snapshot, with totals.
///
/// See the module doc: unavailable until the valuation engine mismatch is resolved.
pub(crate) async fn get_portfolio(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    // Still validates that the caller has a readable document, so a storage/auth failure is
    // reported as that failure rather than masked by the blanket 501 below.
    if let Err(response) = document_for(&state, &caller.0).await {
        return response;
    }
    valuation_not_implemented()
}

/// `GET /portfolio/accounts`: the valued accounts of the caller's portfolio.
///
/// See the module doc: unavailable until the valuation engine mismatch is resolved.
pub(crate) async fn get_accounts(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    if let Err(response) = document_for(&state, &caller.0).await {
        return response;
    }
    valuation_not_implemented()
}

/// `GET /portfolio/positions`: the valued open positions, flattened across accounts.
///
/// See the module doc: unavailable until the valuation engine mismatch is resolved.
pub(crate) async fn get_positions(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    if let Err(response) = document_for(&state, &caller.0).await {
        return response;
    }
    valuation_not_implemented()
}

/// `GET /portfolio/holdings`: the valued delivery holdings, flattened across accounts.
///
/// See the module doc: unavailable until the valuation engine mismatch is resolved.
pub(crate) async fn get_holdings(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    if let Err(response) = document_for(&state, &caller.0).await {
        return response;
    }
    valuation_not_implemented()
}

/// `GET /portfolio/trades`: the caller's trade history, in stored order.
pub(crate) async fn get_trades(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    match document_for(&state, &caller.0).await {
        Ok(document) => success(honba_api::TradesResponse {
            trades: document.trades,
        }),
        Err(response) => response,
    }
}
