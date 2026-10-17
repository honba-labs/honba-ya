//! `GET` / `PUT /trading-state`: read and change the operator trading state at runtime
//! (ADR 0018 decision 7). The state lives in the shared [`AppState`] cell, so the order gate and
//! `POST /positions/close` see a change on the next request. Every transition is allowed; a
//! `PUT` of the current state is a no-op that answers `previous == state`.
//!
//! A change is logged through `tracing` and, when [`AppState::with_operator_journal`] set a
//! path, appended to that NDJSON journal ([`crate::operator_journal`]); the auth audit log
//! records who called which route, not bodies, so the reason and the caller's identity go
//! there. A no-op is not journaled.

use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use axum::Extension;
use honba_api::{TradingStateChange, TradingStateRequest, TradingStateResponse};
use tracing::info;

use crate::auth::TokenInfo;
use crate::market::success;
use crate::{ApiJson, AppState};

/// `GET /trading-state`: the state in force.
pub(crate) async fn get_trading_state(State(state): State<Arc<AppState>>) -> Response {
    success(TradingStateResponse {
        state: state.trading_state(),
    })
}

/// `PUT /trading-state`: set the state and report the one it replaced.
pub(crate) async fn put_trading_state(
    State(state): State<Arc<AppState>>,
    token: Option<Extension<TokenInfo>>,
    ApiJson(request): ApiJson<TradingStateRequest>,
) -> Response {
    let previous = state.set_trading_state(request.state);
    let reason = request.reason.as_deref().unwrap_or("");
    let changed = previous != request.state;
    if changed {
        info!(from = ?previous, to = ?request.state, reason, "trading state changed");
        if let Some(path) = &state.operator_journal {
            let token_id = token.as_ref().map(|t| t.token_id.as_str());
            crate::operator_journal::record_change(
                path,
                previous,
                request.state,
                request.reason.as_deref(),
                token_id,
            );
        }
    } else {
        info!(state = ?request.state, reason, "trading state unchanged");
    }
    success(TradingStateChange {
        state: request.state,
        previous,
    })
}
