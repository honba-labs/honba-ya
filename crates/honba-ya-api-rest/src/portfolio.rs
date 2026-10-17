//! `/portfolio` routes: the caller's own valued portfolio and its parts.
//!
//! The user is the validated token's `token_id` when auth is on, else
//! [`DEFAULT_USER_ID`](crate::profile_store::DEFAULT_USER_ID); there is no user id in any path, so
//! a token can only ever read its own portfolio (ADR 0032 §1). Each read loads the per-user
//! document from [`crate::PortfolioStore`] on the blocking pool, reads the latest mark of every
//! open instrument from the quote port, and values the two with the pure [`honba_portfolio`]
//! engine, stamping `as_of` from the marks — never a wall clock (ADR 0032 §3, §6).
//!
//! `GET /portfolio/trades` reads no marks: the trade history is stored, not valued.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{PortfolioAccountsResponse, PortfolioHoldingsResponse, PortfolioPositionsResponse};
use honba_entities::PortfolioSnapshot;
use honba_messages::{ErrorCode, ErrorDetail, InstrumentId, UnixNanos};
use honba_portfolio::{
    value, AccountState, HeldPosition, Marks, PortfolioError as ValuationError, PortfolioState,
};
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
            ErrorCode::Unsupported,
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
            ErrorDetail::new(ErrorCode::Forbidden, "this token id cannot own a portfolio")
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

/// Maps a valuation failure onto the standard envelope.
///
/// A non-flat position with no mark is unavailable data, not a bad request: the engine refuses to
/// invent a cost mark, so the read answers 503 `missing_mark` naming the instrument (ADR 0032 §3).
fn valuation_failure(error: ValuationError) -> Response {
    match error {
        ValuationError::MissingMark { instrument_id } => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorDetail::new(
                ErrorCode::MarketDataUnavailable,
                format!("no mark for instrument {instrument_id}"),
            )
            .with_context(json!({
                "reason": "missing_mark",
                "instrument_id": instrument_id.to_string(),
            })),
        ),
        ValuationError::Money(error) => failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("portfolio valuation failed: {error}"),
            )
            .with_context(json!({"reason": "valuation_failed"})),
        ),
        other => {
            error!(error = %other, "portfolio valuation failure");
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorDetail::new(ErrorCode::InternalError, "portfolio valuation failed"),
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

/// The document as the valuation engine's input.
fn to_state(document: &PortfolioDocument) -> PortfolioState {
    let mut state = PortfolioState::new(document.base_currency);
    for account in &document.accounts {
        state.add_account(AccountState {
            id: account.id.clone(),
            name: account.name.clone(),
            mode: account.mode,
            source: account.source,
            broker: account.broker.clone(),
            currency: account.currency,
            cash: account.cash,
            leverage: account.leverage,
            margin_used: account.margin_used,
            margin_available: account.margin_available,
            positions: account
                .positions
                .iter()
                .map(|held| {
                    let mut pos = HeldPosition::new(held.position.clone(), held.product);
                    if let Some(ref group) = held.group_name {
                        pos = pos.with_group(group.clone());
                    }
                    if !held.tags.is_empty() {
                        pos = pos.with_tags(held.tags.clone());
                    }
                    if let Some(ref strat) = held.strategy_id {
                        pos = pos.with_strategy(strat.clone());
                    }
                    pos
                })
                .collect(),
            greeks: account.greeks,
        });
    }
    state
}

/// The latest mark of every non-flat instrument, and the newest mark time as `as_of`.
///
/// A mark is the mid of the latest quote (the same value `GET /quotes` reports; a bar store
/// derives bid = ask = close). A held instrument with no quote is left unmarked: the engine
/// refuses it as [`ValuationError::MissingMark`] rather than valuing it at cost.
async fn marks_for(state: &AppState, document: &PortfolioDocument) -> (Marks, UnixNanos) {
    let mut instruments: BTreeSet<InstrumentId> = BTreeSet::new();
    for account in &document.accounts {
        for held in &account.positions {
            if !held.position.is_flat() {
                instruments.insert(held.position.instrument_id().clone());
            }
        }
    }
    let mut marks = Marks::new();
    let mut as_of = UnixNanos::default();
    for instrument in instruments {
        if let Ok(Some(quote)) = state.quotes.read_quote(&instrument, None).await {
            let mid = (quote.bid_price() + quote.ask_price()) / 2.0;
            let mark = if mid.is_finite() {
                mid
            } else {
                quote.bid_price()
            };
            marks.set(instrument, mark);
            as_of = as_of.max(quote.ts_event());
        }
    }
    (marks, as_of)
}

/// The caller's valued snapshot, or a response describing the failure.
async fn snapshot(state: &AppState, user: &str) -> Result<PortfolioSnapshot, Response> {
    let document = document_for(state, user).await?;
    let (marks, as_of) = marks_for(state, &document).await;
    value(&to_state(&document), &marks, user, document.revision, as_of).map_err(valuation_failure)
}

/// `GET /portfolio`: the caller's valued snapshot, with totals.
pub(crate) async fn get_portfolio(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    match snapshot(&state, &caller.0).await {
        Ok(snapshot) => success(snapshot),
        Err(response) => response,
    }
}

/// `GET /portfolio/accounts`: the valued accounts of the caller's portfolio.
pub(crate) async fn get_accounts(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    match snapshot(&state, &caller.0).await {
        Ok(snapshot) => success(PortfolioAccountsResponse {
            accounts: snapshot.accounts,
        }),
        Err(response) => response,
    }
}

/// `GET /portfolio/positions`: the valued open positions, flattened across accounts.
pub(crate) async fn get_positions(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    match snapshot(&state, &caller.0).await {
        Ok(snapshot) => success(PortfolioPositionsResponse {
            positions: snapshot
                .accounts
                .into_iter()
                .flat_map(|account| account.positions)
                .collect(),
        }),
        Err(response) => response,
    }
}

/// `GET /portfolio/holdings`: the valued delivery holdings, flattened across accounts.
pub(crate) async fn get_holdings(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    match snapshot(&state, &caller.0).await {
        Ok(snapshot) => success(PortfolioHoldingsResponse {
            holdings: snapshot
                .accounts
                .into_iter()
                .flat_map(|account| account.holdings)
                .collect(),
        }),
        Err(response) => response,
    }
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
