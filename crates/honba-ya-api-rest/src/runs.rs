//! The run routes (ADR 0017 decision 7): `POST /backtests`, `GET /backtests/{id}`, the two
//! journal routes. Every call into the blocking run service or store goes through
//! `spawn_blocking`; an id is validated by the store (`RunId::parse`) before any registry or
//! filesystem access.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{BacktestRequest, RunKind, SweepRequest, TradesResponse};
use honba_messages::{ErrorCode, ErrorDetail};
use serde_json::json;

use crate::market::{failure, success};
use crate::registry::StrategyRegistry;
use crate::service::resolve_strategy;
use crate::trades::trades_from_journal;
use crate::{ApiJson, AppState};

/// The HTTP status of a run-route failure.
fn status_of(detail: &ErrorDetail) -> StatusCode {
    match detail.code {
        ErrorCode::ValidationInvalidRequest | ErrorCode::Unsupported => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn respond<T: serde::Serialize>(result: Result<T, ErrorDetail>) -> Response {
    match result {
        Ok(data) => success(data),
        Err(detail) => failure(status_of(&detail), detail),
    }
}

fn not_found() -> ErrorDetail {
    ErrorDetail::new(ErrorCode::NotFound, "run not found")
}

/// Runs blocking work off the async runtime; a panicking task is an `internal_error`.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ErrorDetail> + Send + 'static,
) -> Result<T, ErrorDetail> {
    tokio::task::spawn_blocking(work).await.unwrap_or_else(|_| {
        Err(
            ErrorDetail::new(ErrorCode::InternalError, "run task failed")
                .with_context(json!({"reason": "panic"})),
        )
    })
}

/// `POST /backtests`.
pub(crate) async fn post_backtests(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<BacktestRequest>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        // Nothing can run, but a bad request is still a 422 first.
        let checked = request.resolve().and_then(|r| {
            resolve_strategy(
                &state.strategies,
                &StrategyRegistry::builtin(),
                &r.strategy,
                &r.universe,
                &r.bar_spec,
            )
        });
        return match checked {
            Err(detail) => failure(status_of(&detail), detail),
            Ok(_) => failure(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorDetail::new(
                    ErrorCode::Unsupported,
                    "no journals directory is configured",
                )
                .with_context(json!({"reason": "no_journals_dir"})),
            ),
        };
    };
    respond(blocking(move || runs.submit_backtest(&request)).await)
}

/// `GET /backtests/{id}`.
pub(crate) async fn get_backtest_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        return respond::<()>(Err(not_found()));
    };
    respond(
        blocking(move || {
            runs.store()
                .load(&id, RunKind::Backtest)?
                .to_backtest_response()
                .ok_or_else(not_found)
        })
        .await,
    )
}

/// `GET /backtests/{id}/journal`.
pub(crate) async fn get_backtest_journal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    journal(state, id, Some(RunKind::Backtest)).await
}

/// `GET /journals/{id}`.
pub(crate) async fn get_journal_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    journal(state, id, None).await
}

async fn journal(state: Arc<AppState>, id: String, kind: Option<RunKind>) -> Response {
    let Some(runs) = state.runs.clone() else {
        return respond::<()>(Err(not_found()));
    };
    let currency = state.account_currency;
    respond(
        blocking(move || {
            let store = runs.store();
            let manifest = match kind {
                Some(kind) => store.load(&id, kind)?,
                None => store.load_any(&id)?,
            };
            if manifest.kind == RunKind::Sweep {
                return Err(ErrorDetail::new(
                    ErrorCode::ValidationInvalidRequest,
                    "a sweep has one journal per trial, not one for the run",
                )
                .with_context(json!({"reason": "sweep_journal_per_trial"})));
            }
            let records = store.read_journal(&id, manifest.kind)?;
            trades_from_journal(&records, currency).map(|trades| TradesResponse { trades })
        })
        .await,
    )
}

/// `GET /sweeps/{id}/journal` - returns 422 as sweeps have per-trial journals.
pub(crate) async fn get_sweep_journal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        return respond::<()>(Err(not_found()));
    };
    let _currency = state.account_currency;
    respond::<()>(
        blocking(move || {
            let store = runs.store();
            let _manifest = store.load(&id, RunKind::Sweep)?;
            // Sweeps don't have a single journal - they have per-trial journals
            Err(ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "a sweep has one journal per trial, not one for the run",
            )
            .with_context(json!({"reason": "sweep_journal_per_trial"})))
        })
        .await,
    )
}

/// `POST /sweeps`.
pub(crate) async fn post_sweeps(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<SweepRequest>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        // Nothing can run, but a bad request is still a 422 first.
        let checked = request.resolve().and_then(|r| {
            resolve_strategy(
                &state.strategies,
                &StrategyRegistry::builtin(),
                &r.strategy,
                &r.universe,
                &r.bar_spec,
            )
        });
        return match checked {
            Err(detail) => failure(status_of(&detail), detail),
            Ok(_) => failure(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorDetail::new(
                    ErrorCode::Unsupported,
                    "no journals directory is configured",
                )
                .with_context(json!({"reason": "no_journals_dir"})),
            ),
        };
    };
    respond(blocking(move || runs.submit_sweep(&request)).await)
}

/// `GET /sweeps/{id}`.
pub(crate) async fn get_sweep_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        return respond::<()>(Err(not_found()));
    };
    respond(
        blocking(move || {
            runs.store()
                .load(&id, RunKind::Sweep)?
                .to_sweep_response()
                .ok_or_else(not_found)
        })
        .await,
    )
}
