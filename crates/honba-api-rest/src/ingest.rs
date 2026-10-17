//! `POST /bars`: bars over HTTP into the bar pump (E3-S7 follow-up).
//!
//! The handler validates the batch, then reserves room for all of it in the bounded ingest
//! channel before sending anything, so a batch is accepted whole or not at all. It answers
//! once the bars are queued, not once they are applied: ordering is enforced downstream by
//! `advance_market`, where an out-of-order bar is counted by the pump and skipped.
//!
//! Failures carry a stable `context.reason`:
//!
//! | status | code | reason |
//! |---|---|---|
//! | 422 | `validation_invalid_request` | `bar_batch_empty`, `bar_batch_too_large`, `bar_batch_exceeds_capacity` |
//! | 429 | `rate_limited` | `bar_ingest_full` (the pump has not drained yet; retry) |
//! | 503 | `market_data_unavailable` | `bar_ingest_disabled` (no pump running, or it was stopped) |

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{BarsIngestRequest, BarsIngestResponse};
use honba_messages::{ErrorCode, ErrorDetail};
use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;

use crate::market::{failure, success};
use crate::{ApiJson, AppState};

pub(crate) async fn post_bars(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<BarsIngestRequest>,
) -> Response {
    let bars = request.bars;
    if let Err(detail) = honba_api::check_ingest_batch(bars.len()) {
        return failure(StatusCode::UNPROCESSABLE_ENTITY, detail);
    }
    let Some(sender) = state.bar_ingest_sender() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorDetail::new(
                ErrorCode::MarketDataUnavailable,
                "bar ingest is not running on this server",
            )
            .with_context(json!({"reason": "bar_ingest_disabled"})),
        );
    };
    if bars.len() > sender.max_capacity() {
        return failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!(
                    "{} bars can never fit the ingest queue of {}; send smaller batches",
                    bars.len(),
                    sender.max_capacity()
                ),
            )
            .with_context(json!({
                "reason": "bar_batch_exceeds_capacity",
                "capacity": sender.max_capacity(),
            })),
        );
    }
    let accepted = bars.len() as u64;
    let permits = match sender.try_reserve_many(bars.len()) {
        Ok(permits) => permits,
        Err(TrySendError::Full(())) => {
            return failure(
                StatusCode::TOO_MANY_REQUESTS,
                ErrorDetail::new(ErrorCode::RateLimited, "bar ingest queue is full; retry")
                    .with_context(json!({
                        "reason": "bar_ingest_full",
                        "capacity": sender.max_capacity(),
                    })),
            );
        }
        // The pump went away between the lookup and the send.
        Err(TrySendError::Closed(())) => {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorDetail::new(ErrorCode::MarketDataUnavailable, "bar ingest has stopped")
                    .with_context(json!({"reason": "bar_ingest_disabled"})),
            );
        }
    };
    for (permit, bar) in permits.zip(bars) {
        permit.send(bar);
    }
    success(BarsIngestResponse { accepted })
}
