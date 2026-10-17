//! Handler for `GET /screener/scan`.
//!
//! The query is resolved by `honba-api` (bounded universe, validated filter); this handler only
//! reads each instrument's bars through the ports and hands them to the pure evaluator, one
//! instrument at a time so memory stays bounded by the largest single history.

use std::sync::Arc;

use axum::{extract::State, response::Response};
use honba_api::{
    check_scan_budget, check_screener_rows, ScreenerQuery, ScreenerResponse, ScreenerResultRow,
};
use honba_ports::BarRequest;

use crate::market::{
    check_row_cap, instrument_not_found, port_failure, success, unprocessable, ApiQuery,
    MAX_BAR_ROWS,
};
use crate::AppState;

/// `GET /screener/scan`: the universe instruments passing the filter, in instrument-id order.
///
/// An unknown instrument is a 404 and a metric a bar dataset cannot compute is a 422
/// (`reason: unsupported_metric`): a partial or empty answer is never passed off as a result.
/// Over the row or bar limits the answer is a 422 `too_many_rows`; there is no pagination.
pub(crate) async fn get_screener_scan(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ScreenerQuery>,
) -> Response {
    let resolved = match query.resolve() {
        Ok(resolved) => resolved,
        Err(detail) => return unprocessable(detail),
    };
    let mut rows: Vec<ScreenerResultRow> = Vec::new();
    let mut bars_read = 0usize;
    for id in &resolved.universe {
        match state.instruments.get_instrument(id).await {
            Ok(Some(_)) => {}
            Ok(None) => return instrument_not_found(id),
            Err(error) => return port_failure(error),
        }
        let request = match BarRequest::new(id.clone(), resolved.spec, None, resolved.to) {
            Ok(request) => request,
            Err(error) => return port_failure(error),
        };
        let bars = match state.bars.read_bars(&request).await {
            Ok(bars) => bars,
            Err(error) => return port_failure(error),
        };
        if let Err(detail) = check_row_cap(bars.len(), MAX_BAR_ROWS) {
            return unprocessable(detail);
        }
        bars_read = bars_read.saturating_add(bars.len());
        if let Err(detail) = check_scan_budget(bars_read) {
            return unprocessable(detail);
        }
        match resolved.evaluate(id, &bars) {
            Ok(Some(row)) => rows.push(row),
            Ok(None) => {}
            Err(detail) => return unprocessable(detail),
        }
        if let Err(detail) = check_screener_rows(rows.len()) {
            return unprocessable(detail);
        }
    }
    success(ScreenerResponse { rows })
}
