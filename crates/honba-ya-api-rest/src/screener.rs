//! Handler for `GET /screener/scan`.
//!
//! The query is resolved by `honba-api` (bounded universe, validated filter); this handler only
//! reads each instrument's bars through the ports and hands them to the pure evaluator, one
//! instrument at a time so memory stays bounded by the largest single history.

use std::sync::Arc;

use axum::{extract::State, response::Response};
use honba_api::wire::{BarAggregation, BarSpecification, PriceType};
use honba_api::{
    ScreenerQuery, ScreenerResponse, ScreenerResultRow, check_scan_budget, check_screener_rows,
};
use honba_core::rust_decimal::Decimal;
use honba_core::{Bar, Price, Quantity};
use honba_feed::ports::BarRequest;
use honba_feed::wire::UnixNanos;

use crate::AppState;
use crate::market::{
    ApiQuery, MAX_BAR_ROWS, check_row_cap, instrument_not_found, port_failure, success,
    unprocessable,
};

/// Converts the REST contract's bar specification (`honba_api::wire`) to `honba-me-feed`'s own
/// `BarSpecification`, at the boundary where this handler reads bars through the feed port.
/// Mirrors `catalog::to_feed_spec` (private to that module, so duplicated here rather than
/// reaching into a module this file doesn't own).
fn to_feed_spec(spec: BarSpecification) -> honba_feed::wire::BarSpecification {
    let aggregation = match spec.aggregation() {
        BarAggregation::Second => honba_feed::wire::BarAggregation::Second,
        BarAggregation::Minute => honba_feed::wire::BarAggregation::Minute,
        BarAggregation::Hour => honba_feed::wire::BarAggregation::Hour,
        BarAggregation::Day => honba_feed::wire::BarAggregation::Day,
        BarAggregation::Week => honba_feed::wire::BarAggregation::Week,
        BarAggregation::Month => honba_feed::wire::BarAggregation::Month,
    };
    let price_type = match spec.price_type() {
        PriceType::Last => honba_feed::wire::PriceType::Last,
        PriceType::Bid => honba_feed::wire::PriceType::Bid,
        PriceType::Ask => honba_feed::wire::PriceType::Ask,
        PriceType::Open | PriceType::High | PriceType::Low => honba_feed::wire::PriceType::Last,
    };
    let step = u32::try_from(spec.step()).unwrap_or(u32::MAX);
    honba_feed::wire::BarSpecification::new(step, aggregation, price_type)
}

/// Converts an `f64`-valued feed bar (as read off the Parquet catalog) to the exact
/// `Decimal`-valued [`Bar`] the pure screener evaluator (`honba_api::screener`) is written
/// against.
fn to_core_bar(bar: &honba_feed::wire::Bar) -> Bar {
    let to_decimal = |v: f64| Decimal::from_f64_retain(v).unwrap_or_default();
    Bar {
        instrument: bar.bar_type().instrument_id().clone(),
        open: Price::new(to_decimal(bar.open())),
        high: Price::new(to_decimal(bar.high())),
        low: Price::new(to_decimal(bar.low())),
        close: Price::new(to_decimal(bar.close())),
        volume: Quantity::new(to_decimal(bar.volume())),
        timestamp: bar.ts_event().to_datetime(),
    }
}

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
        let request = match BarRequest::new(
            id.clone(),
            to_feed_spec(resolved.spec),
            None,
            resolved.to.map(UnixNanos::from_datetime),
        ) {
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
        let core_bars: Vec<Bar> = bars.iter().map(to_core_bar).collect();
        match resolved.evaluate(id, &core_bars) {
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
