//! Unit tests for `ScreenerQuery::resolve` and the per-instrument evaluation.

use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, ErrorCode, ErrorDetail, Exchange, InstrumentId,
    PriceType, UnixNanos,
};
use serde_json::{json, Value};

use crate::{
    check_scan_budget, ScreenerQuery, MAX_SCREENER_BARS, MAX_SCREENER_ROWS, MAX_SCREENER_UNIVERSE,
};

fn query(filters: Option<Value>, universe: Option<Value>) -> ScreenerQuery {
    ScreenerQuery {
        filters: filters.map(|v| v.to_string()),
        universe: universe.map(|v| v.to_string()),
        tf: None,
        as_of: None,
    }
}

fn reason(detail: &ErrorDetail) -> (&str, &str) {
    let ctx = detail.context.as_ref().expect("context");
    (
        ctx["field"].as_str().expect("field"),
        ctx["reason"].as_str().expect("reason"),
    )
}

fn bars(closes: &[f64]) -> Vec<Bar> {
    let bt = BarType::new(
        InstrumentId::new("TCS", Exchange::new("NSE")),
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last),
    );
    closes
        .iter()
        .enumerate()
        .map(|(i, &c)| {
            let t = UnixNanos::from_u64(i as u64 + 1);
            Bar::new(bt.clone(), c, c + 1.0, c - 1.0, c, 10.0, t, t)
        })
        .collect()
}

fn gt(key: &str, value: f64) -> Value {
    json!({"operator": "AND", "items": [{"key": key, "op": "gt", "value": value}]})
}

#[test]
fn a_universe_is_required_and_must_be_a_json_array_of_ids() {
    for universe in [
        None,
        Some(json!([])),
        Some(json!("TCS.NSE")),
        Some(json!([1])),
    ] {
        let err = query(None, universe).resolve().unwrap_err();
        assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
        assert_eq!(reason(&err).0, "universe");
    }
    let mut not_json = query(None, Some(json!(["TCS.NSE"])));
    not_json.universe = Some("[TCS".into());
    assert_eq!(
        reason(&not_json.resolve().unwrap_err()).1,
        "invalid_universe"
    );
    let err = query(None, Some(json!(["TCS"]))).resolve().unwrap_err();
    assert_eq!(reason(&err), ("universe", "invalid_instrument_id"));
}

#[test]
fn the_universe_is_sorted_and_deduplicated() {
    let resolved = query(None, Some(json!(["TCS.NSE", "INFY.NSE", "TCS.NSE"])))
        .resolve()
        .unwrap();
    let ids: Vec<String> = resolved.universe.iter().map(ToString::to_string).collect();
    assert_eq!(ids, ["INFY.NSE", "TCS.NSE"]);
}

#[test]
fn an_oversized_universe_is_too_many_rows() {
    let ids: Vec<String> = (0..=MAX_SCREENER_UNIVERSE)
        .map(|i| format!("S{i}.NSE"))
        .collect();
    let err = query(None, Some(json!(ids))).resolve().unwrap_err();
    assert_eq!(reason(&err), ("universe", "too_many_rows"));
    assert_eq!(err.context.unwrap()["limit"], MAX_SCREENER_UNIVERSE);
}

#[test]
fn defaults_are_daily_bars_and_no_filter() {
    let resolved = query(None, Some(json!(["TCS.NSE"]))).resolve().unwrap();
    assert_eq!(
        resolved.spec,
        BarSpecification::new(1, BarAggregation::Day, PriceType::Last)
    );
    assert_eq!(resolved.to, None);
    assert!(resolved.metric_keys.is_empty());
}

#[test]
fn as_of_is_an_inclusive_upper_bound() {
    let mut q = query(None, Some(json!(["TCS.NSE"])));
    q.as_of = Some("1970-01-02".into());
    let resolved = q.resolve().unwrap();
    assert_eq!(
        resolved.to,
        Some(UnixNanos::from_u64(86_400_000_000_000 + 1))
    );
    q.as_of = Some("yesterday".into());
    assert_eq!(reason(&q.resolve().unwrap_err()), ("as_of", "invalid_time"));
}

#[test]
fn a_bad_timeframe_is_named() {
    let mut q = query(None, Some(json!(["TCS.NSE"])));
    q.tf = Some("1x".into());
    assert_eq!(reason(&q.resolve().unwrap_err()).0, "tf");
}

#[test]
fn malformed_filters_are_invalid_filters() {
    let mut q = query(None, Some(json!(["TCS.NSE"])));
    q.filters = Some("{not json".into());
    assert_eq!(
        reason(&q.resolve().unwrap_err()),
        ("filters", "invalid_filters")
    );
    let q = query(
        Some(json!({"operator": "XOR", "items": []})),
        Some(json!(["TCS.NSE"])),
    );
    assert_eq!(
        reason(&q.resolve().unwrap_err()),
        ("filters", "invalid_filter")
    );
    let q = query(
        Some(json!({"operator": "AND", "items": [{"key": "close", "op": "between", "value": 1}]})),
        Some(json!(["TCS.NSE"])),
    );
    assert_eq!(
        reason(&q.resolve().unwrap_err()),
        ("filters", "invalid_filter")
    );
}

#[test]
fn unsupported_metrics_are_rejected_at_resolve_before_any_bar_is_read() {
    let q = query(
        Some(gt("price_earnings_ttm", 1.0)),
        Some(json!(["TCS.NSE"])),
    );
    let err = q.resolve().unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(reason(&err), ("filters", "unsupported_metric"));
}

#[test]
fn a_timeframe_dimension_must_match_the_scan_timeframe() {
    let filter = |tf: &str| {
        json!({"operator": "AND",
               "items": [{"key": "close", "op": "gt", "value": 1, "timeframe": tf}]})
    };
    let ok = query(Some(filter("1D")), Some(json!(["TCS.NSE"])));
    assert!(ok.resolve().is_ok());
    let bad = query(Some(filter("1W")), Some(json!(["TCS.NSE"])));
    assert_eq!(
        reason(&bad.resolve().unwrap_err()),
        ("filters", "unsupported_timeframe")
    );
    let mut hourly = query(Some(filter("60")), Some(json!(["TCS.NSE"])));
    hourly.tf = Some("1h".into());
    assert!(hourly.resolve().is_ok());
    let mut weekly = query(Some(filter("1W")), Some(json!(["TCS.NSE"])));
    weekly.tf = Some("1w".into());
    assert!(weekly.resolve().is_ok());
    let mut monthly = query(Some(filter("1M")), Some(json!(["TCS.NSE"])));
    monthly.tf = Some("1mo".into());
    assert!(monthly.resolve().is_ok());
}

#[test]
fn a_matching_instrument_yields_a_row_with_the_filter_metrics() {
    let q = query(
        Some(json!({"operator": "AND", "items": [
            {"key": "close", "op": "gt", "value": 100},
            {"key": "SMA3", "op": "lt", "value": {"key": "close"}}]})),
        Some(json!(["TCS.NSE"])),
    );
    let resolved = q.resolve().unwrap();
    assert_eq!(resolved.metric_keys, ["close", "SMA3"]);
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    let row = resolved
        .evaluate(&id, &bars(&[101.0, 102.0, 106.0]))
        .unwrap()
        .expect("matches");
    assert_eq!(row.instrument_id, id);
    assert_eq!(row.metrics["close"], Some(106.0));
    assert_eq!(row.metrics["SMA3"], Some(103.0));
}

#[test]
fn a_non_matching_instrument_yields_no_row() {
    let resolved = query(Some(gt("close", 500.0)), Some(json!(["TCS.NSE"])))
        .resolve()
        .unwrap();
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    assert_eq!(resolved.evaluate(&id, &bars(&[101.0])).unwrap(), None);
}

#[test]
fn a_warming_up_metric_is_null_in_the_row_of_an_or_match() {
    let resolved = query(
        Some(json!({"operator": "OR", "items": [
            {"key": "close", "op": "gt", "value": 1},
            {"key": "SMA50", "op": "gt", "value": 1}]})),
        Some(json!(["TCS.NSE"])),
    )
    .resolve()
    .unwrap();
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    let row = resolved.evaluate(&id, &bars(&[10.0])).unwrap().unwrap();
    assert_eq!(row.metrics["SMA50"], None);
}

#[test]
fn no_filter_matches_every_instrument() {
    let resolved = query(None, Some(json!(["TCS.NSE"]))).resolve().unwrap();
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    let row = resolved.evaluate(&id, &[]).unwrap().unwrap();
    assert!(row.metrics.is_empty());
}

#[test]
fn the_scan_budget_and_row_cap_are_422_too_many_rows() {
    assert!(check_scan_budget(MAX_SCREENER_BARS).is_ok());
    let err = check_scan_budget(MAX_SCREENER_BARS + 1).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(reason(&err).1, "too_many_rows");
    assert!(crate::check_screener_rows(MAX_SCREENER_ROWS).is_ok());
    let err = crate::check_screener_rows(MAX_SCREENER_ROWS + 1).unwrap_err();
    assert_eq!(reason(&err), ("universe", "too_many_rows"));
}

#[test]
fn the_query_rejects_unknown_fields() {
    let raw = json!({"universe": "[]", "univers": "x"});
    assert!(serde_json::from_value::<ScreenerQuery>(raw).is_err());
}
