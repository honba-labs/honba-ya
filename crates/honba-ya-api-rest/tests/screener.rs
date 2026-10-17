//! `GET /screener/scan` through the real router, over an in-memory daily-bar dataset.
//! No network, no wall clock.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, build_target, AppState};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{
    BarAggregation, BarSpecification, Exchange, InstrumentId, PriceType, UnixNanos,
};
use serde_json::{json, Value};
use tower::util::ServiceExt;

const DAY: u64 = 86_400_000_000_000;
/// 2024-01-01T00:00:00Z.
const T0: u64 = 1_704_067_200_000_000_000;

fn daily(symbol: &str, closes: &[f64]) -> honba_feed::ColumnarSlice {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let mut builder =
        ColumnarSliceBuilder::new(InstrumentId::new(symbol, Exchange::new("NSE")), spec);
    for (i, &c) in closes.iter().enumerate() {
        builder
            .push(
                UnixNanos::from_u64(T0 + i as u64 * DAY),
                c,
                c + 1.0,
                c - 1.0,
                c,
                1000.0,
            )
            .unwrap();
    }
    builder.finish().unwrap()
}

fn rising() -> Vec<f64> {
    (0..20).map(|i| 100.0 + f64::from(i)).collect()
}

fn falling() -> Vec<f64> {
    (0..20).map(|i| 200.0 - f64::from(i)).collect()
}

fn app_with(slices: Vec<honba_feed::ColumnarSlice>) -> Router {
    let dataset = Dataset::from_slices(slices).unwrap();
    api_router_with(AppState::from_reader(DatasetReader::from_dataset(dataset)))
}

fn app() -> Router {
    // Out of id order on purpose.
    app_with(vec![
        daily("TCS", &rising()),
        daily("INFY", &falling()),
        daily("WIPRO", &rising()),
    ])
}

async fn scan(app: Router, query: Value) -> (StatusCode, Value) {
    let target = build_target("/screener/scan", Some(&query.to_string())).unwrap();
    let res = app
        .oneshot(Request::builder().uri(target).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn q(filters: Value, universe: Value) -> Value {
    json!({"filters": filters.to_string(), "universe": universe.to_string()})
}

fn row_ids(body: &Value) -> Vec<String> {
    body["data"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            format!(
                "{}.{}",
                r["instrument_id"]["symbol"].as_str().unwrap(),
                r["instrument_id"]["exchange"].as_str().unwrap()
            )
        })
        .collect()
}

fn gt(key: &str, value: f64) -> Value {
    json!({"operator": "AND", "items": [{"key": key, "op": "gt", "value": value}]})
}

#[tokio::test]
async fn matching_instruments_come_back_in_id_order_with_their_metrics() {
    let universe = json!(["WIPRO.NSE", "TCS.NSE", "INFY.NSE"]);
    let (status, body) = scan(app(), q(gt("close", 150.0), universe)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["error"].is_null() && body["api_version"].is_string());
    // INFY ends at 181 (> 150); TCS and WIPRO end at 119.
    assert_eq!(row_ids(&body), ["INFY.NSE"]);
    assert_eq!(body["data"]["rows"][0]["metrics"], json!({"close": 181.0}));
}

#[tokio::test]
async fn rows_are_in_instrument_id_order_whatever_the_request_order() {
    let filters = json!({"operator": "AND", "items": [
        {"key": "SMA5", "op": "lt", "value": {"key": "close"}}]});
    let universe = json!(["WIPRO.NSE", "TCS.NSE"]);
    let (status, body) = scan(app(), q(filters, universe)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_ids(&body), ["TCS.NSE", "WIPRO.NSE"]);
    let metrics = &body["data"]["rows"][0]["metrics"];
    assert_eq!(metrics["close"], 119.0);
    assert_eq!(metrics["SMA5"], 117.0);
}

#[tokio::test]
async fn rsi_and_crossing_filters_evaluate_over_the_bars() {
    let filters = json!({"operator": "AND", "items": [
        {"key": "RSI", "op": "gte", "value": 100}]});
    let universe = json!(["TCS.NSE", "INFY.NSE"]);
    let (_, body) = scan(app(), q(filters, universe)).await;
    assert_eq!(row_ids(&body), ["TCS.NSE"]);
}

#[tokio::test]
async fn no_filter_returns_every_instrument_of_the_universe() {
    let query = json!({"universe": json!(["TCS.NSE", "INFY.NSE"]).to_string()});
    let (status, body) = scan(app(), query).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_ids(&body), ["INFY.NSE", "TCS.NSE"]);
    assert_eq!(body["data"]["rows"][0]["metrics"], json!({}));
}

#[tokio::test]
async fn as_of_evaluates_on_the_bars_known_then() {
    // TCS closes at 100 + day index; as of the 5th day (2024-01-05) it is 104.
    let mut query = q(gt("close", 110.0), json!(["TCS.NSE"]));
    let (_, latest) = scan(app(), query.clone()).await;
    assert_eq!(row_ids(&latest), ["TCS.NSE"]);
    query["as_of"] = json!("2024-01-05");
    let (status, early) = scan(app(), query).await;
    assert_eq!(status, StatusCode::OK, "{early}");
    assert!(row_ids(&early).is_empty());
    let mut inclusive = q(
        json!({"operator": "AND", "items": [
        {"key": "close", "op": "eq", "value": 104}]}),
        json!(["TCS.NSE"]),
    );
    inclusive["as_of"] = json!("2024-01-05");
    let (_, body) = scan(app(), inclusive).await;
    assert_eq!(row_ids(&body), ["TCS.NSE"], "as_of is inclusive");
}

#[tokio::test]
async fn an_unknown_instrument_is_a_404_not_a_silent_skip() {
    let (status, body) = scan(app(), q(gt("close", 1.0), json!(["TCS.NSE", "NOPE.NSE"]))).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "instrument_not_found");
}

#[tokio::test]
async fn a_timeframe_with_no_bars_is_a_404_market_data_unavailable() {
    let mut query = q(gt("close", 1.0), json!(["TCS.NSE"]));
    query["tf"] = json!("1m");
    let (status, body) = scan(app(), query).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "market_data_unavailable");
}

#[tokio::test]
async fn an_unsupported_metric_is_a_422_naming_the_reason_never_an_empty_result() {
    for key in ["price_earnings_ttm", "market_cap", "ema20"] {
        let (status, body) = scan(app(), q(gt(key, 1.0), json!(["TCS.NSE"]))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{key}: {body}");
        assert_eq!(body["error"]["code"], "validation_invalid_request");
        assert_eq!(body["error"]["context"]["field"], "filters");
        assert_eq!(body["error"]["context"]["reason"], "unsupported_metric");
        assert!(body["data"].is_null());
    }
}

#[tokio::test]
async fn bad_queries_are_422_envelopes_naming_the_field() {
    let cases = [
        (json!({}), "universe", "missing_universe"),
        (json!({"universe": "[]"}), "universe", "missing_universe"),
        (
            json!({"universe": "[\"TCS\"]"}),
            "universe",
            "invalid_instrument_id",
        ),
        (
            json!({"universe": "[\"TCS.NSE\"]", "filters": "{"}),
            "filters",
            "invalid_filters",
        ),
        (
            json!({"universe": "[\"TCS.NSE\"]", "as_of": "soon"}),
            "as_of",
            "invalid_time",
        ),
        (
            json!({"universe": "[\"TCS.NSE\"]", "tf": "9x"}),
            "tf",
            "invalid_timeframe",
        ),
        (json!({"universe": "[\"TCS.NSE\"]", "bogus": "1"}), "", ""),
    ];
    for (query, field, reason) in cases {
        let (status, body) = scan(app(), query.clone()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{query}: {body}");
        assert_eq!(
            body["error"]["code"], "validation_invalid_request",
            "{query}"
        );
        if !field.is_empty() {
            assert_eq!(body["error"]["context"]["field"], field, "{query}");
            assert_eq!(body["error"]["context"]["reason"], reason, "{query}");
        }
    }
}

#[tokio::test]
async fn an_oversized_universe_is_422_too_many_rows() {
    let ids: Vec<String> = (0..=honba_api::MAX_SCREENER_UNIVERSE)
        .map(|i| format!("S{i}.NSE"))
        .collect();
    let (status, body) = scan(app(), q(gt("close", 1.0), json!(ids))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["context"]["reason"], "too_many_rows");
    assert_eq!(
        body["error"]["context"]["limit"],
        honba_api::MAX_SCREENER_UNIVERSE
    );
}

#[tokio::test]
async fn too_many_matches_is_422_too_many_rows_not_a_truncated_list() {
    let count = honba_api::MAX_SCREENER_ROWS + 1;
    let slices: Vec<_> = (0..count)
        .map(|i| daily(&format!("S{i:04}"), &[10.0]))
        .collect();
    let ids: Vec<String> = (0..count).map(|i| format!("S{i:04}.NSE")).collect();
    let query = json!({"universe": json!(ids).to_string()});
    let (status, body) = scan(app_with(slices), query).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["context"]["reason"], "too_many_rows");
}

#[tokio::test]
async fn the_scan_is_deterministic() {
    let query = q(
        gt("close", 1.0),
        json!(["WIPRO.NSE", "TCS.NSE", "INFY.NSE"]),
    );
    let (_, first) = scan(app(), query.clone()).await;
    let (_, second) = scan(app(), query).await;
    assert_eq!(first, second);
}
