//! `POST /bars` (E3-S7 follow-up): bars arrive over HTTP, go through a bounded channel into the
//! bar pump, and fill orders / refresh quotes. Without a pump the route says so with a stable
//! reason code; a full channel is a 429 the caller can retry.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use honba_api_rest::{api_router_with, api_router_with_config, ApiConfig, AppState};
use honba_entities::{Currency, Instrument, InstrumentKind};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, ErrorCode, Exchange, InstrumentId, PriceType,
    UnixNanos,
};
use serde_json::{json, Value};
use tower::util::ServiceExt;

fn iid() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn state() -> AppState {
    let id = iid();
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::new(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

fn bar(ts: u64, open: f64) -> Value {
    let t = UnixNanos::from_u64(ts);
    let bt = BarType::new(
        iid(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    serde_json::to_value(Bar::new(bt, open, open, open, open, 1000.0, t, t)).unwrap()
}

async fn send(
    router: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let res = router.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn call(
    st: &AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    send(api_router_with(st.clone()), method, uri, body, None).await
}

async fn post_bars(st: &AppState, bars: Vec<Value>) -> (StatusCode, Value) {
    call(st, Method::POST, "/bars", Some(json!({ "bars": bars }))).await
}

fn reason(body: &Value) -> &str {
    body["error"]["context"]["reason"].as_str().unwrap_or("")
}

#[tokio::test]
async fn posted_bars_fill_a_market_order_and_refresh_quotes() {
    let st = state();
    let pump = st.start_bar_ingest(16);
    let order = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "buy", "qty": 25.0, "client_order_id": "o1",
    });
    let (s, _) = call(&st, Method::POST, "/orders", Some(order)).await;
    assert_eq!(s, StatusCode::OK);

    let (s, v) = post_bars(&st, vec![bar(1_000, 101.0), bar(2_000, 123.5)]).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["accepted"], json!(2));

    st.stop_bar_ingest();
    let report = pump.await.unwrap();
    assert_eq!((report.bars, report.errors), (2, 0));

    assert_eq!(st.orders.position(&iid()), 25.0);
    let (s, v) = call(&st, Method::GET, "/quotes?symbols=X", None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let text = v.to_string();
    assert!(text.contains("123.5"), "{text}");
}

#[tokio::test]
async fn an_out_of_order_bar_is_counted_not_fatal() {
    let st = state();
    let pump = st.start_bar_ingest(16);
    let (s, _) = post_bars(&st, vec![bar(2_000, 101.0), bar(1_000, 99.0)]).await;
    assert_eq!(s, StatusCode::OK);
    st.stop_bar_ingest();
    let report = pump.await.unwrap();
    assert_eq!((report.bars, report.errors), (2, 1));
}

#[tokio::test]
async fn empty_batch_is_rejected() {
    let st = state();
    let _pump = st.start_bar_ingest(16);
    let (s, v) = post_bars(&st, vec![]).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert_eq!(v["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(reason(&v), "bar_batch_empty");
}

#[tokio::test]
async fn oversized_batch_is_rejected() {
    let st = state();
    let _pump = st.start_bar_ingest(16);
    let bars: Vec<Value> = (1..=(honba_api::MAX_INGEST_BARS as u64 + 1))
        .map(|i| bar(i * 1000, 100.0))
        .collect();
    let (s, v) = post_bars(&st, bars).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert_eq!(v["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(reason(&v), "bar_batch_too_large");
}

#[tokio::test]
async fn an_invalid_bar_rejects_the_whole_batch() {
    let st = state();
    let _pump = st.start_bar_ingest(16);
    let mut bad = bar(2_000, 100.0);
    bad["high"] = json!(1.0); // below low, open and close
    let (s, v) = post_bars(&st, vec![bar(1_000, 100.0), bad]).await;
    assert!(s.is_client_error(), "{s} {v}");
    assert_eq!(v["error"]["code"], json!("validation_invalid_request"));
}

#[tokio::test]
async fn unknown_request_fields_are_rejected() {
    let st = state();
    let _pump = st.start_bar_ingest(16);
    let (s, v) = call(
        &st,
        Method::POST,
        "/bars",
        Some(json!({"bars": [bar(1_000, 100.0)], "extra": 1})),
    )
    .await;
    assert!(s.is_client_error(), "{s} {v}");
}

#[tokio::test]
async fn without_a_pump_the_route_is_unavailable_with_a_reason_code() {
    let st = state();
    let (s, v) = post_bars(&st, vec![bar(1_000, 100.0)]).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert_eq!(v["error"]["code"], json!("market_data_unavailable"));
    assert_eq!(reason(&v), "bar_ingest_disabled");
}

#[tokio::test]
async fn after_the_pump_is_stopped_the_route_is_unavailable_again() {
    let st = state();
    let pump = st.start_bar_ingest(4);
    st.stop_bar_ingest();
    pump.await.unwrap();
    let (s, v) = post_bars(&st, vec![bar(1_000, 100.0)]).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert_eq!(reason(&v), "bar_ingest_disabled");
}

#[tokio::test]
async fn a_full_channel_is_429_with_a_reason_and_accepts_nothing() {
    let st = state();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bar>(2);
    st.attach_bar_ingest(tx);

    let (s, _) = post_bars(&st, vec![bar(1_000, 100.0), bar(2_000, 100.0)]).await;
    assert_eq!(s, StatusCode::OK);

    // The consumer is stalled: nothing was drained, so the next batch finds no room.
    let (s, v) = post_bars(&st, vec![bar(3_000, 100.0)]).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS, "{v}");
    assert_eq!(v["error"]["code"], json!("rate_limited"));
    assert_eq!(reason(&v), "bar_ingest_full");
    assert_eq!(
        v["error"]["retryable"],
        json!(ErrorCode::RateLimited.is_retryable())
    );

    // Draining makes room; the rejected batch left nothing behind.
    assert!(rx.recv().await.is_some());
    assert!(rx.recv().await.is_some());
    assert!(rx.try_recv().is_err());
    let (s, _) = post_bars(&st, vec![bar(3_000, 100.0)]).await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn a_batch_larger_than_the_channel_can_ever_hold_is_a_validation_error() {
    let st = state();
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bar>(1);
    st.attach_bar_ingest(tx);
    let (s, v) = post_bars(&st, vec![bar(1_000, 100.0), bar(2_000, 100.0)]).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert_eq!(reason(&v), "bar_batch_exceeds_capacity");
}

// POST /bars is a write route (scope write:orders), like the order routes.

// sha256("test")
const TEST_HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

fn authed_router(st: &AppState, scopes: &str) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        format!(
            "[[tokens]]\ntoken_id = \"t1\"\ntoken_hash = \"sha256:{TEST_HASH}\"\n\
             scopes = [{scopes}]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    (api_router_with_config(st.clone(), &config), dir)
}

#[tokio::test]
async fn post_bars_requires_a_token() {
    let st = state();
    let _pump = st.start_bar_ingest(4);
    let (router, _dir) = authed_router(&st, "\"write:orders\"");
    let body = json!({"bars": [bar(1_000, 100.0)]});
    let (s, v) = send(router, Method::POST, "/bars", Some(body), None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "{v}");
    assert_eq!(v["error"]["code"], json!("unauthorized"));
}

#[tokio::test]
async fn post_bars_refuses_a_read_scope_token() {
    let st = state();
    let _pump = st.start_bar_ingest(4);
    let (router, _dir) = authed_router(&st, "\"read:market\"");
    let body = json!({"bars": [bar(1_000, 100.0)]});
    let (s, v) = send(router, Method::POST, "/bars", Some(body), Some("test")).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(v["error"]["code"], json!("forbidden"));
}

#[tokio::test]
async fn post_bars_accepts_a_write_scope_token() {
    let st = state();
    let pump = st.start_bar_ingest(4);
    let (router, _dir) = authed_router(&st, "\"write:orders\"");
    let body = json!({"bars": [bar(1_000, 100.0)]});
    let (s, v) = send(router, Method::POST, "/bars", Some(body), Some("test")).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    st.stop_bar_ingest();
    pump.await.unwrap();
}
