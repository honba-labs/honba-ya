//! A live bar source drives the simulated market (E3-S7 follow-up): bars sent into the pump
//! fill market orders and move the ledger, the pump ends when the channel closes, and a bad
//! bar is counted and skipped rather than killing it. Event time only; no wall clock.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use honba_api_rest::{api_router_with, AppState};
use honba_entities::{Currency, Instrument, InstrumentKind};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, PriceType, UnixNanos,
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

fn bar(ts: u64, open: f64) -> Bar {
    let t = UnixNanos::from_u64(ts);
    let bt = BarType::new(
        iid(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    Bar::new(bt, open, open, open, open, 1000.0, t, t)
}

async fn call(
    st: &AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let res = api_router_with(st.clone())
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn place(st: &AppState, id: &str) {
    let body = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "buy", "qty": 25.0, "client_order_id": id,
    });
    let (s, _) = call(st, Method::POST, "/orders", Some(body)).await;
    assert_eq!(s, StatusCode::OK);
}

async fn order_status(st: &AppState, id: &str) -> String {
    let (s, v) = call(st, Method::GET, "/orders", None).await;
    assert_eq!(s, StatusCode::OK);
    let text = v.to_string();
    assert!(text.contains(id), "{text}");
    serde_json::to_value(st.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn bars_through_the_pump_fill_an_order_and_move_the_position() {
    let st = state();
    place(&st, "o1").await;
    assert_eq!(order_status(&st, "o1").await, "submitted");

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let pump = st.spawn_bar_pump(rx);
    tx.send(bar(1_000, 101.0)).await.unwrap();
    drop(tx);
    let report = pump.await.unwrap();

    assert_eq!(report.bars, 1);
    assert_eq!(report.errors, 0);
    assert_eq!(order_status(&st, "o1").await, "filled");
    assert_eq!(st.orders.position(&iid()), 25.0);
    let (s, v) = call(&st, Method::GET, "/orders", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(v.to_string().contains("filled"), "{v}");
}

#[tokio::test]
async fn the_pump_stops_when_the_channel_closes() {
    let st = state();
    let (tx, rx) = tokio::sync::mpsc::channel::<Bar>(1);
    let pump = st.spawn_bar_pump(rx);
    drop(tx);
    let report = tokio::time::timeout(std::time::Duration::from_secs(5), pump)
        .await
        .expect("pump must end on a closed channel")
        .unwrap();
    assert_eq!((report.bars, report.errors), (0, 0));
}

#[tokio::test]
async fn an_advance_error_is_counted_and_does_not_kill_the_pump() {
    let st = state();
    place(&st, "o1").await;
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let pump = st.spawn_bar_pump(rx);
    tx.send(bar(2_000, 101.0)).await.unwrap();
    tx.send(bar(1_000, 99.0)).await.unwrap(); // out of order: advance_market errors
    tx.send(bar(3_000, 102.0)).await.unwrap();
    drop(tx);
    let report = pump.await.unwrap();
    assert_eq!(report.bars, 3);
    assert_eq!(report.errors, 1);
    assert_eq!(order_status(&st, "o1").await, "filled");
}

#[tokio::test]
async fn pumped_bar_refreshes_the_quote_the_risk_gate_reads() {
    let st = state();
    let before = st.quotes.read_quote(&iid(), None).await.unwrap().unwrap();
    assert_eq!(before.ts_event(), UnixNanos::from_u64(1));

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let pump = st.spawn_bar_pump(rx);
    tx.send(bar(5_000, 123.5)).await.unwrap();
    drop(tx);
    pump.await.unwrap();

    // `st.quotes` is the port `risk_request` and `reference_price` read.
    let after = st.quotes.read_quote(&iid(), None).await.unwrap().unwrap();
    assert_eq!(after.ts_event(), UnixNanos::from_u64(5_000));
    assert_eq!(after.bid_price(), 123.5);
    assert!(st.live_quotes().is_some());
}

#[tokio::test]
async fn get_quotes_returns_the_pumped_bar_close() {
    let st = state();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let pump = st.spawn_bar_pump(rx);
    tx.send(bar(5_000, 123.5)).await.unwrap();
    drop(tx);
    pump.await.unwrap();

    let (s, v) = call(&st, Method::GET, "/quotes?symbols=X", None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let text = v.to_string();
    assert!(text.contains("123.5"), "{text}");
    assert!(text.contains("5000"), "{text}");
}
