//! Approved orders reach the embedded `NextOpenSim` (ADR 0016): a market order stays
//! `submitted` until the market advances, then fills at the next bar open and the
//! ledger position follows. No network, no wall clock: event time is the bar's `ts_event`.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
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

/// A bar of `X.NSE` opening at `open` at event time `ts`.
fn bar(ts: u64, open: f64) -> Bar {
    let t = UnixNanos::from_u64(ts);
    let bt = BarType::new(
        iid(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    Bar::new(bt, open, open, open, open, 1000.0, t, t)
}

async fn call(app: Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let res = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn order(side: &str, qty: f64, id: &str, limit: Option<f64>) -> Value {
    let mut body = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side,
        "qty": qty,
        "client_order_id": id,
    });
    if let Some(px) = limit {
        body["order_type"] = json!("limit");
        body["price"] = json!(px);
    }
    body
}

fn status_of(state: &AppState, id: &str) -> String {
    serde_json::to_value(state.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn market_order_is_unfilled_until_the_market_advances() {
    let st = state();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "o1"), "submitted");

    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    assert_eq!(status_of(&st, "o1"), "filled");
    assert_eq!(st.orders.position(&iid()), 25.0);
}

#[tokio::test]
async fn order_placed_after_a_bar_fills_on_the_next_one() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    assert_eq!(status_of(&st, "o1"), "submitted");
    st.advance_market(&[bar(2_000, 102.0)]).unwrap();
    assert_eq!(status_of(&st, "o1"), "filled");
}

#[tokio::test]
async fn close_after_a_fill_flattens_the_position() {
    let st = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // The close order is still working: position is the (conservative) sum.
    st.advance_market(&[bar(2_000, 103.0)]).unwrap();
    assert!(st.orders.positions().is_empty());
    assert!(st
        .orders
        .list()
        .iter()
        .all(|o| serde_json::to_value(o).unwrap()["status"] == "filled"));
}

#[tokio::test]
async fn limit_orders_are_not_filled() {
    let st = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "l1", Some(99.0))),
    )
    .await;
    st.advance_market(&[bar(1_000, 90.0)]).unwrap();
    assert_eq!(status_of(&st, "l1"), "rejected");
    assert_eq!(st.orders.position(&iid()), 0.0);
}

#[tokio::test]
async fn sell_without_a_position_is_rejected_by_the_sim() {
    let st = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("sell", 25.0, "s1", None)),
    )
    .await;
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    assert_eq!(status_of(&st, "s1"), "rejected");
    assert_eq!(st.orders.position(&iid()), 0.0);
}

#[tokio::test]
async fn approval_queue_approve_submits_to_the_sim() {
    let st = state().with_approval_queue(10);
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "q1", None)),
    )
    .await;
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/approval/approve",
        None,
    )
    .await;
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    assert_eq!(status_of(&st, "q1"), "filled");
}

fn row(state: &AppState, id: &str) -> Value {
    serde_json::to_value(state.orders.get(id).unwrap()).unwrap()
}

#[tokio::test]
async fn a_filled_market_order_reports_filled_qty_and_the_fill_price() {
    let st = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    // Unfilled rows carry neither field (additive wire).
    let before = row(&st, "o1");
    assert!(before.get("filled_qty").is_none() && before.get("avg_fill_price").is_none());
    assert!(before.get("filled_at").is_none());

    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let after = row(&st, "o1");
    assert_eq!(after["status"], "filled");
    assert_eq!(after["filled_qty"], 25.0);
    assert!((after["avg_fill_price"].as_f64().unwrap() - 101.0).abs() < 0.5);
    assert!(after.get("reject_reason").is_none());
    // `filled_at` is the fill's event time: the bar the simulator filled on.
    assert_eq!(
        after["filled_at"],
        serde_json::to_value(UnixNanos::from_u64(1_000)).unwrap()
    );

    // GET /orders serves the same fields.
    let (_, body) = call(api_router_with(st.clone()), Method::GET, "/orders", None).await;
    assert_eq!(body["data"]["orders"][0]["filled_qty"], 25.0);
}

#[tokio::test]
async fn simulator_rejections_carry_a_stable_code_and_the_reason() {
    let st = state();
    for (id, side, limit) in [("l1", "buy", Some(99.0)), ("s1", "sell", None)] {
        call(
            api_router_with(st.clone()),
            Method::POST,
            "/orders",
            Some(order(side, 25.0, id, limit)),
        )
        .await;
    }
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let limit = row(&st, "l1");
    assert_eq!(limit["status"], "rejected");
    assert_eq!(limit["reject_reason"]["code"], "order_rejected");
    assert!(limit["reject_reason"]["message"]
        .as_str()
        .unwrap()
        .contains("unsupported_order_type"));
    let sell = row(&st, "s1");
    assert_eq!(sell["reject_reason"]["code"], "order_rejected");
    assert!(sell["reject_reason"]["message"]
        .as_str()
        .unwrap()
        .contains("no_position"));
    assert!(sell.get("filled_qty").is_none());
}
