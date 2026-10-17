//! A venue gateway injected with `AppState::with_execution_gateway` receives the submit and
//! cancel calls of every order route, and the events it emits move ledger rows through the
//! same status mapping as the simulator (E3-S7). Terminal rows never change.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, AppState, ExecutionVenue};
use honba_engine::{ExecutionEngine, Result};
use honba_entities::{Currency, ExecutionEvent, Instrument, InstrumentKind, Trade};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{Order, OrderId, OrderSide, UnixNanos};
use serde_json::{json, Value};
use tower::util::ServiceExt;

#[derive(Default)]
struct Shared {
    calls: Vec<String>,
    pending: Vec<ExecutionEvent>,
}

struct Fake(Arc<Mutex<Shared>>);

impl ExecutionEngine for Fake {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .calls
            .push(format!("submit:{}", order.order_id()));
        Ok(())
    }
    fn cancel(&mut self, order_id: &str, _now: UnixNanos) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .calls
            .push(format!("cancel:{order_id}"));
        Ok(())
    }
    fn native_events(&self) -> bool {
        true
    }
    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.0.lock().unwrap().pending))
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

impl ExecutionVenue for Fake {}

fn iid() -> honba_messages::InstrumentId {
    honba_messages::InstrumentId::new("X", honba_messages::Exchange::new("NSE"))
}

fn state() -> (AppState, Arc<Mutex<Shared>>) {
    let id = iid();
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::new(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    let shared = Arc::new(Mutex::new(Shared::default()));
    let st = AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
        .with_execution_gateway(Fake(shared.clone()));
    (st, shared)
}

async fn call(app: Router, method: Method, uri: &str, body: Option<Value>) -> StatusCode {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    app.oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
        .status()
}

fn order(side: &str, qty: f64, id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side, "qty": qty, "client_order_id": id,
    })
}

fn status_of(state: &AppState, id: &str) -> String {
    serde_json::to_value(state.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn calls(shared: &Arc<Mutex<Shared>>) -> Vec<String> {
    shared.lock().unwrap().calls.clone()
}

fn fill_event(id: &str, qty: f64, cum: f64, complete: bool) -> ExecutionEvent {
    let t = UnixNanos::from_u64(5);
    ExecutionEvent::Fill {
        trade: Trade::new(
            OrderId::new(id),
            iid(),
            OrderSide::Buy,
            qty,
            100.0,
            Currency::Inr,
            t,
            t,
        ),
        cum_qty: cum,
        complete,
        venue_order_id: None,
    }
}

#[tokio::test]
async fn post_orders_submits_to_the_injected_gateway() {
    let (st, shared) = state();
    let s = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(calls(&shared), vec!["submit:o1"]);
    assert_eq!(status_of(&st, "o1"), "submitted");
}

#[tokio::test]
async fn approval_approve_submits_to_the_injected_gateway() {
    let (st, shared) = state();
    let st = st.with_approval_queue(10);
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "q1")),
    )
    .await;
    assert!(calls(&shared).is_empty());
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/approval/approve",
        None,
    )
    .await;
    assert_eq!(calls(&shared), vec!["submit:q1"]);
}

#[tokio::test]
async fn delete_order_cancels_on_the_injected_gateway() {
    let (st, shared) = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1")),
    )
    .await;
    call(
        api_router_with(st.clone()),
        Method::DELETE,
        "/orders/o1",
        None,
    )
    .await;
    assert_eq!(calls(&shared), vec!["submit:o1", "cancel:o1"]);
    assert_eq!(status_of(&st, "o1"), "cancelled");
}

#[tokio::test]
async fn close_positions_submits_the_closing_order() {
    let (st, shared) = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1")),
    )
    .await;
    shared
        .lock()
        .unwrap()
        .pending
        .push(fill_event("o1", 25.0, 25.0, true));
    st.advance_market(&[]).unwrap();
    assert_eq!(status_of(&st, "o1"), "filled");
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    let c = calls(&shared);
    assert_eq!(c.len(), 2, "{c:?}");
    assert!(c[1].starts_with("submit:"));
}

#[tokio::test]
async fn gateway_events_update_ledger_status() {
    let (st, shared) = state();
    for id in ["f", "p", "r", "c"] {
        call(
            api_router_with(st.clone()),
            Method::POST,
            "/orders",
            Some(order("buy", 25.0, id)),
        )
        .await;
    }
    let t = UnixNanos::from_u64(5);
    {
        let mut s = shared.lock().unwrap();
        s.pending.push(fill_event("f", 25.0, 25.0, true));
        s.pending.push(fill_event("p", 10.0, 10.0, false));
        s.pending.push(ExecutionEvent::Rejected {
            order_id: OrderId::new("r"),
            instrument_id: iid(),
            side: OrderSide::Buy,
            quantity: 25.0,
            reason: "venue said no".into(),
            venue_order_id: None,
            ts: t,
        });
        s.pending.push(ExecutionEvent::Cancelled {
            order_id: OrderId::new("c"),
            instrument_id: iid(),
            side: OrderSide::Buy,
            quantity: 25.0,
            venue_order_id: None,
            ts: t,
        });
        // A late event for a terminal row must not change it.
        s.pending.push(fill_event("r", 25.0, 25.0, true));
    }
    st.advance_market(&[]).unwrap();
    assert_eq!(status_of(&st, "f"), "filled");
    assert_eq!(status_of(&st, "p"), "partially_filled");
    assert_eq!(status_of(&st, "r"), "rejected");
    assert_eq!(status_of(&st, "c"), "cancelled");
}

fn priced_fill(id: &str, qty: f64, px: f64, cum: f64, complete: bool) -> ExecutionEvent {
    let t = UnixNanos::from_u64(5);
    ExecutionEvent::Fill {
        trade: Trade::new(
            OrderId::new(id),
            iid(),
            OrderSide::Buy,
            qty,
            px,
            Currency::Inr,
            t,
            t,
        ),
        cum_qty: cum,
        complete,
        venue_order_id: None,
    }
}

fn row(st: &AppState, id: &str) -> Value {
    serde_json::to_value(st.orders.get(id).unwrap()).unwrap()
}

#[tokio::test]
async fn partial_fills_update_filled_qty_and_the_average_incrementally() {
    let (st, shared) = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 50.0, "p")),
    )
    .await;
    shared
        .lock()
        .unwrap()
        .pending
        .push(priced_fill("p", 20.0, 100.0, 20.0, false));
    st.advance_market(&[]).unwrap();
    let r = row(&st, "p");
    assert_eq!(r["status"], "partially_filled");
    assert_eq!(r["filled_qty"], 20.0);
    assert_eq!(r["avg_fill_price"], 100.0);

    shared
        .lock()
        .unwrap()
        .pending
        .push(priced_fill("p", 30.0, 103.0, 50.0, true));
    st.advance_market(&[]).unwrap();
    let r = row(&st, "p");
    assert_eq!(r["status"], "filled");
    assert_eq!(r["filled_qty"], 50.0);
    let avg = r["avg_fill_price"].as_f64().unwrap();
    assert!(
        (avg - (20.0 * 100.0 + 30.0 * 103.0) / 50.0).abs() < 1e-9,
        "{avg}"
    );

    // A late fill on a terminal row changes nothing.
    shared
        .lock()
        .unwrap()
        .pending
        .push(priced_fill("p", 5.0, 500.0, 55.0, true));
    st.advance_market(&[]).unwrap();
    assert_eq!(row(&st, "p")["filled_qty"], 50.0);
}

#[tokio::test]
async fn venue_rejections_carry_the_code_when_the_reason_is_one_else_order_rejected() {
    let (st, shared) = state();
    for id in ["r1", "r2"] {
        call(
            api_router_with(st.clone()),
            Method::POST,
            "/orders",
            Some(order("buy", 25.0, id)),
        )
        .await;
    }
    let t = UnixNanos::from_u64(5);
    for (id, reason) in [
        ("r1", "order_execution_unavailable"),
        ("r2", "venue said no"),
    ] {
        shared
            .lock()
            .unwrap()
            .pending
            .push(ExecutionEvent::Rejected {
                order_id: OrderId::new(id),
                instrument_id: iid(),
                side: OrderSide::Buy,
                quantity: 25.0,
                reason: reason.into(),
                venue_order_id: None,
                ts: t,
            });
    }
    st.advance_market(&[]).unwrap();
    assert_eq!(
        row(&st, "r1")["reject_reason"]["code"],
        "order_execution_unavailable"
    );
    let r2 = row(&st, "r2");
    assert_eq!(r2["reject_reason"]["code"], "order_rejected");
    assert_eq!(r2["reject_reason"]["message"], "venue said no");
}

#[tokio::test]
async fn a_rejected_remainder_after_a_partial_fill_keeps_the_fill_and_records_why() {
    let (st, shared) = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 50.0, "q")),
    )
    .await;
    shared.lock().unwrap().pending.extend([
        priced_fill("q", 20.0, 100.0, 20.0, false),
        ExecutionEvent::Rejected {
            order_id: OrderId::new("q"),
            instrument_id: iid(),
            side: OrderSide::Buy,
            quantity: 30.0,
            reason: "insufficient_funds".into(),
            venue_order_id: None,
            ts: UnixNanos::from_u64(6),
        },
    ]);
    st.advance_market(&[]).unwrap();
    let r = row(&st, "q");
    assert_eq!(r["status"], "partially_filled");
    assert_eq!(r["filled_qty"], 20.0);
    assert_eq!(r["reject_reason"]["code"], "order_rejected");
    assert_eq!(r["reject_reason"]["message"], "insufficient_funds");
}

/// `L2Sim` behind the ledger: its top-of-book partial fills book incrementally.
struct L2Venue(Arc<Mutex<honba_sim::L2Sim>>);

impl ExecutionEngine for L2Venue {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.0.lock().unwrap().submit(order)
    }
    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        self.0.lock().unwrap().cancel(order_id, now)
    }
    fn native_events(&self) -> bool {
        true
    }
    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        self.0.lock().unwrap().drain_events()
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

impl ExecutionVenue for L2Venue {}

#[tokio::test]
async fn l2sim_partial_fills_report_cumulative_qty_and_the_weighted_average() {
    use honba_messages::QuoteTick;
    let sim = Arc::new(Mutex::new(
        honba_sim::L2Sim::new(honba_entities::Money::new(1_000_000_000_000, Currency::Inr))
            .unwrap(),
    ));
    let quote = |ask: f64, asz: f64, ts: u64| {
        let t = UnixNanos::from_u64(ts);
        sim.lock()
            .unwrap()
            .on_quote(&QuoteTick::new(iid(), ask - 2.0, ask, 100.0, asz, t, t))
            .unwrap();
    };
    let (st, _) = state();
    let st = st.with_execution_gateway(L2Venue(sim.clone()));
    quote(101.0, 25.0, 1);
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 50.0, "l2")),
    )
    .await;
    st.advance_market(&[]).unwrap();
    let r = row(&st, "l2");
    assert_eq!(r["status"], "partially_filled");
    assert_eq!(r["filled_qty"], 25.0);
    assert_eq!(r["avg_fill_price"], 101.0);

    quote(103.0, 100.0, 3);
    st.advance_market(&[]).unwrap();
    let r = row(&st, "l2");
    assert_eq!(r["status"], "filled");
    assert_eq!(r["filled_qty"], 50.0);
    assert_eq!(r["avg_fill_price"], 102.0);
}
