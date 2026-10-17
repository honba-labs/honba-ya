//! `POST /orders`, `DELETE /orders/{id}` and `POST /positions/close` through the real
//! router: the risk gate of ADR 0018 decision 7 (E2-S2 r5b) and the approval queue
//! (E5-S4, E11-S7).
//!
//! No network and no wall clock: the state is an in-memory dataset plus one instrument.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, AppState};
use honba_entities::{Currency, Instrument, InstrumentKind};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{Exchange, InstrumentId, TradingState, UnixNanos};
use serde_json::{json, Value};
use tower::util::ServiceExt;

fn instrument_id() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

/// One instrument `X.NSE` (INR equity, lot 25, tick 0.05) with one bar closing at
/// 100; the bar store derives bid = ask = 100, so `reference_price` is 100.
fn state() -> AppState {
    let id = instrument_id();
    let spec = honba_api::parse_timeframe("1m").unwrap();
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::new(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
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
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON body {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn order(symbol: &str, exchange: &str, side: &str, qty: f64, limit: Option<f64>) -> Value {
    let mut body = json!({
        "instrument_id": {"symbol": symbol, "exchange": exchange},
        "side": side,
        "qty": qty,
    });
    if let Some(px) = limit {
        body["order_type"] = json!("limit");
        body["price"] = json!(px);
    }
    body
}

#[tokio::test]
async fn order_refused_by_risk() {
    // An instrument the master does not know is refused and named.
    let (status, body) = call(
        api_router_with(state()),
        Method::POST,
        "/orders",
        Some(order("Z", "NSE", "buy", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_instrument_unknown"));
    assert_eq!(
        body["error"]["context"]["rule"],
        json!("instrument_unknown")
    );
    assert!(body["data"].is_null());

    // A quantity that is not a lot multiple is a typed shape refusal with numbers.
    let (status, body) = call(
        api_router_with(state()),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 30.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_lot_multiple_violation"));
    assert_eq!(body["error"]["context"]["rule"], json!("lot_multiple"));
    assert_eq!(body["error"]["context"]["lot"], json!(25.0));

    // A price off the tick grid is refused with the field and tick.
    let (status, body) = call(
        api_router_with(state()),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.03))),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_tick_size_violation"));
    assert_eq!(body["error"]["context"]["rule"], json!("tick_size"));
    assert_eq!(body["error"]["context"]["field"], json!("price"));
    assert_eq!(body["error"]["context"]["reason"], json!("off_tick"));
}

#[tokio::test]
async fn post_order_flows_to_sim_only() {
    // An approved order answers with its ledger row and is recorded, so
    // `GET /orders` reads it back with the rest of the ledger.
    let app = api_router_with(state());
    let mut placed = order("X", "NSE", "buy", 25.0, None);
    placed["client_order_id"] = json!("post-flow");
    let (status, body) = call(app.clone(), Method::POST, "/orders", Some(placed)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["order_id"], json!("post-flow"));
    assert_eq!(body["data"]["status"], json!("submitted"));

    let (status, body) = call(app, Method::GET, "/orders", None).await;
    assert_eq!(status, StatusCode::OK);
    let orders = body["data"]["orders"].as_array().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["order_id"], json!("post-flow"));
    assert_eq!(
        orders[0]["instrument_id"],
        json!({"symbol": "X", "exchange": "NSE"})
    );
    assert_eq!(orders[0]["side"], json!("buy"));
    assert_eq!(orders[0]["quantity"], json!(25.0));
    assert_eq!(orders[0]["status"], json!("submitted"));
}

#[tokio::test]
async fn a_repeated_idempotency_key_returns_the_first_row_and_adds_nothing() {
    let app = api_router_with(state());
    let mut placed = order("X", "NSE", "buy", 25.0, None);
    placed["client_order_id"] = json!("dup-key");
    for _ in 0..2 {
        let (status, body) = call(app.clone(), Method::POST, "/orders", Some(placed.clone())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["order_id"], json!("dup-key"));
        assert_eq!(body["data"]["status"], json!("submitted"));
    }

    let (status, body) = call(app, Method::GET, "/orders", None).await;
    assert_eq!(status, StatusCode::OK);
    let orders = body["data"]["orders"].as_array().unwrap();
    assert_eq!(orders.len(), 1);
}

#[tokio::test]
async fn the_ledger_keeps_every_acknowledged_order_and_treats_unknown_filters_as_empty() {
    // One keyed order per idempotency key: ADR 0019 decision 3 deduplicates
    // on the client order id, so the two submits are two rows.
    let app = api_router_with(state());
    let mut n = 0;
    for (side, qty) in [("buy", 25.0), ("sell", 25.0)] {
        n += 1;
        let mut body = order("X", "NSE", side, qty, Some(100.0));
        body["client_order_id"] = json!(format!("key-{n}"));
        let (status, _) = call(app.clone(), Method::POST, "/orders", Some(body)).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, body) = call(app.clone(), Method::GET, "/orders", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["orders"].as_array().unwrap().len(), 2);

    // An unknown query key is `validation_invalid_request`, never silently ignored.
    let (status, body) = call(app, Method::GET, "/orders?charger=1", None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
}

#[tokio::test]
async fn halted_refusal_precedes_instrument_lookup() {
    // Regression (ADR 0018 decision 4: the state rules are 1-2, unknown instrument
    // is 3). A halted engine reports why it is closed even for an unknown symbol.
    let app = api_router_with(state().with_trading_state(TradingState::Halted));
    let (status, body) = call(
        app,
        Method::POST,
        "/orders",
        Some(order("Z", "NSE", "buy", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_trading_halted"));
}

#[tokio::test]
async fn cancel_and_close_allowed_when_halted() {
    let app = api_router_with(state().with_trading_state(TradingState::Halted));

    // A placement is refused while halted.
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_trading_halted"));

    // Cancelling and closing cannot add exposure, so both are allowed while halted.
    let (status, body) = call(app.clone(), Method::DELETE, "/orders/o1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("cancelled"));
    let (status, body) = call(app, Method::POST, "/positions/close", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("closing"));
}

#[tokio::test]
async fn approval_queue_holds_placements() {
    // When approval queue is enabled, POST /orders enqueues the order
    let app = api_router_with(state().with_approval_queue(10));
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("queued"));
    assert_eq!(body["data"]["queue_position"], json!(1));
    assert!(body["data"]["message"]
        .as_str()
        .unwrap()
        .contains("approval queue"));

    // GET /approval/queue returns the queued order
    let (status, body) = call(app.clone(), Method::GET, "/approval/queue", None).await;
    assert_eq!(status, StatusCode::OK);
    let queue = body["data"]["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0]["queue_id"], json!("rest"));
    assert_eq!(queue[0]["status"], json!("pending"));
    assert_eq!(queue[0]["instrument_id"], json!("X.NSE"));
    assert_eq!(queue[0]["side"], json!("buy"));
    assert_eq!(queue[0]["qty"], json!(25.0));
    assert_eq!(queue[0]["price"], json!(100.0));
}

#[tokio::test]
async fn approve_flows_through_risk_to_sim() {
    // Approve a queued order: it re-enters the risk stage, lands in the ledger
    // and answers with its row.
    let app = api_router_with(state().with_approval_queue(10));
    let mut placed = order("X", "NSE", "buy", 25.0, None);
    placed["client_order_id"] = json!("approve-flow");
    let (status, body) = call(app.clone(), Method::POST, "/orders", Some(placed)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("queued"));

    // Approve it
    let (status, body) = call(app.clone(), Method::POST, "/approval/approve", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["order_id"], json!("approve-flow"));
    assert_eq!(body["data"]["status"], json!("submitted"));

    // Queue is now empty
    let (status, body) = call(app, Method::GET, "/approval/queue", None).await;
    assert_eq!(status, StatusCode::OK);
    let queue = body["data"]["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn reject_removes_from_queue() {
    // Reject a queued order: it is removed and returns rejected status
    let app = api_router_with(state().with_approval_queue(10));
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("queued"));

    // Reject it
    let (status, body) = call(app.clone(), Method::POST, "/approval/reject", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("rejected"));
    assert_eq!(body["data"]["message"], json!("Order rejected by operator"));

    // Queue is now empty
    let (status, body) = call(app, Method::GET, "/approval/queue", None).await;
    assert_eq!(status, StatusCode::OK);
    let queue = body["data"]["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn cancel_and_close_bypass_queue_even_when_halted() {
    // With approval queue enabled and trading halted
    let app = api_router_with(
        state()
            .with_approval_queue(10)
            .with_trading_state(TradingState::Halted),
    );

    // A placement is queued (not refused by risk because queue catches it first)
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("queued"));

    // Cancelling and closing bypass the queue entirely
    let (status, body) = call(app.clone(), Method::DELETE, "/orders/o1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("cancelled"));
    let (status, body) = call(app, Method::POST, "/positions/close", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("closing"));
}

#[tokio::test]
async fn approval_queue_full_returns_429() {
    // Queue with size 1: second placement returns 429
    let app = api_router_with(state().with_approval_queue(1));
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("queued"));

    // Second order should fail with queue full
    let (status, body) = call(
        app,
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], json!("rate_limited"));
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("approval_queue_full")
    );
}

/// Seeds the write ledger with one working buy of 25 on `X.NSE`, then runs the
/// app in the reduce-only state.
fn reducing_state_with_long_25() -> AppState {
    let state = state();
    state.orders.record(
        honba_messages::Order::new(
            honba_messages::OrderId::new("seed-1"),
            instrument_id(),
            honba_messages::OrderSide::Buy,
            honba_messages::OrderType::Market,
            25.0,
            None,
            honba_messages::TimeInForce::Day,
            UnixNanos::new(0),
            UnixNanos::new(0),
        )
        .with_status(honba_messages::OrderStatus::Submitted),
    );
    state.with_trading_state(TradingState::Reducing)
}

#[tokio::test]
async fn position_from_ledger_drives_reduce_only() {
    // The ledger shows a long 25: a sell of 25 reduces it and passes ...
    let app = api_router_with(reducing_state_with_long_25());
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "sell", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // ... a buy would add exposure and is refused with the derived position ...
    let (status, body) = call(
        api_router_with(reducing_state_with_long_25()),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_reduce_only_violation"));

    // ... and a sell larger than the position would flip it, so it is refused.
    let (status, _) = call(
        api_router_with(reducing_state_with_long_25()),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "sell", 50.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn flat_ledger_refuses_everything_when_reducing() {
    let app = api_router_with(state().with_trading_state(TradingState::Reducing));
    let (status, _) = call(
        app,
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "sell", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// Seeds one submitted market order of `qty` on `X.NSE` with the given side.
fn seed(state: &AppState, id: &str, side: honba_messages::OrderSide, qty: f64) {
    state.orders.record(
        honba_messages::Order::new(
            honba_messages::OrderId::new(id),
            instrument_id(),
            side,
            honba_messages::OrderType::Market,
            qty,
            None,
            honba_messages::TimeInForce::Day,
            UnixNanos::new(0),
            UnixNanos::new(0),
        )
        .with_status(honba_messages::OrderStatus::Submitted),
    );
}

/// The ledger rows other than the seeded ones.
fn close_orders(state: &AppState) -> Vec<honba_messages::Order> {
    state
        .orders
        .list()
        .into_iter()
        .filter(|o| !o.order_id().as_str().starts_with("seed"))
        .collect()
}

#[tokio::test]
async fn close_of_a_long_position_sells_the_full_quantity() {
    let st = state();
    seed(&st, "seed-1", honba_messages::OrderSide::Buy, 50.0);
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["status"], json!("closing"));
    let rows = close_orders(&st);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].side(), honba_messages::OrderSide::Sell);
    assert_eq!(rows[0].quantity(), 50.0);
    assert_eq!(rows[0].order_type(), honba_messages::OrderType::Market);
    assert_eq!(st.orders.position(&instrument_id()), 0.0);
}

#[tokio::test]
async fn close_of_a_short_position_buys_the_full_quantity() {
    let st = state();
    seed(&st, "seed-1", honba_messages::OrderSide::Sell, 25.0);
    let (status, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = close_orders(&st);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].side(), honba_messages::OrderSide::Buy);
    assert_eq!(rows[0].quantity(), 25.0);
    assert_eq!(st.orders.position(&instrument_id()), 0.0);
}

#[tokio::test]
async fn close_of_a_flat_ledger_generates_no_orders() {
    let st = state();
    seed(&st, "seed-1", honba_messages::OrderSide::Buy, 25.0);
    seed(&st, "seed-2", honba_messages::OrderSide::Sell, 25.0);
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("closing"));
    assert!(close_orders(&st).is_empty());
    assert_eq!(st.orders.list().len(), 2);
}

#[tokio::test]
async fn close_passes_the_reduce_only_gate_when_reducing() {
    let st = reducing_state_with_long_25();
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = close_orders(&st);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].side(), honba_messages::OrderSide::Sell);
    assert_eq!(rows[0].quantity(), 25.0);
}

#[tokio::test]
async fn close_with_a_position_is_refused_when_halted() {
    let st = state().with_trading_state(TradingState::Halted);
    seed(&st, "seed-1", honba_messages::OrderSide::Buy, 25.0);
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("risk_trading_halted"));
    assert!(close_orders(&st).is_empty());
}

#[tokio::test]
async fn close_goes_through_the_approval_queue_when_enabled() {
    let st = state().with_approval_queue(10);
    seed(&st, "seed-1", honba_messages::OrderSide::Buy, 25.0);
    let app = api_router_with(st.clone());
    let (status, body) = call(app.clone(), Method::POST, "/positions/close", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Held, not yet in the ledger.
    assert!(close_orders(&st).is_empty());
    let (_, queue) = call(app.clone(), Method::GET, "/approval/queue", None).await;
    assert_eq!(queue["data"]["queue"].as_array().unwrap().len(), 1);
    let (status, _) = call(app, Method::POST, "/approval/approve", None).await;
    assert_eq!(status, StatusCode::OK);
    let rows = close_orders(&st);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].side(), honba_messages::OrderSide::Sell);
    assert_eq!(rows[0].quantity(), 25.0);
}

/// Price collar (ADR 0018 addendum): a pumped bar sets the reference, a wide live quote
/// sets the touch, and a market order whose touch is outside the collar is refused.
mod collar {
    use super::*;
    use honba_messages::{Bar, BarAggregation, BarSpecification, BarType, PriceType, QuoteTick};
    use honba_risk::RiskLimits;

    fn limits(bps: u32) -> RiskLimits {
        RiskLimits {
            price_collar_bps: Some(bps),
            ..Default::default()
        }
    }

    fn pumped(bps: Option<u32>, bid: f64, ask: f64) -> AppState {
        let mut st = state();
        if let Some(bps) = bps {
            st = st.with_risk_limits(limits(bps));
        }
        let t = UnixNanos::from_u64(2);
        let bt = BarType::new(
            instrument_id(),
            BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
        );
        let live = st.live_quotes().expect("live store").clone();
        live.put_bar(&Bar::new(bt, 100.0, 100.0, 100.0, 100.0, 10.0, t, t));
        let t3 = UnixNanos::from_u64(3);
        live.put_quote(QuoteTick::new(instrument_id(), bid, ask, 0.0, 0.0, t3, t3));
        st
    }

    async fn place(st: AppState, side: &str) -> (StatusCode, Value) {
        call(
            api_router_with(st),
            Method::POST,
            "/orders",
            Some(order("X", "NSE", side, 25.0, None)),
        )
        .await
    }

    #[tokio::test]
    async fn off_by_default_a_wide_quote_is_accepted() {
        let (status, _) = place(pumped(None, 90.0, 110.0), "buy").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn market_buy_beyond_the_collar_is_refused_on_the_ask() {
        // mid = 100 (reference), ask 103 -> 300 bps > 200.
        let (status, body) = place(pumped(Some(200), 97.0, 103.0), "buy").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], json!("risk_price_collar"));
        assert_eq!(body["error"]["context"]["rule"], json!("price_collar"));
        assert_eq!(body["error"]["context"]["expected_price"], json!(103.0));
        assert_eq!(body["error"]["context"]["collar_bps"], json!(200));
    }

    #[tokio::test]
    async fn market_sell_beyond_the_collar_is_refused_on_the_bid() {
        let (status, body) = place(pumped(Some(200), 97.0, 103.0), "sell").await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], json!("risk_price_collar"));
        assert_eq!(body["error"]["context"]["expected_price"], json!(97.0));
    }

    #[tokio::test]
    async fn within_the_collar_and_limit_orders_pass() {
        let (status, _) = place(pumped(Some(400), 97.0, 103.0), "buy").await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(
            api_router_with(pumped(Some(200), 97.0, 103.0)),
            Method::POST,
            "/orders",
            Some(order("X", "NSE", "buy", 25.0, Some(100.0))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}

/// A risk refusal with a caller key is a 422 *and* a `rejected` ledger row (ADR 0030): the
/// row names the refusal, and a repeat of the key answers with the same refusal, adding nothing.
#[tokio::test]
async fn a_keyed_risk_refusal_is_a_rejected_row_and_a_repeat_returns_the_same_422() {
    let app = api_router_with(state());
    let mut refused = order("X", "NSE", "buy", 30.0, Some(100.0));
    refused["client_order_id"] = json!("refused-1");
    let (first_status, first) =
        call(app.clone(), Method::POST, "/orders", Some(refused.clone())).await;
    assert_eq!(first_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(first["error"]["code"], json!("risk_lot_multiple_violation"));

    let (status, body) = call(app.clone(), Method::GET, "/orders", None).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["data"]["orders"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["order_id"], json!("refused-1"));
    assert_eq!(rows[0]["status"], json!("rejected"));
    assert_eq!(
        rows[0]["reject_reason"]["code"],
        json!("risk_lot_multiple_violation")
    );
    assert!(rows[0]["reject_reason"]["message"]
        .as_str()
        .unwrap()
        .contains("lot_multiple"));

    // The repeat is the same refusal (status, code, context), not a 200 row and not a new row.
    let (again_status, again) = call(app.clone(), Method::POST, "/orders", Some(refused)).await;
    assert_eq!(again_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(again["error"], first["error"]);
    let (_, body) = call(app, Method::GET, "/orders", None).await;
    assert_eq!(body["data"]["orders"].as_array().unwrap().len(), 1);
}

/// Without a `client_order_id` the route key is the shared placeholder `rest`, so recording a
/// refusal under it would poison every later keyless order: such a refusal stays unrecorded.
#[tokio::test]
async fn a_keyless_risk_refusal_leaves_no_row_and_does_not_poison_later_orders() {
    let app = api_router_with(state());
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 30.0, Some(100.0))),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (_, body) = call(app.clone(), Method::GET, "/orders", None).await;
    assert!(body["data"]["orders"].as_array().unwrap().is_empty());

    let (status, body) = call(
        app,
        Method::POST,
        "/orders",
        Some(order("X", "NSE", "buy", 25.0, None)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], json!("submitted"));
}

/// A refused order moves no exposure: the rejected row is outside `OrderLedger::position`.
#[tokio::test]
async fn a_refused_row_adds_no_exposure() {
    let st = state();
    let app = api_router_with(st.clone());
    let mut refused = order("X", "NSE", "buy", 30.0, Some(100.0));
    refused["client_order_id"] = json!("refused-2");
    call(app, Method::POST, "/orders", Some(refused)).await;
    assert_eq!(st.orders.position(&instrument_id()), 0.0);
}

/// `POST /positions/close` closes what a cancelled partial actually filled (it was exposure
/// the old full-quantity rule excluded once the row was cancelled).
#[tokio::test]
async fn close_covers_the_filled_part_of_a_cancelled_partial() {
    let st = state();
    seed(&st, "seed-1", honba_messages::OrderSide::Buy, 75.0);
    assert!(st.orders.apply_fill(
        "seed-1",
        25.0,
        100.0,
        25.0,
        UnixNanos::new(1),
        honba_messages::OrderStatus::PartiallyFilled
    ));
    st.orders.cancel("seed-1");
    assert_eq!(st.orders.position(&instrument_id()), 25.0);
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = close_orders(&st);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].side(), honba_messages::OrderSide::Sell);
    assert_eq!(rows[0].quantity(), 25.0);
}
