//! The REST order routes against the real embedded `NextOpenSim` (ADR 0016, E3-S7).
//!
//! # Rebuilt on this migration, not a rename
//!
//! This used to inject a crate-local `ExecutionVenue` trait object via
//! `AppState::with_execution_gateway` — a `Fake` test double recording `submit`/`cancel` calls,
//! and an `L2Venue` wrapping `honba_sim::L2Sim` — so the order-route plumbing could be tested
//! independently of whatever venue was behind it, including synthetic partial fills and
//! depth-level matching. `exec.rs`'s module doc explains why none of that survives: there is no
//! generic venue trait any more (only one concrete simulator, `NextOpenSim`, built directly on
//! `honba_core` types with no injectable seam), and `honba_sim` is gone from the workspace with
//! no L2 successor.
//!
//! `tests/orders_exec.rs` already covers the straightforward submit/fill/reject paths through
//! the real simulator, so this file keeps only what that one doesn't: that a cancel actually
//! withdraws the order from the simulator's own working set (not just flips the ledger's status
//! label), and that `/positions/close` reaches the simulator with a second, real order.
//!
//! Dropped outright, with no replacement, because `NextOpenSim` has nothing equivalent to
//! exercise:
//! - `gateway_events_update_ledger_status`,
//!   `partial_fills_update_filled_qty_and_the_average_incrementally`, and
//!   `a_rejected_remainder_after_a_partial_fill_keeps_the_fill_and_records_why` drove the `Fake`
//!   venue with hand-built `ExecutionEvent`s (arbitrary partial fills, arbitrary rejection
//!   strings). `NextOpenSim` only ever fills a market order in full, at the next bar's open, and
//!   only ever rejects with its own two fixed reasons (`unsupported_order_type`, `no_position`);
//!   there is no way to make it report a partial fill or an arbitrary rejection, so nothing
//!   equivalent can be exercised.
//! - `venue_rejections_carry_the_code_when_the_reason_is_one_else_order_rejected` tested the old
//!   async gateway's string-reason-to-`ErrorCode` fallback mapping. `NextOpenSim` sets its own
//!   `ErrorCode` directly when it rejects (see `exec.rs::submit`); there is no string-to-code
//!   mapping step left anywhere on this path.
//! - `l2sim_partial_fills_report_cumulative_qty_and_the_weighted_average` tested
//!   `honba_sim::L2Sim`'s depth-level partial fills directly. `honba_sim` is gone with no
//!   successor.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use honba_api_rest::{AppState, api_router_with};
use honba_core::{Currency, InstrumentKind};
use honba_feed::wire::{BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_testing::fixtures::{any_instrument, flat_bar};
use serde_json::{Value, json};
use tower::util::ServiceExt;

fn state() -> AppState {
    let id = any_instrument();
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

async fn call(st: &AppState, method: Method, uri: &str, body: Option<Value>) -> StatusCode {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    api_router_with(st.clone())
        .oneshot(builder.body(body).unwrap())
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

fn status_of(st: &AppState, id: &str) -> String {
    serde_json::to_value(st.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn delete_order_withdraws_it_from_the_simulators_working_set() {
    let st = state();
    assert_eq!(
        call(&st, Method::POST, "/orders", Some(order("buy", 25.0, "o1"))).await,
        StatusCode::OK
    );
    assert_eq!(status_of(&st, "o1"), "submitted");

    assert_eq!(call(&st, Method::DELETE, "/orders/o1", None).await, StatusCode::OK);
    assert_eq!(status_of(&st, "o1"), "cancelled");

    // If the cancel only flipped the ledger's label and left the order working in the
    // simulator, this bar would fill it. It must not: the row stays cancelled (terminal, so
    // `apply_fill` is a no-op) and the ledger's position stays flat.
    st.advance_market(&[flat_bar("X", 101.0, 1_000)]).unwrap();
    assert_eq!(status_of(&st, "o1"), "cancelled");
    assert!(st.orders.positions().is_empty());
}

#[tokio::test]
async fn close_positions_reaches_the_simulator_with_a_second_real_order() {
    let st = state();
    call(&st, Method::POST, "/orders", Some(order("buy", 25.0, "o1"))).await;
    st.advance_market(&[flat_bar("X", 101.0, 1_000)]).unwrap();
    assert_eq!(status_of(&st, "o1"), "filled");

    assert_eq!(
        call(&st, Method::POST, "/positions/close", None).await,
        StatusCode::OK
    );

    // The close order is submitted to the simulator at once (not merely queued): one more row,
    // already working.
    let rows = st.orders.list();
    assert_eq!(rows.len(), 2, "the closing order is recorded alongside o1");
    let closing_submitted = rows
        .iter()
        .any(|o| serde_json::to_value(o).unwrap()["status"] == "submitted");
    assert!(closing_submitted, "the closing order reached the simulator");

    // Once that order fills too, the ledger's position is flat again.
    st.advance_market(&[flat_bar("X", 103.0, 2_000)]).unwrap();
    assert!(st.orders.positions().is_empty());
    assert!(
        st.orders
            .list()
            .iter()
            .all(|o| serde_json::to_value(o).unwrap()["status"] == "filled")
    );
}
