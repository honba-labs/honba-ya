//! Unit tests for the runner's single ordered event walk (ADR 0019 decision 4).

use honba_engine::Handler;
use honba_entities::ExecutionEvent;
use honba_messages::{OrderId, OrderSide, OrderStatus, UnixNanos};
use honba_sim::{Behavior, ScriptedExecution};
use honba_testing::VecFeed;

use super::any_instrument;
use crate::{BuyAndHold, StrategyContext, StrategyRunner};

fn bar(r: &mut impl Handler, ts: u64) {
    let b = VecFeed::bar("X", 10.0, ts).event().clone();
    r.on_event(&b, UnixNanos::from_u64(ts)).unwrap();
}

fn cancelled(id: &str, qty: f64) -> ExecutionEvent {
    ExecutionEvent::Cancelled {
        order_id: OrderId::new(id),
        instrument_id: any_instrument(),
        side: OrderSide::Buy,
        quantity: qty,
        venue_order_id: None,
        ts: UnixNanos::from_u64(2),
    }
}

fn held() -> (
    StrategyRunner<BuyAndHold, ScriptedExecution>,
    ScriptedExecution,
    String,
) {
    let strategy = BuyAndHold::new(any_instrument(), 5.0);
    let id = format!("{}-0", crate::Strategy::name(&strategy));
    let exec = ScriptedExecution::new(10.0).with(id.clone(), Behavior::Hold);
    (StrategyRunner::new(strategy, exec.clone()), exec, id)
}

#[test]
fn a_duplicate_cancelled_event_releases_once() {
    let (mut r, venue, id) = held();
    bar(&mut r, 1);
    venue.inject(cancelled(&id, 5.0));
    venue.inject(cancelled(&id, 5.0));
    bar(&mut r, 2);
    assert_eq!(r.order_rejections().len(), 1);
    assert_eq!(r.released_quantity(&any_instrument()), 5.0);
    assert_eq!(r.order_state(&id).unwrap().status, OrderStatus::Cancelled);
}

#[test]
fn a_cancel_after_the_order_filled_releases_nothing() {
    let strategy = BuyAndHold::new(any_instrument(), 5.0);
    let mut r = StrategyRunner::new(strategy, ScriptedExecution::new(10.0));
    bar(&mut r, 1);
    assert_eq!(
        r.order_state("buy_and_hold-0").unwrap().status,
        OrderStatus::Filled
    );
    r.cancel("buy_and_hold-0").unwrap();
    assert!(r.order_rejections().is_empty());
    assert_eq!(r.released_quantity(&any_instrument()), 0.0);
    assert!(!r.context().busy(&any_instrument()));
}

#[test]
fn cancel_requested_is_cleared_on_the_terminal_state() {
    let (mut r, _venue, id) = held();
    bar(&mut r, 1);
    r.cancel(&id).unwrap();
    let s = r.order_state(&id).unwrap();
    assert_eq!(s.status, OrderStatus::Cancelled);
    assert!(!s.cancel_requested, "cleared on the terminal state");
}
