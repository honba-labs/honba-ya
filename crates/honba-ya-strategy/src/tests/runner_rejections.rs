//! Unit tests for the runner's rejection queue (cross-language vectors: `tests/order_rejections.rs`).

use honba_engine::Handler;
use honba_messages::UnixNanos;
use honba_sim::{BarFillEngine, Behavior, ScriptedExecution};
use honba_testing::VecFeed;
use honba_testing::fixtures::any_instrument;

use crate::{BuyAndHold, StrategyContext, StrategyRunner};

fn bar(r: &mut impl Handler, ts: u64) {
    let bar = VecFeed::bar("X", 10.0, ts).event().clone();
    r.on_event(&bar, UnixNanos::from_u64(ts)).unwrap();
}

fn first_order_id() -> String {
    format!(
        "{}-0",
        crate::Strategy::name(&BuyAndHold::new(any_instrument(), 1.0))
    )
}

#[test]
fn an_engine_that_never_rejects_records_nothing() {
    let mut exec = BarFillEngine::new();
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), 1.0), exec.clone());
    r.cancel(&first_order_id()).unwrap();
    bar(&mut exec, 1); // the engine observes the price before the runner acts
    bar(&mut r, 1);
    assert_eq!(r.fills().len(), 1);
    assert!(r.order_rejections().is_empty());
}

#[test]
fn a_rejection_is_recorded_and_releases_the_instrument() {
    let exec =
        ScriptedExecution::new(10.0).with(first_order_id(), Behavior::reject("insufficient_funds"));
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), 1.0), exec);
    bar(&mut r, 1);
    assert_eq!(r.order_rejections().len(), 1);
    assert_eq!(r.order_rejections()[0].reason, "insufficient_funds");
    assert!(!r.order_rejections()[0].is_cancelled());
    assert!(!r.context().busy(&any_instrument()));
}

#[test]
fn cancel_books_the_remainder_at_once() {
    let id = first_order_id();
    let exec = ScriptedExecution::new(10.0).with(id.clone(), Behavior::Hold);
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), 1.0), exec);
    bar(&mut r, 1);
    assert!(r.context().busy(&any_instrument()));
    assert!(r.order_rejections().is_empty());

    r.cancel(&id).unwrap();
    assert!(!r.context().busy(&any_instrument()));
    let got = &r.order_rejections()[0];
    assert!(got.is_cancelled());
    assert_eq!(got.reason, "cancelled");
    assert_eq!(got.quantity, 1.0);
}

#[test]
fn cancelling_twice_releases_once_and_a_later_bar_does_not_resurrect_pending() {
    let id = first_order_id();
    let exec = ScriptedExecution::new(10.0).with(id.clone(), Behavior::Hold);
    let mut r = StrategyRunner::new(BuyAndHold::new(any_instrument(), 1.0), exec);
    bar(&mut r, 1);
    r.cancel(&id).unwrap();
    r.cancel(&id).unwrap();
    bar(&mut r, 2);
    assert_eq!(r.order_rejections().len(), 1);
    assert!(!r.context().busy(&any_instrument()));
    assert!(r.fills().is_empty());
}
