//! Unit tests for `crate::scripted`.

use honba_engine::{ExecutionEngine, OrderRejection};
use honba_messages::OrderSide;

use super::{any_instrument, market};
use crate::{Behavior, ScriptedExecution};

#[test]
fn unscripted_orders_fill_in_full_at_the_price() {
    let mut exec = ScriptedExecution::new(10.0);
    exec.submit(market("O-1", OrderSide::Buy, 4.0, 5)).unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity(), 4.0);
    assert_eq!(fills[0].price(), 10.0);
    assert_eq!(fills[0].ts_event().as_u64(), 5);
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
fn a_rejected_order_reports_its_whole_quantity() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::reject("no_position"));
    exec.submit(market("O-1", OrderSide::Sell, 5.0, 3)).unwrap();
    assert!(exec.drain_fills().unwrap().is_empty());
    let rejections = exec.drain_rejections().unwrap();
    assert_eq!(rejections.len(), 1);
    let r = &rejections[0];
    assert_eq!(
        (r.order_id.as_str(), r.quantity, r.reason.as_str()),
        ("O-1", 5.0, "no_position")
    );
    assert_eq!(
        (r.side, r.is_cancelled(), r.ts.as_u64()),
        (OrderSide::Sell, false, 3)
    );
    assert_eq!(r.instrument_id, any_instrument());
}

#[test]
fn a_partial_fill_rejects_only_the_remainder() {
    let mut exec =
        ScriptedExecution::new(10.0).with("O-1", Behavior::partial(4.0, "insufficient_funds"));
    exec.submit(market("O-1", OrderSide::Buy, 10.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap()[0].quantity(), 4.0);
    assert_eq!(exec.drain_rejections().unwrap()[0].quantity, 6.0);
}

#[test]
fn drains_clear_the_queues() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::reject("x"));
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert_eq!(exec.drain_rejections().unwrap().len(), 1);
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
fn cancel_reports_the_working_remainder_once() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    exec.submit(market("O-1", OrderSide::Buy, 7.0, 2)).unwrap();
    assert_eq!(exec.working_orders(), vec!["O-1".to_string()]);
    assert!(exec.drain_rejections().unwrap().is_empty());

    exec.cancel("O-1", honba_messages::UnixNanos::from_u64(5))
        .unwrap();
    let got = exec.drain_rejections().unwrap();
    assert_eq!(
        got,
        vec![OrderRejection::cancelled(
            honba_messages::OrderId::new("O-1"),
            any_instrument(),
            OrderSide::Buy,
            7.0,
            honba_messages::UnixNanos::from_u64(5),
        )]
    );
    assert!(exec.working_orders().is_empty());

    exec.cancel("O-1", honba_messages::UnixNanos::from_u64(6))
        .unwrap();
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
fn cancelling_a_finished_or_unknown_order_is_a_no_op() {
    let mut exec = ScriptedExecution::new(10.0);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    let now = honba_messages::UnixNanos::from_u64(2);
    exec.cancel("O-1", now).unwrap();
    exec.cancel("nope", now).unwrap();
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
#[should_panic(expected = "partial fill must be positive and finite")]
fn scripting_a_non_positive_partial_fill_panics() {
    let _ = ScriptedExecution::new(10.0).with("O-1", Behavior::partial(0.0, "x"));
}

#[test]
#[should_panic(expected = "partial fill must be positive and finite")]
fn scripting_a_nan_partial_fill_panics() {
    let _ = ScriptedExecution::new(10.0).with("O-1", Behavior::partial(f64::NAN, "x"));
}

#[test]
fn a_partial_fill_of_the_whole_order_or_more_is_refused_at_submit() {
    for filled in [10.0, 12.0] {
        let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::partial(filled, "x"));
        let err = exec
            .submit(market("O-1", OrderSide::Buy, 10.0, 1))
            .unwrap_err();
        assert!(err.to_string().contains("must be below the order quantity"));
        assert!(exec.drain_fills().unwrap().is_empty());
        assert!(exec.drain_rejections().unwrap().is_empty());
    }
}

#[test]
fn a_later_script_entry_for_the_same_order_id_replaces_the_earlier_one() {
    let mut exec = ScriptedExecution::new(10.0)
        .with("O-1", Behavior::reject("first"))
        .with("O-1", Behavior::Hold);
    exec.submit(market("O-1", OrderSide::Buy, 3.0, 1)).unwrap();
    assert_eq!(exec.working_orders(), vec!["O-1".to_string()]);
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
fn resubmitting_an_order_id_applies_its_script_each_time() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::reject("no_position"));
    exec.submit(market("O-1", OrderSide::Buy, 3.0, 1)).unwrap();
    exec.submit(market("O-1", OrderSide::Buy, 2.0, 2)).unwrap();
    let got = exec.drain_rejections().unwrap();
    assert_eq!(
        got.iter().map(|r| r.quantity).collect::<Vec<_>>(),
        [3.0, 2.0]
    );
}

#[test]
fn an_order_without_a_side_is_refused_at_submit() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    let err = exec
        .submit(market("O-1", OrderSide::NoOrderSide, 1.0, 1))
        .unwrap_err();
    assert!(err.to_string().contains("side"), "{err}");
    assert!(exec.working_orders().is_empty());
    assert!(exec.drain_fills().unwrap().is_empty());
    assert!(exec.drain_rejections().unwrap().is_empty());
}

#[test]
fn a_duplicate_working_order_id_is_refused_at_submit() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    exec.submit(market("O-1", OrderSide::Buy, 7.0, 1)).unwrap();
    let err = exec
        .submit(market("O-1", OrderSide::Buy, 3.0, 2))
        .unwrap_err();
    assert!(err.to_string().contains("O-1"), "{err}");
    assert_eq!(exec.working_orders(), vec!["O-1".to_string()]);

    // The first order is untouched: one cancel reports its 7.0, once.
    exec.cancel("O-1", honba_messages::UnixNanos::from_u64(5))
        .unwrap();
    let got = exec.drain_rejections().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].quantity, 7.0);
}

#[test]
fn an_id_may_be_resubmitted_once_it_is_no_longer_working() {
    let mut exec = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    exec.submit(market("O-1", OrderSide::Buy, 7.0, 1)).unwrap();
    exec.cancel("O-1", honba_messages::UnixNanos::from_u64(2))
        .unwrap();
    exec.submit(market("O-1", OrderSide::Buy, 4.0, 3)).unwrap();
    exec.cancel("O-1", honba_messages::UnixNanos::from_u64(4))
        .unwrap();
    let quantities: Vec<f64> = exec
        .drain_rejections()
        .unwrap()
        .iter()
        .map(|r| r.quantity)
        .collect();
    assert_eq!(quantities, vec![7.0, 4.0]);
}
