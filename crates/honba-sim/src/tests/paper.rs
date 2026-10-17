//! Unit tests for `crate::paper`.

use honba_engine::ExecutionEngine;
use honba_entities::{ExecutionEvent, Trade};
use honba_messages::{OrderSide, OrderStatus, UnixNanos};

use super::{any_instrument, limit, market};
use crate::{OrderLedger, PaperExecution};

#[test]
fn market_order_fills_at_the_configured_price() {
    let mut exec = PaperExecution::new(250.0);
    exec.submit(market("O-1", OrderSide::Buy, 4.0, 99)).unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    let f = &fills[0];
    assert_eq!(f.order_id().as_str(), "O-1");
    assert_eq!(f.instrument_id(), &any_instrument());
    assert_eq!(f.side(), OrderSide::Buy);
    assert_eq!(f.quantity(), 4.0);
    assert_eq!(f.price(), 250.0);
    assert_eq!(f.notional(), 1000.0);
}

#[test]
fn sell_order_keeps_its_side() {
    let mut exec = PaperExecution::new(10.0);
    exec.submit(market("O-1", OrderSide::Sell, 1.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap()[0].side(), OrderSide::Sell);
}

#[test]
fn limit_order_fills_at_the_configured_price_not_its_limit() {
    let mut exec = PaperExecution::new(100.0);
    exec.submit(limit("O-1", OrderSide::Buy, 1.0, 120.0, 1))
        .unwrap();
    let fills = exec.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price(), 100.0);
}

#[test]
fn set_price_applies_to_later_submissions_only() {
    let mut exec = PaperExecution::new(100.0);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    exec.set_price(110.0);
    assert_eq!(exec.price(), 110.0);
    exec.submit(market("O-2", OrderSide::Sell, 1.0, 2)).unwrap();
    let prices: Vec<f64> = exec
        .drain_fills()
        .unwrap()
        .iter()
        .map(Trade::price)
        .collect();
    assert_eq!(prices, [100.0, 110.0]);
}

#[test]
fn never_rejects_and_cancel_is_a_no_op() {
    let mut exec = PaperExecution::new(1.0);
    for i in 0..3 {
        assert!(exec
            .submit(market(&format!("O-{i}"), OrderSide::Buy, 1.0, 1))
            .is_ok());
    }
    assert!(exec.cancel("O-0", UnixNanos::from_u64(1)).is_ok());
    assert_eq!(exec.drain_fills().unwrap().len(), 3);
}

#[test]
fn fill_timestamps_count_up_from_one_independent_of_order_time() {
    let mut exec = PaperExecution::new(1.0);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 500))
        .unwrap();
    exec.submit(market("O-2", OrderSide::Buy, 1.0, 10)).unwrap();
    let ts: Vec<UnixNanos> = exec
        .drain_fills()
        .unwrap()
        .iter()
        .map(Trade::ts_event)
        .collect();
    assert_eq!(ts, [UnixNanos::from_u64(1), UnixNanos::from_u64(2)]);
}

#[test]
fn drain_empties_the_fill_buffer() {
    let mut exec = PaperExecution::new(1.0);
    exec.submit(market("O-1", OrderSide::Buy, 1.0, 1)).unwrap();
    assert_eq!(exec.drain_fills().unwrap().len(), 1);
    assert!(exec.drain_fills().unwrap().is_empty());
}

#[test]
fn identical_inputs_give_identical_fills() {
    fn run() -> Vec<Trade> {
        let mut exec = PaperExecution::new(100.0);
        for i in 0..5u64 {
            exec.set_price(100.0 + i as f64);
            let side = if i % 2 == 0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            exec.submit(market(&format!("O-{i}"), side, 2.0, i))
                .unwrap();
        }
        exec.drain_fills().unwrap()
    }
    assert_eq!(run(), run());
}

#[test]
fn ledger_records_and_overwrites_statuses() {
    let mut ledger = OrderLedger::new();
    assert_eq!(ledger.status("O-1"), None);
    ledger.set("O-1", OrderStatus::Submitted);
    ledger.set("O-1", OrderStatus::Filled);
    ledger.set("O-2", OrderStatus::Rejected);
    assert_eq!(ledger.status("O-1"), Some(OrderStatus::Filled));
    assert_eq!(ledger.status("O-2"), Some(OrderStatus::Rejected));
}

#[test]
fn with_ack_emits_accepted_before_fill_for_limit_order() {
    let mut exec = PaperExecution::new(100.0).with_ack(true);
    assert!(exec.ack());
    exec.submit(limit("O-1", OrderSide::Buy, 5.0, 100.0, 1))
        .unwrap();
    let events = exec.drain_events().unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], ExecutionEvent::Accepted { .. }));
    assert!(matches!(events[1], ExecutionEvent::Fill { .. }));
}
