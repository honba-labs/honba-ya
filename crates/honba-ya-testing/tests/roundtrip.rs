//! Sanity checks for the fixtures themselves.

use honba_engine::{Engine, ExecutionEngine};
use honba_messages::{
    Exchange, InstrumentId, Order, OrderId, OrderSide, OrderType, TimeInForce, UnixNanos,
};
use honba_sim::PaperExecution;
use honba_testing::{assert_close, assert_close_slice, Recorder, VecFeed};

fn order(id: &str, side: OrderSide, qty: f64) -> Order {
    Order::new(
        OrderId::new(id),
        InstrumentId::new("X", Exchange::new("TEST")),
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
}

#[test]
fn vec_feed_yields_messages_in_order() {
    let mut feed = VecFeed::new(vec![
        VecFeed::quote("X", 1.0, 2.0, 3),
        VecFeed::trade("X", 1.5, 10.0, 1),
        VecFeed::quote("X", 1.0, 2.0, 2),
    ]);

    let rec = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(rec.clone());
    engine.run(&mut feed).unwrap();

    assert_eq!(rec.timestamps(), vec![1, 2, 3]);
    assert!(rec.started());
    assert!(rec.stopped());
}

#[test]
fn recorder_captures_everything() {
    let mut feed = VecFeed::new(vec![
        VecFeed::quote("A", 1.0, 2.0, 1),
        VecFeed::quote("B", 1.0, 2.0, 2),
    ]);
    let rec = Recorder::new();
    let mut engine = Engine::new();
    engine.add_handler(rec.clone());
    engine.run(&mut feed).unwrap();
    assert_eq!(rec.len(), 2);
    assert!(!rec.is_empty());
}

#[test]
fn paper_fills_immediately() {
    let mut p = PaperExecution::new(100.0);
    p.submit(order("O-1", OrderSide::Buy, 10.0)).unwrap();
    let fills = p.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_close(fills[0].notional(), 1000.0, 1e-12);
    assert!(p.drain_fills().unwrap().is_empty(), "fills drained");
}

#[test]
fn paper_price_updates() {
    let mut p = PaperExecution::new(100.0);
    p.set_price(150.0);
    p.submit(order("O-1", OrderSide::Buy, 2.0)).unwrap();
    let fills = p.drain_fills().unwrap();
    assert_close(fills[0].price(), 150.0, 1e-12);
    assert_close(fills[0].notional(), 300.0, 1e-12);
}

#[test]
fn assert_helpers() {
    assert_close(1.0, 1.0 + 1e-13, 1e-12);
    assert_close_slice(&[1.0, 2.0, 3.0], &[1.0 + 1e-13, 2.0, 3.0], 1e-12);
}
