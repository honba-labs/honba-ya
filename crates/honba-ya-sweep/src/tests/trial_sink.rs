//! The trial's paper sink drains one ordered event stream and tees its fills (ADR 0019).

use std::sync::{Arc, Mutex};

use honba_engine::{ExecutionEngine, Handler};
use honba_entities::ExecutionEvent;
use honba_messages::{Order, OrderId, OrderSide, OrderType, TimeInForce, UnixNanos};
use honba_testing::VecFeed;

use super::any_instrument;
use crate::trial::TrialSink;

fn sink_with_one_fill() -> (TrialSink, Arc<Mutex<Vec<honba_entities::Trade>>>) {
    let tape = Arc::new(Mutex::new(Vec::new()));
    let mut sink = TrialSink::new(Arc::clone(&tape));
    let bar = VecFeed::bar("X", 10.0, 1).event().clone();
    sink.on_event(&bar, UnixNanos::from_u64(1)).unwrap();
    sink.submit(Order::new(
        OrderId::new("o-0"),
        any_instrument(),
        OrderSide::Buy,
        OrderType::Market,
        3.0,
        None,
        TimeInForce::Day,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    ))
    .unwrap();
    (sink, tape)
}

#[test]
fn the_sink_is_a_native_event_engine() {
    let (sink, _) = sink_with_one_fill();
    assert!(sink.native_events());
}

#[test]
fn drain_events_tees_fills_into_the_tape() {
    let (mut sink, tape) = sink_with_one_fill();
    let events = sink.drain_events().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ExecutionEvent::Fill { complete: true, .. }]
    ));
    assert_eq!(tape.lock().unwrap().len(), 1);
    assert!(sink.drain_events().unwrap().is_empty());
}
