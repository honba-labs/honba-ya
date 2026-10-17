//! Order lifecycle through the real flow (ADR 0019 (b)): handler -> `Engine::submit`
//! -> `ScriptedExecution` -> one ordered event drain -> wire events back to handlers.

use std::sync::{Arc, Mutex};

use honba_engine::{AuditKind, Engine, EngineOutput, Handler, Result};
use honba_entities::ExecutionEvent;
use honba_messages::{
    Event, Exchange, InstrumentId, Message, Order, OrderId, OrderSide, OrderStatus, OrderType,
    QuoteTick, TimeInForce, UnixNanos,
};
use honba_sim::{Behavior, ScriptedExecution, VenueAction};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            instrument(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(t),
            ts(t),
        )),
        ts(t),
    )
}

fn buy(id: &str, qty: f64, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        instrument(),
        OrderSide::Buy,
        OrderType::Limit,
        qty,
        Some(10.0),
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

/// A strategy stand-in: the scripted output at each quote's ts, and a log
/// of the lifecycle events it is shown, as `(type, quantity)` pairs.
#[derive(Clone)]
struct Strategy {
    steps: Arc<Mutex<Vec<(u64, EngineOutput)>>>,
    seen: Arc<Mutex<Vec<(String, f64)>>>,
}

impl Strategy {
    fn new(steps: Vec<(u64, EngineOutput)>) -> Self {
        Self {
            steps: Arc::new(Mutex::new(steps)),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn seen(&self) -> Vec<(String, f64)> {
        self.seen.lock().unwrap().clone()
    }
}

impl Handler for Strategy {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        let entry = match event {
            Event::Quote(_) => {
                let t = event.ts_event().as_u64();
                let mut steps = self.steps.lock().unwrap();
                return Ok(match steps.iter().position(|(at, _)| *at == t) {
                    Some(i) => steps.remove(i).1,
                    None => EngineOutput::None,
                });
            }
            Event::Order(o) => ("order", o.quantity()),
            Event::OrderAccepted { .. } => ("order_accepted", 0.0),
            Event::OrderPartiallyFilled { cum_qty, .. } => ("order_partially_filled", *cum_qty),
            Event::OrderFilled { last_qty, .. } => ("order_filled", *last_qty),
            Event::OrderRejected { .. } => ("order_rejected", 0.0),
            Event::OrderCancelRequested { .. } => ("order_cancel_requested", 0.0),
            Event::OrderCancelled { .. } => ("order_cancelled", 0.0),
            Event::OrderExpired { .. } => ("order_expired", 0.0),
            _ => return Ok(EngineOutput::None),
        };
        self.seen
            .lock()
            .unwrap()
            .push((entry.0.to_string(), entry.1));
        Ok(EngineOutput::None)
    }
}

fn seen(pairs: &[(&str, f64)]) -> Vec<(String, f64)> {
    pairs.iter().map(|(t, q)| (t.to_string(), *q)).collect()
}

/// Released quantity per drained event, in drain order.
fn released(events: &[ExecutionEvent]) -> Vec<(String, f64)> {
    events
        .iter()
        .filter_map(|e| match e {
            ExecutionEvent::Rejected { quantity, .. } => Some(("rejected".into(), *quantity)),
            ExecutionEvent::Cancelled { quantity, .. } => Some(("cancelled".into(), *quantity)),
            ExecutionEvent::Expired { quantity, .. } => Some(("expired".into(), *quantity)),
            _ => None,
        })
        .collect()
}

fn no_illegal_transitions(engine: &Engine) {
    let bad: Vec<_> = engine
        .audit()
        .iter()
        .filter(|r| matches!(r.kind, AuditKind::IllegalTransition { .. }))
        .collect();
    assert!(bad.is_empty(), "illegal transitions: {bad:?}");
}

/// An engine hosting `strategy` against `venue`, started.
fn engine(venue: &ScriptedExecution, strategy: &Strategy) -> Engine {
    let mut engine = Engine::new();
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(strategy.clone());
    engine.start().unwrap();
    engine
}

fn step(engine: &mut Engine, t: u64) {
    engine.inject(quote(t));
    while engine.pump().unwrap() {}
}

#[test]
fn submit_then_partial_then_fill() {
    let venue = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    let strategy = Strategy::new(vec![(1, EngineOutput::Orders(vec![buy("O-1", 3.0, 1)]))]);
    let mut engine = engine(&venue, &strategy);
    step(&mut engine, 1);
    venue
        .venue("O-1", VenueAction::Fill { quantity: 1.0 }, ts(2))
        .unwrap();
    step(&mut engine, 2);
    venue
        .venue("O-1", VenueAction::Fill { quantity: 2.0 }, ts(3))
        .unwrap();
    step(&mut engine, 3);
    engine.finish().unwrap();

    assert_eq!(
        strategy.seen(),
        seen(&[
            ("order", 3.0),
            ("order_partially_filled", 1.0),
            ("order_filled", 2.0)
        ])
    );
    assert_eq!(
        engine.order("O-1").unwrap().state.status,
        OrderStatus::Filled
    );
    assert_eq!(engine.position(&instrument()), 3.0);
    no_illegal_transitions(&engine);
}

#[test]
fn venue_reject_after_partial_releases_the_remainder() {
    let venue = ScriptedExecution::new(10.0).with(
        "O-1",
        Behavior::Script(vec![
            VenueAction::Fill { quantity: 1.0 },
            VenueAction::Reject {
                reason: "rms_margin".into(),
            },
        ]),
    );
    let strategy = Strategy::new(vec![(1, EngineOutput::Orders(vec![buy("O-1", 3.0, 1)]))]);
    let mut engine = engine(&venue, &strategy);
    step(&mut engine, 1);
    engine.finish().unwrap();

    assert_eq!(
        strategy.seen(),
        seen(&[
            ("order", 3.0),
            ("order_partially_filled", 1.0),
            ("order_rejected", 0.0)
        ])
    );
    let drained = engine.drain_events().unwrap();
    assert_eq!(released(&drained), seen(&[("rejected", 2.0)]));
    assert!(drained.iter().any(|e| matches!(
        e,
        ExecutionEvent::Rejected { reason, .. } if reason == "rms_margin"
    )));
    assert_eq!(
        engine.order("O-1").unwrap().state.status,
        OrderStatus::Rejected
    );
    no_illegal_transitions(&engine);
}

#[test]
fn cancel_race_fill_enqueued_first_wins() {
    let venue = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    let strategy = Strategy::new(vec![
        (1, EngineOutput::Orders(vec![buy("O-1", 3.0, 1)])),
        (2, EngineOutput::Cancels(vec![OrderId::new("O-1")])),
    ]);
    let mut engine = engine(&venue, &strategy);
    step(&mut engine, 1);
    // The venue fills at ts 2 before the strategy's cancel at ts 2 is processed.
    venue
        .venue("O-1", VenueAction::Fill { quantity: 3.0 }, ts(2))
        .unwrap();
    step(&mut engine, 2);
    engine.finish().unwrap();

    assert_eq!(
        strategy.seen(),
        seen(&[("order", 3.0), ("order_filled", 3.0)]),
        "a fill that beats the cancel wins; the cancel is a no-op"
    );
    assert_eq!(
        engine.order("O-1").unwrap().state.status,
        OrderStatus::Filled
    );
    no_illegal_transitions(&engine);
}

#[test]
fn cancel_race_cancel_enqueued_first_wins() {
    let venue = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    let strategy = Strategy::new(vec![
        (1, EngineOutput::Orders(vec![buy("O-1", 3.0, 1)])),
        (2, EngineOutput::Cancels(vec![OrderId::new("O-1")])),
    ]);
    let mut engine = engine(&venue, &strategy);
    step(&mut engine, 1);
    step(&mut engine, 2);
    // The fill at the same ts comes too late: the venue no longer holds the order.
    assert!(venue
        .venue("O-1", VenueAction::Fill { quantity: 3.0 }, ts(2))
        .is_err());
    engine.finish().unwrap();

    assert_eq!(
        strategy.seen(),
        seen(&[
            ("order", 3.0),
            ("order_cancel_requested", 0.0),
            ("order_cancelled", 0.0)
        ])
    );
    let drained = engine.drain_events().unwrap();
    assert_eq!(released(&drained), seen(&[("cancelled", 3.0)]));
    no_illegal_transitions(&engine);
}

#[test]
fn scripted_expiry_releases_the_unfilled_remainder() {
    let venue = ScriptedExecution::new(10.0)
        .with(
            "O-1",
            Behavior::Script(vec![VenueAction::Fill { quantity: 1.0 }]),
        )
        .with("O-2", Behavior::Expire);
    let strategy = Strategy::new(vec![(
        1,
        EngineOutput::Orders(vec![buy("O-1", 3.0, 1), buy("O-2", 2.0, 1)]),
    )]);
    let mut engine = engine(&venue, &strategy);
    step(&mut engine, 1);
    // Session end: the venue expires the working remainder.
    venue.venue("O-1", VenueAction::Expire, ts(5)).unwrap();
    step(&mut engine, 5);
    engine.finish().unwrap();

    assert_eq!(
        strategy.seen(),
        seen(&[
            ("order", 3.0),
            ("order_partially_filled", 1.0),
            ("order", 2.0),
            ("order_expired", 0.0),
            ("order_expired", 0.0)
        ])
    );
    let drained = engine.drain_events().unwrap();
    assert_eq!(
        released(&drained),
        seen(&[("expired", 2.0), ("expired", 2.0)])
    );
    assert_eq!(
        engine.order("O-1").unwrap().state.status,
        OrderStatus::Expired
    );
    assert_eq!(
        engine.order("O-2").unwrap().state.status,
        OrderStatus::Expired
    );
    assert!(venue.working_orders().is_empty());
    no_illegal_transitions(&engine);
}
