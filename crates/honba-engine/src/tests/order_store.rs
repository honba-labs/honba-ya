//! Unit tests for the engine's order store and event translation (ADR 0019 (b)).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use honba_entities::{Currency, ExecutionEvent, Trade};
use honba_messages::{
    Event, Exchange, IllegalTransition, InstrumentId, Message, Order, OrderEventKind, OrderId,
    OrderSide, OrderStatus, OrderType, QuoteTick, TimeInForce, UnixNanos, VenueOrderId,
};

use super::any_instrument;

use crate::{
    AuditKind, Engine, EngineOutput, ExecutionEngine, Handler, LegacyDrains, OrderRejection,
    Result, TradingState,
};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn other_instrument() -> InstrumentId {
    InstrumentId::new("Y", Exchange::new("NSE"))
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            any_instrument(),
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

fn order_on(instrument: InstrumentId, id: &str, side: OrderSide, qty: f64, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        instrument,
        side,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

fn order(id: &str, side: OrderSide, qty: f64, t: u64) -> Order {
    order_on(any_instrument(), id, side, qty, t)
}

fn fill(id: &str, qty: f64, px: f64, cum: f64, complete: bool, t: u64) -> ExecutionEvent {
    fill_on(any_instrument(), id, qty, px, cum, complete, t)
}

fn fill_on(
    instrument: InstrumentId,
    id: &str,
    qty: f64,
    px: f64,
    cum: f64,
    complete: bool,
    t: u64,
) -> ExecutionEvent {
    ExecutionEvent::Fill {
        trade: Trade::new(
            OrderId::new(id),
            instrument,
            OrderSide::Buy,
            qty,
            px,
            Currency::Inr,
            ts(t),
            ts(t),
        ),
        cum_qty: cum,
        complete,
        venue_order_id: None,
    }
}

fn accepted(id: &str, qty: f64, venue: Option<&str>, t: u64) -> ExecutionEvent {
    ExecutionEvent::Accepted {
        order_id: OrderId::new(id),
        instrument_id: any_instrument(),
        side: OrderSide::Buy,
        quantity: qty,
        venue_order_id: venue.map(VenueOrderId::new),
        ts: ts(t),
    }
}

fn cancelled(id: &str, qty: f64, t: u64) -> ExecutionEvent {
    ExecutionEvent::Cancelled {
        order_id: OrderId::new(id),
        instrument_id: any_instrument(),
        side: OrderSide::Buy,
        quantity: qty,
        venue_order_id: None,
        ts: ts(t),
    }
}

fn rejected(id: &str, qty: f64, reason: &str, t: u64) -> ExecutionEvent {
    ExecutionEvent::Rejected {
        order_id: OrderId::new(id),
        instrument_id: any_instrument(),
        side: OrderSide::Buy,
        quantity: qty,
        reason: reason.to_string(),
        venue_order_id: None,
        ts: ts(t),
    }
}

/// A native-event gateway whose venue answers are scripted per order id.
#[derive(Clone, Default)]
struct Venue {
    on_submit: Arc<Mutex<HashMap<String, Vec<ExecutionEvent>>>>,
    on_cancel: Arc<Mutex<HashMap<String, Vec<ExecutionEvent>>>>,
    out: Arc<Mutex<Vec<ExecutionEvent>>>,
    submitted: Arc<Mutex<Vec<String>>>,
    cancelled: Arc<Mutex<Vec<String>>>,
    legacy: Arc<Mutex<LegacyDrains>>,
}

impl Venue {
    fn on_submit(&self, id: &str, events: Vec<ExecutionEvent>) -> &Self {
        self.on_submit
            .lock()
            .unwrap()
            .insert(id.to_string(), events);
        self
    }

    fn on_cancel(&self, id: &str, events: Vec<ExecutionEvent>) -> &Self {
        self.on_cancel
            .lock()
            .unwrap()
            .insert(id.to_string(), events);
        self
    }

    fn push(&self, event: ExecutionEvent) {
        self.out.lock().unwrap().push(event);
    }

    fn cancelled(&self) -> Vec<String> {
        self.cancelled.lock().unwrap().clone()
    }
}

impl ExecutionEngine for Venue {
    fn submit(&mut self, order: Order) -> Result<()> {
        let id = order.order_id().as_str().to_string();
        self.submitted.lock().unwrap().push(id.clone());
        let events = self.on_submit.lock().unwrap().remove(&id);
        self.out.lock().unwrap().extend(events.unwrap_or_default());
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, _now: UnixNanos) -> Result<()> {
        self.cancelled.lock().unwrap().push(order_id.to_string());
        let events = self.on_cancel.lock().unwrap().remove(order_id);
        self.out.lock().unwrap().extend(events.unwrap_or_default());
        Ok(())
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut *self.out.lock().unwrap()))
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        let events = self.drain_events()?;
        let mut legacy = self.legacy.lock().unwrap();
        legacy.absorb(events);
        Ok(legacy.take_fills())
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        let events = self.drain_events()?;
        let mut legacy = self.legacy.lock().unwrap();
        legacy.absorb(events);
        Ok(legacy.take_rejections())
    }

    fn native_events(&self) -> bool {
        true
    }
}

/// Emits the scripted output at the first event with each `ts_event`, and
/// records every order-lifecycle event it sees.
#[derive(Clone)]
struct Script {
    steps: Arc<Mutex<Vec<(u64, EngineOutput)>>>,
    seen: Arc<Mutex<Vec<Event>>>,
}

impl Script {
    fn new(steps: Vec<(u64, EngineOutput)>) -> Self {
        Self {
            steps: Arc::new(Mutex::new(steps)),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn lifecycle(&self) -> Vec<Event> {
        self.seen.lock().unwrap().clone()
    }
}

impl Handler for Script {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        if !matches!(event, Event::Quote(_)) {
            self.seen.lock().unwrap().push(event.clone());
            return Ok(EngineOutput::None);
        }
        let t = event.ts_event().as_u64();
        let mut steps = self.steps.lock().unwrap();
        match steps.iter().position(|(at, _)| *at == t) {
            Some(i) => Ok(steps.remove(i).1),
            None => Ok(EngineOutput::None),
        }
    }
}

fn engine_with(venue: Option<&Venue>, script: &Script, quotes: &[u64]) -> Engine {
    let mut engine = Engine::new();
    if let Some(v) = venue {
        engine.set_execution(Box::new(v.clone()));
    }
    engine.add_handler(script.clone());
    for t in quotes {
        engine.inject(quote(*t));
    }
    engine
}

fn kinds(events: &[ExecutionEvent]) -> Vec<OrderEventKind> {
    events.iter().map(|e| e.order_event().kind()).collect()
}

fn illegal(engine: &Engine) -> Vec<(String, IllegalTransition)> {
    engine
        .audit()
        .iter()
        .filter_map(|r| match &r.kind {
            AuditKind::IllegalTransition { order_id, error } => Some((order_id.clone(), *error)),
            _ => None,
        })
        .collect()
}

#[test]
fn acknowledge_translates_one_to_one() {
    let venue = Venue::default();
    venue.on_submit(
        "O-1",
        vec![
            accepted("O-1", 3.0, Some("V-1"), 5),
            fill("O-1", 1.0, 10.0, 1.0, false, 5),
            fill("O-1", 2.0, 11.0, 3.0, true, 5),
        ],
    );
    let o = order("O-1", OrderSide::Buy, 3.0, 5);
    let script = Script::new(vec![(5, EngineOutput::Orders(vec![o.clone()]))]);
    let mut engine = engine_with(Some(&venue), &script, &[5]);
    engine.finish().unwrap();

    assert_eq!(
        script.lifecycle(),
        vec![
            Event::Order(o.with_status(OrderStatus::Submitted)),
            Event::OrderAccepted {
                order_id: OrderId::new("O-1"),
                venue_order_id: Some(VenueOrderId::new("V-1")),
                ts_event: ts(5),
            },
            Event::OrderPartiallyFilled {
                order_id: OrderId::new("O-1"),
                last_qty: 1.0,
                last_px: 10.0,
                cum_qty: 1.0,
                ts_event: ts(5),
            },
            Event::OrderFilled {
                order_id: OrderId::new("O-1"),
                last_qty: 2.0,
                last_px: 11.0,
                ts_event: ts(5),
            },
        ]
    );
    // The submitter's `Submitted` precedes everything the gateway produced.
    let drained = engine.drain_events().unwrap();
    use OrderEventKind as K;
    assert_eq!(
        kinds(&drained),
        vec![K::Submitted, K::Accepted, K::Fill, K::Fill]
    );
    assert!(engine.drain_events().unwrap().is_empty());

    let tracked = engine.order("O-1").expect("tracked");
    assert_eq!(tracked.state.status, OrderStatus::Filled);
    assert_eq!(tracked.state.filled_qty, 3.0);
    assert_eq!(tracked.venue_order_id, Some(VenueOrderId::new("V-1")));
    assert_eq!(engine.position(&any_instrument()), 3.0);
    assert!(engine.audit().iter().any(|r| r.kind
        == AuditKind::OrderLifecycle {
            order_id: "O-1".into(),
            event: K::Accepted,
            ts_event: 5,
        }));
    assert!(illegal(&engine).is_empty());
}

#[test]
fn engine_drain_fills_shim_leaves_the_other_events_for_drain_events() {
    let venue = Venue::default();
    venue.on_submit("O-1", vec![fill("O-1", 2.0, 10.0, 2.0, true, 1)]);
    let script = Script::new(vec![(
        1,
        EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 2.0, 1)]),
    )]);
    let mut engine = engine_with(Some(&venue), &script, &[1]);
    engine.finish().unwrap();

    let fills = engine.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert!(engine.drain_fills().unwrap().is_empty());
    assert_eq!(
        kinds(&engine.drain_events().unwrap()),
        vec![OrderEventKind::Submitted]
    );
}

#[test]
fn pre_gate_refusal_is_rejected_event() {
    // No execution attached: checked first, `order_execution_unavailable`.
    let script = Script::new(vec![(
        1,
        EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 5.0, 1)]),
    )]);
    let mut engine = engine_with(None, &script, &[1]);
    engine.finish().unwrap();
    assert_eq!(
        script.lifecycle(),
        vec![Event::OrderRejected {
            order_id: OrderId::new("O-1"),
            reason: "order_execution_unavailable".into(),
            ts_event: ts(1),
        }]
    );
    assert_eq!(
        engine.drain_events().unwrap(),
        vec![ExecutionEvent::Rejected {
            order_id: OrderId::new("O-1"),
            instrument_id: any_instrument(),
            side: OrderSide::Buy,
            quantity: 5.0,
            reason: "order_execution_unavailable".into(),
            venue_order_id: None,
            ts: ts(1),
        }]
    );
    assert_eq!(
        engine.order("O-1").unwrap().state.status,
        OrderStatus::Rejected
    );
    assert!(engine.audit().iter().any(|r| r.kind
        == AuditKind::OrderRejected {
            order_id: "O-1".into(),
            reason: "order_execution_unavailable".into(),
        }));

    // Halted: `risk_trading_halted`, and the gateway never sees the order.
    let venue = Venue::default();
    let script = Script::new(vec![
        (1, EngineOutput::StateChange(TradingState::Halted)),
        (
            2,
            EngineOutput::Orders(vec![order("O-2", OrderSide::Sell, 2.0, 2)]),
        ),
    ]);
    let mut engine = engine_with(Some(&venue), &script, &[1, 2]);
    engine.finish().unwrap();
    assert!(venue.submitted.lock().unwrap().is_empty());
    assert_eq!(
        script.lifecycle(),
        vec![Event::OrderRejected {
            order_id: OrderId::new("O-2"),
            reason: "risk_trading_halted".into(),
            ts_event: ts(2),
        }]
    );
    assert_eq!(
        engine.order("O-2").unwrap().state.status,
        OrderStatus::Rejected
    );
    assert!(illegal(&engine).is_empty());
}

#[test]
fn working_exposure_sums_open_remainders() {
    let venue = Venue::default();
    venue
        .on_submit("O-1", vec![fill("O-1", 1.0, 10.0, 1.0, false, 1)])
        .on_submit("O-4", vec![fill("O-4", 5.0, 10.0, 5.0, true, 1)])
        .on_submit("O-5", vec![rejected("O-5", 6.0, "rms", 1)])
        .on_cancel("O-2", vec![cancelled("O-2", 4.0, 2)]);
    let script = Script::new(vec![
        (
            1,
            EngineOutput::Orders(vec![
                order("O-1", OrderSide::Buy, 3.0, 1),
                order("O-2", OrderSide::Buy, 4.0, 1),
                order("O-3", OrderSide::Sell, 2.0, 1),
                order("O-4", OrderSide::Buy, 5.0, 1),
                order("O-5", OrderSide::Buy, 6.0, 1),
                order_on(other_instrument(), "O-6", OrderSide::Buy, 7.0, 1),
            ]),
        ),
        (2, EngineOutput::Cancels(vec![OrderId::new("O-2")])),
    ]);
    let mut engine = engine_with(Some(&venue), &script, &[1, 2]);
    assert_eq!(
        engine.working_exposure(&any_instrument(), OrderSide::Buy),
        0.0
    );

    engine.start().unwrap();
    engine.pump().unwrap();
    let x = any_instrument();
    assert_eq!(engine.working_exposure(&x, OrderSide::Buy), 6.0);
    assert_eq!(engine.working_exposure(&x, OrderSide::Sell), -2.0);
    assert_eq!(
        engine.working_exposure(&other_instrument(), OrderSide::Buy),
        7.0
    );

    engine.finish().unwrap();
    assert_eq!(engine.working_exposure(&x, OrderSide::Buy), 2.0);
    assert_eq!(
        engine.order("O-2").unwrap().state.status,
        OrderStatus::Cancelled
    );
    assert!(illegal(&engine).is_empty());
}

#[test]
fn cancel_emitted_once() {
    let venue = Venue::default();
    let script = Script::new(vec![
        (
            1,
            EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 3.0, 1)]),
        ),
        (
            2,
            EngineOutput::Cancels(vec![
                OrderId::new("O-1"),
                OrderId::new("O-1"),
                OrderId::new("unknown"),
            ]),
        ),
        (3, EngineOutput::Cancels(vec![OrderId::new("O-1")])),
        (5, EngineOutput::Cancels(vec![OrderId::new("O-1")])),
    ]);
    let mut engine = engine_with(Some(&venue), &script, &[1, 2, 3]);
    engine.start().unwrap();
    while engine.pump().unwrap() {}

    // One gateway call, one `order_cancel_requested`; unknown ids are a no-op.
    assert_eq!(venue.cancelled(), vec!["O-1".to_string()]);
    let requested = |e: &Engine, s: &Script| {
        (
            s.lifecycle()
                .iter()
                .filter(|e| matches!(e, Event::OrderCancelRequested { .. }))
                .count(),
            e.audit()
                .iter()
                .filter(|r| matches!(r.kind, AuditKind::CancelRequested { .. }))
                .count(),
        )
    };
    assert_eq!(requested(&engine, &script), (1, 1));
    let tracked = engine.order("O-1").unwrap();
    assert_eq!(
        (tracked.state.status, tracked.state.cancel_requested),
        (OrderStatus::Submitted, true)
    );

    // The venue answers; a cancel of the now terminal order is a no-op.
    venue.push(cancelled("O-1", 3.0, 4));
    engine.inject(quote(4));
    engine.inject(quote(5));
    engine.finish().unwrap();
    assert_eq!(venue.cancelled(), vec!["O-1".to_string()]);
    assert_eq!(requested(&engine, &script), (1, 1));
    let tracked = engine.order("O-1").unwrap();
    assert_eq!(
        (tracked.state.status, tracked.state.cancel_requested),
        (OrderStatus::Cancelled, false)
    );
    assert_eq!(
        script
            .lifecycle()
            .iter()
            .filter(|e| matches!(e, Event::OrderCancelled { .. }))
            .count(),
        1
    );
    assert!(illegal(&engine).is_empty());
}

#[test]
fn illegal_transition_audited_not_panicked() {
    let venue = Venue::default();
    venue.on_submit(
        "O-1",
        vec![
            fill("O-1", 3.0, 10.0, 3.0, true, 1),
            fill("O-1", 1.0, 10.0, 4.0, true, 1),
            cancelled("O-1", 0.5, 1),
            fill("ghost", 2.0, 10.0, 2.0, true, 1),
        ],
    );
    let script = Script::new(vec![(
        1,
        EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 3.0, 1)]),
    )]);
    let mut engine = engine_with(Some(&venue), &script, &[1]);
    engine.finish().unwrap();

    assert_eq!(
        illegal(&engine),
        vec![
            (
                "O-1".to_string(),
                IllegalTransition::Overfill {
                    quantity: 3.0,
                    filled_qty: 3.0,
                    last_qty: 1.0,
                }
            ),
            (
                "O-1".to_string(),
                IllegalTransition::Transition {
                    status: OrderStatus::Filled,
                    cancel_requested: false,
                    event: OrderEventKind::Cancelled,
                }
            ),
            (
                "ghost".to_string(),
                IllegalTransition::Transition {
                    status: OrderStatus::Initialized,
                    cancel_requested: false,
                    event: OrderEventKind::Fill,
                }
            ),
        ]
    );
    // No wire event for a rejected transition; the state is unchanged.
    let wire: Vec<_> = script.lifecycle();
    assert_eq!(wire.len(), 2, "{wire:?}");
    assert!(matches!(wire[1], Event::OrderFilled { last_qty, .. } if last_qty == 3.0));
    let tracked = engine.order("O-1").unwrap();
    assert_eq!(
        (tracked.state.status, tracked.state.filled_qty),
        (OrderStatus::Filled, 3.0)
    );
    // Money moved at the venue: illegal fills still book into the position.
    assert_eq!(engine.position(&any_instrument()), 6.0);
}

#[test]
fn a_later_different_venue_order_id_is_audited_as_drift_not_applied() {
    let venue = Venue::default();
    let mut late = fill("O-1", 2.0, 10.0, 2.0, true, 1);
    if let ExecutionEvent::Fill { venue_order_id, .. } = &mut late {
        *venue_order_id = Some(VenueOrderId::new("V-2"));
    }
    venue.on_submit("O-1", vec![accepted("O-1", 2.0, Some("V-1"), 1), late]);
    let script = Script::new(vec![(
        1,
        EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 2.0, 1)]),
    )]);
    let mut engine = engine_with(Some(&venue), &script, &[1]);
    engine.finish().unwrap();

    let tracked = engine.order("O-1").unwrap();
    assert_eq!(tracked.venue_order_id, Some(VenueOrderId::new("V-1")));
    assert_eq!(tracked.state.status, OrderStatus::Filled);
    assert!(engine.audit().iter().any(|r| r.kind
        == AuditKind::VenueOrderIdDrift {
            order_id: "O-1".into(),
            recorded: VenueOrderId::new("V-1"),
            received: VenueOrderId::new("V-2"),
        }));
}

#[test]
fn positions_are_seeded_and_moved_only_by_fills() {
    let venue = Venue::default();
    let mut sell = fill("O-1", 4.0, 10.0, 4.0, true, 1);
    if let ExecutionEvent::Fill { trade, .. } = &mut sell {
        *trade = Trade::new(
            OrderId::new("O-1"),
            any_instrument(),
            OrderSide::Sell,
            4.0,
            10.0,
            Currency::Inr,
            ts(1),
            ts(1),
        );
    }
    venue.on_submit("O-1", vec![sell]);
    let script = Script::new(vec![(
        1,
        EngineOutput::Orders(vec![
            order("O-1", OrderSide::Sell, 4.0, 1),
            order("O-2", OrderSide::Buy, 9.0, 1),
        ]),
    )]);
    let mut engine = Engine::new().with_positions([(any_instrument(), 10.0)]);
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(script.clone());
    engine.inject(quote(1));
    engine.finish().unwrap();
    assert_eq!(engine.position(&any_instrument()), 6.0);
    assert_eq!(engine.position(&other_instrument()), 0.0);
}
