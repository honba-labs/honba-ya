//! End-to-end engine tests using injected feeds and handlers.

use std::sync::{Arc, Mutex};

use honba_engine::{
    AlgoError, AuditKind, DataFeed, Engine, EngineOutput, ExecutionEngine, Handler, Result,
    TradingState,
};
use honba_entities::Trade;
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Exchange, InstrumentId, Message, Order,
    OrderId, OrderSide, OrderType, PriceType, QuoteTick, TimeInForce, UnixNanos,
};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            InstrumentId::new("X", Exchange::new("NSE")),
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

struct VecFeed {
    items: std::collections::VecDeque<Message>,
}

impl VecFeed {
    fn new(items: Vec<Message>) -> Self {
        Self {
            items: items.into(),
        }
    }
}

impl DataFeed for VecFeed {
    fn next(&mut self) -> Result<Option<Message>> {
        Ok(self.items.pop_front())
    }
}

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
    started: Arc<Mutex<bool>>,
    stopped: Arc<Mutex<bool>>,
}

impl Handler for Recorder {
    fn on_start(&mut self) -> Result<()> {
        *self.started.lock().unwrap() = true;
        Ok(())
    }

    fn on_event(
        &mut self,
        event: &Event,
        _ts_init: UnixNanos,
    ) -> Result<honba_engine::EngineOutput> {
        let mut evs = self.events.lock().unwrap();
        evs.push(event.clone());
        Ok(honba_engine::EngineOutput::None)
    }

    fn on_stop(&mut self) -> Result<()> {
        *self.stopped.lock().unwrap() = true;
        Ok(())
    }
}

#[test]
fn engine_processes_events_in_order() {
    let mut feed = VecFeed::new(vec![quote(3), quote(1), quote(2)]);

    let rec = Recorder::default();
    let events = rec.events.clone();
    let started = rec.started.clone();
    let stopped = rec.stopped.clone();

    let mut engine = Engine::new();
    engine.add_handler(rec);
    engine.run(&mut feed).unwrap();

    assert_eq!(
        *events
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.ts_event().as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(*started.lock().unwrap());
    assert!(*stopped.lock().unwrap());
    assert_eq!(engine.now(), ts(3));
}

#[test]
fn engine_with_empty_feed_finishes() {
    let mut feed = VecFeed::new(vec![]);
    let rec = Recorder::default();
    let events = rec.events.clone();

    let mut engine = Engine::new();
    engine.add_handler(rec);
    engine.run(&mut feed).unwrap();

    assert!(events.lock().unwrap().is_empty());
    assert_eq!(engine.now(), ts(0));
}

#[test]
fn engine_detects_clock_regression() {
    // Feed that returns ts=10 then ts=5 — the queue orders by ts, so 5 pops
    // first. That's fine. To force regression we need a handler that
    // observes ts=10 then a later event with ts=5, which the queue prevents.
    //
    // Instead, test the clock directly by driving an out-of-order queue via
    // a handler that advances a second clock. Here we settle for verifying
    // the engine enforces monotonic time on the events it dispatches.
    let mut feed = VecFeed::new(vec![quote(5), quote(10)]);

    #[derive(Clone, Default)]
    struct MaxTs(Arc<Mutex<u64>>);

    impl Handler for MaxTs {
        fn on_event(
            &mut self,
            event: &Event,
            _init: UnixNanos,
        ) -> Result<honba_engine::EngineOutput> {
            let t = event.ts_event().as_u64();
            let mut m = self.0.lock().unwrap();
            assert!(t >= *m, "event ts went backwards: {t} < {m}");
            *m = t;
            Ok(honba_engine::EngineOutput::None)
        }
    }

    let mut engine = Engine::new();
    engine.add_handler(MaxTs::default());
    engine.run(&mut feed).unwrap();

    // Sanity: the AlgoError variant exists and formats.
    let e = AlgoError::ClockRegression {
        current: 10,
        requested: 5,
    };
    assert!(format!("{e}").contains("clock cannot go backwards"));
}

#[test]
fn multiple_handlers_all_receive_events() {
    let mut feed = VecFeed::new(vec![quote(1), quote(2)]);

    let a = Recorder::default();
    let b = Recorder::default();
    let ea = a.events.clone();
    let eb = b.events.clone();

    let mut engine = Engine::new();
    engine.add_handler(a);
    engine.add_handler(b);
    engine.run(&mut feed).unwrap();

    assert_eq!(
        *ea.lock()
            .unwrap()
            .iter()
            .map(|e| e.ts_event().as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        *eb.lock()
            .unwrap()
            .iter()
            .map(|e| e.ts_event().as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn batch_size_one_requires_ordered_feed() {
    // With batch_size = 1 the engine has no look-ahead, so an unordered
    // feed trips the monotonic clock.
    let mut feed = VecFeed::new(vec![quote(3), quote(1)]);

    let mut engine = Engine::new().with_batch_size(1);
    engine.add_handler(Recorder::default());

    let err = engine.run(&mut feed).unwrap_err();
    assert!(matches!(err, AlgoError::ClockRegression { .. }));
}

fn instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn bar_msg(close: f64, t: u64) -> Message {
    let bar_type = BarType::new(
        instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let bar = Bar::new(bar_type, close, close, close, close, 10.0, ts(t), ts(t));
    Message::new(Event::Bar(bar), ts(t))
}

fn market_order(id: &str, side: OrderSide, quantity: f64, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        instrument(),
        side,
        OrderType::Market,
        quantity,
        None,
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

#[derive(Clone)]
struct SpySink {
    submitted: Arc<Mutex<Vec<Order>>>,
    cancelled: Arc<Mutex<Vec<String>>>,
    fills: Arc<Mutex<Vec<Trade>>>,
    fill_price: f64,
}

impl Default for SpySink {
    fn default() -> Self {
        Self {
            submitted: Arc::new(Mutex::new(Vec::new())),
            cancelled: Arc::new(Mutex::new(Vec::new())),
            fills: Arc::new(Mutex::new(Vec::new())),
            fill_price: 101.0,
        }
    }
}

impl ExecutionEngine for SpySink {
    fn submit(&mut self, order: Order) -> Result<()> {
        let fill = Trade::new(
            order.order_id().clone(),
            order.instrument_id().clone(),
            order.side(),
            order.quantity(),
            self.fill_price,
            honba_entities::Currency::Inr,
            order.ts_event(),
            order.ts_init(),
        );
        self.submitted.lock().unwrap().push(order);
        self.fills.lock().unwrap().push(fill);
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, _now: UnixNanos) -> Result<()> {
        self.cancelled.lock().unwrap().push(order_id.to_string());
        Ok(())
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(std::mem::take(&mut self.fills.lock().unwrap()))
    }
}

struct Scripted {
    steps: Vec<(u64, EngineOutput)>,
    seen: Arc<Mutex<Vec<Event>>>,
}

impl Handler for Scripted {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        self.seen.lock().unwrap().push(event.clone());
        let t = event.ts_event().as_u64();
        match self.steps.iter().position(|(at, _)| *at == t) {
            Some(i) => Ok(self.steps.remove(i).1.clone()),
            None => Ok(EngineOutput::None),
        }
    }
}

#[test]
fn handler_output_routes_orders_to_the_execution_sink() {
    let mut feed = VecFeed::new(vec![
        bar_msg(101.0, 1),
        bar_msg(102.0, 2),
        bar_msg(103.0, 3),
    ]);
    let sink = SpySink::default();

    let mut engine = Engine::new();
    engine.set_execution(Box::new(sink.clone()));
    engine.add_handler(Scripted {
        steps: vec![(
            1,
            EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 5.0, 1)]),
        )],
        seen: Arc::new(Mutex::new(Vec::new())),
    });
    engine.run(&mut feed).unwrap();

    let submitted = sink.submitted.lock().unwrap().clone();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].order_id().as_str(), "O-1");
    assert_eq!(submitted[0].quantity(), 5.0);
    assert_eq!(submitted[0].side(), OrderSide::Buy);

    let audit = engine.audit();
    assert_eq!(
        audit[0],
        honba_engine::AuditRecord {
            seq: 0,
            kind: AuditKind::EventDispatched { ts_event: 1 },
        }
    );
    assert_eq!(
        audit[1],
        honba_engine::AuditRecord {
            seq: 1,
            kind: AuditKind::OrderSubmitted {
                order_id: "O-1".to_string(),
                instrument: "X.NSE".to_string(),
                side: "buy".to_string(),
            },
        }
    );
    assert_eq!(
        audit[2],
        honba_engine::AuditRecord {
            seq: 2,
            kind: AuditKind::FillProduced {
                order_id: "O-1".to_string(),
                quantity: 5.0,
                price: 101.0,
                ts_event: 1,
            },
        }
    );
    assert_eq!(
        audit
            .iter()
            .filter(|r| matches!(r.kind, AuditKind::OrderSubmitted { .. }))
            .count(),
        1
    );
}

#[test]
fn fills_re_enter_the_queue_as_order_filled_events() {
    use honba_sim::BarFillEngine;

    let mut feed = VecFeed::new(vec![bar_msg(101.0, 1), bar_msg(102.0, 2)]);
    let execution = BarFillEngine::new();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut engine = Engine::new();
    engine.set_execution(Box::new(execution.clone()));
    engine.add_handler(execution.clone());
    engine.add_handler(Scripted {
        steps: vec![(
            1,
            EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 5.0, 1)]),
        )],
        seen: seen.clone(),
    });
    engine.run(&mut feed).unwrap();

    let seen = seen.lock().unwrap().clone();
    let bar_index = seen
        .iter()
        .position(|e| matches!(e, Event::Bar(b) if b.close() == 101.0))
        .unwrap();
    let fill_index = seen
        .iter()
        .position(|e| {
            matches!(e, Event::OrderFilled { order_id, last_qty, last_px, .. }
                if order_id.as_str() == "O-1" && *last_qty == 5.0 && *last_px == 101.0)
        })
        .unwrap();
    assert!(fill_index > bar_index, "the ack must follow its command");

    assert!(engine.audit().iter().any(|r| matches!(
        &r.kind,
        AuditKind::FillProduced { order_id, quantity, price, ts_event }
            if order_id == "O-1" && *quantity == 5.0 && *price == 101.0 && *ts_event == 1
    )));

    let fills = engine.drain_fills().unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id().as_str(), "O-1");
    assert_eq!(fills[0].price(), 101.0);
    assert!(engine.drain_fills().unwrap().is_empty());
}

#[test]
fn cancels_and_state_changes_are_applied_and_audited() {
    let mut feed = VecFeed::new(vec![
        bar_msg(101.0, 1),
        bar_msg(102.0, 2),
        bar_msg(103.0, 3),
    ]);
    let sink = SpySink::default();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut engine = Engine::new();
    engine.set_execution(Box::new(sink.clone()));
    engine.add_handler(Scripted {
        steps: vec![
            (1, EngineOutput::Cancels(vec![OrderId::new("O-9")])),
            (2, EngineOutput::StateChange(TradingState::Halted)),
            (
                3,
                EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 5.0, 3)]),
            ),
        ],
        seen: seen.clone(),
    });
    engine.run(&mut feed).unwrap();

    // ADR 0019 decision 6: cancelling an order the engine never submitted is a
    // no-op that reaches no sink and records nothing.
    assert!(sink.cancelled.lock().unwrap().is_empty());
    assert!(sink.submitted.lock().unwrap().is_empty());
    assert_eq!(engine.trading_state(), TradingState::Halted);
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 4, "the run must continue after the rejection");
    assert!(
        matches!(&seen[3], Event::OrderRejected { order_id, reason, .. }
            if order_id.as_str() == "O-1" && reason == "risk_trading_halted"),
        "the refusal reaches the handlers as an order_rejected: {:?}",
        seen[3]
    );
    assert_eq!(engine.now(), ts(3));

    // ADR 0018 decision 5 / ADR 0019 decision 5: the pre-gate reason is the
    // `ErrorCode` wire spelling, and the refusal is a dispatched event.
    let kinds: Vec<AuditKind> = engine.audit().iter().map(|r| r.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            AuditKind::EventDispatched { ts_event: 1 },
            AuditKind::EventDispatched { ts_event: 2 },
            AuditKind::StateChanged {
                from: TradingState::Active,
                to: TradingState::Halted,
            },
            AuditKind::EventDispatched { ts_event: 3 },
            AuditKind::RiskRefused {
                order_id: "O-1".to_string(),
                refusal: honba_risk::RiskRefusal::TradingHalted,
            },
            AuditKind::OrderRejected {
                order_id: "O-1".to_string(),
                reason: "risk_trading_halted".to_string(),
            },
            AuditKind::EventDispatched { ts_event: 3 },
        ]
    );
}

#[test]
fn orders_without_an_execution_sink_are_rejected_not_fatal() {
    let mut feed = VecFeed::new(vec![bar_msg(101.0, 1), bar_msg(102.0, 2)]);
    let seen = Arc::new(Mutex::new(Vec::new()));

    let mut engine = Engine::new();
    engine.add_handler(Scripted {
        steps: vec![
            (
                1,
                EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 5.0, 1)]),
            ),
            (
                2,
                EngineOutput::Orders(vec![market_order("O-2", OrderSide::Sell, 2.0, 2)]),
            ),
        ],
        seen: seen.clone(),
    });
    engine.run(&mut feed).unwrap();

    assert_eq!(engine.now(), ts(2));
    // Two bars and, for each refusal, its `order_rejected` (ADR 0019 decision 5).
    assert_eq!(seen.lock().unwrap().len(), 4);
    let kinds: Vec<AuditKind> = engine.audit().iter().map(|r| r.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            AuditKind::EventDispatched { ts_event: 1 },
            AuditKind::OrderRejected {
                order_id: "O-1".to_string(),
                reason: "order_execution_unavailable".to_string(),
            },
            AuditKind::EventDispatched { ts_event: 1 },
            AuditKind::EventDispatched { ts_event: 2 },
            AuditKind::OrderRejected {
                order_id: "O-2".to_string(),
                reason: "order_execution_unavailable".to_string(),
            },
            AuditKind::EventDispatched { ts_event: 2 },
        ]
    );
}

#[test]
fn two_runs_with_the_same_feed_produce_identical_audit_records() {
    fn one_run() -> Vec<honba_engine::AuditRecord> {
        let mut feed = VecFeed::new(vec![
            bar_msg(101.0, 1),
            bar_msg(102.0, 2),
            bar_msg(103.0, 3),
        ]);
        let sink = SpySink::default();
        let mut engine = Engine::new();
        engine.set_execution(Box::new(sink.clone()));
        engine.add_handler(Scripted {
            steps: vec![
                (
                    1,
                    EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 5.0, 1)]),
                ),
                (2, EngineOutput::Cancels(vec![OrderId::new("O-9")])),
                (3, EngineOutput::StateChange(TradingState::Halted)),
            ],
            seen: Arc::new(Mutex::new(Vec::new())),
        });
        engine.run(&mut feed).unwrap();
        engine.audit().to_vec()
    }

    let first = one_run();
    let second = one_run();
    assert_eq!(first, second);
    assert_eq!(
        first.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    assert_eq!(
        first.iter().map(|r| &r.kind).collect::<Vec<_>>(),
        vec![
            &AuditKind::EventDispatched { ts_event: 1 },
            &AuditKind::OrderSubmitted {
                order_id: "O-1".to_string(),
                instrument: "X.NSE".to_string(),
                side: "buy".to_string(),
            },
            &AuditKind::FillProduced {
                order_id: "O-1".to_string(),
                quantity: 5.0,
                price: 101.0,
                ts_event: 1,
            },
            // `order` (the submitter's Event::Order), then `order_filled`.
            &AuditKind::EventDispatched { ts_event: 1 },
            &AuditKind::EventDispatched { ts_event: 1 },
            &AuditKind::EventDispatched { ts_event: 2 },
            // The cancel of the never-submitted O-9 is a no-op (ADR 0019 decision 6).
            &AuditKind::EventDispatched { ts_event: 3 },
            &AuditKind::StateChanged {
                from: TradingState::Active,
                to: TradingState::Halted,
            },
        ]
    );
}

#[test]
fn run_then_replay_identical_positions() {
    let mut feed = VecFeed::new(vec![
        bar_msg(101.0, 1),
        bar_msg(102.0, 2),
        bar_msg(103.0, 3),
    ]);
    let sink = SpySink::default();
    let inst = InstrumentId::new("X", Exchange::new("NSE"));

    let mut engine = Engine::new();
    engine.set_execution(Box::new(sink.clone()));
    engine.add_handler(Scripted {
        steps: vec![
            (
                1,
                EngineOutput::Orders(vec![market_order("O-1", OrderSide::Buy, 10.0, 1)]),
            ),
            (
                2,
                EngineOutput::Orders(vec![market_order("O-2", OrderSide::Sell, 4.0, 2)]),
            ),
            (3, EngineOutput::StateChange(TradingState::Reducing)),
        ],
        seen: Arc::new(Mutex::new(Vec::new())),
    });
    engine.run(&mut feed).unwrap();

    // Verify engine state
    assert_eq!(engine.position(&inst), 6.0);
    assert_eq!(engine.trading_state(), TradingState::Reducing);

    // Replay directly from audit log
    let replayed = engine.audit_log().replay();
    assert_eq!(replayed.position(&inst), engine.position(&inst));
    assert_eq!(replayed.trading_state(), engine.trading_state());
    assert_eq!(replayed.order("O-1").unwrap().filled_qty, 10.0);
    assert_eq!(replayed.order("O-2").unwrap().filled_qty, 4.0);

    // Write journal as NDJSON and reload
    let mut journal_bytes = Vec::new();
    engine
        .audit_log()
        .write_ndjson(&mut journal_bytes)
        .expect("write_ndjson must succeed");

    let loaded_log =
        honba_engine::AuditLog::read_ndjson(&journal_bytes[..]).expect("read_ndjson must succeed");
    assert_eq!(loaded_log, *engine.audit_log());

    let loaded_replayed = loaded_log.replay();
    assert_eq!(loaded_replayed.position(&inst), engine.position(&inst));
    assert_eq!(loaded_replayed.trading_state(), engine.trading_state());
}
