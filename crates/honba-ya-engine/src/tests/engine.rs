//! Unit tests for `crate::engine`.

use std::sync::{Arc, Mutex};

use honba_entities::Trade;
use honba_messages::{Event, Message, Order, QuoteTick, UnixNanos};

use super::any_instrument;

use crate::{
    AlgoError, AuditKind, AuditRecord, DataFeed, Engine, EngineOutput, ExecutionEngine, Handler,
    Result, TradingState,
};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn quote(ts_event: u64, ts_init: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            any_instrument(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(ts_event),
            ts(ts_init),
        )),
        ts(ts_init),
    )
}

struct EmptyFeed;

impl DataFeed for EmptyFeed {
    fn next(&mut self) -> Result<Option<Message>> {
        Ok(None)
    }
}

#[derive(Clone, Default)]
struct Recorder {
    seen: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl Handler for Recorder {
    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> Result<EngineOutput> {
        let mut seen = self.seen.lock().unwrap();
        seen.push((event.ts_event().as_u64(), ts_init.as_u64()));
        Ok(EngineOutput::None)
    }
}

#[derive(Clone, Default)]
struct StubSink {
    submitted: Arc<Mutex<Vec<String>>>,
    cancelled: Arc<Mutex<Vec<String>>>,
}

impl ExecutionEngine for StubSink {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.submitted
            .lock()
            .unwrap()
            .push(order.order_id().as_str().to_string());
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, _now: UnixNanos) -> Result<()> {
        self.cancelled.lock().unwrap().push(order_id.to_string());
        Ok(())
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

#[test]
fn inject_enqueues_in_ts_event_order_with_fifo_ties() {
    let mut engine = Engine::new();
    engine.inject(quote(5, 1));
    engine.inject(quote(3, 2));
    engine.inject(quote(5, 4));
    engine.inject(quote(5, 3));

    let recorder = Recorder::default();
    let seen = recorder.seen.clone();
    engine.add_handler(recorder);
    engine.run(&mut EmptyFeed).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![(3, 2), (5, 1), (5, 4), (5, 3)]);
}

#[test]
fn inject_does_not_advance_the_clock() {
    let mut engine = Engine::new();
    engine.inject(quote(500, 500));
    assert_eq!(engine.now(), ts(0));
    engine.run(&mut EmptyFeed).unwrap();
    assert_eq!(engine.now(), ts(500));
}

#[test]
fn a_fresh_engine_is_active_with_no_execution_and_an_empty_audit() {
    let engine = Engine::new();
    assert_eq!(engine.trading_state(), TradingState::Active);
    assert!(engine.execution().is_none());
    assert!(engine.audit().is_empty());
}

#[test]
fn execution_is_visible_once_attached() {
    let mut engine = Engine::new();
    let sink = StubSink::default();
    let submitted = sink.submitted.clone();
    let cancelled = sink.cancelled.clone();

    engine.set_execution(Box::new(sink));
    assert!(engine.execution().is_some());

    engine.run(&mut EmptyFeed).unwrap();
    assert!(submitted.lock().unwrap().is_empty());
    assert!(cancelled.lock().unwrap().is_empty());
}

#[test]
fn drain_fills_is_empty_before_anything_runs() {
    let mut engine = Engine::new();
    engine.set_execution(Box::new(StubSink::default()));
    engine.run(&mut EmptyFeed).unwrap();
    assert!(engine.drain_fills().unwrap().is_empty());
}

#[derive(Clone, Default)]
struct Lifecycle {
    starts: Arc<Mutex<usize>>,
    stops: Arc<Mutex<usize>>,
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Lifecycle {
    fn starts(&self) -> usize {
        *self.starts.lock().unwrap()
    }

    fn stops(&self) -> usize {
        *self.stops.lock().unwrap()
    }

    fn seen(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }
}

impl Handler for Lifecycle {
    fn on_start(&mut self) -> Result<()> {
        *self.starts.lock().unwrap() += 1;
        Ok(())
    }

    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        self.seen.lock().unwrap().push(event.ts_event().as_u64());
        Ok(EngineOutput::None)
    }

    fn on_stop(&mut self) -> Result<()> {
        *self.stops.lock().unwrap() += 1;
        Ok(())
    }
}

#[test]
fn start_runs_on_start_once_and_a_second_call_is_a_no_op() {
    let mut engine = Engine::new();
    let lifecycle = Lifecycle::default();
    engine.add_handler(lifecycle.clone());

    engine.start().unwrap();
    engine.start().unwrap();
    engine.start().unwrap();

    assert_eq!(lifecycle.starts(), 1);
    assert_eq!(lifecycle.stops(), 0);
}

#[test]
fn finish_drains_the_queue_in_time_order_then_runs_on_stop_once() {
    let mut engine = Engine::new();
    let lifecycle = Lifecycle::default();
    engine.add_handler(lifecycle.clone());
    engine.inject(quote(3, 3));
    engine.inject(quote(1, 1));

    engine.finish().unwrap();
    engine.finish().unwrap();

    assert_eq!(lifecycle.seen(), vec![1, 3]);
    assert_eq!(lifecycle.stops(), 1);
    assert_eq!(lifecycle.starts(), 0);
}

#[test]
fn pump_dispatches_one_message_per_call_and_reports_an_empty_queue() {
    let mut engine = Engine::new();
    let lifecycle = Lifecycle::default();
    engine.add_handler(lifecycle.clone());
    engine.inject(quote(3, 3));
    engine.inject(quote(1, 1));

    assert!(engine.pump().unwrap());
    assert_eq!(lifecycle.seen(), vec![1]);
    assert!(engine.pump().unwrap());
    assert_eq!(lifecycle.seen(), vec![1, 3]);
    assert_eq!(engine.now(), ts(3));

    assert!(!engine.pump().unwrap());
    assert_eq!(lifecycle.seen(), vec![1, 3]);
    assert_eq!(engine.now(), ts(3));
}

#[test]
fn pending_counts_the_messages_waiting_in_the_queue() {
    let mut engine = Engine::new();
    assert_eq!(engine.pending(), 0);

    engine.inject(quote(1, 1));
    assert_eq!(engine.pending(), 1);

    engine.inject(quote(2, 2));
    assert_eq!(engine.pending(), 2);

    engine.start().unwrap();
    engine.pump().unwrap();
    assert_eq!(engine.pending(), 1);

    engine.finish().unwrap();
    assert_eq!(engine.pending(), 0);
}

#[test]
fn set_trading_state_returns_the_previous_state_and_audits_the_change() {
    let mut engine = Engine::new();

    assert_eq!(
        engine.set_trading_state(TradingState::Halted),
        TradingState::Active
    );
    assert_eq!(engine.trading_state(), TradingState::Halted);
    assert_eq!(
        engine.audit(),
        &[AuditRecord {
            seq: 0,
            kind: AuditKind::StateChanged {
                from: TradingState::Active,
                to: TradingState::Halted,
            },
        }]
    );

    assert_eq!(
        engine.set_trading_state(TradingState::Reducing),
        TradingState::Halted
    );
    assert_eq!(engine.trading_state(), TradingState::Reducing);
    assert_eq!(
        engine.audit(),
        &[
            AuditRecord {
                seq: 0,
                kind: AuditKind::StateChanged {
                    from: TradingState::Active,
                    to: TradingState::Halted,
                },
            },
            AuditRecord {
                seq: 1,
                kind: AuditKind::StateChanged {
                    from: TradingState::Halted,
                    to: TradingState::Reducing,
                },
            },
        ]
    );
}

#[test]
fn set_trading_state_to_the_state_it_is_already_in_records_nothing() {
    let mut engine = Engine::new();

    assert_eq!(
        engine.set_trading_state(TradingState::Active),
        TradingState::Active
    );

    assert_eq!(engine.trading_state(), TradingState::Active);
    assert!(engine.audit().is_empty());
}

#[test]
fn set_trading_state_applies_exactly_the_transitions_the_state_machine_permits() {
    const STATES: [TradingState; 3] = [
        TradingState::Active,
        TradingState::Reducing,
        TradingState::Halted,
    ];

    let mut engine = Engine::new();
    for from in STATES {
        for to in STATES {
            engine.set_trading_state(from);
            let audited = engine.audit().len();

            assert_eq!(engine.set_trading_state(to), from);

            if from.can_transition_to(to) {
                assert_eq!(engine.trading_state(), to, "{from:?} -> {to:?} was refused");
            }
            let changed = from.can_transition_to(to) && from != to;
            assert_eq!(
                engine.audit().len(),
                audited + usize::from(changed),
                "{from:?} -> {to:?} audited the wrong number of records"
            );
        }
    }
}

#[test]
fn a_handler_error_aborts_the_run() {
    struct Failing;

    impl Handler for Failing {
        fn on_event(&mut self, _event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
            Err(AlgoError::Component("boom".to_string()))
        }
    }

    let mut engine = Engine::new();
    engine.add_handler(Failing);
    engine.inject(quote(1, 1));
    let err = engine.run(&mut EmptyFeed).unwrap_err();
    assert!(matches!(err, AlgoError::Component(_)));
}
