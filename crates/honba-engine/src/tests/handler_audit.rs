//! Handler-reported audit records: the default is empty, and the engine merges what a handler
//! drains into its own log in order (ADR 0018 decision 6).

use std::sync::{Arc, Mutex};

use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, Message, PriceType, TradingState,
    UnixNanos,
};

use super::any_instrument;
use crate::{AuditKind, Engine, EngineOutput, Handler, NoopHandler, Result};

fn rejected(id: &str) -> AuditKind {
    AuditKind::OrderRejected {
        order_id: id.to_string(),
        reason: "r".to_string(),
    }
}

/// Reports one scripted batch per `on_event`, and one after a state change.
struct Reporter {
    on_event_batches: Vec<Vec<AuditKind>>,
    on_state: Vec<AuditKind>,
    pending: Vec<AuditKind>,
    drains: Arc<Mutex<usize>>,
}

impl Handler for Reporter {
    fn on_event(&mut self, _event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        if !self.on_event_batches.is_empty() {
            self.pending.extend(self.on_event_batches.remove(0));
        }
        Ok(EngineOutput::None)
    }
    fn on_trading_state(&mut self, _state: TradingState) {
        self.pending.append(&mut self.on_state);
    }
    fn drain_audit(&mut self) -> Vec<AuditKind> {
        *self.drains.lock().unwrap() += 1;
        std::mem::take(&mut self.pending)
    }
}

fn reporter(batches: Vec<Vec<AuditKind>>, on_state: Vec<AuditKind>) -> Reporter {
    Reporter {
        on_event_batches: batches,
        on_state,
        pending: Vec::new(),
        drains: Arc::default(),
    }
}

fn bar(t: u64) -> Message {
    let ty = BarType::new(
        any_instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(t);
    Message::new(
        Event::Bar(Bar::new(ty, 10.0, 10.0, 10.0, 10.0, 1.0, t, t)),
        t,
    )
}

#[test]
fn default_drain_audit_is_empty() {
    assert!(NoopHandler.drain_audit().is_empty());
    let mut engine = Engine::new();
    engine.add_handler(NoopHandler);
    engine.start().unwrap();
    engine.inject(bar(1));
    engine.finish().unwrap();
    assert!(engine
        .audit()
        .iter()
        .all(|r| matches!(r.kind, AuditKind::EventDispatched { .. })));
}

#[test]
fn engine_merges_handler_records_in_order_with_its_own_seq() {
    let refused = AuditKind::RiskRefused {
        order_id: "A".to_string(),
        refusal: honba_risk::RiskRefusal::TradingHalted,
    };
    let mut engine = Engine::new();
    engine.add_handler(reporter(
        vec![vec![refused.clone(), rejected("A")], vec![rejected("B")]],
        vec![],
    ));
    engine.start().unwrap();
    engine.inject(bar(1));
    engine.inject(bar(2));
    engine.finish().unwrap();

    let records = engine.audit();
    let kinds: Vec<_> = records.iter().map(|r| r.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            AuditKind::EventDispatched { ts_event: 1 },
            refused,
            rejected("A"),
            AuditKind::EventDispatched { ts_event: 2 },
            rejected("B"),
        ]
    );
    let seqs: Vec<u64> = records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, (0..5).collect::<Vec<u64>>());
}

#[test]
fn records_reported_after_a_state_change_are_merged_once() {
    let mut engine = Engine::new();
    engine.add_handler(reporter(vec![], vec![rejected("S")]));
    engine.set_trading_state(TradingState::Halted);
    engine.set_trading_state(TradingState::Halted);
    let kinds: Vec<_> = engine.audit().iter().map(|r| r.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            AuditKind::StateChanged {
                from: TradingState::Active,
                to: TradingState::Halted
            },
            rejected("S"),
        ]
    );
}
