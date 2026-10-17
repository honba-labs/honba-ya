//! Integration tests for durable risk state, idempotent fill ledger, and reconnect replay (E2-S11).

use std::sync::{Arc, Mutex};

use honba_engine::audit::AuditKind;
use honba_engine::cache::CacheQuery;
use honba_engine::execution::ExecutionEngine;
use honba_engine::{Engine, EngineOutput, Handler, Result};
use honba_entities::{Currency, ExecutionEvent, Trade};
use honba_messages::{
    Event, Exchange, InstrumentId, Message, Order, OrderId, OrderSide, OrderType, QuoteTick,
    TimeInForce, UnixNanos,
};
use honba_risk::DurableRiskState;
use honba_sim::ScriptedExecution;

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn instrument() -> InstrumentId {
    InstrumentId::new("RELIANCE", Exchange::new("NSE"))
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            instrument(),
            2500.0,
            2501.0,
            2500.0,
            2501.0,
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
        Some(2500.0),
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

#[derive(Clone)]
struct SimpleStrategy {
    steps: Arc<Mutex<Vec<(u64, EngineOutput)>>>,
}

impl SimpleStrategy {
    fn new(steps: Vec<(u64, EngineOutput)>) -> Self {
        Self {
            steps: Arc::new(Mutex::new(steps)),
        }
    }
}

impl Handler for SimpleStrategy {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        if let Event::Quote(_) = event {
            let t = event.ts_event().as_u64();
            let mut steps = self.steps.lock().unwrap();
            if let Some(i) = steps.iter().position(|(at, _)| *at == t) {
                return Ok(steps.remove(i).1);
            }
        }
        Ok(EngineOutput::None)
    }
}

struct ReplayExecution {
    events: Vec<ExecutionEvent>,
}

impl ExecutionEngine for ReplayExecution {
    fn submit(&mut self, _order: Order) -> Result<()> {
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: UnixNanos) -> Result<()> {
        Ok(())
    }
    fn native_events(&self) -> bool {
        true
    }
    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.events))
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

#[test]
fn reconnect_replay_no_double_count() {
    let venue = ScriptedExecution::new(2500.0);
    let strategy = SimpleStrategy::new(vec![(
        1000,
        EngineOutput::Orders(vec![buy("O-1", 10.0, 1000)]),
    )]);

    let mut engine = Engine::new();
    engine.set_execution(Box::new(venue));
    engine.add_handler(strategy);

    engine.start().unwrap();
    engine.inject(quote(1000));
    engine.finish().unwrap();

    // Verify order O-1 filled and moved position to 10.0
    assert_eq!(engine.cache().position(&instrument()), 10.0);
    assert_eq!(engine.fill_ledger().len(), 1);

    let recorded_fill = &engine.fill_ledger().records()[0];
    assert_eq!(recorded_fill.order_id.as_str(), "O-1");
    assert_eq!(recorded_fill.quantity, 10.0);
    assert_eq!(recorded_fill.price, 2500.0);

    // 1. Export durable risk state and verify atomic snapshot persistence
    let durable = engine.export_durable_risk();
    assert_eq!(durable.position(&instrument()), 10.0);
    assert_eq!(durable.fill_ledger.len(), 1);

    let temp_dir = std::env::temp_dir().join(format!("honba_reconnect_{}", std::process::id()));
    let snapshot_file = temp_dir.join("risk_state.json");
    durable.atomic_snapshot(&snapshot_file).unwrap();

    // 2. Load snapshot into a new engine instance simulating a restart / reconnect
    let restored = DurableRiskState::load_snapshot(&snapshot_file).unwrap();
    let mut engine_reconnected = Engine::new().with_durable_risk(restored);

    assert_eq!(engine_reconnected.cache().position(&instrument()), 10.0);
    assert_eq!(engine_reconnected.fill_ledger().len(), 1);

    // 3. Simulate reconnect replay: broker / venue redelivers the identical fill event
    let trade = Trade::new(
        OrderId::new("O-1"),
        instrument(),
        OrderSide::Buy,
        10.0,
        2500.0,
        Currency::Inr,
        ts(1000),
        ts(1000),
    );
    let replayed_fill = ExecutionEvent::Fill {
        trade: trade.clone(),
        cum_qty: 10.0,
        complete: true,
        venue_order_id: None,
    };

    let replay_exec = ReplayExecution {
        events: vec![replayed_fill],
    };
    engine_reconnected.set_execution(Box::new(replay_exec));

    // Acknowledge replayed execution events
    engine_reconnected.acknowledge_events().unwrap();

    // Verify position did NOT double count (remains 10.0, NOT 20.0!)
    assert_eq!(engine_reconnected.cache().position(&instrument()), 10.0);
    assert_eq!(engine_reconnected.fill_ledger().len(), 1);

    // Verify that DuplicateFillIgnored was recorded in audit log
    let dup_audits: Vec<_> = engine_reconnected
        .audit_log()
        .records()
        .iter()
        .filter(|r| {
            matches!(
                &r.kind,
                AuditKind::DuplicateFillIgnored { order_id, .. } if order_id == "O-1"
            )
        })
        .collect();
    assert_eq!(dup_audits.len(), 1);

    // Clean up temporary snapshot
    let _ = std::fs::remove_dir_all(&temp_dir);
}
