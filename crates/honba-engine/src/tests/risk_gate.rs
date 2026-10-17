//! The engine's risk gate: state rules without a stage, the stage's reference price, the
//! handler notification and cancel when halted (ADR 0018 decisions 6-7).

use std::sync::{Arc, Mutex};

use honba_entities::Currency;
use honba_entities::Trade;
use honba_market::{InstrumentRules, PriceBand};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Event, InstrumentId, Message, Order, OrderId,
    OrderSide, OrderType, PriceType, TimeInForce, TradingState, UnixNanos,
};
use honba_risk::{RiskLimits, RiskRefusal, RiskStage, RulesSource};

use super::any_instrument;
use crate::{AuditKind, Engine, EngineOutput, ExecutionEngine, Handler, Result};

/// A sink that records what reaches it and never answers: submitted orders stay working.
#[derive(Clone, Default)]
struct Sink {
    submitted: Arc<Mutex<Vec<String>>>,
    cancelled: Arc<Mutex<Vec<String>>>,
}

impl Sink {
    fn submitted(&self) -> Vec<String> {
        self.submitted.lock().unwrap().clone()
    }
}

impl ExecutionEngine for Sink {
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

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn order(id: &str, side: OrderSide, qty: f64, price: Option<f64>, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        any_instrument(),
        side,
        if price.is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        qty,
        price,
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

struct Rules;

impl RulesSource for Rules {
    fn rules(&self, id: &InstrumentId) -> Option<(InstrumentRules, Option<PriceBand>)> {
        (id == &any_instrument()).then(|| (InstrumentRules::new(1.0, 0.05), None))
    }
}

fn stage(limits: RiskLimits) -> RiskStage {
    let rules: Arc<dyn RulesSource> = Arc::new(Rules);
    RiskStage::new(limits, Currency::Inr, rules).unwrap()
}

fn bar(close: f64, t: u64) -> Message {
    let ty = BarType::new(
        any_instrument(),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    Message::new(
        Event::Bar(Bar::new(ty, close, close, close, close, 1.0, ts(t), ts(t))),
        ts(t),
    )
}

/// Emits the scripted orders on the bar at each ts; records the trading states it is told of.
#[derive(Clone, Default)]
struct Script {
    steps: Arc<Mutex<Vec<(u64, EngineOutput)>>>,
    states: Arc<Mutex<Vec<TradingState>>>,
}

impl Handler for Script {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        let t = event.ts_event().as_u64();
        let mut steps = self.steps.lock().unwrap();
        Ok(match steps.iter().position(|(at, _)| *at == t) {
            Some(i) if matches!(event, Event::Bar(_)) => steps.remove(i).1,
            _ => EngineOutput::None,
        })
    }

    fn on_trading_state(&mut self, state: TradingState) {
        self.states.lock().unwrap().push(state);
    }
}

fn script(steps: Vec<(u64, EngineOutput)>) -> Script {
    Script {
        steps: Arc::new(Mutex::new(steps)),
        states: Arc::default(),
    }
}

fn run(engine: &mut Engine, bars: &[(f64, u64)]) {
    engine.start().unwrap();
    for (close, t) in bars {
        engine.inject(bar(*close, *t));
    }
    engine.finish().unwrap();
}

fn risk_kinds(engine: &Engine) -> Vec<AuditKind> {
    engine
        .audit()
        .iter()
        .map(|r| r.kind.clone())
        .filter(|k| {
            matches!(
                k,
                AuditKind::RiskRefused { .. } | AuditKind::OrderRejected { .. }
            )
        })
        .collect()
}

#[test]
fn halted_engine_without_a_stage_audits_risk_refused_then_rejected() {
    let venue = Sink::default();
    let mut engine = Engine::new();
    engine.set_execution(Box::new(venue.clone()));
    engine.set_trading_state(TradingState::Halted);
    engine.add_handler(script(vec![(
        1,
        EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 1.0, Some(10.0), 1)]),
    )]));
    run(&mut engine, &[(10.0, 1)]);

    assert_eq!(
        risk_kinds(&engine),
        vec![
            AuditKind::RiskRefused {
                order_id: "O-1".to_string(),
                refusal: RiskRefusal::TradingHalted,
            },
            AuditKind::OrderRejected {
                order_id: "O-1".to_string(),
                reason: "risk_trading_halted".to_string(),
            },
        ]
    );
    assert!(venue.submitted().is_empty());
}

#[test]
fn reducing_engine_without_a_stage_is_reduce_only() {
    let venue = Sink::default();
    let mut engine = Engine::new().with_positions([(any_instrument(), 10.0)]);
    engine.set_execution(Box::new(venue.clone()));
    engine.set_trading_state(TradingState::Reducing);
    engine.add_handler(script(vec![(
        1,
        EngineOutput::Orders(vec![
            order("B-1", OrderSide::Buy, 1.0, Some(10.0), 1),
            order("S-1", OrderSide::Sell, 10.0, Some(10.0), 1),
            order("S-2", OrderSide::Sell, 1.0, Some(10.0), 1),
        ]),
    )]));
    run(&mut engine, &[(10.0, 1)]);

    assert_eq!(venue.submitted(), vec!["S-1".to_string()]);
    let refused: Vec<String> = engine
        .audit()
        .iter()
        .filter_map(|r| match &r.kind {
            AuditKind::RiskRefused { order_id, refusal } => {
                Some(format!("{order_id} {}", refusal.rule()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(refused, vec!["B-1 reduce_only", "S-2 reduce_only"]);
}

#[test]
fn cancel_is_allowed_when_halted() {
    let venue = Sink::default();
    let mut engine = Engine::new();
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(script(vec![
        (
            1,
            EngineOutput::Orders(vec![order("O-1", OrderSide::Buy, 1.0, Some(10.0), 1)]),
        ),
        (2, EngineOutput::StateChange(TradingState::Halted)),
        (3, EngineOutput::Cancels(vec![OrderId::new("O-1")])),
    ]));
    run(&mut engine, &[(10.0, 1), (10.0, 2), (10.0, 3)]);

    assert_eq!(*venue.cancelled.lock().unwrap(), vec!["O-1".to_string()]);
    assert!(engine
        .audit()
        .iter()
        .any(|r| matches!(&r.kind, AuditKind::CancelRequested { order_id } if order_id == "O-1")));
}

#[test]
fn handlers_are_told_of_each_real_state_change() {
    let handler = script(Vec::new());
    let mut engine = Engine::new();
    engine.add_handler(handler.clone());
    engine.set_trading_state(TradingState::Reducing);
    engine.set_trading_state(TradingState::Reducing);
    engine.set_trading_state(TradingState::Halted);
    assert_eq!(
        *handler.states.lock().unwrap(),
        vec![TradingState::Reducing, TradingState::Halted]
    );
}

#[test]
fn reference_price_comes_from_the_last_bar_close() {
    // A market order has no price: the stage prices its notional from the last close.
    let limits = RiskLimits {
        max_notional: Some(500.0),
        order_rate: None,
        ..RiskLimits::default()
    };
    let venue = Sink::default();
    let mut engine = Engine::new().with_risk(stage(limits));
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(script(vec![
        (
            2,
            EngineOutput::Orders(vec![
                order("BIG", OrderSide::Buy, 10.0, None, 2),
                order("SMALL", OrderSide::Buy, 4.0, None, 2),
            ]),
        ),
        (
            1,
            EngineOutput::Orders(vec![order("FIRST", OrderSide::Buy, 1.0, None, 1)]),
        ),
    ]));
    run(&mut engine, &[(100.0, 1), (100.0, 2)]);

    let refused: Vec<String> = engine
        .audit()
        .iter()
        .filter_map(|r| match &r.kind {
            AuditKind::RiskRefused { order_id, refusal } => {
                Some(format!("{order_id} {}", refusal.rule()))
            }
            _ => None,
        })
        .collect();
    // The dispatched bar counts: FIRST = 100 passes, BIG = 1000 > 500, SMALL = 400 passes.
    assert_eq!(refused, vec!["BIG max_notional"]);
}
