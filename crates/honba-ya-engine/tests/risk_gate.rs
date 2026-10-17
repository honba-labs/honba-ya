//! The risk stage through the real flow (ADR 0018 decisions 6-8): handler ->
//! `Engine::submit` -> stage -> `ScriptedExecution`, with the audit and the
//! lifecycle events the handler sees.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use honba_engine::{AlgoError, AuditKind, Engine, EngineOutput, Handler, Result};
use honba_entities::Currency;
use honba_market::InstrumentRules;
use honba_messages::{
    Event, Exchange, InstrumentId, Message, Order, OrderId, OrderSide, OrderType, QuoteTick,
    TimeInForce, TradingState, UnixNanos,
};
use honba_risk::{
    OrderRateLimit, RiskConfigError, RiskLimits, RiskRefusal, RiskStage, RulesSource,
};
use honba_sim::{Behavior, ScriptedExecution};

const MS: u64 = 1_000_000;

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn x() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(x(), 1.0, 2.0, 1.0, 1.0, ts(t), ts(t))),
        ts(t),
    )
}

fn limit(id: &str, side: OrderSide, qty: f64, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        x(),
        side,
        OrderType::Limit,
        qty,
        Some(10.0),
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

struct Rules(BTreeMap<InstrumentId, InstrumentRules>);

impl RulesSource for Rules {
    fn rules(
        &self,
        id: &InstrumentId,
    ) -> Option<(InstrumentRules, Option<honba_market::PriceBand>)> {
        self.0.get(id).map(|r| (r.clone(), None))
    }
}

fn stage(limits: RiskLimits, known: bool) -> RiskStage {
    let mut m = BTreeMap::new();
    if known {
        m.insert(x(), InstrumentRules::new(1.0, 0.05));
    }
    RiskStage::new(limits, Currency::Inr, Arc::new(Rules(m))).expect("valid limits")
}

type Steps = Vec<(u64, Vec<Order>)>;

/// Emits the scripted orders at each quote's ts and logs the lifecycle it sees.
#[derive(Clone)]
struct Strategy {
    steps: Arc<Mutex<Steps>>,
    seen: Arc<Mutex<Vec<String>>>,
    holds_stage: bool,
}

impl Strategy {
    fn new(steps: Steps) -> Self {
        Self {
            steps: Arc::new(Mutex::new(steps)),
            seen: Arc::new(Mutex::new(Vec::new())),
            holds_stage: false,
        }
    }

    fn holding_stage() -> Self {
        Self {
            holds_stage: true,
            ..Self::new(Vec::new())
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

impl Handler for Strategy {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        match event {
            Event::Quote(_) => {
                let t = event.ts_event().as_u64();
                let mut steps = self.steps.lock().unwrap();
                Ok(match steps.iter().position(|(at, _)| *at == t) {
                    Some(i) => EngineOutput::Orders(steps.remove(i).1),
                    None => EngineOutput::None,
                })
            }
            Event::OrderRejected {
                order_id, reason, ..
            } => {
                self.seen
                    .lock()
                    .unwrap()
                    .push(format!("rejected {} {reason}", order_id.as_str()));
                Ok(EngineOutput::None)
            }
            _ => Ok(EngineOutput::None),
        }
    }

    fn holds_risk_stage(&self) -> bool {
        self.holds_stage
    }
}

fn run(engine: &mut Engine, quotes: &[u64]) {
    engine.start().unwrap();
    for t in quotes {
        engine.inject(quote(*t));
    }
    engine.finish().unwrap();
}

fn kinds(engine: &Engine) -> Vec<AuditKind> {
    engine
        .audit()
        .iter()
        .map(|r| r.kind.clone())
        .filter(|k| !matches!(k, AuditKind::EventDispatched { .. }))
        .collect()
}

fn rejected(id: &str, reason: &str) -> AuditKind {
    AuditKind::OrderRejected {
        order_id: id.to_string(),
        reason: reason.to_string(),
    }
}

#[test]
fn with_risk_refusal_audits_risk_refused_then_order_rejected_and_reaches_no_venue() {
    let venue = ScriptedExecution::new(10.0).with("O-1", Behavior::Hold);
    let strategy = Strategy::new(vec![(1, vec![limit("O-1", OrderSide::Buy, 5.0, 1)])]);
    let mut engine = Engine::new().with_risk(stage(RiskLimits::default(), false));
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(strategy.clone());
    run(&mut engine, &[1]);

    assert_eq!(
        kinds(&engine),
        vec![
            AuditKind::RiskRefused {
                order_id: "O-1".to_string(),
                refusal: RiskRefusal::InstrumentUnknown { instrument_id: x() },
            },
            rejected("O-1", "risk_instrument_unknown"),
        ]
    );
    assert!(venue.working_orders().is_empty(), "the venue saw a submit");
    assert_eq!(
        strategy.seen(),
        vec!["rejected O-1 risk_instrument_unknown"]
    );
}

#[test]
fn reduce_only_two_orders_in_flight() {
    // Long 100, working sell 60, new sell 50: p = 40, 40 - 50 crosses zero.
    let venue = ScriptedExecution::new(10.0)
        .with("S-1", Behavior::Hold)
        .with("S-2", Behavior::Hold);
    let strategy = Strategy::new(vec![(
        1,
        vec![
            limit("S-1", OrderSide::Sell, 60.0, 1),
            limit("S-2", OrderSide::Sell, 50.0, 1),
        ],
    )]);
    let mut engine = Engine::new()
        .with_risk(stage(RiskLimits::default(), true))
        .with_positions([(x(), 100.0)]);
    engine.set_trading_state(TradingState::Reducing);
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(strategy.clone());
    run(&mut engine, &[1]);

    assert_eq!(venue.working_orders(), vec!["S-1".to_string()]);
    let refusals: Vec<_> = kinds(&engine)
        .into_iter()
        .filter(|k| matches!(k, AuditKind::RiskRefused { .. }))
        .collect();
    assert_eq!(
        refusals,
        vec![AuditKind::RiskRefused {
            order_id: "S-2".to_string(),
            refusal: RiskRefusal::ReduceOnly {
                position: 40.0,
                side: OrderSide::Sell,
                quantity: 50.0,
            },
        }]
    );
    assert_eq!(
        strategy.seen(),
        vec!["rejected S-2 risk_reduce_only_violation"]
    );
}

#[test]
fn order_rate_refused_in_event_time() {
    let limits = RiskLimits {
        max_notional: None,
        order_rate: Some(OrderRateLimit {
            max_orders: 2,
            window_ms: 1000,
        }),
        ..RiskLimits::default()
    };
    let venue = ScriptedExecution::new(10.0);
    let steps = [(1, "A"), (2, "B"), (3, "C"), (1002, "D")]
        .into_iter()
        .map(|(ms, id)| (ms * MS, vec![limit(id, OrderSide::Buy, 1.0, ms * MS)]))
        .collect();
    let strategy = Strategy::new(steps);
    let mut engine = Engine::new().with_risk(stage(limits, true));
    engine.set_execution(Box::new(venue));
    engine.add_handler(strategy.clone());
    run(&mut engine, &[MS, 2 * MS, 3 * MS, 1002 * MS]);

    let refused: Vec<_> = kinds(&engine)
        .into_iter()
        .filter(|k| {
            matches!(
                k,
                AuditKind::RiskRefused { .. } | AuditKind::OrderRejected { .. }
            )
        })
        .collect();
    // C is the third within one second; by D (1002 ms) A and B have left the window.
    assert_eq!(
        refused,
        vec![
            AuditKind::RiskRefused {
                order_id: "C".to_string(),
                refusal: RiskRefusal::OrderRate {
                    count: 2,
                    max_orders: 2,
                    window_ms: 1000,
                },
            },
            rejected("C", "risk_order_rate_exceeded"),
        ]
    );
}

#[test]
fn double_stage_rejected_at_build() {
    // Engine stage + a handler holding one.
    let mut engine = Engine::new().with_risk(stage(RiskLimits::default(), true));
    engine.add_handler(Strategy::holding_stage());
    assert_eq!(engine.start(), Err(AlgoError::DuplicateRiskStage));
    assert_eq!(
        engine.run(&mut Empty),
        Err(AlgoError::DuplicateRiskStage),
        "run starts through the same guard"
    );

    // Two handlers each holding one.
    let mut engine = Engine::new();
    engine.add_handler(Strategy::holding_stage());
    engine.add_handler(Strategy::holding_stage());
    assert_eq!(engine.start(), Err(AlgoError::DuplicateRiskStage));

    // One stage anywhere is fine.
    let mut engine = Engine::new().with_risk(stage(RiskLimits::default(), true));
    engine.add_handler(Strategy::new(Vec::new()));
    assert_eq!(engine.start(), Ok(()));
    let mut engine = Engine::new();
    engine.add_handler(Strategy::holding_stage());
    assert_eq!(engine.start(), Ok(()));
}

struct Empty;

impl honba_engine::DataFeed for Empty {
    fn next(&mut self) -> Result<Option<Message>> {
        Ok(None)
    }
}

#[test]
fn live_run_without_limits_refused() {
    let full = RiskLimits {
        max_notional: Some(500_000.0),
        order_rate: Some(OrderRateLimit {
            max_orders: 30,
            window_ms: 1000,
        }),
        ..RiskLimits::default()
    };
    let only_notional = RiskLimits {
        max_notional: Some(500_000.0),
        order_rate: None,
        ..RiskLimits::default()
    };
    let refused = Err(AlgoError::RiskConfig(RiskConfigError::LiveRunWithoutLimit));

    // No stage at all.
    assert_eq!(Engine::new().require_live_limits(), refused);
    // A stage with default or partial limits.
    let engine = Engine::new().with_risk(stage(RiskLimits::default(), true));
    assert_eq!(engine.require_live_limits(), refused);
    let engine = Engine::new().with_risk(stage(only_notional, true));
    assert_eq!(engine.require_live_limits(), refused);
    // Both set.
    let engine = Engine::new().with_risk(stage(full, true));
    assert_eq!(engine.require_live_limits(), Ok(()));
}

#[test]
fn stale_feed_scenario() {
    let y = InstrumentId::new("Y", Exchange::new("NSE"));
    let limits = RiskLimits {
        stale_after_ms: Some(1000), // 1.0s staleness threshold
        ..RiskLimits::default()
    };
    let venue = ScriptedExecution::new(10.0).with("O-2", Behavior::Hold);

    let mut rules_map = BTreeMap::new();
    rules_map.insert(x(), InstrumentRules::new(1.0, 0.05));
    rules_map.insert(y.clone(), InstrumentRules::new(1.0, 0.05));
    let stage = RiskStage::new(limits, Currency::Inr, Arc::new(Rules(rules_map))).unwrap();

    // Strategy triggers on Y quotes to buy X:
    // At t=2500ms (Y quote): X last quote was at 1000ms -> age 1500ms > 1000ms -> refused as stale!
    // At t=2600ms (X quote): X feed refreshed at 2600ms
    // At t=2700ms (Y quote): X last quote was at 2600ms -> age 100ms <= 1000ms -> approved!
    let strategy = Strategy::new(vec![
        (
            2500 * MS,
            vec![limit("O-1", OrderSide::Buy, 1.0, 2500 * MS)],
        ),
        (
            2700 * MS,
            vec![limit("O-2", OrderSide::Buy, 1.0, 2700 * MS)],
        ),
    ]);

    let mut engine = Engine::new().with_risk(stage);
    engine.set_execution(Box::new(venue.clone()));
    engine.add_handler(strategy.clone());

    engine.start().unwrap();
    engine.inject(Message::new(
        Event::Quote(QuoteTick::new(
            x(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(1000 * MS),
            ts(1000 * MS),
        )),
        ts(1000 * MS),
    ));
    engine.inject(Message::new(
        Event::Quote(QuoteTick::new(
            y.clone(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(2500 * MS),
            ts(2500 * MS),
        )),
        ts(2500 * MS),
    ));
    engine.inject(Message::new(
        Event::Quote(QuoteTick::new(
            x(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(2600 * MS),
            ts(2600 * MS),
        )),
        ts(2600 * MS),
    ));
    engine.inject(Message::new(
        Event::Quote(QuoteTick::new(
            y.clone(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(2700 * MS),
            ts(2700 * MS),
        )),
        ts(2700 * MS),
    ));
    engine.finish().unwrap();

    // O-1 was refused for stale feed
    let refused: Vec<_> = kinds(&engine)
        .into_iter()
        .filter(|k| {
            matches!(
                k,
                AuditKind::RiskRefused { .. } | AuditKind::OrderRejected { .. }
            )
        })
        .collect();
    assert_eq!(
        refused,
        vec![
            AuditKind::RiskRefused {
                order_id: "O-1".to_string(),
                refusal: RiskRefusal::FeedStale {
                    instrument_id: x(),
                    age_ns: 1500 * MS,
                    stale_after_ns: 1000 * MS,
                },
            },
            rejected("O-1", "risk_feed_stale"),
        ]
    );

    // O-2 was approved and reached the venue
    assert_eq!(venue.working_orders(), vec!["O-2".to_string()]);
}

// ---- Market-order price collar fed from the quote stream (ADR 0018 addendum) ----

fn market(id: &str, side: OrderSide, t: u64) -> Order {
    Order::new(
        OrderId::new(id),
        x(),
        side,
        OrderType::Market,
        1.0,
        None,
        TimeInForce::Day,
        ts(t),
        ts(t),
    )
}

fn trade_at(px: f64, t: u64) -> Message {
    Message::new(
        Event::Trade(honba_messages::TradeTick::new(
            x(),
            px,
            1.0,
            honba_messages::AggressorSide::Buyer,
            honba_messages::TradeId::new("T"),
            ts(t),
            ts(t),
        )),
        ts(t),
    )
}

fn quote_at(bid: f64, ask: f64, t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(x(), bid, ask, 1.0, 1.0, ts(t), ts(t))),
        ts(t),
    )
}

/// Reference 100 (trade), then quote bid/ask; the strategy fires at the quote's ts.
fn collar_run(collar_bps: Option<u32>, bid: f64, ask: f64, side: OrderSide) -> Vec<AuditKind> {
    let limits = RiskLimits {
        price_collar_bps: collar_bps,
        ..RiskLimits::default()
    };
    let venue = ScriptedExecution::new(100.0).with("O-1", Behavior::Hold);
    let strategy = Strategy::new(vec![(2, vec![market("O-1", side, 2)])]);
    let mut engine = Engine::new().with_risk(stage(limits, true));
    engine.set_execution(Box::new(venue));
    engine.add_handler(strategy);
    engine.start().unwrap();
    engine.inject(trade_at(100.0, 1));
    engine.inject(quote_at(bid, ask, 2));
    engine.finish().unwrap();
    kinds(&engine)
}

#[test]
fn collar_refuses_market_buy_when_ask_is_outside_and_sell_when_bid_is_outside() {
    let buy = collar_run(Some(100), 100.0, 103.0, OrderSide::Buy);
    assert_eq!(
        buy,
        vec![
            AuditKind::RiskRefused {
                order_id: "O-1".to_string(),
                refusal: RiskRefusal::PriceCollar {
                    side: OrderSide::Buy,
                    expected_price: 103.0,
                    reference_price: 100.0,
                    deviation_bps: 300.0,
                    collar_bps: 100,
                },
            },
            rejected("O-1", "risk_price_collar"),
        ]
    );
    let sell = collar_run(Some(100), 97.0, 100.0, OrderSide::Sell);
    assert!(matches!(
        &sell[0],
        AuditKind::RiskRefused {
            refusal: RiskRefusal::PriceCollar { expected_price, .. },
            ..
        } if *expected_price == 97.0
    ));
}

#[test]
fn collar_uses_the_touch_on_the_orders_side() {
    // Ask is wild but a sell crosses the bid, which is inside the collar.
    let kinds = collar_run(Some(100), 100.5, 150.0, OrderSide::Sell);
    assert!(
        !kinds
            .iter()
            .any(|k| matches!(k, AuditKind::RiskRefused { .. })),
        "{kinds:?}"
    );
}

#[test]
fn collar_passes_inside_and_is_inert_when_unset() {
    for bps in [Some(100), None] {
        let ask = if bps.is_some() { 100.5 } else { 150.0 };
        let kinds = collar_run(bps, 100.0, ask, OrderSide::Buy);
        assert!(
            !kinds
                .iter()
                .any(|k| matches!(k, AuditKind::RiskRefused { .. })),
            "{bps:?}: {kinds:?}"
        );
    }
}

#[test]
fn collar_is_deterministic_across_runs() {
    let a = collar_run(Some(100), 100.0, 103.0, OrderSide::Buy);
    let b = collar_run(Some(100), 100.0, 103.0, OrderSide::Buy);
    assert_eq!(a, b);
}
