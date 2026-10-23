//! The risk stage at the runner's gate through the real flow (ADR 0018 decisions 6-8, 11):
//! feed -> `Engine` -> `StrategyRunner` -> stage -> `ScriptedExecution`, simulated clock and feed.
//!
//! The runner is the submitter here, so it bypasses `Engine::submit`: its refusals are audited
//! in its own log (`StrategyRunner::audit`), not in the engine's.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use honba_engine::{AlgoError, AuditKind, Engine, EngineOutput, Handler, OrderRejection, Result};
use honba_entities::Currency;
use honba_market::{InstrumentRules, PriceBand};
use honba_messages::{Bar, Event, InstrumentId, OrderSide, OrderStatus, TradingState, UnixNanos};
use honba_risk::{
    OrderRateLimit, RiskConfigError, RiskLimits, RiskRefusal, RiskStage, RulesSource,
};
use honba_sim::{Behavior, ScriptedExecution};
use honba_strategy::{OrderIntent, Strategy, StrategyContext, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::any_instrument;

struct Rules(BTreeMap<InstrumentId, InstrumentRules>);

impl RulesSource for Rules {
    fn rules(&self, id: &InstrumentId) -> Option<(InstrumentRules, Option<PriceBand>)> {
        self.0.get(id).map(|r| (r.clone(), None))
    }
}

fn stage(limits: RiskLimits, known: bool) -> RiskStage {
    let mut m = BTreeMap::new();
    if known {
        m.insert(any_instrument(), InstrumentRules::new(1.0, 0.05));
    }
    RiskStage::new(limits, Currency::Inr, Arc::new(Rules(m))).expect("valid limits")
}

/// Emits the planned intents on the bar at each planned ts.
struct Script(Vec<(u64, Vec<OrderIntent>)>);

impl Strategy for Script {
    fn name(&self) -> &str {
        "s"
    }
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        let t = ctx.now().as_u64();
        if let Some(i) = self.0.iter().position(|(at, _)| *at == t) {
            for intent in self.0.remove(i).1 {
                ctx.submit(intent);
            }
        }
        Ok(())
    }
}

type Runner = StrategyRunner<Script, ScriptedExecution>;

/// Hosts a runner in an engine while the test keeps a handle on it.
#[derive(Clone)]
struct Hosted(Arc<Mutex<Runner>>);

impl Hosted {
    fn new(runner: Runner) -> Self {
        Self(Arc::new(Mutex::new(runner)))
    }
    fn with<T>(&self, f: impl FnOnce(&Runner) -> T) -> T {
        f(&self.0.lock().unwrap())
    }
}

impl Handler for Hosted {
    fn on_start(&mut self) -> Result<()> {
        self.0.lock().unwrap().on_start()
    }
    fn on_event(&mut self, event: &Event, ts_init: UnixNanos) -> Result<EngineOutput> {
        self.0.lock().unwrap().on_event(event, ts_init)
    }
    fn on_trading_state(&mut self, state: TradingState) {
        self.0.lock().unwrap().on_trading_state(state);
    }
    fn holds_risk_stage(&self) -> bool {
        self.0.lock().unwrap().holds_risk_stage()
    }
    fn drain_audit(&mut self) -> Vec<AuditKind> {
        self.0.lock().unwrap().drain_audit()
    }
    fn on_stop(&mut self) -> Result<()> {
        self.0.lock().unwrap().on_stop()
    }
}

/// An operator: moves the engine to `state` when it sees the bar at `at`.
struct Operator {
    at: u64,
    state: TradingState,
}

impl Handler for Operator {
    fn on_event(&mut self, event: &Event, _ts_init: UnixNanos) -> Result<EngineOutput> {
        Ok(match event {
            Event::Bar(_) if event.ts_event().as_u64() == self.at => {
                EngineOutput::StateChange(self.state)
            }
            _ => EngineOutput::None,
        })
    }
}

fn feed(ts: &[u64]) -> VecFeed {
    VecFeed::new(ts.iter().map(|t| VecFeed::bar("X", 10.0, *t)).collect())
}

fn buy(qty: f64) -> OrderIntent {
    OrderIntent::market_buy(any_instrument(), qty)
}

fn sell(qty: f64) -> OrderIntent {
    OrderIntent::market_sell(any_instrument(), qty)
}

fn runner(plan: Vec<(u64, Vec<OrderIntent>)>, venue: &ScriptedExecution) -> Runner {
    StrategyRunner::new(Script(plan), venue.clone())
}

fn kinds(h: &Hosted) -> Vec<AuditKind> {
    h.with(|r| r.audit().records().iter().map(|a| a.kind.clone()).collect())
}

fn reasons(h: &Hosted) -> Vec<String> {
    h.with(|r| {
        r.order_rejections()
            .iter()
            .map(|o: &OrderRejection| o.reason.clone())
            .collect()
    })
}

fn rejected(id: &str, reason: &str) -> AuditKind {
    AuditKind::OrderRejected {
        order_id: id.to_string(),
        reason: reason.to_string(),
    }
}

#[test]
fn strategy_engine_risk_fills_refusal_in_audit() {
    let venue = ScriptedExecution::new(10.0);
    let hosted = Hosted::new(
        runner(vec![(1, vec![buy(5.0)])], &venue).with_risk(stage(RiskLimits::default(), false)),
    );
    let mut engine = Engine::new();
    engine.add_handler(hosted.clone());
    engine.run(&mut feed(&[1, 2])).unwrap();

    assert_eq!(
        kinds(&hosted),
        vec![
            AuditKind::RiskRefused {
                order_id: "s-0".to_string(),
                refusal: RiskRefusal::InstrumentUnknown {
                    instrument_id: any_instrument(),
                },
            },
            rejected("s-0", "risk_instrument_unknown"),
        ]
    );
    // The ENGINE's audit shows the refusal, RiskRefused strictly before OrderRejected.
    let engine_risk: Vec<AuditKind> = engine
        .audit()
        .iter()
        .map(|a| a.kind.clone())
        .filter(|k| {
            matches!(
                k,
                AuditKind::RiskRefused { .. } | AuditKind::OrderRejected { .. }
            )
        })
        .collect();
    assert_eq!(engine_risk, kinds(&hosted));
    let seqs: Vec<u64> = engine.audit().iter().map(|a| a.seq).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());
    // The strategy side sees the rejection with the ErrorCode spelling, and the order is Rejected.
    assert_eq!(reasons(&hosted), vec!["risk_instrument_unknown"]);
    hosted.with(|r| {
        assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Rejected);
        assert!(r.fills().is_empty(), "the sim saw a submit");
        assert!(r.submitted().is_empty());
        assert!(!honba_strategy::StrategyContext::busy(
            r.context(),
            &any_instrument()
        ));
    });
    assert!(venue.working_orders().is_empty());
}

#[test]
fn halt_stops_hosted_runner() {
    for with_stage in [false, true] {
        let venue = ScriptedExecution::new(10.0).with("s-0", Behavior::Hold);
        let mut r = runner(vec![(1, vec![buy(5.0)]), (2, vec![buy(5.0)])], &venue);
        if with_stage {
            r = r.with_risk(stage(RiskLimits::default(), true));
        }
        let hosted = Hosted::new(r);
        let mut engine = Engine::new();
        engine.add_handler(Operator {
            at: 2,
            state: TradingState::Halted,
        });
        engine.add_handler(hosted.clone());
        engine.run(&mut feed(&[1, 2, 3])).unwrap();

        assert_eq!(engine.trading_state(), TradingState::Halted);
        hosted.with(|r| assert_eq!(r.trading_state(), TradingState::Halted));
        // The first order (before the halt) reached the venue; the second was refused.
        assert_eq!(venue.working_orders(), vec!["s-0".to_string()]);
        assert_eq!(
            reasons(&hosted),
            vec!["risk_trading_halted"],
            "stage: {with_stage}"
        );
        assert_eq!(
            kinds(&hosted),
            vec![
                AuditKind::RiskRefused {
                    order_id: "s-1".to_string(),
                    refusal: RiskRefusal::TradingHalted,
                },
                rejected("s-1", "risk_trading_halted"),
            ]
        );
    }
}

#[test]
fn reduce_only_two_orders_in_flight() {
    // Long 100 (filled), working sell 60, new sell 50: p = 40, 40 - 50 crosses zero.
    for with_stage in [false, true] {
        let venue = ScriptedExecution::new(10.0).with("s-1", Behavior::Hold);
        let plan = vec![(1, vec![buy(100.0)]), (2, vec![sell(60.0), sell(50.0)])];
        let mut r = runner(plan, &venue);
        if with_stage {
            r = r.with_risk(stage(RiskLimits::default(), true));
        }
        let hosted = Hosted::new(r);
        let mut engine = Engine::new();
        engine.add_handler(Operator {
            at: 2,
            state: TradingState::Reducing,
        });
        engine.add_handler(hosted.clone());
        engine.run(&mut feed(&[1, 2, 3])).unwrap();

        assert_eq!(venue.working_orders(), vec!["s-1".to_string()]);
        assert_eq!(
            kinds(&hosted)[0],
            AuditKind::RiskRefused {
                order_id: "s-2".to_string(),
                refusal: RiskRefusal::ReduceOnly {
                    position: 40.0,
                    side: OrderSide::Sell,
                    quantity: 50.0,
                },
            },
            "stage: {with_stage}"
        );
        assert_eq!(reasons(&hosted), vec!["risk_reduce_only_violation"]);
    }
}

#[test]
fn double_stage_rejected_at_build() {
    let venue = ScriptedExecution::new(10.0);
    let staged =
        || Hosted::new(runner(vec![], &venue).with_risk(stage(RiskLimits::default(), true)));

    // Engine stage + a runner holding one.
    let mut engine = Engine::new().with_risk(stage(RiskLimits::default(), true));
    engine.add_handler(staged());
    assert_eq!(engine.start(), Err(AlgoError::DuplicateRiskStage));
    assert_eq!(
        engine.run(&mut feed(&[1])),
        Err(AlgoError::DuplicateRiskStage),
        "run starts through the same guard"
    );

    // Two runners each holding one.
    let mut engine = Engine::new();
    engine.add_handler(staged());
    engine.add_handler(staged());
    assert_eq!(engine.start(), Err(AlgoError::DuplicateRiskStage));

    // One stage anywhere is fine.
    let mut engine = Engine::new().with_risk(stage(RiskLimits::default(), true));
    engine.add_handler(Hosted::new(runner(vec![], &venue)));
    assert_eq!(engine.start(), Ok(()));
    let mut engine = Engine::new();
    engine.add_handler(staged());
    engine.add_handler(Hosted::new(runner(vec![], &venue)));
    assert_eq!(engine.start(), Ok(()));
}

/// What an assembler for a non-simulated run does before it starts the runner.
fn assemble_live(runner: Runner) -> Result<Runner> {
    runner.require_live_limits()?;
    Ok(runner)
}

#[test]
fn live_run_without_limits_refused() {
    let venue = ScriptedExecution::new(10.0);
    let refused = || AlgoError::RiskConfig(RiskConfigError::LiveRunWithoutLimit);
    let only_notional = RiskLimits {
        max_notional: Some(500_000.0),
        order_rate: None,
        ..RiskLimits::default()
    };
    let full = RiskLimits {
        max_notional: Some(500_000.0),
        order_rate: Some(OrderRateLimit {
            max_orders: 30,
            window_ms: 1000,
        }),
        ..RiskLimits::default()
    };

    // No stage, default limits, partial limits: refused.
    assert_eq!(assemble_live(runner(vec![], &venue)).err(), Some(refused()));
    for limits in [RiskLimits::default(), only_notional] {
        let r = runner(vec![], &venue).with_risk(stage(limits, true));
        assert_eq!(assemble_live(r).err(), Some(refused()));
    }
    // Both limits set: the run may start.
    let r = runner(vec![], &venue).with_risk(stage(full, true));
    assert!(assemble_live(r).is_ok());
}

// ---- Market-order price collar fed from the quote stream (ADR 0018 addendum) ----

fn collar_limits(bps: Option<u32>) -> RiskLimits {
    RiskLimits {
        price_collar_bps: bps,
        ..RiskLimits::default()
    }
}

/// Quote (bid, ask) at ts 1, then the strategy trades on the bar at ts 2 (close 10).
fn collar_hosted(bps: Option<u32>, bid: f64, ask: f64, order: OrderIntent) -> Hosted {
    let venue = ScriptedExecution::new(10.0).with("s-0", Behavior::Hold);
    let hosted = Hosted::new(
        runner(vec![(2, vec![order])], &venue).with_risk(stage(collar_limits(bps), true)),
    );
    let mut engine = Engine::new();
    engine.add_handler(hosted.clone());
    let mut feed = VecFeed::new(vec![
        VecFeed::quote("X", bid, ask, 1),
        VecFeed::bar("X", 10.0, 2),
    ]);
    engine.run(&mut feed).unwrap();
    hosted
}

#[test]
fn collar_refuses_market_orders_outside_the_touch_collar() {
    let buy_h = collar_hosted(Some(100), 10.0, 11.0, buy(1.0));
    assert!(matches!(
        &kinds(&buy_h)[0],
        AuditKind::RiskRefused {
            refusal: RiskRefusal::PriceCollar { expected_price, reference_price, .. },
            ..
        } if *expected_price == 11.0 && *reference_price == 10.0
    ));
    assert_eq!(reasons(&buy_h), vec!["risk_price_collar"]);

    let sell_h = collar_hosted(Some(100), 9.0, 10.0, sell(1.0));
    assert_eq!(reasons(&sell_h), vec!["risk_price_collar"]);
}

#[test]
fn collar_passes_inside_and_is_inert_when_unset() {
    let inside = collar_hosted(Some(100), 10.0, 10.05, buy(1.0));
    assert!(reasons(&inside).is_empty());
    let unset = collar_hosted(None, 10.0, 11.0, buy(1.0));
    assert!(reasons(&unset).is_empty());
    // A sell crosses the bid, so a wild ask does not matter.
    let sell_ok = collar_hosted(Some(100), 10.0, 20.0, sell(1.0));
    assert!(reasons(&sell_ok).is_empty());
}
