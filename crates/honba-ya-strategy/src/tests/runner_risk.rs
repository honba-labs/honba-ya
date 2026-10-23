//! Unit tests for the runner's own risk stage and state rules (ADR 0018 decisions 6-8).

use std::collections::BTreeMap;
use std::sync::Arc;

use honba_engine::{AlgoError, AuditKind, Handler, OrderRejection};
use honba_entities::Currency;
use honba_market::{InstrumentRules, PriceBand};
use honba_messages::{
    Event, InstrumentId, OrderSide, OrderStatus, QuoteTick, TradingState, UnixNanos,
};
use honba_risk::{
    OrderRateLimit, RiskConfigError, RiskLimits, RiskRefusal, RiskStage, RulesSource,
};
use honba_sim::ScriptedExecution;
use honba_testing::VecFeed;
use honba_testing::fixtures::any_instrument;

use crate::{OrderIntent, Strategy, StrategyContext, StrategyRunner};

const MS: u64 = 1_000_000;

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

/// Emits the planned intents on the event at each planned `ts` (bars and quotes alike).
struct Script(Vec<(u64, Vec<OrderIntent>)>);

impl Script {
    fn emit(&mut self, ctx: &mut dyn StrategyContext) {
        let t = ctx.now().as_u64();
        if let Some(i) = self.0.iter().position(|(at, _)| *at == t) {
            for intent in self.0.remove(i).1 {
                ctx.submit(intent);
            }
        }
    }
}

impl Strategy for Script {
    fn name(&self) -> &str {
        "s"
    }
    fn on_bar(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _bar: &honba_messages::Bar,
    ) -> honba_engine::Result<()> {
        self.emit(ctx);
        Ok(())
    }
    fn on_quote(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _quote: &QuoteTick,
    ) -> honba_engine::Result<()> {
        self.emit(ctx);
        Ok(())
    }
}

fn buy(qty: f64) -> OrderIntent {
    OrderIntent::market_buy(any_instrument(), qty)
}

fn runner(
    plan: Vec<(u64, Vec<OrderIntent>)>,
) -> (StrategyRunner<Script, ScriptedExecution>, ScriptedExecution) {
    let venue = ScriptedExecution::new(10.0);
    (StrategyRunner::new(Script(plan), venue.clone()), venue)
}

fn bar(r: &mut impl Handler, ts: u64, close: f64) {
    let b = VecFeed::bar("X", close, ts).event().clone();
    r.on_event(&b, UnixNanos::from_u64(ts)).unwrap();
}

fn quote(r: &mut impl Handler, ts: u64) {
    let t = UnixNanos::from_u64(ts);
    let q = Event::Quote(QuoteTick::new(any_instrument(), 1.0, 2.0, 1.0, 1.0, t, t));
    r.on_event(&q, t).unwrap();
}

fn kinds<S: Strategy, E: honba_engine::ExecutionEngine>(
    r: &StrategyRunner<S, E>,
) -> Vec<AuditKind> {
    r.audit().records().iter().map(|a| a.kind.clone()).collect()
}

fn rejected(id: &str, reason: &str) -> AuditKind {
    AuditKind::OrderRejected {
        order_id: id.to_string(),
        reason: reason.to_string(),
    }
}

fn reasons<S: Strategy, E: honba_engine::ExecutionEngine>(r: &StrategyRunner<S, E>) -> Vec<String> {
    r.order_rejections()
        .iter()
        .map(|o: &OrderRejection| o.reason.clone())
        .collect()
}

#[test]
fn holds_risk_stage_only_with_a_stage() {
    let (plain, _) = runner(vec![]);
    assert!(!plain.holds_risk_stage());
    let (staged, _) = runner(vec![]);
    assert!(
        staged
            .with_risk(stage(RiskLimits::default(), true))
            .holds_risk_stage()
    );
}

#[test]
fn on_trading_state_is_tracked() {
    let (mut r, _) = runner(vec![]);
    assert_eq!(r.trading_state(), TradingState::Active);
    r.on_trading_state(TradingState::Reducing);
    assert_eq!(r.trading_state(), TradingState::Reducing);
}

#[test]
fn without_a_stage_halted_refuses_audits_and_never_reaches_the_venue() {
    let (mut r, venue) = runner(vec![(1, vec![buy(5.0)])]);
    r.on_trading_state(TradingState::Halted);
    bar(&mut r, 1, 10.0);
    assert_eq!(
        kinds(&r),
        vec![
            AuditKind::RiskRefused {
                order_id: "s-0".to_string(),
                refusal: RiskRefusal::TradingHalted,
            },
            rejected("s-0", "risk_trading_halted"),
        ]
    );
    assert_eq!(reasons(&r), vec!["risk_trading_halted"]);
    assert_eq!(r.order_state("s-0").unwrap().status, OrderStatus::Rejected);
    assert!(r.submitted().is_empty());
    assert!(venue.working_orders().is_empty());
    assert!(r.fills().is_empty(), "the venue saw a submit");
    assert!(
        !r.context().busy(&any_instrument()),
        "the refusal releases the intent"
    );
}

#[test]
fn without_a_stage_reducing_refuses_an_order_from_flat() {
    let (mut r, _) = runner(vec![(1, vec![buy(5.0)])]);
    r.on_trading_state(TradingState::Reducing);
    bar(&mut r, 1, 10.0);
    assert_eq!(
        kinds(&r)[0],
        AuditKind::RiskRefused {
            order_id: "s-0".to_string(),
            refusal: RiskRefusal::ReduceOnly {
                position: 0.0,
                side: OrderSide::Buy,
                quantity: 5.0,
            },
        }
    );
    assert_eq!(reasons(&r), vec!["risk_reduce_only_violation"]);
    assert!(r.fills().is_empty());
}

#[test]
fn a_stage_refusal_is_typed_audited_and_releases_the_intent() {
    let (r, venue) = runner(vec![(1, vec![buy(5.0)])]);
    let mut r = r.with_risk(stage(RiskLimits::default(), false));
    bar(&mut r, 1, 10.0);
    assert_eq!(
        kinds(&r),
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
    assert_eq!(reasons(&r), vec!["risk_instrument_unknown"]);
    assert_eq!(r.released_quantity(&any_instrument()), 5.0);
    assert!(!r.context().busy(&any_instrument()));
    assert!(venue.working_orders().is_empty());
    assert!(r.fills().is_empty());
}

#[test]
fn an_approved_order_reaches_the_venue_and_is_not_audited_as_refused() {
    let (r, _) = runner(vec![(1, vec![buy(5.0)])]);
    let mut r = r.with_risk(stage(RiskLimits::default(), true));
    bar(&mut r, 1, 10.0);
    assert_eq!(r.submitted().len(), 1);
    assert_eq!(r.fills().len(), 1);
    assert!(r.audit().is_empty());
    assert!(r.order_rejections().is_empty());
}

#[test]
fn reference_price_is_the_last_bar_close_seen_before_the_order() {
    let limits = RiskLimits {
        max_notional: Some(500.0),
        order_rate: None,
        ..RiskLimits::default()
    };
    // 100 x close 10 = 1000 > 500, priced from the bar that triggered the order.
    let (r, _) = runner(vec![(1, vec![buy(100.0)])]);
    let mut r = r.with_risk(stage(limits.clone(), true));
    bar(&mut r, 1, 10.0);
    assert_eq!(reasons(&r), vec!["risk_max_notional_exceeded"]);
    assert!(matches!(
        kinds(&r)[0],
        AuditKind::RiskRefused {
            refusal: RiskRefusal::MaxNotional { .. },
            ..
        }
    ));

    // No bar or trade yet: nothing to price a market order with.
    let (r, _) = runner(vec![(1, vec![buy(1.0)])]);
    let mut r = r.with_risk(stage(limits, true));
    quote(&mut r, 1);
    assert!(matches!(
        kinds(&r)[0],
        AuditKind::RiskRefused {
            refusal: RiskRefusal::MaxNotionalUnpriceable { .. },
            ..
        }
    ));
}

#[test]
fn order_rate_window_runs_on_event_time() {
    let limits = RiskLimits {
        max_notional: None,
        order_rate: Some(OrderRateLimit {
            max_orders: 1,
            window_ms: 1000,
        }),
        ..RiskLimits::default()
    };
    let plan = vec![
        (MS, vec![buy(1.0)]),
        (2 * MS, vec![buy(1.0)]),
        (1001 * MS, vec![buy(1.0)]),
    ];
    let (r, _) = runner(plan);
    let mut r = r.with_risk(stage(limits, true));
    for ts in [MS, 2 * MS, 1001 * MS] {
        bar(&mut r, ts, 10.0);
    }
    // s-1 is refused; by 1001 ms the first order left the window.
    assert_eq!(reasons(&r), vec!["risk_order_rate_exceeded"]);
    assert_eq!(r.submitted().len(), 2);
    assert_eq!(
        kinds(&r)[0],
        AuditKind::RiskRefused {
            order_id: "s-1".to_string(),
            refusal: RiskRefusal::OrderRate {
                count: 1,
                max_orders: 1,
                window_ms: 1000,
            },
        }
    );
}

#[test]
fn require_live_limits_needs_a_stage_with_both_limits() {
    let refused = Err(AlgoError::RiskConfig(RiskConfigError::LiveRunWithoutLimit));
    let (plain, _) = runner(vec![]);
    assert_eq!(plain.require_live_limits(), refused);
    let (r, _) = runner(vec![]);
    let notional_only = RiskLimits {
        max_notional: Some(1000.0),
        order_rate: None,
        ..RiskLimits::default()
    };
    assert_eq!(
        r.with_risk(stage(notional_only, true))
            .require_live_limits(),
        refused
    );
    let full = RiskLimits {
        max_notional: Some(1000.0),
        order_rate: Some(OrderRateLimit {
            max_orders: 5,
            window_ms: 1000,
        }),
        ..RiskLimits::default()
    };
    let (r, _) = runner(vec![]);
    assert_eq!(r.with_risk(stage(full, true)).require_live_limits(), Ok(()));
}
