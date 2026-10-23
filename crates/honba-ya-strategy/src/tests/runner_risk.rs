//! Unit tests for the runner's own risk stage and state rules (ADR 0018 decisions 6-8).

use honba_core::{Bar, MarketEvent, Price, Quantity, Tick, TradingState};
use honba_mon::{OrderRateLimit, OtrConfig, RiskEngine, RiskLimits};
use honba_testing::fixtures::{any_instrument, exact_decimal, flat_bar, utc_nanos};
use rust_decimal::Decimal;

use crate::{OrderIntent, OrderRejection, Result, Strategy, StrategyContext, StrategyRunner};

const MS: u64 = 1_000_000;

fn stage(limits: RiskLimits, registered: bool) -> RiskEngine {
    let mut engine = RiskEngine::new(limits, OtrConfig::default());
    if registered {
        engine.register_algo("s");
    } else {
        engine.register_algo("other-algo");
    }
    engine
}

/// Emits the planned intents on the event at each planned `ts` (bars and quotes alike).
struct Script(Vec<(u64, Vec<OrderIntent>)>);

impl Script {
    fn emit(&mut self, ctx: &mut dyn StrategyContext) {
        let t = ctx.now().timestamp_nanos_opt().unwrap_or(0) as u64;
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
    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        self.emit(ctx);
        Ok(())
    }
    fn on_quote(&mut self, ctx: &mut dyn StrategyContext, _quote: &Tick) -> Result<()> {
        self.emit(ctx);
        Ok(())
    }
}

fn buy(qty: f64) -> OrderIntent {
    OrderIntent::market_buy(any_instrument(), Quantity::new(exact_decimal(qty)))
}

fn limit_buy(qty: f64, px: f64) -> OrderIntent {
    OrderIntent::limit_buy(
        any_instrument(),
        Quantity::new(exact_decimal(qty)),
        Price::new(exact_decimal(px)),
    )
}

fn runner(plan: Vec<(u64, Vec<OrderIntent>)>) -> StrategyRunner<Script> {
    StrategyRunner::new(Script(plan))
}

fn bar(r: &mut StrategyRunner<Script>, ts: u64, close: f64) {
    let b = flat_bar("X", close, ts);
    r.on_event(&MarketEvent::Bar(b), utc_nanos(ts)).unwrap();
}

fn reasons<S: Strategy>(r: &StrategyRunner<S>) -> Vec<String> {
    r.order_rejections()
        .iter()
        .map(|o: &OrderRejection| o.reason.clone())
        .collect()
}

#[test]
fn holds_risk_engine_only_when_attached() {
    let plain = runner(vec![]);
    assert!(plain.risk_engine().is_none());
    let staged = runner(vec![]).with_risk(stage(RiskLimits::default(), true));
    assert!(staged.risk_engine().is_some());
}

#[test]
fn on_trading_state_is_tracked() {
    let mut r = runner(vec![]);
    assert_eq!(r.trading_state(), &TradingState::Active);
    r.on_trading_state(TradingState::Reducing);
    assert_eq!(r.trading_state(), &TradingState::Reducing);
}

#[test]
fn a_stage_refusal_is_recorded_and_releases_the_intent() {
    let mut r = runner(vec![(1, vec![buy(5.0)])]).with_risk(stage(RiskLimits::default(), false));
    bar(&mut r, 1, 10.0);
    assert_eq!(r.order_rejections().len(), 1);
    assert!(!r.context().busy(&any_instrument()));
    assert!(r.submitted().is_empty());
    assert!(r.drain_orders().is_empty());
}

#[test]
fn an_approved_order_is_queued_and_not_rejected() {
    let mut r = runner(vec![(1, vec![buy(5.0)])]).with_risk(stage(RiskLimits::default(), true));
    bar(&mut r, 1, 10.0);
    assert_eq!(r.submitted().len(), 1);
    assert_eq!(r.drain_orders().len(), 1);
    assert!(r.order_rejections().is_empty());
}

#[test]
fn max_notional_limit_refuses_excessive_limit_order() {
    let limits = RiskLimits {
        max_notional_per_order: Some(Decimal::from(500)),
        ..RiskLimits::default()
    };
    // 100 x limit 10 = 1000 > 500
    let mut r = runner(vec![(1, vec![limit_buy(100.0, 10.0)])]).with_risk(stage(limits, true));
    bar(&mut r, 1, 10.0);
    assert_eq!(reasons(&r).len(), 1);
    assert!(reasons(&r)[0].contains("Notional"));
}

#[test]
fn order_rate_window_runs_on_event_time() {
    let limits = RiskLimits {
        order_rate_limit: Some(OrderRateLimit {
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
    let mut r = runner(plan).with_risk(stage(limits, true));
    for ts in [MS, 2 * MS, 1001 * MS] {
        bar(&mut r, ts, 10.0);
    }
    // s-1 is refused; by 1001 ms the first order left the window.
    assert_eq!(r.order_rejections().len(), 1);
    assert_eq!(r.submitted().len(), 2);
}
