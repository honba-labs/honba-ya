//! `DynStrategy`: the type-erased [`Strategy`] a heterogeneous sweep stores.

use std::sync::{Arc, Mutex};

use honba_engine::Result;
use honba_entities::{Currency, Trade};
use honba_messages::{
    AggressorSide, Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, OrderId,
    OrderSide, PriceType, QuoteTick, TradeId, TradeTick, UnixNanos,
};

use crate::{DynStrategy, IntentError, LedgerContext, OrderIntent, Strategy, StrategyContext};

type Hooks = Arc<Mutex<Vec<&'static str>>>;

struct Recorder {
    hooks: Hooks,
}

impl Recorder {
    fn new(hooks: Hooks) -> Self {
        Self { hooks }
    }

    fn record(&self, hook: &'static str) {
        self.hooks.lock().unwrap().push(hook);
    }
}

impl Strategy for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    fn on_start(&mut self, _ctx: &mut dyn StrategyContext) -> Result<()> {
        self.record("on_start");
        Ok(())
    }

    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        self.record("on_bar");
        Ok(())
    }

    fn on_quote(&mut self, _ctx: &mut dyn StrategyContext, _quote: &QuoteTick) -> Result<()> {
        self.record("on_quote");
        Ok(())
    }

    fn on_trade(&mut self, _ctx: &mut dyn StrategyContext, _trade: &TradeTick) -> Result<()> {
        self.record("on_trade");
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &mut dyn StrategyContext, _fill: &Trade) -> Result<()> {
        self.record("on_fill");
        Ok(())
    }

    fn on_intent_rejected(
        &mut self,
        _ctx: &mut dyn StrategyContext,
        _intent: &OrderIntent,
        _error: &IntentError,
    ) -> Result<()> {
        self.record("on_intent_rejected");
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &mut dyn StrategyContext) -> Result<()> {
        self.record("on_stop");
        Ok(())
    }
}

struct Failing;

impl Strategy for Failing {
    fn name(&self) -> &str {
        "failing"
    }

    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        Err(honba_engine::AlgoError::Component("no bar".to_string()))
    }
}

fn any_bar() -> Bar {
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let at = UnixNanos::from_u64(1);
    Bar::new(
        BarType::new(InstrumentId::new("X", Exchange::new("NSE")), spec),
        10.0,
        11.0,
        9.0,
        10.5,
        100.0,
        at,
        at,
    )
}

fn any_quote() -> QuoteTick {
    let at = UnixNanos::from_u64(1);
    QuoteTick::new(
        InstrumentId::new("X", Exchange::new("NSE")),
        10.4,
        10.6,
        5.0,
        5.0,
        at,
        at,
    )
}

fn any_trade_tick() -> TradeTick {
    let at = UnixNanos::from_u64(1);
    TradeTick::new(
        InstrumentId::new("X", Exchange::new("NSE")),
        10.5,
        5.0,
        AggressorSide::Buyer,
        TradeId::new("T-1"),
        at,
        at,
    )
}

fn any_fill() -> Trade {
    let at = UnixNanos::from_u64(1);
    Trade::new(
        OrderId::new("O-1"),
        InstrumentId::new("X", Exchange::new("NSE")),
        OrderSide::Buy,
        1.0,
        10.5,
        Currency::Inr,
        at,
        at,
    )
}

#[test]
fn every_hook_reaches_the_boxed_strategy() {
    let hooks: Hooks = Arc::new(Mutex::new(Vec::new()));
    let mut strategy = DynStrategy::new(Box::new(Recorder::new(Arc::clone(&hooks))));
    let mut ctx = LedgerContext::new();

    assert_eq!(strategy.name(), "recorder");
    strategy.on_start(&mut ctx).unwrap();
    strategy.on_bar(&mut ctx, &any_bar()).unwrap();
    strategy.on_quote(&mut ctx, &any_quote()).unwrap();
    strategy.on_trade(&mut ctx, &any_trade_tick()).unwrap();
    strategy.on_fill(&mut ctx, &any_fill()).unwrap();
    strategy
        .on_intent_rejected(
            &mut ctx,
            &OrderIntent::market_buy(InstrumentId::new("X", Exchange::new("NSE")), 1.0),
            &IntentError::NonPositiveQuantity(-1.0),
        )
        .unwrap();
    strategy.on_stop(&mut ctx).unwrap();

    assert_eq!(
        *hooks.lock().unwrap(),
        vec![
            "on_start",
            "on_bar",
            "on_quote",
            "on_trade",
            "on_fill",
            "on_intent_rejected",
            "on_stop",
        ]
    );
}

#[test]
fn an_error_from_a_hook_reaches_the_caller() {
    let mut strategy = DynStrategy::new(Box::new(Failing));
    let mut ctx = LedgerContext::new();
    assert_eq!(
        strategy.on_bar(&mut ctx, &any_bar()).unwrap_err(),
        honba_engine::AlgoError::Component("no bar".to_string())
    );
}
