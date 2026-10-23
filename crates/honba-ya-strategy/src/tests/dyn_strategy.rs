//! `DynStrategy`: the type-erased [`Strategy`] a heterogeneous sweep stores.

use std::sync::{Arc, Mutex};

use honba_core::uuid::Uuid;
use honba_core::{Bar, Exchange, FillReport, InstrumentId, OrderSide, Price, Quantity, Tick};
use honba_testing::fixtures::{exact_decimal, flat_bar, utc_nanos};
use rust_decimal::Decimal;

use crate::{
    DynStrategy, IntentError, LedgerContext, OrderIntent, Result, Strategy, StrategyContext,
    StrategyError,
};

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

    fn on_quote(&mut self, _ctx: &mut dyn StrategyContext, _quote: &Tick) -> Result<()> {
        self.record("on_quote");
        Ok(())
    }

    fn on_trade(&mut self, _ctx: &mut dyn StrategyContext, _trade: &Tick) -> Result<()> {
        self.record("on_trade");
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &mut dyn StrategyContext, _fill: &FillReport) -> Result<()> {
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
        Err(StrategyError::other("no bar"))
    }
}

fn any_bar() -> Bar {
    flat_bar("X", 10.5, 1)
}

fn any_quote() -> Tick {
    let at = utc_nanos(1);
    Tick::Quote {
        instrument: InstrumentId::new(Exchange::Nse, "X"),
        bid: Price::new(exact_decimal(10.4)),
        ask: Price::new(exact_decimal(10.6)),
        bid_qty: Quantity::from_u64(5),
        ask_qty: Quantity::from_u64(5),
        last_price: None,
        exchange_ts: at,
        receive_ts: at,
        band: None,
    }
}

fn any_trade_tick() -> Tick {
    let at = utc_nanos(1);
    Tick::Ltp {
        instrument: InstrumentId::new(Exchange::Nse, "X"),
        price: Price::new(exact_decimal(10.5)),
        volume: Quantity::from_u64(5),
        exchange_ts: at,
        receive_ts: at,
    }
}

fn any_fill() -> FillReport {
    FillReport::new(
        "F-1",
        "O-1",
        Uuid::nil(),
        InstrumentId::new(Exchange::Nse, "X"),
        OrderSide::Buy,
        Price::new(exact_decimal(10.5)),
        Quantity::from_u64(1),
        "recorder",
        utc_nanos(1),
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
            &OrderIntent::market_buy(InstrumentId::new(Exchange::Nse, "X"), Quantity::from_u64(1)),
            &IntentError::NonPositiveQuantity(Quantity::new(Decimal::from(-1))),
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
        StrategyError::other("no bar")
    );
}
