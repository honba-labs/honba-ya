//! Tests that `BacktestEngine` is driven by `honba_strategy::StrategyRunner`
//! (the sync, context-driven `Strategy` trait), not the old async
//! `honba_actors::Strategy` trait.

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal_macros::dec;

use honba_backtest::MarketEvent;
use honba_backtest::{BacktestConfig, BacktestEngine};
use honba_backtest::{NseTradingCalendar, ProductType};
use honba_core::{Bar, Exchange, InstrumentId, Price, Quantity};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_strategy::{OrderIntent, Strategy, StrategyContext};

fn bar(inst: &InstrumentId, close: &str, ts: DateTime<Utc>) -> Bar {
    let price = Price::from_rupees(close).unwrap();
    Bar {
        instrument: inst.clone(),
        open: price,
        high: price,
        low: price,
        close: price,
        volume: Quantity::from_u64(1_000),
        timestamp: ts,
    }
}

fn ts(day: NaiveDate, h: u32, m: u32) -> DateTime<Utc> {
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(h, m, 0).unwrap()))
}

/// Buys once on the first bar it sees, via its `StrategyContext`.
struct ToyBarBuyer {
    instrument: InstrumentId,
    quantity: Quantity,
    bought: bool,
}

impl Strategy for ToyBarBuyer {
    fn name(&self) -> &str {
        "toy_bar_buyer"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        if !self.bought {
            ctx.submit(OrderIntent::market_buy(
                self.instrument.clone(),
                self.quantity,
            ));
            self.bought = true;
        }
        Ok(())
    }
}

/// Buys on the first bar, sells on the next bar it is not busy.
struct BuyThenSell {
    instrument: InstrumentId,
    quantity: Quantity,
    bought: bool,
    sold: bool,
}

impl Strategy for BuyThenSell {
    fn name(&self) -> &str {
        "buy_then_sell"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        if !self.bought {
            ctx.submit(OrderIntent::market_buy(
                self.instrument.clone(),
                self.quantity,
            ));
            self.bought = true;
        } else if !self.sold && !ctx.busy(&self.instrument) {
            ctx.submit(OrderIntent::market_sell(
                self.instrument.clone(),
                self.quantity,
            ));
            self.sold = true;
        }
        Ok(())
    }
}

/// Submits a buy intent on every bar it is not already busy on, forever —
/// used to probe the warm-up gate.
struct AlwaysBuyOnBar {
    instrument: InstrumentId,
    quantity: Quantity,
}

impl Strategy for AlwaysBuyOnBar {
    fn name(&self) -> &str {
        "always_buy_on_bar"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        if !ctx.busy(&self.instrument) {
            ctx.submit(OrderIntent::market_buy(
                self.instrument.clone(),
                self.quantity,
            ));
        }
        Ok(())
    }
}

/// Submits one oversized limit buy on the first bar; a tight
/// `max_notional_per_order` risk limit is expected to refuse it.
struct OversizedLimitBuyer {
    instrument: InstrumentId,
    quantity: Quantity,
    price: Price,
    submitted: bool,
}

impl Strategy for OversizedLimitBuyer {
    fn name(&self) -> &str {
        "oversized_limit_buyer"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        if !self.submitted {
            ctx.submit(OrderIntent::limit_buy(
                self.instrument.clone(),
                self.quantity,
                self.price,
            ));
            self.submitted = true;
        }
        Ok(())
    }
}

fn risk_engine_for(algo_id: &str, limits: RiskLimits) -> RiskEngine {
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo(algo_id);
    risk_engine
}

#[test]
fn test_engine_drives_strategy_runner_intents() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let strategy = ToyBarBuyer {
        instrument: inst.clone(),
        quantity: Quantity::from_u64(10),
        bought: false,
    };
    let risk_engine = risk_engine_for(strategy.name(), RiskLimits::default());
    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let events = vec![MarketEvent::Bar(bar(&inst, "1500.00", ts(day, 9, 20)))];

    let result = engine.run_events(events);

    assert_eq!(result.total_trades, 1);
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].instrument, inst);
    assert_eq!(result.fills[0].quantity, Quantity::from_u64(10));
}

#[test]
fn test_engine_respects_warmup_bars() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let bars = vec![
        MarketEvent::Bar(bar(&inst, "1500.00", ts(day, 9, 20))),
        MarketEvent::Bar(bar(&inst, "1510.00", ts(day, 9, 25))),
        MarketEvent::Bar(bar(&inst, "1520.00", ts(day, 9, 30))),
    ];

    let make_engine = || {
        let strategy = AlwaysBuyOnBar {
            instrument: inst.clone(),
            quantity: Quantity::from_u64(10),
        };
        let risk_engine = risk_engine_for(strategy.name(), RiskLimits::default());
        let config = BacktestConfig {
            starting_cash: dec!(1_000_000.00),
            product_type: ProductType::Delivery,
            warmup_bars: 2,
            ..Default::default()
        };
        BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new())
    };

    // Only the first two (warm-up) bars: no order should ever be submitted.
    let mut engine_during_warmup = make_engine();
    let during = engine_during_warmup.run_events(bars[..2].to_vec());
    assert_eq!(during.total_trades, 0);
    assert!(during.fills.is_empty());

    // All three bars: only the third (post warm-up) bar's order fills.
    let mut engine_past_warmup = make_engine();
    let past = engine_past_warmup.run_events(bars.clone());
    assert_eq!(past.total_trades, 1);
    assert_eq!(past.fills.len(), 1);
}

#[test]
fn test_engine_surfaces_intent_rejections() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let strategy = OversizedLimitBuyer {
        instrument: inst.clone(),
        quantity: Quantity::from_u64(1_000),
        price: Price::from_rupees("1500.00").unwrap(),
        submitted: false,
    };
    let limits = RiskLimits {
        max_notional_per_order: Some(dec!(100.00)),
        ..Default::default()
    };
    let risk_engine = risk_engine_for(strategy.name(), limits);
    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let events = vec![MarketEvent::Bar(bar(&inst, "1500.00", ts(day, 9, 20)))];

    let result = engine.run_events(events);

    assert_eq!(result.refused_orders, 1);
    assert_eq!(result.total_trades, 0);
    assert!(result.fills.is_empty());
}

#[test]
fn test_backtest_result_exposes_fills() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let strategy = BuyThenSell {
        instrument: inst.clone(),
        quantity: Quantity::from_u64(10),
        bought: false,
        sold: false,
    };
    let risk_engine = risk_engine_for(strategy.name(), RiskLimits::default());
    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let events = vec![
        MarketEvent::Bar(bar(&inst, "1500.00", ts(day, 9, 20))),
        MarketEvent::Bar(bar(&inst, "1510.00", ts(day, 9, 30))),
    ];

    let result = engine.run_events(events);

    assert_eq!(result.fills.len(), result.total_trades);
    assert_eq!(result.total_trades, 2);

    assert!(!result.equity_curve.is_empty());
    let mut last_ts = None;
    for (point_ts, _equity) in &result.equity_curve {
        if let Some(prev) = last_ts {
            assert!(
                *point_ts >= prev,
                "equity curve timestamps must be monotonic"
            );
        }
        last_ts = Some(*point_ts);
    }
}
