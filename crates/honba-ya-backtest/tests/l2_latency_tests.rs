//! Tests that the L2 book's configured network/broker latency actually
//! delays an order's fill until a later event's timestamp, rather than being
//! a no-op (`OrderBook::apply_latency` used to do nothing with
//! `latency_us`).

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal_macros::dec;

use honba_backtest::sim::latency::NetworkLatency;
use honba_backtest::sim::mis::MisSquareOffManager;
use honba_backtest::sim::slippage::SlippageModel;
use honba_backtest::{BacktestConfig, BacktestEngine, MarketEvent};
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

fn ts_us(day: NaiveDate, h: u32, m: u32, s: u32, micros: u32) -> DateTime<Utc> {
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_micro_opt(h, m, s, micros).unwrap()))
}

/// Buys once on the first bar it sees.
struct OnceBuyer {
    instrument: InstrumentId,
    quantity: Quantity,
    bought: bool,
}

impl Strategy for OnceBuyer {
    fn name(&self) -> &str {
        "once_buyer"
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

fn make_engine(instrument: &InstrumentId, latency_us: u64) -> BacktestEngine<OnceBuyer> {
    let strategy = OnceBuyer {
        instrument: instrument.clone(),
        quantity: Quantity::from_u64(10),
        bought: false,
    };
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(strategy.name());

    let latency = NetworkLatency::new(latency_us, 0, 1);
    let config = BacktestConfig::with_l2_simulation(
        dec!(1_000_000.00),
        ProductType::Delivery,
        latency,
        SlippageModel::new(),
        MisSquareOffManager::new(NseTradingCalendar::new()),
    );

    BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new())
}

#[test]
fn test_latency_delays_fill_to_later_event_timestamp() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    // 5ms (5_000us) latency.
    let t0 = ts_us(day, 9, 20, 0, 0);
    let t1 = ts_us(day, 9, 20, 0, 2_000); // +2ms: still before ready_ts
    let t2 = ts_us(day, 9, 20, 0, 6_000); // +6ms: past ready_ts (t0 + 5ms)

    // Only the submitting bar and one bar before latency elapses: no fill yet.
    let mut engine_before_ready = make_engine(&inst, 5_000);
    let before = engine_before_ready.run_events(vec![
        MarketEvent::Bar(bar(&inst, "1500.00", t0)),
        MarketEvent::Bar(bar(&inst, "1501.00", t1)),
    ]);
    assert_eq!(
        before.total_trades, 0,
        "fill must not happen before latency elapses"
    );
    assert!(before.fills.is_empty());

    // Once an event at or after `t0 + latency` is processed, the order fills.
    let mut engine_after_ready = make_engine(&inst, 5_000);
    let after = engine_after_ready.run_events(vec![
        MarketEvent::Bar(bar(&inst, "1500.00", t0)),
        MarketEvent::Bar(bar(&inst, "1501.00", t1)),
        MarketEvent::Bar(bar(&inst, "1502.00", t2)),
    ]);
    assert_eq!(
        after.total_trades, 1,
        "fill must happen once latency has elapsed"
    );
    assert_eq!(after.fills.len(), 1);
    assert!(after.fills[0].timestamp >= t2);
}

#[test]
fn test_zero_latency_fills_immediately() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let t0 = ts_us(day, 9, 20, 0, 0);

    let mut engine = make_engine(&inst, 0);
    let result = engine.run_events(vec![MarketEvent::Bar(bar(&inst, "1500.00", t0))]);

    assert_eq!(
        result.total_trades, 1,
        "zero latency must fill within the same event"
    );
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].timestamp, t0);
}
