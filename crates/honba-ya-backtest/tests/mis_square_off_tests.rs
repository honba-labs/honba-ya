//! Tests for the MIS (Margin Intraday Square-off) 15:20 IST auto-square-off
//! path, fixing three bugs in `BacktestEngine::check_mis_square_off`:
//! (1) the forced-square-off result was discarded instead of acted on,
//! (2) only one arbitrary instrument was checked instead of every
//! instrument with an open MIS position, and (3)
//! `MisSquareOffManager::process_trade` was never called on fills, so
//! eligibility tracking never updated.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use honba_backtest::sim::mis::MisSquareOffManager;
use honba_backtest::{BacktestConfig, BacktestEngine, MarketEvent};
use honba_backtest::{IndianCostModel, NseTradingCalendar, ProductType};
use honba_core::{Bar, Exchange, InstrumentId, OrderSide, Price, Quantity};
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

/// `NseTradingCalendar::ist_offset()` is UTC+05:30, so IST 15:20 is UTC
/// 09:50 on the same calendar day.
fn ist_15_20_utc(day: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 50, 0).unwrap()))
}

fn before_square_off_utc(day: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()))
}

/// Buys a fixed quantity of each instrument once, the first time it sees a
/// bar for that instrument.
struct MultiOnceBuyer {
    orders: HashMap<InstrumentId, Quantity>,
    bought: HashMap<InstrumentId, bool>,
}

impl MultiOnceBuyer {
    fn new(orders: Vec<(InstrumentId, Quantity)>) -> Self {
        Self {
            bought: orders.iter().map(|(id, _)| (id.clone(), false)).collect(),
            orders: orders.into_iter().collect(),
        }
    }
}

impl Strategy for MultiOnceBuyer {
    fn name(&self) -> &str {
        "multi_once_buyer"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> honba_strategy::Result<()> {
        if let Some(qty) = self.orders.get(&bar.instrument) {
            if !self.bought[&bar.instrument] {
                ctx.submit(OrderIntent::market_buy(bar.instrument.clone(), *qty));
                self.bought.insert(bar.instrument.clone(), true);
            }
        }
        Ok(())
    }
}

fn risk_engine_for(algo_id: &str) -> RiskEngine {
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(algo_id);
    risk_engine
}

#[test]
fn test_square_off_closes_all_open_mis_positions_at_1520_ist() {
    let inst1 = InstrumentId::new(Exchange::Nse, "INFY");
    let inst2 = InstrumentId::new(Exchange::Nse, "TCS");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    let strategy = MultiOnceBuyer::new(vec![
        (inst1.clone(), Quantity::from_u64(10)),
        (inst2.clone(), Quantity::from_u64(5)),
    ]);
    let risk_engine = risk_engine_for(strategy.name());

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Intraday,
        mis_square_off: Some(MisSquareOffManager::new(NseTradingCalendar::new())),
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        IndianCostModel::new(),
        NseTradingCalendar::new(),
    );

    let events = vec![
        MarketEvent::Bar(bar(&inst1, "1500.00", before_square_off_utc(day))),
        MarketEvent::Bar(bar(&inst2, "2500.00", before_square_off_utc(day))),
        // Past 15:20 IST: both open MIS positions must be force-closed.
        MarketEvent::Bar(bar(&inst1, "1510.00", ist_15_20_utc(day))),
    ];

    let result = engine.run_events(events);

    // 2 opening buys + 2 forced square-off closes.
    assert_eq!(result.fills.len(), 4);

    let mut net_position: HashMap<InstrumentId, Decimal> = HashMap::new();
    for fill in &result.fills {
        let signed = if fill.side == OrderSide::Buy {
            fill.quantity.0
        } else {
            -fill.quantity.0
        };
        *net_position.entry(fill.instrument.clone()).or_insert(Decimal::ZERO) += signed;
    }
    assert_eq!(
        net_position.get(&inst1).copied().unwrap_or_default(),
        Decimal::ZERO,
        "INFY must be fully squared off"
    );
    assert_eq!(
        net_position.get(&inst2).copied().unwrap_or_default(),
        Decimal::ZERO,
        "TCS must be fully squared off"
    );
}

#[test]
fn test_process_trade_updates_square_off_eligibility() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    let strategy = MultiOnceBuyer::new(vec![(inst.clone(), Quantity::from_u64(10))]);
    let risk_engine = risk_engine_for(strategy.name());

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Intraday,
        mis_square_off: Some(MisSquareOffManager::new(NseTradingCalendar::new())),
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        IndianCostModel::new(),
        NseTradingCalendar::new(),
    );

    let events = vec![MarketEvent::Bar(bar(&inst, "1500.00", before_square_off_utc(day)))];
    let _ = engine.run_events(events);

    let manager = engine
        .mis_square_off()
        .expect("mis_square_off manager must be present");
    assert!(
        manager.is_eligible(&inst),
        "process_trade must mark the instrument square-off eligible on fill"
    );
}

#[test]
fn test_no_square_off_for_delivery_positions() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    let strategy = MultiOnceBuyer::new(vec![(inst.clone(), Quantity::from_u64(10))]);
    let risk_engine = risk_engine_for(strategy.name());

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        mis_square_off: Some(MisSquareOffManager::new(NseTradingCalendar::new())),
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        IndianCostModel::new(),
        NseTradingCalendar::new(),
    );

    let events = vec![
        MarketEvent::Bar(bar(&inst, "1500.00", before_square_off_utc(day))),
        MarketEvent::Bar(bar(&inst, "1510.00", ist_15_20_utc(day))),
    ];
    let result = engine.run_events(events);

    // Only the opening buy: a delivery (non-MIS) position is never
    // force-closed by the 15:20 IST square-off check.
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].side, OrderSide::Buy);
}
