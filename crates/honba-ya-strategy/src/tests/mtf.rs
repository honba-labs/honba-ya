//! Multi-timeframe strategy execution and look-ahead guarantee (ROADMAP E4-S8).

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use honba_core::{Bar, Exchange, InstrumentId, MarketEvent, OrderSide, Price, Quantity};
use honba_market::{
    BarAggregation, BarSpecification, MarketCalendar, Session, SessionBarAggregator,
};
use honba_testing::fixtures::exact_decimal;
use rust_decimal::Decimal;

use crate::{OrderIntent, Result, Strategy, StrategyContext, StrategyRunner};

#[derive(Clone, Copy, Debug)]
struct TestSessionCalendar;

impl MarketCalendar for TestSessionCalendar {
    fn session(&self) -> Session {
        Session::new(
            NaiveTime::from_hms_opt(9, 15, 0).unwrap(),
            NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
        )
    }

    fn is_holiday(&self, _date: NaiveDate) -> bool {
        false
    }
}

/// A multi-timeframe strategy that trades strictly upon higher-timeframe bar completion.
struct MtfTrendStrategy {
    instrument: InstrumentId,
    aggregator: SessionBarAggregator<TestSessionCalendar>,
    htf_bars_seen: usize,
}

impl MtfTrendStrategy {
    fn new(instrument: InstrumentId) -> Self {
        let htf_spec = BarSpecification::new(1, BarAggregation::Day);
        let aggregator = SessionBarAggregator::new(TestSessionCalendar, htf_spec);
        Self {
            instrument,
            aggregator,
            htf_bars_seen: 0,
        }
    }
}

impl Strategy for MtfTrendStrategy {
    fn name(&self) -> &str {
        "mtf_trend"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> Result<()> {
        if let Some(htf_bar) = self.aggregator.process(bar) {
            self.htf_bars_seen += 1;
            if htf_bar.close.0 > Decimal::from(105) {
                ctx.submit(OrderIntent::market_buy(
                    self.instrument.clone(),
                    Quantity::from_u64(10),
                ));
            } else if htf_bar.close.0 < Decimal::from(95) {
                ctx.submit(OrderIntent::market_sell(
                    self.instrument.clone(),
                    Quantity::from_u64(10),
                ));
            }
        }
        Ok(())
    }
}

fn make_bar(instrument: &InstrumentId, dt: NaiveDateTime, close: f64) -> Bar {
    let c = exact_decimal(close);
    Bar {
        instrument: instrument.clone(),
        open: Price::new(c),
        high: Price::new(c + Decimal::ONE),
        low: Price::new(c - Decimal::ONE),
        close: Price::new(c),
        volume: Quantity::from_u64(100),
        timestamp: dt.and_utc(),
    }
}

#[test]
fn mtf_strategy_run() {
    let instrument = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let strategy = MtfTrendStrategy::new(instrument.clone());
    let mut runner = StrategyRunner::new(strategy);
    runner.on_start().unwrap();

    let day1 = NaiveDate::from_ymd_opt(2026, 5, 10).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2026, 5, 11).unwrap();

    // Day 1: Session open to 15:28 (intermediate bars)
    for minute in [15, 30, 45, 59] {
        let dt = NaiveDateTime::new(day1, NaiveTime::from_hms_opt(9, minute, 0).unwrap());
        let bar = make_bar(&instrument, dt, 102.0);
        let at = bar.timestamp;
        let event = MarketEvent::Bar(bar);
        runner.on_event(&event, at).unwrap();

        // NO intent should be submitted during intraday bars before session completes
        assert_eq!(
            runner.submitted().len(),
            0,
            "no intent before session complete"
        );
    }

    // Day 1 terminal bar at 15:29 (since SessionBarAggregator uses target_spec 1 Day,
    // a bar on Day 2 triggers completion of Day 1 or terminal check at 15:30).
    let dt_term1 = NaiveDateTime::new(day1, NaiveTime::from_hms_opt(15, 30, 0).unwrap());
    let bar_term1 = make_bar(&instrument, dt_term1, 108.0);
    let at1 = bar_term1.timestamp;
    let event1 = MarketEvent::Bar(bar_term1);
    runner.on_event(&event1, at1).unwrap();

    // Exactly 1 buy intent submitted upon Day 1 session completion!
    assert_eq!(
        runner.submitted().len(),
        1,
        "buy intent emitted upon day 1 completion"
    );
    assert_eq!(runner.submitted()[0].intent.side, OrderSide::Buy);

    // Day 2 intermediate bar: no new intent
    let dt_day2_open = NaiveDateTime::new(day2, NaiveTime::from_hms_opt(9, 15, 0).unwrap());
    let bar_d2_open = make_bar(&instrument, dt_day2_open, 98.0);
    let at2_open = bar_d2_open.timestamp;
    let event2_open = MarketEvent::Bar(bar_d2_open);
    runner.on_event(&event2_open, at2_open).unwrap();
    assert_eq!(
        runner.submitted().len(),
        1,
        "still 1 intent during day 2 session"
    );

    // Day 2 terminal bar at 15:30: close < 95.0
    let dt_term2 = NaiveDateTime::new(day2, NaiveTime::from_hms_opt(15, 30, 0).unwrap());
    let bar_term2 = make_bar(&instrument, dt_term2, 92.0);
    let at2 = bar_term2.timestamp;
    let event2 = MarketEvent::Bar(bar_term2);
    runner.on_event(&event2, at2).unwrap();

    // Exactly 2 intents now: the second is a sell!
    assert_eq!(
        runner.submitted().len(),
        2,
        "sell intent emitted upon day 2 completion"
    );
    assert_eq!(runner.submitted()[1].intent.side, OrderSide::Sell);
}
