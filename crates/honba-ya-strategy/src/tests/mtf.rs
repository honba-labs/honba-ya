//! Multi-timeframe strategy execution and look-ahead guarantee (ROADMAP E4-S8).

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use honba_engine::{Handler, Result};
use honba_market::{MarketCalendar, Session, SessionBarAggregator};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, PriceType, UnixNanos,
};
use honba_sim::BarFillEngine;

use crate::{OrderIntent, Strategy, StrategyContext, StrategyRunner};

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
        let htf_spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
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
            if htf_bar.close() > 105.0 {
                ctx.submit(OrderIntent::market_buy(self.instrument.clone(), 10.0));
            } else if htf_bar.close() < 95.0 {
                ctx.submit(OrderIntent::market_sell(self.instrument.clone(), 10.0));
            }
        }
        Ok(())
    }
}

fn make_bar(instrument: &InstrumentId, dt: NaiveDateTime, close: f64) -> Bar {
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let bar_type = BarType::new(instrument.clone(), spec);
    let nanos = dt.and_utc().timestamp_nanos_opt().unwrap() as u64;
    let ts = UnixNanos::from_u64(nanos);
    Bar::new(
        bar_type,
        close,
        close + 1.0,
        close - 1.0,
        close,
        100.0,
        ts,
        ts,
    )
}

#[test]
fn mtf_strategy_run() {
    let instrument = InstrumentId::new("RELIANCE", Exchange::new("NSE"));
    let mut execution = BarFillEngine::new();
    let strategy = MtfTrendStrategy::new(instrument.clone());
    let mut runner = StrategyRunner::new(strategy, execution.clone());
    runner.on_start().unwrap();

    let day1 = NaiveDate::from_ymd_opt(2026, 5, 10).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2026, 5, 11).unwrap();

    // Day 1: Session open to 15:28 (intermediate bars)
    for minute in [15, 30, 45, 59] {
        let dt = NaiveDateTime::new(day1, NaiveTime::from_hms_opt(9, minute, 0).unwrap());
        let bar = make_bar(&instrument, dt, 102.0);
        let event = honba_messages::Event::Bar(bar);
        let at = event.ts_event();
        execution.on_event(&event, at).unwrap();
        runner.on_event(&event, at).unwrap();

        // NO intent should be submitted during intraday bars before session completes
        assert_eq!(
            runner.submitted().len(),
            0,
            "no intent before session complete"
        );
    }

    // Day 1 terminal bar at 15:29: close > 105.0
    let dt_term1 = NaiveDateTime::new(day1, NaiveTime::from_hms_opt(15, 29, 0).unwrap());
    let bar_term1 = make_bar(&instrument, dt_term1, 108.0);
    let event1 = honba_messages::Event::Bar(bar_term1);
    let at1 = event1.ts_event();
    execution.on_event(&event1, at1).unwrap();
    runner.on_event(&event1, at1).unwrap();

    // Exactly 1 buy intent submitted upon Day 1 session completion!
    assert_eq!(
        runner.submitted().len(),
        1,
        "buy intent emitted upon day 1 completion"
    );
    assert_eq!(
        runner.submitted()[0].intent.side,
        honba_messages::OrderSide::Buy
    );

    // Day 2 intermediate bar: no new intent
    let dt_day2_open = NaiveDateTime::new(day2, NaiveTime::from_hms_opt(9, 15, 0).unwrap());
    let bar_d2_open = make_bar(&instrument, dt_day2_open, 98.0);
    let event2_open = honba_messages::Event::Bar(bar_d2_open);
    execution
        .on_event(&event2_open, event2_open.ts_event())
        .unwrap();
    runner
        .on_event(&event2_open, event2_open.ts_event())
        .unwrap();
    assert_eq!(
        runner.submitted().len(),
        1,
        "still 1 intent during day 2 session"
    );

    // Day 2 terminal bar at 15:29: close < 95.0
    let dt_term2 = NaiveDateTime::new(day2, NaiveTime::from_hms_opt(15, 29, 0).unwrap());
    let bar_term2 = make_bar(&instrument, dt_term2, 92.0);
    let event2 = honba_messages::Event::Bar(bar_term2);
    execution.on_event(&event2, event2.ts_event()).unwrap();
    runner.on_event(&event2, event2.ts_event()).unwrap();

    // Exactly 2 intents now: the second is a sell!
    assert_eq!(
        runner.submitted().len(),
        2,
        "sell intent emitted upon day 2 completion"
    );
    assert_eq!(
        runner.submitted()[1].intent.side,
        honba_messages::OrderSide::Sell
    );
}
