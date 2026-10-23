//! End-to-end integration test across the full stack.

use honba_analytics::{PerformanceReport, RoundTrip};
use honba_engine::Handler;
use honba_engine::{DataFeed, Result};
use honba_entities::Trade;
use honba_messages::{Exchange, InstrumentId};
use honba_sim::BarFillEngine;
use honba_strategy::{SmaCrossover, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::any_instrument;

const CLOSES: &[f64] = &[
    100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0, 102.0,
    103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 105.0,
];

fn pair_fills(fills: &[Trade]) -> Vec<RoundTrip> {
    let mut trips = Vec::new();
    let mut i = 0;
    while i + 1 < fills.len() {
        if let Ok(rt) = RoundTrip::from_fills(&fills[i], &fills[i + 1]) {
            trips.push(rt);
        }
        i += 2;
    }
    trips
}

#[test]
fn full_stack_produces_a_report() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let messages: Vec<_> = CLOSES
        .iter()
        .enumerate()
        .map(|(i, &c)| VecFeed::bar("NIFTY50", c, (i as u64 + 1) * 1_000_000))
        .collect();
    let mut feed = VecFeed::new(messages);

    let strategy = SmaCrossover::new(
        InstrumentId::new("NIFTY50", Exchange::new("NSE")),
        3,
        8,
        10.0,
    );
    let mut execution = BarFillEngine::new();
    let mut runner = StrategyRunner::new(strategy, execution.clone());

    runner.on_start()?;
    while let Some(msg) = feed.next()? {
        let ev = msg.event().clone();
        let init = msg.ts_init();
        Handler::on_event(&mut execution, &ev, init)?;
        Handler::on_event(&mut runner, &ev, init)?;
    }
    runner.on_stop()?;

    let (_strategy, _exec, fills) = runner.into_parts();
    assert!(!fills.is_empty(), "expected at least one fill");

    let trips = pair_fills(&fills);
    assert!(!trips.is_empty(), "expected at least one round trip");

    // Every fill for the same instrument on the same side sequence should
    // alternate buy/sell.
    let curve_start = 1_000_000.0;
    let mut curve = vec![curve_start];
    let mut cur = curve_start;
    for t in &trips {
        cur += t.net_pnl;
        curve.push(cur);
    }

    let returns: Vec<f64> = curve.windows(2).map(|w| (w[1] - w[0]) / w[0]).collect();
    let report = PerformanceReport::from_returns(&trips, &returns, 252.0, 0.0)?;

    assert_eq!(report.trades.n_trades, trips.len());
    assert!(report.equity.n_periods >= 1);

    Ok(())
}

#[test]
fn runner_with_empty_feed_finishes() -> Result<()> {
    let mut feed = VecFeed::empty();
    let strategy = SmaCrossover::new(any_instrument(), 3, 8, 1.0);
    let execution = BarFillEngine::new();
    let mut runner = StrategyRunner::new(strategy, execution.clone());

    runner.on_start()?;
    while let Some(msg) = feed.next()? {
        let ev = msg.event().clone();
        Handler::on_event(&mut runner, &ev, msg.ts_init())?;
    }
    runner.on_stop()?;

    let (_s, _e, fills) = runner.into_parts();
    assert!(fills.is_empty());

    Ok(())
}
