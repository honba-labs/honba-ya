//! End-to-end demo: strategy -> engine -> paper execution -> analytics -> report.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p honba-strategy --example end_to_end
//! ```

use honba_analytics::{PerformanceReport, RoundTrip};
use honba_engine::{DataFeed, Handler};
use honba_entities::Trade;
use honba_feed::{MarkdownReportWriter, ReportWriter};
use honba_messages::{Exchange, InstrumentId};
use honba_sim::BarFillEngine;
use honba_strategy::{SmaCrossover, Strategy, StrategyRunner};
use honba_testing::VecFeed;

/// Synthetic price series with several swings to generate crosses.
const CLOSES: &[f64] = &[
    100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0, 102.0,
    103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 105.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0,
    110.0, 111.0, 110.0, 109.0, 108.0,
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

fn equity_curve(starting: f64, trips: &[RoundTrip]) -> Vec<f64> {
    let mut equity = vec![starting];
    let mut cur = starting;
    for t in trips {
        cur += t.net_pnl;
        equity.push(cur);
    }
    equity
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build the feed.
    let messages: Vec<_> = CLOSES
        .iter()
        .enumerate()
        .map(|(i, &c)| VecFeed::bar("NIFTY50", c, (i as u64 + 1) * 1_000_000_000))
        .collect();
    let mut feed = VecFeed::new(messages);

    // 2. Build the execution engine and strategy runner.
    let strategy = SmaCrossover::new(
        InstrumentId::new("NIFTY50", Exchange::new("NSE")),
        3,
        8,
        10.0,
    );
    let mut execution = BarFillEngine::new();
    let mut runner = StrategyRunner::new(strategy, execution.clone());

    // 3. Drive events manually so the runner stays accessible after the run.
    runner.on_start()?;
    while let Some(msg) = feed.next()? {
        let ev = msg.event().clone();
        let init = msg.ts_init();
        Handler::on_event(&mut execution, &ev, init)?;
        Handler::on_event(&mut runner, &ev, init)?;
    }
    runner.on_stop()?;

    // 4. Collect fills.
    let (strategy, _exec, fills) = runner.into_parts();
    println!("strategy={} fills={}", strategy.name(), fills.len());

    let trips = pair_fills(&fills);
    if trips.is_empty() {
        println!("no round trips produced");
        return Ok(());
    }
    println!("round trips={}", trips.len());

    // 5. Build and print the report.
    let curve = equity_curve(1_000_000.0, &trips);
    let returns: Vec<f64> = curve.windows(2).map(|w| (w[1] - w[0]) / w[0]).collect();
    let report = PerformanceReport::from_returns(&trips, &returns, 252.0, 0.0)?;

    let stdout = std::io::stdout();
    let mut writer = MarkdownReportWriter::new(stdout.lock());
    writer.write(&report)?;

    Ok(())
}
