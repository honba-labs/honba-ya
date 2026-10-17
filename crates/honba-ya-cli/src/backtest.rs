use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use honba_analytics::{PerformanceReport, RoundTrip};
use honba_engine::{DataFeed, Handler};
use honba_entities::Trade;
use honba_feed::{MarkdownReportWriter, ReportWriter};
use honba_messages::{Exchange, InstrumentId};
use honba_sim::BarFillEngine;
use honba_strategy::{SmaCrossover, Strategy, StrategyRunner};
use honba_testing::VecFeed;

#[derive(Debug, Deserialize)]
pub(crate) struct BacktestConfig {
    #[serde(default = "default_symbol")]
    pub(crate) symbol: String,
    #[serde(default = "default_exchange")]
    pub(crate) exchange: String,
    pub(crate) strategy: StrategyConfig,
    #[serde(default = "default_starting_equity")]
    pub(crate) starting_equity: f64,
}

fn default_symbol() -> String {
    "NIFTY50".to_string()
}
fn default_exchange() -> String {
    "NSE".to_string()
}
fn default_starting_equity() -> f64 {
    1_000_000.0
}

#[derive(Debug, Deserialize)]
pub(crate) struct StrategyConfig {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) fast: usize,
    #[serde(default)]
    pub(crate) slow: usize,
    #[serde(default)]
    pub(crate) trade_size: f64,
}

pub(crate) fn pair_fills(fills: &[Trade]) -> Vec<RoundTrip> {
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

pub(crate) fn equity_curve(starting: f64, trips: &[RoundTrip]) -> Vec<f64> {
    let mut equity = vec![starting];
    let mut cur = starting;
    for t in trips {
        cur += t.net_pnl;
        equity.push(cur);
    }
    equity
}

pub fn run(config_path: &Path, output: Option<&Path>) -> anyhow::Result<()> {
    let text = fs::read_to_string(config_path)
        .with_context(|| format!("reading config {}", config_path.display()))?;
    let cfg: BacktestConfig = toml::from_str(&text)?;

    // Synthetic series — same shape as the end-to-end example.
    let closes: Vec<f64> = vec![
        100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0, 102.0,
        103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 105.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0,
        110.0, 111.0, 110.0, 109.0, 108.0,
    ];

    let messages: Vec<_> = closes
        .iter()
        .enumerate()
        .map(|(i, &c)| VecFeed::bar(&cfg.symbol, c, (i as u64 + 1) * 1_000_000_000))
        .collect();
    let mut feed = VecFeed::new(messages);

    let instrument = InstrumentId::new(&cfg.symbol, Exchange::new(&cfg.exchange));

    let strategy = match cfg.strategy.name.as_str() {
        "sma_crossover" => {
            let fast = if cfg.strategy.fast == 0 {
                3
            } else {
                cfg.strategy.fast
            };
            let slow = if cfg.strategy.slow == 0 {
                8
            } else {
                cfg.strategy.slow
            };
            let size = if cfg.strategy.trade_size == 0.0 {
                10.0
            } else {
                cfg.strategy.trade_size
            };
            SmaCrossover::new(instrument, fast, slow, size)
        }
        other => anyhow::bail!("unknown strategy: {other}"),
    };

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

    let (strategy, _exec, fills) = runner.into_parts();
    let trips = pair_fills(&fills);
    if trips.is_empty() {
        eprintln!("strategy={} produced no round trips", strategy.name());
        return Ok(());
    }

    let curve = equity_curve(cfg.starting_equity, &trips);
    let returns: Vec<f64> = curve.windows(2).map(|w| (w[1] - w[0]) / w[0]).collect();

    let report = PerformanceReport::from_returns(&trips, &returns, 252.0, 0.0)?;
    if let Some(path) = output {
        let file = fs::File::create(path)
            .with_context(|| format!("creating report {}", path.display()))?;
        MarkdownReportWriter::new(file).write(&report)?;
    } else {
        MarkdownReportWriter::new(std::io::stdout().lock()).write(&report)?;
    }
    Ok(())
}
