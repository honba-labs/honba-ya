use std::collections::VecDeque;
use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use honba_analytics::{PerformanceReport, RoundTrip};
use honba_core::chrono::{DateTime, Utc};
use honba_core::rust_decimal::prelude::*;
use honba_core::{Exchange, FillReport, InstrumentId, OrderSide, Price, Quantity};
use honba_feed::{MarkdownReportWriter, ReportWriter};

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

/// A minimal incremental simple moving average, local to this file so the
/// CLI's synthetic demo backtest doesn't need the full `honba_indicators`
/// crate (or any other new dependency) just to detect SMA crossovers.
struct Sma {
    period: usize,
    buf: VecDeque<f64>,
    sum: f64,
}

impl Sma {
    fn new(period: usize) -> Self {
        Self {
            period,
            buf: VecDeque::with_capacity(period),
            sum: 0.0,
        }
    }

    fn update(&mut self, value: f64) -> Option<f64> {
        self.buf.push_back(value);
        self.sum += value;
        if self.buf.len() > self.period {
            self.sum -= self.buf.pop_front().unwrap_or(0.0);
        }
        if self.buf.len() == self.period {
            Some(self.sum / self.period as f64)
        } else {
            None
        }
    }
}

/// Builds a UTC timestamp from `nanos` nanoseconds since the Unix epoch.
fn ts_from_nanos(nanos: u64) -> DateTime<Utc> {
    let nanos = nanos as i64;
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, sub).expect("fixture timestamp out of range")
}

pub(crate) fn pair_fills(fills: &[FillReport]) -> Vec<RoundTrip> {
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
        cur += t.net_pnl.to_f64().unwrap_or(0.0);
        equity.push(cur);
    }
    equity
}

pub fn run(config_path: &Path, output: Option<&Path>) -> anyhow::Result<()> {
    let text = fs::read_to_string(config_path)
        .with_context(|| format!("reading config {}", config_path.display()))?;
    let cfg: BacktestConfig = toml::from_str(&text)?;

    let exchange: Exchange = cfg.exchange.parse().map_err(|_| {
        anyhow::anyhow!(
            "unknown exchange {:?}; expected NSE, BSE or MCX",
            cfg.exchange
        )
    })?;
    let instrument = InstrumentId::new(exchange, cfg.symbol.clone());

    let (fast_period, slow_period, trade_size) = match cfg.strategy.name.as_str() {
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
            (fast, slow, size)
        }
        other => anyhow::bail!("unknown strategy: {other}"),
    };
    let quantity = Quantity::new(Decimal::from_f64_retain(trade_size).unwrap_or(Decimal::ONE));

    // Synthetic series — same shape as the end-to-end example.
    let closes: Vec<f64> = vec![
        100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0, 102.0,
        103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 105.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0,
        110.0, 111.0, 110.0, 109.0, 108.0,
    ];

    let mut fast = Sma::new(fast_period);
    let mut slow = Sma::new(slow_period);
    let mut prev_above: Option<bool> = None;
    let mut fills: Vec<FillReport> = Vec::new();

    for (i, &close) in closes.iter().enumerate() {
        let (Some(f), Some(s)) = (fast.update(close), slow.update(close)) else {
            continue;
        };
        let above = f > s;
        let crossed = matches!(prev_above, Some(prev) if prev != above);
        prev_above = Some(above);
        if !crossed {
            continue;
        }

        let side = if above { OrderSide::Buy } else { OrderSide::Sell };
        let price = Price::new(Decimal::from_f64_retain(close).unwrap_or(Decimal::ZERO));
        let timestamp = ts_from_nanos((i as u64 + 1) * 1_000_000_000);
        fills.push(FillReport::new(
            format!("F-{i}"),
            format!("O-{i}"),
            honba_core::uuid::Uuid::new_v4(),
            instrument.clone(),
            side,
            price,
            quantity,
            "cli-backtest-sma-crossover",
            timestamp,
        ));
    }

    let trips = pair_fills(&fills);
    if trips.is_empty() {
        eprintln!("strategy={} produced no round trips", cfg.strategy.name);
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
