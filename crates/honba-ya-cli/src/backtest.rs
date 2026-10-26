use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::Deserialize;

use honba_analytics::{PerformanceReport, RoundTrip};
use honba_backtest::calendar::NseTradingCalendar;
use honba_backtest::{BacktestConfig as EngineConfig, BacktestEngine, MarketEvent, ProductType};
use honba_core::{Exchange, FillReport, InstrumentId, Price, Quantity};
use honba_feed::{MarkdownReportWriter, ReportWriter};
use honba_strategy::DynStrategy;
use honba_strategy::factory::StrategyFactoryRegistry;

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

pub fn run(
    config_path: &Path,
    output: Option<&Path>,
    from: Option<&str>,
    to: Option<&str>,
    data_dir: Option<&Path>,
) -> Result<()> {
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
        other => bail!("unknown strategy: {other}"),
    };

    let quantity = Quantity::new(Decimal::from_f64_retain(trade_size).unwrap_or(Decimal::ONE));

    // Construct the strategy dynamically from the registry
    let registry = StrategyFactoryRegistry::new();
    let mut params = BTreeMap::new();
    params.insert("fast".to_string(), serde_json::json!(fast_period));
    params.insert("slow".to_string(), serde_json::json!(slow_period));
    let strategy: DynStrategy = registry
        .create(&cfg.strategy.name, &params, instrument.clone(), quantity)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let engine_cfg = EngineConfig {
        starting_cash: Decimal::from_f64_retain(cfg.starting_equity).unwrap_or(dec!(1_000_000.00)),
        product_type: ProductType::Delivery,
        l2_simulation: false,
        latency: None,
        slippage: None,
        adverse_fill: None,
        mis_square_off: None,
        warmup_bars: 0,
        iccl_margin_holdback_pct: dec!(0.20),
    };

    let mut risk_engine = honba_mon::RiskEngine::default();
    risk_engine.register_algo(&cfg.strategy.name);
    let calendar = Arc::new(NseTradingCalendar::default());

    // Phase 7.1: Re-wire directly to honba_ya_backtest::BacktestEngine
    let mut engine = BacktestEngine::new(strategy, risk_engine, engine_cfg, calendar);

    // If data directory with catalog is supplied, load real parquet bars;
    // otherwise use the canonical synthetic series.
    let result = if let Some(dir) = data_dir {
        let loader = honba_backtest::catalog::CatalogLoader::new(dir);
        let from_ts = from.and_then(|s| DateTime::parse_from_rfc3339(s).ok().map(|d| d.to_utc()));
        let to_ts = to.and_then(|s| DateTime::parse_from_rfc3339(s).ok().map(|d| d.to_utc()));
        engine.run_catalog(&loader, &[instrument], "1d", from_ts, to_ts)?
    } else {
        // Synthetic series — same shape as the end-to-end example.
        let closes: Vec<f64> = vec![
            100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 101.0,
            102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 105.0, 104.0, 105.0, 106.0, 107.0,
            108.0, 109.0, 110.0, 111.0, 110.0, 109.0, 108.0,
        ];
        let events: Vec<MarketEvent> = closes
            .iter()
            .enumerate()
            .map(|(i, &close)| {
                let px = Price::new(Decimal::from_f64_retain(close).unwrap_or(Decimal::ZERO));
                let ts = ts_from_nanos((i as u64 + 1) * 1_000_000_000);
                MarketEvent::Bar(honba_core::Bar {
                    instrument: instrument.clone(),
                    open: px,
                    high: px,
                    low: px,
                    close: px,
                    volume: Quantity::from_u64(100),
                    timestamp: ts,
                })
            })
            .collect();
        engine.run_events(events)
    };

    let trips = pair_fills(&result.fills);
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
