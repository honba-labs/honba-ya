//! Automatic Strategy Enhancer (GA / Bayesian / WFO) CLI runner.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::Args;

use honba_core::{Exchange, InstrumentId, Price, Quantity, Tick};
use honba_strategy::{BarAggregation, ParameterSpec, StrategyManifest, TimeframeSpec, Universe};
use honba_ya_enhancer::{BayesianConfig, EnhancerEngine, GaConfig, WalkForwardConfig};

#[derive(Args, Debug, Clone)]
pub struct OptimizeArgs {
    /// Strategy name (e.g. `sma_crossover`, `rsi_reversal`) or path to manifest JSON
    #[arg(long, default_value = "sma_crossover")]
    pub strategy: String,
    /// Optimization method: `ga` (Genetic Algorithm), `bayesian` (Gaussian Process), or `wfo` (Walk-Forward)
    #[arg(long, default_value = "ga")]
    pub method: String,
    /// Enable Walk-Forward Optimization (WFO)
    #[arg(long)]
    pub wfo: bool,
    /// Data directory holding the Parquet bar catalog
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Symbol to optimize for (default: RELIANCE)
    #[arg(long, default_value = "RELIANCE")]
    pub symbol: String,
    /// Exchange: NSE (default), BSE, or MCX
    #[arg(long, default_value = "NSE")]
    pub exchange: String,
    /// Number of generations for GA
    #[arg(long, default_value_t = 5)]
    pub generations: usize,
    /// Population size for GA
    #[arg(long, default_value_t = 8)]
    pub population: usize,
    /// Number of trials for Bayesian optimizer
    #[arg(long, default_value_t = 10)]
    pub trials: usize,
    /// Output file path to save optimization report
    #[arg(short, long)]
    pub output: Option<PathBuf>,
}

pub fn run(args: &OptimizeArgs) -> Result<()> {
    let exchange: Exchange = args.exchange.parse().map_err(|_| {
        anyhow::anyhow!(
            "unknown exchange {:?}; expected NSE, BSE or MCX",
            args.exchange
        )
    })?;
    let instrument = InstrumentId::new(exchange, args.symbol.clone());
    let quantity = Quantity::from_u64(100);

    // Build or load manifest
    let manifest = if Path::new(&args.strategy).exists() {
        let text = fs::read_to_string(&args.strategy)
            .with_context(|| format!("reading manifest {}", args.strategy))?;
        serde_json::from_str(&text).context("parsing strategy manifest JSON")?
    } else {
        match args.strategy.as_str() {
            "sma_crossover" => {
                let mut m = StrategyManifest::new(
                    "sma_crossover",
                    "cli-opt-sma",
                    Universe::Explicit(vec![instrument.clone()]),
                    TimeframeSpec::new(1, BarAggregation::Minute),
                );
                m = m
                    .with_parameter(
                        "fast",
                        ParameterSpec::new(serde_json::json!(5.0)).with_bounds(2.0, 15.0, 1.0),
                    )
                    .with_parameter(
                        "slow",
                        ParameterSpec::new(serde_json::json!(20.0)).with_bounds(16.0, 40.0, 1.0),
                    );
                m
            }
            "rsi_reversal" => {
                let mut m = StrategyManifest::new(
                    "rsi_reversal",
                    "cli-opt-rsi",
                    Universe::Explicit(vec![instrument.clone()]),
                    TimeframeSpec::new(1, BarAggregation::Minute),
                );
                m = m
                    .with_parameter(
                        "period",
                        ParameterSpec::new(serde_json::json!(14.0)).with_bounds(7.0, 21.0, 1.0),
                    )
                    .with_parameter(
                        "oversold",
                        ParameterSpec::new(serde_json::json!(30.0)).with_bounds(20.0, 40.0, 2.0),
                    )
                    .with_parameter(
                        "overbought",
                        ParameterSpec::new(serde_json::json!(70.0)).with_bounds(60.0, 80.0, 2.0),
                    );
                m
            }
            other => bail!("unknown strategy for optimization: {other}"),
        }
    };

    // Load or generate ticks
    let ticks: Vec<Tick> = if let Some(dir) = &args.data_dir {
        let loader = honba_backtest::catalog::CatalogLoader::new(dir);
        let bars = loader
            .load_bars(&instrument, "1m", None, None)
            .or_else(|_| loader.load_bars(&instrument, "1d", None, None))?;
        bars.into_iter()
            .map(|b| Tick::Ltp {
                instrument: b.instrument,
                price: b.close,
                volume: b.volume,
                exchange_ts: b.timestamp,
                receive_ts: b.timestamp,
            })
            .collect()
    } else {
        // Deterministic synthetic tick series
        let base_ts = Utc::now();
        let prices = [
            1500.0, 1510.0, 1520.0, 1490.0, 1480.0, 1470.0, 1495.0, 1530.0, 1550.0, 1580.0, 1600.0,
            1590.0, 1570.0, 1540.0, 1520.0, 1500.0, 1480.0, 1460.0, 1510.0, 1540.0,
        ];
        prices
            .iter()
            .enumerate()
            .map(|(i, &p)| {
                let ts = base_ts + chrono::Duration::minutes(i as i64);
                Tick::Ltp {
                    instrument: instrument.clone(),
                    price: Price::from_rupees(&p.to_string()).unwrap(),
                    volume: Quantity::from_u64(100),
                    exchange_ts: ts,
                    receive_ts: ts,
                }
            })
            .collect()
    };

    let calendar = Arc::new(honba_backtest::calendar::NseTradingCalendar::default());
    let mut risk_engine = honba_mon::RiskEngine::default();
    risk_engine.register_algo(&manifest.name);

    let mut engine = EnhancerEngine::with_config(
        GaConfig {
            population_size: args.population,
            generations: args.generations,
            ..Default::default()
        },
        BayesianConfig {
            initial_points: args.trials.min(5),
            total_points: args.trials,
            ..Default::default()
        },
        WalkForwardConfig::default(),
    );

    let report_content = if args.wfo || args.method == "wfo" {
        println!("Running Walk-Forward Optimization (WFO)...");
        let wfo_result = engine.run_wfo(
            &manifest,
            &ticks,
            instrument.clone(),
            quantity,
            risk_engine.clone(),
            calendar.clone(),
            |m, data, inst, qty, rk, cal| {
                let res = engine
                    .optimize_ga(m, data, inst, qty, rk, cal)
                    .map_err(|e| e.to_string())?;
                let mut best = BTreeMap::new();
                for (k, v) in res.best_individual.params {
                    if let Some(num) = v.as_f64() {
                        best.insert(k, num);
                    } else if let Some(n) = v.as_i64() {
                        best.insert(k, n as f64);
                    }
                }
                Ok(best)
            },
        );
        let avg_is: f64 = if !wfo_result.in_sample_sharpe.is_empty() {
            wfo_result.in_sample_sharpe.iter().sum::<f64>()
                / wfo_result.in_sample_sharpe.len() as f64
        } else {
            0.0
        };
        let avg_oos: f64 = if !wfo_result.out_sample_sharpe.is_empty() {
            wfo_result.out_sample_sharpe.iter().sum::<f64>()
                / wfo_result.out_sample_sharpe.len() as f64
        } else {
            0.0
        };
        format!(
            "# Walk-Forward Optimization Report\n\n- Strategy: {}\n- Runs Completed: {}\n- Average WFE: {:.4}\n- WFE Std: {:.4}\n- Certified Robust: {}\n- Mean IS Sharpe: {:.4}\n- Mean OOS Sharpe: {:.4}\n",
            manifest.name,
            wfo_result.n_runs,
            wfo_result.avg_wfe,
            wfo_result.wfe_std,
            wfo_result.certified,
            avg_is,
            avg_oos
        )
    } else if args.method == "bayesian" {
        println!("Running Bayesian Hyperparameter Optimization...");
        let bayes_result = engine
            .optimize_bayesian(
                &manifest,
                &ticks,
                instrument.clone(),
                quantity,
                risk_engine,
                calendar,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut out = format!(
            "# Bayesian Optimization Report\n\n- Strategy: {}\n- Best Fitness (Sharpe): {:.4}\n- Evaluations: {}\n\n## Optimal Parameters\n",
            manifest.name, bayes_result.best_fitness, bayes_result.total_evaluations
        );
        for (k, v) in &bayes_result.best_params {
            out.push_str(&format!("- `{k}`: {v:.4}\n"));
        }
        out
    } else {
        println!("Running Genetic Algorithm (GA) Optimization...");
        let ga_result = engine
            .optimize_ga(
                &manifest,
                &ticks,
                instrument.clone(),
                quantity,
                risk_engine,
                calendar,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut out = format!(
            "# GA Optimization Report\n\n- Strategy: {}\n- Generations Run: {}\n- Pareto Solutions: {}\n- Best Fitness: {:.4} (Sharpe: {:.4})\n\n## Best Parameters\n",
            manifest.name,
            ga_result.generations_run,
            ga_result.pareto_frontier.len(),
            ga_result.best_individual.fitness,
            ga_result.best_individual.sharpe
        );
        for (k, v) in &ga_result.best_individual.params {
            out.push_str(&format!("- `{k}`: {v}\n"));
        }
        out
    };

    println!("{report_content}");
    if let Some(path) = &args.output {
        fs::write(path, &report_content)
            .with_context(|| format!("writing optimization report to {}", path.display()))?;
        println!("Report saved to {}", path.display());
    }

    Ok(())
}
