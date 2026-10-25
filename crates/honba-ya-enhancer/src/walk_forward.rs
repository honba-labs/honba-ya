//! Walk-Forward Analysis (WFO) for strategy validation.

use rand::Rng;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

use honba_backtest::NseTradingCalendar;
use honba_core::{InstrumentId, Quantity, Tick};
use honba_mon::RiskEngine;
use honba_strategy::{StrategyManifest, factory::StrategyFactoryRegistry};

/// Walk-Forward Analysis configuration.
#[derive(Debug, Clone)]
pub struct WalkForwardConfig {
    /// In-sample (training) period length in bars.
    pub in_sample_length: usize,
    /// Out-of-sample (validation) period length in bars.
    pub out_sample_length: usize,
    /// Step size: how many bars to advance between each WFO run.
    pub step_size: usize,
    /// Whether to compute walk-forward efficiency (WFE).
    pub compute_wfe: bool,
    /// Minimum WFE threshold for certification (default 0.60).
    pub wfe_threshold: f64,
}

impl Default for WalkForwardConfig {
    fn default() -> Self {
        Self {
            in_sample_length: 252, // ~1 year of daily bars
            out_sample_length: 63, // ~1 quarter
            step_size: 63,         // Advance by 1 quarter
            compute_wfe: true,
            wfe_threshold: 0.60,
        }
    }
}

/// Walk-Forward Analysis result.
#[derive(Debug, Clone)]
pub struct WalkForwardResult {
    /// WFE (Walk-Forward Efficiency) scores per run.
    pub wfe_scores: Vec<f64>,
    /// In-sample Sharpe ratios per run.
    pub in_sample_sharpe: Vec<f64>,
    /// Out-of-sample Sharpe ratios per run.
    pub out_sample_sharpe: Vec<f64>,
    /// In-sample net PnL per run.
    pub in_sample_pnl: Vec<Decimal>,
    /// Out-of-sample net PnL per run.
    pub out_sample_pnl: Vec<Decimal>,
    /// Best parameters found in each in-sample period.
    pub best_params_per_run: Vec<BTreeMap<String, f64>>,
    /// Number of WFO runs performed.
    pub n_runs: usize,
    /// Whether all WFE scores meet the certification threshold (WFE >= threshold).
    pub certified: bool,
    /// Average WFE across all runs.
    pub avg_wfe: f64,
    /// Standard deviation of WFE.
    pub wfe_std: f64,
}

/// Perform walk-forward analysis on a strategy given market data.
///
/// This function:
/// 1. Splits data into rolling in-sample/out-of-sample windows
/// 2. Optimizes parameters on each in-sample window (using GA or Bayesian)
/// 3. Validates on the corresponding out-of-sample window
/// 4. Computes Walk-Forward Efficiency (WFE)
#[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
pub fn run_wfo<S>(
    manifest: &StrategyManifest,
    data: &[Tick],
    config: WalkForwardConfig,
    instrument: InstrumentId,
    quantity: Quantity,
    risk_engine: RiskEngine,
    calendar: NseTradingCalendar,
    optimizer: &mut S,
) -> WalkForwardResult
where
    S: FnMut(
        &StrategyManifest,
        &[Tick],
        InstrumentId,
        Quantity,
        RiskEngine,
        NseTradingCalendar,
    ) -> Result<BTreeMap<String, f64>, String>,
{
    let n_points = data.len();
    let mut wfe_scores = Vec::new();
    let mut in_sample_sharpe = Vec::new();
    let mut out_sample_sharpe = Vec::new();
    let mut in_sample_pnl = Vec::new();
    let mut out_sample_pnl = Vec::new();
    let mut best_params_per_run = Vec::new();

    let mut start = 0usize;
    let mut run_idx = 0;

    while start + config.in_sample_length + config.out_sample_length <= n_points {
        let is_start = start;
        let is_end = start + config.in_sample_length;
        let oos_start = is_end;
        let oos_end = oos_start + config.out_sample_length;

        eprintln!(
            "WFO Run {}: IS [{}:{}] ({} bars), OOS [{}:{}] ({} bars)",
            run_idx,
            is_start,
            is_end,
            config.in_sample_length,
            oos_start,
            oos_end,
            config.out_sample_length
        );

        // In-sample data
        let is_data = &data[is_start..is_end];

        // Optimize on in-sample data
        let best_params = match optimizer(
            manifest,
            is_data,
            instrument.clone(),
            quantity,
            risk_engine.clone(),
            calendar.clone(),
        ) {
            Ok(params) => params,
            Err(e) => {
                eprintln!("WFO Run {} optimization failed: {}", run_idx, e);
                start += config.step_size;
                run_idx += 1;
                continue;
            }
        };
        best_params_per_run.push(best_params.clone());

        // Evaluate in-sample performance with best params
        let is_fitness = evaluate_with_params(
            manifest,
            &best_params,
            is_data,
            instrument.clone(),
            quantity,
            risk_engine.clone(),
            calendar.clone(),
        );
        in_sample_sharpe.push(is_fitness.sharpe);
        in_sample_pnl.push(is_fitness.net_pnl);

        // Out-of-sample data
        let oos_data = &data[oos_start..oos_end];

        // Evaluate on out-of-sample data (never seen during training)
        let oos_fitness = evaluate_with_params(
            manifest,
            &best_params,
            oos_data,
            instrument.clone(),
            quantity,
            risk_engine.clone(),
            calendar.clone(),
        );
        out_sample_sharpe.push(oos_fitness.sharpe);
        out_sample_pnl.push(oos_fitness.net_pnl);

        // Compute WFE if requested
        if config.compute_wfe && is_fitness.sharpe > 0.0 {
            let wfe = oos_fitness.sharpe / is_fitness.sharpe;
            wfe_scores.push(wfe);
            eprintln!(
                "WFO Run {}: IS Sharpe={:.4}, OOS Sharpe={:.4}, WFE={:.4}",
                run_idx, is_fitness.sharpe, oos_fitness.sharpe, wfe
            );
        } else {
            eprintln!(
                "WFO Run {}: IS Sharpe={:.4}, OOS Sharpe={:.4}, WFE=N/A",
                run_idx, is_fitness.sharpe, oos_fitness.sharpe
            );
        }

        // Advance by step size
        start += config.step_size;
        run_idx += 1;
    }

    let n_runs = wfe_scores.len();
    let avg_wfe = if n_runs > 0 {
        wfe_scores.iter().sum::<f64>() / n_runs as f64
    } else {
        0.0
    };

    let wfe_std = if n_runs > 1 {
        let variance = wfe_scores
            .iter()
            .map(|&w| (w - avg_wfe).powi(2))
            .sum::<f64>()
            / (n_runs - 1) as f64;
        variance.sqrt()
    } else {
        0.0
    };

    let certified = n_runs > 0 && wfe_scores.iter().all(|&wfe| wfe >= config.wfe_threshold);

    eprintln!(
        "WFO Complete: {} runs, Avg WFE={:.4} ± {:.4}, Certified={}",
        n_runs, avg_wfe, wfe_std, certified
    );

    WalkForwardResult {
        wfe_scores,
        in_sample_sharpe,
        out_sample_sharpe,
        in_sample_pnl,
        out_sample_pnl,
        best_params_per_run,
        n_runs,
        certified,
        avg_wfe,
        wfe_std,
    }
}

/// Fitness result from a backtest evaluation.
#[derive(Debug, Clone)]
pub struct FitnessResult {
    pub sharpe: f64,
    pub sortino: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
    pub net_pnl: Decimal,
    pub total_trades: usize,
}

/// Evaluate a strategy with given parameters on data.
fn evaluate_with_params(
    manifest: &StrategyManifest,
    params: &BTreeMap<String, f64>,
    data: &[Tick],
    instrument: InstrumentId,
    quantity: Quantity,
    _risk_engine: RiskEngine,
    _calendar: NseTradingCalendar,
) -> FitnessResult {
    // Convert params to JSON for factory
    let param_json: BTreeMap<String, serde_json::Value> = params
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();

    // Create strategy from factory
    let factory_registry = StrategyFactoryRegistry::new();
    let strategy =
        factory_registry.create(&manifest.name, &param_json, instrument.clone(), quantity);

    match strategy {
        Ok(_strat) => {
            // Run backtest (placeholder - in real implementation would use BacktestEngine)
            // For now return synthetic results
            let mut rng = rand::thread_rng();
            let n = data.len() as f64;
            let base_sharpe = (n / 1000.0).min(3.0);
            let noise = rng.gen_range(-0.3..0.3);
            let sharpe = (base_sharpe + noise).clamp(-2.0, 5.0);

            FitnessResult {
                sharpe,
                sortino: sharpe * 0.9,
                profit_factor: 1.0 + sharpe.abs() * 0.2,
                max_drawdown_pct: 5.0 + rng.gen_range(-2.0..5.0),
                net_pnl: Decimal::from((sharpe * 10000.0) as i64),
                total_trades: (n / 10.0) as usize,
            }
        }
        Err(_) => FitnessResult {
            sharpe: f64::NEG_INFINITY,
            sortino: f64::NEG_INFINITY,
            profit_factor: 0.0,
            max_drawdown_pct: 100.0,
            net_pnl: Decimal::MIN,
            total_trades: 0,
        },
    }
}
