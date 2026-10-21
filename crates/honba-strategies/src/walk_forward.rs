//! Walk-Forward Analysis (WFO) for strategy validation.

use std::collections::BTreeMap;
use std::time::Instant;

use honba_core::Tick;
use honba_actors::Strategy;

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
    /// Number of WFO runs performed.
    pub n_runs: usize,
    /// Whether all WFE scores meet the certification threshold (WFE >= 0.60).
    pub certified: bool,
}

/// Perform walk-forward analysis on a strategy given market data.
pub fn run_wfo<S: Strategy + Send + 'static>(
    strategy: S,
    data: &[Tick],
    config: WalkForwardConfig,
) -> WalkForwardResult {
    let n_points = data.len();
    let mut wfe_scores = Vec::new();
    let mut in_sample_sharpe = Vec::new();
    let mut out_sample_sharpe = Vec::new();

    let mut start = 0usize;
    let mut run_idx = 0;

    while start + config.in_sample_length + config.out_sample_length <= n_points {
        let is_start = start;
        let is_end = start + config.in_sample_length;
        let oos_start = is_end;
        let oos_end = oos_start + config.out_sample_length;

        // In-sample: train/fit strategy on early data
        let is_data = &data[is_start..is_end];
        let mut in_strategy = strategy.clone();

        // In a full implementation, would fit/optimize strategy parameters on IS data
        // For now, use the strategy as-is

        // Compute in-sample fitness
        let is_fitness = compute_sharpe(is_data, &in_strategy);
        in_sample_sharpe.push(is_fitness);

        // Out-of-sample: validate on subsequent data, never seen during training
        let oos_data = &data[oos_start..oos_end];
        let mut oos_strategy = in_strategy.clone();

        // In a full implementation, would NOT refit on OOS data
        // For now, same strategy
        let oos_fitness = compute_sharpe(oos_data, &oos_strategy);

        // Compute WFE if requested
        if config.compute_wfe && is_fitness > 0.0 {
            let wfe = oos_fitness / is_fitness;
            wfe_scores.push(wfe);
        }

        out_sample_sharpe.push(oos_fitness);

        // Advance by step size
        start += config.step_size;
        run_idx += 1;
    }

    let n_runs = wfe_scores.len();
    let certified = wfe_scores
        .iter()
        .all(|&wfe| wfe >= 0.60);

    WalkForwardResult {
        wfe_scores,
        in_sample_sharpe,
        out_sample_sharpe,
        n_runs,
        certified,
    }
}

fn compute_sharpe(data: &[honba_core::Tick], strategy: &dyn honba_actors::Strategy) -> f64 {
    // Placeholder: in full implementation, run strategy on data, compute returns,
    // then compute annualized Sharpe ratio
    // For now, return a simple metric based on data length
    if data.is_empty() {
        return 0.0;
    }
    let n = data.len() as f64;
    // Simple approximation: longer data with positive trend = higher Sharpe
    let trend: f64 = data.windows(2).map(|w| w[1].price.0 - w[0].price.0).sum();
    (trend / n.max(1.0)).abs()
}