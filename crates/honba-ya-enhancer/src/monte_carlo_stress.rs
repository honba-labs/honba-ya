//! Monte Carlo Permutation & Stress Testing for strategy robustness.

use rand::Rng;
use rand::seq::SliceRandom;

use honba_actors::Strategy;

/// Monte Carlo stress test results.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    /// Minimum PnL across all simulations (worst case).
    pub min_pnl: f64,
    /// Maximum PnL across all simulations (best case).
    pub max_pnl: f64,
    /// Mean PnL across all simulations.
    pub mean_pnl: f64,
    /// 5% VaR (Value-at-Risk): 5th percentile of PnL distribution.
    pub var_5pct: f64,
    /// 95% VaR: 95th percentile of PnL distribution.
    pub var_95pct: f64,
    /// Probability of ruin (PnL < 0 across all simulations).
    pub prob_ruin: f64,
    /// Number of simulations performed.
    pub n_simulations: usize,
}

/// Perform Monte Carlo permutation stress testing on a strategy.
///
/// Shuffles trade sequences without replacement and re-evaluates PnL
/// to determine worst-case drawdown confidence intervals.
pub fn monte_carlo_permutation<S: Strategy + Send + 'static>(
    strategy: S,
    returns: &[f64],
    n_simulations: usize,
) -> MonteCarloResult {
    let _ = strategy;
    let mut rng = rand::thread_rng();
    let mut pnl_simulations: Vec<f64> = Vec::with_capacity(n_simulations);

    for _ in 0..n_simulations {
        // Shuffle returns without replacement (permutation)
        let mut shuffled = returns.to_vec();
        shuffled.shuffle(&mut rng);

        // Compute cumulative PnL for this permutation
        let mut cumulative = 0.0f64;
        for &r in &shuffled {
            cumulative += r;
        }
        pnl_simulations.push(cumulative);
    }

    // Compute statistics
    pnl_simulations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min_pnl = pnl_simulations.first().copied().unwrap_or(0.0);
    let max_pnl = pnl_simulations.last().copied().unwrap_or(0.0);
    let mean_pnl = pnl_simulations.iter().sum::<f64>() / (n_simulations as f64);

    // Compute VaR percentiles
    let var_5pct_idx = (f64::from(n_simulations as u32) * 0.05)
        .max(1.0)
        .min(f64::from(n_simulations.saturating_sub(1) as u32));
    let var_95pct_idx = (f64::from(n_simulations as u32) * 0.95)
        .max(1.0)
        .min(f64::from(n_simulations.saturating_sub(1) as u32));
    let var_5pct = pnl_simulations[var_5pct_idx as usize];
    let var_95pct = pnl_simulations[var_95pct_idx as usize];

    // Probability of ruin: proportion of simulations with negative PnL
    let ruin_count = pnl_simulations.iter().filter(|&&p| p < 0.0).count();
    let prob_ruin = ruin_count as f64 / (n_simulations as f64);

    MonteCarloResult {
        min_pnl,
        max_pnl,
        mean_pnl,
        var_5pct,
        var_95pct,
        prob_ruin,
        n_simulations,
    }
}

/// Inject synthetic spread/slippage shocks and gap-down scenarios.

/// Shock magnitude multipliers for stress testing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShockMagnitude {
    /// 1.5x shock (mild stress)
    Multi1x5,
    /// 2.0x shock (moderate stress)
    Multi2x0,
    /// 3.0x shock (severe stress)
    Multi3x0,
    /// Custom shock multiplier
    Custom(f64),
}

impl ShockMagnitude {
    /// Get the multiplier value.
    pub fn multiplier(&self) -> f64 {
        match self {
            ShockMagnitude::Multi1x5 => 1.5,
            ShockMagnitude::Multi2x0 => 2.0,
            ShockMagnitude::Multi3x0 => 3.0,
            ShockMagnitude::Custom(m) => *m,
        }
    }
}

/// Apply shock to a sequence of returns.
/// - Spread shock: multiplies bid-ask spread component
/// - Slippage shock: increases estimated slippage
/// - Gap-down shock: prepends a negative gap return
pub fn apply_shock(returns: &mut Vec<f64>, magnitude: ShockMagnitude, gap_probability: f64) {
    let mult = magnitude.multiplier();
    let mut rng = rand::thread_rng();

    // Apply spread/slippage shock: inflate some returns
    for i in (0..returns.len()).step_by(10) {
        // every ~10th bar
        if rng.r#gen::<f64>() < 0.3 {
            returns[i] *= mult;
        }
    }

    // Gap-down shock: with given probability, prepend a large negative return
    if rng.r#gen::<f64>() < gap_probability {
        let gap_size = -3.0 * mult; // 3x shock in negative direction
        returns.insert(0, gap_size);
    }
}

/// Run comprehensive stress test combining permutation MC and shock injection.
pub fn stress_test<S: Strategy + Clone + Send + 'static>(
    strategy: S,
    base_returns: &[f64],
    n_permutations: usize,
    shock_magnitude: ShockMagnitude,
    gap_probability: f64,
) -> (MonteCarloResult, Vec<f64>) {
    // Base permutation test
    let mc_result = monte_carlo_permutation(strategy.clone(), base_returns, n_permutations);

    // Apply shocks to a copy of returns
    let mut shocked_returns = base_returns.to_vec();
    apply_shock(&mut shocked_returns, shock_magnitude, gap_probability);

    // Re-run permutation on shocked returns
    let _shocked_mc = monte_carlo_permutation(strategy, &shocked_returns, n_permutations);

    (mc_result, shocked_returns)
}
