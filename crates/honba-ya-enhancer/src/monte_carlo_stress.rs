//! Monte Carlo Permutation & Stress Testing for strategy robustness.

use rand::Rng;
use rand::seq::SliceRandom;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::BTreeMap;

use honba_backtest::NseTradingCalendar;
use honba_core::{InstrumentId, Quantity};
use honba_mon::RiskEngine;
use honba_strategy::{StrategyManifest, factory::StrategyFactoryRegistry};

/// Monte Carlo stress test results.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    /// Minimum PnL across all simulations (worst case).
    pub min_pnl: Decimal,
    /// Maximum PnL across all simulations (best case).
    pub max_pnl: Decimal,
    /// Mean PnL across all simulations.
    pub mean_pnl: Decimal,
    /// Standard deviation of PnL.
    pub std_pnl: Decimal,
    /// 5% VaR (Value-at-Risk): 5th percentile of PnL distribution.
    pub var_5pct: Decimal,
    /// 1% VaR: 1st percentile of PnL distribution.
    pub var_1pct: Decimal,
    /// 95% VaR: 95th percentile of PnL distribution.
    pub var_95pct: Decimal,
    /// 99% VaR: 99th percentile of PnL distribution.
    pub var_99pct: Decimal,
    /// Expected Shortfall (CVaR) at 5%: mean of losses beyond VaR.
    pub cvar_5pct: Decimal,
    /// Expected Shortfall (CVaR) at 1%.
    pub cvar_1pct: Decimal,
    /// Probability of ruin (PnL < 0 across all simulations).
    pub prob_ruin: f64,
    /// Probability of profit (PnL > 0).
    pub prob_profit: f64,
    /// Median PnL.
    pub median_pnl: Decimal,
    /// Skewness of PnL distribution.
    pub skewness: f64,
    /// Excess kurtosis of PnL distribution.
    pub kurtosis: f64,
    /// Maximum drawdown across simulations (worst).
    pub max_drawdown_pct: Decimal,
    /// Number of simulations performed.
    pub n_simulations: usize,
}

/// Stress test results combining base and shocked scenarios.
#[derive(Debug, Clone)]
pub struct StressTestResult {
    /// Base Monte Carlo result (no shocks).
    pub base_result: MonteCarloResult,
    /// Shocked Monte Carlo results by shock magnitude.
    pub shocked_results: BTreeMap<ShockMagnitude, MonteCarloResult>,
    /// Worst-case PnL across all scenarios.
    pub worst_case_pnl: Decimal,
    /// Whether the strategy survives all stress scenarios (PnL > -50% capital).
    pub survives_stress: bool,
}

/// Perform Monte Carlo permutation stress testing on a strategy.
///
/// Shuffles trade sequences without replacement and re-evaluates PnL
/// to determine worst-case drawdown confidence intervals.
#[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
pub fn monte_carlo_permutation(
    _manifest: &StrategyManifest,
    returns: &[f64],
    n_simulations: usize,
    _factory_registry: &StrategyFactoryRegistry,
    _instrument: InstrumentId,
    _quantity: Quantity,
    _risk_engine: RiskEngine,
    _calendar: NseTradingCalendar,
) -> MonteCarloResult {
    let mut rng = rand::thread_rng();
    let mut pnl_simulations: Vec<Decimal> = Vec::with_capacity(n_simulations);
    let mut dd_simulations: Vec<Decimal> = Vec::with_capacity(n_simulations);

    for _ in 0..n_simulations {
        // Shuffle returns without replacement (permutation)
        let mut shuffled = returns.to_vec();
        shuffled.shuffle(&mut rng);

        // Create strategy and run backtest on shuffled returns
        // In a real implementation, this would create a synthetic tick stream
        // from the shuffled returns and run the full backtest

        // For now, simulate PnL from shuffled returns
        let mut cumulative = Decimal::ZERO;
        let mut peak = Decimal::ZERO;
        let mut max_dd = Decimal::ZERO;

        for &r in &shuffled {
            let r_dec = Decimal::from_f64_retain(r).unwrap_or(Decimal::ZERO);
            cumulative += r_dec;

            if cumulative > peak {
                peak = cumulative;
            }
            let dd = peak - cumulative;
            if dd > max_dd {
                max_dd = dd;
            }
        }

        pnl_simulations.push(cumulative);
        if peak > Decimal::ZERO {
            dd_simulations.push((max_dd / peak * Decimal::from(100)).round_dp(2));
        } else {
            dd_simulations.push(Decimal::ZERO);
        }
    }

    // Compute statistics
    pnl_simulations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    dd_simulations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = n_simulations as f64;
    let min_pnl = pnl_simulations.first().copied().unwrap_or(Decimal::ZERO);
    let max_pnl = pnl_simulations.last().copied().unwrap_or(Decimal::ZERO);
    let mean_pnl = pnl_simulations.iter().sum::<Decimal>() / Decimal::from(n_simulations);

    let median_idx = n_simulations / 2;
    let median_pnl = pnl_simulations[median_idx];

    // Standard deviation
    let variance = pnl_simulations
        .iter()
        .map(|&p| {
            let diff = p - mean_pnl;
            (diff * diff).to_f64().unwrap_or(0.0)
        })
        .sum::<f64>()
        / (n_simulations as f64 - 1.0).max(1.0);
    let std_pnl = Decimal::from_f64_retain(variance.sqrt()).unwrap_or(Decimal::ZERO);

    // Percentiles
    let percentile = |p: f64| -> Decimal {
        let idx = ((n * p).ceil() as usize).min(n_simulations - 1);
        pnl_simulations[idx]
    };

    let var_5pct = percentile(0.05);
    let var_1pct = percentile(0.01);
    let var_95pct = percentile(0.95);
    let var_99pct = percentile(0.99);

    // CVaR (Expected Shortfall) - mean of losses beyond VaR
    let cvar_5pct = {
        let tail: Vec<Decimal> = pnl_simulations
            .iter()
            .filter(|&&p| p <= var_5pct)
            .copied()
            .collect();
        if tail.is_empty() {
            var_5pct
        } else {
            tail.iter().sum::<Decimal>() / Decimal::from(tail.len())
        }
    };
    let cvar_1pct = {
        let tail: Vec<Decimal> = pnl_simulations
            .iter()
            .filter(|&&p| p <= var_1pct)
            .copied()
            .collect();
        if tail.is_empty() {
            var_1pct
        } else {
            tail.iter().sum::<Decimal>() / Decimal::from(tail.len())
        }
    };

    // Probabilities
    let ruin_count = pnl_simulations
        .iter()
        .filter(|&&p| p < Decimal::ZERO)
        .count();
    let prob_ruin = ruin_count as f64 / n;
    let profit_count = pnl_simulations
        .iter()
        .filter(|&&p| p > Decimal::ZERO)
        .count();
    let prob_profit = profit_count as f64 / n;

    // Skewness and kurtosis
    let pnl_f64: Vec<f64> = pnl_simulations
        .iter()
        .map(|d| d.to_f64().unwrap_or(0.0))
        .collect();
    let mean_f64 = mean_pnl.to_f64().unwrap_or(0.0);
    let std_f64 = std_pnl.to_f64().unwrap_or(1.0).max(1e-10);

    let skewness = pnl_f64
        .iter()
        .map(|&x| ((x - mean_f64) / std_f64).powi(3))
        .sum::<f64>()
        / n;

    let kurtosis = pnl_f64
        .iter()
        .map(|&x| ((x - mean_f64) / std_f64).powi(4))
        .sum::<f64>()
        / n
        - 3.0;

    // Max drawdown (worst across simulations)
    let max_drawdown_pct = dd_simulations.last().copied().unwrap_or(Decimal::ZERO);

    MonteCarloResult {
        min_pnl,
        max_pnl,
        mean_pnl,
        std_pnl,
        var_5pct,
        var_1pct,
        var_95pct,
        var_99pct,
        cvar_5pct,
        cvar_1pct,
        prob_ruin,
        prob_profit,
        median_pnl,
        skewness,
        kurtosis,
        max_drawdown_pct,
        n_simulations,
    }
}

/// Inject synthetic spread/slippage shocks and gap-down scenarios.
#[derive(Debug, Clone, Copy)]
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

impl PartialEq for ShockMagnitude {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ShockMagnitude::Multi1x5, ShockMagnitude::Multi1x5) => true,
            (ShockMagnitude::Multi2x0, ShockMagnitude::Multi2x0) => true,
            (ShockMagnitude::Multi3x0, ShockMagnitude::Multi3x0) => true,
            (ShockMagnitude::Custom(a), ShockMagnitude::Custom(b)) => a.to_bits() == b.to_bits(),
            _ => false,
        }
    }
}

impl Eq for ShockMagnitude {}

impl PartialOrd for ShockMagnitude {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShockMagnitude {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (ShockMagnitude::Multi1x5, ShockMagnitude::Multi1x5) => Ordering::Equal,
            (ShockMagnitude::Multi2x0, ShockMagnitude::Multi2x0) => Ordering::Equal,
            (ShockMagnitude::Multi3x0, ShockMagnitude::Multi3x0) => Ordering::Equal,
            (ShockMagnitude::Custom(a), ShockMagnitude::Custom(b)) => a.total_cmp(b),
            (ShockMagnitude::Multi1x5, _) => Ordering::Less,
            (_, ShockMagnitude::Multi1x5) => Ordering::Greater,
            (ShockMagnitude::Multi2x0, ShockMagnitude::Multi3x0) => Ordering::Less,
            (ShockMagnitude::Multi2x0, ShockMagnitude::Custom(_)) => Ordering::Less,
            (ShockMagnitude::Multi3x0, ShockMagnitude::Multi2x0) => Ordering::Greater,
            (ShockMagnitude::Multi3x0, ShockMagnitude::Custom(_)) => Ordering::Less,
            (ShockMagnitude::Custom(_), ShockMagnitude::Multi2x0 | ShockMagnitude::Multi3x0) => {
                Ordering::Greater
            }
        }
    }
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
#[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
pub fn stress_test(
    manifest: &StrategyManifest,
    base_returns: &[f64],
    n_permutations: usize,
    shock_magnitudes: &[ShockMagnitude],
    gap_probability: f64,
    factory_registry: &StrategyFactoryRegistry,
    instrument: InstrumentId,
    quantity: Quantity,
    risk_engine: RiskEngine,
    calendar: NseTradingCalendar,
) -> StressTestResult {
    // Base permutation test
    let base_result = monte_carlo_permutation(
        manifest,
        base_returns,
        n_permutations,
        factory_registry,
        instrument.clone(),
        quantity,
        risk_engine.clone(),
        calendar.clone(),
    );

    let mut shocked_results = BTreeMap::new();
    let mut worst_case = base_result.min_pnl;

    for &magnitude in shock_magnitudes {
        // Apply shocks to a copy of returns
        let mut shocked_returns = base_returns.to_vec();
        apply_shock(&mut shocked_returns, magnitude, gap_probability);

        // Re-run permutation on shocked returns
        let shocked_mc = monte_carlo_permutation(
            manifest,
            &shocked_returns,
            n_permutations,
            factory_registry,
            instrument.clone(),
            quantity,
            risk_engine.clone(),
            calendar.clone(),
        );

        worst_case = worst_case.min(shocked_mc.min_pnl);
        shocked_results.insert(magnitude, shocked_mc);
    }

    // Strategy survives stress if worst case is > -50% of initial capital
    let initial_capital = Decimal::from(1_000_000); // 10L INR
    let survives_stress = worst_case > -(initial_capital * Decimal::from_f64_retain(0.5).unwrap());

    StressTestResult {
        base_result,
        shocked_results,
        worst_case_pnl: worst_case,
        survives_stress,
    }
}
