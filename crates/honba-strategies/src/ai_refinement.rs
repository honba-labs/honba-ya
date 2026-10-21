//! Closed-Loop AI Strategy Refinement Integration.

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

use honba_core::Tick;
use honba_actors::Strategy;
use honba_nou::prompt_replayer::ReplayDecision;

/// Diagnostic report from strategy backtest, fed into the AI refinement loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyDiagnostic {
    /// Strategy name/identifier.
    pub strategy_name: String,
    /// Backtest Sharpe ratio.
    pub sharpe: f64,
    /// Backtest Sortino ratio.
    pub sortino: f64,
    /// Maximum drawdown percentage.
    pub max_drawdown_pct: f64,
    /// Total number of trades.
    pub total_trades: usize,
    /// Number of winning trades.
    pub winning_trades: usize,
    /// Number of losing trades.
    pub losing_trades: usize,
    /// Total fees paid in INR.
    pub total_fees: f64,
    /// Profit factor (gross / net).
    pub profit_factor: f64,
    /// Walk-forward efficiency.
    pub wfe: Option<f64>,
    /// DSR value.
    pub dsr: Option<f64>,
    /// PBO value.
    pub pbo: Option<f64>,
    /// Parameter sensitivity summary (which params had most impact).
    pub parameter_sensitivity: BTreeMap<String, f64>,
    /// Observed market regimes during backtest.
    pub observed_regimes: Vec<String>,
    /// Suggested prompt mutations for AI-driven improvement.
    pub suggested_prompt_mutations: Vec<String>,
}

/// Interface with the honba-nou AI Strategy Enhancer loop.
///
/// Ingests a strategy diagnostic report and returns prompt-driven
/// parameter/rule mutations for the next generation of the strategy.
pub fn refine_strategy_with_ai(diagnostic: &StrategyDiagnostic) -> Vec<String> {
    let mut mutations = Vec::new();

    // Analyze diagnostics and generate targeted prompt mutations
    if diagnostic.sharpe < 1.0 {
        mutations.push(
            "Reduce position sizes by 50% and extend holding period".to_string(),
        );
    }

    if diagnostic.max_drawdown_pct > 10.0 {
        mutations.push(
            "Implement stricter risk limits: max drawdown 5% per trade".to_string(),
        );
    }

    if diagnostic.winning_trades as f64 / diagnostic.total_trades.max(1) as f64 < 0.4 {
        mutations.push(
            "Revise entry signal: add filter for trending market regime".to_string(),
        );
    }

    if diagnostic.pbo.unwrap_or(0.0) > 0.5 {
        mutations.push(
            "Increase out-of-sample validation period; reduce overfitting risk".to_string(),
        );
    }

    if diagnostic.dsr.unwrap_or(3.0) < 2.0 {
        mutations.push(
            "Apply DSR adjustment; reduce Sharpe-based optimism in parameter tuning"
                .to_string(),
        );
    }

    if diagnostic.parameter_sensitivity.is_empty() {
        mutations.push(
            "Conduct parameter sensitivity analysis; identify most influential params"
                .to_string(),
        );
    }

    // Always include at least one mutation if diagnostics indicate improvement needed
    if mutations.is_empty() {
        mutations.push(
            "Fine-tune entry/exit thresholds by 0.5% for incremental improvement"
                .to_string(),
        );
    }

    mutations
}

/// Run the closed-loop AI refinement cycle.
///
/// 1. Evaluate strategy on held-out data
/// 2. Generate diagnostic report
/// 3. Interface with honba-nou AI enhancer
/// 4. Return prompt mutations for strategy improvement
pub fn run_closed_loop_refinement(
    strategy: &dyn Strategy,
    held_out_data: &[Tick],
    prev_diagnostic: &Option<StrategyDiagnostic>,
) -> Vec<String> {
    // Step 1: Evaluate strategy on held-out data
    let diagnostic = evaluate_strategy_diagnostic(strategy, held_out_data);

    // Step 2: Compare with previous diagnostic if available
    let mut mutations = Vec::new();

    if let Some(prev) = prev_diagnostic {
        // Compare key metrics and generate diff-based mutations
        if diagnostic.sharpe < prev.sharpe {
            mutations.push(format!(
                "Sharpe declined from {:.2} to {:.2}; add regime filter",
                prev.sharpe, diagnostic.sharpe
            ));
        }

        if diagnostic.max_drawdown_pct > prev.max_drawdown_pct {
            mutations.push(format!(
                "Max drawdown increased from {:.1}% to {:.1}%; tighten circuit breakers",
                prev.max_drawdown_pct, diagnostic.max_drawdown_pct
            ));
        }
    } else {
        // First evaluation: generate baseline mutations
        mutations = refine_strategy_with_ai(&diagnostic);
    }

    mutations
}

fn evaluate_strategy_diagnostic(strategy: &dyn Strategy, data: &[Tick]) -> StrategyDiagnostic {
    let total_trades = data.len();
    let winning_trades = data
        .iter()
        .filter(|&t| t.price.0 > 0.0) // placeholder: winning trade condition
        .count();

    let total_fees: f64 = 0.0; // placeholder: compute from actual fills
    let profit_factor: f64 = 1.0; // placeholder: gross / net
    let max_drawdown_pct: f64 = 0.0; // placeholder: compute from equity curve
    let sharpe: f64 = 0.0; // placeholder: annualized Sharpe
    let sortino: f64 = 0.0; // placeholder: annualized Sortino
    let wfe: Option<f64> = None; // placeholder: WFE from WFO
    let dsr: Option<f64> = None; // placeholder: DSR computation
    let pbo: Option<f64> = None; // placeholder: PBO computation

    let parameter_sensitivity = BTreeMap::new();
    let observed_regimes = vec![
        "trending".to_string(),
        "range-bound".to_string(),
    ];
    let suggested_prompt_mutations = Vec::new();

    StrategyDiagnostic {
        strategy_name: "unnamed".to_string(),
        sharpe,
        sortino,
        max_drawdown_pct,
        total_trades,
        winning_trades,
        total_fees,
        profit_factor,
        wfe,
        dsr,
        pbo,
        parameter_sensitivity,
        observed_regimes,
        suggested_prompt_mutations,
    }
}