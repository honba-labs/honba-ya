//! Automatic Strategy Enhancer for Honba.
//!
//! Provides multi-objective optimization, Bayesian hyperparameter search,
//! walk-forward analysis, overfitting guardrails, Monte Carlo stress testing,
//! closed-loop AI refinement integration, and StockSharp Odysseus genetic
//! generation backend. Integrates with the honba-ya actor runtime and
//! backtester for closed-loop AI-driven strategy refinement.

pub mod ai_refinement;
pub mod bayesian_optimizer;
pub mod ga_optimizer;
pub mod monte_carlo_stress;
pub mod odysseus;
pub mod overfitting_guardrails;
pub mod walk_forward;

// AI Refinement
pub use ai_refinement::{
    AIRefinementConfig, AIRefinementResult, StrategyDiagnostic, refine_strategy_with_ai,
    run_ai_refinement_loop, run_closed_loop_refinement,
};

// Bayesian Optimizer
pub use bayesian_optimizer::{
    BayesError, BayesOptimizationResult, BayesianConfig, BayesianOptimizer, EvaluatedPoint,
};

// GA Optimizer
pub use ga_optimizer::{GaConfig, GaError, GaOptimizationResult, GaOptimizer, Individual};

// Monte Carlo Stress
pub use monte_carlo_stress::{
    MonteCarloResult, ShockMagnitude, StressTestResult, apply_shock, monte_carlo_permutation,
    stress_test,
};

// Odysseus
pub use odysseus::{
    ComparisonOp, EvalContext, Genome, GenomeFitness, GenomeNode, OdysseusConfig, OdysseusEvaluator,
};

// Overfitting Guardrails
pub use overfitting_guardrails::{
    DSRCalculator, OverfittingAssessment, OverfittingDiagnostics, PBOCalculator,
};

// Walk-Forward
pub use walk_forward::{FitnessResult, WalkForwardConfig, WalkForwardResult, run_wfo};

use honba_backtest::NseTradingCalendar;
use honba_core::Tick;
use honba_mon::RiskEngine;
use honba_strategy::{Strategy, StrategyManifest};
use std::collections::BTreeMap;

/// Optimizer trait for strategy parameter search.
pub trait Optimizer: Send + Sync {
    /// Optimize strategy parameters given a fitness function.
    fn optimize<S: Strategy + Clone + Send + 'static>(
        &self,
        strategy: S,
        data: &[Tick],
        fitness: impl Fn(&S) -> f64,
    ) -> S;

    /// Get optimizer name for logging.
    fn name(&self) -> &str;
}

/// Optimization result containing best parameters and fitness.
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    /// Best fitness value found.
    pub best_fitness: f64,
    /// Parameter values at best fitness (as string map).
    pub best_params: BTreeMap<String, String>,
    /// Number of evaluations performed.
    pub evaluations: usize,
    /// Whether the result converged.
    pub converged: bool,
}

/// High-level optimization entry point that wires together all optimizers
/// with the strategy factory and backtester.
#[derive(Default)]
pub struct EnhancerEngine {
    ga_optimizer: GaOptimizer,
    bayesian_optimizer: BayesianOptimizer,
    walk_forward_config: WalkForwardConfig,
}

impl EnhancerEngine {
    /// Create a new enhancer engine with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new enhancer engine with custom configuration.
    pub fn with_config(
        ga_config: GaConfig,
        bayesian_config: BayesianConfig,
        wfo_config: WalkForwardConfig,
    ) -> Self {
        Self {
            ga_optimizer: GaOptimizer::with_config(ga_config),
            bayesian_optimizer: BayesianOptimizer::with_config(bayesian_config),
            walk_forward_config: wfo_config,
        }
    }

    /// Run GA optimization on a strategy manifest.
    pub fn optimize_ga(
        &self,
        manifest: &StrategyManifest,
        data: &[Tick],
        instrument: honba_core::InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
    ) -> Result<GaOptimizationResult, GaError> {
        self.ga_optimizer
            .optimize(manifest, data, instrument, quantity, risk_engine, calendar)
    }

    /// Run Bayesian optimization on a strategy manifest.
    pub fn optimize_bayesian(
        &mut self,
        manifest: &StrategyManifest,
        data: &[Tick],
        instrument: honba_core::InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
    ) -> Result<BayesOptimizationResult, bayesian_optimizer::BayesError> {
        self.bayesian_optimizer.optimize(
            manifest,
            data,
            instrument,
            quantity,
            risk_engine,
            calendar,
        )
    }

    /// Run walk-forward analysis.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    pub fn run_wfo<F>(
        &self,
        manifest: &StrategyManifest,
        data: &[Tick],
        instrument: honba_core::InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
        mut optimizer: F,
    ) -> WalkForwardResult
    where
        F: FnMut(
            &StrategyManifest,
            &[Tick],
            honba_core::InstrumentId,
            honba_core::Quantity,
            RiskEngine,
            NseTradingCalendar,
        ) -> Result<BTreeMap<String, f64>, String>,
    {
        run_wfo(
            manifest,
            data,
            self.walk_forward_config.clone(),
            instrument,
            quantity,
            risk_engine,
            calendar,
            &mut optimizer,
        )
    }

    /// Run Monte Carlo permutation test.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    pub fn run_monte_carlo(
        &self,
        manifest: &StrategyManifest,
        returns: &[f64],
        n_simulations: usize,
        instrument: honba_core::InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
    ) -> MonteCarloResult {
        let factory = self.ga_optimizer.factory_registry.clone();
        monte_carlo_permutation(
            manifest,
            returns,
            n_simulations,
            &factory,
            instrument,
            quantity,
            risk_engine,
            calendar,
        )
    }

    /// Run comprehensive stress test.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    pub fn run_stress_test(
        &self,
        manifest: &StrategyManifest,
        base_returns: &[f64],
        n_permutations: usize,
        shock_magnitudes: &[ShockMagnitude],
        gap_probability: f64,
        instrument: honba_core::InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
    ) -> StressTestResult {
        let factory = self.ga_optimizer.factory_registry.clone();
        stress_test(
            manifest,
            base_returns,
            n_permutations,
            shock_magnitudes,
            gap_probability,
            &factory,
            instrument,
            quantity,
            risk_engine,
            calendar,
        )
    }

    /// Run overfitting assessment.
    pub fn assess_overfitting(
        &self,
        sharpe: f64,
        returns: &[f64],
        is_fitness: &[f64],
        oos_fitness: &[f64],
        n_trials: usize,
        n_folds: usize,
    ) -> OverfittingAssessment {
        OverfittingAssessment::assess(sharpe, returns, is_fitness, oos_fitness, n_trials, n_folds)
    }

    /// Run AI refinement loop.
    pub fn run_ai_refinement(
        &self,
        strategy: Box<dyn Strategy>,
        held_out_data: &[Tick],
        config: AIRefinementConfig,
    ) -> AIRefinementResult {
        run_ai_refinement_loop(strategy, held_out_data, config)
    }
}
