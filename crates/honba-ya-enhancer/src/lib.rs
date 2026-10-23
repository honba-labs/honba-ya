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

pub use ai_refinement::refine_strategy_with_ai;
pub use bayesian_optimizer::BayesianOptimizer;
pub use ga_optimizer::GaOptimizer;
pub use monte_carlo_stress::{ShockMagnitude, monte_carlo_permutation, stress_test};
pub use odysseus::OdysseusEvaluator;
pub use overfitting_guardrails::{DSRCalculator, OverfittingAssessment, PBOCalculator};
pub use walk_forward::run_wfo;

/// Optimizer trait for strategy parameter search.
pub trait Optimizer: Send + Sync {
    /// Optimize strategy parameters given a fitness function.
    fn optimize<S: honba_actors::Strategy + Clone + Send + 'static>(
        &self,
        strategy: S,
        data: &[honba_core::Tick],
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
    pub best_params: std::collections::BTreeMap<String, String>,
    /// Number of evaluations performed.
    pub evaluations: usize,
    /// Whether the result converged.
    pub converged: bool,
}
