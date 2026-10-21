//! Automatic Strategy Enhancer for Honba.
//!
//! Provides multi-objective optimization, Bayesian hyperparameter search,
//! walk-forward analysis, overfitting guardrails, Monte Carlo stress testing,
//! closed-loop AI refinement integration, and StockSharp Odysseus genetic
//! generation backend. Integrates with the honba-ya actor runtime and
//! backtester for closed-loop AI-driven strategy refinement.

pub mod ga_optimizer;
pub mod bayesian_optimizer;
pub mod walk_forward;
pub mod overfitting_guardrails;
pub mod monte_carlo_stress;
pub mod ai_refinement;
pub mod odysseus;

/// Optimizer trait for strategy parameter search.
pub trait Optimizer: Send + Sync {
    /// Optimize strategy parameters given a fitness function.
    fn optimize<S: honba_actors::Strategy + Send + 'static>(
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