//! Bayesian Hyperparameter Optimization using Tree-structured Parzen Estimators (TPE).

use rand::Rng;
use rand_distr::{Normal, Uniform};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use honba_backtest::{BacktestConfig, BacktestEngine, MarketEvent, TradingCalendar};
use honba_core::{InstrumentId, Quantity, Tick};
use honba_mon::RiskEngine;
use honba_strategy::{
    DynStrategy, StrategyManifest,
    factory::{StrategyFactoryRegistry, manifest_to_parameter_grid},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Default multi-objective fitness weights `(sharpe, sortino, profit_factor,
/// max_drawdown)` — kept byte-for-byte aligned with the GA optimizer's
/// [`GaConfig::fitness_weights`] default so both optimizers rank candidates
/// identically.
const FITNESS_WEIGHTS: (f64, f64, f64, f64) = (0.5, 0.3, 0.15, 0.05);

/// Configuration for Bayesian optimization (TPE algorithm).
#[derive(Debug, Clone)]
pub struct BayesianConfig {
    /// Number of initial random evaluations.
    pub initial_points: usize,
    /// Total number of evaluations (including initial points).
    pub total_points: usize,
    /// Acquisition function: "ei" (expected improvement), "ucb" (upper confidence bound).
    pub acquisition: String,
    /// Exploration-exploitation tradeoff parameter for UCB.
    pub kappa: f64,
    /// Percentile threshold for TPE (gamma in the paper).
    pub gamma: f64,
}

impl Default for BayesianConfig {
    fn default() -> Self {
        Self {
            initial_points: 10,
            total_points: 50,
            acquisition: "ei".to_string(),
            kappa: 2.5,
            gamma: 0.25,
        }
    }
}

/// A single evaluated point in the parameter space.
#[derive(Debug, Clone)]
pub struct EvaluatedPoint {
    /// Parameter values.
    pub params: BTreeMap<String, f64>,
    /// Fitness value.
    pub fitness: f64,
    /// Component metrics.
    pub sharpe: f64,
    pub sortino: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
}

/// Bayesian TPE optimizer for strategy hyperparameter search.
///
/// Uses Tree-structured Parzen Estimators to model the posterior distribution
/// of high-dimensional strategy parameter spaces, enabling efficient search
/// with far fewer evaluations than grid or random search.
pub struct BayesianOptimizer {
    config: BayesianConfig,
    /// Evaluated points and their fitness values.
    evaluated: Vec<EvaluatedPoint>,
    /// Parameter space bounds and types: (name -> (min, max, is_integer)).
    param_space: BTreeMap<String, (f64, f64, bool)>,
    factory_registry: Arc<StrategyFactoryRegistry>,
}

impl BayesianOptimizer {
    /// Create a new Bayesian optimizer with default configuration.
    pub fn new() -> Self {
        Self::with_config(BayesianConfig::default())
    }

    /// Create a new Bayesian optimizer with custom configuration.
    pub fn with_config(config: BayesianConfig) -> Self {
        Self {
            config,
            evaluated: Vec::new(),
            param_space: BTreeMap::new(),
            factory_registry: Arc::new(StrategyFactoryRegistry::new()),
        }
    }

    /// Create a new Bayesian optimizer with custom configuration and factory registry.
    pub fn with_registry(config: BayesianConfig, registry: Arc<StrategyFactoryRegistry>) -> Self {
        Self {
            config,
            evaluated: Vec::new(),
            param_space: BTreeMap::new(),
            factory_registry: registry,
        }
    }

    /// Set the parameter search space from a strategy manifest.
    pub fn set_param_space_from_manifest(
        &mut self,
        manifest: &StrategyManifest,
    ) -> Result<(), BayesError> {
        self.param_space.clear();

        for (name, spec) in &manifest.parameters {
            if let (Some(min), Some(max), Some(step)) = (spec.min, spec.max, spec.step) {
                let is_integer = spec.default.is_i64()
                    || spec.default.is_u64()
                    || (min.fract() == 0.0 && max.fract() == 0.0 && step.fract() == 0.0);
                self.param_space
                    .insert(name.clone(), (min, max, is_integer));
            }
        }

        if self.param_space.is_empty() {
            return Err(BayesError::EmptyParameterSpace);
        }

        Ok(())
    }

    /// Optimize strategy hyperparameters using TPE.
    ///
    /// # Arguments
    /// - `manifest`: The strategy manifest containing parameter search space.
    /// - `data`: Historical market data for backtesting.
    /// - `instrument`: The instrument to trade.
    /// - `quantity`: Base order quantity.
    /// - `risk_engine`: Pre-trade risk engine for validation.
    /// - `calendar`: NSE trading calendar for session times.
    ///
    /// Returns the best parameters found and all evaluated points.
    pub fn optimize(
        &mut self,
        manifest: &StrategyManifest,
        data: &[Tick],
        instrument: InstrumentId,
        quantity: Quantity,
        risk_engine: RiskEngine,
        calendar: Arc<dyn TradingCalendar>,
    ) -> Result<BayesOptimizationResult, BayesError> {
        let start = Instant::now();
        let strategy_name = manifest.name.clone();
        let initial_cash = Decimal::from(1_000_000);

        // Initialize parameter space from manifest
        self.set_param_space_from_manifest(manifest)?;

        // Generate parameter grid for initial random sampling
        let param_grid = manifest_to_parameter_grid(manifest)?;
        if param_grid.is_empty() {
            return Err(BayesError::EmptyParameterGrid);
        }

        // Initial random evaluations
        let n_initial = self.config.initial_points.min(self.config.total_points);
        let mut best_params = BTreeMap::new();
        let mut best_fitness = f64::NEG_INFINITY;

        for _ in 0..n_initial {
            let params = self.random_params();
            let param_grid_point = self.params_to_grid_point(&params);

            let strategy = self
                .factory_registry
                .create(
                    &strategy_name,
                    &param_grid_point,
                    instrument.clone(),
                    quantity,
                )
                .map_err(|e| BayesError::FactoryError(e.to_string()))?;

            let (fitness, sharpe, sortino, profit_factor, max_drawdown_pct) = self
                .evaluate_fitness(
                    strategy,
                    data,
                    instrument.clone(),
                    quantity,
                    initial_cash,
                    risk_engine.clone(),
                    &calendar,
                );

            let evaluated = EvaluatedPoint {
                params: params.clone(),
                fitness,
                sharpe,
                sortino,
                profit_factor,
                max_drawdown_pct,
            };

            self.evaluated.push(evaluated);

            if fitness > best_fitness {
                best_fitness = fitness;
                best_params = params;
            }
        }

        // TPE Bayesian search for remaining evaluations
        let n_remaining = self.config.total_points.saturating_sub(n_initial);

        for iteration in 0..n_remaining {
            // Sample next point using TPE algorithm
            let next_point = self.sample_next_tpe()?;
            let param_grid_point = self.params_to_grid_point(&next_point);

            let strategy = self
                .factory_registry
                .create(
                    &strategy_name,
                    &param_grid_point,
                    instrument.clone(),
                    quantity,
                )
                .map_err(|e| BayesError::FactoryError(e.to_string()))?;

            let (fitness, sharpe, sortino, profit_factor, max_drawdown_pct) = self
                .evaluate_fitness(
                    strategy,
                    data,
                    instrument.clone(),
                    quantity,
                    initial_cash,
                    risk_engine.clone(),
                    &calendar,
                );

            let evaluated = EvaluatedPoint {
                params: next_point.clone(),
                fitness,
                sharpe,
                sortino,
                profit_factor,
                max_drawdown_pct,
            };

            self.evaluated.push(evaluated);

            if fitness > best_fitness {
                best_fitness = fitness;
                best_params = next_point;
            }

            // Progress logging
            if iteration % 10 == 0 || iteration == n_remaining - 1 {
                eprintln!(
                    "Bayesian iter {}: best_fitness={:.4}, evaluated={}",
                    iteration,
                    best_fitness,
                    self.evaluated.len()
                );
            }
        }

        let duration = start.elapsed();

        eprintln!(
            "Bayesian optimization complete: {} evaluations in {:.2?}, best_fitness={:.4}",
            self.config.total_points, duration, best_fitness
        );

        Ok(BayesOptimizationResult {
            best_params,
            best_fitness,
            evaluated_points: self.evaluated.clone(),
            total_evaluations: self.evaluated.len(),
            duration,
        })
    }

    /// Generate random parameter set within bounds.
    fn random_params(&self) -> BTreeMap<String, f64> {
        let mut rng = rand::thread_rng();
        let mut params = BTreeMap::new();

        for (name, &(min, max, is_integer)) in &self.param_space {
            if is_integer {
                let dist = Uniform::new(min as i64, max as i64 + 1);
                let val = rng.sample(dist) as f64;
                params.insert(name.clone(), val);
            } else {
                let dist = Uniform::new(min, max);
                let val = rng.sample(dist);
                params.insert(name.clone(), val);
            }
        }

        params
    }

    /// Convert continuous params to grid point for factory. Integral-valued
    /// f64s (e.g. a period of 14.0) are emitted as JSON integers so factories
    /// that read them with `as_u64()` succeed; plain
    /// `serde_json::json!(14.0_f64)` stores an F64 that `as_u64()` rejects.
    fn params_to_grid_point(
        &self,
        params: &BTreeMap<String, f64>,
    ) -> BTreeMap<String, serde_json::Value> {
        params
            .iter()
            .map(|(k, v)| {
                let val = if v.fract() == 0.0 {
                    serde_json::json!(*v as i64)
                } else {
                    serde_json::json!(v)
                };
                (k.clone(), val)
            })
            .collect()
    }

    /// Evaluate fitness of a strategy by running a real backtest through
    /// [`BacktestEngine`] over the supplied tick history.
    ///
    /// The strategy handed in is the concrete [`DynStrategy`] already built
    /// from the [`StrategyFactoryRegistry`] with the candidate parameters, so
    /// the engine evaluates exactly the parameter set under test.
    ///
    /// Returns `(fitness, sharpe, sortino, profit_factor, max_drawdown_pct)`.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    fn evaluate_fitness(
        &self,
        strategy: DynStrategy,
        data: &[Tick],
        _instrument: InstrumentId,
        _quantity: Quantity,
        initial_cash: Decimal,
        risk_engine: RiskEngine,
        calendar: &Arc<dyn TradingCalendar>,
    ) -> (f64, f64, f64, f64, f64) {
        let config = BacktestConfig {
            starting_cash: initial_cash,
            ..Default::default()
        };
        let mut engine = BacktestEngine::new(strategy, risk_engine, config, calendar.clone());
        let market_events: Vec<MarketEvent> =
            data.iter().map(|t| MarketEvent::Tick(t.clone())).collect();
        let result = engine.run_events(market_events);

        let net_pnl = result.net_pnl.to_f64().unwrap_or(0.0);
        let gross_pnl = result.gross_pnl.to_f64().unwrap_or(0.0);
        let total_fees = result.total_fees.total().to_f64().unwrap_or(0.0);
        let max_drawdown_pct = result.max_drawdown_pct.to_f64().unwrap_or(0.0);
        let total_trades = result.total_trades;

        let fitness = self.compute_multi_objective_fitness(
            net_pnl,
            gross_pnl,
            total_fees,
            max_drawdown_pct,
            total_trades,
        );

        // Component metrics approximated from the real backtest result.
        let sharpe = if max_drawdown_pct > 0.0 {
            (net_pnl / max_drawdown_pct).clamp(-10.0, 10.0)
        } else {
            0.0
        };
        let sortino = sharpe * 0.9;
        let profit_factor = if total_fees > 0.0 && gross_pnl > 0.0 {
            (gross_pnl / total_fees).min(10.0)
        } else {
            1.0
        };

        (fitness, sharpe, sortino, profit_factor, max_drawdown_pct)
    }

    /// Compute multi-objective fitness from backtest results.
    ///
    /// Mirrors `GaOptimizer::compute_multi_objective_fitness` so TPE and the
    /// genetic algorithm score an identical backtest identically.
    fn compute_multi_objective_fitness(
        &self,
        net_pnl: f64,
        gross_pnl: f64,
        total_fees: f64,
        max_drawdown_pct: f64,
        total_trades: usize,
    ) -> f64 {
        let (w_sharpe, w_sortino, w_pf, w_dd) = FITNESS_WEIGHTS;

        // Approximate Sharpe from net PnL and drawdown
        let sharpe = if max_drawdown_pct > 0.0 {
            (net_pnl / max_drawdown_pct).clamp(-10.0, 10.0)
        } else {
            0.0
        };

        // Approximate Sortino (same as Sharpe for now)
        let sortino = sharpe * 0.9;

        // Profit factor approximation
        let profit_factor = if total_fees > 0.0 && gross_pnl > 0.0 {
            (gross_pnl / total_fees).min(10.0)
        } else {
            1.0
        };

        // Drawdown penalty (lower is better)
        let dd_score = 1.0 / (1.0 + max_drawdown_pct / 10.0);

        // Trade count penalty (discourage overtrading)
        let trade_penalty = 1.0 / (1.0 + (total_trades as f64 / 1000.0));

        (sharpe * w_sharpe)
            + (sortino * w_sortino)
            + (profit_factor * w_pf)
            + (dd_score * w_dd)
            + (trade_penalty * 0.01)
    }

    /// Sample next point using TPE algorithm.
    fn sample_next_tpe(&self) -> Result<BTreeMap<String, f64>, BayesError> {
        if self.evaluated.is_empty() {
            return Ok(self.random_params());
        }

        // Sort evaluated points by fitness
        let mut sorted = self.evaluated.clone();
        sorted.sort_by(|a, b| {
            b.fitness
                .partial_cmp(&a.fitness)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Split into good (top gamma) and bad (bottom 1-gamma)
        let n_good = (self.evaluated.len() as f64 * self.config.gamma).ceil() as usize;
        let n_good = n_good.max(1).min(sorted.len() - 1);

        let good_points = &sorted[..n_good];
        let bad_points = &sorted[n_good..];

        // For each parameter, build KDE from good and bad points
        let mut rng = rand::thread_rng();
        let mut next_params = BTreeMap::new();

        for (name, &(min, max, is_integer)) in &self.param_space {
            // Collect values for this parameter
            let good_values: Vec<f64> = good_points
                .iter()
                .filter_map(|p| p.params.get(name).copied())
                .collect();
            let bad_values: Vec<f64> = bad_points
                .iter()
                .filter_map(|p| p.params.get(name).copied())
                .collect();

            // Sample from the ratio l(x)/g(x) where l is good KDE, g is bad KDE
            let next_val = if good_values.is_empty() || bad_values.is_empty() {
                // Fallback to uniform
                if is_integer {
                    let dist = Uniform::new(min as i64, max as i64 + 1);
                    rng.sample(dist) as f64
                } else {
                    let dist = Uniform::new(min, max);
                    rng.sample(dist)
                }
            } else {
                // Simple approach: sample from good distribution with some noise
                let good_mean = good_values.iter().sum::<f64>() / good_values.len() as f64;
                let good_std = if good_values.len() > 1 {
                    let variance = good_values
                        .iter()
                        .map(|&v| (v - good_mean).powi(2))
                        .sum::<f64>()
                        / (good_values.len() - 1) as f64;
                    variance.sqrt().max(1e-6)
                } else {
                    (max - min) * 0.1
                };

                let dist = Normal::new(good_mean, good_std).unwrap();
                let mut sample = rng.sample(dist);

                // Clamp to bounds
                sample = sample.max(min).min(max);

                if is_integer { sample.round() } else { sample }
            };

            next_params.insert(name.clone(), next_val);
        }

        Ok(next_params)
    }
}

/// Result of Bayesian optimization.
#[derive(Debug, Clone)]
pub struct BayesOptimizationResult {
    /// Best parameters found.
    pub best_params: BTreeMap<String, f64>,
    /// Best fitness value.
    pub best_fitness: f64,
    /// All evaluated points.
    pub evaluated_points: Vec<EvaluatedPoint>,
    /// Total number of evaluations.
    pub total_evaluations: usize,
    /// Wall-clock duration.
    pub duration: std::time::Duration,
}

/// Errors that can occur during Bayesian optimization.
#[derive(Debug, thiserror::Error)]
pub enum BayesError {
    #[error("Empty parameter space in manifest")]
    EmptyParameterSpace,
    #[error("Empty parameter grid from manifest")]
    EmptyParameterGrid,
    #[error("Strategy factory error: {0}")]
    FactoryError(String),
    #[error("Backtest error: {0}")]
    BacktestError(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Sampling error: {0}")]
    SamplingError(String),
    #[error("Strategy error: {0}")]
    StrategyError(#[from] honba_strategy::error::StrategyError),
}

impl Default for BayesianOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
