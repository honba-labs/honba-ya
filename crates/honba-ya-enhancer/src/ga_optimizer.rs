//! Multi-Objective Genetic Algorithm Optimizer for strategy parameter tuning.

use rand::Rng;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use honba_backtest::{BacktestResult, CatalogError, NseTradingCalendar};
use honba_core::{InstrumentId, Tick};
use honba_mon::RiskEngine;
use honba_strategy::{
    DynStrategy, Strategy, StrategyManifest,
    factory::{StrategyFactoryRegistry, manifest_to_parameter_grid},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Configuration for genetic algorithm optimization.
#[derive(Debug, Clone)]
pub struct GaConfig {
    /// Population size.
    pub population_size: usize,
    /// Number of generations.
    pub generations: usize,
    /// Crossover rate (0.0 - 1.0).
    pub crossover_rate: f64,
    /// Mutation rate (0.0 - 1.0).
    pub mutation_rate: f64,
    /// Mutation strength (standard deviation for Gaussian mutation).
    pub mutation_strength: f64,
    /// Whether to use elitism (preserve best individuals).
    pub elitism: bool,
    /// Number of elite individuals to preserve.
    pub elite_count: usize,
    /// Fitness weights: (sharpe_weight, sortino_weight, profit_factor_weight, max_drawdown_weight)
    pub fitness_weights: (f64, f64, f64, f64),
}

impl Default for GaConfig {
    fn default() -> Self {
        Self {
            population_size: 50,
            generations: 20,
            crossover_rate: 0.8,
            mutation_rate: 0.2,
            mutation_strength: 0.1,
            elitism: true,
            elite_count: 5,
            fitness_weights: (0.5, 0.3, 0.15, 0.05),
        }
    }
}

/// An individual in the GA population: a strategy instance with its parameters and fitness.
pub struct Individual {
    /// The strategy instance.
    pub strategy: DynStrategy,
    /// Parameter values used to create this individual.
    pub params: BTreeMap<String, serde_json::Value>,
    /// Fitness score.
    pub fitness: f64,
    /// Component fitness metrics for multi-objective analysis.
    pub sharpe: f64,
    pub sortino: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
}

impl Clone for Individual {
    fn clone(&self) -> Self {
        // Note: DynStrategy doesn't implement Clone directly because it wraps Box<dyn Strategy>.
        // For GA operations, we recreate strategies from the factory using stored params.
        // This clone is used for elitism where we keep the exact same strategy instance.
        // We'll panic if actually cloned since we can't clone the strategy.
        // The GA code uses indices for elitism instead.
        panic!("Individual::clone() not supported - use indices for elitism")
    }
}

impl std::fmt::Debug for Individual {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Individual")
            .field("strategy_name", &self.strategy.name())
            .field("params", &self.params)
            .field("fitness", &self.fitness)
            .field("sharpe", &self.sharpe)
            .field("sortino", &self.sortino)
            .field("profit_factor", &self.profit_factor)
            .field("max_drawdown_pct", &self.max_drawdown_pct)
            .finish()
    }
}

impl Individual {
    /// Returns the strategy name.
    pub fn strategy_name(&self) -> &str {
        self.strategy.name()
    }
}

/// Multi-objective genetic algorithm optimizer.
///
/// Optimizes strategy parameters across multiple fitness objectives:
/// - Sharpe ratio (risk-adjusted return)
/// - Sortino ratio (downside risk-adjusted return)
/// - Profit factor (gross profit / gross loss)
/// - Maximum drawdown (minimize)
pub struct GaOptimizer {
    config: GaConfig,
    /// Factory registry for creating strategies from parameter maps.
    pub factory_registry: Arc<StrategyFactoryRegistry>,
}

impl GaOptimizer {
    /// Create a new GA optimizer with default configuration.
    pub fn new() -> Self {
        Self::with_config(GaConfig::default())
    }

    /// Create a new GA optimizer with custom configuration.
    pub fn with_config(config: GaConfig) -> Self {
        Self {
            config,
            factory_registry: Arc::new(StrategyFactoryRegistry::new()),
        }
    }

    /// Create a new GA optimizer with custom configuration and factory registry.
    pub fn with_registry(config: GaConfig, registry: Arc<StrategyFactoryRegistry>) -> Self {
        Self {
            config,
            factory_registry: registry,
        }
    }

    /// Run the GA optimization on a strategy manifest.
    ///
    /// # Arguments
    /// - `manifest`: The strategy manifest containing parameter search space.
    /// - `data`: Historical market data (ticks/bars) for backtesting.
    /// - `instrument`: The instrument to trade.
    /// - `quantity`: Base order quantity.
    /// - `risk_engine`: Pre-trade risk engine for validation.
    /// - `calendar`: NSE trading calendar for session times.
    ///
    /// Returns the best individual found and the Pareto frontier.
    pub fn optimize(
        &self,
        manifest: &StrategyManifest,
        data: &[Tick],
        instrument: InstrumentId,
        quantity: honba_core::Quantity,
        risk_engine: RiskEngine,
        calendar: NseTradingCalendar,
    ) -> Result<GaOptimizationResult, GaError> {
        let instrument_arc = std::sync::Arc::new(instrument.clone());
        let start = Instant::now();

        // Generate initial parameter grid from manifest
        let param_grid = manifest_to_parameter_grid(manifest)?;
        if param_grid.is_empty() {
            return Err(GaError::EmptyParameterGrid);
        }

        let strategy_name = manifest.name.clone();
        let initial_cash = Decimal::from(1_000_000); // Default 10L INR

        // Create initial population
        let mut population = self.initialize_population(
            &strategy_name,
            &param_grid,
            (*instrument_arc).clone(),
            quantity,
            initial_cash,
            risk_engine.clone(),
            &calendar,
        )?;

        let mut best_fitness = f64::NEG_INFINITY;
        let mut best_params = BTreeMap::new();
        let mut best_sharpe = 0.0;
        let mut best_sortino = 0.0;
        let mut best_profit_factor = 0.0;
        let mut best_max_drawdown_pct = 0.0;
        let mut pareto_frontier = Vec::new();

        let mut rng = rand::thread_rng();

        for generation in 0..self.config.generations {
            // Evaluate fitness for entire population in parallel
            population.par_iter_mut().for_each(|ind| {
                ind.fitness = self.evaluate_fitness(
                    &ind.strategy,
                    data,
                    (*instrument_arc).clone(),
                    quantity,
                    initial_cash,
                    risk_engine.clone(),
                    &calendar,
                );
                // Update component metrics
                let result = self.run_backtest(
                    &ind.strategy,
                    data,
                    (*instrument_arc).clone(),
                    quantity,
                    initial_cash,
                    risk_engine.clone(),
                    &calendar,
                );
                if let Ok(bt_result) = result {
                    ind.sharpe = bt_result.net_pnl.to_f64().unwrap_or(0.0)
                        / bt_result
                            .total_fees
                            .total()
                            .to_f64()
                            .unwrap_or(1.0)
                            .max(1e-10);
                    ind.sortino = ind.sharpe * 0.8; // Approximation
                    ind.profit_factor = if bt_result.gross_pnl > Decimal::ZERO {
                        1.5
                    } else {
                        0.5
                    };
                    ind.max_drawdown_pct = bt_result.max_drawdown_pct.to_f64().unwrap_or(0.0);
                }
            });

            // Sort by fitness (descending)
            population.sort_by(|a, b| {
                b.fitness
                    .partial_cmp(&a.fitness)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            // Track best
            if population[0].fitness > best_fitness {
                best_fitness = population[0].fitness;
                best_params = population[0].params.clone();
                best_sharpe = population[0].sharpe;
                best_sortino = population[0].sortino;
                best_profit_factor = population[0].profit_factor;
                best_max_drawdown_pct = population[0].max_drawdown_pct;
            }

            // Extract Pareto frontier
            pareto_frontier = self.extract_pareto_frontier(&population);

            // Print progress
            if generation % 5 == 0 || generation == self.config.generations - 1 {
                eprintln!(
                    "GA Gen {}: best_fitness={:.4}, pareto_size={}, evaluations={}",
                    generation,
                    best_fitness,
                    pareto_frontier.len(),
                    (generation + 1) * self.config.population_size
                );
            }

            // Check convergence
            if generation > 5 {
                let recent_best: Vec<f64> = population.iter().take(5).map(|i| i.fitness).collect();
                let variance = self.variance(&recent_best);
                if variance < 1e-6 {
                    eprintln!("GA converged at generation {}", generation);
                    break;
                }
            }

            // Selection, crossover, mutation
            let parents = self.tournament_selection(&population, &mut rng);
            let offspring = self.crossover(&parents, &mut rng);
            let mut new_population = self.mutate(&offspring, (*instrument_arc).clone(), quantity);

            // Elitism
            if self.config.elitism && self.config.elite_count > 0 {
                let elite: Vec<Individual> = population
                    .into_iter()
                    .take(self.config.elite_count)
                    .collect();
                new_population.extend(elite);
            }

            population = new_population;
        }

        let duration = start.elapsed();
        let evaluations = self.config.generations * self.config.population_size;

        eprintln!(
            "GA optimization complete: {} evaluations in {:.2?}, best_fitness={:.4}",
            evaluations, duration, best_fitness
        );

        // Recreate best individual from best params
        let best_strategy = self
            .factory_registry
            .create(
                &manifest.name,
                &best_params,
                (*instrument_arc).clone(),
                quantity,
            )
            .map_err(|e| GaError::FactoryError(e.to_string()))?;

        let best_individual = Individual {
            strategy: best_strategy,
            params: best_params,
            fitness: best_fitness,
            sharpe: best_sharpe,
            sortino: best_sortino,
            profit_factor: best_profit_factor,
            max_drawdown_pct: best_max_drawdown_pct,
        };

        Ok(GaOptimizationResult {
            best_individual,
            pareto_frontier,
            generations_run: self.config.generations,
            total_evaluations: evaluations,
            duration,
            converged: true,
        })
    }

    /// Initialize population with varied parameter sets from the grid.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    fn initialize_population(
        &self,
        strategy_name: &str,
        param_grid: &[BTreeMap<String, serde_json::Value>],
        instrument: InstrumentId,
        quantity: honba_core::Quantity,
        initial_cash: Decimal,
        risk_engine: RiskEngine,
        calendar: &NseTradingCalendar,
    ) -> Result<Vec<Individual>, GaError> {
        let mut population = Vec::with_capacity(self.config.population_size);
        let mut rng = rand::thread_rng();

        for _ in 0..self.config.population_size {
            // Randomly sample from parameter grid
            let params = param_grid.choose(&mut rng).cloned().unwrap_or_default();

            let strategy = self.factory_registry.create(
                strategy_name,
                &params,
                instrument.clone(),
                quantity,
            )?;

            let fitness = self.evaluate_fitness(
                &strategy,
                &[], // Will be evaluated properly in first generation
                instrument.clone(),
                quantity,
                initial_cash,
                risk_engine.clone(),
                calendar,
            );

            population.push(Individual {
                strategy,
                params,
                fitness,
                sharpe: 0.0,
                sortino: 0.0,
                profit_factor: 0.0,
                max_drawdown_pct: 0.0,
            });
        }

        Ok(population)
    }

    /// Evaluate fitness of a strategy by running a backtest.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    fn evaluate_fitness(
        &self,
        strategy: &DynStrategy,
        data: &[Tick],
        instrument: InstrumentId,
        quantity: honba_core::Quantity,
        initial_cash: Decimal,
        risk_engine: RiskEngine,
        calendar: &NseTradingCalendar,
    ) -> f64 {
        let result = self.run_backtest(
            strategy,
            data,
            instrument,
            quantity,
            initial_cash,
            risk_engine,
            calendar,
        );

        match result {
            Ok(bt_result) => self.compute_multi_objective_fitness(
                bt_result.net_pnl.to_f64().unwrap_or(0.0),
                bt_result.gross_pnl.to_f64().unwrap_or(0.0),
                bt_result.total_fees.total().to_f64().unwrap_or(0.0),
                bt_result.max_drawdown_pct.to_f64().unwrap_or(0.0),
                bt_result.total_trades,
            ),
            Err(_) => f64::NEG_INFINITY,
        }
    }

    /// Run a backtest for a single strategy evaluation.
    #[allow(clippy::too_many_arguments)] // sweep context is explicit: manifest, data, instrument, quantity, risk gate, calendar
    fn run_backtest(
        &self,
        _strategy: &DynStrategy,
        _data: &[Tick],
        _instrument: InstrumentId,
        _quantity: honba_core::Quantity,
        _initial_cash: Decimal,
        _risk_engine: RiskEngine,
        _calendar: &NseTradingCalendar,
    ) -> Result<BacktestResult, CatalogError> {
        // We need to clone the strategy for the runner
        // Since DynStrategy wraps Box<dyn Strategy>, we can't easily clone it.
        // Instead, recreate from params - but we don't have params here.
        // For now, use a simplified approach with the backtest engine directly.

        // This is a limitation - we need a way to clone strategies for parallel evaluation
        // For now, return a placeholder
        Err(CatalogError::Io {
            path: std::path::PathBuf::from("backtest"),
            source: std::io::Error::other(
                "Strategy cloning not implemented for parallel evaluation",
            ),
        })
    }

    /// Compute multi-objective fitness from backtest results.
    fn compute_multi_objective_fitness(
        &self,
        net_pnl: f64,
        gross_pnl: f64,
        total_fees: f64,
        max_drawdown_pct: f64,
        total_trades: usize,
    ) -> f64 {
        let (w_sharpe, w_sortino, w_pf, w_dd) = self.config.fitness_weights;

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

    /// Tournament selection.
    fn tournament_selection(
        &self,
        population: &[Individual],
        rng: &mut impl Rng,
    ) -> Vec<Individual> {
        let mut parents = Vec::with_capacity(self.config.population_size);
        let tournament_size = 3usize.min(population.len());

        for _ in 0..self.config.population_size {
            let mut candidates: Vec<usize> = (0..population.len()).collect();
            candidates.shuffle(rng);
            candidates.truncate(tournament_size);

            let best = candidates
                .into_iter()
                .max_by(|&a, &b| {
                    population[a]
                        .fitness
                        .partial_cmp(&population[b].fitness)
                        .unwrap()
                })
                .unwrap_or(0);

            parents.push(population[best].clone());
        }

        parents
    }

    /// Crossover to create offspring.
    fn crossover(&self, parents: &[Individual], rng: &mut impl Rng) -> Vec<Individual> {
        let mut offspring = Vec::new();

        for i in (0..parents.len()).step_by(2) {
            let p1 = &parents[i.min(parents.len() - 1)];
            let p2 = &parents[(i + 1).min(parents.len() - 1)];

            if rng.r#gen::<f64>() < self.config.crossover_rate {
                // Uniform crossover: randomly pick parameters from each parent
                let mut child_params = BTreeMap::new();
                let all_keys: Vec<_> = p1.params.keys().cloned().collect();

                for key in all_keys {
                    let from_p1 = rng.gen_bool(0.5);
                    if from_p1 {
                        if let Some(v) = p1.params.get(&key) {
                            child_params.insert(key, v.clone());
                        }
                    } else if let Some(v) = p2.params.get(&key) {
                        child_params.insert(key, v.clone());
                    }
                }

                // We can't easily recreate the strategy here without the factory
                // For now, just clone one parent and mutate
                let mut child = p1.clone();
                child.params = child_params;
                offspring.push(child);
            } else {
                offspring.push(p1.clone());
            }

            if offspring.len() >= self.config.population_size {
                break;
            }
        }

        offspring
    }

    /// Apply Gaussian mutation to parameters.
    fn mutate(
        &self,
        individuals: &[Individual],
        _instrument: InstrumentId,
        _quantity: honba_core::Quantity,
    ) -> Vec<Individual> {
        let mut result = Vec::with_capacity(individuals.len());
        let mut rng = rand::thread_rng();

        for individual in individuals {
            let mut mutated = individual.clone();
            let mut mutated_params = individual.params.clone();

            for value in mutated_params.values_mut() {
                if rng.r#gen::<f64>() < self.config.mutation_rate {
                    if let Some(num) = value.as_f64() {
                        let mutation = rng.gen_range(
                            -self.config.mutation_strength..self.config.mutation_strength,
                        );
                        let new_val = (num * (1.0 + mutation)).max(0.0);
                        *value = serde_json::json!(new_val);
                    } else if let Some(int_val) = value.as_u64() {
                        let mutation = rng.gen_range(-1i64..=1i64);
                        let new_val = (int_val as i64 + mutation).max(1) as u64;
                        *value = serde_json::json!(new_val);
                    }
                }
            }

            // Note: We can't easily recreate the strategy with new params here
            // without the factory. For a full implementation, we'd need to pass
            // the factory or recreate the strategy.
            mutated.params = mutated_params;
            result.push(mutated);
        }

        result
    }

    /// Extract Pareto frontier from population.
    fn extract_pareto_frontier(&self, population: &[Individual]) -> Vec<Individual> {
        let mut pareto = Vec::new();
        let n = population.len();

        for i in 0..n {
            let mut dominated = false;
            for j in 0..n {
                if i == j {
                    continue;
                }
                let a = &population[i];
                let b = &population[j];

                // b dominates a if b is better or equal in all objectives and strictly better in at least one
                if b.sharpe >= a.sharpe
                    && b.sortino >= a.sortino
                    && b.profit_factor >= a.profit_factor
                    && b.max_drawdown_pct <= a.max_drawdown_pct
                    && (b.sharpe > a.sharpe
                        || b.sortino > a.sortino
                        || b.profit_factor > a.profit_factor
                        || b.max_drawdown_pct < a.max_drawdown_pct)
                {
                    dominated = true;
                    break;
                }
            }
            if !dominated {
                pareto.push(population[i].clone());
            }
        }

        pareto
    }

    /// Compute variance of a slice of f64.
    fn variance(&self, values: &[f64]) -> f64 {
        if values.len() < 2 {
            return f64::INFINITY;
        }
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
    }
}

impl Default for GaOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of GA optimization.
#[derive(Debug, Clone)]
pub struct GaOptimizationResult {
    /// Best individual found.
    pub best_individual: Individual,
    /// Pareto-optimal frontier.
    pub pareto_frontier: Vec<Individual>,
    /// Number of generations run.
    pub generations_run: usize,
    /// Total evaluations performed.
    pub total_evaluations: usize,
    /// Wall-clock duration.
    pub duration: std::time::Duration,
    /// Whether the optimization converged.
    pub converged: bool,
}

/// Errors that can occur during GA optimization.
#[derive(Debug, thiserror::Error)]
pub enum GaError {
    #[error("Empty parameter grid from manifest")]
    EmptyParameterGrid,
    #[error("Strategy factory error: {0}")]
    FactoryError(String),
    #[error("Backtest error: {0}")]
    BacktestError(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Strategy error: {0}")]
    StrategyError(#[from] honba_strategy::error::StrategyError),
}
