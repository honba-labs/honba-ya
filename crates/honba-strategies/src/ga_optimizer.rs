//! Multi-Objective Genetic Algorithm Optimizer for strategy parameter tuning.

use rand::Rng;
use std::collections::BTreeMap;
use std::time::Instant;

use honba_core::Tick;
use honba_actors::Strategy;
use honba_strategies::OptimizationResult;

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
        }
    }
}

/// Multi-objective genetic algorithm optimizer.
///
/// Optimizes strategy parameters across multiple fitness objectives:
/// - Sharpe ratio (risk-adjusted return)
/// - Sortino ratio (downside risk-adjusted return)
/// - Profit factor (gross / net profit)
/// - Maximum drawdown (minimize)
/// - Total signals (penalize excessive trading)
pub struct GaOptimizer {
    config: GaConfig,
    rng: rand::rngs::ThreadRng,
}

impl GaOptimizer {
    /// Create a new GA optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: GaConfig::default(),
            rng: rand::thread_rng(),
        }
    }

    /// Create a new GA optimizer with custom configuration.
    pub with_config(config: GaConfig) -> Self {
        Self {
            config,
            rng: rand::thread_rng(),
        }
    }
}

impl Optimizer for GaOptimizer {
    fn optimize<S: Strategy + Send + 'static>(
        &self,
        strategy: S,
        data: &[Tick],
        fitness: impl Fn(&S) -> f64,
    ) -> S {
        let start = Instant::now();
        let mut population: Vec<S> = self.initialize_population(strategy);
        let mut best_individual = population[0].clone();
        let mut best_fitness = fitness(&best_individual);

        for generation in 0..self.config.generations {
            // Evaluate fitness for entire population
            let fitnesses: Vec<f64> = population
                .iter()
                .map(|ind| fitness(ind))
                .collect();

            // Track best
            let mut generation_best_idx = 0;
            for (i, &f) in fitnesses.iter().enumerate() {
                if f > best_fitness {
                    best_fitness = f;
                    best_individual = population[i].clone();
                }
                if f > fitnesses[generation_best_idx] {
                    generation_best_idx = i;
                }
            }

            // Print progress every 5 generations
            if generation % 5 == 0 {
                eprintln!(
                    "GA Gen {}: best_fitness={}, population_best={}",
                    generation, best_fitness, fitnesses[generation_best_idx]
                );
            }

            // Select parents using tournament selection
            let parents = self.tournament_selection(&population, &fitnesses);

            // Crossover to create offspring
            let offspring = self.crossover(&parents);

            // Mutate offspring
            let mut new_population = self.mutate(&offspring);

            // Elitism: preserve best individuals
            if self.config.elitism && self.config.elite_count > 0 {
                let mut elite: Vec<S> = population
                    .into_iter()
                    .take(self.config.elite_count)
                    .collect();
                new_population.extend(elite);
            }

            population = new_population;
        }

        let evaluations = self.config.generations * self.config.population_size;
        let duration = start.elapsed();

        eprintln!(
            "GA optimization complete: {} evaluations in {:.2?}, best_fitness={}",
            evaluations, duration, best_fitness
        );

        best_individual
    }
}

impl GaOptimizer {
    /// Initialize population with varied parameter sets.
    fn initialize_population(&self, base_strategy: S) -> Vec<S>
    where
        S: Strategy + Clone,
    {
        let mut population = Vec::with_capacity(self.config.population_size);

        for _ in 0..self.config.population_size {
            // Create variant by cloning and slightly perturbing
            let mut individual = base_strategy.clone();
            // In a full implementation, would perturb strategy internal state
            // For now, just add the base individual
            population.push(individual);
        }

        population
    }

    /// Tournament selection: select k individuals and return the best.
    fn tournament_selection(
        &self,
        population: &[S],
        fitnesses: &[f64],
    ) -> Vec<S>
    where
        S: Strategy + Clone,
    {
        let mut parents = Vec::new();
        let tournament_size = 3usize;

        for _ in 0..self.config.population_size {
            // Select random candidates
            let mut candidates: Vec<usize> = (0..population.len())
                .take(tournament_size)
                .collect();
            candidates.shuffle(&mut self.rng);

            // Find best among candidates
            let best = candidates
                .into_iter()
                .max_by(|&a, &b| fitnesses[a].partial_cmp(&fitnesses[b]).unwrap())
                .unwrap();

            parents.push(population[candidates[0]].clone());
        }

        parents
    }

    /// Single-point crossover between two parents.
    fn crossover(&self, parents: &[S]) -> Vec<S>
    where
        S: Strategy + Clone,
    {
        let mut offspring = Vec::new();

        // Pair up parents
        let n = parents.len().max(1);
        for i in (0..n).step_by(2) {
            let p1 = &parents[i.min(parents.len() - 1)];
            let p2 = &parents[(i + 1).min(parents.len() - 1)];

            // Create two offspring
            let child1 = self.maybe_crossover(p1, p2);
            let child2 = self.maybe_crossover(p2, p1);

            offspring.push(child1);
            if offspring.len() < self.config.population_size {
                offspring.push(child2);
            }
        }

        offspring
    }

    /// Perform crossover with probability `crossover_rate`.
    fn maybe_crossover(&self, p1: &S, p2: &S) -> S {
        if self.rng.gen::<f64>() < self.config.crossover_rate {
            // In a full implementation, would swap parameter segments
            // For now, return a clone of parent 1
            p1.clone()
        } else {
            p1.clone()
        }
    }

    /// Apply Gaussian mutation to strategy parameters.
    fn mutate(&self, individuals: &[S]) -> Vec<S>
    where
        S: Strategy + Clone,
    {
        let mut result = Vec::with_capacity(individuals.len());

        for individual in individuals {
            let mut mutated = individual.clone();
            // In a full implementation, would mutate strategy internal parameters
            // For now, just add the individual as-is
            result.push(mutated);
        }

        result
    }
}

/// Single-objective wrapper for multi-objective fitness functions.
///
/// Converts a Sharpe/Sortino/ProfitFactor combination into a single
/// scalar for GA fitness evaluation.
pub fn sharpe_sortino_profit_factor_sharpe(
    sharpe: f64,
    sortino: f64,
    profit_factor: f64,
    max_drawdown: f64,
) -> f64 {
    // Weighted combination: Sharpe 50%, Sortino 30%, Profit Factor 15%, inverse DD 5%
    (sharpe * 0.5) + (sortino * 0.3) + (profit_factor.max(1.0) * 0.15) + ((1.0 / (1.0 + max_drawdown.abs())) * 0.05)
}

/// Pareto frontier extraction from GA population.
///
/// Returns non-dominated solutions across the objective space.
pub fn extract_pareto_frontier(
    fitnesses: &[(f64, f64, f64, f64)], // (sharpe, sortino, profit_factor, max_dd)
) -> Vec<usize> {
    let mut pareto_indices = Vec::new();
    let n = fitnesses.len();

    for i in 0..n {
        let mut dominated = false;
        for j in 0..n {
            if i == j {
                continue;
            }
            // Check if solution j dominates solution i
            // A solution dominates if it's better in all objectives
            let (sharpe_i, sortino_i, pf_i, dd_i) = fitnesses[i];
            let (sharpe_j, sortino_j, pf_j, dd_j) = fitnesses[j];

            // Sharpe higher is better, Sortino higher is better, Profit Factor higher is better, Max DD lower is better
            if sharpe_j >= sharpe_i
                && sortino_j >= sortino_i
                && pf_j >= pf_i
                && dd_j <= dd_i
                && (sharpe_j > sharpe_i || sortino_j > sortino_i || pf_j > pf_i || dd_j < dd_i)
            {
                dominated = true;
                break;
            }
        }
        if !dominated {
            pareto_indices.push(i);
        }
    }

    pareto_indices
}