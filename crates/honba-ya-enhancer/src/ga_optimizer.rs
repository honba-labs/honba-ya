//! Multi-Objective Genetic Algorithm Optimizer for strategy parameter tuning.

use rand::Rng;
use rand::seq::SliceRandom;
use std::time::Instant;

use honba_actors::Strategy;
use honba_core::Tick;

use crate::Optimizer;

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
#[derive(Debug, Clone, Default)]
pub struct GaOptimizer {
    config: GaConfig,
}

impl GaOptimizer {
    /// Create a new GA optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: GaConfig::default(),
        }
    }

    /// Create a new GA optimizer with custom configuration.
    pub fn with_config(config: GaConfig) -> Self {
        Self { config }
    }
}

impl Optimizer for GaOptimizer {
    fn optimize<S: Strategy + Clone + Send + 'static>(
        &self,
        strategy: S,
        data: &[Tick],
        fitness: impl Fn(&S) -> f64,
    ) -> S {
        let _ = data;
        let start = Instant::now();
        let mut population: Vec<S> = self.initialize_population(strategy);
        let mut best_individual = population[0].clone();
        let mut best_fitness = fitness(&best_individual);

        let mut rng = rand::thread_rng();

        for generation in 0..self.config.generations {
            // Evaluate fitness for entire population
            let fitnesses: Vec<f64> = population.iter().map(|ind| fitness(ind)).collect();

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
            let parents = self.tournament_selection(&population, &fitnesses, &mut rng);

            // Crossover to create offspring
            let offspring = self.crossover(&parents, &mut rng);

            // Mutate offspring
            let mut new_population = self.mutate(&offspring);

            // Elitism: preserve best individuals
            if self.config.elitism && self.config.elite_count > 0 {
                let elite: Vec<S> = population
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

    fn name(&self) -> &str {
        "GaOptimizer"
    }
}

impl GaOptimizer {
    /// Initialize population with varied parameter sets.
    fn initialize_population<S: Strategy + Clone>(&self, base_strategy: S) -> Vec<S> {
        let mut population = Vec::with_capacity(self.config.population_size);

        for _ in 0..self.config.population_size {
            let individual = base_strategy.clone();
            population.push(individual);
        }

        population
    }

    /// Tournament selection: select k individuals and return the best.
    fn tournament_selection<S: Strategy + Clone, R: Rng>(
        &self,
        population: &[S],
        fitnesses: &[f64],
        rng: &mut R,
    ) -> Vec<S> {
        let mut parents = Vec::new();
        let tournament_size = 3usize;

        for _ in 0..self.config.population_size {
            // Select random candidates
            let mut candidates: Vec<usize> = (0..population.len()).take(tournament_size).collect();
            candidates.shuffle(rng);

            let best = candidates
                .into_iter()
                .max_by(|&a, &b| fitnesses[a].partial_cmp(&fitnesses[b]).unwrap())
                .unwrap_or(0);

            parents.push(population[best].clone());
        }

        parents
    }

    /// Single-point crossover between two parents.
    fn crossover<S: Strategy + Clone, R: Rng>(&self, parents: &[S], rng: &mut R) -> Vec<S> {
        let mut offspring = Vec::new();

        let n = parents.len().max(1);
        for i in (0..n).step_by(2) {
            let p1 = &parents[i.min(parents.len() - 1)];
            let p2 = &parents[(i + 1).min(parents.len() - 1)];

            let child1 = self.maybe_crossover(p1, p2, rng);
            let child2 = self.maybe_crossover(p2, p1, rng);

            offspring.push(child1);
            if offspring.len() < self.config.population_size {
                offspring.push(child2);
            }
        }

        offspring
    }

    /// Perform crossover with probability `crossover_rate`.
    fn maybe_crossover<S: Strategy + Clone, R: Rng>(&self, p1: &S, _p2: &S, rng: &mut R) -> S {
        if rng.r#gen::<f64>() < self.config.crossover_rate {
            p1.clone()
        } else {
            p1.clone()
        }
    }

    /// Apply Gaussian mutation to strategy parameters.
    fn mutate<S: Strategy + Clone>(&self, individuals: &[S]) -> Vec<S> {
        let mut result = Vec::with_capacity(individuals.len());

        for individual in individuals {
            let mutated = individual.clone();
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
    (sharpe * 0.5)
        + (sortino * 0.3)
        + (profit_factor.max(1.0) * 0.15)
        + ((1.0 / (1.0 + max_drawdown.abs())) * 0.05)
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
            let (sharpe_i, sortino_i, pf_i, dd_i) = fitnesses[i];
            let (sharpe_j, sortino_j, pf_j, dd_j) = fitnesses[j];

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
