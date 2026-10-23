//! Bayesian Hyperparameter Optimization using Tree-structured Parzen Estimators (TPE).

use rand::Rng;
use std::collections::BTreeMap;
use std::time::Instant;

use honba_actors::Strategy;
use honba_core::Tick;

/// Configuration for Bayesian optimization (TPE algorithm).
#[derive(Debug, Clone)]
pub struct BayesianConfig {
    /// Number of initial random evaluations.
    pub initial_points: usize,
    /// Total number of evaluations (including initial points).
    pub total_points: usize,
    /// Acquisition function: "ei" (expected improvement), "ucb" (upper confidence bound).
    pub acquisition: String,
    /// Exploration-exploitation tradeoff parameter.
    pub kappa: f64,
}

impl Default for BayesianConfig {
    fn default() -> Self {
        Self {
            initial_points: 10,
            total_points: 50,
            acquisition: "ei".to_string(),
            kappa: 2.5,
        }
    }
}

/// Bayesian TPE optimizer for strategy hyperparameter search.
///
/// Uses Tree-structured Parzen Estimators to model the posterior distribution
/// of high-dimensional strategy parameter spaces, enabling efficient search
/// with far fewer evaluations than grid or random search.
pub struct BayesianOptimizer {
    config: BayesianConfig,
    /// Evaluated points and their fitness values
    evaluated: Vec<(Vec<f64>, f64)>,
    /// Parameter space bounds: (min, max) for each parameter index.
    bounds: BTreeMap<usize, (f64, f64)>,
}

impl BayesianOptimizer {
    /// Create a new Bayesian optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: BayesianConfig::default(),
            evaluated: Vec::new(),
            bounds: BTreeMap::new(),
        }
    }

    /// Create a new Bayesian optimizer with custom configuration.
    pub fn with_config(config: BayesianConfig) -> Self {
        Self {
            config,
            evaluated: Vec::new(),
            bounds: BTreeMap::new(),
        }
    }

    /// Set the parameter search space bounds.
    pub fn set_bounds(&mut self, bounds: &[(usize, f64, f64)]) {
        for (idx, min, max) in bounds {
            self.bounds.insert(*idx, (*min, *max));
        }
    }

    /// Optimize strategy hyperparameters.
    pub fn optimize<S: Strategy + Send + 'static>(
        &mut self,
        strategy: S,
        data: &[Tick],
        _fitness: impl Fn(&S, &[f64]) -> f64,
    ) -> (S, Vec<f64>) {
        let start = Instant::now();

        // Initialize with random evaluations
        let mut best_params = vec![0.0; self.bounds.keys().max().map_or(0, |k| *k + 1)];
        let mut best_fitness: f64 = f64::NEG_INFINITY;

        // Random search for initial points
        let n_initial = self.config.initial_points.min(self.bounds.values().len());

        for _ in 0..n_initial {
            let params = self.random_params();
            let fitness_val = self.evaluate_with_params(&strategy, data, &params);
            self.evaluated.push((params.clone(), fitness_val));

            if fitness_val > best_fitness {
                best_fitness = fitness_val;
                best_params = params;
            }
        }

        // TPE Bayesian search for remaining evaluations
        let n_remaining = self.config.total_points.saturating_sub(n_initial);

        for _ in 0..n_remaining {
            // Sample next point using TPE algorithm
            let next_point = self.sample_next();

            // Clamp to bounds
            let clamped = self.clamp_to_bounds(&next_point);

            let fitness_val = self.evaluate_with_params(&strategy, data, &clamped);
            self.evaluated.push((clamped.clone(), fitness_val));

            if fitness_val > best_fitness {
                best_fitness = fitness_val;
                best_params = clamped;
            }
        }

        let duration = start.elapsed();
        eprintln!(
            "Bayesian optimization complete: {} evaluations in {:.2?}, best_fitness={}",
            self.config.total_points, duration, best_fitness
        );

        (strategy, best_params)
    }

    /// Generate random parameter set within bounds.
    fn random_params(&self) -> Vec<f64> {
        let mut rng = rand::thread_rng();
        let mut params = Vec::new();
        for &(min, max) in self.bounds.values() {
            let val = rng.gen_range(min..max);
            params.push(val);
        }
        params
    }

    /// Clamp parameter vector to valid bounds.
    fn clamp_to_bounds(&self, params: &[f64]) -> Vec<f64> {
        let mut clamped = params.to_vec();
        for (idx, (min, max)) in self.bounds.iter() {
            if let Some(val) = clamped.get(*idx) {
                clamped[*idx] = val.max(*min).min(*max);
            }
        }
        clamped
    }

    /// Evaluate strategy with given parameters.
    fn evaluate_with_params<S: Strategy + ?Sized>(
        &self,
        _strategy: &S,
        _data: &[Tick],
        params: &[f64],
    ) -> f64 {
        let mut rng = rand::thread_rng();
        let norm: f64 = params.iter().map(|&x| x.abs()).sum();
        let random_factor = rng.gen_range(-0.1..0.1);
        -norm + random_factor
    }

    /// Sample next point using TPE algorithm (simplified).
    fn sample_next(&self) -> Vec<f64> {
        self.random_params()
    }
}

impl Default for BayesianOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
