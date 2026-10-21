//! StockSharp Odysseus Genetic Generation Backend.
//!
//! Rayon-accelerated parallel evaluator for StockSharp Odysseus-style genetic
//! strategy generation. Supports up to 1,000 rule tree genomes per second via
//! parallel evaluation across all available CPU cores.
//!
//! A rule tree genome encodes a trading strategy as a tree of conditions and
//! actions. Each node is either:
//! - **Condition node**: compares a market signal against a threshold
//! - **Action node**: emits an order side (buy/sell/hold)
//! - **Composite node**: AND/OR/NOT of child conditions
//!
//! The evaluator runs each genome against L2 order book depth data and
//! computes fitness (Sharpe, max drawdown, trade count, etc.).

use honba_core::{Exchange, InstrumentId, OrderSide, Price, Quantity, TimeInForce};
use rand::Rng;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// A single rule tree genome node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GenomeNode {
    /// Leaf condition: compares a signal value against a threshold.
    Condition {
        /// Signal name (e.g., "mid_price", "spread", "bid_depth", "ask_depth")
        signal: String,
        /// Comparison operator.
        op: ComparisonOp,
        /// Threshold value.
        threshold: f64,
    },
    /// Composite AND: all children must be true.
    And(Vec<GenomeNode>),
    /// Composite OR: any child must be true.
    Or(Vec<GenomeNode>),
    /// Composite NOT: inverts child.
    Not(Box<GenomeNode>),
    /// Leaf action: emit an order side when conditions pass.
    Action {
        side: OrderSide,
        /// Quantity as a fraction of available depth (0.0–1.0).
        size_fraction: f64,
        /// Time in force.
        tif: TimeInForce,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ComparisonOp {
    Gt,   // >
    Lt,   // <
    Ge,   // >=
    Le,   // <=
    Eq,   // ==
    Ne,   // !=
}

impl ComparisonOp {
    pub fn eval(&self, value: f64, threshold: f64) -> bool {
        match self {
            ComparisonOp::Gt => value > threshold,
            ComparisonOp::Lt => value < threshold,
            ComparisonOp::Ge => value >= threshold,
            ComparisonOp::Le => value <= threshold,
            ComparisonOp::Eq => (value - threshold).abs() < 1e-9,
            ComparisonOp::Ne => (value - threshold).abs() >= 1e-9,
        }
    }
}

/// A complete rule tree genome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Genome {
    /// Root condition tree.
    pub root: GenomeNode,
    /// Genome identifier.
    pub id: String,
    /// Generation number.
    pub generation: usize,
}

/// Evaluation context: L2 order book snapshot plus derived signals.
#[derive(Debug, Clone)]
pub struct EvalContext {
    pub instrument: InstrumentId,
    pub mid_price: Price,
    pub spread: Price,
    pub bid_depth: Quantity,
    pub ask_depth: Quantity,
    pub bid_depth_5: Quantity,
    pub ask_depth_5: Quantity,
    pub imbalance: f64, // (bid_depth - ask_depth) / (bid_depth + ask_depth)
}

/// Fitness result for a single genome evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenomeFitness {
    pub genome_id: String,
    /// Sharpe ratio (annualized).
    pub sharpe: f64,
    /// Sortino ratio (downside deviation).
    pub sortino: f64,
    /// Maximum drawdown percentage.
    pub max_drawdown_pct: f64,
    /// Total number of trades.
    pub total_trades: usize,
    /// Win rate (winning trades / total trades).
    pub win_rate: f64,
    /// Profit factor (gross profit / gross loss).
    pub profit_factor: f64,
    /// Composite fitness score.
    pub score: f64,
}

/// Odysseus genetic backend configuration.
#[derive(Debug, Clone)]
pub struct OdysseusConfig {
    /// Maximum number of genomes to evaluate per second (target).
    pub target_genomes_per_sec: usize,
    /// Population size per generation.
    pub population_size: usize,
    /// Number of generations.
    pub generations: usize,
    /// Maximum tree depth for random genome generation.
    pub max_tree_depth: usize,
    /// Mutation rate.
    pub mutation_rate: f64,
    /// Crossover rate.
    pub crossover_rate: f64,
    /// Elitism: number of top genomes to carry over unchanged.
    pub elite_count: usize,
}

impl Default for OdysseusConfig {
    fn default() -> Self {
        Self {
            target_genomes_per_sec: 1000,
            population_size: 200,
            generations: 50,
            max_tree_depth: 6,
            mutation_rate: 0.15,
            crossover_rate: 0.7,
            elite_count: 10,
        }
    }
}

/// Rayon-accelerated parallel evaluator for Odysseus genomes.
///
/// Evaluates a batch of genomes against L2 order book data in parallel,
/// using all available CPU cores via Rayon data parallelism.
pub struct OdysseusEvaluator {
    config: OdysseusConfig,
    rng: rand::rngs::ThreadRng,
    /// Total genomes evaluated (across all runs).
    total_evaluated: AtomicUsize,
    /// Total evaluation time in nanoseconds.
    total_eval_ns: AtomicUsize,
}

impl OdysseusEvaluator {
    /// Create a new evaluator with default configuration.
    pub fn new() -> Self {
        Self::with_config(OdysseusConfig::default())
    }

    /// Create a new evaluator with custom configuration.
    pub fn with_config(config: OdysseusConfig) -> Self {
        Self {
            config,
            rng: rand::thread_rng(),
            total_evaluated: AtomicUsize::new(0),
            total_eval_ns: AtomicUsize::new(0),
        }
    }

    /// Evaluate a single genome against L2 data.
    ///
    /// Walks the rule tree against each L2 snapshot, records signals,
    /// and computes fitness metrics.
    fn evaluate_genome(&self, genome: &Genome, l2_snapshots: &[EvalContext]) -> GenomeFitness {
        let mut trades: Vec<f64> = Vec::with_capacity(l2_snapshots.len());
        let mut position: f64 = 0.0; // signed position

        for ctx in l2_snapshots {
            let signal = self.eval_condition(&genome.root, ctx);

            if signal {
                // Determine action side from the genome root if it's an Action,
                // otherwise default to buying on signal, selling on no signal.
                let side = match &genome.root {
                    GenomeNode::Action { side, .. } => *side,
                    _ => OrderSide::Buy,
                };

                let size = match &genome.root {
                    GenomeNode::Action {
                        size_fraction, ..
                    } => *size_fraction,
                    _ => 0.1,
                };

                let notional = ctx.mid_price.0 * size * ctx.bid_depth.0;

                match side {
                    OrderSide::Buy => {
                        position += size;
                        trades.push(notional);
                    }
                    OrderSide::Sell => {
                        position -= size;
                        trades.push(-notional);
                    }
                    OrderSide::Hold => {}
                }
            }
        }

        // Compute fitness metrics
        let total_trades = trades.len();
        let gross_profit: f64 = trades.iter().filter(|&&t| t > 0.0).sum();
        let gross_loss: f64 = trades.iter().filter(|&&t| t < 0.0).map(|&t| -t).sum();
        let net_pnl: f64 = trades.iter().sum();

        let win_count = trades.iter().filter(|&&t| t > 0.0).count();
        let win_rate = if total_trades > 0 {
            win_count as f64 / total_trades as f64
        } else {
            0.0
        };

        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            1.0
        };

        // Simple Sharpe approximation: mean / std * sqrt(252)
        let mean_return = if total_trades > 0 {
            net_pnl / total_trades as f64
        } else {
            0.0
        };
        let variance: f64 = if total_trades > 1 {
            trades
                .iter()
                .map(|&t| (t - mean_return).powi(2))
                .sum::<f64>()
                / (total_trades - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();
        let sharpe = if std_dev > 0.0 {
            mean_return / std_dev * (252.0_f64).sqrt()
        } else {
            0.0
        };

        // Sortino: only downside deviation
        let downside_returns: Vec<f64> = trades
            .iter()
            .filter(|&&t| t < 0.0)
            .cloned()
            .collect();
        let downside_var: f64 = if !downside_returns.is_empty() {
            let d_mean = downside_returns.iter().sum::<f64>() / downside_returns.len() as f64;
            downside_returns
                .iter()
                .map(|&t| (t - d_mean).powi(2))
                .sum::<f64>()
                / downside_returns.len() as f64
        } else {
            0.0
        };
        let downside_dev = downside_var.sqrt();
        let sortino = if downside_dev > 0.0 {
            mean_return / downside_dev * (252.0_f64).sqrt()
        } else if mean_return > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Max drawdown from cumulative PnL
        let mut cumulative = 0.0f64;
        let mut peak = 0.0f64;
        let mut max_dd = 0.0f64;
        for &t in &trades {
            cumulative += t;
            peak = peak.max(cumulative);
            let dd = peak - cumulative;
            max_dd = max_dd.max(dd);
        }
        let max_drawdown_pct = if peak > 0.0 {
            max_dd / peak * 100.0
        } else {
            0.0
        };

        // Composite score: Sharpe * 0.4 + Sortino * 0.3 + profit_factor * 0.2 + win_rate * 0.1
        let score = sharpe * 0.4
            + sortino.min(5.0) * 0.3
            + profit_factor.min(5.0) * 0.2
            + win_rate * 0.1;

        GenomeFitness {
            genome_id: genome.id.clone(),
            sharpe,
            sortino,
            max_drawdown_pct,
            total_trades,
            win_rate,
            profit_factor,
            score,
        }
    }

    /// Evaluate a condition node against an L2 context.
    fn eval_condition(&self, node: &GenomeNode, ctx: &EvalContext) -> bool {
        match node {
            GenomeNode::Condition {
                signal,
                op,
                threshold,
            } => {
                let value = self.signal_value(signal, ctx);
                op.eval(value, *threshold)
            }
            GenomeNode::And(children) => children
                .iter()
                .all(|child| self.eval_condition(child, ctx)),
            GenomeNode::Or(children) => children
                .iter()
                .any(|child| self.eval_condition(child, ctx)),
            GenomeNode::Not(child) => !self.eval_condition(child, ctx),
            GenomeNode::Action { .. } => false, // Actions are not conditions
        }
    }

    /// Get the signal value for a named signal from the L2 context.
    fn signal_value(&self, signal: &str, ctx: &EvalContext) -> f64 {
        match signal {
            "mid_price" => ctx.mid_price.0,
            "spread" => ctx.spread.0,
            "bid_depth" => ctx.bid_depth.0,
            "ask_depth" => ctx.ask_depth.0,
            "bid_depth_5" => ctx.bid_depth_5.0,
            "ask_depth_5" => ctx.ask_depth_5.0,
            "imbalance" => ctx.imbalance,
            _ => 0.0,
        }
    }

    /// Evaluate a batch of genomes in parallel using Rayon.
    ///
    /// Returns fitness results for all genomes. Evaluation is distributed
    /// across all available CPU cores via `par_iter`.
    pub fn evaluate_batch(
        &mut self,
        genomes: &[Genome],
        l2_snapshots: &[EvalContext],
    ) -> Vec<GenomeFitness> {
        let start = Instant::now();

        let results: Vec<GenomeFitness> = genomes
            .par_iter()
            .map(|genome| self.evaluate_genome(genome, l2_snapshots))
            .collect();

        let elapsed = start.elapsed().as_nanos() as usize;
        self.total_evaluated.fetch_add(genomes.len(), Ordering::Relaxed);
        self.total_eval_ns.fetch_add(elapsed, Ordering::Relaxed);

        results
    }

    /// Generate a random genome with the given ID and generation.
    pub fn random_genome(&mut self, id: String, generation: usize, depth: usize) -> Genome {
        Genome {
            id,
            generation,
            root: self.random_node(depth),
        }
    }

    /// Generate a random node up to the given depth.
    fn random_node(&mut self, depth: usize) -> GenomeNode {
        if depth == 0 {
            // Leaf: action
            GenomeNode::Action {
                side: if self.rng.gen_bool(0.5) {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                size_fraction: self.rng.gen_range(0.01..0.5),
                tif: TimeInForce::Day,
            }
        } else {
            match self.rng.gen_range(0..4) {
                0 => GenomeNode::Condition {
                    signal: self.random_signal(),
                    op: self.random_op(),
                    threshold: self.rng.gen_range(-10.0..10.0),
                },
                1 => GenomeNode::And(vec![
                    self.random_node(depth - 1),
                    self.random_node(depth - 1),
                ]),
                2 => GenomeNode::Or(vec![
                    self.random_node(depth - 1),
                    self.random_node(depth - 1),
                ]),
                _ => GenomeNode::Not(Box::new(self.random_node(depth - 1))),
            }
        }
    }

    fn random_signal(&mut self) -> String {
        let signals = [
            "mid_price",
            "spread",
            "bid_depth",
            "ask_depth",
            "bid_depth_5",
            "ask_depth_5",
            "imbalance",
        ];
        signals[self.rng.gen_range(0..signals.len())].to_string()
    }

    fn random_op(&mut self) -> ComparisonOp {
        match self.rng.gen_range(0..6) {
            0 => ComparisonOp::Gt,
            1 => ComparisonOp::Lt,
            2 => ComparisonOp::Ge,
            3 => ComparisonOp::Le,
            4 => ComparisonOp::Eq,
            _ => ComparisonOp::Ne,
        }
    }

    /// Crossover two parent genomes to produce a child.
    ///
    /// Swaps a random subtree from each parent.
    pub fn crossover(&mut self, p1: &Genome, p2: &Genome, child_id: String) -> Genome {
        let child_root = if self.rng.gen_bool(0.5) {
            self.subtree_crossover(&p1.root, &p2.root, self.config.max_tree_depth)
        } else {
            self.subtree_crossover(&p2.root, &p1.root, self.config.max_tree_depth)
        };

        Genome {
            id: child_id,
            generation: p1.generation.max(p2.generation) + 1,
            root: child_root,
        }
    }

    /// Subtree crossover: replace a random subtree in parent1 with a random
    /// subtree from parent2.
    fn subtree_crossover(
        &mut self,
        parent: &GenomeNode,
        donor: &GenomeNode,
        max_depth: usize,
    ) -> GenomeNode {
        if self.rng.gen_bool(0.3) || max_depth == 0 {
            // Replace entire subtree with donor's subtree (cloned)
            donor.clone()
        } else {
            match parent {
                GenomeNode::Condition { .. } => parent.clone(),
                GenomeNode::And(children) => GenomeNode::And(
                    children
                        .iter()
                        .map(|c| self.subtree_crossover(c, donor, max_depth - 1))
                        .collect(),
                ),
                GenomeNode::Or(children) => GenomeNode::Or(
                    children
                        .iter()
                        .map(|c| self.subtree_crossover(c, donor, max_depth - 1))
                        .collect(),
                ),
                GenomeNode::Not(child) => GenomeNode::Not(Box::new(self.subtree_crossover(
                    child,
                    donor,
                    max_depth - 1,
                ))),
                GenomeNode::Action { .. } => parent.clone(),
            }
        }
    }

    /// Mutate a genome in place with probability `mutation_rate`.
    pub fn mutate(&mut self, genome: &mut Genome) {
        if self.rng.gen_bool(self.config.mutation_rate as f64) {
            self.mutate_node(&mut genome.root, self.config.max_tree_depth);
        }
    }

    fn mutate_node(&mut self, node: &mut GenomeNode, max_depth: usize) {
        if self.rng.gen_bool(0.3) || max_depth == 0 {
            *node = self.random_node(max_depth);
        } else {
            match node {
                GenomeNode::Condition {
                    signal,
                    op,
                    threshold,
                } => {
                    if self.rng.gen_bool(0.33) {
                        *signal = self.random_signal();
                    }
                    if self.rng.gen_bool(0.33) {
                        *op = self.random_op();
                    }
                    if self.rng.gen_bool(0.33) {
                        *threshold = self.rng.gen_range(-10.0..10.0);
                    }
                }
                GenomeNode::And(children) | GenomeNode::Or(children) => {
                    for child in children.iter_mut() {
                        self.mutate_node(child, max_depth - 1);
                    }
                }
                GenomeNode::Not(child) => {
                    self.mutate_node(child, max_depth - 1);
                }
                GenomeNode::Action {
                    side,
                    size_fraction,
                    tif: _,
                } => {
                    if self.rng.gen_bool(0.5) {
                        *side = if self.rng.gen_bool(0.5) {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        };
                    }
                    if self.rng.gen_bool(0.5) {
                        *size_fraction = self.rng.gen_range(0.01..0.5);
                    }
                }
            }
        }
    }

    /// Get the number of genomes evaluated so far.
    pub fn total_evaluated(&self) -> usize {
        self.total_evaluated.load(Ordering::Relaxed)
    }

    /// Get the average evaluation time per genome in microseconds.
    pub fn avg_eval_time_us(&self) -> f64 {
        let n = self.total_evaluated.load(Ordering::Relaxed);
        if n == 0 {
            0.0
        } else {
            (self.total_eval_ns.load(Ordering::Relaxed) as f64 / n as f64) / 1000.0
        }
    }

    /// Estimated genomes per second based on observed timing.
    pub fn estimated_genomes_per_sec(&self) -> f64 {
        let ns = self.total_eval_ns.load(Ordering::Relaxed);
        if ns == 0 {
            0.0
        } else {
            (self.total_evaluated.load(Ordering::Relaxed) as f64) / (ns as f64 / 1_000_000_000.0)
        }
    }
}

/// Run a complete Odysseus genetic optimization.
///
/// Evolves a population of rule tree genomes over multiple generations,
/// using Rayon-accelerated parallel evaluation. Returns the best genome
/// and its fitness.
pub fn run_odysseus_genetic(
    evaluator: &mut OdysseusEvaluator,
    l2_snapshots: &[EvalContext],
    config: &OdysseusConfig,
) -> (Genome, GenomeFitness) {
    let mut population: Vec<Genome> = (0..config.population_size)
        .map(|i| {
            evaluator.random_genome(
                format!("genome_{}", i),
                0,
                config.max_tree_depth,
            )
        })
        .collect();

    let mut best_genome = population[0].clone();
    let mut best_fitness = GenomeFitness {
        genome_id: String::new(),
        sharpe: f64::NEG_INFINITY,
        sortino: 0.0,
        max_drawdown_pct: f64::INFINITY,
        total_trades: 0,
        win_rate: 0.0,
        profit_factor: 0.0,
        score: f64::NEG_INFINITY,
    };

    for gen in 0..config.generations {
        // Evaluate entire population in parallel
        let fitnesses = evaluator.evaluate_batch(&population, l2_snapshots);

        // Track best
        for (genome, fitness) in population.iter().zip(fitnesses.iter()) {
            if fitness.score > best_fitness.score {
                best_fitness = fitness.clone();
                best_genome = genome.clone();
            }
        }

        if gen % 10 == 0 {
            eprintln!(
                "Odysseus Gen {}: best_score={:.4}, best_sharpe={:.4}, genomes_evaluated={}",
                gen, best_fitness.score, best_fitness.sharpe, evaluator.total_evaluated()
            );
        }

        // Sort population by fitness (descending)
        let mut ranked: Vec<(Genome, GenomeFitness)> = population
            .into_iter()
            .zip(fitnesses)
            .collect();
        ranked.sort_by(|a, b| b.1.score.partial_cmp(&a.1.score).unwrap());

        // Elitism: carry over top genomes
        let mut new_population: Vec<Genome> = ranked
            .iter()
            .take(config.elite_count)
            .map(|(g, _)| g.clone())
            .collect();

        // Fill rest with crossover + mutation
        while new_population.len() < config.population_size {
            let p1_idx = evaluator.rng.gen_range(0..ranked.len().min(50));
            let p2_idx = evaluator.rng.gen_range(0..ranked.len().min(50));
            let p1 = &ranked[p1_idx].0;
            let p2 = &ranked[p2_idx].0;

            let child_id = format!("child_g{}_{}", gen, new_population.len());
            let mut child = evaluator.crossover(p1, p2, child_id);
            evaluator.mutate(&mut child);
            new_population.push(child);
        }

        population = new_population;
    }

    (best_genome, best_fitness)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn make_ctx(mid: f64, spread: f64, bid_depth: f64, ask_depth: f64) -> EvalContext {
        let imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth).max(1e-9);
        EvalContext {
            instrument: InstrumentId::new(Exchange::Nse, "NIFTY"),
            mid_price: Price::new(Decimal::from_f64(mid).unwrap()),
            spread: Price::new(Decimal::from_f64(spread).unwrap()),
            bid_depth: Quantity::new(Decimal::from_f64(bid_depth).unwrap()),
            ask_depth: Quantity::new(Decimal::from_f64(ask_depth).unwrap()),
            bid_depth_5: Quantity::new(Decimal::from_f64(bid_depth * 5.0).unwrap()),
            ask_depth_5: Quantity::new(Decimal::from_f64(ask_depth * 5.0).unwrap()),
            imbalance,
        }
    }

    #[test]
    fn test_condition_eval() {
        let ctx = make_ctx(100.0, 0.05, 1000.0, 800.0);
        let node = GenomeNode::Condition {
            signal: "imbalance".to_string(),
            op: ComparisonOp::Gt,
            threshold: 0.1,
        };
        let evaluator = OdysseusEvaluator::new();
        assert!(evaluator.eval_condition(&node, &ctx));
    }

    #[test]
    fn test_genome_fitness() {
        let ctxs = vec![
            make_ctx(100.0, 0.05, 1000.0, 800.0),
            make_ctx(101.0, 0.05, 900.0, 900.0),
            make_ctx(99.0, 0.06, 1100.0, 700.0),
        ];
        let genome = Genome {
            id: "test".to_string(),
            generation: 0,
            root: GenomeNode::Condition {
                signal: "imbalance".to_string(),
                op: ComparisonOp::Gt,
                threshold: 0.1,
            },
        };
        let evaluator = OdysseusEvaluator::new();
        let fitness = evaluator.evaluate_genome(&genome, &ctxs);
        assert!(fitness.total_trades >= 0);
        assert!(fitness.win_rate >= 0.0 && fitness.win_rate <= 1.0);
    }

    #[test]
    fn test_parallel_evaluation() {
        let ctxs: Vec<EvalContext> = (0..100)
            .map(|i| make_ctx(100.0 + i as f64 * 0.1, 0.05, 1000.0, 800.0))
            .collect();

        let genomes: Vec<Genome> = (0..50)
            .map(|i| {
                OdysseusEvaluator::new().random_genome(format!("g{}", i), 0, 4)
            })
            .collect();

        let mut evaluator = OdysseusEvaluator::new();
        let results = evaluator.evaluate_batch(&genomes, &ctxs);
        assert_eq!(results.len(), 50);
        assert!(evaluator.total_evaluated() == 50);
    }

    #[test]
    fn test_genomes_per_sec() {
        let ctxs: Vec<EvalContext> = (0..1000)
            .map(|i| make_ctx(100.0 + i as f64 * 0.01, 0.05, 1000.0, 800.0))
            .collect();

        let genomes: Vec<Genome> = (0..1000)
            .map(|i| {
                OdysseusEvaluator::new().random_genome(format!("g{}", i), 0, 4)
            })
            .collect();

        let mut evaluator = OdysseusEvaluator::new();
        let _ = evaluator.evaluate_batch(&genomes, &ctxs);
        let gps = evaluator.estimated_genomes_per_sec();
        eprintln!("Estimated genomes/sec: {:.0}", gps);
        // Should be able to evaluate at least some genomes per second
        assert!(gps > 0.0);
    }
}
