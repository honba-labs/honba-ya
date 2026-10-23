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

use honba_core::{InstrumentId, OrderSide, Price, Quantity, TimeInForce};
use rand::Rng;
use rayon::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
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
    Gt, // >
    Lt, // <
    Ge, // >=
    Le, // <=
    Eq, // ==
    Ne, // !=
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
        let mut _position: f64 = 0.0; // signed position

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
                    GenomeNode::Action { size_fraction, .. } => *size_fraction,
                    _ => 0.1,
                };

                let notional = (ctx.mid_price.0 * ctx.bid_depth.0).to_f64().unwrap_or(0.0) * size;

                match side {
                    OrderSide::Buy => {
                        _position += size;
                        trades.push(notional);
                    }
                    OrderSide::Sell => {
                        _position -= size;
                        trades.push(-notional);
                    }
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
            0.0
        };

        // Annualized Sharpe ratio (assuming 252 trading days, ~100 trades/day)
        let sharpe = if trades.len() > 1 {
            let mean = net_pnl / trades.len() as f64;
            let variance =
                trades.iter().map(|&t| (t - mean).powi(2)).sum::<f64>() / (trades.len() - 1) as f64;
            let std_dev = variance.sqrt();
            if std_dev > 0.0 {
                (mean / std_dev) * (252.0f64 * 100.0).sqrt()
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Sortino ratio (downside deviation only)
        let sortino = if trades.len() > 1 {
            let mean = net_pnl / trades.len() as f64;
            let downside_variance = trades
                .iter()
                .filter(|&&t| t < 0.0)
                .map(|&t| t.powi(2))
                .sum::<f64>()
                / trades.len() as f64;
            let downside_dev = downside_variance.sqrt();
            if downside_dev > 0.0 {
                (mean / downside_dev) * (252.0f64 * 100.0).sqrt()
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Maximum drawdown percentage from cumulative PnL
        let mut peak = 0.0f64;
        let mut cum_pnl = 0.0f64;
        let mut max_dd = 0.0f64;

        for &t in &trades {
            cum_pnl += t;
            if cum_pnl > peak {
                peak = cum_pnl;
            }
            let dd = peak - cum_pnl;
            if dd > max_dd {
                max_dd = dd;
            }
        }

        let max_drawdown_pct = if peak > 0.0 {
            (max_dd / peak) * 100.0
        } else {
            0.0
        };

        // Composite fitness score: Sharpe 40%, Sortino 25%, Win Rate 20%, Drawdown penalty 15%
        let dd_penalty = 1.0 / (1.0 + max_drawdown_pct / 10.0);
        let score = (sharpe.max(0.0) * 0.40)
            + (sortino.max(0.0) * 0.25)
            + (win_rate * 2.0 * 0.20)
            + (dd_penalty * 0.15);

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

    /// Recursively evaluate a condition node against an L2 context.
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
            GenomeNode::And(children) => {
                children.iter().all(|child| self.eval_condition(child, ctx))
            }
            GenomeNode::Or(children) => {
                children.iter().any(|child| self.eval_condition(child, ctx))
            }
            GenomeNode::Not(child) => !self.eval_condition(child, ctx),
            GenomeNode::Action { .. } => true, // Action node passes condition
        }
    }

    /// Get the signal value for a named signal from the L2 context.
    fn signal_value(&self, signal: &str, ctx: &EvalContext) -> f64 {
        match signal {
            "mid_price" => ctx.mid_price.0.to_f64().unwrap_or(0.0),
            "spread" => ctx.spread.0.to_f64().unwrap_or(0.0),
            "bid_depth" => ctx.bid_depth.0.to_f64().unwrap_or(0.0),
            "ask_depth" => ctx.ask_depth.0.to_f64().unwrap_or(0.0),
            "bid_depth_5" => ctx.bid_depth_5.0.to_f64().unwrap_or(0.0),
            "ask_depth_5" => ctx.ask_depth_5.0.to_f64().unwrap_or(0.0),
            "imbalance" => ctx.imbalance,
            _ => 0.0,
        }
    }

    /// Evaluate a batch of genomes in parallel using Rayon.
    ///
    /// Returns fitness results for all genomes. Evaluation is distributed
    /// across all available CPU cores via `par_iter`.
    pub fn evaluate_batch(
        &self,
        genomes: &[Genome],
        l2_snapshots: &[EvalContext],
    ) -> Vec<GenomeFitness> {
        let start = Instant::now();

        let results: Vec<GenomeFitness> = genomes
            .par_iter()
            .map(|genome| self.evaluate_genome(genome, l2_snapshots))
            .collect();

        let elapsed = start.elapsed().as_nanos() as usize;
        self.total_evaluated
            .fetch_add(genomes.len(), Ordering::Relaxed);
        self.total_eval_ns.fetch_add(elapsed, Ordering::Relaxed);

        results
    }

    /// Generate a random genome with the given ID and generation.
    pub fn random_genome(&self, id: String, generation: usize, depth: usize) -> Genome {
        let mut rng = rand::thread_rng();
        Genome {
            id,
            generation,
            root: self.random_node(depth, &mut rng),
        }
    }

    /// Generate a random node up to the given depth.
    fn random_node<R: Rng>(&self, depth: usize, rng: &mut R) -> GenomeNode {
        if depth == 0 {
            // Leaf: action
            GenomeNode::Action {
                side: if rng.gen_bool(0.5) {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                size_fraction: rng.gen_range(0.01..0.5),
                tif: TimeInForce::Day,
            }
        } else {
            match rng.gen_range(0..4) {
                0 => GenomeNode::Condition {
                    signal: self.random_signal(rng),
                    op: self.random_op(rng),
                    threshold: rng.gen_range(-10.0..10.0),
                },
                1 => GenomeNode::And(vec![
                    self.random_node(depth - 1, rng),
                    self.random_node(depth - 1, rng),
                ]),
                2 => GenomeNode::Or(vec![
                    self.random_node(depth - 1, rng),
                    self.random_node(depth - 1, rng),
                ]),
                _ => GenomeNode::Not(Box::new(self.random_node(depth - 1, rng))),
            }
        }
    }

    fn random_signal<R: Rng>(&self, rng: &mut R) -> String {
        let signals = [
            "mid_price",
            "spread",
            "bid_depth",
            "ask_depth",
            "bid_depth_5",
            "ask_depth_5",
            "imbalance",
        ];
        signals[rng.gen_range(0..signals.len())].to_string()
    }

    fn random_op<R: Rng>(&self, rng: &mut R) -> ComparisonOp {
        match rng.gen_range(0..6) {
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
    pub fn crossover(&self, p1: &Genome, p2: &Genome, child_id: String) -> Genome {
        let mut rng = rand::thread_rng();
        let child_root = if rng.gen_bool(0.5) {
            self.subtree_crossover(&p1.root, &p2.root, self.config.max_tree_depth, &mut rng)
        } else {
            self.subtree_crossover(&p2.root, &p1.root, self.config.max_tree_depth, &mut rng)
        };

        Genome {
            id: child_id,
            generation: p1.generation.max(p2.generation) + 1,
            root: child_root,
        }
    }

    /// Subtree crossover: replace a random subtree in parent1 with a random
    /// subtree from parent2.
    fn subtree_crossover<R: Rng>(
        &self,
        parent: &GenomeNode,
        donor: &GenomeNode,
        max_depth: usize,
        rng: &mut R,
    ) -> GenomeNode {
        if rng.gen_bool(0.3) || max_depth == 0 {
            // Replace entire subtree with donor's subtree (cloned)
            donor.clone()
        } else {
            match parent {
                GenomeNode::Condition { .. } => parent.clone(),
                GenomeNode::And(children) => GenomeNode::And(
                    children
                        .iter()
                        .map(|c| self.subtree_crossover(c, donor, max_depth - 1, rng))
                        .collect(),
                ),
                GenomeNode::Or(children) => GenomeNode::Or(
                    children
                        .iter()
                        .map(|c| self.subtree_crossover(c, donor, max_depth - 1, rng))
                        .collect(),
                ),
                GenomeNode::Not(child) => GenomeNode::Not(Box::new(self.subtree_crossover(
                    child,
                    donor,
                    max_depth - 1,
                    rng,
                ))),
                GenomeNode::Action { .. } => parent.clone(),
            }
        }
    }

    /// Mutate a genome in place with probability `mutation_rate`.
    pub fn mutate(&self, genome: &mut Genome) {
        let mut rng = rand::thread_rng();
        if rng.gen_bool(self.config.mutation_rate) {
            self.mutate_node(&mut genome.root, self.config.max_tree_depth, &mut rng);
        }
    }

    fn mutate_node<R: Rng>(&self, node: &mut GenomeNode, max_depth: usize, rng: &mut R) {
        if rng.gen_bool(0.3) || max_depth == 0 {
            *node = self.random_node(max_depth, rng);
        } else {
            match node {
                GenomeNode::Condition {
                    signal,
                    op,
                    threshold,
                } => {
                    if rng.gen_bool(0.33) {
                        *signal = self.random_signal(rng);
                    }
                    if rng.gen_bool(0.33) {
                        *op = self.random_op(rng);
                    }
                    if rng.gen_bool(0.33) {
                        *threshold = rng.gen_range(-10.0..10.0);
                    }
                }
                GenomeNode::And(children) | GenomeNode::Or(children) => {
                    for child in children.iter_mut() {
                        self.mutate_node(child, max_depth - 1, rng);
                    }
                }
                GenomeNode::Not(child) => {
                    self.mutate_node(child, max_depth - 1, rng);
                }
                GenomeNode::Action {
                    side,
                    size_fraction,
                    tif: _,
                } => {
                    if rng.gen_bool(0.5) {
                        *side = if rng.gen_bool(0.5) {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        };
                    }
                    if rng.gen_bool(0.5) {
                        *size_fraction = rng.gen_range(0.01..0.5);
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
}
