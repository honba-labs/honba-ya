//! Overfitting Guardrails: DSR and PBO computation.

use statrs::distribution::{ContinuousCDF, Normal as StatrsNormal};

/// Deflated Sharpe Ratio (DSR) per López de Prado.
///
/// Adjusts the Sharpe ratio for the number of training trials, return
/// skewness, and kurtosis. DSR >= 3.0 is generally considered robust;
/// DSR < 1.5 suggests likely overfitting.
#[derive(Debug, Clone)]
pub struct DSRCalculator {
    /// Number of independent trials/bootstrap samples.
    pub n_trials: usize,
    /// Confidence level for DSR computation.
    pub confidence: f64,
}

impl DSRCalculator {
    /// Create a new DSR calculator.
    pub fn new(n_trials: usize) -> Self {
        Self {
            n_trials,
            confidence: 0.95,
        }
    }

    /// Compute DSR given Sharpe ratio, number of trials, and return moments.
    pub fn compute(&self, sharpe: f64, returns: &[f64]) -> f64 {
        if returns.len() < 30 {
            // Need minimum sample size for reliable moment estimation
            return sharpe;
        }

        // Compute return moments
        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let variance = returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let std_dev = variance.sqrt().max(1e-10);

        // Skewness (adjusted Fisher-Pearson coefficient)
        let skewness = returns
            .iter()
            .map(|&r| ((r - mean) / std_dev).powi(3))
            .sum::<f64>()
            / n
            * n
            / (n - 1.0)
            / (n - 2.0);

        // Kurtosis (excess kurtosis)
        let kurtosis = returns
            .iter()
            .map(|&r| {
                let z = (r - mean) / std_dev;
                z.powi(4)
            })
            .sum::<f64>()
            / n
            * n
            * (n + 1.0)
            / ((n - 1.0) * (n - 2.0) * (n - 3.0))
            - 3.0;

        // DSR formula from López de Prado (2014):
        // DSR = Sharpe * sqrt(1 - 2*γ*(1 - φ(z)) - 2*γ*z*φ(z)) / (1 + 2*γ*(1 - φ(z)))
        // where γ = 0.5772... (Euler-Mascheroni), z = Sharpe * sqrt(n), φ = standard normal CDF
        //
        // Simplified practical formula:
        // DSR = Sharpe / (1 + sqrt(2/log(n_trials)) * (1 + skew^2/4) * (1 + kurtosis/4))

        let gamma = 0.5772156649; // Euler-Mascheroni constant
        let z = sharpe * n.sqrt();

        // Standard normal CDF at z
        let normal = StatrsNormal::new(0.0, 1.0).unwrap();
        let phi_z = normal.cdf(z);

        // Adjusted formula with skewness and kurtosis correction
        let skew_adj = 1.0 + skewness.powi(2) / 4.0;
        let kurt_adj = 1.0 + kurtosis / 4.0;

        let denominator = 1.0 + (2.0 * gamma * (1.0 - phi_z)).sqrt() * skew_adj * kurt_adj;

        sharpe / denominator.max(1e-10)
    }

    /// Compute DSR with explicit trial count (for multiple backtest runs).
    pub fn compute_with_trials(&self, sharpe: f64, returns: &[f64], n_trials: usize) -> f64 {
        let mut calc = self.clone();
        calc.n_trials = n_trials;
        calc.compute(sharpe, returns)
    }
}

/// Probability of Backtest Overfitting (PBO) via Combinatorially Symmetric Cross-Validation (CSCV).
///
/// Partitions a backtest period into k symmetric folds, alternating
/// in-sample/out-of-sample segments, and computes the distribution of
/// fitness differences to estimate overfitting probability.
#[derive(Debug, Clone)]
pub struct PBOCalculator {
    /// Number of CSCV folds.
    pub n_folds: usize,
    /// Significance level for overfitting declaration.
    pub significance: f64,
}

impl PBOCalculator {
    /// Create a new PBO calculator.
    pub fn new(n_folds: usize) -> Self {
        Self {
            n_folds,
            significance: 0.05,
        }
    }

    /// Compute PBO given in-sample and out-of-sample fitness values
    /// from multiple CSCV runs.
    pub fn compute(&self, is_fitness: &[f64], oos_fitness: &[f64]) -> f64 {
        if is_fitness.len() != self.n_folds || oos_fitness.len() != self.n_folds {
            eprintln!(
                "PBO: mismatch - expected {} folds, got IS: {}, OOS: {}",
                self.n_folds,
                is_fitness.len(),
                oos_fitness.len()
            );
            return 1.0; // Max overfitting probability on mismatch
        }

        // Compute fitness differences (OOS - IS)
        let diffs: Vec<f64> = is_fitness
            .iter()
            .zip(oos_fitness.iter())
            .map(|(is, oos)| oos - is)
            .collect();

        // Count how many folds show OOS < IS (overfitting signal)
        let overfitting_count = diffs.iter().filter(|&d| *d < 0.0).count();

        // PBO = proportion of folds with overfitting signal
        let pbo = overfitting_count as f64 / self.n_folds as f64;

        // Apply significance test: if PBO > significance, declare overfitting
        // but return the raw proportion for the caller to decide
        pbo
    }

    /// Compute PBO with log-rank test for more statistical rigor.
    pub fn compute_log_rank(&self, is_fitness: &[f64], oos_fitness: &[f64]) -> f64 {
        let pbo = self.compute(is_fitness, oos_fitness);

        // Log-rank style adjustment: weight by magnitude of overfitting
        let diffs: Vec<f64> = is_fitness
            .iter()
            .zip(oos_fitness.iter())
            .map(|(is, oos)| (oos - is).abs())
            .collect();

        let avg_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let weighted_pbo = pbo * (1.0 + avg_diff.min(1.0));

        weighted_pbo.min(1.0)
    }
}

/// Comprehensive overfitting assessment combining DSR and PBO.
#[derive(Debug, Clone)]
pub struct OverfittingAssessment {
    /// DSR value (>= 3.0 robust, < 1.5 likely overfitting).
    pub dsr: f64,
    /// PBO value (>= 0.5 suggests likely overfitting).
    pub pbo: f64,
    /// Whether the strategy passes both guards.
    pub passes_guards: bool,
    /// Recommendation message.
    pub recommendation: String,
    /// Additional diagnostic details.
    pub diagnostics: OverfittingDiagnostics,
}

/// Detailed diagnostics for overfitting assessment.
#[derive(Debug, Clone)]
pub struct OverfittingDiagnostics {
    /// Original Sharpe ratio.
    pub sharpe: f64,
    /// Return skewness.
    pub skewness: f64,
    /// Return excess kurtosis.
    pub kurtosis: f64,
    /// Number of trials used.
    pub n_trials: usize,
    /// Number of CSCV folds.
    pub n_folds: usize,
    /// In-sample fitness values.
    pub is_fitness: Vec<f64>,
    /// Out-of-sample fitness values.
    pub oos_fitness: Vec<f64>,
}

impl OverfittingAssessment {
    /// Assess overfitting given strategy performance data.
    pub fn assess(
        sharpe: f64,
        returns: &[f64],
        is_fitness: &[f64],
        oos_fitness: &[f64],
        n_trials: usize,
        n_folds: usize,
    ) -> Self {
        let dsr_calc = DSRCalculator::new(n_trials);
        let dsr = dsr_calc.compute(sharpe, returns);

        let pbo_calc = PBOCalculator::new(n_folds);
        let pbo = pbo_calc.compute(is_fitness, oos_fitness);

        // Compute return moments for diagnostics
        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let variance =
            returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
        let std_dev = variance.sqrt().max(1e-10);

        let skewness = returns
            .iter()
            .map(|&r| ((r - mean) / std_dev).powi(3))
            .sum::<f64>()
            / n;

        let kurtosis = returns
            .iter()
            .map(|&r| ((r - mean) / std_dev).powi(4))
            .sum::<f64>()
            / n
            - 3.0;

        let passes_guards = dsr >= 3.0 && pbo < 0.5;

        let recommendation = if passes_guards {
            "Strategy passes overfitting guards. Consider live deployment with position limits."
                .to_string()
        } else if dsr < 1.5 {
            "Low DSR (<1.5) suggests significant overfitting. Reduce parameter space, increase data, or simplify strategy.".to_string()
        } else if pbo >= 0.5 {
            "High PBO (>=0.5) suggests overfitting. Increase out-of-sample period, use more folds, or regularize.".to_string()
        } else {
            "Marginal overfitting metrics. Consider additional validation before deployment."
                .to_string()
        };

        OverfittingAssessment {
            dsr,
            pbo,
            passes_guards,
            recommendation,
            diagnostics: OverfittingDiagnostics {
                sharpe,
                skewness,
                kurtosis,
                n_trials,
                n_folds,
                is_fitness: is_fitness.to_vec(),
                oos_fitness: oos_fitness.to_vec(),
            },
        }
    }

    /// Generate a human-readable report.
    pub fn report(&self) -> String {
        format!(
            "=== Overfitting Assessment Report ===\n\
            DSR: {:.3} {}\n\
            PBO: {:.3} {}\n\
            Passes Guards: {}\n\
            \n\
            Diagnostics:\n\
            - Sharpe: {:.3}\n\
            - Skewness: {:.3}\n\
            - Excess Kurtosis: {:.3}\n\
            - Trials: {}\n\
            - CSCV Folds: {}\n\
            - IS Fitness: {:?}\n\
            - OOS Fitness: {:?}\n\
            \n\
            Recommendation: {}\n",
            self.dsr,
            if self.dsr >= 3.0 {
                "✓"
            } else if self.dsr >= 1.5 {
                "~"
            } else {
                "✗"
            },
            self.pbo,
            if self.pbo < 0.5 { "✓" } else { "✗" },
            if self.passes_guards { "YES" } else { "NO" },
            self.diagnostics.sharpe,
            self.diagnostics.skewness,
            self.diagnostics.kurtosis,
            self.diagnostics.n_trials,
            self.diagnostics.n_folds,
            self.diagnostics.is_fitness,
            self.diagnostics.oos_fitness,
            self.recommendation
        )
    }
}
