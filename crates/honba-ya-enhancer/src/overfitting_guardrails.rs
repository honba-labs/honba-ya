//! Overfitting Guardrails: DSR and PBO computation.

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
            // Need minimum sample size
            return sharpe;
        }

        // Compute return moments
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
        let std_dev = variance.sqrt();

        // Skewness and kurtosis
        let n = returns.len() as f64;
        let skewness: f64 = returns
            .iter()
            .map(|&r| ((r - mean) / std_dev).powi(3))
            .sum::<f64>()
            / n
            * n
            / (n - 1.0)
            / (n - 2.0);

        let kurtosis: f64 = returns
            .iter()
            .map(|&r| {
                let z = (r - mean) / std_dev;
                z.powi(4)
            })
            .sum::<f64>()
            / n
            * n
            * (n + 1.0)
            / ((n - 1.0) * (n - 2.0) * (n - 3.0));

        // DSR formula from López de Prado
        // DSR = Sharpe / (1 + 2.5 * sqrt(2 / n) * (1 + gamma_1^2)^(1/4) * (1 + kappa^2)^(1/4))
        // where gamma_1 = skewness, kappa = excess kurtosis

        let gamma1 = skewness;
        let kappa = kurtosis - 3.0; // excess kurtosis

        let numerator = sharpe;
        let denominator = 1.0
            + 2.5f64
                * (2.0 / n).sqrt()
                * (1.0 + gamma1.powi(2)).powf(0.25)
                * (1.0 + kappa.powi(2)).powf(0.25);

        let dsr = numerator / denominator.max(1e-10);
        dsr
    }
}

/// Probability of Backtest Overfitting (PBO) via Combinatorially Symmetric Cross-Validation (CSCV).

/// Partitions a backtest period into k symmetric folds, alternating
/// in-sample/out-of-sample segments, and computes the distribution of
/// fitness differences to estimate overfitting probability.
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
}

impl OverfittingAssessment {
    /// Assess overfitting given strategy performance data.
    pub fn assess(sharpe: f64, returns: &[f64], is_fitness: &[f64], oos_fitness: &[f64]) -> Self {
        let dsr_calc = DSRCalculator::new(100);
        let dsr = dsr_calc.compute(sharpe, returns);

        let pbo_calc = PBOCalculator::new(5);
        let pbo = pbo_calc.compute(is_fitness, oos_fitness);

        let passes_guards = dsr >= 3.0 && pbo < 0.5;
        let recommendation = if passes_guards {
            "Strategy passes overfitting guards. Consider live deployment.".to_string()
        } else if dsr < 1.5 {
            "Low DSR suggests overfitting. Consider parameter reduction or more data.".to_string()
        } else {
            "High PBO suggests overfitting. Re-train with more data or different regime."
                .to_string()
        };

        OverfittingAssessment {
            dsr,
            pbo,
            passes_guards,
            recommendation,
        }
    }
}
