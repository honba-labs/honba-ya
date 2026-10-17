//! How a trial is scored.
//!
//! A [`Fitness`] is the only place a sweep decides that one parameter set beat
//! another, so it is deliberately narrow: a pure function from a finished
//! [`TrialReport`] to a number, with no clock, no I/O and no state that could
//! change between two calls on the same report. The ranking in
//! [`SweepReport`](crate::SweepReport) is a sort over those numbers, so a
//! fitness that is not reproducible makes the whole sweep unreproducible.

use crate::report::TrialReport;

/// Scores a finished trial, best is highest.
///
/// Implementations must be deterministic: [`run`](crate::run()) sorts the scores
/// to rank trials, so a fitness that returned a different number for the same
/// report would make the ranking depend on when it was called.
pub trait Fitness: Send + Sync {
    /// Returns the score of `report`; higher is better.
    fn score(&self, report: &TrialReport) -> f64;
}

/// Ranks by the Sharpe ratio of the trial's return series, zero when there is
/// none.
///
/// Zero is the "no information" score, and it is what a trial gets when it
/// produced no round trips at all (there is no return series to annualize) and
/// when it produced fewer than two return observations or a return series with
/// no variance (the Sharpe ratio is undefined). A strategy that never trades is
/// therefore ranked with the strategies that traded and lost nothing, not
/// ranked as though it had an infinite or undefined ratio.
///
/// ```
/// use honba_messages::{Exchange, InstrumentId};
/// use honba_sweep::{Fitness, SharpeFitness, StrategySpec, TrialMetrics, TrialParams, TrialReport};
///
/// let report = TrialReport {
///     trial_id: 0,
///     params: TrialParams {
///         seed: 0,
///         spec: StrategySpec::BuyAndHold {
///             instrument: InstrumentId::new("NIFTY50", Exchange::new("NSE")),
///             quantity: 75.0,
///         },
///     },
///     metrics: TrialMetrics { fills: 0, round_trips: 0, trades: None, equity: None },
///     audit: Vec::new(),
/// };
/// assert_eq!(SharpeFitness.score(&report), 0.0);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SharpeFitness;

impl Fitness for SharpeFitness {
    fn score(&self, report: &TrialReport) -> f64 {
        report
            .metrics
            .equity
            .and_then(|equity| equity.sharpe)
            .unwrap_or(0.0)
    }
}

/// Ranks by the Calmar ratio, `annualized_return / max_drawdown`, zero when it is undefined.
///
/// Like [`SharpeFitness`], zero is the "no information" score: a trial with no return series,
/// or one whose maximum drawdown was zero (the ratio is undefined), is ranked with the trials
/// that traded and lost nothing rather than as though it had an infinite ratio.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CalmarFitness;

impl Fitness for CalmarFitness {
    fn score(&self, report: &TrialReport) -> f64 {
        report
            .metrics
            .equity
            .and_then(|equity| equity.calmar)
            .unwrap_or(0.0)
    }
}

/// Ranks by the Sortino ratio, the mean excess return over downside deviation, zero when it is
/// undefined (no return series, or no downside).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SortinoFitness;

impl Fitness for SortinoFitness {
    fn score(&self, report: &TrialReport) -> f64 {
        report
            .metrics
            .equity
            .and_then(|equity| equity.sortino)
            .unwrap_or(0.0)
    }
}

/// Ranks by the Omega ratio at the zero threshold, which reduces to the profit factor
/// (`gross_profit / gross_loss`) of the trial's round trips.
///
/// Zero is the "no information" score: a trial with no round trips, or one with no losing
/// trips (the ratio is undefined), is ranked with the trials that broke even rather than as
/// though it had an infinite ratio.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OmegaFitness;

impl Fitness for OmegaFitness {
    fn score(&self, report: &TrialReport) -> f64 {
        report
            .metrics
            .trades
            .and_then(|trades| trades.profit_factor)
            .unwrap_or(0.0)
    }
}
