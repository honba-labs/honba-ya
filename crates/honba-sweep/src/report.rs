//! What a sweep produces: one outcome per trial, and the ranking.
//!
//! Both types here are values, not views: a [`SweepReport`] owns its trials and
//! is `Clone + PartialEq`, which is what lets a caller prove that two sweeps
//! over the same plan and dataset are equal field for field.

use std::cmp::Ordering;

use honba_analytics::{EquityStats, TradeStats};
use honba_engine::AuditRecord;

use crate::fitness::Fitness;
use crate::plan::TrialParams;

/// What one trial of a sweep produced.
///
/// A sweep always has one outcome per trial, in trial order, whether the trial
/// worked or not: an outcome that could go missing would make the report's
/// length depend on how many trials happened to finish.
#[derive(Clone, Debug, PartialEq)]
pub enum TrialOutcome {
    /// The trial ran to completion.
    Completed(Box<TrialReport>),
    /// The trial failed and the sweep carried on without it.
    Failed {
        /// The trial that failed.
        trial_id: usize,
        /// Why it failed.
        reason: String,
    },
}

/// Everything one trial produced.
///
/// Two trials with equal [`params`](TrialReport::params) produce equal reports,
/// which is the reproducibility contract of a sweep: same plan, same dataset,
/// same numbers, whatever else was running at the time.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialReport {
    /// The trial's index in the plan, which is also its slot in the report.
    pub trial_id: usize,
    /// The trial's parameters, as the plan gave them.
    pub params: TrialParams,
    /// What the trial's fills add up to.
    pub metrics: TrialMetrics,
    /// The kernel's audit trail for the trial, in kernel order.
    ///
    /// This is [`Engine::audit`](honba_engine::Engine::audit) copied verbatim:
    /// every record the kernel made, in the order it made them, with nothing
    /// added, dropped or reordered. It is the record a sweep is judged
    /// reproducible on, so a sweep that filtered it would be claiming
    /// reproducibility it had not earned.
    pub audit: Vec<AuditRecord>,
}

/// What one trial's fills add up to.
///
/// The trade and equity statistics are `Option` because a trial that produced
/// no fills still has to be reportable: [`TradeStats`] needs at least one round
/// trip and [`EquityStats`] at least one return, and both refuse an empty
/// input. `None` means the trial had nothing to compute the statistic from,
/// which is not the same as a statistic of zero — so `fills` and `round_trips`
/// are always present and say how much the trial actually did.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrialMetrics {
    /// How many fills the trial produced.
    pub fills: usize,
    /// How many round trips those fills were paired into.
    pub round_trips: usize,
    /// Statistics over the round trips; `None` when there were none.
    pub trades: Option<TradeStats>,
    /// Statistics over the return series; `None` when there were no returns.
    pub equity: Option<EquityStats>,
}

/// Every trial of a sweep, in trial order, plus the ranking.
///
/// Built by [`run`](crate::run()) and nothing else, which is what keeps the two
/// invariants together: one outcome per trial in trial order, and a ranking
/// derived only from those outcomes.
#[derive(Clone, Debug, PartialEq)]
pub struct SweepReport {
    outcomes: Vec<TrialOutcome>,
    ranking: Vec<usize>,
}

impl SweepReport {
    /// Builds a report from outcomes that are already in trial order, ranking
    /// the completed ones with `fitness`.
    pub(crate) fn from_outcomes(outcomes: Vec<TrialOutcome>, fitness: &dyn Fitness) -> Self {
        let ranking = rank(&outcomes, fitness);
        Self { outcomes, ranking }
    }

    /// Returns every outcome, in trial order, completed or failed.
    pub fn outcomes(&self) -> &[TrialOutcome] {
        &self.outcomes
    }

    /// Returns the trial ids ordered best-first by
    /// [`Fitness::score`], ties broken by ascending
    /// trial id. Failed trials are not ranked.
    ///
    /// The tiebreak is what makes the ranking a function of the trials rather
    /// than of the order they finished in: two trials that score the same
    /// always come out in the same order, whichever landed first.
    pub fn ranking(&self) -> &[usize] {
        &self.ranking
    }

    /// Returns the reports of the trials that completed, in trial order.
    pub fn reports(&self) -> impl Iterator<Item = &TrialReport> {
        self.outcomes.iter().filter_map(|outcome| match outcome {
            TrialOutcome::Completed(report) => Some(report.as_ref()),
            TrialOutcome::Failed { .. } => None,
        })
    }

    /// Returns how many trials the sweep ran, completed or not.
    pub fn len(&self) -> usize {
        self.outcomes.len()
    }

    /// Returns `true` when the sweep had no trials.
    pub fn is_empty(&self) -> bool {
        self.outcomes.is_empty()
    }
}

fn rank(outcomes: &[TrialOutcome], fitness: &dyn Fitness) -> Vec<usize> {
    let mut scored: Vec<(usize, f64)> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            TrialOutcome::Completed(report) => Some((report.trial_id, fitness.score(report))),
            TrialOutcome::Failed { .. } => None,
        })
        .collect();
    scored.sort_by(|left, right| score_order(left.1, right.1).then_with(|| left.0.cmp(&right.0)));
    scored.into_iter().map(|(trial_id, _)| trial_id).collect()
}

fn score_order(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
    }
}
