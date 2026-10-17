//! Unit tests for `crate::report`.

use crate::{SharpeFitness, SweepReport, TrialOutcome};

use super::{no_equity_report, report};

fn completed(trial_id: usize, sharpe: Option<f64>) -> TrialOutcome {
    TrialOutcome::Completed(Box::new(report(trial_id, sharpe)))
}

fn no_trades(trial_id: usize) -> TrialOutcome {
    TrialOutcome::Completed(Box::new(no_equity_report(trial_id)))
}

fn failed(trial_id: usize) -> TrialOutcome {
    TrialOutcome::Failed {
        trial_id,
        reason: format!("trial {trial_id} blew up"),
    }
}

fn sweep(outcomes: Vec<TrialOutcome>) -> SweepReport {
    SweepReport::from_outcomes(outcomes, &SharpeFitness)
}

#[test]
fn ranking_sorts_by_score_descending() {
    let report = sweep(vec![
        completed(0, Some(0.1)),
        completed(1, Some(0.9)),
        completed(2, Some(0.5)),
        completed(3, Some(-2.0)),
    ]);
    assert_eq!(report.ranking(), &[1, 2, 0, 3]);
}

#[test]
fn equal_scores_are_ordered_by_ascending_trial_id() {
    let report = sweep(vec![
        completed(2, Some(0.4)),
        completed(0, Some(0.4)),
        completed(1, Some(0.9)),
        completed(3, Some(0.4)),
    ]);
    assert_eq!(report.ranking(), &[1, 0, 2, 3]);
}

#[test]
fn failed_trials_are_left_out_of_the_ranking() {
    let report = sweep(vec![
        completed(0, Some(0.2)),
        failed(1),
        completed(2, Some(0.8)),
    ]);
    assert_eq!(report.ranking(), &[2, 0]);
}

#[test]
fn trials_that_never_traded_rank_by_their_zero_score() {
    let report = sweep(vec![
        no_trades(0),
        completed(1, Some(0.5)),
        completed(2, Some(0.0)),
    ]);
    assert_eq!(report.ranking(), &[1, 0, 2]);
}

#[test]
fn a_nan_score_ranks_last_rather_than_breaking_the_order() {
    let report = sweep(vec![
        completed(0, Some(0.2)),
        completed(1, Some(f64::NAN)),
        completed(2, Some(0.1)),
    ]);
    assert_eq!(report.ranking(), &[0, 2, 1]);
}

#[test]
fn the_ranking_does_not_depend_on_the_order_the_outcomes_arrive_in() {
    let ordered = sweep(vec![
        completed(0, Some(0.4)),
        completed(1, Some(0.9)),
        completed(2, Some(0.4)),
        failed(3),
    ]);
    let shuffled = sweep(vec![
        completed(2, Some(0.4)),
        failed(3),
        completed(1, Some(0.9)),
        completed(0, Some(0.4)),
    ]);
    assert_eq!(ordered.ranking(), shuffled.ranking());
}

#[test]
fn reports_yields_completed_trials_only_and_in_trial_order() {
    let report = sweep(vec![
        completed(0, Some(0.2)),
        failed(1),
        completed(2, Some(0.8)),
        failed(3),
    ]);
    let ids: Vec<usize> = report.reports().map(|trial| trial.trial_id).collect();
    assert_eq!(ids, vec![0, 2]);
    assert_eq!(report.len(), 4, "len counts every trial, completed or not");
    assert!(!report.is_empty());
    assert_eq!(report.outcomes().len(), 4);
}

#[test]
fn a_failure_carries_its_trial_id_and_reason() {
    let report = sweep(vec![completed(0, Some(0.2)), failed(1)]);
    let TrialOutcome::Failed { trial_id, reason } = &report.outcomes()[1] else {
        panic!("expected a failed outcome, got {:?}", report.outcomes()[1]);
    };
    assert_eq!(*trial_id, 1);
    assert_eq!(reason, "trial 1 blew up");
}

#[test]
fn an_empty_sweep_is_empty() {
    let report = sweep(Vec::new());
    assert!(report.is_empty());
    assert_eq!(report.len(), 0);
    assert!(report.outcomes().is_empty());
    assert!(report.ranking().is_empty());
    assert_eq!(report.reports().count(), 0);
}

#[test]
fn two_sweeps_over_the_same_trials_compare_equal() {
    let left = sweep(vec![
        completed(0, Some(0.2)),
        failed(1),
        completed(2, Some(0.8)),
    ]);
    let right = sweep(vec![
        completed(0, Some(0.2)),
        failed(1),
        completed(2, Some(0.8)),
    ]);
    assert_eq!(left, right);
}
