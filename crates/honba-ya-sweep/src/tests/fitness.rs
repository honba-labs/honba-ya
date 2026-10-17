//! Unit tests for `crate::fitness`.

use super::{no_equity_report, report, report_with};
use crate::{
    CalmarFitness, Fitness, OmegaFitness, SharpeFitness, SortinoFitness, SweepReport, TrialOutcome,
};

#[test]
fn the_score_is_the_sharpe_ratio() {
    assert_eq!(SharpeFitness.score(&report(0, Some(1.75))), 1.75);
    assert_eq!(SharpeFitness.score(&report(0, Some(-0.5))), -0.5);
    assert_eq!(SharpeFitness.score(&report(0, Some(0.0))), 0.0);
}

#[test]
fn a_trial_that_produced_no_returns_scores_zero() {
    assert_eq!(SharpeFitness.score(&no_equity_report(0)), 0.0);
}

#[test]
fn an_undefined_sharpe_scores_zero() {
    assert_eq!(SharpeFitness.score(&report(0, None)), 0.0);
}

#[test]
fn two_trials_with_the_same_equity_score_the_same_whatever_their_id() {
    assert_eq!(
        SharpeFitness.score(&report(0, Some(0.3))),
        SharpeFitness.score(&report(9, Some(0.3)))
    );
}

#[test]
fn scoring_is_a_pure_function_of_the_report() {
    let trial = report(4, Some(0.42));
    let before = trial.clone();
    assert_eq!(SharpeFitness.score(&trial), 0.42);
    assert_eq!(trial, before, "scoring must not touch the report");
    assert_eq!(SharpeFitness.score(&trial), 0.42);
}

#[test]
fn the_fitness_is_object_safe_and_shareable() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<SharpeFitness>();
    assert_send_sync::<&'static dyn Fitness>();
    let boxed: Box<dyn Fitness> = Box::new(SharpeFitness);
    assert_eq!(boxed.score(&report(1, Some(2.0))), 2.0);
}

#[test]
fn the_calmar_score_is_the_calmar_ratio() {
    assert_eq!(
        CalmarFitness.score(&report_with(0, None, Some(1.25), None)),
        1.25
    );
    assert_eq!(CalmarFitness.score(&report_with(0, None, None, None)), 0.0);
    assert_eq!(CalmarFitness.score(&no_equity_report(0)), 0.0);
}

#[test]
fn the_sortino_score_is_the_sortino_ratio() {
    assert_eq!(
        SortinoFitness.score(&report_with(0, Some(0.9), None, None)),
        0.9
    );
    assert_eq!(SortinoFitness.score(&report_with(0, None, None, None)), 0.0);
    assert_eq!(SortinoFitness.score(&no_equity_report(0)), 0.0);
}

#[test]
fn the_omega_score_is_the_profit_factor() {
    assert_eq!(
        OmegaFitness.score(&report_with(0, None, None, Some(2.5))),
        2.5
    );
    assert_eq!(OmegaFitness.score(&report_with(0, None, None, None)), 0.0);
    assert_eq!(OmegaFitness.score(&no_equity_report(0)), 0.0);
}

#[test]
fn the_new_fitnesses_are_object_safe_and_shareable() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<CalmarFitness>();
    assert_send_sync::<SortinoFitness>();
    assert_send_sync::<OmegaFitness>();
}

/// The E4-S5 acceptance: a sweep ranked by any of the fitnesses is a stable function of the
/// trials — best-first by score, ties broken by ascending trial id, whatever the trial ids.
#[test]
fn fitness_ranking_stable() {
    fn rank(fitness: &dyn Fitness) -> Vec<usize> {
        let outcomes = vec![
            TrialOutcome::Completed(Box::new(report_with(0, Some(1.0), Some(0.5), Some(3.0)))),
            TrialOutcome::Completed(Box::new(report_with(1, Some(2.0), Some(2.0), Some(1.0)))),
            TrialOutcome::Completed(Box::new(report_with(2, Some(1.0), Some(0.5), Some(3.0)))),
            TrialOutcome::Failed {
                trial_id: 3,
                reason: "boom".to_string(),
            },
        ];
        SweepReport::from_outcomes(outcomes, fitness)
            .ranking()
            .to_vec()
    }

    // Sharpe is 2.0 for trial 1, 1.0 for trials 0 and 2: 1 first, then 0 before 2 (tie).
    assert_eq!(rank(&SharpeFitness), vec![1, 0, 2]);
    // Sortino mirrors Sharpe in these fixtures.
    assert_eq!(rank(&SortinoFitness), vec![1, 0, 2]);
    // Calmar is 2.0 for trial 1, 0.5 for trials 0 and 2.
    assert_eq!(rank(&CalmarFitness), vec![1, 0, 2]);
    // Omega (profit factor) is 3.0 for trials 0 and 2, 1.0 for trial 1: 0 before 2, then 1.
    assert_eq!(rank(&OmegaFitness), vec![0, 2, 1]);
}
