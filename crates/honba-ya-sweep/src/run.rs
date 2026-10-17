//! The async entry points that drive many trials concurrently.
//!
//! [`run`] is the only function a sweep caller uses. It spawns one blocking
//! task per trial, at most `max_concurrency` at a time, and collects the
//! results into a [`SweepReport`] whose trial order and ranking are fixed by
//! the plan and the fitness, not by completion order.
//!
//! [`run_one`] is the convenience that reproduces exactly one trial of a sweep
//! by running it inside `spawn_blocking`, so a caller that saw something
//! interesting in trial 3 of a 100-trial sweep can rerun just that one without
//! rebuilding the whole plan.

use std::sync::Arc;

use honba_feed::Dataset;
use tokio::task::JoinSet;

use crate::error::{Result, SweepError};
use crate::plan::SweepPlan;
use crate::report::{SweepReport, TrialOutcome, TrialReport};
use crate::trial::{guard_trial, run_trial_with, TrialConfig};

/// Runs every trial in `plan` over the shared `data`, at most `max_concurrency`
/// at a time.
///
/// Results are written into a slot per trial id and only compacted after every
/// task has landed, so the report is byte-identical whatever order the trials
/// complete in — that property is what makes a sweep reproducible, and the
/// next story asserts it at 1, 4 and 16 worker threads.
///
/// A **panicking** trial is captured as [`TrialOutcome::Failed`] with the panic
/// message, not abort the sweep. A trial that returns `Err` becomes `Failed`
/// too: **one bad trial must not lose the others**. `run` itself only errors
/// on plan-level problems (`max_concurrency == 0`, invalid cash/periods), never
/// on trial failure.
///
/// ```
/// use std::sync::Arc;
/// use honba_feed::{ColumnarSliceBuilder, Dataset};
/// use honba_messages::{BarAggregation, BarSpecification, Exchange, InstrumentId, PriceType, UnixNanos};
/// use honba_sweep::{SharpeFitness, StrategySpec, SweepPlan, TrialParams};
///
/// let id = InstrumentId::new("NIFTY50", Exchange::new("NSE"));
/// let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
/// let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
/// builder.push(UnixNanos::from_u64(1), 10.0, 12.0, 9.0, 11.0, 500.0).unwrap();
/// let slice = builder.finish().unwrap();
/// let data = Dataset::from_slices(vec![slice]).unwrap();
///
/// let trials = vec![TrialParams {
///     seed: 0,
///     spec: StrategySpec::BuyAndHold { instrument: id, quantity: 10.0 },
/// }];
/// let plan = SweepPlan::new(trials, Arc::new(SharpeFitness));
/// // run(&plan, Arc::new(data)).await
/// ```
pub async fn run(plan: &SweepPlan, data: Arc<Dataset>) -> Result<SweepReport> {
    plan.validate()?;

    let config = TrialConfig::from_plan(plan);
    let max_concurrency = plan.max_concurrency();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrency));
    let mut join_set: JoinSet<(usize, Result<TrialReport>)> = JoinSet::new();

    for (trial_id, params) in plan.trials().iter().enumerate() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let params = params.clone();
        let data = Arc::clone(&data);
        join_set.spawn(async move {
            let _permit = permit;
            let inner =
                tokio::task::spawn_blocking(move || guard_trial(trial_id, &params, &data, config))
                    .await;
            let result = match inner {
                Ok(report) => report,
                Err(join_error) => {
                    if join_error.is_panic() {
                        Err(SweepError::Trial {
                            trial_id,
                            reason: "the trial panicked".to_string(),
                        })
                    } else {
                        Err(SweepError::Join(join_error.to_string()))
                    }
                }
            };
            (trial_id, result)
        });
    }

    let mut outcomes: Vec<Option<TrialOutcome>> = vec![None; plan.trials().len()];
    while let Some(result) = join_set.join_next().await {
        let (trial_id, outcome) = match result {
            Ok((trial_id, Ok(report))) => (trial_id, TrialOutcome::Completed(Box::new(report))),
            Ok((trial_id, Err(error))) => (
                trial_id,
                TrialOutcome::Failed {
                    trial_id,
                    reason: error.to_string(),
                },
            ),
            Err(join_error) => {
                if join_error.is_panic() {
                    return Err(SweepError::Join("the sweep task panicked".to_string()));
                }
                return Err(SweepError::Join(join_error.to_string()));
            }
        };
        if trial_id < outcomes.len() {
            outcomes[trial_id] = Some(outcome);
        } else {
            return Err(SweepError::InvalidPlan(format!(
                "trial_id {trial_id} out of bounds for {} trials",
                plan.trials().len()
            )));
        }
    }

    let mut compacted = Vec::with_capacity(outcomes.len());
    for (index, outcome) in outcomes.into_iter().enumerate() {
        compacted.push(outcome.unwrap_or_else(|| TrialOutcome::Failed {
            trial_id: index,
            reason: "the trial was never spawned".to_string(),
        }));
    }

    Ok(SweepReport::from_outcomes(compacted, plan.fitness()))
}

/// Runs exactly one trial of `plan` over `data`, inside `spawn_blocking`, and
/// returns its report or its failure.
///
/// This is the async convenience that reproduces one trial of a sweep exactly:
/// the trial runs on the blocking pool with the same [`TrialConfig`] the full
/// sweep used, and its report is `PartialEq` with the one in the sweep's
/// [`SweepReport`]. A caller that wants to debug trial 3 of a 100-trial sweep
/// does not need to rerun the other 99.
///
/// The trial id must exist in `plan`; an out-of-bounds id is an
/// [`SweepError::InvalidPlan`], not a panic.
pub async fn run_one(plan: &SweepPlan, data: Arc<Dataset>, trial_id: usize) -> Result<TrialReport> {
    plan.validate()?;
    if trial_id >= plan.trials().len() {
        return Err(SweepError::InvalidPlan(format!(
            "trial_id {trial_id} out of bounds for {} trials",
            plan.trials().len()
        )));
    }

    let params = plan.trials()[trial_id].clone();
    let config = TrialConfig::from_plan(plan);

    tokio::task::spawn_blocking(move || run_trial_with(trial_id, &params, &data, config))
        .await
        .map_err(|join_error| {
            if join_error.is_panic() {
                SweepError::Trial {
                    trial_id,
                    reason: "the trial panicked".to_string(),
                }
            } else {
                SweepError::Join(join_error.to_string())
            }
        })?
}
