//! Unit tests for `crate::error`.

use crate::SweepError;

#[test]
fn every_variant_names_itself_when_displayed() {
    assert_eq!(
        SweepError::InvalidPlan("max_concurrency must be at least 1, got 0".to_string())
            .to_string(),
        "invalid sweep plan: max_concurrency must be at least 1, got 0"
    );
    assert_eq!(
        SweepError::Join("the task was cancelled".to_string()).to_string(),
        "sweep task did not complete: the task was cancelled"
    );
    assert_eq!(
        SweepError::Trial {
            trial_id: 2,
            reason: "clock cannot go backwards".to_string(),
        }
        .to_string(),
        "trial 2 failed: clock cannot go backwards"
    );
    assert_eq!(
        SweepError::Analytics("no return observations".to_string()).to_string(),
        "analytics failed: no return observations"
    );
}

#[test]
fn equality_compares_each_variants_fields() {
    assert_eq!(
        SweepError::InvalidPlan("same".to_string()),
        SweepError::InvalidPlan("same".to_string())
    );
    assert_ne!(
        SweepError::InvalidPlan("left".to_string()),
        SweepError::InvalidPlan("right".to_string())
    );
    assert_eq!(
        SweepError::Trial {
            trial_id: 1,
            reason: "same".to_string()
        },
        SweepError::Trial {
            trial_id: 1,
            reason: "same".to_string()
        }
    );
    assert_ne!(
        SweepError::Trial {
            trial_id: 1,
            reason: "same".to_string()
        },
        SweepError::Trial {
            trial_id: 2,
            reason: "same".to_string()
        }
    );
    assert_ne!(
        SweepError::Trial {
            trial_id: 1,
            reason: "left".to_string()
        },
        SweepError::Trial {
            trial_id: 1,
            reason: "right".to_string()
        }
    );
    assert_ne!(
        SweepError::Join("same".to_string()),
        SweepError::Analytics("same".to_string())
    );
}

#[test]
fn a_sweep_error_is_a_std_error() {
    fn boxed(error: SweepError) -> Box<dyn std::error::Error> {
        Box::new(error)
    }
    let error = boxed(SweepError::Join("the task was cancelled".to_string()));
    assert_eq!(
        error.to_string(),
        "sweep task did not complete: the task was cancelled"
    );
}

#[test]
fn a_sweep_error_round_trips_through_the_crate_result() {
    let result: crate::Result<u8> = Err(SweepError::Analytics("no returns".to_string()));
    assert!(matches!(result, Err(SweepError::Analytics(msg)) if msg == "no returns"));
}
