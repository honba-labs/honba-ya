#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! Concurrent parameter sweeps for the Honba platform.
//!
//! A sweep runs many parameter sets over one shared dataset and scores each of
//! them, which makes it the only part of the platform that needs real threads.
//! It is deliberately boring about them: every trial builds its own engine, its
//! own paper sink and its own strategy, and owns all three outright.

pub mod error;
pub mod fitness;
pub mod plan;
pub mod report;
pub mod run;
pub mod trial;

pub use error::{Result, SweepError};
pub use fitness::{CalmarFitness, Fitness, OmegaFitness, SharpeFitness, SortinoFitness};
pub use honba_feed::Dataset;
pub use plan::{
    default_initial_cash, StrategyFactory, StrategySpec, SweepPlan, TrialParams,
    DEFAULT_MAX_CONCURRENCY, DEFAULT_PERIODS_PER_YEAR,
};
pub use report::{SweepReport, TrialMetrics, TrialOutcome, TrialReport};
pub use run::{run, run_one};
pub use trial::{guard_trial, run_trial, run_trial_with, TrialConfig};

#[cfg(test)]
mod tests;
