//! The run execution ports (ADR 0017 decision 8): [`JournalWriter`], [`RunExecutor`] and
//! the values that cross them, [`RunJob`] and [`RunOutcome`].
//!
//! Pure: no I/O lives here. `honba-api-rest` supplies the file journal, the worker threads
//! and the concrete executor.

use honba_messages::{ErrorCode, ErrorDetail, Message};
use honba_strategy::StrategyIr;
use serde_json::Value;

use crate::responses::{BacktestMetrics, SweepReportResponse};
use crate::run_id::RunId;
use crate::run_resolve::ResolvedRequest;
use crate::runs::{RunKind, RunManifest};

/// Synchronous, append-only, per-run journal.
///
/// Same contract as `honba_ports::Sink` (append-only, explicit flush, a failure is kept), but
/// sync: the worker is a plain thread, and `honba-api` may not depend on `honba-ports`.
pub trait JournalWriter: Send {
    /// Appends one message as one record.
    fn append(&mut self, msg: &Message) -> Result<(), ErrorDetail>;
    /// Pushes buffered records to the underlying store.
    fn flush(&mut self) -> Result<(), ErrorDetail>;
}

/// Runs one job to completion on the calling (worker) thread.
///
/// The synchronous kernel has no stop check between events and a job carries no cancel
/// token, so an executor cannot be interrupted once started (ADR 0017 decision 7); it
/// either finishes or fails.
pub trait RunExecutor: Send + Sync {
    /// Executes `job`, appending its records to `journal`.
    fn execute(
        &self,
        job: RunJob,
        journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail>;
}

/// Everything an executor needs that is pinned in the manifest.
///
/// The data root, execution and account config reach the concrete executor at
/// construction, not through the job.
#[derive(Clone, Debug, PartialEq)]
pub struct RunJob {
    /// The run's id.
    pub run_id: RunId,
    /// Backtest or sweep.
    pub kind: RunKind,
    /// Strategy content id or registered name, as resolved at submit.
    pub strategy_id: String,
    /// The resolved strategy IR.
    pub strategy: StrategyIr,
    /// The resolved request.
    pub request: ResolvedRequest,
    /// The seed.
    pub seed: u64,
}

impl RunJob {
    /// The job a worker runs for `manifest`.
    pub fn from_manifest(manifest: &RunManifest) -> Self {
        Self {
            run_id: manifest.run_id.clone(),
            kind: manifest.kind,
            strategy_id: manifest.strategy_id.clone(),
            strategy: manifest.strategy.clone(),
            request: manifest.request.clone(),
            seed: manifest.seed,
        }
    }
}

/// What a finished run produced.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum RunOutcome {
    /// A backtest's results.
    Backtest {
        /// Headline metrics.
        metrics: BacktestMetrics,
        /// What the run did not model, stated explicitly.
        assumptions: Value,
    },
    /// A sweep's report.
    Sweep {
        /// Ranked trials.
        report: SweepReportResponse,
    },
}

impl RunManifest {
    /// `running -> completed` with `outcome`.
    pub fn complete(&mut self, outcome: RunOutcome, at: &str) -> Result<(), ErrorDetail> {
        match (self.kind, outcome) {
            (
                RunKind::Backtest,
                RunOutcome::Backtest {
                    metrics,
                    assumptions,
                },
            ) => self.complete_backtest(metrics, assumptions, at),
            (RunKind::Sweep, RunOutcome::Sweep { report }) => self.complete_sweep(report, at),
            _ => Err(ErrorDetail::new(
                ErrorCode::InternalError,
                "run outcome does not match the run kind",
            )
            .with_context(serde_json::json!({"reason": "outcome_kind_mismatch"}))),
        }
    }
}
