//! The run state machine and the run manifest (ADR 0017 decisions 1 and 3).

use honba_messages::{ErrorCode, ErrorDetail, SCHEMA_VERSION};
use honba_strategy::StrategyIr;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::responses::{
    BacktestMetrics, BacktestResponse, RunStatus, SweepReportResponse, SweepResponse,
};
use crate::run_id::RunId;
use crate::run_resolve::ResolvedRequest;

/// On-disk version of the manifest (its own axis, not `schema_version`). Starts at 1.
pub const MANIFEST_VERSION: u32 = 1;

/// What happened to a run; the input of [`RunStatus::next`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunEvent {
    /// A worker picked the run up.
    Start,
    /// The run finished successfully.
    Complete,
    /// The run finished with a failure.
    Fail,
    /// The run was cancelled (graceful shutdown in v1).
    Cancel,
}

/// A transition the state machine refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("illegal run transition: {event:?} while {from:?}")]
pub struct TransitionError {
    /// The status the run was in.
    pub from: RunStatus,
    /// The refused event.
    pub event: RunEvent,
}

impl RunStatus {
    /// Whether the status is final: `completed`, `failed` or `cancelled`.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// The transition table. A terminal status never changes; every cell not listed in
    /// ADR 0017 decision 1 is refused (including `pending -> failed`).
    pub fn next(self, event: RunEvent) -> Result<Self, TransitionError> {
        match (self, event) {
            (Self::Pending, RunEvent::Start) => Ok(Self::Running),
            (Self::Pending | Self::Running, RunEvent::Cancel) => Ok(Self::Cancelled),
            (Self::Running, RunEvent::Complete) => Ok(Self::Completed),
            (Self::Running, RunEvent::Fail) => Ok(Self::Failed),
            _ => Err(TransitionError { from: self, event }),
        }
    }

    /// Whether some event moves `self` to `to`.
    pub fn can_transition_to(self, to: Self) -> bool {
        [
            RunEvent::Start,
            RunEvent::Complete,
            RunEvent::Fail,
            RunEvent::Cancel,
        ]
        .into_iter()
        .any(|event| self.next(event) == Ok(to))
    }
}

/// What a run executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// `run_id` space, `/backtests`.
    Backtest,
    /// `job_id` space, `/sweeps`.
    Sweep,
}

/// The per-run metadata record (`manifest.json`), also the in-memory registry entry.
///
/// Pins the resolved strategy and request. It deliberately has no data root or journals
/// path: those stay in the REST layer and never reach a response or an error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunManifest {
    /// [`MANIFEST_VERSION`] at write time; a reader rejects an unknown value.
    pub manifest_version: u32,
    /// The run's id (directory name on disk).
    pub run_id: RunId,
    /// Backtest or sweep.
    pub kind: RunKind,
    /// Current status.
    pub status: RunStatus,
    /// The run's seed (also inside `request`).
    pub seed: u64,
    /// Strategy content id or registered name, as resolved at submit.
    pub strategy_id: String,
    /// The resolved strategy IR, copied so a run never consults the catalog again.
    pub strategy: StrategyIr,
    /// The resolved request.
    pub request: ResolvedRequest,
    /// The journal's wire `schema_version`.
    pub schema_version: u32,
    /// Wall-clock creation time (RFC 3339, metadata only).
    pub created_at: String,
    /// When a worker started the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When the run became terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// Backtest metrics, once completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<BacktestMetrics>,
    /// What a backtest did not model, once terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assumptions: Option<Value>,
    /// Sweep report, once completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<SweepReportResponse>,
    /// Why the run failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

fn illegal(e: TransitionError) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::InternalError, e.to_string())
        .with_context(serde_json::json!({"reason": "illegal_transition"}))
}

impl RunManifest {
    /// A freshly admitted run: `pending`, nothing started.
    pub fn new_pending(
        run_id: RunId,
        strategy_id: String,
        strategy: StrategyIr,
        request: ResolvedRequest,
        created_at: impl Into<String>,
    ) -> Self {
        let (kind, seed) = match &request {
            ResolvedRequest::Backtest(r) => (RunKind::Backtest, r.seed),
            ResolvedRequest::Sweep(r) => (RunKind::Sweep, r.seed),
        };
        Self {
            manifest_version: MANIFEST_VERSION,
            run_id,
            kind,
            status: RunStatus::Pending,
            seed,
            strategy_id,
            strategy,
            request,
            schema_version: SCHEMA_VERSION,
            created_at: created_at.into(),
            started_at: None,
            finished_at: None,
            metrics: None,
            assumptions: None,
            report: None,
            error: None,
        }
    }

    fn apply(&mut self, event: RunEvent, at: &str) -> Result<(), ErrorDetail> {
        self.status = self.status.next(event).map_err(illegal)?;
        if self.status == RunStatus::Running {
            self.started_at = Some(at.to_owned());
        } else {
            self.finished_at = Some(at.to_owned());
        }
        Ok(())
    }

    /// `pending -> running`.
    pub fn start(&mut self, at: &str) -> Result<(), ErrorDetail> {
        self.apply(RunEvent::Start, at)
    }

    /// `running -> completed` with a backtest's results.
    pub fn complete_backtest(
        &mut self,
        metrics: BacktestMetrics,
        assumptions: Value,
        at: &str,
    ) -> Result<(), ErrorDetail> {
        self.apply(RunEvent::Complete, at)?;
        self.metrics = Some(metrics);
        self.assumptions = Some(assumptions);
        Ok(())
    }

    /// `running -> completed` with a sweep's report.
    pub fn complete_sweep(
        &mut self,
        report: SweepReportResponse,
        at: &str,
    ) -> Result<(), ErrorDetail> {
        self.apply(RunEvent::Complete, at)?;
        self.report = Some(report);
        Ok(())
    }

    /// `running -> failed`, keeping `error`.
    pub fn fail(&mut self, error: ErrorDetail, at: &str) -> Result<(), ErrorDetail> {
        self.apply(RunEvent::Fail, at)?;
        self.error = Some(error);
        Ok(())
    }

    /// `pending | running -> cancelled`.
    pub fn cancel(&mut self, at: &str) -> Result<(), ErrorDetail> {
        self.apply(RunEvent::Cancel, at)
    }

    /// The `GET /backtests/{id}` body; `None` for a sweep (the route answers 404).
    ///
    /// `metrics` only when completed, `assumptions` only when terminal, `error` only when
    /// failed (ADR 0017 decision 7).
    pub fn to_backtest_response(&self) -> Option<BacktestResponse> {
        (self.kind == RunKind::Backtest).then(|| BacktestResponse {
            run_id: self.run_id.to_string(),
            status: self.status,
            metrics: self
                .metrics
                .clone()
                .filter(|_| self.status == RunStatus::Completed),
            assumptions: self
                .assumptions
                .clone()
                .filter(|_| self.status.is_terminal()),
            validation: None,
            error: self
                .error
                .clone()
                .filter(|_| self.status == RunStatus::Failed),
        })
    }

    /// The `GET /sweeps/{id}` body; `None` for a backtest.
    pub fn to_sweep_response(&self) -> Option<SweepResponse> {
        (self.kind == RunKind::Sweep).then(|| SweepResponse {
            job_id: self.run_id.to_string(),
            status: self.status,
            report: self
                .report
                .clone()
                .filter(|_| self.status == RunStatus::Completed),
            error: self
                .error
                .clone()
                .filter(|_| self.status == RunStatus::Failed),
        })
    }
}
