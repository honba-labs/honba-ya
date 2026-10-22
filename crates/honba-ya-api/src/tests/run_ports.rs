//! ADR 0017 decision 8: the executor and journal ports and the values crossing them.

use rust_decimal::Decimal;
use serde_json::json;

use crate::wire::{ErrorCode, ErrorDetail, Event, Message, QuoteTick};
use crate::{
    BacktestMetrics, JournalWriter, ResolvedBacktest, ResolvedRequest, ResolvedSweep, RunExecutor,
    RunJob, RunKind, RunManifest, RunOutcome, RunStatus, StrategyCatalog, SweepReportResponse,
    compile_strategy,
};
use crate::{RunId, RunIdGenerator};
use honba_core::{Exchange, InstrumentId};

use super::{tests_manifest, ts};

fn manifest(request: ResolvedRequest) -> RunManifest {
    let compiled = compile_strategy(&mut StrategyCatalog::default(), tests_manifest()).unwrap();
    let id: RunId = RunIdGenerator::default().next(1, [0; 10]).unwrap();
    RunManifest::new_pending(
        id,
        compiled.id,
        compiled.ir,
        request,
        "2026-10-08T00:00:00Z",
    )
}

fn backtest() -> ResolvedRequest {
    ResolvedRequest::Backtest(ResolvedBacktest {
        strategy: "sma".into(),
        universe: "TCS.NSE".into(),
        start: ts(1),
        end: ts(9),
        bar_spec: "1d".into(),
        initial_capital: Decimal::from(1_000_i64),
        seed: 42,
        fill_model: None,
        commission_per_fill: Decimal::ZERO,
        slippage_bps: Decimal::ZERO,
    })
}

fn metrics() -> BacktestMetrics {
    serde_json::from_value(json!({
        "trades": 2,
        "net_pnl": "5.0",
        "sharpe": "1.0",
        "max_drawdown": "0.1",
        "total_return": "0.01"
    }))
    .unwrap()
}

#[test]
fn a_job_carries_what_the_manifest_pins() {
    let m = manifest(backtest());
    let job = RunJob::from_manifest(&m);
    assert_eq!(job.run_id, m.run_id);
    assert_eq!(job.kind, RunKind::Backtest);
    assert_eq!(job.strategy_id, m.strategy_id);
    assert_eq!(job.strategy, m.strategy);
    assert_eq!(job.request, m.request);
    assert_eq!(job.seed, 42);
}

#[test]
fn a_sweep_job_carries_the_sweep_kind_and_seed() {
    let m = manifest(ResolvedRequest::Sweep(ResolvedSweep {
        strategy: "sma".into(),
        params: json!({"params": {"fast": [1, 2, 1]}, "fitness": "sharpe"}),
        trials: 2,
        seed: 7,
        universe: "TCS.NSE".into(),
        start: ts(1),
        end: ts(2),
        bar_spec: "1d".into(),
        initial_capital: Decimal::from(1_000_000_i64),
    }));
    let job = RunJob::from_manifest(&m);
    assert_eq!(job.kind, RunKind::Sweep);
    assert_eq!(job.seed, 7);
}

#[test]
fn a_backtest_outcome_completes_a_running_manifest() {
    let mut m = manifest(backtest());
    m.start("2026-10-08T00:00:01Z").unwrap();
    m.complete(
        RunOutcome::Backtest {
            metrics: metrics(),
            assumptions: json!({"not_modelled": []}),
        },
        "2026-10-08T00:00:02Z",
    )
    .unwrap();
    assert_eq!(m.status, RunStatus::Completed);
    assert_eq!(m.metrics, Some(metrics()));
    assert_eq!(m.assumptions, Some(json!({"not_modelled": []})));
    assert_eq!(m.finished_at.as_deref(), Some("2026-10-08T00:00:02Z"));
}

#[test]
fn a_sweep_outcome_completes_a_running_sweep_manifest() {
    let mut m = manifest(ResolvedRequest::Sweep(ResolvedSweep {
        strategy: "sma".into(),
        params: json!({"params": {"fast": [1, 2, 1]}, "fitness": "sharpe"}),
        trials: 2,
        seed: 7,
        universe: "TCS.NSE".into(),
        start: ts(1),
        end: ts(2),
        bar_spec: "1d".into(),
        initial_capital: Decimal::from(1_000_000_i64),
    }));
    m.start("a").unwrap();
    let report = SweepReportResponse {
        ranked: vec![json!({"trial": 0})],
        best: None,
        validation: None,
    };
    m.complete(
        RunOutcome::Sweep {
            report: report.clone(),
        },
        "b",
    )
    .unwrap();
    assert_eq!(m.status, RunStatus::Completed);
    assert_eq!(m.report, Some(report));
}

#[test]
fn an_outcome_of_the_wrong_kind_is_refused_and_changes_nothing() {
    let mut m = manifest(backtest());
    m.start("a").unwrap();
    let err = m
        .complete(
            RunOutcome::Sweep {
                report: SweepReportResponse {
                    ranked: vec![],
                    best: None,
                    validation: None,
                },
            },
            "b",
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(m.status, RunStatus::Running);
    assert!(m.finished_at.is_none());
}

#[test]
fn completing_a_terminal_run_is_refused() {
    let mut m = manifest(backtest());
    m.cancel("a").unwrap();
    let err = m
        .complete(
            RunOutcome::Backtest {
                metrics: metrics(),
                assumptions: json!({}),
            },
            "b",
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(m.status, RunStatus::Cancelled);
    assert!(m.metrics.is_none());
}

/// An in-memory journal and an executor over it: the ports are object safe and usable
/// as `&mut dyn JournalWriter` / `dyn RunExecutor` from a plain thread.
#[derive(Default)]
struct Memory {
    records: Vec<Message>,
    flushes: usize,
}

impl JournalWriter for Memory {
    fn append(&mut self, msg: &Message) -> Result<(), ErrorDetail> {
        self.records.push(msg.clone());
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ErrorDetail> {
        self.flushes += 1;
        Ok(())
    }
}

struct OneQuote;

impl RunExecutor for OneQuote {
    fn execute(
        &self,
        job: RunJob,
        journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        let q = QuoteTick::new(
            InstrumentId::new(Exchange::Nse, "TCS"),
            Decimal::from(1_i64),
            Decimal::from(2_i64),
            Decimal::from(1_i64),
            Decimal::from(1_i64),
            ts(job.seed as i64),
            ts(job.seed as i64),
        );
        journal.append(&Message::new(Event::Quote(q), ts(1)))?;
        journal.flush()?;
        Ok(RunOutcome::Backtest {
            metrics: BacktestMetrics::default(),
            assumptions: json!({}),
        })
    }
}

#[test]
fn the_ports_are_object_safe_and_send() {
    fn assert_send<T: Send + ?Sized>() {}
    assert_send::<dyn JournalWriter>();
    let executor: std::sync::Arc<dyn RunExecutor> = std::sync::Arc::new(OneQuote);
    let job = RunJob::from_manifest(&manifest(backtest()));
    let mut journal = Memory::default();
    let handle = std::thread::spawn(move || {
        let out = executor.execute(job, &mut journal);
        (out, journal)
    });
    let (out, journal) = handle.join().unwrap();
    assert!(matches!(out, Ok(RunOutcome::Backtest { .. })));
    assert_eq!(journal.records.len(), 1);
    assert_eq!(journal.flushes, 1);
}
