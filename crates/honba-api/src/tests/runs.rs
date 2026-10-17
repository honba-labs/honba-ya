//! ADR 0017 decision 1 and 3: the run state machine and the run manifest.

use serde_json::json;

use crate::{
    compile_strategy, BacktestMetrics, ErrorCode, ErrorDetail, ResolvedBacktest, ResolvedRequest,
    ResolvedSweep, RunEvent, RunId, RunIdGenerator, RunKind, RunManifest, RunStatus,
    StrategyCatalog, MANIFEST_VERSION,
};
use honba_messages::UnixNanos;

use super::tests_manifest;

const ALL_STATUSES: [RunStatus; 5] = [
    RunStatus::Pending,
    RunStatus::Running,
    RunStatus::Completed,
    RunStatus::Failed,
    RunStatus::Cancelled,
];
const ALL_EVENTS: [RunEvent; 4] = [
    RunEvent::Start,
    RunEvent::Complete,
    RunEvent::Fail,
    RunEvent::Cancel,
];

#[test]
fn run_status_transition_table() {
    use RunEvent::{Cancel, Complete, Fail, Start};
    use RunStatus::{Cancelled, Completed, Failed, Pending, Running};
    // (status, event, expected next). One literal covers every cell, legal and illegal.
    let table: [(RunStatus, RunEvent, Option<RunStatus>); 20] = [
        (Pending, Start, Some(Running)),
        (Pending, Complete, None),
        (Pending, Fail, None),
        (Pending, Cancel, Some(Cancelled)),
        (Running, Start, None),
        (Running, Complete, Some(Completed)),
        (Running, Fail, Some(Failed)),
        (Running, Cancel, Some(Cancelled)),
        (Completed, Start, None),
        (Completed, Complete, None),
        (Completed, Fail, None),
        (Completed, Cancel, None),
        (Failed, Start, None),
        (Failed, Complete, None),
        (Failed, Fail, None),
        (Failed, Cancel, None),
        (Cancelled, Start, None),
        (Cancelled, Complete, None),
        (Cancelled, Fail, None),
        (Cancelled, Cancel, None),
    ];
    assert_eq!(table.len(), ALL_STATUSES.len() * ALL_EVENTS.len());
    for (status, event, expected) in table {
        assert_eq!(status.next(event).ok(), expected, "{status:?} + {event:?}");
    }
}

#[test]
fn terminal_is_final() {
    for status in ALL_STATUSES {
        let terminal = matches!(
            status,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        );
        assert_eq!(status.is_terminal(), terminal, "{status:?}");
        if terminal {
            for event in ALL_EVENTS {
                assert!(status.next(event).is_err(), "{status:?} + {event:?}");
            }
            for to in ALL_STATUSES {
                assert!(!status.can_transition_to(to), "{status:?} -> {to:?}");
            }
        }
    }
}

#[test]
fn pending_to_cancelled_legal() {
    assert!(RunStatus::Pending.can_transition_to(RunStatus::Cancelled));
    assert!(RunStatus::Pending.can_transition_to(RunStatus::Running));
    assert!(!RunStatus::Pending.can_transition_to(RunStatus::Completed));
    assert!(!RunStatus::Pending.can_transition_to(RunStatus::Failed));
    assert!(RunStatus::Running.can_transition_to(RunStatus::Cancelled));
}

#[test]
fn cancelled_serializes_in_snake_case_and_other_spellings_are_unchanged() {
    assert_eq!(
        serde_json::to_value(RunStatus::Cancelled).unwrap(),
        json!("cancelled")
    );
    for (status, text) in [
        (RunStatus::Pending, "pending"),
        (RunStatus::Running, "running"),
        (RunStatus::Completed, "completed"),
        (RunStatus::Failed, "failed"),
    ] {
        assert_eq!(serde_json::to_value(status).unwrap(), json!(text));
    }
}

fn run_id() -> RunId {
    RunIdGenerator::default().next(1, [0; 10]).unwrap()
}

fn backtest_manifest() -> RunManifest {
    let mut catalog = StrategyCatalog::default();
    let compiled = compile_strategy(&mut catalog, tests_manifest()).unwrap();
    RunManifest::new_pending(
        run_id(),
        compiled.id,
        compiled.ir,
        ResolvedRequest::Backtest(ResolvedBacktest {
            strategy: "sma".into(),
            universe: "nifty50".into(),
            start: crate::market::parse_bound("start", "2024-01-01").unwrap(),
            end: crate::market::parse_bound("end", "2025-01-01").unwrap(),
            bar_spec: "1d".into(),
            initial_capital: 1_000_000.0,
            seed: 42,
            fill_model: None,
            commission_per_fill: 0.0,
            slippage_bps: 0.0,
        }),
        "2026-10-08T00:00:00Z",
    )
}

#[test]
fn a_new_manifest_is_pending_and_versioned() {
    let m = backtest_manifest();
    assert_eq!(MANIFEST_VERSION, 1);
    assert_eq!(m.manifest_version, MANIFEST_VERSION);
    assert_eq!(m.kind, RunKind::Backtest);
    assert_eq!(m.status, RunStatus::Pending);
    assert_eq!(m.seed, 42);
    assert_eq!(m.schema_version, honba_messages::SCHEMA_VERSION);
    assert!(m.started_at.is_none() && m.finished_at.is_none());
}

#[test]
fn a_manifest_pins_inputs_and_leaks_no_paths() {
    let value = serde_json::to_value(backtest_manifest()).unwrap();
    assert_eq!(value["manifest_version"], json!(1));
    assert_eq!(value["kind"], json!("backtest"));
    assert_eq!(value["request"]["bar_spec"], json!("1d"));
    assert_eq!(value["request"]["initial_capital"], json!(1_000_000.0));
    assert_eq!(value["seed"], json!(42));
    assert!(value["strategy_id"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let text = value.to_string();
    for forbidden in ["data_root", "journals", "catalog", "/home", "/tmp"] {
        assert!(!text.contains(forbidden), "manifest mentions {forbidden}");
    }
}

#[test]
fn a_manifest_round_trips_through_json() {
    let mut m = backtest_manifest();
    m.start("2026-10-08T00:00:01Z").unwrap();
    let back: RunManifest = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
    assert_eq!(back, m);
}

#[test]
fn manifest_transitions_stamp_times_and_refuse_leaving_terminal() {
    let mut m = backtest_manifest();
    m.start("t1").unwrap();
    assert_eq!(
        (m.status, m.started_at.as_deref()),
        (RunStatus::Running, Some("t1"))
    );
    m.complete_backtest(
        BacktestMetrics::default(),
        json!({"not_modelled": []}),
        "t2",
    )
    .unwrap();
    assert_eq!(m.status, RunStatus::Completed);
    assert_eq!(m.finished_at.as_deref(), Some("t2"));
    // A late cancel is refused and does not rewrite the finish time.
    assert!(m.cancel("t3").is_err());
    assert_eq!(m.status, RunStatus::Completed);
    assert_eq!(m.finished_at.as_deref(), Some("t2"));
}

#[test]
fn response_projection_follows_the_adr_rules() {
    // metrics only when completed, assumptions only when terminal, error only when failed.
    let mut pending = backtest_manifest();
    let r = pending.to_backtest_response().unwrap();
    assert_eq!(r.status, RunStatus::Pending);
    assert!(r.metrics.is_none() && r.assumptions.is_none() && r.error.is_none());

    pending.start("t1").unwrap();
    let mut failed = pending.clone();
    failed
        .fail(ErrorDetail::new(ErrorCode::InternalError, "boom"), "t2")
        .unwrap();
    let r = failed.to_backtest_response().unwrap();
    assert_eq!(r.status, RunStatus::Failed);
    assert_eq!(r.error.unwrap().code, ErrorCode::InternalError);
    assert!(r.metrics.is_none());

    pending
        .complete_backtest(BacktestMetrics::default(), json!({"k": 1}), "t2")
        .unwrap();
    let r = pending.to_backtest_response().unwrap();
    assert_eq!(r.run_id, pending.run_id.to_string());
    assert!(r.metrics.is_some() && r.error.is_none());
    assert_eq!(r.assumptions, Some(json!({"k": 1})));
    assert!(pending.to_sweep_response().is_none(), "kind mismatch");
}

#[test]
fn a_sweep_manifest_projects_a_sweep_response() {
    let mut catalog = StrategyCatalog::default();
    let compiled = compile_strategy(&mut catalog, tests_manifest()).unwrap();
    let mut m = RunManifest::new_pending(
        run_id(),
        compiled.id,
        compiled.ir,
        ResolvedRequest::Sweep(ResolvedSweep {
            strategy: "sma".into(),
            params: json!({"params": {"fast": [5, 20, 5]}, "fitness": "sharpe"}),
            trials: 10,
            seed: 7,
            universe: "TCS.NSE".into(),
            start: UnixNanos::from_u64(1),
            end: UnixNanos::from_u64(2),
            bar_spec: "1d".into(),
            initial_capital: 1_000_000.0,
        }),
        "t0",
    );
    assert_eq!(m.kind, RunKind::Sweep);
    m.start("t1").unwrap();
    m.fail(ErrorDetail::new(ErrorCode::InternalError, "x"), "t2")
        .unwrap();
    let r = m.to_sweep_response().unwrap();
    assert_eq!(r.job_id, m.run_id.to_string());
    assert!(r.error.is_some());
    assert!(m.to_backtest_response().is_none());
}
