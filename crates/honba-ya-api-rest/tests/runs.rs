//! The run service end to end: admission, queue, workers, shutdown and restart, over real
//! journal directories under `target/tmp` and the real backtest kernel.

mod support;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use honba_api::{BacktestRequest, RunKind, RunStatus};
use honba_api_rest::{RetentionPolicy, RunService};
use honba_messages::{ErrorCode, Event};
use serde_json::json;
use support::*;

fn service_at(scratch: &Scratch, config: honba_api_rest::RunServiceConfig) -> RunService {
    let clock = FakeClock::at(T0);
    start_service(
        Arc::new(store(scratch.journals(), &clock)),
        real_executor(),
        config,
    )
}

#[test]
fn submit_poll_journal() {
    let scratch = Scratch::new("svc-submit");
    let service = service_at(&scratch, config(2, 8, 1_000));
    let response = service.submit_backtest(&sma_request(42)).unwrap();
    assert_eq!(response.status, RunStatus::Pending);
    assert!(response.metrics.is_none());

    let done = wait_terminal(service.store(), &response.run_id);
    assert_eq!(done.status, RunStatus::Completed, "{:?}", done.error);
    let body = done.to_backtest_response().unwrap();
    let metrics = body.metrics.unwrap();
    assert!(metrics.trades >= 1);
    assert!(body.assumptions.unwrap()["not_modelled"].is_array());
    assert!(body.error.is_none());
    assert!(done.started_at.is_some() && done.finished_at.is_some());

    // The journal is on disk, in ADR 0019 vocabulary, readable through the store.
    let records = service
        .store()
        .read_journal(&response.run_id, RunKind::Backtest)
        .unwrap();
    assert!(records.iter().any(|m| matches!(m.event(), Event::Bar(_))));
    assert!(records.iter().any(|m| matches!(m.event(), Event::Order(_))));
    assert!(records
        .iter()
        .any(|m| matches!(m.event(), Event::OrderFilled { .. })));
    let manifest_on_disk = std::fs::read_to_string(
        scratch
            .journals()
            .join(&response.run_id)
            .join("manifest.json"),
    )
    .unwrap();
    assert!(manifest_on_disk.contains("\"completed\""));
    service.shutdown();
}

#[test]
fn responses_leak_no_paths() {
    let scratch = Scratch::new("svc-leak");
    let service = service_at(&scratch, config(1, 8, 1_000));
    let ok = service.submit_backtest(&sma_request(1)).unwrap();
    let mut bad_window = sma_request(2);
    bad_window.start = Some("2030-01-01".into());
    bad_window.end = Some("2030-02-01".into());
    let failed = service.submit_backtest(&bad_window).unwrap();
    let panicked = {
        let s = service_with_failing(&scratch, true);
        let r = s.submit_backtest(&sma_request(3)).unwrap();
        let m = wait_terminal(s.store(), &r.run_id);
        s.shutdown();
        m
    };
    for id in [&ok.run_id, &failed.run_id] {
        wait_terminal(service.store(), id);
    }
    let root = scratch.path().to_string_lossy().into_owned();
    let mut bodies = vec![
        serde_json::to_string(
            &service
                .store()
                .load(&ok.run_id, RunKind::Backtest)
                .unwrap()
                .to_backtest_response(),
        )
        .unwrap(),
        serde_json::to_string(
            &service
                .store()
                .load(&failed.run_id, RunKind::Backtest)
                .unwrap()
                .to_backtest_response(),
        )
        .unwrap(),
        serde_json::to_string(&panicked.to_backtest_response()).unwrap(),
    ];
    let err = service
        .submit_backtest(&BacktestRequest::default())
        .unwrap_err();
    bodies.push(serde_json::to_string(&err).unwrap());
    for body in bodies {
        assert!(!body.contains(&root), "{body}");
        assert!(!body.contains("/secret/path"), "{body}");
        assert!(!body.contains("journals"), "{body}");
    }
    service.shutdown();
}

fn service_with_failing(scratch: &Scratch, panic: bool) -> RunService {
    let clock = FakeClock::at(T0);
    start_service(
        Arc::new(store(scratch.path().join("failing"), &clock)),
        Arc::new(FailingExecutor(panic)),
        config(1, 8, 500),
    )
}

#[test]
fn unknown_strategy_is_422_at_admission_and_leaves_nothing() {
    let scratch = Scratch::new("svc-422");
    let service = service_at(&scratch, config(1, 8, 500));
    for strategy in ["nope", "sha256:deadbeef"] {
        let mut request = sma_request(1);
        request.strategy = Some(strategy.into());
        let err = service.submit_backtest(&request).unwrap_err();
        assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
        assert_eq!(err.context.unwrap()["field"], "strategy");
    }
    // A resolver failure is 422 too, and also leaves nothing.
    let err = service
        .submit_backtest(&BacktestRequest::default())
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert!(service.store().list().is_empty());
    assert!(
        !scratch.journals().exists(),
        "a refused request must not create the journals root"
    );
    service.shutdown();
}

#[test]
fn a_catalog_strategy_without_a_rust_implementation_is_422() {
    let scratch = Scratch::new("svc-catalog");
    let clock = FakeClock::at(T0);
    let catalog = Arc::new(std::sync::Mutex::new(honba_api::StrategyCatalog::default()));
    let compiled = {
        let mut manifest = tests_manifest("py_only");
        manifest.name = "py_only".into();
        honba_api::compile_strategy(&mut catalog.lock().unwrap(), manifest).unwrap()
    };
    let service = RunService::start(
        Arc::new(store(scratch.journals(), &clock)),
        real_executor(),
        catalog,
        honba_api_rest::StrategyRegistry::builtin(),
        config(1, 8, 500),
    );
    let mut request = sma_request(1);
    request.strategy = Some(compiled.id);
    let err = service.submit_backtest(&request).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(err.context.unwrap()["reason"], "no_rust_implementation");
    assert!(service.store().list().is_empty());
    service.shutdown();
}

#[test]
fn queue_full_is_429_and_writes_nothing() {
    let scratch = Scratch::new("svc-queue");
    let gate = Arc::new(Gate::default());
    let clock = FakeClock::at(T0);
    let service = start_service(
        Arc::new(store(scratch.journals(), &clock)),
        Arc::new(GatedExecutor(gate.clone())),
        config(1, 1, 500),
    );
    let first = service.submit_backtest(&sma_request(1)).unwrap();
    wait_started(&gate, 1); // the worker holds it: running, not queued
    let second = service.submit_backtest(&sma_request(2)).unwrap();
    assert_eq!(second.status, RunStatus::Pending);

    let err = service.submit_backtest(&sma_request(3)).unwrap_err();
    assert_eq!(err.code, ErrorCode::RateLimited);
    assert!(err.retryable);
    assert_eq!(
        err.context.unwrap(),
        json!({"reason": "run_queue_full", "max_queued": 1})
    );
    assert_eq!(
        service.store().list().len(),
        2,
        "the refused run left nothing"
    );
    assert_eq!(std::fs::read_dir(scratch.journals()).unwrap().count(), 2);

    gate.open();
    for id in [&first.run_id, &second.run_id] {
        assert_eq!(
            wait_terminal(service.store(), id).status,
            RunStatus::Completed
        );
    }
    // Space again: the queue drained.
    let third = service.submit_backtest(&sma_request(3)).unwrap();
    assert_eq!(
        wait_terminal(service.store(), &third.run_id).status,
        RunStatus::Completed
    );
    service.shutdown();
}

#[test]
fn at_most_max_concurrent_runs_execute_at_once() {
    let scratch = Scratch::new("svc-concurrency");
    let gate = Arc::new(Gate::default());
    let clock = FakeClock::at(T0);
    let service = start_service(
        Arc::new(store(scratch.journals(), &clock)),
        Arc::new(GatedExecutor(gate.clone())),
        config(2, 8, 500),
    );
    let ids: Vec<String> = (1..=5)
        .map(|seed| service.submit_backtest(&sma_request(seed)).unwrap().run_id)
        .collect();
    wait_started(&gate, 2);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        gate.started.load(Ordering::SeqCst),
        2,
        "a third run started"
    );
    let running = ids
        .iter()
        .filter(|id| run_status(service.store(), &id.parse().unwrap()) == RunStatus::Running)
        .count();
    assert_eq!(running, 2);
    gate.open();
    for id in &ids {
        assert_eq!(
            wait_terminal(service.store(), id).status,
            RunStatus::Completed
        );
    }
    service.shutdown();
}

#[test]
fn an_executor_failure_is_a_failed_run_with_its_error() {
    let scratch = Scratch::new("svc-fail");
    let service = service_with_failing(&scratch, false);
    let response = service.submit_backtest(&sma_request(1)).unwrap();
    let done = wait_terminal(service.store(), &response.run_id);
    assert_eq!(done.status, RunStatus::Failed);
    let body = done.to_backtest_response().unwrap();
    assert!(body.metrics.is_none());
    let error = body.error.unwrap();
    assert_eq!(error.code, ErrorCode::MarketDataUnavailable);
    assert_eq!(error.context.unwrap()["reason"], "no_data");
    service.shutdown();
}

#[test]
fn an_executor_panic_is_a_failed_run_and_the_worker_survives() {
    let scratch = Scratch::new("svc-panic");
    let service = service_with_failing(&scratch, true);
    let first = service.submit_backtest(&sma_request(1)).unwrap();
    let second = service.submit_backtest(&sma_request(2)).unwrap();
    for id in [&first.run_id, &second.run_id] {
        let done = wait_terminal(service.store(), id);
        assert_eq!(done.status, RunStatus::Failed);
        let error = done.error.unwrap();
        assert_eq!(error.code, ErrorCode::InternalError);
        assert_eq!(error.context.unwrap()["reason"], "panic");
        assert!(!error.message.contains("/secret/path"));
    }
    service.shutdown();
}

#[test]
fn a_real_failure_surfaces_on_the_run() {
    let scratch = Scratch::new("svc-nodata");
    let service = service_at(&scratch, config(1, 8, 500));
    let mut request = sma_request(1);
    request.start = Some("2030-01-01".into());
    request.end = Some("2030-02-01".into());
    let response = service.submit_backtest(&request).unwrap();
    let done = wait_terminal(service.store(), &response.run_id);
    assert_eq!(done.status, RunStatus::Failed);
    assert_eq!(done.error.unwrap().code, ErrorCode::MarketDataUnavailable);
    service.shutdown();
}

#[test]
fn shutdown_cancels_pending_and_a_running_run_that_outlives_the_grace() {
    let scratch = Scratch::new("svc-shutdown");
    let gate = Arc::new(Gate::default());
    let clock = FakeClock::at(T0);
    let service = start_service(
        Arc::new(store(scratch.journals(), &clock)),
        Arc::new(GatedExecutor(gate.clone())),
        config(1, 8, 200),
    );
    let running = service.submit_backtest(&sma_request(1)).unwrap();
    wait_started(&gate, 1);
    let pending = service.submit_backtest(&sma_request(2)).unwrap();

    let began = Instant::now();
    let report = service.shutdown();
    let took = began.elapsed();
    assert_eq!(report.cancelled_pending, 1);
    assert_eq!(report.cancelled_running, 1);
    assert!(
        took >= Duration::from_millis(200),
        "returned before the grace: {took:?}"
    );
    assert!(took < Duration::from_secs(10), "{took:?}");

    for id in [&running.run_id, &pending.run_id] {
        let m = service.store().load(id, RunKind::Backtest).unwrap();
        assert_eq!(m.status, RunStatus::Cancelled);
        assert!(m.finished_at.is_some());
    }
    // The cancelled run keeps the records already flushed.
    assert_eq!(
        service
            .store()
            .read_journal(&running.run_id, RunKind::Backtest)
            .unwrap()
            .len(),
        1
    );
    // The late worker finishing cannot overwrite the cancellation, and the pending run
    // never starts.
    gate.open();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    for id in [&running.run_id, &pending.run_id] {
        let m = service.store().load(id, RunKind::Backtest).unwrap();
        assert_eq!(m.status, RunStatus::Cancelled);
        assert!(m.metrics.is_none());
    }
    // Admission is closed.
    let err = service.submit_backtest(&sma_request(9)).unwrap_err();
    assert_eq!(err.code, ErrorCode::RateLimited);
    assert_eq!(err.context.unwrap()["reason"], "shutting_down");
    // Idempotent.
    assert_eq!(service.shutdown(), Default::default());
}

#[test]
fn shutdown_lets_a_run_finish_within_the_grace() {
    let scratch = Scratch::new("svc-grace");
    let gate = Arc::new(Gate::default());
    let clock = FakeClock::at(T0);
    let service = start_service(
        Arc::new(store(scratch.journals(), &clock)),
        Arc::new(GatedExecutor(gate.clone())),
        config(1, 8, 20_000),
    );
    let running = service.submit_backtest(&sma_request(1)).unwrap();
    wait_started(&gate, 1);
    let opener = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            gate.open();
        })
    };
    let began = Instant::now();
    let report = service.shutdown();
    opener.join().unwrap();
    assert!(
        began.elapsed() < Duration::from_secs(10),
        "waited the whole grace"
    );
    assert_eq!(report.cancelled_running, 0);
    let m = service
        .store()
        .load(&running.run_id, RunKind::Backtest)
        .unwrap();
    assert_eq!(m.status, RunStatus::Completed);
}

#[test]
fn start_recovers_interrupted_runs_then_applies_retention() {
    let scratch = Scratch::new("svc-restart");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let first = store(root.clone(), &clock);
    let finish = |seed: u64| {
        let m = submit_backtest(&first, seed);
        first.transition(&m.run_id, |m, at| m.start(at)).unwrap();
        first
            .transition(&m.run_id, |m, at| {
                m.complete_backtest(Default::default(), json!({}), at)
            })
            .unwrap();
        m
    };
    let old = finish(1);
    clock.advance(3_600_000);
    let recent = finish(2);
    let running = submit_backtest(&first, 3);
    first
        .transition(&running.run_id, |m, at| m.start(at))
        .unwrap();
    let pending = submit_backtest(&first, 4);
    drop(first); // crash

    clock.advance(2 * 86_400_000);
    let mut config = config(1, 8, 200);
    config.retention = RetentionPolicy {
        keep_runs: 2,
        keep_days: 1,
    };
    let service = start_service(
        Arc::new(store(root.clone(), &clock)),
        real_executor(),
        config,
    );
    let report = service.recovery();
    assert_eq!(report.loaded, 2);
    assert_eq!(report.interrupted.len(), 2);

    // Recovery ran before retention: the two interrupted runs are terminal and newest, so
    // they fill keep_runs=2; `old` and `recent` are beyond the count and over a day old.
    for gone in [&old, &recent] {
        assert_eq!(
            service
                .store()
                .load(gone.run_id.as_str(), RunKind::Backtest)
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        assert!(!run_dir(&root, gone).exists());
    }
    for kept in [&running, &pending] {
        let m = service
            .store()
            .load(kept.run_id.as_str(), RunKind::Backtest)
            .unwrap();
        assert_eq!(m.status, RunStatus::Failed);
        assert_eq!(m.error.unwrap().context.unwrap()["reason"], "interrupted");
    }
    // And the restarted service runs new work.
    let response = service.submit_backtest(&sma_request(5)).unwrap();
    assert_eq!(
        wait_terminal(service.store(), &response.run_id).status,
        RunStatus::Completed
    );
    service.shutdown();
}
