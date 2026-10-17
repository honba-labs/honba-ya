//! `RunStore` against real files under `target/`: submit, record, reload, refuse, shut down.

mod support;

use honba_api::{BacktestMetrics, RunKind, RunStatus};
use honba_api_rest::JournalWriter;
use honba_messages::{ErrorCode, SCHEMA_VERSION};
use serde_json::json;
use support::*;

fn metrics() -> BacktestMetrics {
    serde_json::from_value(json!({
        "total_return": 0.1, "sharpe": 1.0, "max_drawdown": 0.05, "trades": 3, "net_pnl": 100.0
    }))
    .unwrap()
}

#[test]
fn submit_writes_a_pending_manifest_and_an_empty_journal() {
    let scratch = Scratch::new("submit");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let m = submit_backtest(&store, 42);

    assert_eq!(m.status, RunStatus::Pending);
    assert_eq!(m.kind, RunKind::Backtest);
    assert_eq!(m.created_at, "2026-10-08T00:00:00.000Z");
    let dir = run_dir(&scratch.journals(), &m);
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    // No temp file survives the atomic write.
    assert_eq!(names, ["events.ndjson", "manifest.json"]);
    assert_eq!(
        std::fs::metadata(dir.join("events.ndjson")).unwrap().len(),
        0
    );
    let on_disk: honba_api::RunManifest =
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(on_disk, m);
}

#[test]
fn ids_are_distinct_and_increase_in_issue_order() {
    let scratch = Scratch::new("ids");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let a = submit_backtest(&store, 1);
    let b = submit_backtest(&store, 1); // same seed, same millisecond
    clock.set(T0 - 5_000); // wall clock steps back
    let c = submit_backtest(&store, 1);
    assert!(a.run_id < b.run_id && b.run_id < c.run_id);
}

#[test]
fn submit_record_reload_is_equal() {
    let scratch = Scratch::new("reload");
    let clock = FakeClock::at(T0);
    let first = store(scratch.journals(), &clock);
    let m = submit_backtest(&first, 7);
    let id = m.run_id.clone();
    first.transition(&id, |m, at| m.start(at)).unwrap();
    clock.advance(1_000);
    let done = first
        .transition(&id, |m, at| {
            m.complete_backtest(metrics(), json!({"not_modelled": []}), at)
        })
        .unwrap();
    assert_eq!(done.status, RunStatus::Completed);
    assert_eq!(
        done.finished_at.as_deref(),
        Some("2026-10-08T00:00:01.000Z")
    );
    assert_eq!(first.load(id.as_str(), RunKind::Backtest).unwrap(), done);
    drop(first);

    let second = store(scratch.journals(), &clock);
    let report = second.recover();
    assert_eq!(report.loaded, 1);
    assert!(report.interrupted.is_empty());
    assert_eq!(second.load(id.as_str(), RunKind::Backtest).unwrap(), done);
    assert_eq!(second.list(), vec![done]);
}

#[test]
fn a_refused_transition_changes_nothing_on_disk() {
    let scratch = Scratch::new("refused");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let m = submit_backtest(&store, 3);
    let path = run_dir(&scratch.journals(), &m).join("manifest.json");
    let before = std::fs::read(&path).unwrap();
    // pending -> completed is illegal.
    let err = store
        .transition(&m.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        store
            .load(m.run_id.as_str(), RunKind::Backtest)
            .unwrap()
            .status,
        RunStatus::Pending
    );
}

#[test]
fn a_sweep_id_is_not_a_backtest_and_vice_versa() {
    let scratch = Scratch::new("kinds");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let bt = submit_backtest(&store, 1);
    let sw = submit(&store, sweep_request(1));
    assert_eq!(sw.kind, RunKind::Sweep);

    assert!(store.load(bt.run_id.as_str(), RunKind::Backtest).is_ok());
    assert!(store.load(sw.run_id.as_str(), RunKind::Sweep).is_ok());
    for (id, wrong) in [(&bt, RunKind::Sweep), (&sw, RunKind::Backtest)] {
        let err = store.load(id.run_id.as_str(), wrong).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        let err = store.read_journal(id.run_id.as_str(), wrong).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }
    // /journals/{id} sees both kinds.
    assert_eq!(
        store.load_any(sw.run_id.as_str()).unwrap().kind,
        RunKind::Sweep
    );
    assert_eq!(
        store.load_any(bt.run_id.as_str()).unwrap().kind,
        RunKind::Backtest
    );
}

#[test]
fn traversal_ids_never_reach_the_filesystem() {
    let scratch = Scratch::new("traversal");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let store = store(root.clone(), &clock);
    let real = submit_backtest(&store, 1);

    // A decoy run directory outside the journals root, as a sibling.
    let decoy = scratch.path().join("outside");
    std::fs::create_dir_all(&decoy).unwrap();
    std::fs::copy(
        run_dir(&root, &real).join("manifest.json"),
        decoy.join("manifest.json"),
    )
    .unwrap();
    std::fs::write(decoy.join("events.ndjson"), b"").unwrap();

    let lower = real.run_id.as_str().to_lowercase();
    let bad = [
        "../outside".to_owned(),
        "..%2Foutside".to_owned(),
        "..%2F..%2Fetc".to_owned(),
        "%2e%2e%2foutside".to_owned(),
        "%2e%2e".to_owned(),
        "..".to_owned(),
        ".".to_owned(),
        format!("{}/../../outside", real.run_id),
        format!("{}\0", real.run_id),
        lower,
        real.run_id.as_str()[..25].to_owned(),
        format!("{}0", real.run_id),
    ];
    for id in &bad {
        for kind in [RunKind::Backtest, RunKind::Sweep] {
            assert_eq!(
                store.load(id, kind).unwrap_err().code,
                ErrorCode::NotFound,
                "{id:?}"
            );
            assert_eq!(
                store.read_journal(id, kind).unwrap_err().code,
                ErrorCode::NotFound,
                "{id:?}"
            );
        }
        assert_eq!(
            store.load_any(id).unwrap_err().code,
            ErrorCode::NotFound,
            "{id:?}"
        );
    }
}

#[test]
fn errors_leak_no_path() {
    let scratch = Scratch::new("leak");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let m = submit_backtest(&store, 1);
    let root = scratch.path().to_string_lossy().into_owned();
    let mut errors = vec![
        store.load("../x", RunKind::Backtest).unwrap_err(),
        store
            .load("0000000000000000000000ABCD", RunKind::Backtest)
            .unwrap_err(),
        store.load(m.run_id.as_str(), RunKind::Sweep).unwrap_err(),
    ];
    // Make the journal unreadable so the read path errors, then check that error too.
    std::fs::remove_file(run_dir(&scratch.journals(), &m).join("events.ndjson")).unwrap();
    errors.push(
        store
            .read_journal(m.run_id.as_str(), RunKind::Backtest)
            .unwrap_err(),
    );
    for e in errors {
        let text = serde_json::to_string(&e).unwrap();
        assert!(!text.contains(&root), "{text}");
    }
}

#[test]
fn identical_messages_give_byte_identical_journals() {
    let scratch = Scratch::new("bytes");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let a = submit_backtest(&store, 9);
    let b = submit_backtest(&store, 9);
    assert_ne!(a.run_id, b.run_id);
    for id in [&a.run_id, &b.run_id] {
        let mut j = store.open_journal(id).unwrap();
        for n in 1..=5 {
            j.append(&quote(n)).unwrap();
        }
        j.flush().unwrap();
    }
    let read = |m: &honba_api::RunManifest| {
        std::fs::read(run_dir(&scratch.journals(), m).join("events.ndjson")).unwrap()
    };
    let bytes = read(&a);
    assert!(!bytes.is_empty());
    assert_eq!(bytes, read(&b));
    // The body names neither run id nor path nor wall time.
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains(a.run_id.as_str()) && !text.contains(b.run_id.as_str()));
    assert!(!text.contains("journals") && !text.contains("2026"));
    assert_eq!(text.lines().count(), 5);
}

#[test]
fn journal_reads_are_a_prefix_of_complete_records() {
    let scratch = Scratch::new("prefix");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let m = submit_backtest(&store, 1);
    let id = m.run_id.as_str();
    // Pending, nothing written yet: empty, not an error.
    assert!(store
        .read_journal(id, RunKind::Backtest)
        .unwrap()
        .is_empty());

    store.transition(&m.run_id, |m, at| m.start(at)).unwrap();
    let mut j = store.open_journal(&m.run_id).unwrap();
    j.append(&quote(1)).unwrap();
    j.append(&quote(2)).unwrap();
    j.flush().unwrap();
    assert_eq!(
        store.read_journal(id, RunKind::Backtest).unwrap(),
        vec![quote(1), quote(2)]
    );

    // A torn tail (a record being written) is ignored.
    use std::io::Write;
    let path = run_dir(&scratch.journals(), &m).join("events.ndjson");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    f.write_all(b"{\"schema_version\":4,\"ev").unwrap();
    assert_eq!(store.read_journal(id, RunKind::Backtest).unwrap().len(), 2);

    // Once the record completes it appears, and the earlier answer was its prefix.
    let full = serde_json::to_string(&quote(3)).unwrap();
    let mut tail = std::fs::read_to_string(&path).unwrap();
    tail.truncate(tail.rfind("{\"schema_version\":4,\"ev").unwrap());
    std::fs::write(&path, format!("{tail}{full}\n")).unwrap();
    assert_eq!(
        store.read_journal(id, RunKind::Backtest).unwrap(),
        vec![quote(1), quote(2), quote(3)]
    );
}

#[test]
fn a_journal_at_another_schema_version_is_unsupported() {
    let scratch = Scratch::new("schema");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let m = submit_backtest(&store, 1);
    let mut line = serde_json::to_value(quote(1)).unwrap();
    line["schema_version"] = json!(SCHEMA_VERSION - 1);
    std::fs::write(
        run_dir(&scratch.journals(), &m).join("events.ndjson"),
        format!("{line}\n"),
    )
    .unwrap();
    let err = store
        .read_journal(m.run_id.as_str(), RunKind::Backtest)
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unsupported);
    assert_eq!(
        err.context.unwrap(),
        json!({"found": SCHEMA_VERSION - 1, "expected": SCHEMA_VERSION})
    );
}

#[test]
fn shutdown_cancels_every_non_terminal_run_and_stops_admitting() {
    let scratch = Scratch::new("shutdown");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let pending = submit_backtest(&store, 1);
    let running = submit_backtest(&store, 2);
    store
        .transition(&running.run_id, |m, at| m.start(at))
        .unwrap();
    let done = submit_backtest(&store, 3);
    store.transition(&done.run_id, |m, at| m.start(at)).unwrap();
    let done = store
        .transition(&done.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .unwrap();
    // The cancelled run keeps the records already flushed.
    let mut j = store.open_journal(&running.run_id).unwrap();
    j.append(&quote(1)).unwrap();
    j.flush().unwrap();

    clock.advance(5_000);
    assert_eq!(store.shutdown(), 2);

    for id in [&pending.run_id, &running.run_id] {
        let m = store.load(id.as_str(), RunKind::Backtest).unwrap();
        assert_eq!(m.status, RunStatus::Cancelled);
        assert_eq!(m.finished_at.as_deref(), Some("2026-10-08T00:00:05.000Z"));
    }
    assert_eq!(
        store.load(done.run_id.as_str(), RunKind::Backtest).unwrap(),
        done
    );
    assert_eq!(
        store
            .read_journal(running.run_id.as_str(), RunKind::Backtest)
            .unwrap(),
        vec![quote(1)]
    );
    // Persisted, not only cached.
    let reloaded = self::store(scratch.journals(), &clock);
    reloaded.recover();
    assert_eq!(
        reloaded
            .load(pending.run_id.as_str(), RunKind::Backtest)
            .unwrap()
            .status,
        RunStatus::Cancelled
    );

    // A worker finishing late cannot overwrite the cancellation.
    let err = store
        .transition(&running.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(
        store
            .load(running.run_id.as_str(), RunKind::Backtest)
            .unwrap()
            .status,
        RunStatus::Cancelled
    );

    // No new admissions: 429 rate_limited / shutting_down, and nothing written.
    let before = std::fs::read_dir(scratch.journals()).unwrap().count();
    let shared = {
        let other = self::store(scratch.path().join("other"), &clock);
        submit_backtest(&other, 1)
    };
    let err = store
        .submit(
            shared.strategy_id.clone(),
            shared.strategy.clone(),
            shared.request.clone(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::RateLimited);
    assert_eq!(err.context.unwrap()["reason"], "shutting_down");
    assert_eq!(
        std::fs::read_dir(scratch.journals()).unwrap().count(),
        before
    );
}

#[test]
fn staged_shutdown_cancels_pending_first_and_running_after_the_grace() {
    let scratch = Scratch::new("staged-shutdown");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let pending = submit_backtest(&store, 1);
    let running = submit_backtest(&store, 2);
    store
        .transition(&running.run_id, |m, at| m.start(at))
        .unwrap();

    // Phase one: admission stops, pending is cancelled, running is untouched.
    assert_eq!(store.begin_shutdown(), 1);
    let status = |id: &honba_api::RunId| store.load(id.as_str(), RunKind::Backtest).unwrap().status;
    assert_eq!(status(&pending.run_id), RunStatus::Cancelled);
    assert_eq!(status(&running.run_id), RunStatus::Running);
    // A run that finishes within the grace completes normally.
    store
        .transition(&running.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .unwrap();
    assert_eq!(status(&running.run_id), RunStatus::Completed);

    // Nothing is left running, so phase two cancels nothing.
    assert_eq!(store.cancel_running(), 0);
}

#[test]
fn cancel_running_cancels_a_run_that_outlived_the_grace() {
    let scratch = Scratch::new("cancel-running");
    let clock = FakeClock::at(T0);
    let store = store(scratch.journals(), &clock);
    let running = submit_backtest(&store, 2);
    store
        .transition(&running.run_id, |m, at| m.start(at))
        .unwrap();
    assert_eq!(store.begin_shutdown(), 0);
    clock.advance(10_000);
    assert_eq!(store.cancel_running(), 1);
    let m = store
        .load(running.run_id.as_str(), RunKind::Backtest)
        .unwrap();
    assert_eq!(m.status, RunStatus::Cancelled);
    assert_eq!(m.finished_at.as_deref(), Some("2026-10-08T00:00:10.000Z"));
    // The late worker cannot overwrite it.
    assert!(store
        .transition(&running.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .is_err());
}
