//! Start-up recovery and retention against scratch journals roots on real disk.

mod support;

use honba_api::{BacktestMetrics, RunKind, RunStatus};
use honba_api_rest::RetentionPolicy;
use honba_messages::ErrorCode;
use serde_json::json;
use support::*;

fn metrics() -> BacktestMetrics {
    serde_json::from_value(json!({
        "total_return": 0.1, "sharpe": 1.0, "max_drawdown": 0.05, "trades": 3, "net_pnl": 100.0
    }))
    .unwrap()
}

fn finish(store: &honba_api_rest::RunStore, m: &honba_api::RunManifest) {
    store.transition(&m.run_id, |m, at| m.start(at)).unwrap();
    store
        .transition(&m.run_id, |m, at| {
            m.complete_backtest(metrics(), json!({}), at)
        })
        .unwrap();
}

#[test]
fn restart_recovers_interrupted() {
    let scratch = Scratch::new("restart");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let first = store(root.clone(), &clock);
    let pending = submit_backtest(&first, 1);
    let running = submit_backtest(&first, 2);
    first
        .transition(&running.run_id, |m, at| m.start(at))
        .unwrap();
    let done = submit_backtest(&first, 3);
    finish(&first, &done);
    let sweep = submit(&first, sweep_request(4));
    let mut j = first.open_journal(&running.run_id).unwrap();
    honba_api_rest::JournalWriter::append(&mut j, &quote(1)).unwrap();
    honba_api_rest::JournalWriter::flush(&mut j).unwrap();
    drop(j);
    let done_bytes = std::fs::read(run_dir(&root, &done).join("manifest.json")).unwrap();
    drop(first); // the "crash": nothing is closed gracefully

    clock.advance(3_600_000);
    let second = store(root.clone(), &clock);
    let report = second.recover();
    assert_eq!(report.loaded, 1);
    assert_eq!(report.skipped, 0);
    let mut interrupted = report.interrupted.clone();
    interrupted.sort();
    let mut expected = vec![
        pending.run_id.clone(),
        running.run_id.clone(),
        sweep.run_id.clone(),
    ];
    expected.sort();
    assert_eq!(interrupted, expected);

    for (m, kind) in [
        (&pending, RunKind::Backtest),
        (&running, RunKind::Backtest),
        (&sweep, RunKind::Sweep),
    ] {
        let got = second.load(m.run_id.as_str(), kind).unwrap();
        assert_eq!(got.status, RunStatus::Failed);
        assert_eq!(got.finished_at.as_deref(), Some("2026-10-08T01:00:00.000Z"));
        let err = got.error.unwrap();
        assert_eq!(err.code, ErrorCode::InternalError);
        assert_eq!(err.context.unwrap(), json!({"reason": "interrupted"}));
    }
    // Terminal manifests are untouched, byte for byte; journals keep what was flushed.
    assert_eq!(
        std::fs::read(run_dir(&root, &done).join("manifest.json")).unwrap(),
        done_bytes
    );
    assert_eq!(
        second
            .read_journal(running.run_id.as_str(), RunKind::Backtest)
            .unwrap(),
        vec![quote(1)]
    );
    // The closure is persisted: a third start-up loads three terminal runs, closes nothing,
    // and there is no marker file anywhere.
    drop(second);
    let third = store(root.clone(), &clock);
    let report = third.recover();
    assert_eq!((report.loaded, report.interrupted.len()), (4, 0));
    for entry in std::fs::read_dir(&root).unwrap() {
        let files: Vec<_> = std::fs::read_dir(entry.unwrap().path())
            .unwrap()
            .map(|f| f.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            files
                .iter()
                .all(|f| f == "manifest.json" || f == "events.ndjson"),
            "{files:?}"
        );
    }
}

#[test]
fn recovery_skips_what_it_does_not_understand_and_touches_nothing() {
    let scratch = Scratch::new("skip");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let first = store(root.clone(), &clock);
    let future = submit_backtest(&first, 1);
    let mismatch = submit_backtest(&first, 2);
    drop(first);

    // Unknown manifest_version (a running one: it must NOT be closed as failed).
    let path = run_dir(&root, &future).join("manifest.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    v["manifest_version"] = json!(99);
    v["status"] = json!("running");
    std::fs::write(&path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
    let future_bytes = std::fs::read(&path).unwrap();

    // A manifest whose run_id does not match its directory name.
    let other = run_dir(&root, &mismatch).join("manifest.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&other).unwrap()).unwrap();
    v["run_id"] = json!(future.run_id.as_str());
    std::fs::write(&other, serde_json::to_vec(&v).unwrap()).unwrap();
    let mismatch_bytes = std::fs::read(&other).unwrap();

    // Names that fail the id check, stray files, garbage manifest.
    std::fs::create_dir_all(root.join("not-an-id")).unwrap();
    std::fs::write(root.join("not-an-id/manifest.json"), b"{}").unwrap();
    std::fs::write(root.join("README"), b"hi").unwrap();
    let garbage = root.join("0000000000000000000000ZZZZ");
    std::fs::create_dir_all(&garbage).unwrap();
    std::fs::write(garbage.join("manifest.json"), b"not json").unwrap();

    let second = store(root.clone(), &clock);
    let report = second.recover();
    assert_eq!(report.loaded, 0);
    assert!(report.interrupted.is_empty());
    assert_eq!(report.skipped, 4); // future, mismatch, not-an-id, garbage (README is not a dir)
    assert!(second.list().is_empty());
    assert_eq!(
        second
            .load(future.run_id.as_str(), RunKind::Backtest)
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    assert_eq!(std::fs::read(&path).unwrap(), future_bytes);
    assert_eq!(std::fs::read(&other).unwrap(), mismatch_bytes);
    assert!(root.join("not-an-id/manifest.json").exists());
    assert_eq!(
        std::fs::read(garbage.join("manifest.json")).unwrap(),
        b"not json"
    );
}

#[test]
fn recovery_of_a_missing_root_is_empty() {
    let scratch = Scratch::new("noroot");
    let clock = FakeClock::at(T0);
    let s = store(scratch.journals(), &clock);
    let report = s.recover();
    assert_eq!(
        (report.loaded, report.skipped, report.interrupted.len()),
        (0, 0, 0)
    );
    assert!(
        !scratch.journals().exists(),
        "recovery must not create the root"
    );
}

#[test]
fn retention_runs_only_on_request_and_deletes_whole_directories() {
    let scratch = Scratch::new("retention");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let first = store(root.clone(), &clock);
    // Five completed runs, finished 50, 45, 40, 10 and 1 days before "now" (T0 + 50 days).
    let mut runs = Vec::new();
    for days_ago in [50u64, 45, 40, 10, 1] {
        clock.set(T0 + (50 - days_ago) * DAY);
        let m = submit_backtest(&first, days_ago);
        finish(&first, &m);
        runs.push(m);
    }
    // A pending run is never evicted however old.
    clock.set(T0);
    let pending = submit_backtest(&first, 99);
    drop(first);

    clock.set(T0 + 50 * DAY);
    let second = store(root.clone(), &clock);
    let report = second.recover();
    // recovery closes the pending run as failed (finished "now"); it is young.
    assert_eq!(report.interrupted, vec![pending.run_id.clone()]);
    assert_eq!(second.list().len(), 6, "recover must not evict");

    let policy = RetentionPolicy {
        keep_runs: 3,
        keep_days: 30,
    };
    // Newest 3 by finished_at: pending-turned-failed (now), 1d, 10d. Of the rest, 40d, 45d,
    // 50d are older than 30 days -> evicted.
    let mut evicted = second.apply_retention(&policy);
    evicted.sort();
    let mut expected: Vec<_> = runs[..3].iter().map(|m| m.run_id.clone()).collect();
    expected.sort();
    assert_eq!(evicted, expected);
    for m in &runs[..3] {
        assert!(!run_dir(&root, m).exists());
        assert_eq!(
            second
                .load(m.run_id.as_str(), RunKind::Backtest)
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
    }
    for m in &runs[3..] {
        assert!(run_dir(&root, m).exists());
        assert!(second.load(m.run_id.as_str(), RunKind::Backtest).is_ok());
    }
    assert_eq!(second.list().len(), 3);
}

#[test]
fn retention_keeps_old_runs_inside_the_count_and_young_runs_beyond_it() {
    let scratch = Scratch::new("retention-or");
    let clock = FakeClock::at(T0);
    let root = scratch.journals();
    let s = store(root.clone(), &clock);
    let old = submit_backtest(&s, 1);
    finish(&s, &old);
    clock.advance(DAY);
    let young = submit_backtest(&s, 2);
    finish(&s, &young);
    clock.set(T0 + 100 * DAY);
    let newest = submit_backtest(&s, 3);
    finish(&s, &newest);
    let live = submit_backtest(&s, 4); // pending, never evicted

    // Count rule alone protects: keep 3 newest of 3 terminal.
    assert!(s
        .apply_retention(&RetentionPolicy {
            keep_runs: 3,
            keep_days: 30
        })
        .is_empty());
    // keep_runs 1: old and young(101-day-old too) both beyond the count and older than 30d.
    let evicted = s.apply_retention(&RetentionPolicy {
        keep_runs: 1,
        keep_days: 30,
    });
    assert_eq!(evicted.len(), 2);
    assert!(s.load(newest.run_id.as_str(), RunKind::Backtest).is_ok());
    assert!(s.load(live.run_id.as_str(), RunKind::Backtest).is_ok());
}
