//! ADR 0020 / E4-S1: Backtest result cache keyed by hash(manifest, params, dataset id, cost-model version).
//!
//! Verifies:
//! - First run misses cache, executes, writes journal records, and stores result in cache.
//! - Second run with identical parameters hits cache, returns identical metrics, and emits 0 new journal events.
//! - Parameter change (seed, capital) invalidates cache key and re-executes.
//! - Disk-backed cache persists across executor restarts.
//! - End-to-end `RunService` integration with caching.

mod support;

use std::sync::Arc;

use honba_api::{JournalWriter, ResultCache, RunExecutor, RunJob, RunKind, RunOutcome, RunStatus};
use honba_api_rest::{BacktestExecutor, StrategyRegistry};
use honba_messages::{Exchange, InstrumentId, Message, UnixNanos};
use support::*;

/// A simple in-memory journal writer to verify record emission count.
#[derive(Default)]
struct TestJournal {
    records: Vec<Message>,
}

impl JournalWriter for TestJournal {
    fn append(&mut self, record: &Message) -> Result<(), honba_messages::ErrorDetail> {
        self.records.push(record.clone());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), honba_messages::ErrorDetail> {
        Ok(())
    }
}

fn job_with(strategy: &str, universe: &str, bar_spec: &str, seed: u64, capital: f64) -> RunJob {
    let tcs = InstrumentId::new("TCS", Exchange::new("NSE"));
    let ir = StrategyRegistry::builtin()
        .ir_for(strategy, &tcs, "1d")
        .unwrap();
    RunJob {
        run_id: honba_api::RunIdGenerator::default()
            .next(1, [0; 10])
            .unwrap(),
        kind: RunKind::Backtest,
        strategy_id: strategy.to_owned(),
        strategy: ir,
        request: honba_api::ResolvedRequest::Backtest(honba_api::ResolvedBacktest {
            strategy: strategy.to_owned(),
            universe: universe.to_owned(),
            start: UnixNanos::from_u64(1_704_067_200_000_000_000),
            end: UnixNanos::from_u64(1_704_067_200_000_000_000 + 80 * 86_400_000_000_000),
            bar_spec: bar_spec.to_owned(),
            initial_capital: capital,
            seed,
            fill_model: None,
            commission_per_fill: 0.0,
            slippage_bps: 0.0,
        }),
        seed,
    }
}

#[test]
fn rerun_hits_cache() {
    let cache = ResultCache::in_memory();
    let executor = BacktestExecutor::new(fixture_reader(), StrategyRegistry::builtin())
        .with_cache(cache.clone());

    let job1 = job_with("sma_crossover", "TCS.NSE", "1d", 42, 1_000_000.0);
    let mut journal1 = TestJournal::default();

    // Run 1: cache miss, executes kernel and populates cache
    let outcome1 = executor
        .execute(job1.clone(), &mut journal1)
        .expect("run 1 should succeed");
    let metrics1 = match &outcome1 {
        RunOutcome::Backtest { metrics, .. } => metrics.clone(),
        _ => panic!("expected backtest outcome"),
    };

    assert!(
        journal1.records.len() > 10,
        "run 1 should have dispatched events to journal, got {}",
        journal1.records.len()
    );
    assert_eq!(cache.len(), 1, "cache should now hold 1 result");

    // Run 2: identical parameters -> cache HIT
    let mut journal2 = TestJournal::default();
    let outcome2 = executor
        .execute(job1.clone(), &mut journal2)
        .expect("run 2 should succeed from cache");
    let metrics2 = match &outcome2 {
        RunOutcome::Backtest { metrics, .. } => metrics.clone(),
        _ => panic!("expected backtest outcome"),
    };

    // Metrics are identical
    assert_eq!(metrics1, metrics2);
    // Journal received 0 new records because execution was bypassed!
    assert_eq!(
        journal2.records.len(),
        0,
        "cache hit should bypass simulation and write 0 events to journal"
    );

    // Run 3: parameter change (different seed) -> cache MISS
    let job3 = job_with("sma_crossover", "TCS.NSE", "1d", 99, 1_000_000.0);
    let mut journal3 = TestJournal::default();
    let _outcome3 = executor
        .execute(job3, &mut journal3)
        .expect("run 3 should succeed");
    assert!(
        journal3.records.len() > 10,
        "run 3 with different seed should execute and emit records"
    );
    assert_eq!(cache.len(), 2, "cache should now hold 2 results");

    // Run 4: parameter change (different capital) -> cache MISS
    let job4 = job_with("sma_crossover", "TCS.NSE", "1d", 42, 500_000.0);
    let mut journal4 = TestJournal::default();
    let outcome4 = executor
        .execute(job4, &mut journal4)
        .expect("run 4 should succeed");
    let metrics4 = match &outcome4 {
        RunOutcome::Backtest { metrics, .. } => metrics.clone(),
        _ => panic!("expected backtest outcome"),
    };
    assert_ne!(
        metrics1.total_return, metrics4.total_return,
        "total_return should differ with different capital"
    );
    assert!(
        journal4.records.len() > 10,
        "run 4 with different capital should execute"
    );
    assert_eq!(cache.len(), 3, "cache should now hold 3 results");
}

#[test]
fn disk_cache_persistence_across_restarts() {
    let scratch = Scratch::new("disk-cache-test");
    let cache_dir = scratch.path().join("cache");

    let job = job_with("sma_crossover", "TCS.NSE", "1d", 42, 1_000_000.0);

    // Instance 1: write to disk cache
    let cache1 = ResultCache::with_disk_dir(&cache_dir);
    let executor1 =
        BacktestExecutor::new(fixture_reader(), StrategyRegistry::builtin()).with_cache(cache1);

    let mut journal1 = TestJournal::default();
    let outcome1 = executor1.execute(job.clone(), &mut journal1).unwrap();
    let metrics1 = match outcome1 {
        RunOutcome::Backtest { metrics, .. } => metrics,
        _ => panic!("expected backtest outcome"),
    };
    assert!(!journal1.records.is_empty());

    // Instance 2: simulated restart, fresh ResultCache pointing at same disk dir
    let cache2 = ResultCache::with_disk_dir(&cache_dir);
    assert_eq!(cache2.len(), 1, "disk cache should recover cached item");
    let executor2 =
        BacktestExecutor::new(fixture_reader(), StrategyRegistry::builtin()).with_cache(cache2);

    let mut journal2 = TestJournal::default();
    let outcome2 = executor2.execute(job, &mut journal2).unwrap();
    let metrics2 = match outcome2 {
        RunOutcome::Backtest { metrics, .. } => metrics,
        _ => panic!("expected backtest outcome"),
    };

    assert_eq!(metrics1, metrics2);
    assert_eq!(
        journal2.records.len(),
        0,
        "second run should hit persisted disk cache with 0 journal records"
    );
}

#[test]
fn rerun_hits_cache_end_to_end_service() {
    let scratch = Scratch::new("svc-cache");
    let clock = FakeClock::at(T0);
    let store_arc = Arc::new(store(scratch.journals(), &clock));
    let cache = ResultCache::in_memory();
    let executor = Arc::new(
        BacktestExecutor::new(fixture_reader(), StrategyRegistry::builtin())
            .with_cache(cache.clone()),
    );
    let service = start_service(store_arc, executor, config(2, 8, 1_000));

    // Submit first run
    let req1 = sma_request(42);
    let resp1 = service.submit_backtest(&req1).unwrap();
    let done1 = wait_terminal(service.store(), &resp1.run_id);
    assert_eq!(done1.status, RunStatus::Completed);
    let body1 = done1.to_backtest_response().unwrap();
    let metrics1 = body1.metrics.unwrap();

    let journal_path1 = scratch.journals().join(&resp1.run_id).join("events.ndjson");
    let size1 = std::fs::metadata(&journal_path1).unwrap().len();
    assert!(size1 > 0, "run 1 journal should have events");

    // Submit rerun with identical request
    let req2 = sma_request(42);
    let resp2 = service.submit_backtest(&req2).unwrap();
    let done2 = wait_terminal(service.store(), &resp2.run_id);
    assert_eq!(done2.status, RunStatus::Completed);
    let body2 = done2.to_backtest_response().unwrap();
    let metrics2 = body2.metrics.unwrap();

    // Metrics are identical
    assert_eq!(metrics1, metrics2);

    // Run 2's journal file was not populated with simulated events because it hit cache
    let journal_path2 = scratch.journals().join(&resp2.run_id).join("events.ndjson");
    let size2 = std::fs::metadata(&journal_path2).unwrap().len();
    assert_eq!(size2, 0, "cached run should write 0 events to its journal");

    service.shutdown();
}
