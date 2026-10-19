//! Shared fixtures for the run-store integration tests: scratch dirs under cargo's
//! `target/tmp` (never `/tmp`, never the repo's `data/`), a settable clock and a counting
//! entropy source.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use honba_api::{
    ResolvedBacktest, ResolvedRequest, ResolvedSweep, RunKind, RunManifest, StrategyCatalog,
    compile_strategy,
};
use honba_api_rest::{RunClock, RunEntropy, RunStore};
use honba_messages::{ErrorDetail, Event, Exchange, InstrumentId, Message, QuoteTick, UnixNanos};
use serde_json::json;

/// 2026-10-08T00:00:00Z.
pub const T0: u64 = 1_791_417_600_000;
pub const DAY: u64 = 86_400_000;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A scratch directory removed on drop.
pub struct Scratch(PathBuf);

impl Scratch {
    pub fn new(name: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "runs-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// The journals root inside the scratch dir.
    pub fn journals(&self) -> PathBuf {
        self.0.join("journals")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A clock the test moves by hand; clones share the time.
#[derive(Clone)]
pub struct FakeClock(Arc<AtomicU64>);

impl FakeClock {
    pub fn at(ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(ms)))
    }
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl RunClock for FakeClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Entropy that counts up, so ids are reproducible.
#[derive(Default)]
pub struct CountingEntropy(AtomicU64);

impl RunEntropy for CountingEntropy {
    fn next_bits(&self) -> Result<[u8; 10], ErrorDetail> {
        let n = self.0.fetch_add(1 << 20, Ordering::SeqCst);
        let mut bits = [0u8; 10];
        bits[2..].copy_from_slice(&n.to_be_bytes());
        Ok(bits)
    }
}

pub fn store(root: PathBuf, clock: &FakeClock) -> RunStore {
    RunStore::new(
        root,
        Box::new(clock.clone()),
        Box::new(CountingEntropy::default()),
    )
}

fn compiled() -> honba_api::CompiledStrategy {
    let tcs = json!({"symbol": "TCS", "exchange": "NSE"});
    let manifest = json!({
        "api_version": "1.0.0",
        "name": "sma",
        "source_hash": "abc",
        "universe": {"explicit": [tcs]},
        "subscriptions": {"instruments": [tcs]},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 20
    });
    compile_strategy(
        &mut StrategyCatalog::default(),
        serde_json::from_value(manifest).unwrap(),
    )
    .unwrap()
}

pub fn backtest_request(seed: u64) -> ResolvedRequest {
    ResolvedRequest::Backtest(ResolvedBacktest {
        strategy: "sma".into(),
        universe: "nifty50".into(),
        start: UnixNanos::from_u64(1),
        end: UnixNanos::from_u64(2),
        bar_spec: "1d".into(),
        initial_capital: 1_000_000.0,
        seed,
        fill_model: None,
        commission_per_fill: 0.0,
        slippage_bps: 0.0,
    })
}

pub fn sweep_request(seed: u64) -> ResolvedRequest {
    ResolvedRequest::Sweep(ResolvedSweep {
        strategy: "sma".into(),
        params: json!({"params": {"fast": [5, 10]}, "fitness": "sharpe"}),
        trials: 2,
        seed,
        universe: "TCS.NSE".into(),
        start: UnixNanos::from_u64(1),
        end: UnixNanos::from_u64(2),
        bar_spec: "1d".into(),
        initial_capital: 1_000_000.0,
    })
}

pub fn submit(store: &RunStore, request: ResolvedRequest) -> RunManifest {
    let c = compiled();
    store.submit(c.id, c.ir, request).unwrap()
}

pub fn submit_backtest(store: &RunStore, seed: u64) -> RunManifest {
    submit(store, backtest_request(seed))
}

pub fn quote(n: u64) -> Message {
    let q = QuoteTick::new(
        InstrumentId::new("TCS", Exchange::new("NSE")),
        100.0 + n as f64,
        101.0 + n as f64,
        10.0,
        20.0,
        UnixNanos::from_u64(n),
        UnixNanos::from_u64(n),
    );
    Message::new(Event::Quote(q), UnixNanos::from_u64(n + 1))
}

pub fn run_dir(root: &Path, m: &RunManifest) -> PathBuf {
    root.join(m.run_id.as_str())
}

pub fn kind_of(m: &RunManifest) -> RunKind {
    m.kind
}

// ---- run service fixtures ------------------------------------------------------------

use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use honba_api::{
    BacktestMetrics, BacktestRequest, JournalWriter, RunExecutor, RunId, RunJob, RunOutcome,
    RunStatus,
};
use honba_api_rest::{BacktestExecutor, RunService, RunServiceConfig, StrategyRegistry};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{BarAggregation, BarSpecification, PriceType};

/// 2024-01-01T00:00:00Z in nanoseconds.
const BARS_FROM_NS: u64 = 1_704_067_200_000_000_000;
const DAY_NS: u64 = 86_400_000_000_000;

/// 80 daily TCS bars from 2024-01-01 shaped to make the 3/8 SMA crossover trade.
pub fn fixture_reader() -> Arc<DatasetReader> {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let mut builder =
        ColumnarSliceBuilder::new(InstrumentId::new("TCS", Exchange::new("NSE")), spec);
    for i in 0..80u64 {
        let c = 100.0 + 12.0 * (i as f64 * 0.35).sin() + i as f64 * 0.05;
        let ts = UnixNanos::from_u64(BARS_FROM_NS + i * DAY_NS);
        builder
            .push(ts, c - 0.5, c + 1.0, c - 1.0, c, 1_000.0)
            .unwrap();
    }
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    Arc::new(DatasetReader::from_dataset(dataset))
}

pub fn sma_request(seed: u64) -> BacktestRequest {
    BacktestRequest {
        strategy: Some("sma_crossover".into()),
        universe: Some("TCS.NSE".into()),
        start: Some("2024-01-01".into()),
        end: Some("2024-06-01".into()),
        bar_spec: Some("1d".into()),
        initial_capital: Some(1_000_000.0),
        seed: Some(seed),
        fill_model: None,
        commission_per_fill: None,
        slippage_bps: None,
    }
}

pub fn real_executor() -> Arc<dyn RunExecutor> {
    Arc::new(BacktestExecutor::new(
        fixture_reader(),
        StrategyRegistry::builtin(),
    ))
}

pub fn config(max_concurrent: usize, max_queued: usize, grace_ms: u64) -> RunServiceConfig {
    RunServiceConfig {
        max_concurrent,
        max_queued,
        shutdown_grace: Duration::from_millis(grace_ms),
        ..RunServiceConfig::default()
    }
}

pub fn start_service(
    store: Arc<RunStore>,
    executor: Arc<dyn RunExecutor>,
    config: RunServiceConfig,
) -> RunService {
    RunService::start(
        store,
        executor,
        Arc::default(),
        StrategyRegistry::builtin(),
        config,
    )
}

/// Polls the run until it is terminal (30 s cap).
pub fn wait_terminal(store: &RunStore, id: &str) -> RunManifest {
    wait_for(store, id, |m| m.status.is_terminal())
}

pub fn wait_for(store: &RunStore, id: &str, done: impl Fn(&RunManifest) -> bool) -> RunManifest {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let m = store.load(id, RunKind::Backtest).unwrap();
        if done(&m) {
            return m;
        }
        assert!(
            Instant::now() < deadline,
            "run {id} stuck in {:?}",
            m.status
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// An executor that blocks until [`Gate::open`], then succeeds.
#[derive(Default)]
pub struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
    pub started: AtomicUsize,
}

impl Gate {
    pub fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

pub struct GatedExecutor(pub Arc<Gate>);

impl RunExecutor for GatedExecutor {
    fn execute(
        &self,
        _job: RunJob,
        journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        self.0.started.fetch_add(1, Ordering::SeqCst);
        journal.append(&quote(1))?;
        journal.flush()?;
        let guard = self.0.open.lock().unwrap();
        let (_guard, timeout) = self
            .0
            .cv
            .wait_timeout_while(guard, Duration::from_secs(30), |open| !*open)
            .unwrap();
        assert!(!timeout.timed_out(), "gate never opened");
        Ok(RunOutcome::Backtest {
            metrics: BacktestMetrics::default(),
            assumptions: json!({}),
        })
    }
}

pub fn wait_started(gate: &Gate, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while gate.started.load(Ordering::SeqCst) < n {
        assert!(Instant::now() < deadline, "workers never started");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// An executor that always fails or panics.
pub struct FailingExecutor(pub bool);

impl RunExecutor for FailingExecutor {
    fn execute(
        &self,
        _job: RunJob,
        _journal: &mut dyn JournalWriter,
    ) -> Result<RunOutcome, ErrorDetail> {
        if self.0 {
            panic!("boom at /secret/path");
        }
        Err(
            ErrorDetail::new(honba_messages::ErrorCode::MarketDataUnavailable, "no bars")
                .with_context(json!({"reason": "no_data"})),
        )
    }
}

pub fn run_status(store: &RunStore, id: &RunId) -> RunStatus {
    store.load(id.as_str(), RunKind::Backtest).unwrap().status
}

/// The shared test manifest, renamed by the caller.
pub fn tests_manifest(name: &str) -> honba_strategy::StrategyManifest {
    use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec, Universe};
    let tcs = InstrumentId::new("TCS", Exchange::new("NSE"));
    StrategyManifest::new(
        name,
        "sha256:abc",
        Universe::Explicit(vec![tcs.clone()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs],
        quotes: false,
        trades: false,
    })
}
