//! The run service: admission, the bounded queue and the worker pool (ADR 0017 decisions 4
//! and 5). Every method blocks; the route layer calls them through `spawn_blocking`.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use honba_api::{
    unknown_strategy, BacktestRequest, BacktestResponse, JournalWriter, ResolvedRequest,
    RunExecutor, RunId, RunJob, StrategyCatalog, SweepRequest,
};
use honba_messages::{ErrorCode, ErrorDetail};
use honba_strategy::StrategyIr;

use serde_json::json;

use crate::executor::parse_universe;
use crate::registry::StrategyRegistry;
use crate::retention::RetentionPolicy;
use crate::run_store::{RecoveryReport, RunStore};

/// Default bound on pending runs.
pub const DEFAULT_MAX_QUEUED: usize = 64;

/// Default grace period for running runs at shutdown.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Tunables of a [`RunService`].
#[derive(Clone, Debug)]
pub struct RunServiceConfig {
    /// Worker threads, one run each (default: available parallelism).
    pub max_concurrent: usize,
    /// Most pending runs the queue holds (default 64).
    pub max_queued: usize,
    /// How long shutdown waits for running runs (default 10 s).
    pub shutdown_grace: Duration,
    /// Start-up retention.
    pub retention: RetentionPolicy,
}

impl Default for RunServiceConfig {
    fn default() -> Self {
        Self {
            max_concurrent: std::thread::available_parallelism().map_or(1, usize::from),
            max_queued: DEFAULT_MAX_QUEUED,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            retention: RetentionPolicy::default(),
        }
    }
}

/// What [`RunService::shutdown`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Pending runs cancelled before they started.
    pub cancelled_pending: usize,
    /// Runs still running after the grace period, cancelled.
    pub cancelled_running: usize,
}

/// Resolves `strategy` for a backtest over `universe` (ADR 0017 decision 6).
pub(crate) fn resolve_strategy(
    catalog: &Mutex<StrategyCatalog>,
    registry: &StrategyRegistry,
    strategy: &str,
    universe: &str,
    bar_spec: &str,
) -> Result<(String, StrategyIr), ErrorDetail> {
    let instrument = parse_universe(universe)?;
    let compiled = catalog
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(strategy)
        .cloned();
    if let Some(compiled) = compiled {
        // A compiled manifest runs only if a Rust strategy carries its name (decision 6).
        if !registry.contains(&compiled.ir.manifest.name) {
            return Err(ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "strategy has no registered Rust implementation",
            )
            .with_context(json!({"field": "strategy", "reason": "no_rust_implementation"})));
        }
        return Ok((compiled.id, compiled.ir));
    }
    if registry.contains(strategy) {
        let ir = registry.ir_for(strategy, &instrument, bar_spec)?;
        return Ok((strategy.to_owned(), ir));
    }
    Err(unknown_strategy(strategy))
}

struct Queue {
    items: VecDeque<RunId>,
    closed: bool,
    /// Runs a worker has taken off the queue and not finished.
    active: usize,
}

struct Shared {
    queue: Mutex<Queue>,
    /// Signalled when work arrives or the queue closes.
    work: Condvar,
    /// Signalled when a worker finishes a run.
    idle: Condvar,
    store: Arc<RunStore>,
    executor: Arc<dyn RunExecutor>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Admission, queue and workers over a [`RunStore`].
///
/// `max_concurrent` plain threads drain one bounded FIFO queue; each runs one job at a time
/// through the [`RunExecutor`]. The kernel is synchronous and cannot be interrupted, so a
/// run that outlives the shutdown grace is marked `cancelled` and its thread is left to
/// finish on its own (its late transition is refused by the store); cooperative stop is
/// deferred (ADR 0017 decision 7). Dropping the service shuts it down with no grace.
pub struct RunService {
    shared: Arc<Shared>,
    strategies: Arc<Mutex<StrategyCatalog>>,
    registry: StrategyRegistry,
    max_queued: usize,
    grace: Duration,
    report: RecoveryReport,
    evicted: Vec<RunId>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    stopped: AtomicBool,
}

fn shutting_down() -> ErrorDetail {
    ErrorDetail::new(ErrorCode::RateLimited, "server is shutting down")
        .with_context(json!({"reason": "shutting_down"}))
}

impl RunService {
    /// Recovers the store, applies retention (in that order, once) and starts the workers.
    pub fn start(
        store: Arc<RunStore>,
        executor: Arc<dyn RunExecutor>,
        strategies: Arc<Mutex<StrategyCatalog>>,
        registry: StrategyRegistry,
        config: RunServiceConfig,
    ) -> Self {
        let report = store.recover();
        let evicted = store.apply_retention(&config.retention);
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                closed: false,
                active: 0,
            }),
            work: Condvar::new(),
            idle: Condvar::new(),
            store,
            executor,
        });
        let workers = (0..config.max_concurrent.max(1))
            .filter_map(|n| {
                let shared = Arc::clone(&shared);
                std::thread::Builder::new()
                    .name(format!("honba-run-{n}"))
                    .spawn(move || worker(&shared))
                    .map_err(|e| tracing::error!(error = %e, "cannot start a run worker"))
                    .ok()
            })
            .collect();
        Self {
            shared,
            strategies,
            registry,
            max_queued: config.max_queued,
            grace: config.shutdown_grace,
            report,
            evicted,
            workers: Mutex::new(workers),
            stopped: AtomicBool::new(false),
        }
    }

    /// What start-up recovery did.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.report
    }

    /// The runs start-up retention evicted.
    pub fn evicted(&self) -> &[RunId] {
        &self.evicted
    }

    /// The run store.
    pub fn store(&self) -> &Arc<RunStore> {
        &self.shared.store
    }

    /// `POST /backtests`: resolve and validate, check the queue, write the pending manifest
    /// and enqueue. A refusal at any step leaves nothing on disk and consumes no id.
    ///
    /// Errors: 422 `validation_invalid_request` from the resolver or the strategy lookup;
    /// 429 `rate_limited` with `reason` `run_queue_full` (and `max_queued`) or
    /// `shutting_down`.
    pub fn submit_backtest(
        &self,
        request: &BacktestRequest,
    ) -> Result<BacktestResponse, ErrorDetail> {
        let resolved = request.resolve()?;
        let (strategy_id, ir) = resolve_strategy(
            &self.strategies,
            &self.registry,
            &resolved.strategy,
            &resolved.universe,
            &resolved.bar_spec,
        )?;
        // The check and the push share one lock, so the bound cannot be overshot.
        let mut queue = lock(&self.shared.queue);
        if queue.closed {
            return Err(shutting_down());
        }
        if queue.items.len() >= self.max_queued {
            return Err(
                ErrorDetail::new(ErrorCode::RateLimited, "the run queue is full").with_context(
                    json!({
                        "reason": "run_queue_full",
                        "max_queued": self.max_queued
                    }),
                ),
            );
        }
        let manifest =
            self.shared
                .store
                .submit(strategy_id, ir, ResolvedRequest::Backtest(resolved))?;
        queue.items.push_back(manifest.run_id.clone());
        drop(queue);
        self.shared.work.notify_one();
        manifest.to_backtest_response().ok_or_else(|| {
            ErrorDetail::new(ErrorCode::InternalError, "admitted run is not a backtest")
                .with_context(json!({"reason": "run_kind"}))
        })
    }

    /// `POST /sweeps`: resolve and validate, check the queue, write the pending manifest
    /// and enqueue. A refusal at any step leaves nothing on disk and consumes no id.
    ///
    /// Errors: 422 `validation_invalid_request` from the resolver or the strategy lookup;
    /// 429 `rate_limited` with `reason` `run_queue_full` (and `max_queued`) or
    /// `shutting_down`.
    pub fn submit_sweep(
        &self,
        request: &SweepRequest,
    ) -> Result<honba_api::SweepResponse, ErrorDetail> {
        let resolved = request.resolve()?;
        let (strategy_id, ir) = resolve_strategy(
            &self.strategies,
            &self.registry,
            &resolved.strategy,
            &resolved.universe,
            &resolved.bar_spec,
        )?;
        // The check and the push share one lock, so the bound cannot be overshot.
        let mut queue = lock(&self.shared.queue);
        if queue.closed {
            return Err(shutting_down());
        }
        if queue.items.len() >= self.max_queued {
            return Err(
                ErrorDetail::new(ErrorCode::RateLimited, "the run queue is full").with_context(
                    json!({
                        "reason": "run_queue_full",
                        "max_queued": self.max_queued
                    }),
                ),
            );
        }
        let manifest =
            self.shared
                .store
                .submit(strategy_id, ir, ResolvedRequest::Sweep(resolved))?;
        queue.items.push_back(manifest.run_id.clone());
        drop(queue);
        self.shared.work.notify_one();
        manifest.to_sweep_response().ok_or_else(|| {
            ErrorDetail::new(ErrorCode::InternalError, "admitted run is not a sweep")
                .with_context(json!({"reason": "run_kind"}))
        })
    }

    /// Graceful shutdown (ADR 0017 decision 5): stop admitting, cancel every pending run,
    /// wait up to the grace period for running runs to finish on their own, then cancel
    /// those still running. Idempotent: later calls return an empty report.
    pub fn shutdown(&self) -> ShutdownReport {
        self.stop(self.grace)
    }

    fn stop(&self, grace: Duration) -> ShutdownReport {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return ShutdownReport::default();
        }
        let cancelled_pending = self.shared.store.begin_shutdown();
        {
            let mut queue = lock(&self.shared.queue);
            queue.closed = true;
            queue.items.clear();
        }
        self.shared.work.notify_all();

        let deadline = Instant::now() + grace;
        {
            let mut queue = lock(&self.shared.queue);
            while queue.active > 0 {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                queue = self
                    .shared
                    .idle
                    .wait_timeout(queue, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        }
        let cancelled_running = self.shared.store.cancel_running();

        // Idle workers exit now; a worker stuck in a run is detached, not joined.
        let mut workers = std::mem::take(&mut *lock(&self.workers));
        let give_up = Instant::now() + Duration::from_millis(200);
        while !workers.is_empty() && Instant::now() < give_up {
            let (done, rest): (Vec<_>, Vec<_>) =
                workers.into_iter().partition(JoinHandle::is_finished);
            for handle in done {
                let _ = handle.join();
            }
            workers = rest;
            if !workers.is_empty() {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        ShutdownReport {
            cancelled_pending,
            cancelled_running,
        }
    }
}

impl Drop for RunService {
    fn drop(&mut self) {
        self.stop(Duration::ZERO);
    }
}

fn worker(shared: &Shared) {
    loop {
        let id = {
            let mut queue = lock(&shared.queue);
            loop {
                if let Some(id) = queue.items.pop_front() {
                    queue.active += 1;
                    break Some(id);
                }
                if queue.closed {
                    break None;
                }
                queue = shared
                    .work
                    .wait(queue)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        let Some(id) = id else { return };
        run_one(shared, &id);
        lock(&shared.queue).active -= 1;
        shared.idle.notify_all();
    }
}

/// `pending -> running`, execute, then `completed` or `failed`. Every transition goes through
/// the store, which refuses a change to a run shutdown already cancelled.
fn run_one(shared: &Shared, id: &RunId) {
    let Ok(started) = shared.store.transition(id, |m, at| m.start(at)) else {
        return; // cancelled while queued
    };
    let job = RunJob::from_manifest(&started);
    let result = match shared.store.open_journal(id) {
        Err(e) => Err(e),
        Ok(mut journal) => {
            let executed = catch_unwind(AssertUnwindSafe(|| {
                shared.executor.execute(job, &mut journal)
            }));
            let flushed = journal.flush();
            match executed {
                Ok(Ok(outcome)) => flushed.map(|()| outcome),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(
                    ErrorDetail::new(ErrorCode::InternalError, "the run panicked")
                        .with_context(json!({"reason": "panic"})),
                ),
            }
        }
    };
    let applied = shared.store.transition(id, |m, at| match result {
        Ok(outcome) => m.complete(outcome, at),
        Err(error) => m.fail(error, at),
    });
    if let Err(e) = applied {
        tracing::warn!(run_id = %id, error = %e, "run result discarded; the run is already final");
    }
}
