//! One trial: one engine, one strategy, one paper sink, synchronously.
//!
//! A trial is an ordinary function call. It builds everything it needs, runs the
//! synchronous kernel over the shared dataset, reads its own results back and
//! returns them. There is no async in this file and no shared engine: whatever
//! concurrency a sweep runs at, a trial owns its [`Engine`] outright.
//!
//! The wiring, in order:
//!
//! 1. a [`BarFillEngine`] paper sink, wrapped so the trial can read its fills
//!    back once the engine has taken ownership of the runner that holds the
//!    other half of that sink;
//! 2. the strategy, built from the trial's spec and seed;
//! 3. a [`StrategyRunner`] pairing the two, over a fresh ledger;
//! 4. the execution sink registered with the engine *before* the runner, so the
//!    price the runner fills at is the price of the bar being dispatched rather
//!    than the one before it;
//! 5. [`Engine::run`] over a [`DatasetFeed`] reading the shared dataset;
//! 6. metrics over the fills, and the kernel's audit trail, copied verbatim.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use honba_analytics::{AnalyticsError, EquityStats, RoundTrip, TradeStats};
use honba_engine::{Engine, EngineOutput, ExecutionEngine, Handler};
use honba_entities::{ExecutionEvent, Money, Trade};
use honba_feed::{Dataset, DatasetFeed};
use honba_messages::Event;
use honba_messages::{Order, UnixNanos};
use honba_sim::BarFillEngine;
use honba_strategy::{DynStrategy, LedgerContext, StrategyRunner};

use crate::error::{Result, SweepError};
use crate::plan::{SweepPlan, TrialParams};
use crate::report::{TrialMetrics, TrialReport};

/// The part of a [`SweepPlan`] a trial needs, copied out of it.
///
/// A task moved onto a blocking thread cannot borrow the caller's plan, and
/// copying two numbers is cheaper than cloning every trial in the plan.
#[derive(Clone, Copy, Debug)]
pub struct TrialConfig {
    initial_cash: Money,
    periods_per_year: f64,
}

impl TrialConfig {
    /// Copies the settings a trial reads out of `plan`.
    pub(crate) fn from_plan(plan: &SweepPlan) -> Self {
        Self {
            initial_cash: Money::new(plan.initial_cash(), plan.currency()),
            periods_per_year: plan.periods_per_year(),
        }
    }
}

/// Runs one trial synchronously over `data`.
///
/// The trial owns its engine, its strategy and its paper sink outright: it
/// builds them, runs them and reads them back, and nothing it builds outlives
/// the call. `ts_init` is taken from the data, never from the wall clock, and
/// nothing outside `data` and `plan` is read, so the same arguments always
/// produce the same [`TrialReport`].
///
/// A trial that fails returns [`SweepError::Trial`]. A trial that *panics*
/// propagates the panic: only the sweep, which knows which trial is running and
/// where to put the failure, turns a panic into a
/// [`TrialOutcome::Failed`](crate::TrialOutcome::Failed) outcome.
pub fn run_trial(
    trial_id: usize,
    params: &TrialParams,
    data: &Dataset,
    plan: &SweepPlan,
) -> Result<TrialReport> {
    plan.validate()?;
    run_trial_with(trial_id, params, data, TrialConfig::from_plan(plan))
}

/// Runs one trial from a copied [`TrialConfig`], without revalidating a plan.
///
/// A plan that could not produce a report cannot produce a trial either, so
/// [`run_trial`] validates before it gets here and the sweep validates once,
/// before it spawns anything.
pub fn run_trial_with(
    trial_id: usize,
    params: &TrialParams,
    data: &Dataset,
    config: TrialConfig,
) -> Result<TrialReport> {
    let tape: Arc<Mutex<Vec<Trade>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = TrialSink::new(Arc::clone(&tape));
    let strategy = DynStrategy::new(params.spec.build(params.seed));
    let runner = StrategyRunner::with_context(
        strategy,
        sink.clone(),
        LedgerContext::with_cash(config.initial_cash),
    );

    let mut engine = Engine::new();
    engine.add_handler(sink);
    engine.add_handler(runner);

    let mut feed = DatasetFeed::new(data, run_ts_init(data));
    engine.run(&mut feed).map_err(|error| SweepError::Trial {
        trial_id,
        reason: error.to_string(),
    })?;

    let fills = take_tape(&tape);
    let metrics = metrics(
        &fills,
        config.initial_cash.to_major_f64(),
        config.periods_per_year,
    )?;
    Ok(TrialReport {
        trial_id,
        params: params.clone(),
        metrics,
        audit: engine.audit().to_vec(),
    })
}

/// Runs a trial inside a task, turning a panic into a typed failure.
///
/// A panic carries no trial id, and a sweep has to say which trial died, so the
/// panic is caught here, inside the task, while the id is still in scope. A
/// panic elsewhere — in the runtime, or in code around the trial — still
/// surfaces as a join error and is the sweep's problem, not the trial's.
pub fn guard_trial(
    trial_id: usize,
    params: &TrialParams,
    data: &Dataset,
    config: TrialConfig,
) -> Result<TrialReport> {
    match std::panic::catch_unwind(AssertUnwindSafe(|| {
        run_trial_with(trial_id, params, data, config)
    })) {
        Ok(result) => result,
        Err(payload) => Err(SweepError::Trial {
            trial_id,
            reason: panic_reason(payload),
        }),
    }
}

/// The run's `ts_init`: the first bar's `ts_event` in dataset order, or zero
/// for an empty dataset.
///
/// Taken from the data and nothing else, so every trial of every sweep over the
/// same dataset stamps the same `ts_init` on every message it replays.
fn run_ts_init(data: &Dataset) -> UnixNanos {
    data.slices()
        .first()
        .and_then(|slice| slice.timestamps().first())
        .copied()
        .unwrap_or_else(|| UnixNanos::from_u64(0))
}

fn metrics(fills: &[Trade], initial_cash: f64, periods_per_year: f64) -> Result<TrialMetrics> {
    let round_trips = round_trips(fills);
    let returns = returns(&round_trips, initial_cash);
    let analytics = |error: AnalyticsError| SweepError::Analytics(error.to_string());

    let trades = if round_trips.is_empty() {
        None
    } else {
        Some(TradeStats::from_round_trips(&round_trips).map_err(analytics)?)
    };
    let equity = if returns.is_empty() {
        None
    } else {
        Some(EquityStats::from_returns(&returns, periods_per_year, 0.0).map_err(analytics)?)
    };

    Ok(TrialMetrics {
        fills: fills.len(),
        round_trips: round_trips.len(),
        trades,
        equity,
    })
}

/// Pairs the trial's fills into round trips, two at a time, in fill order.
///
/// A strategy that opens and then closes a position produces the pair. Two
/// fills on the same side, or on two different instruments, cannot be a round
/// trip — [`RoundTrip::from_fills`] refuses them — and are dropped rather than
/// paired up by guesswork.
fn round_trips(fills: &[Trade]) -> Vec<RoundTrip> {
    let mut trips = Vec::with_capacity(fills.len() / 2);
    for pair in fills.chunks(2) {
        let [entry, exit] = pair else {
            continue;
        };
        if let Ok(trip) = RoundTrip::from_fills(entry, exit) {
            trips.push(trip);
        }
    }
    trips
}

/// The trial's per-round-trip returns.
///
/// The equity curve starts at the plan's initial cash and moves by each round
/// trip's net PnL; the returns are its differences, with the same rule
/// [`EquityStats::from_equity_curve`] uses for a curve that reaches zero. A
/// trial with no round trips has no returns at all, which is why
/// [`TrialMetrics::equity`] is an `Option` rather than a zero.
fn returns(trips: &[RoundTrip], initial_cash: f64) -> Vec<f64> {
    let mut equity = Vec::with_capacity(trips.len() + 1);
    let mut current = initial_cash;
    equity.push(current);
    for trip in trips {
        current += trip.net_pnl;
        equity.push(current);
    }
    equity
        .windows(2)
        .map(|pair| {
            if pair[0] != 0.0 {
                (pair[1] - pair[0]) / pair[0]
            } else {
                0.0
            }
        })
        .collect()
}

fn panic_reason(payload: Box<dyn Any + Send + 'static>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "the trial panicked".to_string(),
        },
    }
}

fn take_tape(tape: &Mutex<Vec<Trade>>) -> Vec<Trade> {
    std::mem::take(&mut *tape.lock().expect("the fill tape is not poisoned"))
}

/// The trial's paper sink: a [`BarFillEngine`] that tees every drained fill
/// into a tape the trial can read back.
///
/// The engine takes ownership of the handler it is given, so by the time the run
/// is over the runner that drains the fills has gone into it. The sink is
/// cloned: one half observes bars as a handler, the other fills orders as the
/// runner's execution port, and both halves share the same price and the same
/// buffer. The tape is shared because it crosses that ownership boundary; the
/// lock it takes guards a vector of results, never an engine.
#[derive(Clone)]
pub(crate) struct TrialSink {
    inner: BarFillEngine,
    tape: Arc<Mutex<Vec<Trade>>>,
}

impl TrialSink {
    pub(crate) fn new(tape: Arc<Mutex<Vec<Trade>>>) -> Self {
        Self {
            inner: BarFillEngine::new(),
            tape,
        }
    }
}

impl Handler for TrialSink {
    fn on_start(&mut self) -> honba_engine::Result<()> {
        self.inner.on_start()
    }

    fn on_event(
        &mut self,
        event: &Event,
        ts_init: UnixNanos,
    ) -> honba_engine::Result<EngineOutput> {
        self.inner.on_event(event, ts_init)
    }

    fn on_stop(&mut self) -> honba_engine::Result<()> {
        self.inner.on_stop()
    }
}

impl ExecutionEngine for TrialSink {
    fn submit(&mut self, order: Order) -> honba_engine::Result<()> {
        self.inner.submit(order)
    }

    fn cancel(
        &mut self,
        order_id: &str,
        now: honba_messages::UnixNanos,
    ) -> honba_engine::Result<()> {
        self.inner.cancel(order_id, now)
    }

    fn drain_events(&mut self) -> honba_engine::Result<Vec<ExecutionEvent>> {
        let events = self.inner.drain_events()?;
        self.tape
            .lock()
            .expect("the fill tape is not poisoned")
            .extend(events.iter().filter_map(|e| match e {
                ExecutionEvent::Fill { trade, .. } => Some(trade.clone()),
                _ => None,
            }));
        Ok(events)
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> honba_engine::Result<Vec<Trade>> {
        let fills = self.inner.drain_fills()?;
        self.tape
            .lock()
            .expect("the fill tape is not poisoned")
            .extend(fills.iter().cloned());
        Ok(fills)
    }
}
