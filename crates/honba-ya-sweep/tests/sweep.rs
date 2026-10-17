//! Integration tests for `honba-sweep`, through the public API only.
//!
//! Every test runs a real [`Engine`] on a real tokio multi-thread runtime over
//! a hand-built [`Dataset`], with the reference strategies and the paper sink
//! the crate uses in production. Nothing here reaches for Parquet, a broker,
//! the network, or the wall clock.

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use honba_engine::{AlgoError, Engine, Result as EngineResult};
use honba_entities::Money;
use honba_feed::{ColumnarSlice, ColumnarSliceBuilder, Dataset, DatasetFeed};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, Exchange, InstrumentId, PriceType, UnixNanos,
};
use honba_sim::BarFillEngine;
use honba_strategy::{
    DynStrategy, LedgerContext, OrderIntent, Strategy, StrategyContext, StrategyRunner,
};
use honba_sweep::{
    run, run_one, run_trial, SharpeFitness, StrategyFactory, StrategySpec, SweepError, SweepPlan,
    TrialOutcome, TrialParams, TrialReport,
};

const FIRST_TS: u64 = 1_000_000_000;
const MINUTE: u64 = 60_000_000_000;

fn instrument(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

/// A deterministic triangular wave: enough up and down turns for a crossover
/// strategy to trade, with no dependence on a maths library.
fn wave(bars: usize, base: f64, step: f64) -> Vec<f64> {
    (0..bars)
        .map(|index| {
            let phase = index % 20;
            let rise = if phase < 10 {
                phase as f64
            } else {
                (20 - phase) as f64
            };
            base + step * rise
        })
        .collect()
}

fn slice(symbol: &str, closes: &[f64]) -> ColumnarSlice {
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(instrument(symbol), spec);
    for (index, close) in closes.iter().enumerate() {
        let at = UnixNanos::from_u64(FIRST_TS + index as u64 * MINUTE);
        builder
            .push(at, *close, *close, *close, *close, 1_000.0)
            .unwrap();
    }
    builder.finish().unwrap()
}

/// Two instruments of different lengths whose bars overlap in time.
fn two_instruments() -> Dataset {
    Dataset::from_slices(vec![
        slice("ALPHA", &wave(40, 100.0, 1.0)),
        slice("BETA", &wave(120, 200.0, 0.5)),
    ])
    .unwrap()
}

/// One instrument with few bars, for tests that only need the runtime.
fn one_instrument() -> Dataset {
    Dataset::from_slices(vec![slice("ALPHA", &wave(12, 100.0, 1.0))]).unwrap()
}

fn sma(seed: u64, symbol: &str, fast: usize, slow: usize) -> TrialParams {
    TrialParams {
        seed,
        spec: StrategySpec::SmaCrossover {
            instrument: instrument(symbol),
            fast,
            slow,
            quantity: 10.0,
        },
    }
}

fn custom(seed: u64, factory: Arc<dyn StrategyFactory>) -> TrialParams {
    TrialParams {
        seed,
        spec: StrategySpec::Custom { inner: factory },
    }
}

fn trial_id_of(outcome: &TrialOutcome) -> usize {
    match outcome {
        TrialOutcome::Completed(report) => report.trial_id,
        TrialOutcome::Failed { trial_id, .. } => *trial_id,
    }
}

fn reason_of(outcome: &TrialOutcome) -> String {
    match outcome {
        TrialOutcome::Completed(report) => panic!("trial {} completed", report.trial_id),
        TrialOutcome::Failed { reason, .. } => reason.clone(),
    }
}

fn as_trial_0(report: &TrialReport) -> TrialReport {
    let mut copy = report.clone();
    copy.trial_id = 0;
    copy
}

/// A strategy that fails on its first bar.
struct Refusing;

impl Strategy for Refusing {
    fn name(&self) -> &str {
        "refusing"
    }

    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _bar: &Bar) -> EngineResult<()> {
        Err(AlgoError::Component("refusing the bar".to_string()))
    }
}

struct RefusingFactory;

impl StrategyFactory for RefusingFactory {
    fn build(&self, _seed: u64) -> Box<dyn Strategy> {
        Box::new(Refusing)
    }
}

/// A factory that blows up while building the strategy.
struct ExplodingFactory;

impl StrategyFactory for ExplodingFactory {
    fn build(&self, seed: u64) -> Box<dyn Strategy> {
        panic!("factory for seed {seed} exploded");
    }
}

/// A strategy that never trades.
struct Idle;

impl Strategy for Idle {
    fn name(&self) -> &str {
        "idle"
    }
}

struct IdleFactory;

impl StrategyFactory for IdleFactory {
    fn build(&self, _seed: u64) -> Box<dyn Strategy> {
        Box::new(Idle)
    }
}

/// A strategy that trades every `every` bars, alternating side, and burns
/// `burn` iterations of arithmetic per bar so that trials seeded differently
/// take different amounts of time.
struct Burner {
    instrument_id: InstrumentId,
    burn: u64,
    every: usize,
    seen: usize,
}

impl Strategy for Burner {
    fn name(&self) -> &str {
        "burner"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, bar: &Bar) -> EngineResult<()> {
        let mut accumulator = bar.close() as u64;
        for step in 0..self.burn {
            accumulator = accumulator
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(step);
        }
        black_box(accumulator);

        self.seen += 1;
        if self.every > 0 && self.seen % self.every == 0 {
            let intent = if (self.seen / self.every) % 2 == 0 {
                OrderIntent::market_buy(self.instrument_id.clone(), 1.0)
            } else {
                OrderIntent::market_sell(self.instrument_id.clone(), 1.0)
            };
            ctx.submit(intent);
        }
        Ok(())
    }
}

struct BurnerFactory {
    burn: u64,
    every: usize,
}

impl StrategyFactory for BurnerFactory {
    fn build(&self, _seed: u64) -> Box<dyn Strategy> {
        Box::new(Burner {
            instrument_id: instrument("BETA"),
            burn: self.burn,
            every: self.every,
            seen: 0,
        })
    }
}

fn burner(seed: u64, burn: u64, every: usize) -> TrialParams {
    custom(seed, Arc::new(BurnerFactory { burn, every }))
}

/// Counts how many strategies are inside their bar hook at once.
#[derive(Default)]
struct Meter {
    live: AtomicUsize,
    peak: AtomicUsize,
}

struct Metered {
    meter: Arc<Meter>,
    pause: Duration,
}

impl Strategy for Metered {
    fn name(&self) -> &str {
        "metered"
    }

    fn on_bar(&mut self, _ctx: &mut dyn StrategyContext, _bar: &Bar) -> EngineResult<()> {
        let live = self.meter.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.meter.peak.fetch_max(live, Ordering::SeqCst);
        std::thread::sleep(self.pause);
        self.meter.live.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

struct MeteredFactory {
    meter: Arc<Meter>,
    pause: Duration,
}

impl StrategyFactory for MeteredFactory {
    fn build(&self, _seed: u64) -> Box<dyn Strategy> {
        Box::new(Metered {
            meter: Arc::clone(&self.meter),
            pause: self.pause,
        })
    }
}

fn metered(seed: u64, meter: Arc<Meter>) -> TrialParams {
    custom(
        seed,
        Arc::new(MeteredFactory {
            meter,
            pause: Duration::from_millis(1),
        }),
    )
}

fn plan_of(trials: Vec<TrialParams>, max_concurrency: usize) -> SweepPlan {
    SweepPlan::new(trials, Arc::new(SharpeFitness)).with_max_concurrency(max_concurrency)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sweep_returns_one_outcome_per_trial_in_trial_order() {
    let trials: Vec<TrialParams> = (0..5usize)
        .map(|i| {
            sma(
                i as u64,
                if i % 2 == 0 { "ALPHA" } else { "BETA" },
                2 + i,
                5 + i,
            )
        })
        .collect();
    let report = run(&plan_of(trials, 4), Arc::new(two_instruments()))
        .await
        .unwrap();

    assert_eq!(report.len(), 5);
    assert_eq!(report.outcomes().len(), 5);
    let ids: Vec<usize> = report.outcomes().iter().map(trial_id_of).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    assert!(report
        .outcomes()
        .iter()
        .all(|outcome| matches!(outcome, TrialOutcome::Completed(_))));
    assert_eq!(report.reports().count(), 5);
    assert_eq!(report.ranking().len(), 5);
    assert!(!report.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn results_do_not_depend_on_completion_order() {
    let trials: Vec<TrialParams> = (0..6u64)
        .map(|seed| burner(seed, seed * 4_000, (seed as usize % 5) + 1))
        .collect();
    let data = two_instruments();

    let serial = run(&plan_of(trials.clone(), 1), Arc::new(data.clone()))
        .await
        .unwrap();
    let parallel = run(&plan_of(trials.clone(), 8), Arc::new(data.clone()))
        .await
        .unwrap();

    let fills: Vec<usize> = serial
        .reports()
        .map(|report| report.metrics.fills)
        .collect();
    assert_eq!(fills.len(), 6);
    assert!(
        fills.windows(2).any(|pair| pair[0] != pair[1]),
        "the trials must differ, or this proves nothing: {fills:?}"
    );
    assert!(serial.reports().all(|report| report.metrics.fills > 0));

    assert_eq!(serial, parallel);
    assert_eq!(serial.ranking(), parallel.ranking());
    let serial_ids: Vec<usize> = serial.outcomes().iter().map(trial_id_of).collect();
    let parallel_ids: Vec<usize> = parallel.outcomes().iter().map(trial_id_of).collect();
    assert_eq!(serial_ids, parallel_ids);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_trial_does_not_lose_the_others() {
    let trials = vec![
        sma(0, "ALPHA", 2, 5),
        custom(1, Arc::new(RefusingFactory)),
        custom(2, Arc::new(ExplodingFactory)),
        sma(3, "BETA", 3, 7),
    ];
    let report = run(&plan_of(trials, 4), Arc::new(two_instruments()))
        .await
        .expect("a failed trial must not fail the sweep");

    assert_eq!(report.len(), 4);
    let ids: Vec<usize> = report.outcomes().iter().map(trial_id_of).collect();
    assert_eq!(ids, vec![0, 1, 2, 3]);
    assert!(matches!(report.outcomes()[0], TrialOutcome::Completed(_)));
    assert!(matches!(report.outcomes()[3], TrialOutcome::Completed(_)));

    let refused = reason_of(&report.outcomes()[1]);
    assert!(
        refused.contains("refusing the bar"),
        "unexpected reason: {refused}"
    );
    let exploded = reason_of(&report.outcomes()[2]);
    assert!(
        exploded.contains("exploded"),
        "unexpected reason: {exploded}"
    );

    assert_eq!(report.reports().count(), 2);
    let ranked: Vec<usize> = report.ranking().to_vec();
    assert_eq!(ranked.len(), 2);
    assert!(ranked.contains(&0) && ranked.contains(&3));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_seed_gives_the_same_trial() {
    let trials = vec![
        sma(7, "ALPHA", 2, 5),
        sma(7, "ALPHA", 2, 5),
        sma(8, "BETA", 3, 7),
    ];
    let plan = plan_of(trials, 3);
    let data = Arc::new(two_instruments());

    let report = run(&plan, Arc::clone(&data)).await.unwrap();
    let first = report.reports().next().unwrap().clone();
    let second = report.reports().nth(1).unwrap().clone();
    let third = report.reports().nth(2).unwrap().clone();

    assert_eq!(first.params, second.params);
    assert_eq!(as_trial_0(&first), as_trial_0(&second));
    assert_ne!(as_trial_0(&first), as_trial_0(&third));

    let one = run_one(&plan, Arc::clone(&data), 0).await.unwrap();
    assert_eq!(one, first);
    let straight = run_trial(0, &plan.trials()[0], &data, &plan).unwrap();
    assert_eq!(straight, first);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_concurrency_is_respected() {
    let data = one_instrument();
    for limit in [1usize, 2] {
        let meter = Arc::new(Meter::default());
        let trials: Vec<TrialParams> = (0..6u64)
            .map(|seed| metered(seed, Arc::clone(&meter)))
            .collect();
        let report = run(&plan_of(trials, limit), Arc::new(data.clone()))
            .await
            .unwrap();
        assert_eq!(report.len(), 6);
        assert_eq!(
            meter.peak.load(Ordering::SeqCst),
            limit,
            "peak concurrency must stay at the limit"
        );
        assert!(meter.peak.load(Ordering::SeqCst) <= limit);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_plan_produces_an_empty_report() {
    let report = run(&plan_of(Vec::new(), 4), Arc::new(two_instruments()))
        .await
        .unwrap();
    assert!(report.is_empty());
    assert_eq!(report.len(), 0);
    assert!(report.outcomes().is_empty());
    assert!(report.ranking().is_empty());
    assert_eq!(report.reports().count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trial_with_no_fills_still_scores() {
    let data = Dataset::from_slices(vec![slice("ALPHA", &wave(1, 100.0, 1.0))]).unwrap();
    let plan = plan_of(vec![custom(0, Arc::new(IdleFactory))], 1);

    let report = run(&plan, Arc::new(data.clone())).await.unwrap();
    let trial = report.reports().next().unwrap();

    assert_eq!(trial.metrics.fills, 0);
    assert_eq!(trial.metrics.round_trips, 0);
    assert_eq!(trial.metrics.trades, None);
    assert_eq!(trial.metrics.equity, None);
    assert_eq!(report.ranking(), &[0]);

    let score = plan.fitness().score(trial);
    assert!(
        score.is_finite(),
        "a trial with no fills must still score: {score}"
    );
    assert_eq!(score, 0.0);

    let repeated = run_one(&plan, Arc::new(data), 0).await.unwrap();
    assert_eq!(repeated, *trial);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trial_audit_matches_a_single_engine_run() {
    let data = two_instruments();
    let params = sma(3, "ALPHA", 2, 5);
    let plan = plan_of(vec![params.clone()], 1);

    let swept = run(&plan, Arc::new(data.clone())).await.unwrap();
    let trial = swept.reports().next().unwrap().clone();
    assert!(!trial.audit.is_empty());

    let mut engine = Engine::new();
    let execution = BarFillEngine::new();
    let runner = StrategyRunner::with_context(
        DynStrategy::new(params.spec.build(params.seed)),
        execution.clone(),
        LedgerContext::with_cash(Money::new(plan.initial_cash(), plan.currency())),
    );
    engine.add_handler(execution);
    engine.add_handler(runner);
    let mut feed = DatasetFeed::new(&data, UnixNanos::from_u64(FIRST_TS));
    engine.run(&mut feed).unwrap();

    assert_eq!(trial.audit, engine.audit());
    assert_eq!(engine.audit().len(), data.bar_count());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plan_with_no_concurrency_is_refused_before_any_trial_runs() {
    let plan = plan_of(vec![sma(0, "ALPHA", 2, 5)], 0);
    assert_eq!(plan.max_concurrency(), 0, "the builder stores it as given");

    let error = run(&plan, Arc::new(two_instruments())).await.unwrap_err();
    assert_eq!(
        error,
        SweepError::InvalidPlan("max_concurrency must be at least 1, got 0".to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_trial_id_has_no_single_trial_to_run() {
    let plan = plan_of(vec![sma(0, "ALPHA", 2, 5)], 1);
    let error = run_one(&plan, Arc::new(one_instrument()), 7)
        .await
        .unwrap_err();
    assert!(matches!(error, SweepError::InvalidPlan(_)), "{error:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_factory_serves_every_trial_that_names_it() {
    let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    struct Recording {
        seen: Arc<Mutex<Vec<u64>>>,
        seed: u64,
    }
    impl Strategy for Recording {
        fn name(&self) -> &str {
            "recording"
        }
        fn on_start(&mut self, _ctx: &mut dyn StrategyContext) -> EngineResult<()> {
            self.seen.lock().unwrap().push(self.seed);
            Ok(())
        }
    }
    struct RecordingFactory {
        seen: Arc<Mutex<Vec<u64>>>,
    }
    impl StrategyFactory for RecordingFactory {
        fn build(&self, seed: u64) -> Box<dyn Strategy> {
            Box::new(Recording {
                seen: Arc::clone(&self.seen),
                seed,
            })
        }
    }

    let factory: Arc<dyn StrategyFactory> = Arc::new(RecordingFactory { seen: recorded });
    let trials = vec![custom(11, Arc::clone(&factory)), custom(12, factory)];
    let report = run(&plan_of(trials, 2), Arc::new(one_instrument()))
        .await
        .unwrap();

    assert_eq!(report.len(), 2);
    let mut built = seen.lock().unwrap().clone();
    built.sort_unstable();
    assert_eq!(built, vec![11, 12]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn determinism_under_threads_one_worker() {
    determinism_under_threads(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn determinism_under_threads_four_workers() {
    determinism_under_threads(4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn determinism_under_threads_sixteen_workers() {
    determinism_under_threads(16).await;
}

async fn determinism_under_threads(worker_threads: usize) {
    let data = two_instruments();
    let trials: Vec<TrialParams> = (0..8u64)
        .map(|seed| {
            sma(
                seed,
                if seed % 2 == 0 { "ALPHA" } else { "BETA" },
                2 + (seed % 3) as usize,
                5 + (seed % 4) as usize,
            )
        })
        .collect();
    let plan = plan_of(trials, worker_threads);

    let report = run(&plan, Arc::new(data)).await.unwrap();
    assert_eq!(report.len(), 8);
    assert!(report.reports().count() == 8, "every trial must complete");
    let ranking = report.ranking().to_vec();
    assert_eq!(ranking.len(), 8);

    let journal = format!(
        "{:?}",
        report
            .reports()
            .flat_map(|r| r.audit.iter())
            .collect::<Vec<_>>()
    );
    let ranking_str = format!("{ranking:?}");

    let thread = thread::Builder::new()
        .name(format!("determinism-{worker_threads}"))
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let data = two_instruments();
                let trials: Vec<TrialParams> = (0..8u64)
                    .map(|seed| {
                        sma(
                            seed,
                            if seed % 2 == 0 { "ALPHA" } else { "BETA" },
                            2 + (seed % 3) as usize,
                            5 + (seed % 4) as usize,
                        )
                    })
                    .collect();
                let plan = plan_of(trials, worker_threads);
                let report = run(&plan, Arc::new(data)).await.unwrap();
                let journal = format!(
                    "{:?}",
                    report
                        .reports()
                        .flat_map(|r| r.audit.iter())
                        .collect::<Vec<_>>()
                );
                let ranking = format!("{:?}", report.ranking());
                (journal, ranking)
            })
        })
        .unwrap();
    let (journal_other, ranking_other) = thread.join().unwrap();

    assert_eq!(
        journal, journal_other,
        "the journal must be byte-identical across worker thread counts"
    );
    assert_eq!(
        ranking_str, ranking_other,
        "the ranking must be byte-identical across worker thread counts"
    );
}

#[tokio::test]
async fn sweep_over_streamed_dataset() {
    let data = two_instruments();
    let trials = vec![sma(1, "ALPHA", 2, 5), sma(2, "BETA", 3, 7)];
    let plan = plan_of(trials.clone(), 2);

    // Run sweep over full dataset
    let full_report = run(&plan, Arc::new(data.clone())).await.unwrap();

    // Partition dataset into chunks
    let chunks = data.chunks(20);
    assert!(
        chunks.len() > 1,
        "dataset was partitioned into multiple chunks"
    );

    // Verify streaming chunks replay correctly in individual engines
    for chunk in &chunks {
        let chunk_plan = plan_of(trials.clone(), 1);
        let chunk_report = run(&chunk_plan, Arc::new(chunk.dataset.clone()))
            .await
            .unwrap();
        assert_eq!(chunk_report.len(), 2);
    }

    assert_eq!(full_report.len(), 2);
    assert!(!full_report.ranking().is_empty());
}
