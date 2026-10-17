//! `BacktestExecutor`: one backtest through the real kernel over an in-memory bar store.

use std::sync::Arc;

use honba_api::{
    JournalWriter, ResolvedBacktest, ResolvedRequest, ResolvedSweep, RunExecutor, RunJob, RunKind,
    RunOutcome,
};
use honba_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use honba_messages::{
    BarAggregation, BarSpecification, ErrorCode, ErrorDetail, Event, Exchange, InstrumentId,
    Message, PriceType, UnixNanos, SCHEMA_VERSION,
};
use serde_json::{json, Value};

use crate::{BacktestExecutor, StrategyRegistry};

const DAY_NS: u64 = 86_400_000_000_000;
const T0: u64 = 1_700_000_000_000_000_000;

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

fn day_spec() -> BarSpecification {
    BarSpecification::new(1, BarAggregation::Day, PriceType::Last)
}

/// A wave that makes the 3/8 SMA crossover trade several times.
fn closes() -> Vec<f64> {
    (0..80)
        .map(|i| 100.0 + 12.0 * (f64::from(i) * 0.35).sin() + f64::from(i) * 0.05)
        .collect()
}

fn reader(spec: BarSpecification) -> Arc<DatasetReader> {
    let mut builder = ColumnarSliceBuilder::new(tcs(), spec);
    for (i, c) in closes().into_iter().enumerate() {
        let ts = UnixNanos::from_u64(T0 + i as u64 * DAY_NS);
        builder
            .push(ts, c - 0.5, c + 1.0, c - 1.0, c, 1_000.0)
            .unwrap();
    }
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    Arc::new(DatasetReader::from_dataset(dataset))
}

fn executor() -> BacktestExecutor {
    BacktestExecutor::new(reader(day_spec()), StrategyRegistry::builtin())
}

fn job_with(strategy: &str, universe: &str, bar_spec: &str, seed: u64) -> RunJob {
    let ir = StrategyRegistry::builtin()
        .ir_for(strategy, &tcs(), "1d")
        .unwrap_or_else(|_| {
            StrategyRegistry::builtin()
                .ir_for("sma_crossover", &tcs(), "1d")
                .unwrap()
        });
    RunJob {
        run_id: honba_api::RunIdGenerator::default()
            .next(1, [0; 10])
            .unwrap(),
        kind: RunKind::Backtest,
        strategy_id: strategy.to_owned(),
        strategy: ir,
        request: ResolvedRequest::Backtest(ResolvedBacktest {
            strategy: strategy.to_owned(),
            universe: universe.to_owned(),
            start: UnixNanos::from_u64(T0),
            end: UnixNanos::from_u64(T0 + 90 * DAY_NS),
            bar_spec: bar_spec.to_owned(),
            initial_capital: 1_000_000.0,
            seed,
            fill_model: None,
            commission_per_fill: 0.0,
            slippage_bps: 0.0,
        }),
        seed,
    }
}

fn job(seed: u64) -> RunJob {
    job_with("sma_crossover", "TCS.NSE", "1d", seed)
}

/// The default job with commission/slippage knobs set (Gap A).
fn job_with_costs(commission_per_fill: f64, slippage_bps: f64) -> RunJob {
    let mut job = job(42);
    if let ResolvedRequest::Backtest(spec) = &mut job.request {
        spec.commission_per_fill = commission_per_fill;
        spec.slippage_bps = slippage_bps;
    }
    job
}

#[derive(Default)]
struct Memory {
    records: Vec<Message>,
    flushed: bool,
}

impl JournalWriter for Memory {
    fn append(&mut self, msg: &Message) -> Result<(), ErrorDetail> {
        self.records.push(msg.clone());
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ErrorDetail> {
        self.flushed = true;
        Ok(())
    }
}

struct Broken;

impl JournalWriter for Broken {
    fn append(&mut self, _: &Message) -> Result<(), ErrorDetail> {
        Err(
            ErrorDetail::new(ErrorCode::InternalError, "journal write failed: Other")
                .with_context(json!({"reason": "journal_write"})),
        )
    }
    fn flush(&mut self) -> Result<(), ErrorDetail> {
        Ok(())
    }
}

fn kinds(journal: &Memory) -> Vec<String> {
    journal
        .records
        .iter()
        .map(|m| {
            serde_json::to_value(m).unwrap()["event"]["type"]
                .as_str()
                .unwrap_or("?")
                .to_owned()
        })
        .collect()
}

fn lines(journal: &Memory) -> Vec<String> {
    journal
        .records
        .iter()
        .map(|m| serde_json::to_string(m).unwrap())
        .collect()
}

fn metrics_of(outcome: RunOutcome) -> (honba_api::BacktestMetrics, Value) {
    match outcome {
        RunOutcome::Backtest {
            metrics,
            assumptions,
        } => (metrics, assumptions),
        other => panic!("expected a backtest outcome, got {other:?}"),
    }
}

#[test]
fn a_run_journals_bars_orders_and_fills_at_the_current_schema_version() {
    let mut journal = Memory::default();
    executor().execute(job(42), &mut journal).unwrap();
    let kinds = kinds(&journal);
    assert_eq!(kinds.iter().filter(|k| *k == "bar").count(), 80);
    assert!(kinds.iter().any(|k| k == "order"), "{kinds:?}");
    assert!(kinds.iter().any(|k| k == "order_filled"), "{kinds:?}");
    assert!(journal
        .records
        .iter()
        .all(|m| m.schema_version() == SCHEMA_VERSION));
    assert_eq!(SCHEMA_VERSION, 4);
    assert!(
        journal.flushed,
        "the journal is flushed at the end of the run"
    );
    // Kernel order: the first record is the first bar.
    assert!(matches!(journal.records[0].event(), Event::Bar(_)));
}

#[test]
fn every_fill_follows_its_order_in_the_journal() {
    let mut journal = Memory::default();
    executor().execute(job(42), &mut journal).unwrap();
    let mut ordered = std::collections::HashSet::new();
    let mut fills = 0;
    for message in &journal.records {
        match message.event() {
            Event::Order(order) => {
                ordered.insert(order.order_id().as_str().to_owned());
            }
            Event::OrderFilled { order_id, .. } | Event::OrderPartiallyFilled { order_id, .. } => {
                assert!(
                    ordered.contains(order_id.as_str()),
                    "{order_id:?} before its order"
                );
                fills += 1;
            }
            _ => {}
        }
    }
    assert!(fills >= 2);
}

#[test]
fn the_outcome_has_metrics_and_stated_assumptions() {
    let mut journal = Memory::default();
    let outcome = executor().execute(job(42), &mut journal).unwrap();
    let (metrics, assumptions) = metrics_of(outcome);
    assert!(metrics.trades >= 1, "{metrics:?}");
    assert!(metrics.net_pnl.is_finite() && metrics.sharpe.is_finite());
    assert!((0.0..=1.0).contains(&metrics.max_drawdown));
    assert!((metrics.total_return - metrics.net_pnl / 1_000_000.0).abs() < 1e-9);
    assert!(assumptions["not_modelled"]
        .as_array()
        .is_some_and(|a| !a.is_empty()));
    assert!(assumptions["timing"].is_string());
}

#[test]
fn commission_and_slippage_reduce_pnl_and_are_stated_as_applied() {
    let (free, free_assumptions) =
        metrics_of(executor().execute(job(42), &mut Memory::default()).unwrap());
    let (costed, costed_assumptions) = metrics_of(
        executor()
            .execute(job_with_costs(50.0, 10.0), &mut Memory::default())
            .unwrap(),
    );

    assert!(
        costed.net_pnl < free.net_pnl,
        "costs must lower net PnL: costed={costed:?} free={free:?}"
    );

    // Both knobs are reported, and no longer listed as unmodelled.
    assert_eq!(
        costed_assumptions["applied"]["commission_per_fill"],
        json!(50.0)
    );
    assert_eq!(costed_assumptions["applied"]["slippage_bps"], json!(10.0));
    let costs_off = costed_assumptions["not_modelled"].as_array().unwrap();
    assert!(!costs_off.iter().any(|v| v == "transaction_costs"));
    assert!(!costs_off.iter().any(|v| v == "slippage"));

    // With both knobs at zero, the gap is stated honestly.
    let free_off = free_assumptions["not_modelled"].as_array().unwrap();
    assert!(free_off.iter().any(|v| v == "transaction_costs"));
    assert!(free_off.iter().any(|v| v == "slippage"));
}

#[test]
fn the_same_job_gives_byte_identical_journal_lines() {
    let (mut a, mut b) = (Memory::default(), Memory::default());
    let first = executor().execute(job(42), &mut a).unwrap();
    let second = executor().execute(job(42), &mut b).unwrap();
    assert!(!a.records.is_empty());
    assert_eq!(lines(&a), lines(&b));
    assert_eq!(first, second);
}

#[test]
fn the_journal_body_carries_no_wall_clock_run_id_or_path() {
    let mut journal = Memory::default();
    let job = job(42);
    let id = job.run_id.to_string();
    executor().execute(job, &mut journal).unwrap();
    let text = lines(&journal).join("\n");
    assert!(!text.contains(&id));
    assert!(!text.contains("journals"));
    assert!(!text.contains("/"));
}

#[test]
fn an_unregistered_strategy_is_an_error_not_a_panic() {
    let mut job = job(42);
    job.strategy_id = "sha256:abc".into();
    job.strategy.manifest.name = "mystery".into();
    let err = executor().execute(job, &mut Memory::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(err.context.unwrap()["field"], "strategy");
}

#[test]
fn a_catalog_ir_runs_through_its_registered_name() {
    // A compiled manifest named after a registered strategy has a Rust implementation.
    let mut job = job(42);
    job.strategy_id = "sha256:abc".into();
    let mut journal = Memory::default();
    executor().execute(job, &mut journal).unwrap();
    assert!(!journal.records.is_empty());
}

#[test]
fn a_sweep_job_is_implemented_by_this_executor() {
    let mut job = job(42);
    job.kind = RunKind::Sweep;
    job.request = ResolvedRequest::Sweep(ResolvedSweep {
        strategy: "sma_crossover".into(),
        params: json!({"params": {"fast": [1, 2, 1]}, "fitness": "sharpe"}),
        trials: 2,
        seed: 42,
        universe: "TCS.NSE".into(),
        start: UnixNanos::from_u64(1),
        end: UnixNanos::from_u64(2),
        bar_spec: "1d".into(),
        initial_capital: 1_000_000.0,
    });
    // The executor now implements sweeps; it should return a proper error (not NotImplemented)
    // because the Memory journal writer doesn't have real data
    let err = executor().execute(job, &mut Memory::default()).unwrap_err();
    // Should be market_data_unavailable or some other error, not NotImplemented
    assert_ne!(err.code, ErrorCode::NotImplemented);
}

#[test]
fn no_bars_in_the_window_is_market_data_unavailable() {
    let mut job = job(42);
    if let ResolvedRequest::Backtest(r) = &mut job.request {
        r.start = UnixNanos::from_u64(1);
        r.end = UnixNanos::from_u64(2);
    }
    let err = executor().execute(job, &mut Memory::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::MarketDataUnavailable);
    assert_eq!(err.context.unwrap()["reason"], "no_data");
}

#[test]
fn a_timeframe_the_store_does_not_hold_is_unsupported() {
    let job = job_with("sma_crossover", "TCS.NSE", "5m", 42);
    let err = executor().execute(job, &mut Memory::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::Unsupported);
}

#[test]
fn a_universe_that_is_not_one_instrument_is_refused() {
    for universe in ["TCS", "TCS.NSE,INFY.NSE", ""] {
        let job = job_with("sma_crossover", universe, "1d", 42);
        let err = executor().execute(job, &mut Memory::default()).unwrap_err();
        assert_eq!(
            err.code,
            ErrorCode::ValidationInvalidRequest,
            "{universe:?}"
        );
        assert_eq!(err.context.unwrap()["field"], "universe");
    }
}

#[test]
fn a_journal_failure_fails_the_run_with_its_reason() {
    let err = executor().execute(job(42), &mut Broken).unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(err.context.unwrap()["reason"], "journal_write");
}

#[test]
fn a_buy_and_hold_run_has_one_unpaired_fill_and_no_round_trips() {
    let job = job_with("buy_and_hold", "TCS.NSE", "1d", 5);
    let mut journal = Memory::default();
    let (metrics, _) = metrics_of(executor().execute(job, &mut journal).unwrap());
    assert_eq!(metrics.trades, 0);
    assert_eq!(metrics.net_pnl, 0.0);
    let fills = kinds(&journal)
        .iter()
        .filter(|k| *k == "order_filled")
        .count();
    assert_eq!(fills, 1);
}

#[test]
fn fill_model_l2_without_ticks_is_refused_with_a_stable_reason() {
    use honba_config::{ExecutionConfig, FillModel};
    let exec = executor().with_execution(ExecutionConfig {
        fill_model: FillModel::L2,
        ..ExecutionConfig::default()
    });
    let err = exec.execute(job(42), &mut Memory::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::Unsupported);
    assert_eq!(err.context.unwrap()["reason"], "l2_requires_ticks");
}

#[test]
fn other_non_bar_fill_models_stay_unsupported_without_the_l2_reason() {
    use honba_config::{ExecutionConfig, FillModel};
    let exec = executor().with_execution(ExecutionConfig {
        fill_model: FillModel::None,
        ..ExecutionConfig::default()
    });
    let err = exec.execute(job(42), &mut Memory::default()).unwrap_err();
    assert_eq!(err.code, ErrorCode::Unsupported);
    assert!(err.context.is_none());
}

// ---- fill_model l2 over a tick source (ADR 0030) ----

mod l2 {
    use super::*;
    use honba_config::{ExecutionConfig, FillModel};
    use honba_feed::InMemoryTickReader;
    use honba_messages::{AggressorSide, QuoteTick, TradeId, TradeTick};

    fn l2_exec(ticks: InMemoryTickReader) -> BacktestExecutor {
        executor()
            .with_execution(ExecutionConfig {
                fill_model: FillModel::L2,
                ..ExecutionConfig::default()
            })
            .with_tick_reader(Arc::new(ticks))
    }

    fn quote(t: u64, bid: f64, ask: f64) -> QuoteTick {
        let t = UnixNanos::from_u64(t);
        QuoteTick::new(tcs(), bid, ask, 1_000.0, 1_000.0, t, t)
    }

    fn fill_prices(journal: &Memory) -> Vec<f64> {
        journal
            .records
            .iter()
            .filter_map(|m| {
                let v = serde_json::to_value(m).unwrap();
                (v["event"]["type"] == "order_filled")
                    .then(|| v["event"]["last_px"].as_f64().unwrap())
            })
            .collect()
    }

    #[test]
    fn a_market_order_fills_at_the_quote_ask_not_the_bar_close() {
        // Quote stamped exactly at the first bar: ticks come first on a tie.
        let ticks = InMemoryTickReader::new()
            .with_quotes(vec![quote(T0, 99.0, 100.5), quote(T0 + 1, 150.0, 151.0)]);
        let job = job_with("buy_and_hold", "TCS.NSE", "1d", 5);
        let mut journal = Memory::default();
        l2_exec(ticks).execute(job, &mut journal).unwrap();
        assert_eq!(fill_prices(&journal), [100.5]);
    }

    #[test]
    fn ticks_are_not_journaled() {
        let ticks = InMemoryTickReader::new().with_quotes(vec![quote(T0, 99.0, 100.5)]);
        let job = job_with("buy_and_hold", "TCS.NSE", "1d", 5);
        let mut journal = Memory::default();
        l2_exec(ticks).execute(job, &mut journal).unwrap();
        let kinds = kinds(&journal);
        assert!(
            !kinds.iter().any(|k| k == "quote" || k == "trade"),
            "{kinds:?}"
        );
        assert!(kinds.iter().any(|k| k == "bar"));
    }

    #[test]
    fn a_resting_order_with_no_book_fills_on_a_later_quote() {
        // The first quote comes after the first bar: the order rests, then fills at that ask.
        let ticks = InMemoryTickReader::new().with_quotes(vec![quote(T0 + 1, 99.0, 120.0)]);
        let job = job_with("buy_and_hold", "TCS.NSE", "1d", 5);
        let mut journal = Memory::default();
        l2_exec(ticks).execute(job, &mut journal).unwrap();
        assert_eq!(fill_prices(&journal), [120.0]);
    }

    #[test]
    fn merge_puts_ticks_before_a_bar_at_the_same_instant_and_keeps_order() {
        use crate::executor::merge_events;
        let bar = |t: u64| {
            let ts = UnixNanos::from_u64(t);
            honba_messages::Bar::new(
                honba_messages::BarType::new(tcs(), day_spec()),
                1.0,
                2.0,
                0.5,
                1.5,
                1.0,
                ts,
                ts,
            )
        };
        let label = |e: &Event| match e {
            Event::Bar(b) => format!("bar{}", b.ts_event().as_u64()),
            Event::Quote(q) => format!("q{}", q.ts_event().as_u64()),
            Event::Trade(t) => format!("t{}", t.ts_event().as_u64()),
            _ => "?".into(),
        };
        let t = |n: u64| {
            let ts = UnixNanos::from_u64(n);
            Event::Trade(TradeTick::new(
                tcs(),
                1.0,
                1.0,
                AggressorSide::Buyer,
                TradeId::new("x"),
                ts,
                ts,
            ))
        };
        let merged = merge_events(
            vec![bar(10), bar(20)],
            vec![
                Event::Quote(quote(5, 1.0, 2.0)),
                Event::Quote(quote(10, 1.0, 2.0)),
                t(10),
                t(25),
            ],
        );
        let got: Vec<String> = merged.iter().map(label).collect();
        assert_eq!(got, ["q5", "q10", "t10", "bar10", "bar20", "t25"]);
    }

    #[test]
    fn trades_alone_do_not_break_the_run() {
        let t = UnixNanos::from_u64(T0);
        let ticks = InMemoryTickReader::new().with_trades(vec![TradeTick::new(
            tcs(),
            100.0,
            5.0,
            AggressorSide::Buyer,
            TradeId::new("T1"),
            t,
            t,
        )]);
        let job = job_with("buy_and_hold", "TCS.NSE", "1d", 5);
        l2_exec(ticks).execute(job, &mut Memory::default()).unwrap();
    }

    #[test]
    fn a_store_with_no_ticks_for_the_window_keeps_the_stable_refusal() {
        for ticks in [
            InMemoryTickReader::new(),
            InMemoryTickReader::new().with_quotes(vec![quote(T0 + 365 * DAY_NS, 1.0, 2.0)]),
        ] {
            let err = l2_exec(ticks)
                .execute(job(42), &mut Memory::default())
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::Unsupported);
            assert_eq!(err.context.unwrap()["reason"], "l2_requires_ticks");
        }
    }

    #[test]
    fn bar_fill_runs_ignore_a_configured_tick_source() {
        let ticks = InMemoryTickReader::new().with_quotes(vec![quote(T0, 99.0, 100.5)]);
        let with = executor().with_tick_reader(Arc::new(ticks));
        let a = lines_of(with.execute(job(42), &mut Memory::default()));
        let b = lines_of(executor().execute(job(42), &mut Memory::default()));
        assert_eq!(a, b);
    }

    fn lines_of(r: Result<RunOutcome, ErrorDetail>) -> String {
        let (m, _) = metrics_of(r.unwrap());
        format!("{m:?}")
    }

    // ---- a per-request fill_model (ADR 0030) overrides the server's configured one ----

    fn with_fill_model(mut job: RunJob, name: &str) -> RunJob {
        match &mut job.request {
            ResolvedRequest::Backtest(spec) => spec.fill_model = Some(name.to_owned()),
            ResolvedRequest::Sweep(_) => unreachable!("these tests submit backtests"),
        }
        job
    }

    #[test]
    fn a_request_l2_uses_a_tick_source_a_bar_fill_server_ignored() {
        // The server's model is bar_fill and it has a tick source; only the request asking for
        // l2 makes the source matter, and the fill is the quote ask, not the bar close.
        let ticks = InMemoryTickReader::new().with_quotes(vec![quote(T0, 99.0, 100.5)]);
        let exec = executor().with_tick_reader(Arc::new(ticks));
        let job = with_fill_model(job_with("buy_and_hold", "TCS.NSE", "1d", 5), "l2");
        let mut journal = Memory::default();
        exec.execute(job, &mut journal).unwrap();
        assert_eq!(fill_prices(&journal), [100.5], "the request chose l2");
    }

    #[test]
    fn a_request_bar_fill_overrides_an_l2_server_without_a_source() {
        // The server is l2 and has no tick source, so a bar_fill request still runs.
        let exec = executor().with_execution(ExecutionConfig {
            fill_model: FillModel::L2,
            ..ExecutionConfig::default()
        });
        let job = with_fill_model(job(42), "bar_fill");
        exec.execute(job, &mut Memory::default()).unwrap();
    }

    #[test]
    fn a_request_l2_without_a_tick_source_keeps_the_stable_refusal() {
        let job = with_fill_model(job(42), "l2");
        let err = executor().execute(job, &mut Memory::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unsupported);
        assert_eq!(err.context.unwrap()["reason"], "l2_requires_ticks");
    }
}

/// The names `honba_api::FILL_MODELS` accepts are exactly `honba_config::FillModel`'s, so the
/// request-side list cannot drift from the config enum the executor deserializes into.
#[test]
fn fill_model_names_match_the_config_enum() {
    use honba_api::FILL_MODELS;
    use honba_config::FillModel;
    let from_config: std::collections::BTreeSet<String> = [
        FillModel::BarFill,
        FillModel::BarFillThrough,
        FillModel::L2,
        FillModel::None,
    ]
    .into_iter()
    .map(|m| {
        serde_json::to_value(m)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    })
    .collect();
    let from_api: std::collections::BTreeSet<String> =
        FILL_MODELS.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(from_api, from_config);
    for name in FILL_MODELS {
        serde_json::from_value::<FillModel>(serde_json::json!(name))
            .unwrap_or_else(|e| panic!("{name} must deserialize into FillModel: {e}"));
    }
}

/// `honba_api::MAX_COMMISSION_PER_FILL` is the simulator's own flat-cost ceiling, pinned here so
/// the request can never admit a commission the fill engine would then reject.
#[test]
fn max_commission_matches_the_simulator_ceiling() {
    assert_eq!(
        honba_api::MAX_COMMISSION_PER_FILL,
        honba_sim::bar_fill::MAX_FLAT_COST
    );
}
