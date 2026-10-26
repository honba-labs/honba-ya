//! `BacktestExecutor`: one backtest through the real kernel over an in-memory bar store.

use chrono::DateTime;
use std::sync::Arc;

use honba_api::{
    ErrorCode, ErrorDetail, JournalWriter, Message, ResolvedBacktest, ResolvedRequest, RunExecutor,
    RunJob, RunKind,
};
use honba_core::{BarReader, PortResult};

use rust_decimal_macros::dec;
use serde_json::json;

use crate::{BacktestExecutor, StrategyRegistry};

const DAY_NS: u64 = 86_400_000_000_000;
const T0: u64 = 1_700_000_000_000_000_000;

struct DummyBarReader;
impl BarReader for DummyBarReader {
    fn read_bars(&self, _req: &honba_core::BarRequest) -> PortResult<Vec<honba_core::Bar>> {
        Ok(Vec::new())
    }
}

fn executor() -> BacktestExecutor {
    BacktestExecutor::new(Arc::new(DummyBarReader), StrategyRegistry::builtin())
}

fn compiled_ir() -> honba_api::StrategyIr {
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
    honba_api::compile_strategy(
        &mut honba_api::StrategyCatalog::default(),
        serde_json::from_value(manifest).unwrap(),
    )
    .unwrap()
    .ir
}

fn job_with(strategy: &str, universe: &str, bar_spec: &str, seed: u64) -> RunJob {
    RunJob {
        run_id: honba_api::RunIdGenerator::default()
            .next(1, [0; 10])
            .unwrap(),
        kind: RunKind::Backtest,
        strategy_id: strategy.to_owned(),
        strategy: compiled_ir(),
        request: ResolvedRequest::Backtest(ResolvedBacktest {
            strategy: strategy.to_owned(),
            universe: universe.to_owned(),
            start: DateTime::from_timestamp_nanos(T0 as i64),
            end: DateTime::from_timestamp_nanos((T0 + 90 * DAY_NS) as i64),
            bar_spec: bar_spec.to_owned(),
            initial_capital: dec!(1_000_000.0),
            seed,
            fill_model: None,
            commission_per_fill: dec!(0.0),
            slippage_bps: dec!(0.0),
        }),
        seed,
    }
}

fn job(seed: u64) -> RunJob {
    job_with("sma_crossover", "TCS.NSE", "1d", seed)
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

/// The names `honba_api::FILL_MODELS` accepts are exactly `honba_config::FillModel`\x27s, so the
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

#[test]
fn execute_reports_not_implemented_cleanly() {
    let mut journal = Memory::default();
    let err = executor().execute(job(42), &mut journal).unwrap_err();
    assert_eq!(err.code, ErrorCode::NotImplemented);
    assert_eq!(err.context.unwrap()["reason"], "engine_kernel_removed");
}
