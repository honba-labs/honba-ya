//! Shared strategy conformance fixture, Rust side (ADR 008).
//!
//! Runs every scenario in `schema/conformance/strategy_contract.json` through
//! the Rust reference strategy, `StrategyRunner` and `BarFillEngine` (handler
//! order: execution first, as in an engine) and compares intents, fills,
//! context observations, final positions and cash with the fixture. The Python
//! suite (`python/tests/integration/test_strategy_conformance.py`) reads the
//! same file.

use std::path::PathBuf;

use honba_engine::Handler;
use honba_entities::{Currency, Instrument, InstrumentKind, Money};
use honba_messages::{InstrumentId, Message, SCHEMA_VERSION};
use honba_sim::{BarFillEngine, FillCosts};
use honba_strategy::{
    BuyAndHold, ContractProbe, LedgerContext, SmaCrossover, Strategy, StrategyContext,
    StrategyRunner,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/conformance/strategy_contract.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn instrument_id(v: &Value) -> InstrumentId {
    serde_json::from_value(v.clone()).unwrap()
}

fn instrument(v: &Value) -> Instrument {
    let kind = match v["kind"].as_str().unwrap() {
        "equity" => InstrumentKind::Equity,
        "future" => InstrumentKind::Future,
        "option" => InstrumentKind::Option,
        "fx" => InstrumentKind::Fx,
        "index" => InstrumentKind::Index,
        "mutual_fund" => InstrumentKind::MutualFund,
        other => panic!("unknown instrument kind {other}"),
    };
    let currency: Currency = serde_json::from_value(v["currency"].clone()).unwrap();
    Instrument::new(
        instrument_id(&v["instrument_id"]),
        kind,
        currency,
        v["lot_size"].as_f64().unwrap(),
        v["tick_size"].as_f64().unwrap(),
    )
}

fn context(scenario: &Value) -> LedgerContext {
    let mut ctx = LedgerContext::with_cash(
        Money::from_major_f64(scenario["initial_cash"].as_f64().unwrap(), Currency::Inr).unwrap(),
    );
    for i in scenario["instruments"].as_array().unwrap() {
        ctx.add_instrument(instrument(i));
    }
    ctx
}

/// The scenario's optional `fill_costs` (`{"flat", "bps"}`); none when absent.
fn fill_costs(scenario: &Value) -> FillCosts {
    match scenario.get("fill_costs") {
        None => FillCosts::default(),
        Some(c) => FillCosts::new(c["flat"].as_f64().unwrap(), c["bps"].as_f64().unwrap()).unwrap(),
    }
}

/// Runs `strategy` over the scenario's messages and returns the outcome in
/// the fixture's JSON shape (observations are added by the caller).
fn run<S: Strategy>(strategy: S, scenario: &Value) -> (S, Value) {
    let mut execution = BarFillEngine::with_costs(fill_costs(scenario));
    let mut runner = StrategyRunner::with_context(strategy, execution.clone(), context(scenario));
    runner.on_start().unwrap();
    for m in scenario["events"].as_array().unwrap() {
        let msg: Message = serde_json::from_value(m.clone()).unwrap();
        Handler::on_event(&mut execution, msg.event(), msg.ts_init()).unwrap();
        Handler::on_event(&mut runner, msg.event(), msg.ts_init()).unwrap();
    }
    runner.on_stop().unwrap();

    let intents: Vec<Value> = runner
        .submitted()
        .iter()
        .map(|s| json!({"ts_init": s.ts_init, "intent": s.intent}))
        .collect();
    let fills = serde_json::to_value(runner.fills()).unwrap();
    let ctx = runner.context();
    let positions: Vec<Value> = ctx
        .positions()
        .into_iter()
        .map(|(id, q)| json!({"instrument_id": id, "quantity": q}))
        .collect();
    let outcome = json!({
        "intents": intents,
        "fills": fills,
        "positions": positions,
        "cash": ctx.cash(),
    });
    let (strategy, _, _) = runner.into_parts();
    (strategy, outcome)
}

fn run_scenario(scenario: &Value) -> Value {
    let p = &scenario["params"];
    let id = instrument_id(&p["instrument_id"]);
    let (observations, mut outcome) = match scenario["strategy"].as_str().unwrap() {
        "contract_probe" => {
            let (probe, outcome) = run(ContractProbe::new(id), scenario);
            (serde_json::to_value(probe.observations()).unwrap(), outcome)
        }
        "buy_and_hold" => {
            let s = BuyAndHold::new(id, p["quantity"].as_f64().unwrap());
            (json!([]), run(s, scenario).1)
        }
        "sma_crossover" => {
            let s = SmaCrossover::new(
                id,
                p["fast"].as_u64().unwrap() as usize,
                p["slow"].as_u64().unwrap() as usize,
                p["quantity"].as_f64().unwrap(),
            );
            (json!([]), run(s, scenario).1)
        }
        other => panic!("unknown strategy {other}"),
    };
    outcome["observations"] = observations;
    outcome
}

#[test]
fn fixture_header() {
    let doc = fixture();
    assert_eq!(doc["schema_version"], u64::from(SCHEMA_VERSION));
    assert_eq!(doc["type"], "StrategyConformance");
    assert_eq!(doc["fill_model"], "bar_close");
    assert_eq!(doc["scenarios"].as_array().unwrap().len(), 6);
}

#[test]
fn rust_runs_match_the_fixture() {
    let doc = fixture();
    for scenario in doc["scenarios"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let got = run_scenario(scenario);
        for key in ["intents", "fills", "observations", "positions", "cash"] {
            assert_eq!(got[key], scenario["expected"][key], "{name}: {key}");
        }
    }
}
