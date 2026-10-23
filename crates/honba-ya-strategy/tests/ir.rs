//! Integration: shared manifest vectors -> `StrategyIr::compile` -> JSON ->
//! the runner, through the public API only (docs/archive/plan.md E0-S8).

use std::path::PathBuf;

use honba_engine::{ExecutionEngine, Result};
use honba_entities::Trade;
use honba_messages::{Order, SCHEMA_VERSION};
use honba_strategy::{BuyAndHold, StrategyIr, StrategyManifest, StrategyRunner};
use serde_json::Value;

/// Accepts every order, never fills.
struct NeverFills;

impl ExecutionEngine for NeverFills {
    fn submit(&mut self, _order: Order) -> Result<()> {
        Ok(())
    }
    fn cancel(&mut self, _order_id: &str, _now: honba_messages::UnixNanos) -> Result<()> {
        Ok(())
    }
    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

fn manifest_vectors() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/conformance/strategy_manifest.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn every_shared_manifest_vector_compiles_or_fails_with_its_code() {
    let doc = manifest_vectors();
    let mut compiled = 0;
    for case in doc["cases"].as_array().unwrap() {
        let label = case["name"].as_str().unwrap();
        let Ok(manifest) = serde_json::from_value::<StrategyManifest>(case["value"].clone()) else {
            assert_eq!(case["error"], "deserialize", "{label}");
            continue;
        };
        match (
            case["error"].as_str(),
            StrategyIr::compile(manifest.clone()),
        ) {
            (None, Ok(ir)) => {
                assert_eq!(ir.manifest, manifest, "{label}");
                assert_eq!(ir.warmup_bars, manifest.warmup_bars, "{label}");
                assert_eq!(ir.universe.instruments, manifest.instruments(), "{label}");
                compiled += 1;
            }
            (Some(code), Err(e)) => assert_eq!(e.code(), code, "{label}"),
            (want, got) => panic!("{label}: expected {want:?}, got {got:?}"),
        }
    }
    assert!(compiled >= 2, "valid vectors compiled: {compiled}");
}

#[test]
fn the_compiled_ir_crosses_json_and_configures_the_runner() {
    let doc = manifest_vectors();
    let valid = doc["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "valid_with_warmup")
        .unwrap();
    let manifest: StrategyManifest = serde_json::from_value(valid["value"].clone()).unwrap();
    let ir = StrategyIr::compile(manifest).unwrap();

    let text = serde_json::to_string(&ir).unwrap();
    let doc: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(doc["schema_version"], SCHEMA_VERSION);
    assert_eq!(doc["warmup_bars"], 20);
    assert_eq!(
        doc["subscriptions"]["bars"],
        serde_json::json!([{"symbol": "NIFTY50", "exchange": "NSE"}])
    );
    let back: StrategyIr = serde_json::from_str(&text).unwrap();
    assert_eq!(back, ir);

    let first = back.subscriptions.bars[0].clone();
    let runner = StrategyRunner::new(BuyAndHold::new(first, 1.0), NeverFills)
        .with_warmup_bars(back.warmup_bars);
    assert!(runner.warming_up());
}
