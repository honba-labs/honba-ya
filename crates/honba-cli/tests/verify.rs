//! `honba verify <manifest.json>` prints the compiled strategy IR as JSON
//! (docs/archive/plan.md E0-S8). Runs the built binary; hermetic.

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::{json, Value};

fn verify(name: &str, manifest: &Value) -> Output {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("honba-cli-verify");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.json"));
    fs::write(&path, manifest.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_honba"))
        .args(["verify", path.to_str().unwrap()])
        .output()
        .expect("failed to spawn the honba binary")
}

fn manifest() -> Value {
    json!({
        "api_version": "1.0.0",
        "name": "sma_crossover",
        "source_hash": "sha256:abc123",
        "universe": {"explicit": [{"symbol": "NIFTY50", "exchange": "NSE"}]},
        "subscriptions": {"instruments": [{"symbol": "NIFTY50", "exchange": "NSE"}]},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 20
    })
}

#[test]
fn a_valid_manifest_prints_its_ir_as_json() {
    let out = verify("valid", &manifest());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    let ir: Value = serde_json::from_slice(&out.stdout).expect("stdout is JSON");
    assert_eq!(ir["schema_version"], honba_messages::SCHEMA_VERSION);
    assert_eq!(ir["warmup_bars"], 20);
    assert_eq!(ir["manifest"]["name"], "sma_crossover");
    assert_eq!(
        ir["subscriptions"]["bars"],
        json!([{"symbol": "NIFTY50", "exchange": "NSE"}])
    );
    assert_eq!(
        ir["timeframes"],
        json!([{"interval": 1, "aggregation": "day"}])
    );
}

#[test]
fn an_invalid_manifest_fails_with_its_error_code_and_no_stdout() {
    let mut bad = manifest();
    bad["subscriptions"]["instruments"] = json!([]);
    let out = verify("no_subs", &bad);
    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no_subscriptions"));
}

#[test]
fn a_typo_in_the_manifest_is_rejected() {
    let mut bad = manifest();
    bad["warmup_barz"] = json!(5);
    let out = verify("typo", &bad);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("warmup_barz"));
}
