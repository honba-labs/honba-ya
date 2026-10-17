//! Tests for `crate::strategies` (`POST /strategies`, `GET /strategies`).

use honba_messages::{BarAggregation, Exchange, InstrumentId};
use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec, Universe};
use serde_json::json;

use crate::{
    compile_strategy, list_strategies, parse_compile_request, ErrorCode, StrategyCatalog,
    MAX_COMPILED_STRATEGIES,
};

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

fn manifest_named(name: &str, hash: &str) -> StrategyManifest {
    StrategyManifest::new(
        name,
        hash,
        Universe::Explicit(vec![tcs()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs()],
        quotes: false,
        trades: false,
    })
    .with_warmup_bars(20)
}

fn manifest() -> StrategyManifest {
    manifest_named("sma", "sha256:abc")
}

#[test]
fn compiling_returns_the_verified_ir_under_a_content_id() {
    let mut catalog = StrategyCatalog::default();
    let compiled = compile_strategy(&mut catalog, manifest()).expect("compiles");
    assert!(compiled.id.starts_with("sha256:"), "{}", compiled.id);
    assert_eq!(compiled.id.len(), "sha256:".len() + 64);
    assert_eq!(compiled.ir.warmup_bars, 20);
    assert_eq!(compiled.ir.manifest.name, "sma");
}

#[test]
fn the_id_is_deterministic_and_covers_the_whole_manifest() {
    let id = |m| {
        compile_strategy(&mut StrategyCatalog::default(), m)
            .unwrap()
            .id
    };
    assert_eq!(id(manifest()), id(manifest()));
    // Same source hash, different declaration: a different strategy.
    assert_ne!(id(manifest()), id(manifest().with_warmup_bars(21)));
}

#[test]
fn compiling_the_same_manifest_twice_stores_it_once() {
    let mut catalog = StrategyCatalog::default();
    let first = compile_strategy(&mut catalog, manifest()).unwrap();
    let second = compile_strategy(&mut catalog, manifest()).unwrap();
    assert_eq!(first, second);
    assert_eq!(list_strategies(&catalog).strategies.len(), 1);
}

#[test]
fn a_manifest_that_does_not_verify_is_not_stored() {
    let mut catalog = StrategyCatalog::default();
    let mut bad = manifest();
    bad.subscriptions.instruments.clear();
    let err = compile_strategy(&mut catalog, bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(err.context, Some(json!({"reason": "no_subscriptions"})));
    assert!(list_strategies(&catalog).strategies.is_empty());
}

#[test]
fn the_list_is_ordered_by_id_whatever_the_insertion_order() {
    let mut catalog = StrategyCatalog::default();
    for n in 0..5 {
        compile_strategy(&mut catalog, manifest_named("s", &format!("sha256:{n}"))).unwrap();
    }
    let ids: Vec<String> = list_strategies(&catalog)
        .strategies
        .into_iter()
        .map(|s| s.id)
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
    assert_eq!(ids.len(), 5);
}

#[test]
fn a_full_catalog_rejects_a_new_strategy_but_still_accepts_a_known_one() {
    let mut catalog = StrategyCatalog::with_limit(2);
    compile_strategy(&mut catalog, manifest_named("a", "sha256:a")).unwrap();
    compile_strategy(&mut catalog, manifest_named("b", "sha256:b")).unwrap();
    let err = compile_strategy(&mut catalog, manifest_named("c", "sha256:c")).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(
        err.context,
        Some(json!({"reason": "catalog_full", "limit": 2}))
    );
    assert!(compile_strategy(&mut catalog, manifest_named("a", "sha256:a")).is_ok());
    assert_eq!(catalog.len(), 2);
}

#[test]
fn the_default_limit_is_the_documented_constant() {
    assert_eq!(StrategyCatalog::default().limit(), MAX_COMPILED_STRATEGIES);
}

#[test]
fn a_request_with_a_manifest_parses() {
    let body = json!({"manifest": serde_json::to_value(manifest()).unwrap()});
    assert_eq!(parse_compile_request(body).unwrap().manifest, manifest());
}

#[test]
fn source_code_is_unsupported_with_its_own_reason() {
    for key in ["code", "source"] {
        let body = json!({ key: "class S: pass" });
        let err = parse_compile_request(body).unwrap_err();
        assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
        assert_eq!(
            err.context,
            Some(json!({"reason": "source_unsupported"})),
            "{key}"
        );
    }
}

#[test]
fn a_malformed_request_is_a_validation_error() {
    for body in [
        json!({}),
        json!([]),
        json!({"manifest": 1}),
        json!({"manifest": {}, "x": 1}),
    ] {
        let err = parse_compile_request(body.clone()).unwrap_err();
        assert_eq!(err.code, ErrorCode::ValidationInvalidRequest, "{body}");
    }
}
