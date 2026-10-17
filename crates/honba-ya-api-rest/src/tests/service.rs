//! Admission rules and defaults of the run service (no filesystem, no threads).

use std::sync::Mutex;
use std::time::Duration;

use honba_api::{compile_strategy, StrategyCatalog};
use honba_messages::{BarAggregation, ErrorCode, Exchange, InstrumentId};
use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec, Universe};

use crate::service::resolve_strategy;
use crate::{RunServiceConfig, StrategyRegistry, DEFAULT_MAX_QUEUED, DEFAULT_SHUTDOWN_GRACE};

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

fn catalog_with(name: &str) -> (Mutex<StrategyCatalog>, String) {
    let manifest = StrategyManifest::new(
        name,
        "sha256:abc",
        Universe::Explicit(vec![tcs()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs()],
        quotes: false,
        trades: false,
    });
    let mut catalog = StrategyCatalog::default();
    let id = compile_strategy(&mut catalog, manifest).unwrap().id;
    (Mutex::new(catalog), id)
}

fn resolve(
    catalog: &Mutex<StrategyCatalog>,
    strategy: &str,
    universe: &str,
) -> Result<(String, honba_strategy::StrategyIr), honba_messages::ErrorDetail> {
    resolve_strategy(
        catalog,
        &StrategyRegistry::builtin(),
        strategy,
        universe,
        "1d",
    )
}

#[test]
fn defaults_follow_the_adr() {
    let config = RunServiceConfig::default();
    assert_eq!(config.max_queued, 64);
    assert_eq!(DEFAULT_MAX_QUEUED, 64);
    assert_eq!(config.shutdown_grace, Duration::from_secs(10));
    assert_eq!(DEFAULT_SHUTDOWN_GRACE, Duration::from_secs(10));
    let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
    assert_eq!(config.max_concurrent, parallelism);
    assert_eq!(config.retention.keep_runs, 1_000);
    assert_eq!(config.retention.keep_days, 30);
}

#[test]
fn a_registered_name_resolves_to_itself_and_a_synthesised_ir() {
    let (catalog, _) = catalog_with("sma_crossover");
    let (id, ir) = resolve(&catalog, "sma_crossover", "TCS.NSE").unwrap();
    assert_eq!(id, "sma_crossover");
    assert_eq!(ir.manifest.name, "sma_crossover");
    assert_eq!(ir.universe.instruments, vec![tcs()]);
}

#[test]
fn a_catalog_id_with_a_registered_name_resolves_to_its_content_id_and_ir() {
    let (catalog, content_id) = catalog_with("sma_crossover");
    let (id, ir) = resolve(&catalog, &content_id, "TCS.NSE").unwrap();
    assert_eq!(id, content_id);
    assert_eq!(ir.manifest.name, "sma_crossover");
    assert_eq!(ir.manifest.source_hash, "sha256:abc");
}

#[test]
fn a_catalog_strategy_without_a_rust_implementation_is_a_typed_422() {
    let (catalog, content_id) = catalog_with("my_python_idea");
    let err = resolve(&catalog, &content_id, "TCS.NSE").unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    let context = err.context.unwrap();
    assert_eq!(context["field"], "strategy");
    assert_eq!(context["reason"], "no_rust_implementation");
}

#[test]
fn an_unknown_strategy_is_a_typed_422() {
    let (catalog, _) = catalog_with("sma_crossover");
    for unknown in ["nope", "sha256:deadbeef", ""] {
        let err = resolve(&catalog, unknown, "TCS.NSE").unwrap_err();
        assert_eq!(err.code, ErrorCode::ValidationInvalidRequest, "{unknown:?}");
        assert_eq!(err.context.unwrap()["field"], "strategy");
    }
}

#[test]
fn a_universe_that_is_not_one_instrument_is_refused_at_admission() {
    let (catalog, content_id) = catalog_with("sma_crossover");
    for strategy in ["sma_crossover", content_id.as_str()] {
        for universe in ["nifty50", "TCS.NSE,INFY.NSE", ""] {
            let err = resolve(&catalog, strategy, universe).unwrap_err();
            assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
            assert_eq!(err.context.unwrap()["field"], "universe", "{universe:?}");
        }
    }
}
