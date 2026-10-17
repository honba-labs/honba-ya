//! Tests for the run-configuration types (docs/archive/plan.md E0-S4).

use crate::*;

use honba_messages::{BarAggregation, Exchange, InstrumentId};
use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec};

fn any_id(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn valid_strategy() -> StrategyManifest {
    StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![any_id("TCS")]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![any_id("TCS")],
        quotes: false,
        trades: false,
    })
}

fn valid_config() -> BacktestRunConfig {
    BacktestRunConfig {
        strategy: valid_strategy(),
        data: DataSourceConfig::default(),
        execution: ExecutionConfig::default(),
        account: AccountConfig::default(),
        seed: 42,
        risk: RiskLimits::default(),
    }
}

#[test]
fn a_default_config_is_valid() {
    assert_eq!(valid_config().validate(), Ok(()));
}

#[test]
fn defaults_are_applied_for_omitted_sections() {
    let text = r#"{"strategy": {
        "api_version": "1.0.0", "name": "sma", "source_hash": "h",
        "universe": {"explicit": []},
        "subscriptions": {"instruments": [], "quotes": false, "trades": false},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 0
    }, "seed": 7}"#;
    let cfg: BacktestRunConfig = serde_json::from_str(text).expect("optional sections default");
    assert_eq!(cfg.execution.fill_model, FillModel::BarFill);
    assert_eq!(cfg.account.currency, "INR");
    assert_eq!(cfg.seed, 7);
}

#[test]
fn an_invalid_manifest_fails_the_config() {
    let mut cfg = valid_config();
    cfg.strategy.name = String::new();
    assert!(matches!(
        cfg.validate(),
        Err(RunConfigError::Strategy(
            honba_strategy::ManifestError::EmptyName
        ))
    ));
}

#[test]
fn a_zero_seed_is_rejected() {
    // Seed 0 is allowed by the type but makes runs indistinguishable.
    let mut cfg = valid_config();
    cfg.seed = 0;
    assert_eq!(cfg.validate(), Err(RunConfigError::ZeroSeed));
}

#[test]
fn negative_slippage_is_rejected() {
    let mut cfg = valid_config();
    cfg.execution.slippage_multiplier = -1.0;
    assert_eq!(cfg.validate(), Err(RunConfigError::NegativeSlippage));
}

#[test]
fn a_non_positive_or_non_finite_starting_cash_is_rejected() {
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut cfg = valid_config();
        cfg.account.starting_cash = bad;
        assert_eq!(
            cfg.validate(),
            Err(RunConfigError::InvalidStartingCash),
            "accepted {bad}"
        );
    }
}

#[test]
fn a_blank_local_data_root_is_rejected() {
    let mut cfg = valid_config();
    cfg.data = DataSourceConfig::Local { root: "  ".into() };
    assert_eq!(cfg.validate(), Err(RunConfigError::EmptyDataRoot));
}

#[test]
fn the_config_round_trips_through_json() {
    let cfg = valid_config();
    let text = serde_json::to_string(&cfg).unwrap();
    assert_eq!(
        serde_json::from_str::<BacktestRunConfig>(&text).unwrap(),
        cfg
    );
}

#[test]
fn a_run_config_round_trips_its_embedded_manifest() {
    // The manifest is nested in the config, so its own contract has to hold
    // inside the larger document too.
    let cfg = valid_config();
    let v = serde_json::to_value(&cfg).unwrap();
    assert_eq!(v["strategy"]["name"], serde_json::json!("sma"));
    assert_eq!(v["strategy"]["api_version"], serde_json::json!("1.0.0"));
}

#[test]
fn a_resolved_universe_is_sorted_and_deduped() {
    let r = ResolvedUniverse::new(
        "nifty50",
        vec![any_id("TCS"), any_id("INFY"), any_id("TCS")],
    );
    assert_eq!(r.instruments, vec![any_id("INFY"), any_id("TCS")]);
}

#[test]
fn applying_a_resolved_universe_overwrites_a_named_one() {
    let mut manifest = StrategyManifest::new(
        "sma",
        "h",
        Universe::Named("nifty50".into()),
        TimeframeSpec::new(1, BarAggregation::Day),
    );
    assert!(manifest.validate().is_err(), "named universe is unresolved");
    ResolvedUniverse::new("nifty50", vec![any_id("TCS")]).apply(&mut manifest);
    assert_eq!(manifest.validate(), Ok(()));
    assert_eq!(manifest.universe, Universe::Explicit(vec![any_id("TCS")]));
}

#[test]
fn a_config_reports_the_instruments_it_covers() {
    assert_eq!(valid_config().instruments(), vec![any_id("TCS")]);
}

#[test]
fn an_unknown_config_field_is_rejected() {
    let mut v = serde_json::to_value(valid_config()).unwrap();
    v["seedd"] = serde_json::json!(1);
    assert!(serde_json::from_value::<BacktestRunConfig>(v).is_err());
}

#[test]
fn the_error_display_names_the_problem() {
    assert_eq!(
        RunConfigError::ZeroSeed.to_string(),
        "seed must be non-zero"
    );
}

#[test]
fn fill_model_l2_is_a_wire_variant() {
    let model: FillModel = serde_json::from_str("\"l2\"").expect("l2 parses");
    assert_eq!(model, FillModel::L2);
    assert_eq!(serde_json::to_string(&FillModel::L2).unwrap(), "\"l2\"");
}
