//! Tests for the strategy manifest (docs/archive/plan.md E0-S8, E4-S2).

use honba_messages::{BarAggregation, Exchange, InstrumentId};
use serde_json::json;

use crate::manifest::{
    ManifestError, ParameterSpec, STRATEGY_API_VERSION, StrategyManifest, Subscriptions,
    TimeframeSpec, Universe, param_range_to_schema,
};

fn any_id(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn daily() -> TimeframeSpec {
    TimeframeSpec::new(1, BarAggregation::Day)
}

fn sample() -> StrategyManifest {
    StrategyManifest::new(
        "sma_crossover",
        "sha256:abc123",
        Universe::Explicit(vec![any_id("NIFTY50")]),
        daily(),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![any_id("NIFTY50")],
        quotes: false,
        trades: false,
    })
    .with_warmup_bars(20)
    .with_schedule("session_open", "09:15:00+05:30")
}

#[test]
fn a_builder_produces_a_valid_manifest() {
    assert_eq!(sample().validate(), Ok(()));
}

#[test]
fn the_current_contract_version_is_stamped_on_construction() {
    assert_eq!(sample().api_version, STRATEGY_API_VERSION);
}

#[test]
fn an_empty_name_is_rejected() {
    let m = StrategyManifest::new("  ", "sha256:abc", Universe::Explicit(vec![]), daily());
    assert_eq!(m.validate(), Err(ManifestError::EmptyName));
}

#[test]
fn a_missing_source_hash_is_rejected() {
    // Without the hash a run cannot be tied to the code that produced it, so a
    // cached result could be silently reused for different logic.
    let m = StrategyManifest::new("sma", "", Universe::Explicit(vec![]), daily());
    assert_eq!(m.validate(), Err(ManifestError::EmptySourceHash));
}

#[test]
fn an_unknown_contract_version_is_rejected() {
    let mut m = sample();
    m.api_version = "99.0.0".into();
    assert_eq!(
        m.validate(),
        Err(ManifestError::UnsupportedApiVersion {
            found: "99.0.0".into()
        })
    );
}

#[test]
fn a_zero_timeframe_interval_is_rejected() {
    // A zero interval would never emit a bar, so the strategy would never act.
    let m = StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![any_id("NIFTY50")]),
        TimeframeSpec::new(0, BarAggregation::Day),
    );
    assert_eq!(m.validate(), Err(ManifestError::ZeroInterval));
}

#[test]
fn a_named_universe_must_be_resolved_before_running() {
    let m = StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Named("nifty50".into()),
        daily(),
    );
    assert_eq!(m.validate(), Err(ManifestError::NamedUniverseUnresolved));
}

#[test]
fn a_resolved_named_universe_validates() {
    let m = StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Named("nifty50".into()),
        daily(),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![any_id("RELIANCE"), any_id("TCS")],
        quotes: false,
        trades: false,
    });
    assert_eq!(m.validate(), Ok(()));
}

#[test]
fn instruments_are_the_union_of_universe_and_subscriptions_deduped() {
    let m = StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![any_id("TCS"), any_id("RELIANCE")]),
        daily(),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![any_id("TCS"), any_id("INFY")],
        quotes: false,
        trades: false,
    });
    assert_eq!(
        m.instruments(),
        vec![any_id("INFY"), any_id("RELIANCE"), any_id("TCS")]
    );
}

#[test]
fn subscriptions_report_whether_an_instrument_is_covered() {
    let subs = Subscriptions {
        instruments: vec![any_id("TCS")],
        quotes: true,
        trades: false,
    };
    assert!(subs.covers(&any_id("TCS")));
    assert!(!subs.covers(&any_id("INFY")));
}

#[test]
fn a_manifest_round_trips_through_json() {
    // The manifest is what a frontend posts and a server stores, so the wire
    // form has to be lossless.
    let m = sample();
    let text = serde_json::to_string(&m).unwrap();
    let back: StrategyManifest = serde_json::from_str(&text).unwrap();
    assert_eq!(m, back);
}

#[test]
fn omitting_schedules_keeps_them_absent_rather_than_null() {
    let m = StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![any_id("TCS")]),
        daily(),
    );
    let v = serde_json::to_value(&m).unwrap();
    assert!(v.get("schedules").is_none(), "got {v}");
    let back: StrategyManifest = serde_json::from_value(v).unwrap();
    assert!(back.schedules.is_empty());
}

#[test]
fn an_unknown_field_is_rejected() {
    let mut v = serde_json::to_value(sample()).unwrap();
    v["typo_field"] = serde_json::json!(1);
    assert!(serde_json::from_value::<StrategyManifest>(v).is_err());
}

#[test]
fn a_manifest_error_message_names_the_problem() {
    assert_eq!(
        ManifestError::ZeroInterval.to_string(),
        "timeframe interval must be > 0"
    );
    assert!(ManifestError::EmptyName.to_string().contains("name"));
}

#[test]
fn param_range_to_schema_parses_valid_range() {
    let spec = param_range_to_schema("10:50:5", json!(20)).expect("valid range should succeed");
    assert_eq!(spec.range.as_deref(), Some("10:50:5"));
    assert_eq!(spec.min, Some(10.0));
    assert_eq!(spec.max, Some(50.0));
    assert_eq!(spec.step, Some(5.0));
    assert_eq!(spec.default, json!(20));
    assert_eq!(
        spec.discrete_values(),
        vec![
            json!(10),
            json!(15),
            json!(20),
            json!(25),
            json!(30),
            json!(35),
            json!(40),
            json!(45),
            json!(50)
        ]
    );
}

#[test]
fn param_range_to_schema_rejects_default_outside_range() {
    // default below min
    let err = param_range_to_schema("10:50:5", json!(5)).unwrap_err();
    assert!(matches!(
        err,
        ManifestError::ParameterDefaultOutOfRange { .. }
    ));

    // default above max
    let err = param_range_to_schema("10:50:5", json!(60)).unwrap_err();
    assert!(matches!(
        err,
        ManifestError::ParameterDefaultOutOfRange { .. }
    ));
}

#[test]
fn param_range_to_schema_rejects_invalid_syntax_and_bounds() {
    // not 3 parts
    assert!(matches!(
        param_range_to_schema("10:50", json!(20)),
        Err(ManifestError::InvalidParameterRange { .. })
    ));

    // min > max
    assert!(matches!(
        param_range_to_schema("50:10:5", json!(20)),
        Err(ManifestError::InvalidParameterRange { .. })
    ));

    // negative step
    assert!(matches!(
        param_range_to_schema("10:50:-5", json!(20)),
        Err(ManifestError::InvalidParameterRange { .. })
    ));
}

#[test]
fn manifest_with_parameters_validates_and_rejects_out_of_range_default() {
    let valid_spec = ParameterSpec::new(json!(20)).with_range("10:50:5");
    let valid_manifest = sample().with_parameter("fast", valid_spec);
    assert_eq!(valid_manifest.validate(), Ok(()));

    let invalid_spec = ParameterSpec::new(json!(100)).with_range("10:50:5");
    let invalid_manifest = sample().with_parameter("fast", invalid_spec);
    let err = invalid_manifest.validate().unwrap_err();
    assert!(matches!(
        err,
        ManifestError::ParameterDefaultOutOfRange { ref name, .. } if name == "fast"
    ));
}
