//! Tests for the verified strategy IR (docs/archive/plan.md E0-S8).

use honba_messages::{BarAggregation, Exchange, InstrumentId, SCHEMA_VERSION};

use crate::ir::{IrError, StrategyIr};
use crate::manifest::{ManifestError, StrategyManifest, Subscriptions, TimeframeSpec, Universe};

fn id(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new("NSE"))
}

fn five_min() -> TimeframeSpec {
    TimeframeSpec::new(5, BarAggregation::Minute)
}

fn subs(ids: &[&str], quotes: bool, trades: bool) -> Subscriptions {
    Subscriptions {
        instruments: ids.iter().map(|s| id(s)).collect(),
        quotes,
        trades,
    }
}

fn explicit(universe: &[&str], subscribed: &[&str]) -> StrategyManifest {
    StrategyManifest::new(
        "pairs",
        "sha256:abc",
        Universe::Explicit(universe.iter().map(|s| id(s)).collect()),
        five_min(),
    )
    .with_subscriptions(subs(subscribed, true, false))
    .with_warmup_bars(30)
}

#[test]
fn compiling_an_explicit_manifest_resolves_every_channel() {
    let manifest = explicit(&["TCS", "INFY", "TCS"], &["TCS", "INFY"]);
    let ir = StrategyIr::compile(manifest.clone()).unwrap();
    assert_eq!(ir.schema_version, SCHEMA_VERSION);
    assert_eq!(ir.strategy_api_version, manifest.api_version);
    assert_eq!(ir.universe.named, None);
    assert_eq!(ir.universe.instruments, vec![id("INFY"), id("TCS")]);
    assert_eq!(ir.subscriptions.bars, vec![id("INFY"), id("TCS")]);
    assert_eq!(ir.subscriptions.quotes, vec![id("INFY"), id("TCS")]);
    assert!(ir.subscriptions.trades.is_empty());
    assert_eq!(ir.driving_timeframe, five_min());
    assert_eq!(ir.timeframes, vec![five_min()]);
    assert_eq!(ir.warmup_bars, 30);
    assert_eq!(ir.manifest, manifest);
}

#[test]
fn a_named_universe_resolves_to_its_subscribed_instruments() {
    let manifest =
        StrategyManifest::new("alpha", "h", Universe::Named("NIFTY50".into()), five_min())
            .with_subscriptions(subs(&["RELIANCE", "HDFCBANK"], false, true));
    let ir = StrategyIr::compile(manifest).unwrap();
    assert_eq!(ir.universe.named.as_deref(), Some("NIFTY50"));
    assert_eq!(
        ir.universe.instruments,
        vec![id("HDFCBANK"), id("RELIANCE")]
    );
    assert_eq!(ir.subscriptions.trades, ir.universe.instruments);
    assert_eq!(ir.warmup_bars, 0);
}

#[test]
fn an_invalid_manifest_does_not_compile() {
    let mut manifest = explicit(&["TCS"], &["TCS"]);
    manifest.name = " ".into();
    assert_eq!(
        StrategyIr::compile(manifest),
        Err(IrError::Manifest(ManifestError::EmptyName))
    );
}

#[test]
fn a_manifest_that_subscribes_to_nothing_does_not_compile() {
    // It would run, receive no events, and look like a strategy that never trades.
    let manifest = explicit(&["TCS"], &[]);
    assert_eq!(StrategyIr::compile(manifest), Err(IrError::NoSubscriptions));
}

#[test]
fn a_subscription_outside_an_explicit_universe_does_not_compile() {
    let manifest = explicit(&["TCS"], &["TCS", "INFY"]);
    assert_eq!(
        StrategyIr::compile(manifest),
        Err(IrError::SubscriptionOutsideUniverse(id("INFY")))
    );
}

#[test]
fn errors_have_stable_codes() {
    assert_eq!(IrError::NoSubscriptions.code(), "no_subscriptions");
    assert_eq!(
        IrError::SubscriptionOutsideUniverse(id("X")).code(),
        "subscription_outside_universe"
    );
    assert_eq!(
        IrError::Manifest(ManifestError::ZeroInterval).code(),
        "zero_interval"
    );
}

#[test]
fn the_ir_round_trips_through_json() {
    let ir = StrategyIr::compile(explicit(&["TCS"], &["TCS"])).unwrap();
    let json = serde_json::to_value(&ir).unwrap();
    assert_eq!(json["schema_version"], SCHEMA_VERSION);
    assert_eq!(json["warmup_bars"], 30);
    let back: StrategyIr = serde_json::from_value(json).unwrap();
    assert_eq!(back, ir);
}

#[test]
fn the_ir_is_a_record_unknown_fields_are_ignored_but_not_unknown_versions() {
    let ir = StrategyIr::compile(explicit(&["TCS"], &["TCS"])).unwrap();
    let mut json = serde_json::to_value(&ir).unwrap();
    json["compiled_by"] = "honba 9".into();
    assert_eq!(
        serde_json::from_value::<StrategyIr>(json.clone()).unwrap(),
        ir
    );
    json["schema_version"] = (SCHEMA_VERSION + 1).into();
    assert!(serde_json::from_value::<StrategyIr>(json).is_err());
}
