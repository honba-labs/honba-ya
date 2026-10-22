//! Tests for `crate::verify` (`POST /strategies/verify`).

use serde_json::json;

use crate::strategy_ir::{StrategyManifest, Subscriptions, TimeframeSpec, Universe};
use crate::wire::BarAggregation;
use crate::{ErrorCode, verify_strategy};
use honba_core::{Exchange, InstrumentId};

fn tcs() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "TCS")
}

fn manifest() -> StrategyManifest {
    StrategyManifest::new(
        "sma",
        "sha256:abc",
        Universe::Explicit(vec![tcs()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs()],
    })
    .with_warmup_bars(20)
}

#[test]
fn a_valid_manifest_verifies_to_its_ir() {
    let ir = verify_strategy(manifest()).expect("verifies");
    // The IR pins the subscriptions by instrument and the bar series by the
    // driving timeframe (the manifest's subscriptions have no bar series yet).
    assert_eq!(ir.subscriptions.instruments, vec![tcs()]);
    assert_eq!(
        ir.subscriptions.bars,
        vec![TimeframeSpec::new(1, BarAggregation::Day)]
    );
    assert_eq!(ir.warmup_bars, 20);
}

#[test]
fn a_manifest_without_subscriptions_is_a_validation_error_with_its_reason() {
    let mut m = manifest();
    m.subscriptions.instruments.clear();
    let err = verify_strategy(m).unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert!(!err.retryable);
    assert_eq!(err.context, Some(json!({"reason": "no_subscriptions"})));
}

#[test]
fn a_manifest_error_keeps_the_manifest_code() {
    let mut m = manifest();
    m.name.clear();
    let err = verify_strategy(m).unwrap_err();
    assert_eq!(err.context, Some(json!({"reason": "empty_name"})));
}
