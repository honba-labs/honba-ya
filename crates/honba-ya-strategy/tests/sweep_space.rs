//! Integration test for E4-S2: sweep space extraction and Cartesian parameter grid generation.

use honba_messages::{BarAggregation, Exchange, InstrumentId};
use honba_strategy::{
    ParameterSpec, StrategyManifest, Subscriptions, TimeframeSpec, Universe,
    parameter_grid_from_manifest, sweep_space_from_manifest,
};
use serde_json::json;

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

#[test]
fn sweep_space_from_manifest_generates_grid_and_combinations() {
    let manifest = StrategyManifest::new(
        "sma_crossover",
        "sha256:source123",
        Universe::Explicit(vec![tcs()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs()],
        quotes: false,
        trades: false,
    })
    .with_warmup_bars(20)
    .with_parameter(
        "fast",
        ParameterSpec::new(json!(10))
            .with_range("10:30:10")
            .with_description("Fast SMA period"),
    )
    .with_parameter(
        "slow",
        ParameterSpec::new(json!(50))
            .with_range("50:100:25")
            .with_description("Slow SMA period"),
    );

    let space = sweep_space_from_manifest(&manifest).expect("sweep space generation must succeed");
    assert_eq!(space.len(), 2);
    assert_eq!(
        space.get("fast").unwrap(),
        &vec![json!(10), json!(20), json!(30)]
    );
    assert_eq!(
        space.get("slow").unwrap(),
        &vec![json!(50), json!(75), json!(100)]
    );

    let grid = parameter_grid_from_manifest(&manifest).expect("grid generation must succeed");
    assert_eq!(
        grid.len(),
        3 * 3,
        "Cartesian product should yield 9 combinations"
    );

    // First trial
    assert_eq!(grid[0].get("fast").unwrap(), &json!(10));
    assert_eq!(grid[0].get("slow").unwrap(), &json!(50));

    // Last trial
    assert_eq!(grid[8].get("fast").unwrap(), &json!(30));
    assert_eq!(grid[8].get("slow").unwrap(), &json!(100));
}

#[test]
fn sweep_space_from_manifest_without_parameters_returns_empty() {
    let manifest = StrategyManifest::new(
        "buy_and_hold",
        "sha256:source456",
        Universe::Explicit(vec![tcs()]),
        TimeframeSpec::new(1, BarAggregation::Day),
    )
    .with_subscriptions(Subscriptions {
        instruments: vec![tcs()],
        quotes: false,
        trades: false,
    });

    let space = sweep_space_from_manifest(&manifest).unwrap();
    assert!(space.is_empty());

    let grid = parameter_grid_from_manifest(&manifest).unwrap();
    assert!(grid.is_empty());
}
