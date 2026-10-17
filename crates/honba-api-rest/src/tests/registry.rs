//! The Rust-registered strategy set.

use honba_messages::{BarAggregation, ErrorCode, Exchange, InstrumentId};
use honba_strategy::Universe;

use crate::StrategyRegistry;

fn tcs() -> InstrumentId {
    InstrumentId::new("TCS", Exchange::new("NSE"))
}

#[test]
fn the_builtin_set_is_the_three_reference_strategies() {
    assert_eq!(
        StrategyRegistry::builtin().names(),
        vec!["buy_and_hold", "rsi_reversal", "sma_crossover"]
    );
}

#[test]
fn contains_is_exact() {
    let registry = StrategyRegistry::builtin();
    assert!(registry.contains("sma_crossover"));
    assert!(!registry.contains("SMA_CROSSOVER"));
    assert!(!registry.contains("sha256:abc"));
    assert!(!registry.contains(""));
}

#[test]
fn build_returns_a_strategy_carrying_the_registered_name() {
    let registry = StrategyRegistry::builtin();
    for name in registry.names() {
        let strategy = registry.build(name, &tcs(), 7).unwrap();
        assert!(!strategy.name().is_empty(), "{name}");
    }
    assert!(registry.build("nope", &tcs(), 7).is_none());
}

#[test]
fn the_ir_pins_a_one_instrument_universe_and_the_bar_timeframe() {
    let ir = StrategyRegistry::builtin()
        .ir_for("sma_crossover", &tcs(), "5m")
        .unwrap();
    assert_eq!(ir.manifest.name, "sma_crossover");
    assert_eq!(ir.manifest.universe, Universe::Explicit(vec![tcs()]));
    assert_eq!(ir.subscriptions.bars, vec![tcs()]);
    assert_eq!(ir.driving_timeframe.interval, 5);
    assert_eq!(ir.driving_timeframe.aggregation, BarAggregation::Minute);
}

#[test]
fn the_ir_is_stable_for_equal_inputs() {
    let registry = StrategyRegistry::builtin();
    let a = registry.ir_for("buy_and_hold", &tcs(), "1d").unwrap();
    let b = registry.ir_for("buy_and_hold", &tcs(), "1d").unwrap();
    assert_eq!(a, b);
}

#[test]
fn an_unregistered_name_or_bad_bar_spec_is_a_typed_422() {
    let registry = StrategyRegistry::builtin();
    let err = registry.ir_for("nope", &tcs(), "1d").unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
    assert_eq!(err.context.unwrap()["field"], "strategy");
    let err = registry.ir_for("sma_crossover", &tcs(), "zz").unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationInvalidRequest);
}
