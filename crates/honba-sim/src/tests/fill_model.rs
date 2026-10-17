//! Unit tests for `crate::fill_model` (E3-S2).

use honba_messages::OrderSide;
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::fill_model::{NoSlippage, SlippageKind, SlippageModel};
use crate::FillModel;

fn seeded(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

#[test]
fn no_slippage_is_the_identity() {
    let mut rng = seeded(0);
    assert_eq!(NoSlippage.price(OrderSide::Buy, 100.0, &mut rng), 100.0);
    assert_eq!(NoSlippage.price(OrderSide::Sell, 100.0, &mut rng), 100.0);
}

#[test]
fn static_adverse_slippage_moves_against_the_side() {
    let mut rng = seeded(0);
    let model = SlippageModel::Static {
        bps: 10.0,
        kind: SlippageKind::Adverse,
    };
    // 10 bps of 100.0 is 0.1: a buy pays more, a sell receives less.
    assert_eq!(model.price(OrderSide::Buy, 100.0, &mut rng), 100.1);
    assert_eq!(model.price(OrderSide::Sell, 100.0, &mut rng), 99.9);
}

#[test]
fn static_favorable_slippage_moves_with_the_side() {
    let mut rng = seeded(0);
    let model = SlippageModel::Static {
        bps: 10.0,
        kind: SlippageKind::Favorable,
    };
    assert_eq!(model.price(OrderSide::Buy, 100.0, &mut rng), 99.9);
    assert_eq!(model.price(OrderSide::Sell, 100.0, &mut rng), 100.1);
}

#[test]
fn seeded_slippage_is_reproducible_and_seed_aware() {
    let model = SlippageModel::Seed { bps: 5.0, seed: 42 };
    let draw = || model.price(OrderSide::Buy, 100.0, &mut seeded(42));
    assert_eq!(draw(), draw(), "same seed, same fill price");

    let other = SlippageModel::Seed { bps: 5.0, seed: 7 };
    assert_ne!(draw(), other.price(OrderSide::Buy, 100.0, &mut seeded(7)));

    assert_eq!(model.seed(), Some(42));
    let fixed = SlippageModel::Static {
        bps: 1.0,
        kind: SlippageKind::Adverse,
    };
    assert_eq!(fixed.seed(), None);
}

#[test]
fn seeded_slippage_is_unbiased_over_many_draws() {
    // The move is a signed `Normal(0, bps)`, not always adverse: its mean is zero.
    let model = SlippageModel::Seed { bps: 10.0, seed: 1 };
    let mut rng = seeded(1);
    let n = 20_000;
    let mean: f64 = (0..n)
        .map(|_| model.price(OrderSide::Buy, 100.0, &mut rng) - 100.0)
        .sum::<f64>()
        / n as f64;
    assert!(
        mean.abs() < 0.05,
        "mean slippage {mean} should be near zero"
    );
}
