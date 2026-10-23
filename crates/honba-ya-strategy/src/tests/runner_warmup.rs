//! Unit tests for the runner warm-up gate (cross-language vectors: `tests/warmup.rs`).

use honba_core::{MarketEvent, Quantity};
use honba_testing::fixtures::{any_instrument, flat_bar, utc_nanos};

use crate::{BuyAndHold, StrategyContext, StrategyRunner};

fn runner(warmup: u32) -> StrategyRunner<BuyAndHold> {
    StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)))
        .with_warmup_bars(warmup)
}

#[test]
fn the_default_runner_has_no_warmup() {
    let r = StrategyRunner::new(BuyAndHold::new(any_instrument(), Quantity::from_u64(1)));
    assert_eq!(r.warmup_bars(), 0);
    assert!(!r.warming_up());
}

#[test]
fn warming_up_ends_after_the_last_warmup_bar() {
    let mut r = runner(2);
    assert!(r.warming_up(), "before the first bar");
    for (ts, still) in [(1_u64, true), (2, true), (3, false)] {
        let bar = flat_bar("X", 10.0, ts);
        r.on_event(&MarketEvent::Bar(bar), utc_nanos(ts)).unwrap();
        assert_eq!(r.warming_up(), still, "after bar {ts}");
    }
}

#[test]
fn a_repeated_ts_init_is_one_driving_bar() {
    let mut r = runner(1);
    let bar = flat_bar("X", 10.0, 5);
    let ev = MarketEvent::Bar(bar);
    r.on_event(&ev, utc_nanos(5)).unwrap();
    r.on_event(&ev, utc_nanos(5)).unwrap();
    assert!(r.warming_up());
    assert!(r.submitted().is_empty());
    assert!(!r.suppressed().is_empty());
    assert!(!r.context().busy(&any_instrument()));
}
