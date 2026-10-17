//! Shared test-data builders.
//!
//! Use these where the concrete instrument or bar shape is irrelevant to the
//! test; keep explicit literals where the value is the point of the test.
//!
//! ```
//! use honba_testing::fixtures::{any_instrument, flat_bar, instrument};
//!
//! assert_eq!(any_instrument(), instrument("X"));
//! assert_eq!(flat_bar("X", 10.0, 1).close(), 10.0);
//! ```

use honba_messages::{
    Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, PriceType, UnixNanos,
};

/// Exchange used by every fixture (and by [`crate::VecFeed`]'s builders).
pub const TEST_EXCHANGE: &str = "TEST";

/// Instrument `symbol` on the [`TEST_EXCHANGE`].
pub fn instrument(symbol: &str) -> InstrumentId {
    InstrumentId::new(symbol, Exchange::new(TEST_EXCHANGE))
}

/// A placeholder instrument (`X` on the [`TEST_EXCHANGE`]) for tests where the
/// instrument does not matter.
pub fn any_instrument() -> InstrumentId {
    instrument("X")
}

/// One-minute, last-price bar type for `symbol` on the [`TEST_EXCHANGE`].
pub fn minute_bar_type(symbol: &str) -> BarType {
    BarType::new(
        instrument(symbol),
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    )
}

/// A flat one-minute bar: open, high, low and close all equal `close`,
/// volume 1, and both timestamps at `ts` nanoseconds.
pub fn flat_bar(symbol: &str, close: f64, ts: u64) -> Bar {
    let t = UnixNanos::from_u64(ts);
    Bar::new(
        minute_bar_type(symbol),
        close,
        close,
        close,
        close,
        1.0,
        t,
        t,
    )
}
