//! Shared test-data builders.
//!
//! Use these where the concrete instrument or bar shape is irrelevant to the
//! test; keep explicit literals where the value is the point of the test.
//!
//! ```
//! use honba_testing::fixtures::{any_instrument, flat_bar, instrument};
//!
//! assert_eq!(any_instrument(), instrument("X"));
//! assert_eq!(flat_bar("X", 10.0, 1).close.to_string(), "10");
//! ```

use honba_core::chrono::{DateTime, Utc};
use honba_core::rust_decimal::Decimal;
use honba_core::{Bar, Exchange, InstrumentId, Price, Quantity};

/// Exchange label used by every fixture (and by [`crate::VecFeed`]'s builders).
pub const TEST_EXCHANGE: &str = "NSE";

/// The canonical exchange every fixture runs on (the value behind [`TEST_EXCHANGE`]).
pub fn test_exchange() -> Exchange {
    Exchange::Nse
}

/// Builds a UTC timestamp from `nanos` nanoseconds since the Unix epoch.
///
/// # Panics
///
/// Panics only for values outside the representable `DateTime<Utc>` range, which no `i64`
/// nanosecond count can reach.
pub fn utc_from_nanos(nanos: i64) -> DateTime<Utc> {
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, sub).expect("fixture timestamp out of range")
}

/// Builds a UTC timestamp from `u64` nanoseconds since the Unix epoch.
pub fn utc_nanos(nanos: u64) -> DateTime<Utc> {
    utc_from_nanos(nanos as i64)
}

/// Converts an `f64` fixture literal into an exact [`Decimal`].
///
/// # Panics
///
/// Panics if the value is not a finite decimal literal.
pub fn exact_decimal(value: f64) -> Decimal {
    value
        .to_string()
        .parse::<Decimal>()
        .unwrap_or_else(|_| panic!("fixture f64 must be a finite decimal, got {value}"))
}

/// Instrument `symbol` on the [`TEST_EXCHANGE`] (canonical [`Exchange`]).
pub fn instrument(symbol: &str) -> InstrumentId {
    InstrumentId::new(test_exchange(), symbol)
}

/// A placeholder instrument (`X` on the [`TEST_EXCHANGE`]) for tests where the
/// instrument does not matter.
pub fn any_instrument() -> InstrumentId {
    instrument("X")
}

/// A flat bar: open, high, low and close all equal `close`, volume 1, and the
/// timestamp at `ts` nanoseconds since the Unix epoch.
pub fn flat_bar(symbol: &str, close: f64, ts: u64) -> Bar {
    let price = Price::new(exact_decimal(close));
    Bar {
        instrument: instrument(symbol),
        open: price,
        high: price,
        low: price,
        close: price,
        volume: Quantity::from_u64(1),
        timestamp: utc_nanos(ts),
    }
}
