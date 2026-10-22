//! Unit tests for `crate::fixtures`.

use honba_core::{Exchange, Quantity};

use crate::fixtures::{TEST_EXCHANGE, any_instrument, flat_bar, instrument, utc_nanos};

#[test]
fn instrument_uses_the_test_exchange() {
    let id = instrument("NIFTY50");
    assert_eq!(id.symbol, "NIFTY50");
    assert_eq!(id.exchange, Exchange::Nse);
    assert_eq!(id.exchange.to_string(), TEST_EXCHANGE);
}

#[test]
fn instrument_uppercases_the_symbol() {
    assert_eq!(instrument("nifty").symbol, "NIFTY");
}

#[test]
fn any_instrument_is_stable() {
    assert_eq!(any_instrument(), instrument("X"));
    assert_eq!(any_instrument(), any_instrument());
}

#[test]
fn flat_bar_has_ohlc_at_close_volume_one_and_timestamp_at_ts() {
    let bar = flat_bar("X", 101.5, 7);
    assert_eq!(bar.instrument, instrument("X"));
    assert_eq!(
        (
            bar.open.to_string(),
            bar.high.to_string(),
            bar.low.to_string(),
            bar.close.to_string()
        ),
        ("101.5".to_owned(), "101.5".to_owned(), "101.5".to_owned(), "101.5".to_owned())
    );
    assert_eq!(bar.volume, Quantity::from_u64(1));
    assert_eq!(bar.timestamp, utc_nanos(7));
}
