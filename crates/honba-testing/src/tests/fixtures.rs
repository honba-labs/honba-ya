//! Unit tests for `crate::fixtures`.

use honba_messages::{BarAggregation, Exchange, PriceType, UnixNanos};

use crate::fixtures::{any_instrument, flat_bar, instrument, minute_bar_type, TEST_EXCHANGE};

#[test]
fn instrument_uses_the_test_exchange() {
    let id = instrument("NIFTY50");
    assert_eq!(id.symbol(), "NIFTY50");
    assert_eq!(id.exchange(), &Exchange::new(TEST_EXCHANGE));
}

#[test]
fn any_instrument_is_stable() {
    assert_eq!(any_instrument(), instrument("X"));
    assert_eq!(any_instrument(), any_instrument());
}

#[test]
fn minute_bar_type_is_one_minute_last_price() {
    let bt = minute_bar_type("X");
    assert_eq!(bt.instrument_id(), &instrument("X"));
    assert_eq!(bt.spec().step(), 1);
    assert_eq!(bt.spec().aggregation(), BarAggregation::Minute);
    assert_eq!(bt.spec().price_type(), PriceType::Last);
}

#[test]
fn flat_bar_has_ohlc_at_close_and_both_timestamps_at_ts() {
    let bar = flat_bar("X", 101.5, 7);
    assert_eq!(bar.bar_type(), &minute_bar_type("X"));
    assert_eq!(
        (bar.open(), bar.high(), bar.low(), bar.close()),
        (101.5, 101.5, 101.5, 101.5)
    );
    assert_eq!(bar.volume(), 1.0);
    assert_eq!(bar.ts_event(), UnixNanos::from_u64(7));
    assert_eq!(bar.ts_init(), UnixNanos::from_u64(7));
    assert_eq!(bar.validate(), Ok(()));
}
