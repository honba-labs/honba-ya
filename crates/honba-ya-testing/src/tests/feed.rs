//! Unit tests for `crate::feed`.

use honba_engine::DataFeed;
use honba_messages::{AggressorSide, Event, UnixNanos};

use crate::fixtures::{flat_bar, instrument};
use crate::VecFeed;

#[test]
fn empty_feed_yields_nothing() {
    assert!(VecFeed::empty().next().unwrap().is_none());
}

#[test]
fn feed_yields_messages_in_insertion_order_not_time_order() {
    let mut feed = VecFeed::new(vec![VecFeed::bar("X", 1.0, 5), VecFeed::bar("X", 2.0, 1)]);
    let ts: Vec<u64> = std::iter::from_fn(|| feed.next().unwrap())
        .map(|m| m.ts_init().as_u64())
        .collect();
    assert_eq!(ts, [5, 1]);
}

#[test]
fn quote_builder_sets_prices_unit_sizes_and_timestamps() {
    let msg = VecFeed::quote("ABC", 9.5, 10.5, 3);
    assert_eq!(msg.ts_init(), UnixNanos::from_u64(3));
    let Event::Quote(q) = msg.event() else {
        panic!("expected a quote, got {:?}", msg.event());
    };
    assert_eq!(q.instrument_id(), &instrument("ABC"));
    assert_eq!((q.bid_price(), q.ask_price()), (9.5, 10.5));
    assert_eq!((q.bid_size(), q.ask_size()), (1.0, 1.0));
    assert_eq!(q.ts_event(), UnixNanos::from_u64(3));
}

#[test]
fn bar_builder_wraps_a_flat_bar() {
    let msg = VecFeed::bar("ABC", 42.0, 9);
    assert_eq!(msg.event(), &Event::Bar(flat_bar("ABC", 42.0, 9)));
    assert_eq!(msg.ts_init(), UnixNanos::from_u64(9));
}

#[test]
fn trade_builder_derives_a_trade_id_from_the_timestamp() {
    let msg = VecFeed::trade("ABC", 100.0, 7.0, 11);
    let Event::Trade(t) = msg.event() else {
        panic!("expected a trade, got {:?}", msg.event());
    };
    assert_eq!(t.instrument_id(), &instrument("ABC"));
    assert_eq!((t.price(), t.size()), (100.0, 7.0));
    assert_eq!(t.aggressor_side(), AggressorSide::Buyer);
    assert_eq!(t.trade_id().as_str(), "T-11");
}
