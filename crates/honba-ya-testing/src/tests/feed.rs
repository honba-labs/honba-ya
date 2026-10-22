//! Unit tests for `crate::feed`.

use honba_core::rust_decimal::Decimal;
use honba_core::{Quantity, Tick};

use crate::VecFeed;
use crate::fixtures::{flat_bar, instrument, utc_nanos};
use crate::ports::{FeedItem, MarketDataFeed};

#[tokio::test]
async fn empty_feed_yields_nothing() {
    assert!(VecFeed::empty().next().await.unwrap().is_none());
}

#[tokio::test]
async fn feed_yields_items_in_insertion_order_not_time_order() {
    let mut feed = VecFeed::new(vec![VecFeed::bar("X", 1.0, 5), VecFeed::bar("X", 2.0, 1)]);
    let mut ts = Vec::new();
    while let Some(item) = feed.next().await.unwrap() {
        ts.push(item.timestamp().timestamp_nanos());
    }
    assert_eq!(ts, [5, 1]);
}

#[tokio::test]
async fn quote_builder_sets_prices_unit_sizes_and_timestamps() {
    let item = VecFeed::quote("ABC", 9.5, 10.5, 3);
    assert_eq!(item.timestamp(), utc_nanos(3));
    let FeedItem::Tick(Tick::Quote {
        instrument: id,
        bid,
        ask,
        bid_qty,
        ask_qty,
        last_price,
        exchange_ts,
        receive_ts,
        band,
    }) = &item
    else {
        panic!("expected a quote, got {item:?}");
    };
    assert_eq!(id, &instrument("ABC"));
    assert_eq!(bid.to_string(), "9.5");
    assert_eq!(ask.to_string(), "10.5");
    assert_eq!(
        (*bid_qty, *ask_qty),
        (Quantity::from_u64(1), Quantity::from_u64(1))
    );
    assert_eq!(last_price, &None);
    assert_eq!((*exchange_ts, *receive_ts), (utc_nanos(3), utc_nanos(3)));
    assert_eq!(band, &None);
}

#[tokio::test]
async fn bar_builder_wraps_a_flat_bar() {
    let item = VecFeed::bar("ABC", 42.0, 9);
    assert_eq!(item, FeedItem::Bar(flat_bar("ABC", 42.0, 9)));
    assert_eq!(item.timestamp(), utc_nanos(9));
}

#[tokio::test]
async fn trade_builder_derives_an_ltp_tick() {
    let item = VecFeed::trade("ABC", 100.0, 7.0, 11);
    let FeedItem::Tick(Tick::Ltp {
        instrument: id,
        price,
        volume,
        exchange_ts,
        receive_ts,
    }) = &item
    else {
        panic!("expected an LTP tick, got {item:?}");
    };
    assert_eq!(id, &instrument("ABC"));
    assert_eq!(price.0, Decimal::from(100));
    assert_eq!(*volume, Quantity::from_u64(7));
    assert_eq!((*exchange_ts, *receive_ts), (utc_nanos(11), utc_nanos(11)));
}
