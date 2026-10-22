//! In-memory data feeds.

use std::collections::VecDeque;

use async_trait::async_trait;
use honba_core::{InstrumentId, Price, Quantity, Tick};

use crate::fixtures;
use crate::ports::{FeedItem, MarketDataFeed, PortResult};

/// A feed backed by an in-memory vector of market items.
///
/// ```
/// use honba_testing::{MarketDataFeed, VecFeed};
///
/// # tokio::runtime::Builder::new_current_thread()
/// #     .build()
/// #     .unwrap()
/// #     .block_on(async {
/// let mut feed = VecFeed::new(vec![
///     VecFeed::quote("X", 1.0, 2.0, 1),
///     VecFeed::quote("X", 1.5, 2.5, 2),
/// ]);
///
/// assert!(feed.next().await.unwrap().is_some());
/// assert!(feed.next().await.unwrap().is_some());
/// assert!(feed.next().await.unwrap().is_none());
/// # });
/// ```
pub struct VecFeed {
    items: VecDeque<FeedItem>,
}

impl VecFeed {
    /// Creates a feed from a vector of market items.
    pub fn new(items: Vec<FeedItem>) -> Self {
        Self {
            items: items.into(),
        }
    }

    /// Creates an empty feed.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Builds a top-of-book quote tick for `symbol` at `ts` nanoseconds since the Unix epoch.
    pub fn quote(symbol: &str, bid: f64, ask: f64, ts: u64) -> FeedItem {
        let instrument = fixtures::instrument(symbol);
        let t = fixtures::utc_nanos(ts);
        FeedItem::Tick(Tick::Quote {
            instrument,
            bid: Price::new(fixtures::exact_decimal(bid)),
            ask: Price::new(fixtures::exact_decimal(ask)),
            bid_qty: Quantity::from_u64(1),
            ask_qty: Quantity::from_u64(1),
            last_price: None,
            exchange_ts: t,
            receive_ts: t,
            band: None,
        })
    }

    /// Builds a flat one-minute bar item (see [`fixtures::flat_bar`]).
    pub fn bar(symbol: &str, close: f64, ts: u64) -> FeedItem {
        FeedItem::Bar(fixtures::flat_bar(symbol, close, ts))
    }

    /// Builds a last-traded-price tick for `symbol` at `ts` nanoseconds since the Unix epoch.
    pub fn trade(symbol: &str, price: f64, qty: f64, ts: u64) -> FeedItem {
        let instrument = fixtures::instrument(symbol);
        let t = fixtures::utc_nanos(ts);
        FeedItem::Tick(Tick::Ltp {
            instrument,
            price: Price::new(fixtures::exact_decimal(price)),
            volume: Quantity::new(fixtures::exact_decimal(qty)),
            exchange_ts: t,
            receive_ts: t,
        })
    }
}

#[async_trait]
impl MarketDataFeed for VecFeed {
    async fn subscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        // A replay feed has a fixed item list; subscriptions are accepted and ignored.
        Ok(())
    }

    async fn unsubscribe(&mut self, _instruments: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<FeedItem>> {
        Ok(self.items.pop_front())
    }
}
