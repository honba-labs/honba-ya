//! The top-of-book state of one instrument.

use honba_messages::{OrderSide, QuoteTick};

/// Best bid and ask with their sizes, as last set by a `QuoteTick` and drawn down by fills.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct L2Book {
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
}

impl L2Book {
    /// The book a quote describes.
    pub fn from_quote(q: &QuoteTick) -> Self {
        Self {
            bid: q.bid_price(),
            ask: q.ask_price(),
            bid_size: q.bid_size(),
            ask_size: q.ask_size(),
        }
    }

    /// Best bid price.
    pub fn bid(&self) -> f64 {
        self.bid
    }

    /// Best ask price.
    pub fn ask(&self) -> f64 {
        self.ask
    }

    /// Size left at the best bid.
    pub fn bid_size(&self) -> f64 {
        self.bid_size
    }

    /// Size left at the best ask.
    pub fn ask_size(&self) -> f64 {
        self.ask_size
    }

    /// The `(price, size)` an order of `side` trades against: a buy lifts the ask, a sell hits
    /// the bid. `None` when the price is unusable or no size is left.
    pub fn touch(&self, side: OrderSide) -> Option<(f64, f64)> {
        let (px, sz) = match side {
            OrderSide::Buy => (self.ask, self.ask_size),
            OrderSide::Sell => (self.bid, self.bid_size),
            _ => return None,
        };
        (px.is_finite() && px > 0.0 && sz > 0.0).then_some((px, sz))
    }

    /// Removes `qty` from the touch an order of `side` traded against.
    pub(crate) fn consume(&mut self, side: OrderSide, qty: f64) {
        match side {
            OrderSide::Buy => self.ask_size = (self.ask_size - qty).max(0.0),
            OrderSide::Sell => self.bid_size = (self.bid_size - qty).max(0.0),
            _ => {}
        }
    }
}
