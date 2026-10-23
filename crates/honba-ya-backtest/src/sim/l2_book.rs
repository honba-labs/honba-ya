//! L2 Order book depth simulation and matching engine.

use std::collections::BTreeMap;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use honba_core::{OrderSide, Price};

/// Order book level with price and accumulated quantity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookLevel {
    pub price: Price,
    /// Accumulated quantity at this price level (positive for bids, negative for asks)
    pub qty: i64,
}

/// Order book side (bids/asks).
#[derive(Debug, Clone)]
pub struct OrderBookSide {
    /// Price levels sorted by price priority
    levels: BTreeMap<Price, i64>, // price -> accumulated qty
    /// Best (top) price level
    best_price: Option<Price>,
    /// Whether this is bids (true) or asks (false)
    is_bids: bool,
}

impl OrderBookSide {
    /// Create a new order book side.
    pub fn new(is_bids: bool) -> Self {
        Self {
            levels: BTreeMap::new(),
            best_price: None,
            is_bids,
        }
    }

    /// Add an order to the book.
    pub fn add_order(&mut self, price: Price, quantity: i64) {
        let qty = self.levels.entry(price).or_insert(0);
        *qty += quantity;
        // Remove level if quantity becomes zero
        if *qty == 0 {
            self.levels.remove(&price);
        }
        // Update best price
        if self.levels.is_empty() {
            self.best_price = None;
        } else if self.is_bids {
            // For bids, best is highest price
            self.best_price = self.levels.keys().last().cloned();
        } else {
            // For asks, best is lowest price
            self.best_price = self.levels.keys().next().cloned();
        }
    }

    /// Remove an order from the book.
    pub fn remove_order(&mut self, price: Price, quantity: i64) {
        if let Some(qty) = self.levels.get_mut(&price) {
            *qty -= quantity;
            if *qty == 0 {
                self.levels.remove(&price);
            }
        }
        if self.levels.is_empty() {
            self.best_price = None;
        } else if self.is_bids {
            self.best_price = self.levels.keys().last().cloned();
        } else {
            self.best_price = self.levels.keys().next().cloned();
        }
    }

    /// Get best price (highest for bids, lowest for asks).
    pub fn best_price(&self) -> Option<Price> {
        self.best_price
    }

    /// Get total quantity at best price.
    pub fn best_quantity(&self) -> i64 {
        self.levels
            .get(&self.best_price.unwrap_or(Price(Decimal::ZERO)))
            .copied()
            .unwrap_or(0)
    }

    /// Get total depth at price level.
    pub fn depth_at(&self, price: Price) -> i64 {
        self.levels.get(&price).copied().unwrap_or(0)
    }

    /// Check if book is empty.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Get all price levels.
    pub fn levels(&self) -> &BTreeMap<Price, i64> {
        &self.levels
    }
}

/// L2 Order book matching engine for market simulation.
///
/// Matches incoming orders against resting depth using price-time priority.
/// Supports market orders, limit orders with price improvement, and adverse
/// slippage modeling for realistic execution simulation.
pub struct OrderBook {
    /// Bid side (buy orders)
    bids: OrderBookSide,
    /// Ask side (sell orders)
    asks: OrderBookSide,
    /// Latency model: microseconds to simulate order propagation delay
    latency_us: u64,
    /// Volume participation cap: max percentage of average daily volume
    participation_cap: Decimal,
    /// Average daily volume for the instrument this book represents, used
    /// together with `participation_cap` to clamp a single order's size.
    /// Zero means uncapped (no average daily volume known yet).
    avg_daily_volume: i64,
}

impl OrderBook {
    /// Create a new L2 order book.
    pub fn new(latency_us: u64, participation_cap: Decimal) -> Self {
        Self {
            bids: OrderBookSide::new(true),
            asks: OrderBookSide::new(false),
            latency_us,
            participation_cap,
            avg_daily_volume: 0,
        }
    }

    /// Sets the average daily volume used by the participation cap. The
    /// backtest engine calls this as it processes bars, since a bar's
    /// traded volume is the simulation's only observable proxy for ADV.
    pub(crate) fn set_avg_daily_volume(&mut self, avg_daily_volume: i64) {
        self.avg_daily_volume = avg_daily_volume;
    }

    /// Clamps `order_qty` to the volume participation cap (half of
    /// `avg_daily_volume * participation_cap`, per `check_participation_cap`),
    /// given this book's current `avg_daily_volume`. Returns `order_qty`
    /// unchanged when it already respects the cap or no ADV is known yet.
    fn clamp_to_participation_cap(&self, order_qty: i64) -> i64 {
        if self.check_participation_cap(order_qty, self.avg_daily_volume) {
            return order_qty;
        }
        let cap = (Decimal::from(self.avg_daily_volume) * self.participation_cap) / Decimal::from(2);
        cap.max(Decimal::ZERO).to_i64().unwrap_or(0).min(order_qty.abs())
    }

    /// Execute a market order against the order book.
    ///
    /// The requested `quantity` is first clamped to the volume participation
    /// cap: any amount above the cap is never matched against the book and
    /// is reported back as part of `remaining_quantity` (a market order is
    /// partially filled up to the cap rather than rejected outright).
    ///
    /// Returns: (filled_quantity, fill_price, remaining_quantity, worst_price_encountered)
    pub fn execute_market_order(
        &mut self,
        side: OrderSide,
        quantity: i64,
    ) -> (i64, Price, i64, Option<Price>) {
        let allowed = self.clamp_to_participation_cap(quantity);
        let capped_excess = quantity - allowed;
        let (filled_qty, filled_price, remaining, worst_price) = match side {
            OrderSide::Buy => self._buy_market(allowed),
            OrderSide::Sell => self._sell_market(allowed),
        };
        (filled_qty, filled_price, remaining + capped_excess, worst_price)
    }

    /// Execute a limit order against the order book.
    ///
    /// Same participation-cap clamping as [`Self::execute_market_order`].
    ///
    /// Returns: (filled_quantity, fill_price, remaining_quantity)
    pub fn execute_limit_order(
        &mut self,
        side: OrderSide,
        quantity: i64,
        limit_price: Price,
    ) -> (i64, Price, i64) {
        let allowed = self.clamp_to_participation_cap(quantity);
        let capped_excess = quantity - allowed;
        let (filled, price, remaining) = match side {
            OrderSide::Buy => self._buy_limit(allowed, limit_price),
            OrderSide::Sell => self._sell_limit(allowed, limit_price),
        };
        (filled, price, remaining + capped_excess)
    }

    /// Get current best bid and ask prices.
    pub fn get_spread(&self) -> (Option<Price>, Option<Price>) {
        (self.bids.best_price(), self.asks.best_price())
    }

    /// Get current mid-price (average of best bid and ask).
    pub fn get_mid_price(&self) -> Option<Price> {
        let (bid, ask) = self.get_spread();
        match (bid, ask) {
            (Some(b), Some(a)) => {
                let mid = (b.0 + a.0) / Decimal::from(2);
                Some(Price(mid))
            }
            _ => None,
        }
    }

    /// Add synthetic depth at a price level.
    ///
    /// A tick/bar feed carries no bid/ask side information for the volume
    /// it reports, so this adds the same depth to both sides — a simple
    /// approximation of resting liquidity derived from traded volume.
    pub fn add_order(&mut self, price: Price, quantity: i64) {
        self.bids.add_order(price, quantity);
        self.asks.add_order(price, quantity);
    }

    /// Total depth (bid + ask) resting at a price level.
    pub fn depth_at(&self, price: Price) -> i64 {
        self.bids.depth_at(price) + self.asks.depth_at(price)
    }

    /// The timestamp at which an order submitted at `submit_ts` becomes
    /// ready to execute, given this book's configured latency. Pure
    /// arithmetic on the caller-supplied timestamp — no clock is read here,
    /// so replaying the same events always yields the same `ready_ts`
    /// (`AGENTS.md` §2.5).
    pub(crate) fn ready_ts(&self, submit_ts: DateTime<Utc>) -> DateTime<Utc> {
        submit_ts + Duration::microseconds(self.latency_us as i64)
    }

    /// Check if order respects volume participation cap.
    fn check_participation_cap(&self, order_qty: i64, avg_daily_volume: i64) -> bool {
        if avg_daily_volume == 0 {
            return true;
        }
        let cap = (Decimal::from(avg_daily_volume) * self.participation_cap) / Decimal::from(2); // half on each side
        let abs_qty = Decimal::from(order_qty.abs());
        abs_qty <= cap
    }

    // ---------- Internal match methods ----------

    fn _buy_market(&mut self, quantity: i64) -> (i64, Price, i64, Option<Price>) {
        let mut filled = 0i64;
        let mut remaining = quantity;
        let mut worst_price: Option<Price> = None;
        let mut worst_price_val: Decimal = Decimal::ZERO;

        while remaining > 0 && !self.asks.is_empty() {
            let ask_price = self.asks.best_price().unwrap();
            let available = self.asks.best_quantity();

            let take = std::cmp::min(remaining, available);
            filled += take;
            remaining -= take;

            // Track worst (highest) price paid
            if worst_price.is_none() || ask_price.0 > worst_price_val {
                worst_price = Some(ask_price);
                worst_price_val = ask_price.0;
            }

            // Remove or reduce level
            self.asks.remove_order(ask_price, take as i64);
        }

        let fill_price = worst_price.unwrap_or(Price(Decimal::ZERO));
        (filled, fill_price, remaining, worst_price)
    }

    fn _sell_market(&mut self, quantity: i64) -> (i64, Price, i64, Option<Price>) {
        let mut filled = 0i64;
        let mut remaining = quantity;
        let mut worst_price: Option<Price> = None;
        let mut worst_price_val: Decimal = Decimal::MAX;

        while remaining > 0 && !self.bids.is_empty() {
            let bid_price = self.bids.best_price().unwrap();
            let available = self.bids.best_quantity();

            let take = std::cmp::min(remaining, available);
            filled += take;
            remaining -= take;

            // Track worst (lowest) price received
            if worst_price.is_none() || bid_price.0 < worst_price_val {
                worst_price = Some(bid_price);
                worst_price_val = bid_price.0;
            }

            self.bids.remove_order(bid_price, take as i64);
        }

        let fill_price = worst_price.unwrap_or(Price(Decimal::ZERO));
        (filled, fill_price, remaining, worst_price)
    }

    fn _buy_limit(&mut self, quantity: i64, limit_price: Price) -> (i64, Price, i64) {
        let mut filled = 0i64;
        let mut remaining = quantity;

        // Market buy can only fill up to limit price
        while remaining > 0 && !self.asks.is_empty() {
            let ask_price = self.asks.best_price().unwrap();

            // Limit check: only buy if ask <= limit
            if ask_price.0 > limit_price.0 {
                break; // Above limit, cannot fill
            }

            let take = std::cmp::min(remaining, self.asks.best_quantity());
            filled += take;
            remaining -= take;
            self.asks.remove_order(ask_price, take as i64);
        }

        let fill_price = if filled > 0 {
            // Use volume-weighted average price
            // Simplified: use the best ask price that was available
            let (_, vwap_price, _, _) = self._buy_market(filled);
            vwap_price
        } else {
            Price(Decimal::ZERO)
        };

        (filled, fill_price, remaining)
    }

    fn _sell_limit(&mut self, quantity: i64, limit_price: Price) -> (i64, Price, i64) {
        let mut filled = 0i64;
        let mut remaining = quantity;

        // Market sell can only fill down to limit price
        while remaining > 0 && !self.bids.is_empty() {
            let bid_price = self.bids.best_price().unwrap();

            // Limit check: only sell if bid >= limit
            if bid_price.0 < limit_price.0 {
                break; // Below limit, cannot fill
            }

            let take = std::cmp::min(remaining, self.bids.best_quantity());
            filled += take;
            remaining -= take;
            self.bids.remove_order(bid_price, take as i64);
        }

        let fill_price = if filled > 0 {
            let (_, vwap_price, _, _) = self._sell_market(filled);
            vwap_price
        } else {
            Price(Decimal::ZERO)
        };

        (filled, fill_price, remaining)
    }
}