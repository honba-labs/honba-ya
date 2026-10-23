//! Volatility and liquidity slippage models for backtest execution simulation.

use honba_core::{OrderSide, Price};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;

/// Models slippage based on order book depth, spread crossing, and volatility.
///
/// When a large order crosses the spread, the execution price deviates from
/// the mid-price. This model quantifies that deviation based on:
/// - Order size relative to book depth
/// - Spread crossing penalty (half-spread on aggressive market orders)
/// - Recent volatility (quantified by typical daily range)
/// - Order type (market orders cause more slippage than limit orders)
#[derive(Debug, Clone)]
pub struct SlippageModel {
    /// Typical daily range as percentage of price (e.g., 0.02 = 2%)
    daily_range_pct: Decimal,
    /// Recent volatility scaling factor (1.0 = normal, >1.0 = high vol)
    volatility_factor: Decimal,
    /// Recent average spread as percentage
    avg_spread_pct: Decimal,
}

impl Default for SlippageModel {
    fn default() -> Self {
        Self::new()
    }
}

impl SlippageModel {
    /// Create a new slippage model with default parameters.
    pub fn new() -> Self {
        Self {
            daily_range_pct: dec!(0.02),     // 2% typical daily range
            volatility_factor: Decimal::ONE, // 1.0 = normal
            avg_spread_pct: dec!(0.001),     // 0.1% average spread
        }
    }

    /// Set daily range percentage.
    pub fn set_daily_range_pct(&mut self, pct: Decimal) {
        self.daily_range_pct = pct;
    }

    /// Set volatility factor.
    pub fn set_volatility_factor(&mut self, factor: Decimal) {
        self.volatility_factor = factor;
    }

    /// Set average spread percentage.
    pub fn set_avg_spread_pct(&mut self, pct: Decimal) {
        self.avg_spread_pct = pct;
    }

    /// Typical daily range as a percentage of price.
    pub fn daily_range_pct(&self) -> Decimal {
        self.daily_range_pct
    }

    /// Average bid-ask spread as a percentage of price.
    pub fn avg_spread_pct(&self) -> Decimal {
        self.avg_spread_pct
    }

    /// Compute the spread-crossing penalty (in rupees per share) when an aggressive
    /// order crosses from mid-price to the contra touch.
    ///
    /// Uses the observed `(bid, ask)` spread when `ask > bid`, or falls back to
    /// `reference_price * avg_spread_pct / 2`.
    pub fn compute_spread_crossing_penalty(
        &self,
        bid: Option<Price>,
        ask: Option<Price>,
        reference_price: Price,
    ) -> Decimal {
        if let (Some(b), Some(a)) = (bid, ask)
            && a.0 > b.0
        {
            return ((a.0 - b.0) / dec!(2)).round_dp(4);
        }
        (reference_price.0 * self.avg_spread_pct / dec!(2)).round_dp(4)
    }

    /// Compute slippage for a market order quantity.
    ///
    /// Returns the price impact in decimal (e.g., 0.001 = 0.1% price degradation).
    pub fn compute_slippage(
        &self,
        side: OrderSide,
        order_quantity: i64,
        book_depth: i64, // total depth at best price
        _current_price: Decimal,
    ) -> Decimal {
        if order_quantity == 0 || book_depth == 0 {
            return Decimal::ZERO;
        }

        // Volume participation ratio
        let participation = Decimal::from(order_quantity.abs()) / Decimal::from(book_depth);

        // Base slippage from participation
        let base_slippage = self.avg_spread_pct * participation;

        // Volatility scaling
        let vol_adjusted = base_slippage * self.volatility_factor;

        // Side-dependent adjustment (sellers typically get slightly better,
        // buyers slightly worse due to spread dynamics)
        let side_factor = match side {
            OrderSide::Buy => Decimal::ONE + self.avg_spread_pct / Decimal::from(2),
            OrderSide::Sell => Decimal::ONE - self.avg_spread_pct / Decimal::from(2),
        };

        // Clamp to reasonable bounds [-5%, +5%]
        let clamped = vol_adjusted * side_factor;
        let max_pct = dec!(0.05); // 5%
        clamped.min(max_pct).max(-max_pct)
    }

    /// Compute slippage with order book levels (more accurate).
    pub fn compute_slippage_with_levels(
        &self,
        side: OrderSide,
        order_quantity: i64,
        levels: &BTreeMap<Price, i64>,
        _current_price: Decimal,
    ) -> Decimal {
        if order_quantity == 0 || levels.is_empty() {
            return Decimal::ZERO;
        }

        // Find how much of the order can be filled at each price level
        let mut remaining = order_quantity.abs();
        let mut total_price_impact: Decimal = Decimal::ZERO;

        for depth in levels.values() {
            if remaining <= 0 {
                break;
            }
            let depth_val = (*depth).max(1);
            let take = std::cmp::min(remaining, depth_val);
            remaining -= take;

            // Price impact proportional to quantity taken at this level
            let participation = Decimal::from(take) / Decimal::from(depth_val);
            let level_impact = self.avg_spread_pct * participation * self.volatility_factor;
            total_price_impact += level_impact;
        }

        // Apply side factor
        let side_factor = match side {
            OrderSide::Buy => Decimal::ONE + self.avg_spread_pct / Decimal::from(2),
            OrderSide::Sell => Decimal::ONE - self.avg_spread_pct / Decimal::from(2),
        };

        total_price_impact * side_factor
    }
}

/// Models adverse fill slippage when market moves against the position.
///
/// Simulates the effect of market orders being filled at progressively
/// worse prices as the order walks deeper into the order book.
#[derive(Debug, Clone)]
pub struct AdverseFillModel {
    /// How many price levels to slip per fill round
    levels_per_round: usize,
    /// Maximum slippage accumulation percentage before capping (e.g., 5.0 for 5%)
    max_slippage_pct: Decimal,
    /// Whether slippage compounds (gets worse with each level walked)
    compounding: bool,
}

impl AdverseFillModel {
    /// Create a new adverse fill model.
    pub fn new(levels_per_round: usize, max_slippage_pct: Decimal, compounding: bool) -> Self {
        Self {
            levels_per_round,
            max_slippage_pct,
            compounding,
        }
    }

    /// Compute slippage percentage and adverse fill price given current book state.
    ///
    /// Walks `book_levels` in execution priority order (ascending asks for `Buy`,
    /// descending bids for `Sell`) up to `levels_per_round` levels.
    pub fn compute_fill_slippage(
        &self,
        side: OrderSide,
        mut fill_quantity: i64,
        book_levels: &BTreeMap<Price, i64>,
        entry_price: Decimal,
    ) -> (Decimal, Option<Price>) {
        if fill_quantity <= 0 || book_levels.is_empty() || entry_price <= Decimal::ZERO {
            return (Decimal::ZERO, None);
        }

        let mut total_slippage = Decimal::ZERO;
        let mut worst_price: Option<Price> = None;
        let mut worst_price_val: Decimal = entry_price;

        let ordered_levels: Vec<(Price, i64)> = match side {
            OrderSide::Buy => book_levels.iter().map(|(p, d)| (*p, *d)).collect(),
            OrderSide::Sell => book_levels.iter().rev().map(|(p, d)| (*p, *d)).collect(),
        };

        for (round_idx, (level_price, best_depth)) in ordered_levels
            .into_iter()
            .take(self.levels_per_round)
            .enumerate()
        {
            if fill_quantity <= 0 || level_price.0 <= Decimal::ZERO {
                break;
            }
            let rounds = round_idx + 1;
            let depth = best_depth.max(1);
            let take = std::cmp::min(fill_quantity, depth);

            // Price impact at this level, in percentage points relative to entry_price
            let price_pct_change =
                (Decimal::from(take) * Decimal::from(100)) / (Decimal::from(depth) * entry_price);

            let round_slippage = if self.compounding {
                price_pct_change * Decimal::from(rounds as i64)
            } else {
                price_pct_change
            };
            total_slippage += round_slippage;

            match side {
                OrderSide::Buy => {
                    worst_price_val *= Decimal::ONE + (round_slippage / Decimal::from(100));
                    if worst_price.is_none_or(|p| worst_price_val > p.0) {
                        worst_price = Some(Price(worst_price_val.round_dp(4)));
                    }
                }
                OrderSide::Sell => {
                    let factor =
                        (Decimal::ONE - (round_slippage / Decimal::from(100))).max(dec!(0.01));
                    worst_price_val *= factor;
                    if worst_price.is_none_or(|p| worst_price_val < p.0) {
                        worst_price = Some(Price(worst_price_val.round_dp(4)));
                    }
                }
            }

            fill_quantity -= take;
        }

        let capped = total_slippage.min(self.max_slippage_pct);
        (capped, worst_price)
    }
}
