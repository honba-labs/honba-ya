//! Volatility and liquidity slippage models for backtest execution simulation.

use rust_decimal::Decimal;
use std::collections::BTreeMap;
use honba_core::{OrderSide, OrderType, Price, Tick, InstrumentId};

/// Models slippage based on order book depth and volatility.
///
/// When a large order crosses the spread, the execution price deviates from
/// the mid-price. This model quantifies that deviation based on:
/// - Order size relative to book depth
/// - Recent volatility (quantified by typical daily range)
/// - Order type (market orders cause more slippage than limit orders)
pub struct SlippageModel {
    /// Typical daily range as percentage of price (e.g., 0.02 = 2%)
    daily_range_pct: Decimal,
    /// Recent volatility scaling factor (1.0 = normal, >1.0 = high vol)
    volatility_factor: Decimal,
    /// Recent average spread as percentage
    avg_spread_pct: Decimal,
}

impl SlippageModel {
    /// Create a new slippage model with default parameters.
    pub fn new() -> Self {
        Self {
            daily_range_pct: Decimal::from(0.02),   // 2% typical daily range
            volatility_factor: Decimal::one(),        // 1.0 = normal
            avg_spread_pct: Decimal::from(0.001),     // 0.1% average spread
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

    /// Compute slippage for a market order quantity.
    ///
    /// Returns the price impact in decimal (e.g., 0.001 = 0.1% price degradation).
    pub fn compute_slippage(
        &self,
        side: OrderSide,
        order_quantity: i64,
        book_depth: i64, // total depth at best price
        current_price: Decimal,
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
            OrderSide::Buy => Decimal::one() + self.avg_spread_pct / Decimal::from(2),
            OrderSide::Sell => Decimal::one() - self.avg_spread_pct / Decimal::from(2),
        };

        // Clamp to reasonable bounds [-5%, +5%]
        let clamped = vol_adjusted * side_factor;
        let max_pct = Decimal::from(500); // 5% in basis points
        clamped.min(max_pct).max(-max_pct)
    }

    /// Compute slippage with order book levels (more accurate).
    pub fn compute_slippage_with_levels(
        &self,
        side: OrderSide,
        order_quantity: i64,
        levels: &BTreeMap<Price, i64>,
        current_price: Decimal,
    ) -> Decimal {
        if order_quantity == 0 || levels.is_empty() {
            return Decimal::ZERO;
        }

        // Find how much of the order can be filled at each price level
        let mut remaining = order_quantity.abs();
        let mut total_price_impact: Decimal = Decimal::ZERO;
        let mut cumulative_qty: i64 = 0;

        for (price, depth) in levels {
            if remaining <= 0 {
                break;
            }
            let take = std::cmp::min(remaining, *depth as i64);
            cumulative_qty += take;
            remaining -= take;

            // Price impact proportional to quantity taken at this level
            let participation = Decimal::from(take) / Decimal::from(*depth);
            let level_impact = self.avg_spread_pct * participation;
            total_price_impact += level_impact;
        }

        // Apply side factor
        let side_factor = match side {
            OrderSide::Buy => Decimal::one() + self.avg_spread_pct / Decimal::from(2),
            OrderSide::Sell => Decimal::one() - self.avg_spread_pct / Decimal::from(2),
        };

        total_price_impact * side_factor
    }
}

/// Models adverse fill slippage when market moves against the position.
///
/// Simulates the effect of market orders being filled at progressively
/// worse prices as the order interacts with the book.
#[derive(Debug, Clone)]
pub struct AdverseFillModel {
    /// How many price levels to slip per fill round
    levels_per_round: usize,
    /// Maximum slippage accumulation before order cancellation
    max_slippage_pct: Decimal,
    /// Whether slippage compounds (gets worse with each fill)
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

    /// Compute slippage for a fill given current book state.
    pub fn compute_fill_slippage(
        &self,
        side: OrderSide,
        fill_quantity: i64,
        book_levels: &BTreeMap<Price, i64>,
        entry_price: Decimal,
    ) -> (Decimal, Option<Price>) {
        if fill_quantity == 0 || book_levels.is_empty() {
            return (Decimal::ZERO, None);
        }

        let mut total_slippage = Decimal::ZERO;
        let mut worst_price: Option<Price> = None;
        let mut worst_price_val: Decimal = entry_price;

        let mut levels = book_levels.clone();
        let mut rounds = 0;

        while fill_quantity != 0 && rounds < self.levels_per_round {
            rounds += 1;

            // Get best price level for this side
            let (best_price, best_depth) = match side {
                OrderSide::Buy => {
                    let price = book_levels.keys().next().copied().unwrap_or(Price(Decimal::ZERO));
                    let depth = book_levels.values().next().copied().unwrap_or(0);
                    (price, depth)
                }
                OrderSide::Sell => {
                    let price = book_levels.keys().last().copied().unwrap_or(Price(Decimal::ZERO));
                    let depth = book_levels.values().last().copied().unwrap_or(0);
                    (price, depth)
                }
            };

            if best_price.0 == Decimal::ZERO {
                break;
            }

            let depth = best_depth.max(1);
            let take = std::cmp::min(fill_quantity, depth);

            // Price at this level determines slippage
            let price_at_level = best_price.0;
            let price_pct_change = (take as Decimal * Decimal::from(100))
                / (Decimal::from(best_depth) * current_price);

            total_slippage += pct_change * Decimal::from(10).powi(rounds as i32);
            worst_price_val = worst_price_val * (Decimal::one() + pct_change);

            // Update worst price tracking
            if worst_price.is_none() || worst_price_val > worst_price.unwrap().0 {
                worst_price = Some(Price(worst_price_val));
            }

            // Remove filled level
            if let Some(first_key) = book_levels.keys().next().cloned() {
                // Simplified: just reduce depth or remove level
                // In full impl, would update the book state
            }

            fill_quantity -= take as i64;
        }

        // Cap at max slippage
        let capped = total_slippage.min(self.max_slippage_pct);

        (capped, worst_price)
    }
}