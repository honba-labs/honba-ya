//! Adapters that turn a generic market cost schedule into a simulator fill cost.
//!
//! [`ScheduleFillCost`] bridges `honba-market`'s itemized [`CostSchedule`] into the
//! simulator's per-fill [`FillCostFn`](crate::next_open::FillCostFn). It rounds each named
//! [`Charge`] to minor units exactly once, then sums the legs (ADR 0011), so the total a fill
//! books is one a broker's contract note can reproduce.

use std::sync::Arc;

use honba_engine::AlgoError;
use honba_entities::{Currency, Money};
use honba_market::costs::{CostSchedule, FeeBreakdown, MarketSegment};
use honba_messages::OrderSide;

use crate::next_open::FillCostFn;

/// A market [`CostSchedule`] bound to one [`MarketSegment`] and settlement [`Currency`],
/// evaluated per fill.
///
/// Cheap to clone: the schedule is shared behind an [`Arc`].
#[derive(Clone)]
pub struct ScheduleFillCost {
    schedule: Arc<dyn CostSchedule>,
    segment: MarketSegment,
    currency: Currency,
}

impl ScheduleFillCost {
    /// Binds `schedule` to `segment`, pricing fills in `currency`.
    pub fn new(
        schedule: Arc<dyn CostSchedule>,
        segment: MarketSegment,
        currency: Currency,
    ) -> Self {
        Self {
            schedule,
            segment,
            currency,
        }
    }

    /// The itemized fee breakdown of one fill. The notional is `abs(quantity * price)`.
    pub fn breakdown(&self, side: OrderSide, quantity: f64, price: f64) -> FeeBreakdown {
        self.schedule
            .compute_costs(&self.segment, side, (quantity * price).abs())
    }

    /// The total cost of one fill in [`Self`]'s currency: every named charge is rounded to minor
    /// units once (half away from zero) and the legs are summed.
    ///
    /// A leg that cannot be represented, or a sum that overflows `i64`, is an error, never a
    /// silent zero cost.
    pub fn total(&self, side: OrderSide, quantity: f64, price: f64) -> Result<Money, AlgoError> {
        let fees = self.breakdown(side, quantity, price);
        let mut total = 0_i64;
        for charge in &fees.charges {
            let leg = Money::from_major_f64(charge.amount, self.currency)
                .map_err(|e| AlgoError::Component(format!("cost leg {}: {e}", charge.name)))?;
            total = total
                .checked_add(leg.minor())
                .ok_or_else(|| AlgoError::Component("fill cost overflow".into()))?;
        }
        Ok(Money::new(total, self.currency))
    }

    /// Consumes the adapter into the simulator's per-fill cost function.
    pub fn into_fn(self) -> FillCostFn {
        Box::new(move |side, quantity, price| self.total(side, quantity, price))
    }

    /// The segment the schedule is bound to.
    pub fn segment(&self) -> &MarketSegment {
        &self.segment
    }

    /// The settlement currency.
    pub fn currency(&self) -> Currency {
        self.currency
    }
}
