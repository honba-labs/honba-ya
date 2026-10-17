//! Product-aware margin queries and MIS (intraday) square-off.
//!
//! An intraday (MIS) position may not be carried overnight: a broker squares it off before the
//! session closes (15:15 IST on NSE, for example). A delivery (CNC) position is held instead.
//! This module holds the two pieces shared by the simulators:
//!
//! - [`MarginQuery`] reads `initial_margin` / `maintenance_margin` for a position from the
//!   active [`MarketProfile`]'s [`MarginModel`] (ADR 0018 keeps the cash/margin pre-check in the
//!   risk stage).
//! - [`MisPositions`] remembers which instruments a run holds under an intraday product, so a
//!   session close can flatten them. [`square_off_order`] builds each closing market order.
//!
//! When no market calendar is wired the session close defaults to 23:59:00 UTC of the
//! reference day ([`default_session_close_ts`]); the caller drives the close, exactly as it
//! drives the session open.

use honba_entities::{InstrumentKind, PositionSide};
use honba_market::settlement::{MarginModel, MarginRequirement};
use honba_market::MarketProfile;
use honba_messages::{
    InstrumentId, Order, OrderId, OrderSide, OrderType, Product, TimeInForce, UnixNanos,
};

/// Quantities closer than this are equal (float residue).
pub(crate) const QTY_EPS: f64 = 1e-9;

/// Nanoseconds in a UTC day.
const NS_PER_DAY: u64 = 86_400_000_000_000;
/// 23:59:00 UTC in nanoseconds since the epoch.
const CLOSE_OF_DAY_NS: u64 = 86_340_000_000_000;

/// Whether `product` is squared off before the session close (MIS / intraday).
pub fn is_intraday(product: Option<Product>) -> bool {
    matches!(product, Some(Product::Intraday))
}

/// The fallback session close when no market calendar is wired: 23:59:00 UTC on `reference`'s
/// UTC day, or the next day when `reference` is already past it. Returned as a `UnixNanos` on
/// the same, wall-clock-free epoch as the driving events.
pub fn default_session_close_ts(reference: UnixNanos) -> UnixNanos {
    let ns = reference.as_u64();
    let day = ns / NS_PER_DAY * NS_PER_DAY;
    let candidate = day + CLOSE_OF_DAY_NS;
    UnixNanos::from_u64(if candidate < ns {
        candidate + NS_PER_DAY
    } else {
        candidate
    })
}

/// A closing market order that flattens `position` in `instrument` at the session close.
///
/// The order takes the opposite side of the position and `abs(position)` quantity; `ts` stamps
/// both its event and init time. `position` must be non-flat; a zero position is the caller's
/// mistake, not an order.
pub fn square_off_order(
    id: &str,
    instrument: &InstrumentId,
    position: f64,
    ts: UnixNanos,
) -> Order {
    let (side, quantity) = if position > 0.0 {
        (OrderSide::Sell, position)
    } else {
        (OrderSide::Buy, -position)
    };
    Order::new(
        OrderId::new(id),
        instrument.clone(),
        side,
        OrderType::Market,
        quantity,
        None,
        TimeInForce::Day,
        ts,
        ts,
    )
}

/// Read-only margin queries against a [`MarginModel`] (the active market profile's).
///
/// The model is borrowed, so the query is cheap and never clones a market pack. Build it from a
/// profile with [`Self::from_profile`] or from a model directly with [`Self::new`].
#[derive(Clone, Copy)]
pub struct MarginQuery<'a> {
    model: &'a dyn MarginModel,
}

impl<'a> MarginQuery<'a> {
    /// Wraps `model` directly.
    pub fn new(model: &'a dyn MarginModel) -> Self {
        Self { model }
    }

    /// Reads the model of the active [`MarketProfile`].
    pub fn from_profile(profile: &'a dyn MarketProfile) -> Self {
        Self {
            model: profile.margin_model(),
        }
    }

    /// The full initial and maintenance requirement for a position of `notional`.
    pub fn requirement(
        &self,
        kind: InstrumentKind,
        side: PositionSide,
        notional: f64,
    ) -> MarginRequirement {
        self.model.calculate_margin(kind, side, notional)
    }

    /// The initial margin required to place or hold a position of `notional`.
    pub fn initial_margin(&self, kind: InstrumentKind, side: PositionSide, notional: f64) -> f64 {
        self.requirement(kind, side, notional).initial
    }

    /// The maintenance margin below which a margin call or liquidation triggers.
    pub fn maintenance_margin(
        &self,
        kind: InstrumentKind,
        side: PositionSide,
        notional: f64,
    ) -> f64 {
        self.requirement(kind, side, notional).maintenance
    }
}

/// The set of instruments a run holds under an intraday (MIS) product.
///
/// A fill whose order [`Product`] is [`Product::Intraday`] marks the instrument; the mark clears
/// once the instrument's net position is flat. Positions are net-per-instrument here, so an
/// instrument held under more than one product is treated as intraday (conservative: it is
/// squared off rather than carried).
#[derive(Debug, Default, Clone)]
pub struct MisPositions {
    intraday: Vec<InstrumentId>,
}

impl MisPositions {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a fill: `product` is the order's product and `position_after` the instrument's
    /// net position once the fill has booked.
    pub fn apply(
        &mut self,
        instrument: &InstrumentId,
        product: Option<Product>,
        position_after: f64,
    ) {
        if position_after.abs() <= QTY_EPS {
            self.clear(instrument);
        } else if is_intraday(product) && !self.is_intraday(instrument) {
            self.intraday.push(instrument.clone());
        }
    }

    /// Whether `instrument` is held under an intraday product.
    pub fn is_intraday(&self, instrument: &InstrumentId) -> bool {
        self.intraday.iter().any(|i| i == instrument)
    }

    /// Every instrument marked intraday, in the order first marked.
    pub fn intraday(&self) -> &[InstrumentId] {
        &self.intraday
    }

    /// Removes the intraday mark from `instrument` (a no-op when absent).
    pub fn clear(&mut self, instrument: &InstrumentId) {
        self.intraday.retain(|i| i != instrument);
    }

    /// Whether no instrument is marked intraday.
    pub fn is_empty(&self) -> bool {
        self.intraday.is_empty()
    }
}
