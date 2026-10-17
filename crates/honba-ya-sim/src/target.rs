//! Target-position ("smart order") execution with per-instrument serialisation.
//!
//! A strategy states the position it *wants* rather than the order it wants: a target of 100
//! from flat buys 100, a later target of 0 sells it back. [`TargetPosition`] turns a desired
//! position into the signed delta order for the current position, honouring the instrument's lot
//! size (the delta floors to whole lots, never overshooting the target) and capping at the freeze
//! (maximum single-order) quantity when the active market exposes one.
//!
//! [`SmartOrderController`] serialises the two per instrument: at most one target order is
//! outstanding for an instrument at a time ([`PerInstrumentLock`]), so a target submitted while an
//! order is working is deferred rather than doubled. The caller releases the instrument once its
//! order has filled, been rejected or cancelled.
//!
//! [`TargetPosition::delta`] stays the single (first) lot-aligned clip; [`TargetPosition::delta_clips`]
//! and [`TargetPosition::orders`] are the lot/freeze splitter (E3-S6), slicing a large target into
//! several lot-aligned, freeze-capped clips whose fills aggregate to the lot-aligned delta.

use std::collections::HashMap;

use honba_engine::{AlgoError, Result};
use honba_entities::{Instrument, Position};
use honba_market::{InstrumentRules, MarketProfile};
use honba_messages::{InstrumentId, Order, OrderId, OrderSide, OrderType, TimeInForce, UnixNanos};

use crate::lot::split_order;

/// Quantities closer than this are equal (float residue).
const QTY_EPS: f64 = 1e-9;

/// Ticks of float noise tolerated when snapping a delta to a whole number of lots.
const LOT_TOLERANCE: f64 = 1e-6;

/// The net signed quantity of `position`: `+` long, `-` short, `0` flat.
fn net_position(position: &Position) -> f64 {
    position.side().sign() * position.quantity()
}

/// An in-memory per-instrument serialisation lock.
///
/// Keyed by [`InstrumentId`], it tracks which instruments already have an outstanding target
/// order: [`acquire`](Self::acquire) succeeds only when the instrument is free, and
/// [`release`](Self::release) frees it again. There is no blocking and no wall clock; a caller
/// that finds an instrument busy defers its target and retries once the order is done.
#[derive(Debug, Default, Clone)]
pub struct PerInstrumentLock {
    active: HashMap<InstrumentId, bool>,
}

impl PerInstrumentLock {
    /// An empty lock holding no instrument.
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquires `instrument`, returning `false` (and changing nothing) when it is already held.
    pub fn acquire(&mut self, instrument: &InstrumentId) -> bool {
        if self.is_locked(instrument) {
            return false;
        }
        self.active.insert(instrument.clone(), true);
        true
    }

    /// Releases `instrument` (a no-op when it is not held).
    pub fn release(&mut self, instrument: &InstrumentId) {
        self.active.remove(instrument);
    }

    /// Whether `instrument` is currently held.
    pub fn is_locked(&self, instrument: &InstrumentId) -> bool {
        self.active.get(instrument).copied().unwrap_or(false)
    }

    /// The instruments currently held, in no particular order.
    pub fn locked(&self) -> Vec<InstrumentId> {
        self.active.keys().cloned().collect()
    }

    /// Releases every held instrument.
    pub fn clear(&mut self) {
        self.active.clear();
    }
}

/// Turns a desired position into an order: the quantity step its delta floors to, plus an optional
/// freeze cap.
///
/// Build it from an instrument's [`InstrumentRules`] — as the active [`MarketProfile`] gives them
/// ([`Self::from_profile`]) — or directly with [`Self::with_lot_size`]. The default is a lot of 1
/// and no freeze, the shape of cash equity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TargetPosition {
    lot_size: f64,
    max_order_quantity: Option<f64>,
}

impl Default for TargetPosition {
    fn default() -> Self {
        Self {
            lot_size: 1.0,
            max_order_quantity: None,
        }
    }
}

impl TargetPosition {
    /// A target sizer with lot 1 and no freeze.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the quantity step the delta floors to. Errors (state unchanged) when `lot_size` is not
    /// a positive, finite number.
    pub fn with_lot_size(mut self, lot_size: f64) -> Result<Self> {
        if !(lot_size.is_finite() && lot_size > 0.0) {
            return Err(AlgoError::Component(format!(
                "lot_size must be positive and finite, got {lot_size}"
            )));
        }
        self.lot_size = lot_size;
        Ok(self)
    }

    /// Sets the freeze (maximum single-order) quantity the delta's clips cap at. Errors (state
    /// unchanged) when `freeze_qty` is not a positive, finite number.
    pub fn with_freeze(mut self, freeze_qty: f64) -> Result<Self> {
        if !(freeze_qty.is_finite() && freeze_qty > 0.0) {
            return Err(AlgoError::Component(format!(
                "freeze_qty must be positive and finite, got {freeze_qty}"
            )));
        }
        self.max_order_quantity = Some(freeze_qty);
        Ok(self)
    }

    /// Reads an instrument's rules from `profile` (its lot size and, when it has one, its freeze
    /// quantity).
    pub fn from_profile(profile: &dyn MarketProfile, instrument: &Instrument) -> Self {
        Self::from_rules(&profile.instrument_rules().rules_for(instrument))
    }

    /// Reads lot size and freeze directly from an instrument's rules.
    pub fn from_rules(rules: &InstrumentRules) -> Self {
        Self {
            lot_size: rules.lot_size,
            max_order_quantity: rules.max_order_quantity,
        }
    }

    /// The quantity step the delta floors to.
    pub fn lot_size(&self) -> f64 {
        self.lot_size
    }

    /// The freeze (maximum single-order) quantity, if the instrument has one.
    pub fn max_order_quantity(&self) -> Option<f64> {
        self.max_order_quantity
    }

    /// The signed quantity to trade to move `current` to `target`.
    ///
    /// The raw difference floors to a whole number of lots towards zero, so a target is never
    /// overshot; the result is capped at the freeze when one is set (this is the first clip —
    /// [`Self::delta_clips`] returns the lot/freeze-split sequence). A result within [`QTY_EPS`]
    /// of zero is returned as `0.0`.
    pub fn delta(&self, current: &Position, target: f64) -> f64 {
        let raw = target - net_position(current);
        let lots = raw / self.lot_size;
        let lots = if (lots - lots.round()).abs() <= LOT_TOLERANCE {
            lots.round()
        } else {
            lots.trunc()
        };
        let mut qty = lots * self.lot_size;
        if let Some(max) = self.max_order_quantity {
            if qty.abs() > max + QTY_EPS {
                let clip = (max / self.lot_size + LOT_TOLERANCE).floor() * self.lot_size;
                qty = qty.signum() * qty.abs().min(clip);
            }
        }
        if qty.abs() <= QTY_EPS {
            0.0
        } else {
            qty
        }
    }

    /// The market order that moves `current` to `target`, or `None` when already at target.
    ///
    /// `side_hint`, when given, must agree with the delta's direction; a contradicting hint is an
    /// error rather than a silently reversed order.
    pub fn order(
        &self,
        current: &Position,
        target: f64,
        order_id: &str,
        side_hint: Option<OrderSide>,
        ts: UnixNanos,
    ) -> Result<Option<Order>> {
        let delta = self.delta(current, target);
        if delta.abs() <= QTY_EPS {
            return Ok(None);
        }
        let side = if delta > 0.0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if let Some(hint) = side_hint {
            if hint != side {
                return Err(AlgoError::Component(format!(
                    "side hint {hint:?} contradicts the target delta for {}",
                    current.instrument_id().symbol()
                )));
            }
        }
        Ok(Some(Order::new(
            OrderId::new(order_id),
            current.instrument_id().clone(),
            side,
            OrderType::Market,
            delta.abs(),
            None,
            TimeInForce::Day,
            ts,
            ts,
        )))
    }

    /// The signed lot-aligned clips that move `current` to `target`, each no larger than the
    /// freeze.
    ///
    /// The raw delta is lot-rounded exactly as [`Self::delta`] does; it is then sliced by
    /// [`crate::lot::split_order`] into clips that are each a whole number of lots and no larger
    /// than the freeze, so their fills aggregate to the same quantity one uncapped order would
    /// trade. The clips are signed like the delta (buys positive, sells negative); the vector is
    /// empty when already at target or when the delta is below one lot.
    pub fn delta_clips(&self, current: &Position, target: f64) -> Vec<f64> {
        let raw = target - net_position(current);
        let lots = raw / self.lot_size;
        let lots = if (lots - lots.round()).abs() <= LOT_TOLERANCE {
            lots.round()
        } else {
            lots.trunc()
        };
        let magnitude = (lots * self.lot_size).abs();
        if magnitude <= QTY_EPS {
            return Vec::new();
        }
        let freeze = self.max_order_quantity.unwrap_or(f64::INFINITY);
        let sign = raw.signum();
        split_order(magnitude, self.lot_size, freeze)
            .into_iter()
            .map(|clip| sign * clip)
            .collect()
    }

    /// The market orders that move `current` to `target`, sliced to respect lot and freeze.
    ///
    /// Each clip from [`Self::delta_clips`] becomes one market order: the first carries
    /// `order_id`, the rest `order_id-2`, `order_id-3`, … `side_hint`, when given, must agree
    /// with the delta's direction; a contradicting hint is an error rather than a silently
    /// reversed order. Empty when already at target.
    pub fn orders(
        &self,
        current: &Position,
        target: f64,
        order_id: &str,
        side_hint: Option<OrderSide>,
        ts: UnixNanos,
    ) -> Result<Vec<Order>> {
        let clips = self.delta_clips(current, target);
        if clips.is_empty() {
            return Ok(Vec::new());
        }
        let side = if clips[0] > 0.0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        if let Some(hint) = side_hint {
            if hint != side {
                return Err(AlgoError::Component(format!(
                    "side hint {hint:?} contradicts the target delta for {}",
                    current.instrument_id().symbol()
                )));
            }
        }
        Ok(clips
            .iter()
            .enumerate()
            .map(|(i, clip)| {
                let id = if i == 0 {
                    order_id.to_string()
                } else {
                    format!("{order_id}-{}", i + 1)
                };
                Order::new(
                    OrderId::new(id),
                    current.instrument_id().clone(),
                    side,
                    OrderType::Market,
                    clip.abs(),
                    None,
                    TimeInForce::Day,
                    ts,
                    ts,
                )
            })
            .collect())
    }
}

/// Drives target-position submission with per-instrument serialisation.
///
/// Holds a [`TargetPosition`] sizer and a [`PerInstrumentLock`]. [`Self::submit_target_position`]
/// computes the delta for the current position and, when the instrument is free, returns the one
/// order that closes the gap and books the instrument; when the instrument already has an
/// outstanding target order it returns `None` (deferred) so a working order is never doubled.
#[derive(Debug, Default, Clone)]
pub struct SmartOrderController {
    sizing: TargetPosition,
    lock: PerInstrumentLock,
    seq: u64,
}

impl SmartOrderController {
    /// A controller with the default sizer (lot 1, no freeze).
    pub fn new() -> Self {
        Self::default()
    }

    /// A controller using `sizing` to turn targets into orders.
    pub fn with_sizing(sizing: TargetPosition) -> Self {
        Self {
            sizing,
            ..Self::default()
        }
    }

    /// The per-instrument serialisation lock.
    pub fn lock(&self) -> &PerInstrumentLock {
        &self.lock
    }

    /// Whether `instrument` already has an outstanding target order.
    pub fn is_busy(&self, instrument: &InstrumentId) -> bool {
        self.lock.is_locked(instrument)
    }

    /// The instruments with an outstanding target order, in no particular order.
    pub fn busy(&self) -> Vec<InstrumentId> {
        self.lock.locked()
    }

    /// Submits the target `target` for the instrument of `current`.
    ///
    /// Returns `Ok(None)` when already at target, and `Ok(None)` (deferred) when the instrument
    /// already has an outstanding order; otherwise returns the single order that moves the
    /// position towards the target and books the instrument until [`Self::release`].
    pub fn submit_target_position(
        &mut self,
        current: &Position,
        target: f64,
        side_hint: Option<OrderSide>,
        ts: UnixNanos,
    ) -> Result<Option<Order>> {
        let instrument = current.instrument_id().clone();
        if !self.lock.acquire(&instrument) {
            return Ok(None);
        }
        self.seq += 1;
        let order_id = format!("target-{}-{}", instrument.symbol(), self.seq);
        match self.sizing.order(current, target, &order_id, side_hint, ts) {
            Ok(Some(order)) => Ok(Some(order)),
            Ok(None) => {
                self.lock.release(&instrument);
                Ok(None)
            }
            Err(e) => {
                self.lock.release(&instrument);
                Err(e)
            }
        }
    }

    /// Submits the target `target` for the instrument of `current`, sliced to respect lot and
    /// freeze.
    ///
    /// Returns an empty vector when already at target, and an empty vector (deferred) when the
    /// instrument already has an outstanding order; otherwise returns the sequence of
    /// lot-aligned, freeze-capped clip orders that move the position towards the target and
    /// books the instrument until [`Self::release`]. Every clip must fill (or be cancelled)
    /// before the instrument is released.
    pub fn submit_target_orders(
        &mut self,
        current: &Position,
        target: f64,
        side_hint: Option<OrderSide>,
        ts: UnixNanos,
    ) -> Result<Vec<Order>> {
        let instrument = current.instrument_id().clone();
        if !self.lock.acquire(&instrument) {
            return Ok(Vec::new());
        }
        self.seq += 1;
        let order_id = format!("target-{}-{}", instrument.symbol(), self.seq);
        match self
            .sizing
            .orders(current, target, &order_id, side_hint, ts)
        {
            Ok(orders) if orders.is_empty() => {
                self.lock.release(&instrument);
                Ok(Vec::new())
            }
            Ok(orders) => Ok(orders),
            Err(e) => {
                self.lock.release(&instrument);
                Err(e)
            }
        }
    }

    /// Releases `instrument`'s serialisation lock once its order has filled, been rejected or
    /// cancelled. A no-op when the instrument is not held.
    pub fn release(&mut self, instrument: &InstrumentId) {
        self.lock.release(instrument);
    }

    /// Releases every instrument's lock.
    pub fn release_all(&mut self) {
        self.lock.clear();
    }
}
