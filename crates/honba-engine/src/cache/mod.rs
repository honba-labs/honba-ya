//! The state cache: orders, positions, instruments, and market data (E2-S9).
//!
//! Provides a deterministic in-memory view over the current runtime state,
//! updated either event-by-event ([`StateCache::apply_event`]) or initialized
//! from snapshots / seeds. Used by execution, risk, strategies, and reconciliation.

use std::collections::HashMap;

use honba_entities::Instrument;
use honba_messages::{
    Bar, Event, InstrumentId, OrderEvent, OrderSide, OrderState, OrderStatus, QuoteTick, UnixNanos,
    VenueOrderId,
};

/// What the engine knows about one order it submitted or refused (ADR 0019
/// decision 5). Entries are never removed during a run: a terminal entry
/// answers "what happened to `O-7`".
#[derive(Clone, Debug, PartialEq)]
pub struct TrackedOrder {
    /// The order's lifecycle state.
    pub state: OrderState,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// The side.
    pub side: OrderSide,
    /// The first venue order id any event named, if any.
    pub venue_order_id: Option<VenueOrderId>,
}

impl TrackedOrder {
    /// Whether the order is currently working.
    pub fn is_working(&self) -> bool {
        matches!(
            self.state.status,
            OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        )
    }
}

/// Query trait for inspecting cached orders, positions, instruments, and market data.
pub trait CacheQuery {
    /// Returns the tracked order for `order_id`, if any.
    fn order(&self, order_id: &str) -> Option<&TrackedOrder>;
    /// Returns all tracked orders.
    fn orders(&self) -> &HashMap<String, TrackedOrder>;
    /// Returns all orders that are currently working (Submitted, Accepted, PartiallyFilled).
    fn open_orders(&self) -> Vec<&TrackedOrder>;
    /// Net signed position in `instrument` (0.0 if flat/never traded).
    fn position(&self, instrument: &InstrumentId) -> f64;
    /// Returns all positions map.
    fn positions(&self) -> &HashMap<InstrumentId, f64>;
    /// Metadata for `instrument`, if known.
    fn instrument(&self, instrument: &InstrumentId) -> Option<&Instrument>;
    /// All known instruments.
    fn instruments(&self) -> &HashMap<InstrumentId, Instrument>;
    /// Latest quote tick for `instrument`, if any.
    fn last_quote(&self, instrument: &InstrumentId) -> Option<&QuoteTick>;
    /// Latest bar for `instrument`, if any.
    fn last_bar(&self, instrument: &InstrumentId) -> Option<&Bar>;
    /// Latest observed price for `instrument`.
    fn last_price(&self, instrument: &InstrumentId) -> Option<f64>;
    /// Timestamp of the latest applied event.
    fn last_ts(&self) -> UnixNanos;
}

/// An in-memory cache of trading state: orders, positions, instruments, and latest market data.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StateCache {
    orders: HashMap<String, TrackedOrder>,
    positions: HashMap<InstrumentId, f64>,
    instruments: HashMap<InstrumentId, Instrument>,
    quotes: HashMap<InstrumentId, QuoteTick>,
    bars: HashMap<InstrumentId, Bar>,
    last_px: HashMap<InstrumentId, f64>,
    last_ts: UnixNanos,
}

impl StateCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds initial positions.
    pub fn with_positions(mut self, seed: impl IntoIterator<Item = (InstrumentId, f64)>) -> Self {
        self.positions.extend(seed);
        self
    }

    /// Seeds initial instruments.
    pub fn with_instruments(mut self, instruments: impl IntoIterator<Item = Instrument>) -> Self {
        for inst in instruments {
            self.instruments.insert(inst.id().clone(), inst);
        }
        self
    }

    /// Sets or updates the position for `instrument`.
    pub fn seed_position(&mut self, instrument: InstrumentId, qty: f64) {
        self.positions.insert(instrument, qty);
    }

    /// Registers instrument metadata.
    pub fn add_instrument(&mut self, instrument: Instrument) {
        self.instruments.insert(instrument.id().clone(), instrument);
    }

    /// Seeds a tracked order into the cache.
    pub fn seed_order(&mut self, order_id: String, tracked: TrackedOrder) {
        self.orders.insert(order_id, tracked);
    }

    /// Applies an event to update cached orders, positions, or market data.
    pub fn apply_event(&mut self, event: &Event) {
        self.last_ts = event.ts_event();
        match event {
            Event::Quote(quote) => {
                let id = quote.instrument_id().clone();
                let mid = (quote.bid_price() + quote.ask_price()) / 2.0;
                self.quotes.insert(id.clone(), quote.clone());
                self.last_px.insert(id, mid);
            }
            Event::Bar(bar) => {
                let id = bar.bar_type().instrument_id().clone();
                self.bars.insert(id.clone(), bar.clone());
                self.last_px.insert(id, bar.close());
            }
            Event::Trade(tick) => {
                let id = tick.instrument_id().clone();
                self.last_px.insert(id, tick.price());
            }
            Event::Order(order) => {
                let id = order.order_id().as_str().to_string();
                self.orders.entry(id).or_insert_with(|| {
                    let mut state = OrderState::new();
                    let _ = state.apply(&OrderEvent::Submitted {
                        quantity: order.quantity(),
                    });
                    TrackedOrder {
                        state,
                        instrument_id: order.instrument_id().clone(),
                        side: order.side(),
                        venue_order_id: None,
                    }
                });
            }
            Event::OrderAccepted {
                order_id,
                venue_order_id,
                ..
            } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Accepted);
                    if let Some(v_id) = venue_order_id {
                        tracked.venue_order_id = Some(v_id.clone());
                    }
                }
            }
            Event::OrderRejected { order_id, .. } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Rejected);
                }
            }
            Event::OrderPartiallyFilled {
                order_id,
                last_qty,
                last_px,
                ..
            } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Fill {
                        last_qty: *last_qty,
                        complete: false,
                    });
                    let signed = match tracked.side {
                        OrderSide::Sell => -*last_qty,
                        _ => *last_qty,
                    };
                    *self
                        .positions
                        .entry(tracked.instrument_id.clone())
                        .or_insert(0.0) += signed;
                    self.last_px.insert(tracked.instrument_id.clone(), *last_px);
                }
            }
            Event::OrderFilled {
                order_id,
                last_qty,
                last_px,
                ..
            } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Fill {
                        last_qty: *last_qty,
                        complete: true,
                    });
                    let signed = match tracked.side {
                        OrderSide::Sell => -*last_qty,
                        _ => *last_qty,
                    };
                    *self
                        .positions
                        .entry(tracked.instrument_id.clone())
                        .or_insert(0.0) += signed;
                    self.last_px.insert(tracked.instrument_id.clone(), *last_px);
                }
            }
            Event::OrderCancelled { order_id, .. } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Cancelled);
                }
            }
            Event::OrderExpired { order_id, .. } => {
                let id = order_id.as_str();
                if let Some(tracked) = self.orders.get_mut(id) {
                    let _ = tracked.state.apply(&OrderEvent::Expired);
                }
            }
            _ => {}
        }
    }
}

impl CacheQuery for StateCache {
    fn order(&self, order_id: &str) -> Option<&TrackedOrder> {
        self.orders.get(order_id)
    }

    fn orders(&self) -> &HashMap<String, TrackedOrder> {
        &self.orders
    }

    fn open_orders(&self) -> Vec<&TrackedOrder> {
        self.orders.values().filter(|o| o.is_working()).collect()
    }

    fn position(&self, instrument: &InstrumentId) -> f64 {
        self.positions.get(instrument).copied().unwrap_or(0.0)
    }

    fn positions(&self) -> &HashMap<InstrumentId, f64> {
        &self.positions
    }

    fn instrument(&self, instrument: &InstrumentId) -> Option<&Instrument> {
        self.instruments.get(instrument)
    }

    fn instruments(&self) -> &HashMap<InstrumentId, Instrument> {
        &self.instruments
    }

    fn last_quote(&self, instrument: &InstrumentId) -> Option<&QuoteTick> {
        self.quotes.get(instrument)
    }

    fn last_bar(&self, instrument: &InstrumentId) -> Option<&Bar> {
        self.bars.get(instrument)
    }

    fn last_price(&self, instrument: &InstrumentId) -> Option<f64> {
        self.last_px.get(instrument).copied()
    }

    fn last_ts(&self) -> UnixNanos {
        self.last_ts
    }
}
