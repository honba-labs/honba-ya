//! Execution engine trait: one ordered event drain (ADR 0019 decision 4) and
//! the legacy two-drain shims kept until 0.3.0.

use std::collections::HashMap;

use honba_entities::{ExecutionEvent, Trade};
use honba_messages::{InstrumentId, Order, OrderEvent, OrderId, OrderSide, OrderState, UnixNanos};

use crate::error::Result;

/// An order, or the part of one, that will never fill (ADR 008, decision 13).
///
/// Either the venue or engine refused it, or it was cancelled: a cancellation is
/// exactly a rejection whose `reason` is [`OrderRejection::CANCELLED`], so
/// [`Self::is_cancelled`] is derived from the reason and the two cannot disagree.
/// `quantity` is the unfilled remainder: the amount the strategy's context must
/// release, so a partly filled order reports only what is left. The Python
/// mirror is `honba.strategies.execution.OrderRejection`.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderRejection {
    /// The order the remainder belongs to.
    pub order_id: OrderId,
    /// The instrument of the order.
    pub instrument_id: InstrumentId,
    /// The side of the order.
    pub side: OrderSide,
    /// The quantity that will never fill.
    pub quantity: f64,
    /// Why, in a stable machine-readable form (`insufficient_funds`,
    /// `no_position`, `cancelled`, ...).
    pub reason: String,
    /// The engine's time for the event.
    pub ts: UnixNanos,
}

impl OrderRejection {
    /// The reason string of a cancellation.
    pub const CANCELLED: &'static str = "cancelled";

    /// The reason string an [`ExecutionEvent::Expired`] maps to in the legacy
    /// rejection view (ADR 0019 decision 4, buffered shim).
    pub const EXPIRED: &'static str = "expired";

    /// A rejection of `quantity` of an order, for `reason`.
    pub fn rejected(
        order_id: OrderId,
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: f64,
        reason: impl Into<String>,
        ts: UnixNanos,
    ) -> Self {
        Self {
            order_id,
            instrument_id,
            side,
            quantity,
            reason: reason.into(),
            ts,
        }
    }

    /// True for a cancellation (reason [`Self::CANCELLED`]), false for a rejection.
    pub fn is_cancelled(&self) -> bool {
        self.reason == Self::CANCELLED
    }

    /// A cancellation of the unfilled `quantity` of an order.
    pub fn cancelled(
        order_id: OrderId,
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: f64,
        ts: UnixNanos,
    ) -> Self {
        Self::rejected(order_id, instrument_id, side, quantity, Self::CANCELLED, ts)
    }
}

/// Receives orders and reports what became of them.
///
/// Implementations range from a paper-trading simulator (fills against
/// observed prices) to a live adapter (routes to a broker).
///
/// **One ordered drain** (ADR 0019 decision 4): [`Self::drain_events`] returns
/// every [`ExecutionEvent`] the engine produced since the last call, in the
/// order it produced them; queue order, not timestamp, is the tiebreak. The
/// legacy pair [`Self::drain_fills`] / [`Self::drain_rejections`] stays until
/// 0.3.0 (ADR 0008 decision 7 policy):
///
/// - an engine that implements `drain_events` natively says so with
///   [`Self::native_events`] and serves the legacy pair through a
///   [`LegacyDrains`] buffer (each legacy call drains the events once and
///   splits them; nothing is lost, only the cross-queue order);
/// - an engine that implements only the legacy pair gets the default
///   `drain_events` (fills, then rejections). That default has no per-order
///   state, so it reports every fill as complete; wrap such an engine in
///   [`LegacyPortEvents`] for correct partial fills (the [`Engine`](crate::Engine)
///   does so itself).
///
/// Implementations never emit `Submitted` or `CancelRequested`: the submitter
/// synthesises those. An L1 engine (simulators without a venue round trip)
/// never emits `Accepted`.
pub trait ExecutionEngine: Send {
    /// Submits an order.
    fn submit(&mut self, order: Order) -> Result<()>;

    /// Cancels an order by id, at time `now`.
    ///
    /// An engine holding the order reports the unfilled remainder as an
    /// [`ExecutionEvent::Cancelled`] (legacy view: a rejection with
    /// `is_cancelled()`). Cancelling an unknown or finished order is a no-op.
    /// The cancellation is stamped with `now`, the engine time at which the
    /// cancel is processed, not the order's original `ts_event` (ADR 008,
    /// decision 13 addendum).
    ///
    /// Fills the engine has produced but not yet drained are not an ordering
    /// hazard: the cancelled remainder excludes whatever already filled, and
    /// the context's pending count only ever decreases by `filled + released`,
    /// which sums to the ordered quantity whichever is booked first. The
    /// remainder must never include a quantity that is also in a fill.
    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()>;

    /// Drains every event produced since the last call, in production order.
    ///
    /// The default adapts an engine that implements only the legacy pair:
    /// its fills, then its rejections (the documented *legacy* order), each
    /// fill reported as `complete` with `cum_qty` equal to its own quantity.
    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        let fills = self.drain_fills()?;
        let rejections = self.drain_rejections()?;
        Ok(legacy_events(
            fills.into_iter().map(|t| {
                let q = t.quantity();
                (t, q, true)
            }),
            rejections,
        ))
    }

    /// Whether [`Self::drain_events`] is implemented natively. Legacy engines
    /// (the default, `false`) are wrapped in [`LegacyPortEvents`] by callers
    /// that need correct partial fills. Removed with the legacy pair.
    fn native_events(&self) -> bool {
        false
    }

    /// Legacy: drains any fills produced since the last call.
    ///
    /// On a native engine this is the buffered shim over
    /// [`Self::drain_events`] (see [`LegacyDrains`]).
    fn drain_fills(&mut self) -> Result<Vec<Trade>>;

    /// Legacy: drains the orders (or parts of orders) that will never fill,
    /// rejected, cancelled or expired since the last call.
    ///
    /// The default reports none: an engine that fills everything it accepts
    /// needs no override, so engines written before this method compile
    /// unchanged. Once every working order is cancelled, `filled + released ==
    /// ordered` for each order.
    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        Ok(Vec::new())
    }
}

/// Converts legacy drains into events: `(trade, cum_qty, complete)` fills
/// first, then each rejection as `Cancelled` (reason
/// [`OrderRejection::CANCELLED`]) or `Rejected`.
fn legacy_events(
    fills: impl IntoIterator<Item = (Trade, f64, bool)>,
    rejections: Vec<OrderRejection>,
) -> Vec<ExecutionEvent> {
    let mut events: Vec<ExecutionEvent> = fills
        .into_iter()
        .map(|(trade, cum_qty, complete)| ExecutionEvent::Fill {
            trade,
            cum_qty,
            complete,
            venue_order_id: None,
        })
        .collect();
    events.extend(rejections.into_iter().map(|r| {
        if r.is_cancelled() {
            ExecutionEvent::Cancelled {
                order_id: r.order_id,
                instrument_id: r.instrument_id,
                side: r.side,
                quantity: r.quantity,
                venue_order_id: None,
                ts: r.ts,
            }
        } else {
            ExecutionEvent::Rejected {
                order_id: r.order_id,
                instrument_id: r.instrument_id,
                side: r.side,
                quantity: r.quantity,
                reason: r.reason,
                venue_order_id: None,
                ts: r.ts,
            }
        }
    }));
    events
}

/// The buffered legacy view of an event stream (ADR 0019 decision 4, "new
/// port, legacy caller").
///
/// [`Self::absorb`] splits events into a fill buffer (`Fill` -> [`Trade`]) and
/// a rejection buffer (`Rejected`/`Cancelled`/`Expired` -> [`OrderRejection`],
/// `Expired` with reason [`OrderRejection::EXPIRED`]); `Submitted`, `Accepted`
/// and `CancelRequested` have no legacy form and are dropped. Each take
/// returns and clears only its own buffer, so no fill or release is lost; only
/// the order between the two queues is.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LegacyDrains {
    fills: Vec<Trade>,
    rejections: Vec<OrderRejection>,
}

impl LegacyDrains {
    /// An empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Splits `events` into the two buffers, keeping their order within each.
    pub fn absorb(&mut self, events: Vec<ExecutionEvent>) {
        for ev in events {
            let rejection = |order_id, instrument_id, side, quantity, reason: &str, ts| {
                OrderRejection::rejected(order_id, instrument_id, side, quantity, reason, ts)
            };
            match ev {
                ExecutionEvent::Fill { trade, .. } => self.fills.push(trade),
                ExecutionEvent::Rejected {
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    reason,
                    ts,
                    ..
                } => self.rejections.push(rejection(
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    &reason,
                    ts,
                )),
                ExecutionEvent::Cancelled {
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    ts,
                    ..
                } => self.rejections.push(rejection(
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    OrderRejection::CANCELLED,
                    ts,
                )),
                ExecutionEvent::Expired {
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    ts,
                    ..
                } => self.rejections.push(rejection(
                    order_id,
                    instrument_id,
                    side,
                    quantity,
                    OrderRejection::EXPIRED,
                    ts,
                )),
                _ => {}
            }
        }
    }

    /// Returns and clears the fill buffer.
    pub fn take_fills(&mut self) -> Vec<Trade> {
        std::mem::take(&mut self.fills)
    }

    /// Returns and clears the rejection buffer.
    pub fn take_rejections(&mut self) -> Vec<OrderRejection> {
        std::mem::take(&mut self.rejections)
    }
}

/// Adapts an engine that implements only the legacy pair to the event drain
/// (ADR 0019 decision 4, "legacy port, new runner").
///
/// It records each submitted order's quantity in its own [`OrderState`], so a
/// legacy engine's partial fills come out with the right `cum_qty` and
/// `complete`. Each [`ExecutionEngine::drain_events`] returns the inner
/// engine's fills, then its rejections (the documented legacy order).
pub struct LegacyPortEvents<E> {
    inner: E,
    states: HashMap<String, OrderState>,
}

impl<E: ExecutionEngine> LegacyPortEvents<E> {
    /// Wraps `inner`.
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            states: HashMap::new(),
        }
    }

    /// The wrapped engine.
    pub fn inner(&self) -> &E {
        &self.inner
    }

    /// Unwraps the engine.
    pub fn into_inner(self) -> E {
        self.inner
    }

    fn fill_progress(&mut self, trade: &Trade) -> (f64, bool) {
        let q = trade.quantity();
        let Some(state) = self.states.get_mut(trade.order_id().as_str()) else {
            return (q, true);
        };
        let cum = state.filled_qty + q;
        let complete = cum + 1e-9 >= state.quantity;
        // An illegal fill (overfill) is left for the consumer to flag.
        let _ = state.apply(&OrderEvent::Fill {
            last_qty: q,
            complete,
        });
        (cum, complete)
    }
}

impl<E: ExecutionEngine> ExecutionEngine for LegacyPortEvents<E> {
    fn submit(&mut self, order: Order) -> Result<()> {
        let id = order.order_id().as_str().to_string();
        let quantity = order.quantity();
        self.inner.submit(order)?;
        let mut state = OrderState::new();
        let _ = state.apply(&OrderEvent::Submitted { quantity });
        self.states.insert(id, state);
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        self.inner.cancel(order_id, now)
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        let fills = self.inner.drain_fills()?;
        let rejections = self.inner.drain_rejections()?;
        let fills: Vec<(Trade, f64, bool)> = fills
            .into_iter()
            .map(|t| {
                let (cum, complete) = self.fill_progress(&t);
                (t, cum, complete)
            })
            .collect();
        Ok(legacy_events(fills, rejections))
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        self.inner.drain_fills()
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        self.inner.drain_rejections()
    }
}

/// A boxed engine is an engine (so a `Box<dyn ExecutionEngine>` can be wrapped).
impl<E: ExecutionEngine + ?Sized> ExecutionEngine for Box<E> {
    fn submit(&mut self, order: Order) -> Result<()> {
        (**self).submit(order)
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        (**self).cancel(order_id, now)
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        (**self).drain_events()
    }

    fn native_events(&self) -> bool {
        (**self).native_events()
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        (**self).drain_fills()
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        (**self).drain_rejections()
    }
}
