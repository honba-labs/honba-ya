//! A deterministic execution engine whose behavior per order is scripted.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use honba_engine::{AlgoError, ExecutionEngine, LegacyDrains, OrderRejection, Result};
use honba_entities::{Currency, ExecutionEvent, Trade};
use honba_messages::{Order, OrderId, OrderSide, UnixNanos, VenueOrderId};

/// Quantities closer than this are equal (ADR 0016 convention).
const QTY_EPS: f64 = 1e-9;

/// One thing the scripted venue does to a working order (ADR 0019 decision 7).
#[derive(Clone, Debug, PartialEq)]
pub enum VenueAction {
    /// Acknowledge the order (the `ack` profile; L1 never acks unscripted).
    Accept {
        /// The venue's id for the order, if the ack names one.
        venue_order_id: Option<VenueOrderId>,
    },
    /// Fill `quantity` at the engine's price; the fill that reaches the order
    /// quantity completes it.
    Fill {
        /// The quantity of this fill.
        quantity: f64,
    },
    /// Reject the unfilled remainder (venue-initiated).
    Reject {
        /// Why.
        reason: String,
    },
    /// Cancel the unfilled remainder (unsolicited, or the IOC/FOK remainder).
    Cancel,
    /// Expire the unfilled remainder (time-in-force expiry).
    Expire,
}

/// What a [`ScriptedExecution`] does with an order it accepts.
#[derive(Clone, Debug, PartialEq)]
pub enum Behavior {
    /// Fill in full (the default for an unscripted order).
    Fill,
    /// Refuse the whole order for `reason`.
    Reject {
        /// Why it was refused.
        reason: String,
    },
    /// Fill `filled`, refuse the rest for `reason`.
    Partial {
        /// The quantity that fills.
        filled: f64,
        /// Why the remainder was refused.
        reason: String,
    },
    /// Keep the order working until it is cancelled.
    Hold,
    /// Expire the whole order at submit (time-in-force expiry).
    Expire,
    /// Apply these venue actions at submit, in order, stamped with the
    /// order's `ts_event`; an order still working afterwards is held.
    Script(Vec<VenueAction>),
}

impl Behavior {
    /// Refuses the whole order for `reason`.
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    /// Fills `filled` and refuses the remainder for `reason`.
    pub fn partial(filled: f64, reason: impl Into<String>) -> Self {
        Self::Partial {
            filled,
            reason: reason.into(),
        }
    }
}

/// An [`ExecutionEngine`] that fills at one fixed price and does with each
/// order what its script says (fill, reject, partly fill, expire, run a
/// sequence of [`VenueAction`]s, or hold until cancelled). Fills and
/// rejections at submit are stamped with the order's `ts_event`; a
/// cancellation with the time of the cancel; a [`Self::venue`] action with
/// the time it is given.
///
/// Clones share one venue: keep a clone to drive venue events
/// ([`Self::venue`], [`Self::inject`]) while another is attached to an
/// engine. It emits [`ExecutionEvent`]s in one ordered queue
/// ([`ExecutionEngine::drain_events`]); the legacy drains are the buffered
/// shim. It never acknowledges an order unless scripted to
/// ([`VenueAction::Accept`]).
///
/// It is the reference engine for the shared conformance vectors
/// `schema/conformance/order_rejections.json`.
#[derive(Clone)]
pub struct ScriptedExecution {
    inner: Arc<Mutex<Inner>>,
}

struct Working {
    order: Order,
    filled: f64,
}

struct Inner {
    price: f64,
    currency: Currency,
    script: HashMap<String, Behavior>,
    working: Vec<Working>,
    events: Vec<ExecutionEvent>,
    legacy: LegacyDrains,
}

impl ScriptedExecution {
    /// Creates an engine that fills at `price`; unscripted orders fill in full.
    pub fn new(price: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                price,
                currency: Currency::Inr,
                script: HashMap::new(),
                working: Vec::new(),
                events: Vec::new(),
                legacy: LegacyDrains::new(),
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("scripted venue lock poisoned")
    }

    /// Scripts what happens to the order with id `order_id`. A later entry
    /// for the same id replaces the earlier one; the script applies each time
    /// that id is submitted.
    ///
    /// # Panics
    ///
    /// If `behavior` is a [`Behavior::Partial`] whose `filled` is not positive
    /// and finite: the script is a test fixture, so a bad one fails loudly when
    /// built. A `filled` at or above the order's quantity is only known at
    /// submit, where it is refused with an error.
    #[must_use]
    pub fn with(self, order_id: impl Into<String>, behavior: Behavior) -> Self {
        if let Behavior::Partial { filled, .. } = &behavior {
            assert!(
                filled.is_finite() && *filled > 0.0,
                "partial fill must be positive and finite, got {filled}"
            );
        }
        self.lock().script.insert(order_id.into(), behavior);
        self
    }

    /// Ids of the held orders, in submission order.
    pub fn working_orders(&self) -> Vec<String> {
        self.lock()
            .working
            .iter()
            .map(|w| w.order.order_id().as_str().to_string())
            .collect()
    }

    /// The venue does `action` to the working order `order_id` at `ts`.
    ///
    /// Errors, emitting nothing, when the venue does not hold the order (never
    /// submitted, or already finished) or when a fill is not positive or would
    /// exceed the order quantity.
    pub fn venue(&self, order_id: &str, action: VenueAction, ts: UnixNanos) -> Result<()> {
        let mut inner = self.lock();
        let Some(i) = inner
            .working
            .iter()
            .position(|w| w.order.order_id().as_str() == order_id)
        else {
            return Err(AlgoError::Component(format!(
                "scripted venue holds no working order {order_id}"
            )));
        };
        inner.act(i, action, ts)
    }

    /// Enqueues `event` exactly as given, bypassing the venue's bookkeeping
    /// (duplicate, late or illegal events for FSM tests).
    pub fn inject(&self, event: ExecutionEvent) {
        self.lock().events.push(event);
    }
}

impl Inner {
    fn trade(&self, order: &Order, quantity: f64, ts: UnixNanos) -> Trade {
        // Buy or Sell: `submit` refuses an order without a side.
        Trade::new(
            OrderId::new(order.order_id().as_str()),
            order.instrument_id().clone(),
            order.side(),
            quantity,
            self.price,
            self.currency,
            ts,
            ts,
        )
    }

    fn fill(&mut self, order: &Order, filled: f64, quantity: f64, ts: UnixNanos) {
        let cum = filled + quantity;
        let trade = self.trade(order, quantity, ts);
        self.events.push(ExecutionEvent::Fill {
            trade,
            cum_qty: cum,
            complete: cum + QTY_EPS >= order.quantity(),
            venue_order_id: None,
        });
    }

    fn release(&mut self, order: &Order, quantity: f64, how: VenueAction, ts: UnixNanos) {
        let (order_id, instrument_id, side) = (
            order.order_id().clone(),
            order.instrument_id().clone(),
            order.side(),
        );
        self.events.push(match how {
            VenueAction::Reject { reason } => ExecutionEvent::Rejected {
                order_id,
                instrument_id,
                side,
                quantity,
                reason,
                venue_order_id: None,
                ts,
            },
            VenueAction::Expire => ExecutionEvent::Expired {
                order_id,
                instrument_id,
                side,
                quantity,
                venue_order_id: None,
                ts,
            },
            _ => ExecutionEvent::Cancelled {
                order_id,
                instrument_id,
                side,
                quantity,
                venue_order_id: None,
                ts,
            },
        });
    }

    /// Applies `action` to working order `i`; a finished order leaves the book.
    fn act(&mut self, i: usize, action: VenueAction, ts: UnixNanos) -> Result<()> {
        let order = self.working[i].order.clone();
        let filled = self.working[i].filled;
        let open = order.quantity() - filled;
        match action {
            VenueAction::Accept { venue_order_id } => {
                self.events.push(ExecutionEvent::Accepted {
                    order_id: order.order_id().clone(),
                    instrument_id: order.instrument_id().clone(),
                    side: order.side(),
                    quantity: open,
                    venue_order_id,
                    ts,
                });
            }
            VenueAction::Fill { quantity } => {
                if !(quantity.is_finite() && quantity > 0.0) || quantity > open + QTY_EPS {
                    return Err(AlgoError::Component(format!(
                        "scripted fill {quantity} of {} must be positive and at most the open {open}",
                        order.order_id().as_str()
                    )));
                }
                self.fill(&order, filled, quantity, ts);
                self.working[i].filled += quantity;
                if filled + quantity + QTY_EPS >= order.quantity() {
                    self.working.remove(i);
                }
            }
            how => {
                self.working.remove(i);
                self.release(&order, open, how, ts);
            }
        }
        Ok(())
    }
}

impl ExecutionEngine for ScriptedExecution {
    fn submit(&mut self, order: Order) -> Result<()> {
        if order.side() == OrderSide::NoOrderSide {
            return Err(AlgoError::Component(format!(
                "order {} has no side: it must be buy or sell",
                order.order_id().as_str()
            )));
        }
        let mut inner = self.lock();
        if inner
            .working
            .iter()
            .any(|w| w.order.order_id().as_str() == order.order_id().as_str())
        {
            return Err(AlgoError::Component(format!(
                "order id {} is already working",
                order.order_id().as_str()
            )));
        }
        let behavior = inner
            .script
            .get(order.order_id().as_str())
            .cloned()
            .unwrap_or(Behavior::Fill);
        let ts = order.ts_event();
        let actions = match behavior {
            Behavior::Fill => vec![VenueAction::Fill {
                quantity: order.quantity(),
            }],
            Behavior::Reject { reason } => vec![VenueAction::Reject { reason }],
            Behavior::Partial { filled, reason } => {
                if filled >= order.quantity() {
                    return Err(AlgoError::Component(format!(
                        "scripted partial fill {filled} must be below the order quantity {}",
                        order.quantity()
                    )));
                }
                vec![
                    VenueAction::Fill { quantity: filled },
                    VenueAction::Reject { reason },
                ]
            }
            Behavior::Hold => Vec::new(),
            Behavior::Expire => vec![VenueAction::Expire],
            Behavior::Script(actions) => actions,
        };
        let id = order.order_id().as_str().to_string();
        inner.working.push(Working { order, filled: 0.0 });
        for action in actions {
            let Some(i) = inner
                .working
                .iter()
                .position(|w| w.order.order_id().as_str() == id)
            else {
                return Err(AlgoError::Component(format!(
                    "scripted action {action:?} after order {id} finished"
                )));
            };
            inner.act(i, action, ts)?;
        }
        Ok(())
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> Result<()> {
        let mut inner = self.lock();
        if let Some(i) = inner
            .working
            .iter()
            .position(|w| w.order.order_id().as_str() == order_id)
        {
            inner.act(i, VenueAction::Cancel, now)?;
        }
        Ok(())
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.lock().events))
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        let mut inner = self.lock();
        let events = std::mem::take(&mut inner.events);
        inner.legacy.absorb(events);
        Ok(inner.legacy.take_fills())
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        let mut inner = self.lock();
        let events = std::mem::take(&mut inner.events);
        inner.legacy.absorb(events);
        Ok(inner.legacy.take_rejections())
    }
}
