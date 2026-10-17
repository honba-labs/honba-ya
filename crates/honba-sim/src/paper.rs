//! Paper execution engine.

use std::collections::HashMap;

use honba_engine::{ExecutionEngine, LegacyDrains, OrderRejection, Result};
use honba_entities::{Currency, ExecutionEvent, Trade};
use honba_messages::{Order, OrderId, OrderSide, OrderStatus, OrderType, UnixNanos};

/// A minimal paper-trading execution engine.
///
/// Fills every order immediately at the price the caller specifies. No
/// slippage, no partial fills, no rejection: each order produces one complete
/// [`ExecutionEvent::Fill`], or an [`ExecutionEvent::Accepted`] before it when
/// configured with `with_ack(true)` (the `ack` profile). Enough to drive a
/// strategy through the kernel and collect trades; real fill simulation
/// belongs in a dedicated adapter.
///
/// ```
/// use honba_engine::ExecutionEngine;
/// use honba_sim::PaperExecution;
/// use honba_messages::{
///     InstrumentId, Order, OrderId, OrderSide, OrderType, TimeInForce,
///     UnixNanos, Exchange,
/// };
///
/// let mut engine = PaperExecution::new(100.0);
/// let order = Order::new(
///     OrderId::new("O-1"),
///     InstrumentId::new("X", Exchange::new("TEST")),
///     OrderSide::Buy,
///     OrderType::Market,
///     10.0, None, TimeInForce::Day,
///     UnixNanos::from_u64(1), UnixNanos::from_u64(1),
/// );
/// engine.submit(order).unwrap();
/// let fills = engine.drain_fills().unwrap();
/// assert_eq!(fills.len(), 1);
/// assert_eq!(fills[0].notional(), 1000.0);
/// ```
pub struct PaperExecution {
    price: f64,
    pending: Vec<Order>,
    events: Vec<ExecutionEvent>,
    legacy: LegacyDrains,
    next_ts: u64,
    /// The currency fills settle in. See [`BarFillEngine`](crate::BarFillEngine)
    /// for why the engine — not the `Order` — declares it.
    currency: Currency,
    ack: bool,
}

impl PaperExecution {
    /// Creates a paper engine that fills at the given fixed price.
    pub fn new(price: f64) -> Self {
        Self {
            price,
            pending: Vec::new(),
            events: Vec::new(),
            legacy: LegacyDrains::new(),
            next_ts: 1,
            currency: Currency::Inr,
            ack: false,
        }
    }

    /// Enables or disables the `ack` profile (ADR 0019 decision 8): when enabled,
    /// limit orders emit an `Accepted` event before filling.
    pub fn with_ack(mut self, ack: bool) -> Self {
        self.ack = ack;
        self
    }

    /// Whether the `ack` profile is enabled.
    pub fn ack(&self) -> bool {
        self.ack
    }

    /// Sets the settlement currency fills carry.
    pub fn with_currency(mut self, currency: Currency) -> Self {
        self.currency = currency;
        self
    }

    /// Changes the fill price for subsequent submissions.
    pub fn set_price(&mut self, price: f64) {
        self.price = price;
    }

    /// Returns the current fill price.
    pub fn price(&self) -> f64 {
        self.price
    }
}

impl ExecutionEngine for PaperExecution {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.pending.push(order);
        // Fill immediately.
        let order = self.pending.pop().expect("just pushed");
        let qty = order.quantity();
        let side = order.side();
        let instrument = order.instrument_id().clone();
        let order_id: OrderId = order.order_id().clone();
        let t = UnixNanos::from_u64(self.next_ts);
        self.next_ts += 1;

        let side = if side == OrderSide::Sell {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };
        if self.ack && order.order_type() == OrderType::Limit {
            self.events.push(ExecutionEvent::Accepted {
                order_id: order_id.clone(),
                instrument_id: instrument.clone(),
                side,
                quantity: qty,
                venue_order_id: None,
                ts: t,
            });
        }
        self.events.push(ExecutionEvent::Fill {
            trade: Trade::new(
                order_id,
                instrument,
                side,
                qty,
                self.price,
                self.currency,
                t,
                t,
            ),
            cum_qty: qty,
            complete: true,
            venue_order_id: None,
        });
        Ok(())
    }

    fn cancel(&mut self, _order_id: &str, _now: UnixNanos) -> Result<()> {
        // Nothing pending long enough to cancel.
        Ok(())
    }

    fn drain_events(&mut self) -> Result<Vec<ExecutionEvent>> {
        Ok(std::mem::take(&mut self.events))
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        let events = std::mem::take(&mut self.events);
        self.legacy.absorb(events);
        Ok(self.legacy.take_fills())
    }

    fn drain_rejections(&mut self) -> Result<Vec<OrderRejection>> {
        let events = std::mem::take(&mut self.events);
        self.legacy.absorb(events);
        Ok(self.legacy.take_rejections())
    }
}

/// Bookkeeping for a set of simulated orders.
///
/// Useful when a test wants to assert on the state of submitted orders
/// without implementing `ExecutionEngine`.
#[derive(Default)]
pub struct OrderLedger {
    orders: HashMap<String, OrderStatus>,
}

impl OrderLedger {
    /// Creates an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an order status.
    pub fn set(&mut self, order_id: &str, status: OrderStatus) {
        self.orders.insert(order_id.to_string(), status);
    }

    /// Returns the recorded status for an order.
    pub fn status(&self, order_id: &str) -> Option<OrderStatus> {
        self.orders.get(order_id).copied()
    }
}
