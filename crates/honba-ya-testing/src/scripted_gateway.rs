//! A scriptable [`ExecutionGateway`] double with a shared script.
//!
//! Unlike [`StubGateway`](crate::StubGateway), which fills once and is consumed by its owner,
//! `ScriptedGateway` is `Clone` and every clone shares one script. A test hands one clone to the
//! code under test (often moved onto another task) and keeps another to push fills, inject
//! failures and read back the calls.
//!
//! It models a broker: `submit_order` returns a *venue* id (`V-1`, `V-2`, ...) that differs from
//! the client order id, and scripted fills carry the venue id, so a bridge has to translate.
//! Failures set with the `fail_next_*` / `script_fill_error` methods are one-shot. No method
//! awaits while holding the script lock.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use honba_entities::{Currency, Trade};
use honba_messages::{Order, OrderId, UnixNanos};
use honba_ports::{ExecutionGateway, PortError, PortResult};

#[derive(Default)]
struct Script {
    next_id: u64,
    orders: Vec<(OrderId, Order)>,
    cancelled: Vec<OrderId>,
    fail_submit: VecDeque<PortError>,
    fail_cancel: VecDeque<PortError>,
    fills: VecDeque<PortResult<Trade>>,
}

/// A clonable, scriptable [`ExecutionGateway`]; clones share one script.
#[derive(Clone, Default)]
pub struct ScriptedGateway {
    script: Arc<Mutex<Script>>,
}

impl ScriptedGateway {
    /// Creates a gateway with an empty script.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Script> {
        self.script.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes the next `submit_order` fail with `error`.
    pub fn fail_next_submit(&self, error: PortError) {
        self.lock().fail_submit.push_back(error);
    }

    /// Makes the next `cancel_order` fail with `error`.
    pub fn fail_next_cancel(&self, error: PortError) {
        self.lock().fail_cancel.push_back(error);
    }

    /// Queues a fill of `qty` at `price` for the order accepted under `venue_id`.
    ///
    /// The trade carries the venue id, as a broker's execution report would. An id the gateway
    /// never issued yields a trade for a placeholder instrument, which exercises a bridge's
    /// unknown-order path.
    pub fn script_fill(&self, venue_id: &OrderId, qty: f64, price: f64) {
        let mut script = self.lock();
        let known = script
            .orders
            .iter()
            .find(|(id, _)| id == venue_id)
            .map(|(_, o)| o.clone());
        let (instrument, side, ts) = match known {
            Some(o) => (o.instrument_id().clone(), o.side(), o.ts_event()),
            None => (
                crate::fixtures::any_instrument(),
                honba_messages::OrderSide::Buy,
                UnixNanos::from_u64(1),
            ),
        };
        let trade = Trade::new(
            venue_id.clone(),
            instrument,
            side,
            qty,
            price,
            Currency::Inr,
            ts,
            ts,
        );
        script.fills.push_back(Ok(trade));
    }

    /// Queues a stream error: the next `next_fill` returns `error`.
    pub fn script_fill_error(&self, error: PortError) {
        self.lock().fills.push_back(Err(error));
    }

    /// Every order accepted so far, as submitted (client ids), in order.
    pub fn submitted(&self) -> Vec<Order> {
        self.lock().orders.iter().map(|(_, o)| o.clone()).collect()
    }

    /// The venue id issued to each accepted order, in order.
    pub fn venue_ids(&self) -> Vec<OrderId> {
        self.lock()
            .orders
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Every id passed to `cancel_order`, in call order (failed calls included).
    pub fn cancellations(&self) -> Vec<OrderId> {
        self.lock().cancelled.clone()
    }
}

#[async_trait]
impl ExecutionGateway for ScriptedGateway {
    async fn submit_order(&mut self, order: Order) -> PortResult<OrderId> {
        let mut script = self.lock();
        if let Some(e) = script.fail_submit.pop_front() {
            return Err(e);
        }
        script.next_id += 1;
        let id = OrderId::new(format!("V-{}", script.next_id));
        script.orders.push((id.clone(), order));
        Ok(id)
    }

    async fn cancel_order(&mut self, id: OrderId) -> PortResult<()> {
        let mut script = self.lock();
        script.cancelled.push(id);
        match script.fail_cancel.pop_front() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn modify_order(
        &mut self,
        _id: OrderId,
        _new_qty: f64,
        _new_price: Option<f64>,
    ) -> PortResult<()> {
        Err(PortError::Unsupported("scripted gateway: modify".into()))
    }

    async fn next_fill(&mut self) -> PortResult<Option<Trade>> {
        self.lock().fills.pop_front().transpose()
    }
}
