//! A scriptable [`ExecutionGateway`] double with a shared script.
//!
//! Unlike [`StubGateway`](crate::StubGateway), which fills once and is consumed by its owner,
//! `ScriptedGateway` is `Clone` and every clone shares one script. A test hands one clone to the
//! code under test (often moved onto another task) and keeps another to push fills, inject
//! failures and read back the calls.
//!
//! It models a broker: `submit_order` returns a *venue* order id (`V-1`, `V-2`, ...) that differs
//! from the client order id, and scripted fills carry the venue id, so a bridge has to translate.
//! Failures set with the `fail_next_*` / `script_fill_error` methods are one-shot. No method
//! awaits while holding the script lock, and no method reads the wall clock: acknowledgements
//! carry a deterministic epoch timestamp.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use honba_core::uuid::Uuid;
use honba_core::{FillReport, OrderAck, OrderRequest, OrderSide, OrderStatus, Price, Quantity};

use crate::fixtures;
use crate::ports::{ExecutionGateway, PortError, PortResult};

#[derive(Default)]
struct Script {
    next_id: u64,
    fill_seq: u64,
    orders: Vec<(String, OrderRequest)>,
    cancelled: Vec<String>,
    fail_submit: VecDeque<PortError>,
    fail_cancel: VecDeque<PortError>,
    fills: VecDeque<PortResult<FillReport>>,
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
    /// The fill report carries the venue id, as a broker's execution report would. An id the
    /// gateway never issued yields a fill for a placeholder instrument, which exercises a
    /// bridge's unknown-order path.
    pub fn script_fill(&self, venue_id: &str, qty: Quantity, price: Price) {
        let mut script = self.lock();
        let known = script
            .orders
            .iter()
            .find(|(id, _)| id == venue_id)
            .map(|(_, o)| o.clone());
        let (instrument, side, client_order_id, algo_id) = match known {
            Some(o) => (o.instrument, o.side, o.client_order_id, o.algo_id),
            None => (
                fixtures::any_instrument(),
                OrderSide::Buy,
                Uuid::nil(),
                "unknown".to_string(),
            ),
        };
        script.fill_seq += 1;
        let fill = FillReport::new(
            format!("{venue_id}-F{}", script.fill_seq),
            venue_id,
            client_order_id,
            instrument,
            side,
            price,
            qty,
            algo_id,
            fixtures::utc_nanos(0),
        );
        script.fills.push_back(Ok(fill));
    }

    /// Queues a stream error: the next `next_fill` returns `error`.
    pub fn script_fill_error(&self, error: PortError) {
        self.lock().fills.push_back(Err(error));
    }

    /// Every order accepted so far, as submitted, in order.
    pub fn submitted(&self) -> Vec<OrderRequest> {
        self.lock().orders.iter().map(|(_, o)| o.clone()).collect()
    }

    /// The venue id issued to each accepted order, in order.
    pub fn venue_ids(&self) -> Vec<String> {
        self.lock()
            .orders
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Every id passed to `cancel_order`, in call order (failed calls included).
    pub fn cancellations(&self) -> Vec<String> {
        self.lock().cancelled.clone()
    }
}

#[async_trait]
impl ExecutionGateway for ScriptedGateway {
    async fn submit_order(&mut self, order: OrderRequest) -> PortResult<OrderAck> {
        let mut script = self.lock();
        if let Some(e) = script.fail_submit.pop_front() {
            return Err(e);
        }
        script.next_id += 1;
        let venue_id = format!("V-{}", script.next_id);
        let ack = OrderAck::new(
            venue_id.as_str(),
            order.client_order_id,
            OrderStatus::New,
            fixtures::utc_nanos(0),
        );
        script.orders.push((venue_id, order));
        Ok(ack)
    }

    async fn cancel_order(&mut self, order_id: &str) -> PortResult<()> {
        let mut script = self.lock();
        script.cancelled.push(order_id.to_string());
        match script.fail_cancel.pop_front() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn modify_order(
        &mut self,
        _order_id: &str,
        _new_qty: Quantity,
        _new_price: Option<Price>,
    ) -> PortResult<()> {
        Err(PortError::Unsupported("scripted gateway: modify".into()))
    }

    async fn next_fill(&mut self) -> PortResult<Option<FillReport>> {
        self.lock().fills.pop_front().transpose()
    }
}
