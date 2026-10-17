//! The execution gateway behind the order routes (ADR 0016, E3-S7).
//!
//! A risk-approved order is recorded in the [`OrderLedger`] as `Submitted` and handed to an
//! [`ExecutionVenue`]. The default venue is the embedded [`NextOpenSim`], which fills market
//! orders at the open of the instrument's first bar in a later session, so a fill needs the
//! market to advance: the composition root (or a test) pushes bars with
//! [`AppState::advance_market`]. Whatever the venue, each [`ExecutionEvent`] it drains updates
//! the matching ledger row (`Filled`, `PartiallyFilled`, `Rejected`, `Cancelled`) through one
//! mapping, and terminal rows never change. Event time is the venue's own; nothing here reads
//! the wall clock, and the simulator is seeded, so a replay books the same fills.
//!
//! Why a crate-local trait and not `honba_ports::ExecutionGateway`: the ports trait is async,
//! takes `&mut self` and only reports fills (`next_fill`), so it cannot carry the rejections and
//! cancels the ledger needs, and the sync handlers here hold a std mutex across the call. The
//! sync `honba_engine::ExecutionEngine` already speaks `ExecutionEvent` and `NextOpenSim`
//! implements it, so [`ExecutionVenue`] only extends it with the market hook. A venue backed
//! by the async port is bridged by [`GatewayVenue`](crate::GatewayVenue): a task owns the gateway,
//! the sync calls only enqueue commands, and its answers come back as events (ADR 0030).
//!
//! Scope: the default simulator handles market orders only. A limit order is refused by it
//! (`unsupported_order_type`) and its ledger row becomes `Rejected`; the row's
//! `reject_reason` is `order_rejected` with the simulator's text as the message. The simulator is long-only, so a sell with no
//! simulated position is `Rejected` (`no_position`). No real broker gateway is wired into `honba serve` (E3-S7).

use std::sync::{Arc, Mutex};

use honba_engine::{ExecutionEngine, Result as EngineResult};
use honba_entities::{Currency, ExecutionEvent, Money};
use honba_messages::{Bar, ErrorCode, ErrorDetail, Order, OrderStatus, RejectReason};
use honba_sim::NextOpenSim;

use crate::OrderLedger;

/// Starting cash of the default simulator, in minor units (10 billion INR in paise).
const DEFAULT_CASH_MINOR: i64 = 1_000_000_000_000;

/// A venue the order routes can submit to and cancel on: an [`ExecutionEngine`] that may also
/// react to market bars.
///
/// `on_bar` defaults to a no-op, for a venue driven by its own feed; such a venue's events are
/// drained on every [`AppState::advance_market`](crate::AppState::advance_market) call (with or without bars).
pub trait ExecutionVenue: ExecutionEngine + 'static {
    /// Feeds one market bar (ascending event time). An out-of-order bar is an error.
    fn on_bar(&mut self, _bar: &Bar) -> EngineResult<()> {
        Ok(())
    }
}

impl ExecutionVenue for NextOpenSim {
    fn on_bar(&mut self, bar: &Bar) -> EngineResult<()> {
        NextOpenSim::on_bar(self, bar)
    }
}

/// Shared handle on the execution venue.
#[derive(Clone)]
pub(crate) struct ExecGateway {
    sim: Arc<Mutex<Box<dyn ExecutionVenue>>>,
}

impl ExecGateway {
    pub(crate) fn new(venue: impl ExecutionVenue) -> Self {
        Self {
            sim: Arc::new(Mutex::new(Box::new(venue))),
        }
    }

    pub(crate) fn default_sim() -> Self {
        let sim = NextOpenSim::new(Money::new(DEFAULT_CASH_MINOR, Currency::Inr))
            .expect("non-negative cash");
        Self::new(sim)
    }

    /// Hands `order` to the simulator and applies any immediate event (a rejection).
    pub(crate) fn submit(&self, ledger: &OrderLedger, order: Order) {
        let mut sim = self
            .sim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(e) = sim.submit(order.clone()) {
            tracing::warn!(order = %order.order_id(), error = %e, "venue refused submit");
            ledger.reject(
                order.order_id().as_str(),
                RejectReason::new(ErrorCode::OrderRejected, e.to_string()),
            );
            return;
        }
        apply(sim.as_mut(), ledger);
    }

    /// Feeds `bars` (ascending event time) to the simulator and applies its events.
    pub(crate) fn advance(&self, ledger: &OrderLedger, bars: &[Bar]) -> Result<(), ErrorDetail> {
        let mut sim = self
            .sim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut result = Ok(());
        for bar in bars {
            if let Err(e) = sim.on_bar(bar) {
                result = Err(ErrorDetail::new(ErrorCode::InternalError, e.to_string()));
                break;
            }
        }
        apply(sim.as_mut(), ledger);
        result
    }

    /// Withdraws a working order from the venue (the ledger row is cancelled by the caller; any
    /// event the venue reports meanwhile goes through the ledger's terminal-row rule).
    pub(crate) fn cancel(
        &self,
        ledger: &OrderLedger,
        order_id: &str,
        ts: honba_messages::UnixNanos,
    ) {
        let mut sim = self
            .sim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = sim.cancel(order_id, ts);
        apply(sim.as_mut(), ledger);
    }
}

/// The ledger's [`RejectReason`] for a venue's free-form rejection `reason`.
///
/// A reason that is an `ErrorCode` wire string (the pre-gate refusals) keeps that code with a
/// stock description. `<code>: <text>` (the [`GatewayVenue`](crate::GatewayVenue), which carries
/// the broker's own words) keeps the code and puts the text in the message. Anything else (a
/// simulator text such as `no_position`) is an `order_rejected` carrying the text as the message.
fn reject_reason(reason: &str) -> RejectReason {
    let code_of =
        |s: &str| serde_json::from_value::<ErrorCode>(serde_json::Value::String(s.to_owned())).ok();
    if let Some(code) = code_of(reason) {
        return RejectReason::new(code, format!("venue refused the order ({code})"));
    }
    if let Some((head, text)) = reason.split_once(": ") {
        if let Some(code) = code_of(head) {
            return RejectReason::new(code, text);
        }
    }
    RejectReason::new(ErrorCode::OrderRejected, reason)
}

/// Drains the venue's events into ledger status changes.
fn apply(sim: &mut dyn ExecutionVenue, ledger: &OrderLedger) {
    let events = sim.drain_events().unwrap_or_default();
    for event in events {
        match event {
            ExecutionEvent::Fill {
                trade,
                cum_qty,
                complete,
                ..
            } => {
                let status = if complete {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
                ledger.apply_fill(
                    trade.order_id().as_str(),
                    trade.quantity(),
                    trade.price(),
                    cum_qty,
                    trade.ts_event(),
                    status,
                );
            }
            ExecutionEvent::Rejected {
                order_id, reason, ..
            } => {
                tracing::info!(order = %order_id, %reason, "venue rejected order");
                // The remainder of a partial fill is also reported as a rejection; the
                // filled part stays exposure, so that row keeps `PartiallyFilled`.
                ledger.reject(order_id.as_str(), reject_reason(&reason));
            }
            ExecutionEvent::Cancelled { order_id, .. } => {
                ledger.set_status(order_id.as_str(), OrderStatus::Cancelled);
            }
            _ => {}
        }
    }
}
