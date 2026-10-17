//! Unit tests for this crate, one file per area.

mod audit;
mod cache;
mod clock;
mod engine;
mod execution;
mod handler_audit;
mod order_store;
mod queue;
mod reconciliation;
mod risk_gate;

use honba_messages::{Exchange, InstrumentId};

/// A placeholder instrument for tests where the instrument does not matter.
fn any_instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}
