//! Unit tests for this crate, one file per area.

mod context;
mod dyn_strategy;
mod intent;
mod ir;
mod ledger;
mod look_ahead;
mod manifest;
mod mtf;
mod runner_errors;
mod runner_events;
mod runner_rejections;
mod runner_risk;
mod runner_warmup;

use honba_messages::{Exchange, InstrumentId};

/// A placeholder instrument for tests where the instrument does not matter.
fn any_instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}
