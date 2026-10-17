#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! Simulation and paper execution engines for the Honba platform.

pub mod bar_fill;
pub mod cost;
pub mod fill_model;
pub mod l2;
pub mod latency;
pub mod lot;
pub mod margin;
pub mod next_open;
pub mod paper;
pub mod sandbox;
pub mod scripted;
pub mod target;

pub use bar_fill::{BarFillEngine, FillCosts, FillCostsError};
pub use cost::ScheduleFillCost;
pub use fill_model::{FillModel, NoSlippage, SlippageKind, SlippageModel};
pub use l2::{L2Book, L2Sim};
pub use latency::{depth_survival_discount, LatencyModel, NoLatency, SeededLatency};
pub use lot::{lot_aligned_size, split_order};
pub use margin::{
    default_session_close_ts, is_intraday, square_off_order, MarginQuery, MisPositions,
};
pub use next_open::{FillCostFn, NextOpenSim};
pub use paper::{OrderLedger, PaperExecution};
pub use sandbox::{LiveFeed, SandboxAdapter, SimulatedFeed};
pub use scripted::{Behavior, ScriptedExecution, VenueAction};
pub use target::{PerInstrumentLock, SmartOrderController, TargetPosition};

#[cfg(test)]
mod tests;
