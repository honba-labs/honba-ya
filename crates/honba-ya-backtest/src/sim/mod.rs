//! Execution simulation primitives: L2 order book depth, latency modeling,
//! MIS 15:20 square-off enforcement, and slippage modeling.

pub mod l2_book;
pub mod latency;
pub mod mis;
pub mod slippage;
