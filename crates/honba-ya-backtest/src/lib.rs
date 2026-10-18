//! # 本場・検 honba-ya-backtest
//!
//! Research-live parity event-driven backtesting engine with accurate Indian cost modeling,
//! NSE trading calendar, and T+1 rolling settlement.
//!
//! Event loop:
//! $$\text{MarketEvent} \longrightarrow \text{SignalEvent} \longrightarrow \text{OrderEvent} \longrightarrow \text{FillEvent}$$
//!
//! Uses the identical [`Strategy`] and [`RiskEngine`] traits as live execution.

pub mod calendar;
pub mod costs;
pub mod engine;
pub mod events;
pub mod settlement;

pub use calendar::{NseTradingCalendar, SessionWindow};
pub use costs::{IndianCostConfig, IndianCostModel, IndianFeeBreakdown, ProductType};
pub use engine::{BacktestConfig, BacktestEngine, BacktestResult};
pub use events::{FillEvent, MarketEvent, OrderEvent, SignalEvent};
pub use settlement::SettlementLedger;
