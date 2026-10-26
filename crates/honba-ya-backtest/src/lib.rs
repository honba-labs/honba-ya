//! # 本場・検 honba-ya-backtest
//!
//! Research-live parity event-driven backtesting engine. Indian statutory
//! cost modeling, T+1 settlement timing, and position/PnL tracking are all
//! delegated to `honba-kokoro-ledger`/`honba-kokoro-portfolio` — the same
//! canonical implementations the live-trading actor path uses — so a
//! backtest and a live run agree byte-for-byte on cost and settlement math.
//! This crate owns the real NSE trading/holiday calendar
//! (`NseTradingCalendar`), zero-copy Arrow/Parquet catalog ingestion
//! (`CatalogLoader`), and the event-driven simulation loop itself.
//!
//! Event loop:
//! $$\text{MarketEvent} \longrightarrow \text{SignalEvent} \longrightarrow \text{OrderEvent} \longrightarrow \text{FillEvent}$$
//!
//! Uses the identical [`Strategy`] and [`RiskEngine`] traits as live execution.

pub mod calendar;
pub mod catalog;
pub mod engine;
pub mod events;
pub mod sim;

pub use calendar::{NseTradingCalendar, SessionWindow};
pub use catalog::{CatalogError, CatalogLoader, decimal_from_f64_lossless};
pub use engine::{BacktestConfig, BacktestEngine, BacktestResult, decimal_qty_to_u64};
pub use events::{FillEvent, MarketEvent, OrderEvent, SignalEvent};
pub use honba_core::TradingCalendar;
// Canonical Indian statutory cost & double-entry ledger types now live in
// honba-kokoro-ledger; re-exported here so existing call sites keep working.
pub use honba_kokoro_ledger::{
    Account as LedgerAccount, Exchange as TaxExchange, LedgerError, ProductType, TaxBreakdown,
    TaxCalculator, TradeSide, Transaction as LedgerTransaction, post_transaction,
};
pub use honba_portfolio::{
    ClearingCalendar, DematHoldings, FillSettlementResult, SettlementEngine, SettlementInstruction,
};
