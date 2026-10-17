#![allow(missing_docs)]
//! Response DTOs. These are *records* (ADR 0012): unknown fields are ignored
//! on parse so an older client reads a newer server's response.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use honba_entities::{PortfolioAccount, PortfolioHolding, PortfolioPosition, Position, Trade};
use std::collections::BTreeMap;

use honba_messages::{Bar, ErrorDetail, InstrumentId, Message, Order, QuoteTick};
use honba_strategy::StrategyIr;

use crate::capabilities::Capabilities;

/// Capabilities response.
pub type CapabilitiesResponse = Capabilities;

/// Instruments response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct InstrumentsResponse {
    /// Matching instruments, as of the requested snapshot.
    pub instruments: Vec<serde_json::Value>,
}

/// Quotes response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QuotesResponse {
    /// Latest top-of-book per requested symbol.
    pub quotes: Vec<QuoteTick>,
}

/// Bars response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BarsResponse {
    /// Bars in ascending `ts_event` order.
    pub bars: Vec<Bar>,
}

/// One instrument that passed a screener scan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ScreenerResultRow {
    /// The instrument that matched.
    pub instrument_id: InstrumentId,
    /// Latest value of every metric the filter reads, by key as written in the filter;
    /// `null` while a metric is still warming up.
    pub metrics: BTreeMap<String, Option<f64>>,
}

/// Screener scan response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ScreenerResponse {
    /// Matching instruments in instrument-id order.
    pub rows: Vec<ScreenerResultRow>,
}

/// One side of the order book.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DepthLevel {
    /// Price.
    pub price: f64,
    /// Quantity resting at this price.
    pub qty: f64,
}

/// Depth (order book) response.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DepthResponse {
    /// Bid levels, best (highest) first.
    pub bids: Vec<DepthLevel>,
    /// Ask levels, best (lowest) first.
    pub asks: Vec<DepthLevel>,
}

/// A strategy that verified, with the id the server keeps it under.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CompiledStrategy {
    /// Content id: `sha256:` plus the hex digest of the canonical manifest JSON.
    /// The same manifest always has the same id; a backtest names it as `strategy`.
    pub id: String,
    /// The verified IR the manifest compiled to.
    pub ir: StrategyIr,
}

/// Strategies response: the compiled strategies of this process, ordered by id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StrategiesResponse {
    /// Compiled strategies.
    pub strategies: Vec<CompiledStrategy>,
}

/// Lifecycle of an asynchronous job.
///
/// `pending -> running -> completed | failed | cancelled`, plus `pending -> cancelled`.
/// The three terminal states are final (ADR 0017 decision 1; the transition table is
/// `RunStatus::next`). `Cancelled` was added within `/api/v1` (ADR 0012 rule 2); exhaustive
/// decoders must be regenerated (rule 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    /// Accepted, not started.
    Pending,
    /// In progress.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with a failure; see `error` on the response.
    Failed,
    /// Stopped before completing (ADR 0017: reachable only through graceful shutdown in v1).
    Cancelled,
}

/// Lifecycle of a backtest run or sweep job.
///
/// Named `RunStatus` and re-exported under the two names the plan's endpoint
/// list uses, so both spellings refer to one enum.
pub type BacktestStatus = RunStatus;

/// Lifecycle of a sweep job.
pub type SweepStatus = RunStatus;

/// Headline performance numbers for a completed backtest.
///
/// A fixed, documented set rather than a free-form object, so a dashboard can
/// bind to fields that are guaranteed present.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BacktestMetrics {
    /// Number of completed round trips.
    pub trades: u64,
    /// Net profit across the run.
    pub net_pnl: f64,
    /// Annualized Sharpe ratio.
    pub sharpe: f64,
    /// Maximum peak-to-trough drawdown, as a positive fraction.
    pub max_drawdown: f64,
    /// Total return as a fraction of initial capital.
    pub total_return: f64,
}

/// Backtest response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BacktestResponse {
    /// Server-assigned run id.
    pub run_id: String,
    /// Current lifecycle state.
    pub status: RunStatus,
    /// Present once `status` is `completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<BacktestMetrics>,
    /// What the run did *not* model, stated explicitly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assumptions: Option<serde_json::Value>,
    /// Statistical overfitting validation block (E4-S6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<honba_analytics::ValidationBlock>,
    /// Why the run failed; present only when `status` is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

/// Sweep response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SweepResponse {
    /// Server-assigned job id.
    pub job_id: String,
    /// Current lifecycle state.
    pub status: RunStatus,
    /// Present once `status` is `completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<SweepReportResponse>,
    /// Why the job failed; present only when `status` is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

/// Sweep results: best trials plus the ranking, in a deterministic order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SweepReportResponse {
    /// Trials ranked best-first by the fitness function.
    pub ranked: Vec<serde_json::Value>,
    /// Best trial's parameter set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best: Option<serde_json::Value>,
    /// Statistical overfitting validation block across sweep trials (E4-S6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<honba_analytics::ValidationBlock>,
}

/// Orders response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OrdersResponse {
    /// Orders matching the query.
    pub orders: Vec<Order>,
}

/// Positions response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PositionsResponse {
    /// Open positions.
    pub positions: Vec<Position>,
}

/// Trades response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TradesResponse {
    /// Realized trades.
    pub trades: Vec<Trade>,
}

/// `GET /portfolio/accounts` response: the valued accounts of the caller's portfolio
/// (ADR 0032 §6). The full snapshot is `GET /portfolio`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioAccountsResponse {
    /// The valued accounts.
    pub accounts: Vec<PortfolioAccount>,
}

/// `GET /portfolio/positions` response: the valued open positions across the caller's accounts
/// (ADR 0032 §6). Delivery holdings are `GET /portfolio/holdings`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioPositionsResponse {
    /// The valued open positions, flattened across accounts.
    pub positions: Vec<PortfolioPosition>,
}

/// `GET /portfolio/holdings` response: the valued delivery holdings across the caller's accounts
/// (ADR 0032 §6).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioHoldingsResponse {
    /// The valued delivery holdings, flattened across accounts.
    pub holdings: Vec<PortfolioHolding>,
}

/// One frame of the run event stream (ADR 0021, `GET /backtests/{id}/events`).
///
/// A `Record` frame carries one journal `Message` with its 0-based line index; a `Status` frame
/// carries the manifest status with `seq` = the number of journal records delivered so far.
/// Every frame's `seq` is a valid resume token: resuming at `N` replays records with line
/// index `>= N`, so the stream neither skips nor repeats a record across a reconnect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamFrame {
    /// The run's lifecycle state; emitted on connect, on every status change, and once more
    /// (terminal) when the stream ends. `error` is present only for a failed run.
    Status {
        /// The manifest status.
        status: RunStatus,
        /// Next journal line index to read (records delivered so far).
        seq: u64,
        /// Why the run failed; present only when `status` is `failed`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<ErrorDetail>,
    },
    /// One journal record.
    Record {
        /// The record's 0-based line index in `events.ndjson` industries.
        seq: u64,
        /// The journal message.
        message: Message,
    },
}

/// `POST /bars` response: how many bars were handed to the bar pump.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BarsIngestResponse {
    /// Bars queued for the pump; the whole batch, or the request failed.
    pub accepted: u64,
}

/// `GET /trading-state` response: the operator trading state the write routes evaluate against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TradingStateResponse {
    /// The current trading state.
    pub state: honba_messages::TradingState,
}

/// `PUT /trading-state` response: the state now in force and the one it replaced.
///
/// `previous == state` means the request changed nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TradingStateChange {
    /// The trading state now in force.
    pub state: honba_messages::TradingState,
    /// The trading state before this request.
    pub previous: honba_messages::TradingState,
}
