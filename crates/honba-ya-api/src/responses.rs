#![allow(missing_docs)]
//! Response DTOs. These are *records* (ADR 0012): unknown fields are ignored
//! on parse so an older client reads a newer server's response.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use honba_entities::{
    Currency, PortfolioAccount, PortfolioHolding, PortfolioPosition, Position, Trade,
};
use std::collections::BTreeMap;

use honba_messages::{
    Bar, ErrorDetail, InstrumentId, Message, Order, OrderSide, QuoteTick, UnixNanos,
};
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

/// Ledger entry for transaction journal (trades, fees, transfers, adjustments).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LedgerEntry {
    /// Unique entry id.
    pub id: String,
    /// Event timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Entry category.
    pub category: String,
    /// Description.
    pub description: String,
    /// Settlement currency.
    pub currency: Currency,
    /// Amount (positive = credit, negative = debit).
    pub amount: f64,
    /// Amount in USD (optional).
    pub amount_usd: Option<f64>,
    /// Running balance after this entry.
    pub balance: f64,
    /// Balance in USD (optional).
    pub balance_usd: Option<f64>,
    /// Wallet/account name.
    pub wallet: Option<String>,
    /// Instrument (optional).
    pub instrument: Option<String>,
    /// Side for trades (optional).
    pub side: Option<OrderSide>,
    /// Fees for this entry (optional).
    pub fees: Option<f64>,
}

/// Ledger history response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LedgerHistoryResponse {
    /// Ledger entries.
    pub entries: Vec<LedgerEntry>,
    /// Total count.
    pub total_count: u64,
}

/// Tax transaction for STCG/LTCG reporting.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaxTransaction {
    /// Unique transaction id.
    pub id: String,
    /// Event timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable sell date.
    pub date: String,
    /// Instrument name.
    pub instrument: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Side (buy/sell).
    pub side: OrderSide,
    /// Quantity.
    pub quantity: f64,
    /// Buy price.
    pub buy_price: f64,
    /// Sell price.
    pub sell_price: f64,
    /// Buy date.
    pub buy_date: String,
    /// Sell date.
    pub sell_date: String,
    /// Holding period in days.
    pub holding_period_days: u64,
    /// Gross gain/loss.
    pub gain_loss: f64,
    /// Gross gain/loss in USD (optional).
    pub gain_loss_usd: Option<f64>,
    /// Short-term capital gain.
    pub stcg: f64,
    /// Long-term capital gain.
    pub ltcg: f64,
    /// Securities Transaction Tax.
    pub stt: f64,
    /// Stamp duty.
    pub stamp_duty: f64,
    /// Exchange charges.
    pub exchange_charges: f64,
    /// SEBI fees.
    pub sebi_fees: f64,
    /// GST.
    pub gst: f64,
    /// Total charges.
    pub total_charges: f64,
    /// Net gain/loss after charges.
    pub net_gain_loss: f64,
    /// Tax liability.
    pub tax_liability: f64,
    /// Category (STCG/LTCG/Exempt).
    pub category: String,
}

/// Tax report response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaxReportResponse {
    /// Tax transactions.
    pub transactions: Vec<TaxTransaction>,
    /// Total count.
    pub total_count: u64,
    /// Summary totals.
    pub summary: TaxReportSummary,
}

/// Tax report summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TaxReportSummary {
    /// Total STCG.
    pub total_stcg: f64,
    /// Total LTCG.
    pub total_ltcg: f64,
    /// Total STT.
    pub total_stt: f64,
    /// Total stamp duty.
    pub total_stamp_duty: f64,
    /// Total exchange charges.
    pub total_exchange_charges: f64,
    /// Total SEBI fees.
    pub total_sebi_fees: f64,
    /// Total GST.
    pub total_gst: f64,
    /// Total net gain/loss.
    pub total_net_gain_loss: f64,
    /// Total tax liability.
    pub total_tax_liability: f64,
}

/// Portfolio snapshot entry (historical).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotEntry {
    /// Snapshot id.
    pub id: String,
    /// Timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Total equity.
    pub total_equity: f64,
    /// Total cash.
    pub total_cash: f64,
    /// Positions value.
    pub positions_value: f64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Day change.
    pub day_change: f64,
    /// Day change percentage.
    pub day_change_pct: f64,
    /// Number of accounts.
    pub accounts: u32,
    /// Number of positions.
    pub positions: u32,
}

/// Portfolio snapshots response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioSnapshotsResponse {
    /// Snapshot entries.
    pub snapshots: Vec<SnapshotEntry>,
    /// Total count.
    pub total_count: u64,
}

/// Position audit trail entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AuditEntry {
    /// Entry id.
    pub id: String,
    /// Timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Instrument.
    pub instrument: String,
    /// Side.
    pub side: OrderSide,
    /// Quantity change.
    pub quantity_change: f64,
    /// Price.
    pub price: f64,
    /// Position after change.
    pub position_after: f64,
    /// Average price after.
    pub avg_price_after: f64,
    /// Realized PnL from this change.
    pub realized_pnl: f64,
    /// Order id that caused the change.
    pub order_id: String,
    /// Change reason.
    pub reason: String,
}

/// Position audit trail response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PositionAuditTrailResponse {
    /// Audit entries.
    pub entries: Vec<AuditEntry>,
    /// Total count.
    pub total_count: u64,
}

/// Concentration risk entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConcentrationEntry {
    /// Instrument.
    pub instrument: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Market value.
    pub market_value: f64,
    /// Percentage of portfolio.
    pub pct_of_portfolio: f64,
    /// Quantity.
    pub quantity: f64,
    /// Average price.
    pub avg_price: f64,
    /// Current mark.
    pub mark: f64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Sector (optional).
    pub sector: Option<String>,
}

/// Concentration risk response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConcentrationRiskResponse {
    /// Concentration entries.
    pub entries: Vec<ConcentrationEntry>,
    /// Total portfolio value.
    pub total_portfolio_value: f64,
    /// Herfindahl-Hirschman Index.
    pub hhi: f64,
    /// Top 5 concentration.
    pub top5_concentration: f64,
    /// Top 10 concentration.
    pub top10_concentration: f64,
}

/// Weighted average cost entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WACEntry {
    /// Instrument.
    pub instrument: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange.
    pub exchange: String,
    /// Quantity.
    pub quantity: f64,
    /// Weighted average cost.
    pub weighted_avg_cost: f64,
    /// Current mark.
    pub mark: f64,
    /// Market value.
    pub market_value: f64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Total cost basis.
    pub total_cost_basis: f64,
    /// Total proceeds.
    pub total_proceeds: f64,
}

/// Weighted average cost response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WeightedAverageCostResponse {
    /// WAC entries.
    pub entries: Vec<WACEntry>,
}

/// Fees report entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FeeEntry {
    /// Entry id.
    pub id: String,
    /// Timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Fee type.
    pub fee_type: String,
    /// Instrument (optional).
    pub instrument: Option<String>,
    /// Amount.
    pub amount: f64,
    /// Currency.
    pub currency: Currency,
    /// Related order id (optional).
    pub order_id: Option<String>,
}

/// Fees report response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FeesReportResponse {
    /// Fee entries.
    pub entries: Vec<FeeEntry>,
    /// Total count.
    pub total_count: u64,
    /// Summary by fee type.
    pub by_type: Vec<FeeTypeSummary>,
}

/// Fee type summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FeeTypeSummary {
    /// Fee type.
    pub fee_type: String,
    /// Total amount.
    pub total_amount: f64,
    /// Count.
    pub count: u64,
}

/// Portfolio value chart point (equity curve).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EquityPoint {
    /// Timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Equity value.
    pub equity: f64,
    /// Cash.
    pub cash: f64,
    /// Positions value.
    pub positions_value: f64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Drawdown from peak.
    pub drawdown: f64,
    /// Drawdown percentage.
    pub drawdown_pct: f64,
}

/// Portfolio value chart response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioValueChartResponse {
    /// Equity curve points.
    pub points: Vec<EquityPoint>,
    /// Summary metrics.
    pub summary: EquityCurveSummary,
}

/// Equity curve summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EquityCurveSummary {
    /// Starting equity.
    pub start_equity: f64,
    /// Ending equity.
    pub end_equity: f64,
    /// Total return.
    pub total_return: f64,
    /// Total return percentage.
    pub total_return_pct: f64,
    /// Max drawdown.
    pub max_drawdown: f64,
    /// Max drawdown percentage.
    pub max_drawdown_pct: f64,
    /// Sharpe ratio (if available).
    pub sharpe: Option<f64>,
}

/// Fill entry (fill-level detail for an order).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FillEntry {
    /// Fill id.
    pub id: String,
    /// Order id.
    pub order_id: String,
    /// Timestamp (Unix nanos).
    pub ts_event: UnixNanos,
    /// Human-readable date.
    pub date: String,
    /// Instrument.
    pub instrument: String,
    /// Side.
    pub side: OrderSide,
    /// Fill quantity.
    pub quantity: f64,
    /// Fill price.
    pub price: f64,
    /// Costs.
    pub costs: f64,
    /// Currency.
    pub currency: Currency,
}

/// Order fills response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OrderFillsResponse {
    /// Fill entries.
    pub fills: Vec<FillEntry>,
    /// Total count.
    pub total_count: u64,
}
