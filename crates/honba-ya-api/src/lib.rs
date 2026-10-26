//! Transport-agnostic versioned API surface for the Honba platform.
//!
//! The envelope, error taxonomy, endpoint registry, and version constants live
//! in [`wire`] because they are wire types that every surface — Python, REST,
//! WASM, MCP — reads and writes. This crate holds the request and response
//! DTOs that sit *inside* that envelope, plus the endpoint registry the code
//! generator walks to emit OpenAPI `paths`. The strategy manifest → IR step
//! lives in [`strategy_ir`].
//!
//! It stays transport-free on purpose: no axum, no wasm-bindgen, no I/O. The
//! REST and WASM crates depend on this one, not on each other.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

pub mod cache;
pub mod capabilities;
pub mod ingest;
pub mod market;
pub mod profile;
pub mod requests;
pub mod responses;
pub mod run_id;
pub mod run_ports;
pub mod run_resolve;
pub mod runs;
pub mod screener;
pub mod strategies;
pub mod strategy_ir;
pub mod verify;
pub mod wire;

pub use cache::{CacheKey, CachedResult, ResultCache, canonical_json_bytes};
pub use capabilities::{Capabilities, CapabilityManifest};
pub use ingest::{MAX_INGEST_BARS, check_ingest_batch};
pub use market::{
    DEFAULT_DEPTH_LEVELS, DEFAULT_TIMEFRAME, MAX_DEPTH_LEVELS, ResolvedBarsQuery,
    ResolvedQuotesQuery, instrument_json, parse_instrument_id, parse_timeframe,
};
pub use profile::{
    LayoutDocument, LayoutsResponse, Module, ModulesResponse, Profile, ProfileRevision, Source,
};
pub use requests::{
    BacktestEventsQuery, BacktestRequest, BarsIngestRequest, BarsQuery, DepthQuery,
    InstrumentsQuery, OrdersQuery, OrdersRequest, QuotesQuery, ScreenerQuery, StrategiesRequest,
    SweepRequest, TradingStateRequest,
};
pub use responses::{
    AuditEntry, BacktestMetrics, BacktestResponse, BacktestStatus, BarsIngestResponse,
    BarsResponse, CapabilitiesResponse, CompiledStrategy, ConcentrationEntry,
    ConcentrationRiskResponse, DepthLevel, DepthResponse, EquityCurveSummary, EquityPoint,
    FeeEntry, FeeTypeSummary, FeesReportResponse, FillEntry, InstrumentsResponse, LedgerEntry,
    LedgerHistoryResponse, OrderFillsResponse, OrdersResponse, PortfolioAccountsResponse,
    PortfolioHoldingsResponse, PortfolioPositionsResponse, PortfolioSnapshotsResponse,
    PortfolioValueChartResponse, PositionAuditTrailResponse, PositionsResponse, QuotesResponse,
    RunStatus, ScreenerResponse, ScreenerResultRow, SnapshotEntry, StrategiesResponse, StreamFrame,
    SweepReportResponse, SweepResponse, SweepStatus, TaxReportResponse, TaxReportSummary,
    TaxTransaction, TradesResponse, TradingStateChange, TradingStateResponse, WACEntry,
    WeightedAverageCostResponse,
};
pub use run_id::{RunId, RunIdError, RunIdGenerator};
pub use run_ports::{JournalWriter, RunExecutor, RunJob, RunOutcome};
pub use run_resolve::{
    DEFAULT_BAR_SPEC, DEFAULT_INITIAL_CAPITAL, FILL_MODELS, MAX_COMMISSION_PER_FILL,
    MAX_SLIPPAGE_BPS, ResolvedBacktest, ResolvedRequest, ResolvedSweep, unknown_strategy,
};
pub use runs::{MANIFEST_VERSION, RunEvent, RunKind, RunManifest, TransitionError};
pub use screener::{
    DEFAULT_SCREENER_TIMEFRAME, MAX_SCREENER_BARS, MAX_SCREENER_ROWS, MAX_SCREENER_UNIVERSE,
    ResolvedScreenerQuery, check_scan_budget, check_screener_rows,
};
pub use strategies::{
    MAX_COMPILED_STRATEGIES, StrategyCatalog, compile_strategy, list_strategies,
    parse_compile_request,
};
pub use strategy_ir::{
    IrError, StrategyIr, StrategyManifest, StrategySubscriptions, Subscriptions, TimeframeSpec,
    Universe,
};
pub use verify::{VerifyStrategyRequest, VerifyStrategyResponse, verify_strategy};
pub use wire::{
    API_VERSION, Access, ApiResponse, ApiVersion, ENDPOINTS, Endpoint, ErrorCode, ErrorDetail,
    Event, HttpMethod, Message, ParamLocation, ResponseEnvelope, SCHEMA_VERSION, TradingState,
    WRITE_PATHS, write_endpoints,
};

#[cfg(test)]
mod tests;
