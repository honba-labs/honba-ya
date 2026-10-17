//! Transport-agnostic versioned API surface for the Honba platform.
//!
//! The envelope, error taxonomy, endpoint registry, and version constants live in
//! `honba-messages` (L0) because they are wire types that every surface —
//! Python, REST, WASM, MCP — reads and writes. This crate holds the request
//! and response DTOs that sit *inside* that envelope, plus the endpoint
//! registry the code generator walks to emit OpenAPI `paths`.
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
pub mod verify;

pub use cache::{canonical_json_bytes, CacheKey, CachedResult, ResultCache};
pub use capabilities::{Capabilities, CapabilityManifest};
pub use honba_messages::{
    write_endpoints, Access, ApiResponse, ApiVersion, Endpoint, ErrorCategory, ErrorCode,
    ErrorDetail, HttpMethod, ParamLocation, ResponseEnvelope, API_VERSION, ENDPOINTS, WRITE_PATHS,
};
pub use ingest::{check_ingest_batch, MAX_INGEST_BARS};
pub use market::{
    instrument_json, parse_instrument_id, parse_timeframe, ResolvedBarsQuery, ResolvedQuotesQuery,
    DEFAULT_DEPTH_LEVELS, DEFAULT_TIMEFRAME, MAX_DEPTH_LEVELS,
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
    BacktestMetrics, BacktestResponse, BacktestStatus, BarsIngestResponse, BarsResponse,
    CapabilitiesResponse, CompiledStrategy, DepthLevel, DepthResponse, InstrumentsResponse,
    OrdersResponse, PortfolioAccountsResponse, PortfolioHoldingsResponse,
    PortfolioPositionsResponse, PositionsResponse, QuotesResponse, RunStatus, ScreenerResponse,
    ScreenerResultRow, StrategiesResponse, StreamFrame, SweepReportResponse, SweepResponse,
    SweepStatus, TradesResponse, TradingStateChange, TradingStateResponse,
};
pub use run_id::{RunId, RunIdError, RunIdGenerator};
pub use run_ports::{JournalWriter, RunExecutor, RunJob, RunOutcome};
pub use run_resolve::{
    unknown_strategy, ResolvedBacktest, ResolvedRequest, ResolvedSweep, DEFAULT_BAR_SPEC,
    DEFAULT_INITIAL_CAPITAL, FILL_MODELS, MAX_COMMISSION_PER_FILL, MAX_SLIPPAGE_BPS,
};
pub use runs::{RunEvent, RunKind, RunManifest, TransitionError, MANIFEST_VERSION};
pub use screener::{
    check_scan_budget, check_screener_rows, ResolvedScreenerQuery, DEFAULT_SCREENER_TIMEFRAME,
    MAX_SCREENER_BARS, MAX_SCREENER_ROWS, MAX_SCREENER_UNIVERSE,
};
pub use strategies::{
    compile_strategy, list_strategies, parse_compile_request, StrategyCatalog,
    MAX_COMPILED_STRATEGIES,
};
pub use verify::{verify_strategy, VerifyStrategyRequest, VerifyStrategyResponse};

#[cfg(test)]
mod tests;
