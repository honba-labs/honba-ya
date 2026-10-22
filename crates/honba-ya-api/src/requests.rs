#![allow(missing_docs)]
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use honba_core::InstrumentId;

use crate::strategy_ir::StrategyManifest;
use crate::wire::TradingState;

/// Instruments query parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InstrumentsQuery {
    /// Exchange filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,
    /// Symbol filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Quotes query parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuotesQuery {
    /// Comma-separated symbols.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbols: Option<String>,
    /// Venue/exchange.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    /// Return the latest quote known at this time (RFC3339 or ISO-8601 date, inclusive);
    /// the latest held when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

/// Bars query parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BarsQuery {
    /// Timeframe/bar specification (e.g. "1m", "1d").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tf: Option<String>,
    /// Inclusive start time, RFC3339 or ISO-8601 date.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Exclusive end time, RFC3339 or ISO-8601 date.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
}

/// `GET /screener/scan` query parameters.
///
/// The structured values are JSON text inside the query string, so the whole request is one
/// flat string map on every transport.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScreenerQuery {
    /// JSON-encoded filter group, `{"operator": "AND"|"OR", "items": [...]}`; items are
    /// predicates (`{"key", "op", "value"}`) or nested groups. Omitted means no filter: every
    /// instrument of the universe matches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<String>,
    /// JSON-encoded array of instrument ids (`["NSE:TCS", ...]`) to scan; required, at most
    /// `MAX_SCREENER_UNIVERSE` entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub universe: Option<String>,
    /// Timeframe of the bars evaluated (default `1d`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tf: Option<String>,
    /// Evaluate on the bars known at this time (RFC3339 or ISO-8601 date, inclusive); the
    /// latest held when omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

/// Depth (order book) query parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DepthQuery {
    /// Number of levels per side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
}

/// `POST /strategies` request: the manifest to verify, compile and keep.
///
/// Source code is not accepted (`code` or `source` is a 422 with
/// `reason = source_unsupported`); a manifest is the only input a server can
/// verify without running author code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategiesRequest {
    /// The strategy manifest, as accepted by `POST /strategies/verify`.
    pub manifest: StrategyManifest,
}

/// Backtest request.
///
/// Every field is optional on the wire, but `BacktestRequest::resolve` requires `seed`
/// (non-zero), `strategy`, `universe`, `start` and `end` (ADR 0017 decision 6).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BacktestRequest {
    /// Strategy name or manifest id, as returned by `POST /strategies`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// Universe of instruments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub universe: Option<String>,
    /// Inclusive start date/time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Exclusive end date/time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Bar specification as a string (e.g. "1d").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bar_spec: Option<String>,
    /// Initial capital, as a decimal string on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_capital: Option<Decimal>,
    /// Seed, for reproducibility; required and non-zero on submit. Same seed and data give a
    /// byte-identical journal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// Fill model for this run, overriding the server's configured one: `bar_fill` (the
    /// default), `bar_fill_through`, `l2` or `none`. `l2` needs a tick source; without one the
    /// run fails with `l2_requires_ticks`. See [`FILL_MODELS`](crate::FILL_MODELS).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_model: Option<String>,
    /// Flat commission charged on every fill, in the account's settlement currency;
    /// `None` or `0` charges none. Applied only by the `bar_fill` models (ADR 0017;
    /// see [`MAX_COMMISSION_PER_FILL`](crate::MAX_COMMISSION_PER_FILL)). Added in this
    /// request's first version, so an older client that omits it still parses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commission_per_fill: Option<Decimal>,
    /// Slippage applied to every fill, in basis points of the fill price and always adverse
    /// to the order's side; `None` or `0` models none. Applied only by the `bar_fill` models.
    /// See [`MAX_SLIPPAGE_BPS`](crate::MAX_SLIPPAGE_BPS).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_bps: Option<Decimal>,
}

/// Sweep request.
///
/// Every field is optional on the wire, but `SweepRequest::resolve` requires `seed`
/// (non-zero), `strategy`, `params` and `trials` (ADR 0017 decision 6).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SweepRequest {
    /// Strategy name or manifest id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    /// Universe of instruments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub universe: Option<String>,
    /// Inclusive start date/time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Exclusive end date/time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Bar specification as a string (e.g. "1d").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bar_spec: Option<String>,
    /// Initial capital, as a decimal string on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_capital: Option<Decimal>,
    /// Parameter ranges, as a JSON object of name to [low, high, step].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    /// Number of trials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trials: Option<u64>,
    /// Seed; required and non-zero on submit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

/// `GET /backtests/{id}/events` query parameters (ADR 0021).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BacktestEventsQuery {
    /// Replay the journal records with line index `>=` this value (0-based; default 0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_from_seq: Option<u64>,
}

/// `GET /orders` query parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrdersQuery {
    /// Exchange filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,
    /// Symbol filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Orders request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrdersRequest {
    /// Instrument to trade.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<InstrumentId>,
    /// Side: buy or sell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// Order type: market or limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<String>,
    /// Quantity, as a decimal string on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<Decimal>,
    /// Limit price, as a decimal string on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    /// Time in force: day, gtc, ioc, fok.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tif: Option<String>,
    /// Client order id: the idempotency key (ADR 0019 decision 3). The ledger
    /// records the order under it; a repeat of the same key is a duplicate
    /// no-op that returns the first row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
}

/// `PUT /trading-state` request body: the state the operator wants (ADR 0018 decision 7).
///
/// Every transition is allowed and setting the current state again is a no-op. `reason` is
/// free text for the operator log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TradingStateRequest {
    /// The new trading state: `active`, `reducing` or `halted`.
    pub state: TradingState,
    /// Why the state is changing; logged with the change, never interpreted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `POST /bars` request body: a batch of bars for the live bar pump (E3-S7 follow-up).
///
/// Each bar must carry finite prices with `low <= open, close <= high` and `volume >= 0`;
/// the batch is validated before it is queued, so one bad bar rejects the whole batch. The
/// batch size is bounded by [`crate::MAX_INGEST_BARS`]. Bars must ascend by `ts_event`;
/// ordering is enforced downstream by the simulator, where an out-of-order bar is counted
/// and skipped rather than failing the request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BarsIngestRequest {
    /// The bars to feed, oldest first.
    pub bars: Vec<honba_core::Bar>,
}
