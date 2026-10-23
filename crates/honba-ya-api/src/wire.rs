//! Transport-owned versioned wire types for every Honba API surface.
//!
//! The envelope, error taxonomy, endpoint registry, version constants, and the
//! read models that sit inside the envelope live here, so this crate depends on
//! `honba-core` alone for wire concerns. Domain values that already exist in
//! [`honba_core`] (`Bar`, `InstrumentId`, `OrderRequest`, `FillReport`,
//! `Currency`, `OrderSide`, …) are referenced directly; everything else is
//! defined here.
//!
//! Monetary values are exact [`Decimal`] amounts that serialize as strings;
//! timestamps are [`DateTime<Utc>`]. There are no floating-point money fields
//! anywhere on the wire (Zero Floats Policy).

use std::fmt;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use honba_core::{Bar, Currency, FillReport, InstrumentId, OrderRequest};

/// The wire `schema_version` this crate emits and stamps on every journal
/// message. Bumped whenever a wire shape changes incompatibly (ADR 0012).
pub const SCHEMA_VERSION: &str = "4";

/// The API version every route is served under (`/api/{API_VERSION}/…`).
pub const API_VERSION: &str = "v1";

/// The API version an envelope or manifest was written against.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct ApiVersion(String);

impl Default for ApiVersion {
    fn default() -> Self {
        // Default to v1 (API_VERSION) rather than an empty string.
        Self(API_VERSION.to_owned())
    }
}

impl ApiVersion {
    /// The version as text, e.g. `v1`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ApiVersion {
    fn from(version: &str) -> Self {
        Self(version.to_owned())
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The response envelope every transport wraps a payload in (ADR 0012).
///
/// A success carries `data`; a failure carries `error`. Both are optional on
/// the wire so a payload from a newer server (extra fields, both absent) still
/// parses for an older client.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResponseEnvelope<T> {
    /// The API version the server answered under.
    #[serde(default)]
    pub api_version: ApiVersion,
    /// The wire schema version the payload was written against.
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    /// The payload; absent on a failure.
    #[serde(default)]
    pub data: Option<T>,
    /// The failure; absent on a success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

fn default_schema_version() -> String {
    SCHEMA_VERSION.to_owned()
}

impl<T> ResponseEnvelope<T> {
    /// Unwraps a success, or returns the carried failure.
    ///
    /// An envelope with neither `data` nor `error` is a server bug and answers
    /// `internal_error`.
    pub fn into_result(self) -> Result<T, ErrorDetail> {
        match (self.data, self.error) {
            (Some(data), _) => Ok(data),
            (None, Some(error)) => Err(error),
            (None, None) => Err(ErrorDetail::new(
                ErrorCode::InternalError,
                "the envelope holds neither data nor an error",
            )),
        }
    }
}

/// Constructors for a [`ResponseEnvelope`]; not a wire type itself.
pub struct ApiResponse;

impl ApiResponse {
    /// A success envelope carrying `data`.
    pub fn success<T>(data: T) -> ResponseEnvelope<T> {
        ResponseEnvelope {
            api_version: ApiVersion::default(),
            schema_version: default_schema_version(),
            data: Some(data),
            error: None,
        }
    }

    /// A failure envelope carrying `error`.
    pub fn error<T>(error: ErrorDetail) -> ResponseEnvelope<T> {
        ResponseEnvelope {
            api_version: ApiVersion::default(),
            schema_version: default_schema_version(),
            data: None,
            error: Some(error),
        }
    }
}

/// The stable error taxonomy. The wire spelling is `snake_case`; a caller
/// branches on the code, never on the message.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request failed validation; `context.field` names the field.
    ValidationInvalidRequest,
    /// The caller is not authenticated or not allowed to.
    Unauthorized,
    /// The addressed resource does not exist.
    NotFound,
    /// The request conflicts with the current state (e.g. a duplicate id).
    Conflict,
    /// The route exists in the contract but is not implemented in this build.
    NotImplemented,
    /// An internal failure; never a caller's fault.
    InternalError,
    /// The operation timed out and may succeed if retried.
    Timeout,
    /// A pre-trade risk check refused the order.
    RiskMaxNotionalExceeded,
}

impl ErrorCode {
    /// The stable wire spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ValidationInvalidRequest => "validation_invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::NotImplemented => "not_implemented",
            Self::InternalError => "internal_error",
            Self::Timeout => "timeout",
            Self::RiskMaxNotionalExceeded => "risk_max_notional_exceeded",
        }
    }

    /// Whether a caller that retries the identical request may succeed.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Timeout)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One failure, as it crosses the wire (ADR 0012).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ErrorDetail {
    /// The stable error code.
    pub code: ErrorCode,
    /// A human-readable sentence; never parsed.
    pub message: String,
    /// Whether retrying the identical request may succeed.
    #[serde(default)]
    pub retryable: bool,
    /// Machine-readable extras, e.g. `{"field": …, "reason": …}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

impl ErrorDetail {
    /// A new failure with `code` and `message`; `retryable` follows the code.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable(),
            context: None,
        }
    }

    /// Attaches machine-readable context and returns `self`.
    pub fn with_context(mut self, context: Value) -> Self {
        self.context = Some(context);
        self
    }
}

impl fmt::Display for ErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// The operator trading state every write route evaluates against (ADR 0018).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TradingState {
    /// Orders flow normally.
    Active,
    /// Only orders that reduce exposure are accepted.
    Reducing,
    /// No order reaches the exchange.
    Halted,
}

/// HTTP method of a registered endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HttpMethod {
    /// `GET`
    Get,
    /// `POST`
    Post,
    /// `PUT`
    Put,
    /// `DELETE`
    Delete,
}

impl HttpMethod {
    /// The uppercase spelling OpenAPI requires.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
        }
    }
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for HttpMethod {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Access tier a route is gated by.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    /// Reads state; never moves money.
    ReadOnly,
    /// Places orders or changes the trading state.
    Trade,
    /// Administrative operation.
    Admin,
}

/// Where a route reads its parameters from.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ParamLocation {
    /// `{placeholder}` segments of the path.
    Path,
    /// The query string.
    Query,
}

/// One registered endpoint, typed by its query and body DTOs.
///
/// The generic parameters never appear at runtime; they exist so a code
/// generator can name the DTOs a route reads and writes.
pub struct Endpoint<Q, B> {
    /// The HTTP method.
    pub method: HttpMethod,
    /// The path, joined onto `/api/v1`; `{name}` marks a path parameter.
    pub path: String,
    /// The access tier the route is gated by.
    pub access: Access,
    /// Where the route reads its parameters from.
    pub params: ParamLocation,
    /// Whether the route upgrades to a stream (WebSocket / SSE).
    pub stream: bool,
    /// Short operation tag for the generated OpenAPI document.
    pub tag: String,
    _query: PhantomData<Q>,
    _body: PhantomData<B>,
}

impl<B> Endpoint<(), B> {
    /// A new registered endpoint.
    pub fn new(
        method: HttpMethod,
        path: &str,
        access: Access,
        params: ParamLocation,
        stream: bool,
        tag: &str,
    ) -> Self {
        Self {
            method,
            path: path.to_owned(),
            access,
            params,
            stream,
            tag: tag.to_owned(),
            _query: PhantomData,
            _body: PhantomData,
        }
    }
}

impl<Q, B> Endpoint<Q, B> {
    /// The `{name}` path parameters, in order of appearance. An unterminated
    /// `{` ends the scan rather than looping forever.
    pub fn path_params(&self) -> Vec<&str> {
        let mut out = Vec::new();
        let bytes = self.path.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'{' {
                match self.path[i + 1..].find('}') {
                    Some(end) if end > 0 => {
                        out.push(&self.path[i + 1..i + 1 + end]);
                        i += 1 + end + 1;
                    }
                    _ => break,
                }
            } else {
                i += 1;
            }
        }
        out
    }

    /// The registry key, `METHOD /path`.
    pub fn key(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

/// The full endpoint registry: every `METHOD /path` pair this API version
/// serves, in registry order. The code generator walks it to emit OpenAPI
/// `paths`.
pub static ENDPOINTS: &[(&str, &str)] = &[
    ("GET", "/health"),
    ("GET", "/capabilities"),
    ("GET", "/profile"),
    ("PUT", "/profile"),
    ("GET", "/profile/layouts"),
    ("PUT", "/profile/layouts/{name}"),
    ("GET", "/profile/modules"),
    ("GET", "/instruments"),
    ("GET", "/quotes"),
    ("GET", "/bars/{id}"),
    ("POST", "/bars"),
    ("GET", "/depth/{id}"),
    ("GET", "/screener/scan"),
    ("GET", "/strategies"),
    ("POST", "/strategies"),
    ("POST", "/strategies/verify"),
    ("GET", "/backtests"),
    ("POST", "/backtests"),
    ("GET", "/backtests/{id}"),
    ("GET", "/backtests/{id}/events"),
    ("GET", "/sweeps"),
    ("POST", "/sweeps"),
    ("GET", "/sweeps/{id}"),
    ("GET", "/orders"),
    ("POST", "/orders"),
    ("DELETE", "/orders/{id}"),
    ("GET", "/orders/{id}/fills"),
    ("POST", "/positions/close"),
    ("GET", "/positions"),
    ("GET", "/positions/audit"),
    ("GET", "/trades"),
    ("GET", "/portfolio"),
    ("GET", "/portfolio/accounts"),
    ("GET", "/portfolio/positions"),
    ("GET", "/portfolio/holdings"),
    ("GET", "/portfolio/chart"),
    ("GET", "/portfolio/concentration"),
    ("GET", "/portfolio/wac"),
    ("GET", "/ledger"),
    ("GET", "/tax/report"),
    ("GET", "/fees"),
    ("GET", "/trading-state"),
    ("PUT", "/trading-state"),
];

/// The routes that place or cancel orders or move the trading state, in
/// registry order. They sit behind the approval queue and the trading-state
/// gate in every transport.
pub static WRITE_PATHS: &[(&str, &str)] = &[
    ("POST", "/bars"),
    ("PUT", "/trading-state"),
    ("POST", "/orders"),
    ("DELETE", "/orders/{id}"),
    ("POST", "/positions/close"),
];

/// The write-gated routes as `METHOD /path` keys.
pub fn write_endpoints() -> Vec<String> {
    WRITE_PATHS
        .iter()
        .map(|(method, path)| format!("{method} {path}"))
        .collect()
}

/// One journal record: a versioned event plus the time it was initialized.
///
/// `schema_version` is stamped from [`SCHEMA_VERSION`] at construction, so a
/// replaying reader always knows which decoder the record needs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Message {
    /// The wire schema version the record was written against.
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    /// The event payload.
    pub event: Event,
    /// The time the record was initialized (UTC).
    pub ts_init: DateTime<Utc>,
}

impl Message {
    /// A new record stamped with the current [`SCHEMA_VERSION`].
    pub fn new(event: Event, ts_init: DateTime<Utc>) -> Self {
        Self {
            schema_version: default_schema_version(),
            event,
            ts_init,
        }
    }

    /// The wire schema version the record was written against.
    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }
}

/// The events a run journal carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Event {
    /// A top-of-book quote tick.
    Quote(QuoteTick),
    /// An aggregated OHLCV bar.
    Bar(Bar),
    /// An order state change.
    Order(Order),
    /// An execution fill.
    Fill(Trade),
    /// A named scalar metric, e.g. an equity mark.
    Metric {
        /// The metric name.
        name: String,
        /// The metric value.
        value: Value,
    },
}

/// The top-of-book quote read model (`GET /quotes`).
///
/// The transport-level view of [`honba_core::Tick`]'s quote arm, without the
/// band enrichment, so a plain quote list stays a flat record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QuoteTick {
    /// The quoted instrument.
    pub instrument: InstrumentId,
    /// Best bid price.
    pub bid: Decimal,
    /// Best ask price.
    pub ask: Decimal,
    /// Best bid quantity.
    pub bid_qty: Decimal,
    /// Best ask quantity.
    pub ask_qty: Decimal,
    /// Exchange timestamp.
    pub exchange_ts: DateTime<Utc>,
    /// Local ingestion receive timestamp.
    pub receive_ts: DateTime<Utc>,
}

impl QuoteTick {
    /// A quote with both sides and both timestamps.
    pub fn new(
        instrument: InstrumentId,
        bid: Decimal,
        ask: Decimal,
        bid_qty: Decimal,
        ask_qty: Decimal,
        exchange_ts: DateTime<Utc>,
        receive_ts: DateTime<Utc>,
    ) -> Self {
        Self {
            instrument,
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts,
            receive_ts,
        }
    }
}

/// The order read model (`GET /orders`): a placed order as the ledger holds it.
pub type Order = OrderRequest;

/// The trade read model (`GET /trades`): one realized execution.
pub type Trade = FillReport;

/// The position read model (`GET /positions`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Position {
    /// The instrument held.
    pub instrument: InstrumentId,
    /// Signed quantity; negative when short.
    pub quantity: Decimal,
    /// Volume-weighted average entry price.
    pub avg_price: Decimal,
    /// Last mark price, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_price: Option<Decimal>,
    /// Unrealized PnL at the mark.
    pub unrealized_pnl: Decimal,
    /// Realized PnL booked on this position.
    pub realized_pnl: Decimal,
}

/// `GET /portfolio/accounts`: one valued account of the caller's portfolio.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioAccount {
    /// The account id.
    pub account_id: String,
    /// Settlement currency.
    pub currency: Currency,
    /// Free cash.
    pub cash: Decimal,
    /// Market value of the open positions.
    pub positions_value: Decimal,
    /// Cash plus position value.
    pub equity: Decimal,
}

/// `GET /portfolio/positions`: one valued open position, flattened across
/// the caller's accounts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioPosition {
    /// The account the position sits in.
    pub account_id: String,
    /// The instrument held.
    pub instrument: InstrumentId,
    /// Signed quantity; negative when short.
    pub quantity: Decimal,
    /// Volume-weighted average entry price.
    pub avg_price: Decimal,
    /// Quantity times mark price.
    pub market_value: Decimal,
    /// Unrealized PnL at the mark.
    pub unrealized_pnl: Decimal,
}

/// `GET /portfolio/holdings`: one valued delivery holding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PortfolioHolding {
    /// The account the holding sits in.
    pub account_id: String,
    /// The instrument held.
    pub instrument: InstrumentId,
    /// Quantity held.
    pub quantity: Decimal,
    /// Volume-weighted average acquisition cost.
    pub avg_price: Decimal,
    /// Quantity times mark price.
    pub market_value: Decimal,
}

/// Statistical overfitting validation block (E4-S6).
///
/// Attached to a completed backtest or sweep report; every field is optional
/// and the record tolerates fields a newer server adds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ValidationBlock {
    /// Probability the strategy is overfit (E-values style).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probability_of_overfitting: Option<Decimal>,
    /// Deflated Sharpe ratio.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deflated_sharpe_ratio: Option<Decimal>,
    /// Expected maximum drawdown under resampling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_max_drawdown: Option<Decimal>,
    /// Trials the validation was computed across.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number_of_trials: Option<u64>,
}

/// The instrument kind a reference instrument carries (`GET /instruments`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    /// Ordinary share.
    Equity,
    /// Exchange-traded fund.
    Etf,
    /// Bond.
    Bond,
    /// IPO allotment.
    Ipo,
    /// Futures contract.
    Future,
    /// Options contract.
    Option,
    /// Foreign exchange.
    Fx,
    /// Index (not directly tradable).
    Index,
    /// Mutual fund unit.
    MutualFund,
    /// Anything else.
    Other,
}

/// The reference-data read model (`GET /instruments`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Instrument {
    /// The instrument id.
    pub id: InstrumentId,
    /// What kind of instrument it is.
    pub kind: InstrumentKind,
    /// Settlement currency.
    pub currency: Currency,
    /// Units per tradable lot.
    pub lot_size: Decimal,
    /// Minimum price increment.
    pub tick_size: Decimal,
}

impl Instrument {
    /// A reference instrument record.
    pub fn new(
        id: InstrumentId,
        kind: InstrumentKind,
        currency: Currency,
        lot_size: Decimal,
        tick_size: Decimal,
    ) -> Self {
        Self {
            id,
            kind,
            currency,
            lot_size,
            tick_size,
        }
    }

    /// The instrument id.
    pub fn id(&self) -> &InstrumentId {
        &self.id
    }

    /// The instrument kind.
    pub fn kind(&self) -> InstrumentKind {
        self.kind
    }

    /// The settlement currency.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    /// Units per tradable lot.
    pub fn lot_size(&self) -> Decimal {
        self.lot_size
    }

    /// The minimum price increment.
    pub fn tick_size(&self) -> Decimal {
        self.tick_size
    }
}

/// Bar length unit of a [`BarSpecification`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum BarAggregation {
    /// Seconds.
    Second,
    /// Minutes.
    Minute,
    /// Hours.
    Hour,
    /// Trading days.
    Day,
    /// Weeks.
    Week,
    /// Calendar months.
    Month,
}

/// Which price of a bar an indicator or query reads.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PriceType {
    /// The close (last traded) price.
    Last,
    /// The best bid.
    Bid,
    /// The best ask.
    Ask,
    /// The open price.
    Open,
    /// The high price.
    High,
    /// The low price.
    Low,
}

/// A concrete bar length and price series, e.g. `1d` last-price.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BarSpecification {
    step: u64,
    aggregation: BarAggregation,
    price_type: PriceType,
}

impl BarSpecification {
    /// A specification: `step` × `aggregation` bars of `price_type`.
    pub const fn new(step: u64, aggregation: BarAggregation, price_type: PriceType) -> Self {
        Self {
            step,
            aggregation,
            price_type,
        }
    }

    /// The number of units per bar.
    pub fn step(&self) -> u64 {
        self.step
    }

    /// The unit of `step`.
    pub fn aggregation(&self) -> BarAggregation {
        self.aggregation
    }

    /// The price series the bars of this spec carry.
    pub fn price_type(&self) -> PriceType {
        self.price_type
    }
}
