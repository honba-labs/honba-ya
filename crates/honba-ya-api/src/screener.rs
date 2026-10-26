//! Query resolution and per-instrument evaluation for `GET /screener/scan`.
//!
//! Pure like [`crate::market`]: the REST handler reads bars through its port and hands each
//! instrument's history to [`ResolvedScreenerQuery::evaluate`]; nothing here does I/O. The
//! filter grammar ([`ScreenerFilterGroup`], [`ScreenerPredicate`], [`Timeframe`]) and its
//! evaluator live in this module, so it is self-contained: a metric is a bar column (`open`,
//! `high`, `low`, `close`, `volume`) or a simple moving average of the close (`SMA<n>`), and
//! every threshold is an exact [`Decimal`] — no float ever touches a price.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use honba_core::{Bar, InstrumentId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::market::{invalid, parse_bound, parse_instrument_id, parse_timeframe};
use crate::requests::ScreenerQuery;
use crate::responses::ScreenerResultRow;
use crate::wire::{BarAggregation, BarSpecification, ErrorCode, ErrorDetail};

/// The timeframe used when `tf` is omitted from a scan.
pub const DEFAULT_SCREENER_TIMEFRAME: &str = "1d";

/// The most instruments one scan may name.
pub const MAX_SCREENER_UNIVERSE: usize = 1_000;

/// The most matching rows one scan may return; there is no pagination, so the caller narrows the
/// universe or the filter.
pub const MAX_SCREENER_ROWS: usize = 500;

/// The most bars one scan may read in total across the universe.
pub const MAX_SCREENER_BARS: usize = 2_000_000;

/// A [`ScreenerQuery`] resolved into domain values.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedScreenerQuery {
    /// Instruments to scan, sorted and de-duplicated.
    pub universe: Vec<InstrumentId>,
    /// The parsed filter (empty group when none was given); validated against the metrics a bar
    /// dataset can compute.
    pub filters: ScreenerFilterGroup,
    /// Every metric key the filter reads, in first-seen order; the keys of a row's `metrics`.
    pub metric_keys: Vec<String>,
    /// Bar specification evaluated (always last-price).
    pub spec: BarSpecification,
    /// Exclusive end of the bars read: just past `as_of`, or `None` for all history.
    pub to: Option<DateTime<Utc>>,
}

fn filter_error(error: &ScreenerError) -> ErrorDetail {
    invalid("filters", error.reason(), error.to_string())
}

/// Whether a wire timeframe names the same bar length as `spec`.
fn timeframe_matches(tf: &Timeframe, spec: &BarSpecification) -> bool {
    let (step, seconds): (usize, Option<u64>) = match tf {
        Timeframe::M1 => (1, Some(60)),
        Timeframe::M5 => (5, Some(60)),
        Timeframe::M15 => (15, Some(60)),
        Timeframe::M30 => (30, Some(60)),
        Timeframe::H1 => (60, Some(60)),
        Timeframe::H2 => (120, Some(60)),
        Timeframe::H4 => (240, Some(60)),
        Timeframe::D1 => (1, Some(86_400)),
        Timeframe::W1 => (1, Some(604_800)),
        Timeframe::Month1 => {
            return spec.aggregation() == BarAggregation::Month && spec.step() == 1;
        }
    };
    let unit: u64 = match spec.aggregation() {
        BarAggregation::Second => 1,
        BarAggregation::Minute => 60,
        BarAggregation::Hour => 3_600,
        BarAggregation::Day => 86_400,
        BarAggregation::Week => 604_800,
        _ => return false,
    };
    let wanted = seconds.map(|s| s.saturating_mul(step as u64));
    wanted == Some(unit.saturating_mul(spec.step()))
}

fn parse_universe(text: Option<&str>) -> Result<Vec<InstrumentId>, ErrorDetail> {
    let bad = |message: String| invalid("universe", "invalid_universe", message);
    let Some(text) = text else {
        return Err(invalid(
            "universe",
            "missing_universe",
            "`universe` must be a JSON array of instrument ids, e.g. [\"TCS.NSE\"]",
        ));
    };
    let raw: Vec<serde_json::Value> = serde_json::from_str(text)
        .map_err(|e| bad(format!("`universe` is not a JSON array: {e}")))?;
    if raw.is_empty() {
        return Err(invalid(
            "universe",
            "missing_universe",
            "`universe` must name at least one instrument",
        ));
    }
    if raw.len() > MAX_SCREENER_UNIVERSE {
        return Err(too_many(
            "universe",
            MAX_SCREENER_UNIVERSE,
            format!(
                "the universe names {} instruments, over the limit of {MAX_SCREENER_UNIVERSE}",
                raw.len()
            ),
        ));
    }
    let mut ids = Vec::with_capacity(raw.len());
    for item in &raw {
        let text = item
            .as_str()
            .ok_or_else(|| bad(format!("universe entry {item} is not a string")))?;
        let id = parse_instrument_id(text).map_err(|_| {
            invalid(
                "universe",
                "invalid_instrument_id",
                format!("universe entry {text:?} is not SYMBOL.EXCHANGE"),
            )
        })?;
        ids.push(id);
    }
    // Canonical instrument ids have no `Ord` (exchange, symbol), so sort by the canonical
    // textual form; dedup then drops exact repeats.
    ids.sort_by_key(|id| id.to_string());
    ids.dedup();
    Ok(ids)
}

fn too_many(field: &str, limit: usize, message: String) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message).with_context(json!({
        "field": field,
        "reason": "too_many_rows",
        "limit": limit,
    }))
}

/// Rejects a scan that has read more than [`MAX_SCREENER_BARS`] bars so far.
pub fn check_scan_budget(bars_read: usize) -> Result<(), ErrorDetail> {
    if bars_read <= MAX_SCREENER_BARS {
        return Ok(());
    }
    Err(too_many(
        "universe",
        MAX_SCREENER_BARS,
        format!(
            "the scan reads over {MAX_SCREENER_BARS} bars; narrow the universe or use as_of with a \
             coarser tf"
        ),
    ))
}

/// Rejects a scan with more than [`MAX_SCREENER_ROWS`] matches.
pub fn check_screener_rows(rows: usize) -> Result<(), ErrorDetail> {
    if rows <= MAX_SCREENER_ROWS {
        return Ok(());
    }
    Err(too_many(
        "universe",
        MAX_SCREENER_ROWS,
        format!(
            "the scan matched {rows} instruments, over the limit of {MAX_SCREENER_ROWS}; narrow the universe or the filter"
        ),
    ))
}

impl ScreenerQuery {
    /// Resolves the query: bounded universe, parsed and validated filter, timeframe and `as_of`.
    ///
    /// A filter that reads a metric a bar dataset cannot compute is rejected here, before any
    /// bar is read.
    pub fn resolve(&self) -> Result<ResolvedScreenerQuery, ErrorDetail> {
        let universe = parse_universe(self.universe.as_deref())?;
        let spec = parse_timeframe(self.tf.as_deref().unwrap_or(DEFAULT_SCREENER_TIMEFRAME))?;
        let to = self
            .as_of
            .as_deref()
            .map(|text| parse_bound("as_of", text))
            .transpose()?
            .map(|t| t + Duration::nanoseconds(1));
        let filters: ScreenerFilterGroup = match self.filters.as_deref() {
            None => ScreenerFilterGroup {
                operator: "AND".to_owned(),
                items: Vec::new(),
            },
            Some(text) => serde_json::from_str(text).map_err(|e| {
                invalid(
                    "filters",
                    "invalid_filters",
                    format!("`filters` is not a JSON filter group: {e}"),
                )
            })?,
        };
        validate_group(&filters).map_err(|e| filter_error(&e))?;
        for tf in group_timeframes(&filters).map_err(|e| filter_error(&e))? {
            if !timeframe_matches(&tf, &spec) {
                return Err(invalid(
                    "filters",
                    "unsupported_timeframe",
                    format!(
                        "a predicate asks for timeframe {tf} but the scan evaluates `tf` bars; \
                         set `tf` to match"
                    ),
                ));
            }
        }
        let metric_keys = group_metric_keys(&filters).map_err(|e| filter_error(&e))?;
        Ok(ResolvedScreenerQuery {
            universe,
            filters,
            metric_keys,
            spec,
            to,
        })
    }
}

impl ResolvedScreenerQuery {
    /// Evaluates the filter on one instrument's bars (ascending, latest last).
    ///
    /// `Some(row)` when the instrument matches, carrying the latest value of every metric the
    /// filter reads; `None` when it does not.
    pub fn evaluate(
        &self,
        id: &InstrumentId,
        bars: &[Bar],
    ) -> Result<Option<ScreenerResultRow>, ErrorDetail> {
        if !evaluate_group(&self.filters, bars).map_err(|e| filter_error(&e))? {
            return Ok(None);
        }
        let metrics: BTreeMap<String, Option<Decimal>> =
            latest_metrics(&self.metric_keys, bars).map_err(|e| filter_error(&e))?;
        Ok(Some(ScreenerResultRow {
            instrument_id: id.clone(),
            metrics,
        }))
    }
}

// ---------------------------------------------------------------------------------------------
// Filter grammar: `{"operator": "AND"|"OR", "items": [predicate | nested group]}`.
// ---------------------------------------------------------------------------------------------

/// Why a filter was refused. `reason()` is the stable `context.reason` of the wire error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScreenerError {
    /// A malformed group, operator, op or value shape.
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
    /// A metric key a bar dataset cannot compute.
    #[error("unsupported metric: {0}")]
    UnsupportedMetric(String),
    /// A timeframe dimension the scan cannot satisfy.
    #[error("unsupported timeframe: {0}")]
    UnsupportedTimeframe(String),
}

impl ScreenerError {
    /// The stable `context.reason` for this error.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidFilter(_) => "invalid_filter",
            Self::UnsupportedMetric(_) => "unsupported_metric",
            Self::UnsupportedTimeframe(_) => "unsupported_timeframe",
        }
    }
}

/// The timeframe dimension of a predicate, as written on the wire (`1m`, `5m`, `60`, `1h`,
/// `1D`, `1W`, `1M`).
///
/// Spelling is case-sensitive where it matters: lower-case `m` is minutes, upper-case `M` is
/// months; a bare number is minutes (`"60"` is one hour).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Timeframe {
    /// One minute.
    M1,
    /// Five minutes.
    M5,
    /// Fifteen minutes.
    M15,
    /// Thirty minutes.
    M30,
    /// One hour.
    H1,
    /// Two hours.
    H2,
    /// Four hours.
    H4,
    /// One day.
    D1,
    /// One week.
    W1,
    /// One calendar month.
    Month1,
}

impl Timeframe {
    /// Parses a wire timeframe (`1m`, `5m`, `15m`, `30m`, `60`, `1h`, `2h`, `4h`, `1D`, `1W`,
    /// `1M`); anything else is [`ScreenerError::UnsupportedTimeframe`].
    pub fn parse(text: &str) -> Result<Self, ScreenerError> {
        let unsupported = || ScreenerError::UnsupportedTimeframe(text.to_owned());
        let digits = text.chars().take_while(|c| c.is_ascii_digit()).count();
        let (n, unit) = text.split_at(digits);
        let n: u32 = n.parse().map_err(|_| unsupported())?;
        match unit {
            "" | "m" => match n {
                1 => Ok(Self::M1),
                5 => Ok(Self::M5),
                15 => Ok(Self::M15),
                30 => Ok(Self::M30),
                60 => Ok(Self::H1),
                120 => Ok(Self::H2),
                240 => Ok(Self::H4),
                _ => Err(unsupported()),
            },
            "h" => match n {
                1 => Ok(Self::H1),
                2 => Ok(Self::H2),
                4 => Ok(Self::H4),
                _ => Err(unsupported()),
            },
            "D" | "d" if n == 1 => Ok(Self::D1),
            "W" | "w" if n == 1 => Ok(Self::W1),
            "M" | "mo" if n == 1 => Ok(Self::Month1),
            _ => Err(unsupported()),
        }
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H2 => "2h",
            Self::H4 => "4h",
            Self::D1 => "1D",
            Self::W1 => "1W",
            Self::Month1 => "1M",
        })
    }
}

impl From<Timeframe> for String {
    fn from(tf: Timeframe) -> Self {
        tf.to_string()
    }
}

impl TryFrom<String> for Timeframe {
    type Error = ScreenerError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

/// A filter group: `{"operator": "AND"|"OR", "items": [...]}`.
///
/// `AND` matches when every item matches, `OR` when at least one does; an empty group matches
/// every instrument.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScreenerFilterGroup {
    /// `AND` (every item) or `OR` (at least one).
    pub operator: String,
    /// Predicates and nested groups.
    pub items: Vec<ScreenerFilterItem>,
}

/// One entry of a group's `items`: a predicate or a nested group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScreenerFilterItem {
    /// A comparison of a metric against a threshold.
    Predicate(ScreenerPredicate),
    /// A nested group, evaluated with the same rules.
    Group(Box<ScreenerFilterGroup>),
}

/// One predicate: `{"key", "op", "value", "timeframe"?}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScreenerPredicate {
    /// Metric read on the left-hand side (`close`, `volume`, `SMA20`, ...).
    pub key: String,
    /// Comparison: `eq`, `ne`, `gt`, `gte`, `lt`, `lte` or `between`.
    pub op: String,
    /// Threshold: a number, a metric reference `{"key": "<metric>"}`, or (for `between`) a
    /// two-element array of those.
    pub value: Value,
    /// Optional timeframe dimension this predicate asks for; must match the scan's `tf`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeframe: Option<Timeframe>,
}

/// The metrics a bar dataset computes.
const BASE_METRICS: [&str; 5] = ["open", "high", "low", "close", "volume"];

/// The period of an `SMA<n>` metric key, or `None` when `key` is not one.
fn parse_sma(key: &str) -> Option<u32> {
    let digits = key.strip_prefix("SMA")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let period: u32 = digits.parse().ok()?;
    (period >= 1).then_some(period)
}

fn validate_metric_key(key: &str) -> Result<(), ScreenerError> {
    if BASE_METRICS.contains(&key) || parse_sma(key).is_some() || key == "RSI" {
        Ok(())
    } else {
        Err(ScreenerError::UnsupportedMetric(key.to_owned()))
    }
}

fn validate_op(op: &str) -> Result<(), ScreenerError> {
    match op {
        "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "between" => Ok(()),
        other => Err(ScreenerError::InvalidFilter(format!(
            "unknown op {other:?}; expected eq, ne, gt, gte, lt, lte or between"
        ))),
    }
}

/// A threshold is a number or a metric reference; both must name a computable metric.
fn validate_operand(value: &Value) -> Result<(), ScreenerError> {
    match value {
        Value::Number(_) => Ok(()),
        Value::Object(map) => {
            let key = map.get("key").and_then(Value::as_str).ok_or_else(|| {
                ScreenerError::InvalidFilter(
                    "a metric reference must be {\"key\": \"<metric>\"}".to_owned(),
                )
            })?;
            validate_metric_key(key)
        }
        other => Err(ScreenerError::InvalidFilter(format!(
            "value {other} is a number or a metric reference"
        ))),
    }
}

fn validate_value(op: &str, value: &Value) -> Result<(), ScreenerError> {
    if op == "between" {
        let bounds = value.as_array().filter(|a| a.len() == 2).ok_or_else(|| {
            ScreenerError::InvalidFilter("`between` needs [low, high] as its value".to_owned())
        })?;
        for bound in bounds {
            validate_operand(bound)?;
        }
        return Ok(());
    }
    validate_operand(value)
}

/// Validates a group: operators, ops, value shapes and metric keys, recursively.
///
/// Rejected before any bar is read, so a typo costs nothing.
pub fn validate_group(group: &ScreenerFilterGroup) -> Result<(), ScreenerError> {
    if group.operator != "AND" && group.operator != "OR" {
        return Err(ScreenerError::InvalidFilter(format!(
            "unknown operator {:?}; expected \"AND\" or \"OR\"",
            group.operator
        )));
    }
    for item in &group.items {
        match item {
            ScreenerFilterItem::Group(nested) => validate_group(nested)?,
            ScreenerFilterItem::Predicate(predicate) => {
                validate_metric_key(&predicate.key)?;
                validate_op(&predicate.op)?;
                validate_value(&predicate.op, &predicate.value)?;
            }
        }
    }
    Ok(())
}

/// Every metric key the group reads, in first-seen order, de-duplicated.
pub fn group_metric_keys(group: &ScreenerFilterGroup) -> Result<Vec<String>, ScreenerError> {
    let mut keys = Vec::new();
    collect_metric_keys(group, &mut keys);
    Ok(keys)
}

fn collect_metric_keys(group: &ScreenerFilterGroup, keys: &mut Vec<String>) {
    let push = |key: &str, keys: &mut Vec<String>| {
        if !keys.iter().any(|k| k == key) {
            keys.push(key.to_owned());
        }
    };
    for item in &group.items {
        match item {
            ScreenerFilterItem::Group(nested) => collect_metric_keys(nested, keys),
            ScreenerFilterItem::Predicate(predicate) => {
                push(&predicate.key, keys);
                for key in operand_metric_keys(&predicate.value) {
                    push(key, keys);
                }
            }
        }
    }
}

fn operand_metric_keys(value: &Value) -> Vec<&str> {
    match value {
        Value::Object(map) => map.get("key").and_then(Value::as_str).into_iter().collect(),
        Value::Array(bounds) => bounds.iter().flat_map(operand_metric_keys).collect(),
        _ => Vec::new(),
    }
}

/// Every timeframe dimension the group asks for, in first-seen order, de-duplicated.
pub fn group_timeframes(group: &ScreenerFilterGroup) -> Result<Vec<Timeframe>, ScreenerError> {
    let mut timeframes = Vec::new();
    collect_timeframes(group, &mut timeframes);
    Ok(timeframes)
}

fn collect_timeframes(group: &ScreenerFilterGroup, timeframes: &mut Vec<Timeframe>) {
    for item in &group.items {
        match item {
            ScreenerFilterItem::Group(nested) => collect_timeframes(nested, timeframes),
            ScreenerFilterItem::Predicate(predicate) => {
                if let Some(tf) = predicate.timeframe {
                    if !timeframes.contains(&tf) {
                        timeframes.push(tf);
                    }
                }
            }
        }
    }
}

fn to_decimal(number: &serde_json::Number) -> Result<Decimal, ScreenerError> {
    // `Number`'s Display is the exact shortest decimal the client sent (ryu), so no binary
    // float enters the comparison.
    Decimal::from_str(&number.to_string()).map_err(|_| {
        ScreenerError::InvalidFilter(format!("value {number} is not a representable decimal"))
    })
}

/// The latest value of one metric over the bars, or `None` while it is warming up.
pub(crate) fn metric_value(key: &str, bars: &[Bar]) -> Option<Decimal> {
    if let Some(period) = parse_sma(key) {
        let period = period as usize;
        if bars.len() < period {
            return None;
        }
        let window = &bars[bars.len() - period..];
        let sum: Decimal = window.iter().map(|bar| bar.close.0).sum();
        // Exact decimal mean: the sum over the period, at the period's scale.
        return Some(sum / Decimal::from(period as u32));
    }
    if key == "RSI" {
        if bars.len() < 15 {
            return None;
        }
        let period = 14;
        let window = &bars[bars.len() - (period + 1)..];
        let mut gains = Decimal::ZERO;
        let mut losses = Decimal::ZERO;
        for i in 1..=period {
            let diff = window[i].close.0 - window[i - 1].close.0;
            if diff > Decimal::ZERO {
                gains += diff;
            } else {
                losses -= diff;
            }
        }
        if losses == Decimal::ZERO {
            return Some(Decimal::from(100));
        }
        if gains == Decimal::ZERO {
            return Some(Decimal::ZERO);
        }
        let rs = gains / losses;
        return Some(Decimal::from(100) - (Decimal::from(100) / (Decimal::ONE + rs)));
    }
    let last = bars.last()?;
    match key {
        "open" => Some(last.open.0),
        "high" => Some(last.high.0),
        "low" => Some(last.low.0),
        "close" => Some(last.close.0),
        "volume" => Some(last.volume.0),
        _ => None,
    }
}

/// Evaluates one item: a nested group, or a predicate whose metrics are missing (still
/// warming up) counts as not matching rather than an error.
fn evaluate_item(item: &ScreenerFilterItem, bars: &[Bar]) -> Result<bool, ScreenerError> {
    match item {
        ScreenerFilterItem::Group(nested) => evaluate_group(nested, bars),
        ScreenerFilterItem::Predicate(predicate) => {
            let Some(left) = metric_value(&predicate.key, bars) else {
                return Ok(false);
            };
            if predicate.op == "between" {
                let bounds = predicate
                    .value
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or_else(|| {
                        ScreenerError::InvalidFilter("`between` needs [low, high]".to_owned())
                    })?;
                let (Some(low), Some(high)) = (
                    operand_value(&bounds[0], bars)?,
                    operand_value(&bounds[1], bars)?,
                ) else {
                    return Ok(false);
                };
                return Ok(low <= left && left <= high);
            }
            let Some(right) = operand_value(&predicate.value, bars)? else {
                return Ok(false);
            };
            Ok(match predicate.op.as_str() {
                "eq" => left == right,
                "ne" => left != right,
                "gt" => left > right,
                "gte" => left >= right,
                "lt" => left < right,
                "lte" => left <= right,
                other => {
                    return Err(ScreenerError::InvalidFilter(format!(
                        "unknown op {other:?}"
                    )));
                }
            })
        }
    }
}

/// A threshold: a constant, or a metric of the same bars (so it, too, may be warming up).
fn operand_value(value: &Value, bars: &[Bar]) -> Result<Option<Decimal>, ScreenerError> {
    match value {
        Value::Number(number) => to_decimal(number).map(Some),
        Value::Object(_) => {
            let key = value.get("key").and_then(Value::as_str).ok_or_else(|| {
                ScreenerError::InvalidFilter(
                    "a metric reference must be {\"key\": \"<metric>\"}".to_owned(),
                )
            })?;
            Ok(metric_value(key, bars))
        }
        other => Err(ScreenerError::InvalidFilter(format!(
            "value {other} is a number or a metric reference"
        ))),
    }
}

/// Evaluates a group over one instrument's bars (ascending, latest last).
pub fn evaluate_group(group: &ScreenerFilterGroup, bars: &[Bar]) -> Result<bool, ScreenerError> {
    match group.operator.as_str() {
        "AND" => {
            for item in &group.items {
                if !evaluate_item(item, bars)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        // An OR group matches as soon as one item does; an empty one matches everything.
        "OR" => {
            if group.items.is_empty() {
                return Ok(true);
            }
            for item in &group.items {
                if evaluate_item(item, bars)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        other => Err(ScreenerError::InvalidFilter(format!(
            "unknown operator {other:?}; expected \"AND\" or \"OR\""
        ))),
    }
}

/// The latest value of every metric key over the bars, by key; `None` while warming up.
pub fn latest_metrics(
    keys: &[String],
    bars: &[Bar],
) -> Result<BTreeMap<String, Option<Decimal>>, ScreenerError> {
    let mut metrics = BTreeMap::new();
    for key in keys {
        metrics.insert(key.clone(), metric_value(key, bars));
    }
    Ok(metrics)
}
