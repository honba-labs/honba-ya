//! Query resolution and response mapping for the market-data read endpoints
//! (`/instruments`, `/bars/{id}`, `/quotes`, `/depth/{id}`).
//!
//! Everything here is pure: it turns the string-typed query DTOs into domain
//! values and reports a bad request as an [`ErrorDetail`] that names the
//! offending field in `context.field`, so every transport rejects the same way.

use chrono::{DateTime, NaiveDate};
use honba_entities::{Instrument, InstrumentKind};
use honba_messages::{
    BarAggregation, BarSpecification, ErrorCode, ErrorDetail, Exchange, InstrumentId, PriceType,
    UnixNanos,
};
use serde_json::{json, Value};

use crate::requests::{BarsQuery, DepthQuery, InstrumentsQuery, QuotesQuery};

/// The timeframe used when `tf` is omitted from a bars query.
pub const DEFAULT_TIMEFRAME: &str = "1m";

/// Levels per side returned by `GET /depth/{id}` when `depth` is omitted.
pub const DEFAULT_DEPTH_LEVELS: usize = 5;

/// The most levels per side `GET /depth/{id}` accepts.
pub const MAX_DEPTH_LEVELS: u32 = 50;

/// A [`QuotesQuery`] resolved into domain values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedQuotesQuery {
    /// Requested symbols, trimmed, de-duplicated, in request order.
    pub symbols: Vec<String>,
    /// Exchange the symbols are restricted to, if any.
    pub venue: Option<Exchange>,
    /// Inclusive point in time the quotes are known at; `None` means the latest.
    pub as_of: Option<UnixNanos>,
}

/// A [`BarsQuery`] resolved into domain values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedBarsQuery {
    /// Bar specification (always last-price).
    pub spec: BarSpecification,
    /// Inclusive start, if bounded.
    pub from: Option<UnixNanos>,
    /// Exclusive end, if bounded.
    pub to: Option<UnixNanos>,
}

pub(crate) fn invalid(field: &str, reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
        .with_context(json!({"field": field, "reason": reason}))
}

/// Parses a timeframe such as `30s`, `1m`, `4h`, `1d`, `2w` or `1mo`.
///
/// The step is a positive integer and the unit is lower-case; the result is a
/// last-price [`BarSpecification`].
pub fn parse_timeframe(text: &str) -> Result<BarSpecification, ErrorDetail> {
    let bad = || {
        invalid(
            "tf",
            "invalid_timeframe",
            format!("timeframe {text:?} is not <n><s|m|h|d|w|mo>, e.g. 1m or 1d"),
        )
    };
    let digits = text.chars().take_while(char::is_ascii_digit).count();
    let (step, unit) = text.split_at(digits);
    let step: usize = step.parse().map_err(|_| bad())?;
    if step == 0 {
        return Err(bad());
    }
    let aggregation = match unit {
        "s" => BarAggregation::Second,
        "m" => BarAggregation::Minute,
        "h" => BarAggregation::Hour,
        "d" => BarAggregation::Day,
        "w" => BarAggregation::Week,
        "mo" => BarAggregation::Month,
        _ => return Err(bad()),
    };
    Ok(BarSpecification::new(step, aggregation, PriceType::Last))
}

pub(crate) fn parse_bound(field: &str, text: &str) -> Result<UnixNanos, ErrorDetail> {
    let bad = || {
        invalid(
            field,
            "invalid_time",
            format!("{field} {text:?} is not an RFC3339 time or a YYYY-MM-DD date"),
        )
    };
    let instant = match DateTime::parse_from_rfc3339(text) {
        Ok(parsed) => parsed.to_utc(),
        Err(_) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(|_| bad())?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(bad)?
            .and_utc(),
    };
    let nanos = instant.timestamp_nanos_opt().ok_or_else(bad)?;
    u64::try_from(nanos).map(UnixNanos::from_u64).map_err(|_| {
        invalid(
            field,
            "before_epoch",
            format!("{field} {text:?} is before 1970-01-01"),
        )
    })
}

impl BarsQuery {
    /// Resolves the query: default timeframe, parsed bounds, and a non-empty range.
    pub fn resolve(&self) -> Result<ResolvedBarsQuery, ErrorDetail> {
        let spec = parse_timeframe(self.tf.as_deref().unwrap_or(DEFAULT_TIMEFRAME))?;
        let from = self
            .from
            .as_deref()
            .map(|text| parse_bound("from", text))
            .transpose()?;
        let to = self
            .to
            .as_deref()
            .map(|text| parse_bound("to", text))
            .transpose()?;
        if let (Some(from), Some(to)) = (from, to) {
            if from >= to {
                return Err(invalid(
                    "to",
                    "empty_range",
                    "`from` must be strictly before `to`",
                ));
            }
        }
        Ok(ResolvedBarsQuery { spec, from, to })
    }
}

impl InstrumentsQuery {
    /// Reports whether `id` passes every filter that is set (exact, case-sensitive).
    pub fn matches(&self, id: &InstrumentId) -> bool {
        self.exchange
            .as_deref()
            .map_or(true, |exchange| id.exchange().as_str() == exchange)
            && self
                .symbol
                .as_deref()
                .map_or(true, |symbol| id.symbol() == symbol)
    }
}

/// Parses the `{id}` path segment, `SYMBOL.EXCHANGE`, splitting on the last dot.
pub fn parse_instrument_id(text: &str) -> Result<InstrumentId, ErrorDetail> {
    match text.rsplit_once('.') {
        Some((symbol, exchange)) if !symbol.is_empty() && !exchange.is_empty() => {
            Ok(InstrumentId::new(symbol, Exchange::new(exchange)))
        }
        _ => Err(invalid(
            "id",
            "invalid_instrument_id",
            format!("instrument id {text:?} is not SYMBOL.EXCHANGE"),
        )),
    }
}

/// Maps an [`Instrument`] to its wire object.
pub fn instrument_json(instrument: &Instrument) -> Value {
    let kind = match instrument.kind() {
        InstrumentKind::Equity => "equity",
        InstrumentKind::Etf => "etf",
        InstrumentKind::Bond => "bond",
        InstrumentKind::Ipo => "ipo",
        InstrumentKind::Future => "future",
        InstrumentKind::Option => "option",
        InstrumentKind::Fx => "fx",
        InstrumentKind::Index => "index",
        InstrumentKind::MutualFund => "mutual_fund",
        _ => "other",
    };
    json!({
        "id": instrument.id(),
        "kind": kind,
        "currency": instrument.currency(),
        "lot_size": instrument.lot_size(),
        "tick_size": instrument.tick_size(),
    })
}

impl QuotesQuery {
    /// Resolves the query: at least one symbol, an optional venue, an optional `as_of` time.
    pub fn resolve(&self) -> Result<ResolvedQuotesQuery, ErrorDetail> {
        let mut symbols: Vec<String> = Vec::new();
        for symbol in self.symbols.as_deref().unwrap_or_default().split(',') {
            let symbol = symbol.trim();
            if !symbol.is_empty() && !symbols.iter().any(|seen| seen == symbol) {
                symbols.push(symbol.to_owned());
            }
        }
        if symbols.is_empty() {
            return Err(invalid(
                "symbols",
                "missing_symbols",
                "`symbols` must name at least one symbol, comma-separated",
            ));
        }
        let as_of = self
            .as_of
            .as_deref()
            .map(|text| parse_bound("as_of", text))
            .transpose()?;
        Ok(ResolvedQuotesQuery {
            symbols,
            venue: self.venue.as_deref().map(Exchange::new),
            as_of,
        })
    }
}

impl DepthQuery {
    /// Resolves the number of levels per side: the default, or `1..=MAX_DEPTH_LEVELS`.
    pub fn levels(&self) -> Result<usize, ErrorDetail> {
        match self.depth {
            None => Ok(DEFAULT_DEPTH_LEVELS),
            Some(levels) if (1..=MAX_DEPTH_LEVELS).contains(&levels) => Ok(levels as usize),
            Some(levels) => Err(invalid(
                "depth",
                "out_of_range",
                format!("depth {levels} is not between 1 and {MAX_DEPTH_LEVELS}"),
            )),
        }
    }
}
