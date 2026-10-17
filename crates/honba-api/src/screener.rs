//! Query resolution and per-instrument evaluation for `GET /screener/scan`.
//!
//! Pure like [`crate::market`]: the REST handler reads bars through its port and hands each
//! instrument's history to [`ResolvedScreenerQuery::evaluate`]; nothing here does I/O. The
//! predicate semantics live in `honba_indicators::screener`, which is pinned to the Python
//! reference evaluator by golden vectors.

use std::collections::BTreeMap;

use honba_entities::{ScreenerFilterGroup, Timeframe};
use honba_indicators::screener::{
    evaluate_group, group_metric_keys, group_timeframes, latest_metrics, validate_group,
    ScreenerError,
};
use honba_messages::{
    Bar, BarAggregation, BarSpecification, ErrorCode, ErrorDetail, InstrumentId, UnixNanos,
};
use serde_json::json;

use crate::market::{invalid, parse_bound, parse_instrument_id, parse_timeframe};
use crate::requests::ScreenerQuery;
use crate::responses::ScreenerResultRow;

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
    pub to: Option<UnixNanos>,
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
            return spec.aggregation() == BarAggregation::Month && spec.step() == 1
        }
    };
    let unit = match spec.aggregation() {
        BarAggregation::Second => 1,
        BarAggregation::Minute => 60,
        BarAggregation::Hour => 3_600,
        BarAggregation::Day => 86_400,
        BarAggregation::Week => 604_800,
        _ => return false,
    };
    let wanted = seconds.map(|s| s.saturating_mul(step as u64));
    wanted == Some(unit * spec.step() as u64)
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
    ids.sort();
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
        format!("the scan matched {rows} instruments, over the limit of {MAX_SCREENER_ROWS}; narrow the universe or the filter"),
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
            .map(|t| UnixNanos::from_u64(t.as_u64().saturating_add(1)));
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
                        "a predicate asks for timeframe {tf:?} but the scan evaluates `tf` bars; \
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
        let metrics: BTreeMap<String, Option<f64>> =
            latest_metrics(&self.metric_keys, bars).map_err(|e| filter_error(&e))?;
        Ok(Some(ScreenerResultRow {
            instrument_id: id.clone(),
            metrics,
        }))
    }
}
