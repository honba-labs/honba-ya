//! `BacktestRequest::resolve` and `SweepRequest::resolve` (ADR 0017 decision 6).
//!
//! Pure, like `BarsQuery::resolve`: the wire DTOs keep every field optional, and the
//! strictness lives here. A missing or invalid field is `422 validation_invalid_request`
//! with `context.field`. Strategy lookup (catalog or registered factory) is a submit-time
//! step in the REST layer; [`unknown_strategy`] is its shared error.

use honba_messages::{ErrorDetail, UnixNanos};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::market::{invalid, parse_bound, parse_timeframe};
use crate::requests::{BacktestRequest, SweepRequest};

/// Bar specification used when `bar_spec` is omitted.
pub const DEFAULT_BAR_SPEC: &str = "1d";

/// Fill models a [`BacktestRequest`] may name, in the server's own spellings.
///
/// `honba-api` sits above `honba-config` in the layering and cannot read
/// `honba_config::FillModel`, so this list is pinned equal to it by a test in
/// `honba-api-rest` (which can read both). A name outside the list is a 422 at submit.
pub const FILL_MODELS: [&str; 4] = ["bar_fill", "bar_fill_through", "l2", "none"];

/// Initial capital used when `initial_capital` is omitted.
///
/// Mirrors `honba_config::AccountConfig::default().starting_cash`; `honba-api` sits above
/// `honba-config` in the layering and cannot read it, so the REST crate pins the two equal.
pub const DEFAULT_INITIAL_CAPITAL: f64 = 1_000_000.0;

/// Largest flat commission per fill a [`BacktestRequest`] may name, equal to
/// `honba_sim::bar_fill::MAX_FLAT_COST`.
///
/// `honba-api` sits above `honba-sim` in the layering and cannot read the constant, so a test in
/// `honba-api-rest` (which can read both) pins the two equal. A value outside the range is a 422
/// at submit, never a silent clamp.
pub const MAX_COMMISSION_PER_FILL: f64 = 1e9;

/// Largest slippage, in basis points, a [`BacktestRequest`] may name (100% of the fill price).
///
/// `SlippageModel` itself bounds nothing, so this is the API's own guard against a fill whose
/// price overflows; it is likewise enforced at submit.
pub const MAX_SLIPPAGE_BPS: f64 = 10_000.0;

/// A [`BacktestRequest`] with every required field present and valid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedBacktest {
    /// Strategy content id or registered name (not yet looked up).
    pub strategy: String,
    /// Universe of instruments.
    pub universe: String,
    /// Inclusive start.
    pub start: UnixNanos,
    /// Exclusive end, strictly after `start`.
    pub end: UnixNanos,
    /// Bar specification text, e.g. `1d`.
    pub bar_spec: String,
    /// Initial capital, finite and positive.
    pub initial_capital: f64,
    /// Seed, non-zero.
    pub seed: u64,
    /// Fill model this run asked for, if any (see [`FILL_MODELS`]). `None` means the request did
    /// not override the server's configured model. Omitted from a manifest when unset, so an old
    /// manifest still parses and a new optional field does not bump the manifest version
    /// (ADR 0012 rule 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_model: Option<String>,
    /// Flat commission charged on every fill, in the settlement currency; `0.0` charges none.
    /// Omitted when zero so a manifest written before this field still round-trips (ADR 0012
    /// rule 1).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub commission_per_fill: f64,
    /// Slippage in basis points, adverse to the order's side; `0.0` models none. Omitted when
    /// zero, as above.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub slippage_bps: f64,
}

/// A [`SweepRequest`] with every required field present and valid.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedSweep {
    /// Strategy content id or registered name (not yet looked up).
    pub strategy: String,
    /// Parameter ranges: a non-empty JSON object.
    pub params: Value,
    /// Number of trials, at least one.
    pub trials: u64,
    /// Seed, non-zero.
    pub seed: u64,
    /// Universe of instruments.
    pub universe: String,
    /// Inclusive start.
    pub start: UnixNanos,
    /// Exclusive end, strictly after `start`.
    pub end: UnixNanos,
    /// Bar specification text, e.g. `1d`.
    pub bar_spec: String,
    /// Initial capital, finite and positive.
    pub initial_capital: f64,
}

/// A resolved run request of either kind (what a manifest pins).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolvedRequest {
    /// A single backtest.
    Backtest(ResolvedBacktest),
    /// A parameter sweep.
    Sweep(ResolvedSweep),
}

/// The error for a `strategy` that is neither a catalog content id nor a registered name.
pub fn unknown_strategy(strategy: &str) -> ErrorDetail {
    invalid(
        "strategy",
        "unknown_strategy",
        format!("strategy {strategy:?} is not in the catalog or the registered strategies"),
    )
}

fn required<'a>(field: &str, value: &'a Option<String>) -> Result<&'a str, ErrorDetail> {
    match value.as_deref().map(str::trim) {
        Some(text) if !text.is_empty() => Ok(text),
        Some(_) => Err(invalid(
            field,
            "empty",
            format!("{field} must not be empty"),
        )),
        None => Err(invalid(field, "missing", format!("{field} is required"))),
    }
}

fn seed(value: Option<u64>) -> Result<u64, ErrorDetail> {
    match value {
        None => Err(invalid("seed", "missing", "seed is required")),
        Some(0) => Err(invalid("seed", "zero_seed", "seed must be non-zero")),
        Some(seed) => Ok(seed),
    }
}

/// `skip_serializing_if` for a resolved money/rate field: an unset knob is omitted so a manifest
/// written before the field existed still round-trips.
fn is_zero(value: &f64) -> bool {
    *value == 0.0
}

/// Resolves an optional non-negative money/rate knob: `None` is `0.0`, and anything outside
/// `0..=max` (or non-finite) is a 422 naming `field`.
fn bounded(field: &str, value: Option<f64>, max: f64) -> Result<f64, ErrorDetail> {
    match value {
        None => Ok(0.0),
        Some(v) if v.is_finite() && (0.0..=max).contains(&v) => Ok(v),
        Some(_) => Err(invalid(
            field,
            "out_of_range",
            format!("{field} must be a finite number in 0..={max}"),
        )),
    }
}

impl BacktestRequest {
    /// Resolves a submit: required fields present, times parsed, defaults applied.
    pub fn resolve(&self) -> Result<ResolvedBacktest, ErrorDetail> {
        let seed = seed(self.seed)?;
        let strategy = required("strategy", &self.strategy)?.to_owned();
        let universe = required("universe", &self.universe)?.to_owned();
        let start = parse_bound("start", required("start", &self.start)?)?;
        let end = parse_bound("end", required("end", &self.end)?)?;
        if start >= end {
            return Err(invalid(
                "end",
                "empty_range",
                "`start` must be strictly before `end`",
            ));
        }
        let bar_spec = self
            .bar_spec
            .as_deref()
            .map_or(DEFAULT_BAR_SPEC, str::trim)
            .to_owned();
        parse_timeframe(&bar_spec)
            .map_err(|e| invalid("bar_spec", "invalid_bar_spec", e.message))?;
        let initial_capital = self.initial_capital.unwrap_or(DEFAULT_INITIAL_CAPITAL);
        if !(initial_capital.is_finite() && initial_capital > 0.0) {
            return Err(invalid(
                "initial_capital",
                "not_positive",
                "initial_capital must be a finite positive number",
            ));
        }
        let fill_model = match self.fill_model.as_deref().map(str::trim) {
            None => None,
            Some("") => {
                return Err(invalid(
                    "fill_model",
                    "empty",
                    "fill_model must not be empty",
                ));
            }
            Some(name) if FILL_MODELS.contains(&name) => Some(name.to_owned()),
            Some(name) => {
                return Err(invalid(
                    "fill_model",
                    "unknown_fill_model",
                    format!(
                        "unknown fill_model {name:?}; expected one of {}",
                        FILL_MODELS.join(", ")
                    ),
                ));
            }
        };
        let commission_per_fill = bounded(
            "commission_per_fill",
            self.commission_per_fill,
            MAX_COMMISSION_PER_FILL,
        )?;
        let slippage_bps = bounded("slippage_bps", self.slippage_bps, MAX_SLIPPAGE_BPS)?;
        Ok(ResolvedBacktest {
            strategy,
            universe,
            start,
            end,
            bar_spec,
            initial_capital,
            seed,
            fill_model,
            commission_per_fill,
            slippage_bps,
        })
    }
}

impl SweepRequest {
    /// Resolves a submit: required fields present and valid.
    pub fn resolve(&self) -> Result<ResolvedSweep, ErrorDetail> {
        let seed = seed(self.seed)?;
        let strategy = required("strategy", &self.strategy)?.to_owned();
        let universe = required("universe", &self.universe)?.to_owned();
        let start = required("start", &self.start)?.to_owned();
        let end = required("end", &self.end)?.to_owned();
        let bar_spec = self.bar_spec.as_deref().map_or("1d", str::trim).to_owned();
        let initial_capital = self.initial_capital.unwrap_or(1_000_000.0);

        let params = match &self.params {
            None => return Err(invalid("params", "missing", "params is required")),
            Some(Value::Object(map)) if !map.is_empty() => Value::Object(map.clone()),
            Some(_) => {
                return Err(invalid(
                    "params",
                    "not_an_object",
                    "params must be a non-empty JSON object of name to [low, high, step]",
                ))
            }
        };
        let trials = match self.trials {
            None => return Err(invalid("trials", "missing", "trials is required")),
            Some(0) => return Err(invalid("trials", "zero", "trials must be at least 1")),
            Some(n) => n,
        };

        let start_ns = crate::market::parse_bound("start", &start)?;
        let end_ns = crate::market::parse_bound("end", &end)?;
        if start_ns >= end_ns {
            return Err(crate::market::invalid(
                "end",
                "empty_range",
                "`start` must be strictly before `end`",
            ));
        }

        Ok(ResolvedSweep {
            strategy,
            params,
            trials,
            seed,
            universe,
            start: start_ns,
            end: end_ns,
            bar_spec,
            initial_capital,
        })
    }
}
