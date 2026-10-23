//! The strategy manifest: the single unit a backtest, sweep, live run, MCP
//! call, and frontend request all name (docs/archive/plan.md E0-S8).
//!
//! A strategy written in Python or TypeScript is turned into a manifest *once*,
//! by a verify step, and everything downstream consumes the manifest rather
//! than the source. That is what makes backtest and live the same run: they
//! read identical subscriptions, universe, and timeframe, so they cannot
//! silently diverge.
//!
//! The manifest is deliberately data, not behaviour. It carries no code and no
//! strategy parameters — those live in the strategy — so it can be hashed,
//! stored, diffed, and sent over a wire.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use honba_core::InstrumentId;

/// A bar aggregation unit, for [`TimeframeSpec`].
///
/// `honba_messages::BarAggregation` (the pre-migration type this was ported
/// from) no longer exists; `honba-core` has no replacement either, since bar
/// aggregation granularity is a strategy-manifest concept, not a shared wire
/// type every subsystem needs. Defined locally here instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BarAggregation {
    /// Second-based bars.
    Second,
    /// Minute-based bars.
    Minute,
    /// Hour-based bars.
    Hour,
    /// Day-based bars.
    Day,
    /// Week-based bars.
    Week,
}

/// Version of the strategy contract a manifest was compiled against.
///
/// Bumping this means the strategy's hook set or context changed. It is
/// separate from the wire `SCHEMA_VERSION`: a strategy can be rebuilt for a
/// newer contract without the envelope moving.
pub const STRATEGY_API_VERSION: &str = "1.0.0";

/// A verified, hashable description of a strategy, ready to be run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategyManifest {
    /// Strategy contract version this was compiled against.
    pub api_version: String,
    /// Strategy's declared name, used in journals and reports.
    pub name: String,
    /// Stable digest of the source that produced this manifest.
    ///
    /// Two runs with the same `source_hash` and the same data must produce a
    /// byte-identical journal; a changed hash is what tells a cache it cannot
    /// reuse a previous result.
    pub source_hash: String,
    /// The universe the strategy may trade, as a named set or explicit list.
    pub universe: Universe,
    /// What the strategy needs fed to it.
    pub subscriptions: Subscriptions,
    /// The bar timeframe that drives the strategy's clock.
    pub driving_timeframe: TimeframeSpec,
    /// Bars required before the first decision, so indicators converge.
    pub warmup_bars: u32,
    /// Optional wall-clock schedules (session open/close, rebalance times).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub schedules: BTreeMap<String, String>,
    /// Optional parameter declarations defining defaults and search spaces (E4-S2).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, ParameterSpec>,
}

/// A parameter declaration with a default value and optional sweep range (E4-S2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParameterSpec {
    /// Default value for the parameter.
    pub default: serde_json::Value,
    /// Search range expression: "min:max:step" (e.g. "10:50:5").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Minimum value (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum value (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Step size between trials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,
    /// Optional documentation description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ParameterSpec {
    /// Builds a parameter specification with a default value.
    pub fn new(default: impl Into<serde_json::Value>) -> Self {
        Self {
            default: default.into(),
            range: None,
            min: None,
            max: None,
            step: None,
            description: None,
        }
    }

    /// Sets the range expression string ("min:max:step").
    #[must_use]
    pub fn with_range(mut self, range: impl Into<String>) -> Self {
        self.range = Some(range.into());
        self
    }

    /// Sets explicit numerical bounds and step size.
    #[must_use]
    pub fn with_bounds(mut self, min: f64, max: f64, step: f64) -> Self {
        self.min = Some(min);
        self.max = Some(max);
        self.step = Some(step);
        self.range = Some(format!("{min}:{max}:{step}"));
        self
    }

    /// Sets a documentation description.
    #[must_use]
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Parses a "min:max:step" string expression.
    pub fn parse_range_str(range_str: &str) -> Result<(f64, f64, f64), ManifestError> {
        let parts: Vec<&str> = range_str.split(':').map(str::trim).collect();
        if parts.len() != 3 {
            return Err(ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("expected min:max:step format, got {range_str:?}"),
            });
        }
        let min: f64 = parts[0]
            .parse()
            .map_err(|e| ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("invalid min in {range_str:?}: {e}"),
            })?;
        let max: f64 = parts[1]
            .parse()
            .map_err(|e| ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("invalid max in {range_str:?}: {e}"),
            })?;
        let step: f64 = parts[2]
            .parse()
            .map_err(|e| ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("invalid step in {range_str:?}: {e}"),
            })?;
        if min > max {
            return Err(ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("min ({min}) cannot be greater than max ({max})"),
            });
        }
        if step <= 0.0 {
            return Err(ManifestError::InvalidParameterRange {
                name: String::new(),
                reason: format!("step ({step}) must be greater than 0"),
            });
        }
        Ok((min, max, step))
    }

    /// Validates internal consistency and ensures default is within [min, max].
    pub fn validate(&self, name: &str) -> Result<(), ManifestError> {
        let (min, max, step) = if let Some(ref r) = self.range {
            let (min, max, step) = Self::parse_range_str(r).map_err(|e| match e {
                ManifestError::InvalidParameterRange { reason, .. } => {
                    ManifestError::InvalidParameterRange {
                        name: name.to_string(),
                        reason,
                    }
                }
                other => other,
            })?;
            (Some(min), Some(max), Some(step))
        } else {
            (self.min, self.max, self.step)
        };

        if let (Some(min), Some(max)) = (min, max) {
            if min > max {
                return Err(ManifestError::InvalidParameterRange {
                    name: name.to_string(),
                    reason: format!("min ({min}) cannot be greater than max ({max})"),
                });
            }
        }
        if let Some(step) = step {
            if step <= 0.0 {
                return Err(ManifestError::InvalidParameterRange {
                    name: name.to_string(),
                    reason: format!("step ({step}) must be positive"),
                });
            }
        }

        // Validate default value against bounds
        if let Some(def_num) = self.default.as_f64() {
            if let Some(min) = min {
                if def_num < min {
                    return Err(ManifestError::ParameterDefaultOutOfRange {
                        name: name.to_string(),
                        default: self.default.to_string(),
                        min: min.to_string(),
                        max: max.map_or_else(|| "inf".to_string(), |m| m.to_string()),
                    });
                }
            }
            if let Some(max) = max {
                if def_num > max {
                    return Err(ManifestError::ParameterDefaultOutOfRange {
                        name: name.to_string(),
                        default: self.default.to_string(),
                        min: min.map_or_else(|| "-inf".to_string(), |m| m.to_string()),
                        max: max.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Computes the discrete parameter values across the declared range.
    pub fn discrete_values(&self) -> Vec<serde_json::Value> {
        let (min, max, step) = if let Some(ref r) = self.range {
            if let Ok(triple) = Self::parse_range_str(r) {
                (Some(triple.0), Some(triple.1), Some(triple.2))
            } else {
                (self.min, self.max, self.step)
            }
        } else {
            (self.min, self.max, self.step)
        };

        if let (Some(min), Some(max), Some(step)) = (min, max, step) {
            let is_integer = self.default.is_i64()
                || self.default.is_u64()
                || (min.fract() == 0.0 && max.fract() == 0.0 && step.fract() == 0.0);
            let mut values = Vec::new();
            let mut curr = min;
            while curr <= max + step * 1e-6 {
                if is_integer {
                    values.push(serde_json::json!(curr.round() as i64));
                } else {
                    values.push(serde_json::json!((curr * 1e6).round() / 1e6));
                }
                curr += step;
            }
            if !values.is_empty() {
                return values;
            }
        }
        vec![self.default.clone()]
    }
}

/// Constructs a [`ParameterSpec`] from a range expression like `"10:50:5"` and a default value.
pub fn param_range_to_schema(
    range: &str,
    default: serde_json::Value,
) -> Result<ParameterSpec, ManifestError> {
    let (min, max, step) = ParameterSpec::parse_range_str(range)?;
    let spec = ParameterSpec {
        default,
        range: Some(range.to_string()),
        min: Some(min),
        max: Some(max),
        step: Some(step),
        description: None,
    };
    spec.validate("param")?;
    Ok(spec)
}

/// Extracts discrete parameter combinations / search space from a strategy manifest.
pub fn sweep_space_from_manifest(
    manifest: &StrategyManifest,
) -> Result<BTreeMap<String, Vec<serde_json::Value>>, ManifestError> {
    manifest.validate()?;
    let mut space = BTreeMap::new();
    for (name, spec) in &manifest.parameters {
        space.insert(name.clone(), spec.discrete_values());
    }
    Ok(space)
}

/// Generates the Cartesian product grid of parameter assignments from a strategy manifest.
pub fn parameter_grid_from_manifest(
    manifest: &StrategyManifest,
) -> Result<Vec<BTreeMap<String, serde_json::Value>>, ManifestError> {
    let space = sweep_space_from_manifest(manifest)?;
    if space.is_empty() {
        return Ok(Vec::new());
    }
    let mut grid: Vec<BTreeMap<String, serde_json::Value>> = vec![BTreeMap::new()];
    for (name, values) in space {
        let mut next = Vec::new();
        for entry in &grid {
            for v in &values {
                let mut updated = entry.clone();
                updated.insert(name.clone(), v.clone());
                next.push(updated);
            }
        }
        grid = next;
    }
    Ok(grid)
}

/// The set of instruments a strategy may trade.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Universe {
    /// A named index or curated list resolved by the broker adapter.
    Named(String),
    /// An explicit list of instruments.
    Explicit(Vec<InstrumentId>),
}

/// The market data a strategy subscribes to.
///
/// A strategy that forgets a subscription gets no events for it, which in a
/// backtest looks like a strategy that never trades. Recording the set
/// explicitly is what lets the loader fail loudly instead.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Subscriptions {
    /// Instruments whose bars this strategy consumes.
    pub instruments: Vec<InstrumentId>,
    /// Whether top-of-book quotes are delivered.
    #[serde(default)]
    pub quotes: bool,
    /// Whether trade prints are delivered.
    #[serde(default)]
    pub trades: bool,
}

impl Subscriptions {
    /// Returns true when this subscription set mentions `id` in any channel.
    pub fn covers(&self, id: &InstrumentId) -> bool {
        self.instruments.contains(id)
    }
}

/// A bar aggregation interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TimeframeSpec {
    /// Number of units per bar, e.g. 5 in `5m`.
    pub interval: u32,
    /// The unit the interval counts.
    pub aggregation: BarAggregation,
}

impl TimeframeSpec {
    /// Builds a timeframe.
    pub const fn new(interval: u32, aggregation: BarAggregation) -> Self {
        Self {
            interval,
            aggregation,
        }
    }
}

impl StrategyManifest {
    /// Starts a manifest, filling in the current contract version.
    pub fn new(
        name: impl Into<String>,
        source_hash: impl Into<String>,
        universe: Universe,
        driving_timeframe: TimeframeSpec,
    ) -> Self {
        Self {
            api_version: STRATEGY_API_VERSION.to_string(),
            name: name.into(),
            source_hash: source_hash.into(),
            universe,
            subscriptions: Subscriptions::default(),
            driving_timeframe,
            warmup_bars: 0,
            schedules: BTreeMap::new(),
            parameters: BTreeMap::new(),
        }
    }

    /// Sets the instruments and channels this strategy consumes.
    #[must_use]
    pub fn with_subscriptions(mut self, subscriptions: Subscriptions) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// Sets the number of warm-up bars.
    #[must_use]
    pub fn with_warmup_bars(mut self, warmup_bars: u32) -> Self {
        self.warmup_bars = warmup_bars;
        self
    }

    /// Adds one named schedule entry.
    #[must_use]
    pub fn with_schedule(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.schedules.insert(key.into(), value.into());
        self
    }

    /// Adds a parameter declaration.
    #[must_use]
    pub fn with_parameter(mut self, name: impl Into<String>, spec: ParameterSpec) -> Self {
        self.parameters.insert(name.into(), spec);
        self
    }

    /// Every instrument this strategy may act on.
    ///
    /// The union of the explicit subscription list and, for a named universe,
    /// whatever the loader resolved the name to. Verification uses this to
    /// reject a strategy that subscribes to something outside its universe.
    pub fn instruments(&self) -> Vec<InstrumentId> {
        let mut out = self.subscriptions.instruments.clone();
        if let Universe::Explicit(list) = &self.universe {
            for id in list {
                if !out.contains(id) {
                    out.push(id.clone());
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// Checks the manifest's internal consistency.
    ///
    /// Run before a run starts, so an unusable manifest is rejected at the door
    /// rather than producing an empty journal that looks like a strategy that
    /// did nothing.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.name.trim().is_empty() {
            return Err(ManifestError::EmptyName);
        }
        if self.source_hash.trim().is_empty() {
            return Err(ManifestError::EmptySourceHash);
        }
        if self.api_version != STRATEGY_API_VERSION {
            return Err(ManifestError::UnsupportedApiVersion {
                found: self.api_version.clone(),
            });
        }
        if self.driving_timeframe.interval == 0 {
            return Err(ManifestError::ZeroInterval);
        }
        // A named universe with no resolved instruments would silently trade
        // nothing; the loader must fill the subscription list first.
        if matches!(self.universe, Universe::Named(_)) && self.subscriptions.instruments.is_empty()
        {
            return Err(ManifestError::NamedUniverseUnresolved);
        }
        // Validate each declared parameter and check default is within bounds
        for (name, spec) in &self.parameters {
            spec.validate(name)?;
        }
        Ok(())
    }

    /// Parses a `step.toml` string directly into a [`StrategyManifest`].
    pub fn from_step_toml_str(toml_str: &str) -> Result<Self, ManifestError> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(toml_str.as_bytes());
        let source_hash = format!("{:x}", hasher.finalize());

        let step = crate::step::StepManifest::from_toml_str(toml_str)?;
        step.to_strategy_manifest_with_hash(&source_hash)
    }
}

/// Why a manifest was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ManifestError {
    /// The strategy has no name.
    EmptyName,
    /// The source digest is missing, so runs cannot be keyed to code.
    EmptySourceHash,
    /// The manifest targets a contract version this build does not implement.
    UnsupportedApiVersion {
        /// The version found in the manifest.
        found: String,
    },
    /// A bar timeframe of zero would never produce a bar.
    ZeroInterval,
    /// A named universe was not resolved to concrete instruments.
    NamedUniverseUnresolved,
    /// Parameter range expression was malformed or inconsistent.
    InvalidParameterRange {
        /// Name of the offending parameter.
        name: String,
        /// Description of the issue.
        reason: String,
    },
    /// A parameter's default value fell outside its declared sweep range.
    ParameterDefaultOutOfRange {
        /// Name of the offending parameter.
        name: String,
        /// Default value string representation.
        default: String,
        /// Minimum allowed string representation.
        min: String,
        /// Maximum allowed string representation.
        max: String,
    },
    /// Failed to parse TOML.
    TomlParse(String),
    /// Step manifest structure is invalid.
    InvalidStep(String),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "strategy name must not be empty"),
            Self::EmptySourceHash => write!(f, "source_hash must not be empty"),
            Self::UnsupportedApiVersion { found } => {
                write!(f, "unsupported strategy api_version: {found}")
            }
            Self::ZeroInterval => write!(f, "timeframe interval must be > 0"),
            Self::NamedUniverseUnresolved => {
                write!(
                    f,
                    "named universe must be resolved to instruments before running"
                )
            }
            Self::InvalidParameterRange { name, reason } => {
                write!(f, "parameter {name:?} has invalid range: {reason}")
            }
            Self::ParameterDefaultOutOfRange {
                name,
                default,
                min,
                max,
            } => {
                write!(
                    f,
                    "parameter {name:?} default {default} is outside range [{min}, {max}]"
                )
            }
            Self::TomlParse(msg) => write!(f, "failed to parse TOML: {msg}"),
            Self::InvalidStep(msg) => write!(f, "invalid step manifest: {msg}"),
        }
    }
}

impl std::error::Error for ManifestError {}
