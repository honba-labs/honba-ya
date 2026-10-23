//! Step manifest (`step.toml`) parser and converter to [`StrategyManifest`].
//!
//! A `step.toml` declaratively describes a chainable trading step (screen, filter, signal,
//! size, risk, execute), matching `honba-strategies/schema/step.schema.json`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::manifest::{
    BarAggregation, ManifestError, ParameterSpec, StrategyManifest, Subscriptions, TimeframeSpec,
    Universe,
};

/// Step kind according to `step.schema.json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// Universe screening step.
    Screen,
    /// Candidate filtering step.
    Filter,
    /// Signal generation step.
    Signal,
    /// Position sizing step.
    Size,
    /// Risk management step.
    Risk,
    /// Order execution step.
    Execute,
}

/// A parameter declaration in `step.toml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepParam {
    /// Type of parameter: int, float, bool, str, enum, etc.
    pub r#type: String,
    /// Default value.
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    /// Whether the parameter is optional.
    #[serde(default)]
    pub optional: Option<bool>,
    /// Minimum numerical value.
    #[serde(default)]
    pub min: Option<f64>,
    /// Maximum numerical value.
    #[serde(default)]
    pub max: Option<f64>,
    /// Step size.
    #[serde(default)]
    pub step: Option<f64>,
    /// Enumeration choices.
    #[serde(default)]
    pub choices: Option<Vec<String>>,
    /// Regex pattern for strings.
    #[serde(default)]
    pub pattern: Option<String>,
    /// Human-readable description.
    pub description: String,
    /// Whether the parameter is sweepable.
    #[serde(default)]
    pub sweepable: Option<bool>,
}

/// History bars requirement in `step.toml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum HistoryBars {
    /// Fixed count of history bars.
    Count(u32),
    /// Parameter reference, e.g. `"$slow"`.
    Param(String),
}

/// Data requirements in `step.toml`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepRequires {
    /// OHLCV bars needed (e.g. `["close"]`).
    #[serde(default)]
    pub bars: Vec<String>,
    /// Fundamental data fields.
    #[serde(default)]
    pub fundamentals: Vec<String>,
    /// Indicators used.
    #[serde(default)]
    pub indicators: Vec<serde_json::Value>,
    /// History bars needed before first decision.
    #[serde(default)]
    pub history_bars: Option<HistoryBars>,
    /// Point-in-time requirement.
    #[serde(default)]
    pub pit: Option<bool>,
}

/// Input shape accepted by the step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum StepShapeOrList {
    /// Single accepted shape name.
    Single(String),
    /// Multiple accepted shape names.
    Multiple(Vec<String>),
}

/// Consumes declaration in `step.toml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepConsumes {
    /// Accepted data shape(s).
    pub shape: StepShapeOrList,
    /// Input columns.
    #[serde(default)]
    pub columns: Vec<String>,
}

/// Emits declaration in `step.toml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepEmits {
    /// Emitted data shape.
    pub shape: String,
    /// Added columns.
    #[serde(default)]
    pub adds: Vec<String>,
    /// Reported columns.
    #[serde(default)]
    pub report: Vec<String>,
    /// Optional reportable columns.
    #[serde(default)]
    pub reportable: Vec<String>,
}

/// Chain positioning hints in `step.toml`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepChain {
    /// Steps that can come after.
    #[serde(default)]
    pub after: Vec<String>,
    /// Steps that can come before.
    #[serde(default)]
    pub before: Vec<String>,
}

/// Full step manifest corresponding to `step.schema.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepManifest {
    /// Schema version (e.g. 1).
    pub schema: u32,
    /// Step identifier, e.g. `"signal.sma_crossover"`.
    pub id: String,
    /// Step kind.
    pub kind: StepKind,
    /// Human-readable title.
    pub title: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: Option<String>,
    /// Semantic search tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Warnings and pitfalls.
    #[serde(default)]
    pub pitfalls: Vec<String>,
    /// Parameter declarations.
    #[serde(default)]
    pub params: BTreeMap<String, StepParam>,
    /// Data requirements.
    #[serde(default)]
    pub requires: Option<StepRequires>,
    /// Consumed shapes.
    pub consumes: StepConsumes,
    /// Emitted shapes.
    pub emits: StepEmits,
    /// Chaining hints.
    #[serde(default)]
    pub chain: Option<StepChain>,
}

impl StepManifest {
    /// Parses a `step.toml` from a TOML string.
    pub fn from_toml_str(toml_str: &str) -> Result<Self, ManifestError> {
        toml::from_str(toml_str).map_err(|e| ManifestError::TomlParse(e.to_string()))
    }

    /// Converts this `StepManifest` into a canonical [`StrategyManifest`].
    pub fn to_strategy_manifest(&self) -> Result<StrategyManifest, ManifestError> {
        let mut hasher = Sha256::new();
        hasher.update(self.id.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        self.to_strategy_manifest_with_hash(&hash)
    }

    /// Converts this `StepManifest` into a canonical [`StrategyManifest`] using the given source hash.
    pub fn to_strategy_manifest_with_hash(
        &self,
        source_hash: &str,
    ) -> Result<StrategyManifest, ManifestError> {
        // Extract strategy name from id (e.g. "signal.sma_crossover" -> "sma_crossover")
        let name = if let Some((_kind, rest)) = self.id.split_once('.') {
            rest.to_string()
        } else {
            self.id.clone()
        };

        // Resolve warmup bars from requires.history_bars
        let mut warmup_bars = 0u32;
        if let Some(req) = &self.requires {
            if let Some(hb) = &req.history_bars {
                match hb {
                    HistoryBars::Count(n) => warmup_bars = *n,
                    HistoryBars::Param(p) => {
                        let param_name = p.strip_prefix('$').unwrap_or(p);
                        if let Some(param) = self.params.get(param_name) {
                            if let Some(def) = &param.default {
                                if let Some(n) = def.as_u64() {
                                    warmup_bars = n as u32;
                                } else if let Some(n) = def.as_i64() {
                                    if n > 0 {
                                        warmup_bars = n as u32;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Map params to ParameterSpec
        let mut parameters = BTreeMap::new();
        for (pname, p) in &self.params {
            let default_val = p.default.clone().unwrap_or(serde_json::Value::Null);
            let mut spec = ParameterSpec::new(default_val);
            if !p.description.is_empty() {
                spec = spec.with_description(&p.description);
            }
            if let (Some(min), Some(max), Some(step)) = (p.min, p.max, p.step) {
                spec = spec.with_bounds(min, max, step);
            } else {
                spec.min = p.min;
                spec.max = p.max;
                spec.step = p.step;
            }
            spec.validate(pname)?;
            parameters.insert(pname.clone(), spec);
        }

        let universe = Universe::Named("NSE_500".to_string());
        let driving_timeframe = TimeframeSpec::new(1, BarAggregation::Day);

        let mut manifest = StrategyManifest::new(name, source_hash, universe, driving_timeframe);
        manifest.warmup_bars = warmup_bars;
        manifest.parameters = parameters;
        manifest.subscriptions = Subscriptions {
            instruments: vec![honba_core::InstrumentId::new(honba_core::Exchange::Nse, "NIFTY50")],
            quotes: false,
            trades: false,
        };

        Ok(manifest)
    }
}
