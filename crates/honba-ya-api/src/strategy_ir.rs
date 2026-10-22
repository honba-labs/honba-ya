//! Strategy IR: the verified form of a strategy manifest (ADR 0010).
//!
//! [`StrategyManifest`] is the input of `POST /strategies/verify` and
//! `POST /strategies`; [`StrategyIr`] is what a server keeps, runs and streams
//! thereafter. [`StrategyIr::compile`] is pure and total: a manifest either
//! compiles or fails with a stable [`IrError`] whose [`IrError::code`] every
//! transport reports verbatim, so callers branch without parsing prose.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use honba_core::InstrumentId;

use crate::wire::BarAggregation;

fn default_api_version() -> String {
    "1.0.0".to_owned()
}

/// A strategy manifest: the wire input of the strategy endpoints.
///
/// The manifest is an *input* (ADR 0012), so unknown fields are rejected.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategyManifest {
    /// Manifest schema version; defaults to `1.0.0` when omitted.
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Strategy name; must be non-empty after trimming.
    pub name: String,
    /// Source content hash, `sha256:<hex>`; must be non-empty.
    pub source_hash: String,
    /// What the strategy may trade.
    pub universe: Universe,
    /// The instruments the strategy subscribes to; empty until set.
    #[serde(default)]
    pub subscriptions: Subscriptions,
    /// The timeframe that drives the strategy's bar lifecycle.
    pub driving_timeframe: TimeframeSpec,
    /// Bars of warm-up history computed before the first decision; `0` when unset.
    #[serde(default)]
    pub warmup_bars: u64,
}

impl StrategyManifest {
    /// A manifest skeleton: `api_version` defaults to `1.0.0` and subscriptions
    /// start empty, so [`StrategyIr::compile`] rejects it until they are set.
    pub fn new(
        name: impl Into<String>,
        source_hash: impl Into<String>,
        universe: Universe,
        driving_timeframe: TimeframeSpec,
    ) -> Self {
        Self {
            api_version: default_api_version(),
            name: name.into(),
            source_hash: source_hash.into(),
            universe,
            subscriptions: Subscriptions::default(),
            driving_timeframe,
            warmup_bars: 0,
        }
    }

    /// Replaces the subscription list.
    pub fn with_subscriptions(mut self, subscriptions: Subscriptions) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// Sets the warm-up bar count.
    pub fn with_warmup_bars(mut self, warmup_bars: u64) -> Self {
        self.warmup_bars = warmup_bars;
        self
    }
}

/// What a strategy may trade.
///
/// Externally tagged: an explicit list is `{"explicit": [...]}` (the pinned
/// sample `{"explicit": [{"symbol": "TCS", "exchange": "NSE"}]}`), the open
/// variant is the plain string `"unrestricted"`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum Universe {
    /// An explicit instrument list; at least one for [`StrategyIr::compile`].
    Explicit(Vec<InstrumentId>),
    /// No restriction beyond the server's catalog.
    Unrestricted,
}

impl Universe {
    /// Whether the universe names no instruments at all.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Explicit(instruments) => instruments.is_empty(),
            Self::Unrestricted => false,
        }
    }
}

/// The manifest's subscription list.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Subscriptions {
    /// Instruments the strategy receives data for; empty means none.
    pub instruments: Vec<InstrumentId>,
}

/// A concrete driving timeframe: `interval` × `aggregation`.
///
/// Serializes as `{"interval": 1, "aggregation": "day"}`; the aggregation is
/// lower-case (`second`, `minute`, `hour`, `day`, `week`, `month`).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TimeframeSpec {
    /// Number of aggregation units per bar.
    pub interval: u32,
    /// The unit of `interval`.
    pub aggregation: BarAggregation,
}

impl TimeframeSpec {
    /// A timeframe of `interval` units of `aggregation`.
    pub const fn new(interval: u32, aggregation: BarAggregation) -> Self {
        Self {
            interval,
            aggregation,
        }
    }

    /// One trading day — the driving timeframe of the example strategies.
    pub const fn one_day() -> Self {
        Self {
            interval: 1,
            aggregation: BarAggregation::Day,
        }
    }
}

/// A compiled strategy's resolved subscriptions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategySubscriptions {
    /// Every instrument the strategy receives, in manifest order.
    pub instruments: Vec<InstrumentId>,
    /// Bar series consumed; the driving timeframe first, so `bars` is never
    /// empty on a compiled IR.
    pub bars: Vec<TimeframeSpec>,
    /// Whether top-of-book quotes flow to the strategy.
    pub quotes: bool,
    /// Whether trade prints flow to the strategy.
    pub trades: bool,
}

/// The verified strategy IR: what a server compiles once and runs thereafter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategyIr {
    /// The manifest this IR compiled from, verbatim.
    pub manifest: StrategyManifest,
    /// Subscriptions derived from the manifest.
    pub subscriptions: StrategySubscriptions,
    /// Warm-up bars before the first decision.
    pub warmup_bars: u64,
}

impl StrategyIr {
    /// Compiles `manifest` into its verified IR.
    ///
    /// Checks run in a fixed order — name, source hash, universe,
    /// subscriptions — so a manifest with several defects reports exactly one
    /// stable code.
    pub fn compile(manifest: StrategyManifest) -> Result<StrategyIr, IrError> {
        if manifest.name.trim().is_empty() {
            return Err(IrError::EmptyName);
        }
        if manifest.source_hash.trim().is_empty() {
            return Err(IrError::EmptySourceHash);
        }
        if manifest.universe.is_empty() {
            return Err(IrError::EmptyUniverse);
        }
        if manifest.subscriptions.instruments.is_empty() {
            return Err(IrError::NoSubscriptions);
        }
        let subscriptions = StrategySubscriptions {
            instruments: manifest.subscriptions.instruments.clone(),
            bars: vec![manifest.driving_timeframe],
            quotes: true,
            trades: true,
        };
        let warmup_bars = manifest.warmup_bars;
        Ok(StrategyIr {
            manifest,
            subscriptions,
            warmup_bars,
        })
    }
}

/// A verification failure, reported through `context.reason` as [`Self::code`].
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum IrError {
    /// `name` is empty or whitespace.
    #[error("strategy name is empty")]
    EmptyName,
    /// `source_hash` is empty or whitespace.
    #[error("source hash is empty")]
    EmptySourceHash,
    /// The universe lists no instruments.
    #[error("universe has no instruments")]
    EmptyUniverse,
    /// The manifest subscribes to nothing.
    #[error("manifest subscribes to nothing")]
    NoSubscriptions,
}

impl IrError {
    /// The stable wire code: `empty_name`, `empty_source_hash`,
    /// `empty_universe` or `no_subscriptions`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyName => "empty_name",
            Self::EmptySourceHash => "empty_source_hash",
            Self::EmptyUniverse => "empty_universe",
            Self::NoSubscriptions => "no_subscriptions",
        }
    }
}
