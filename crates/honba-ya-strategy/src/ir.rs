//! The verified strategy IR (docs/archive/plan.md E0-S8).
//!
//! A [`StrategyManifest`] is what an author (or an agent) declares; a
//! [`StrategyIr`] is what `verify` proves about it and what every runner
//! consumes. Compiling resolves everything a loader would otherwise infer at
//! run time — the concrete universe, the per-channel subscription lists, the
//! set of timeframes, the warm-up — so backtest, sweep and live read one
//! precomputed, serializable answer instead of each re-deriving it.
//!
//! The IR is a wire *record* (ADR 0012): unknown fields are ignored and an
//! unknown `schema_version` is rejected. The manifest inside it stays an
//! *input* and still rejects unknown fields.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use honba_core::{InstrumentId, SCHEMA_VERSION};

use crate::manifest::{ManifestError, StrategyManifest, TimeframeSpec, Universe};

/// A verified strategy, ready to run: the manifest plus everything resolved
/// from it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StrategyIr {
    /// Wire version of this document; readers reject any other value.
    #[serde(deserialize_with = "current_schema_version")]
    pub schema_version: u32,
    /// Strategy contract version the manifest was compiled against.
    pub strategy_api_version: String,
    /// The manifest exactly as verified.
    pub manifest: StrategyManifest,
    /// The concrete instrument set the strategy may trade.
    pub universe: IrUniverse,
    /// What is fed to the strategy, per channel.
    pub subscriptions: IrSubscriptions,
    /// The bar timeframe that drives the strategy's clock.
    pub driving_timeframe: TimeframeSpec,
    /// Every bar timeframe the run must load, driving timeframe first.
    pub timeframes: Vec<TimeframeSpec>,
    /// Driving bars consumed before the first decision (the manifest's
    /// `warmup_bars`, same name in Rust and Python).
    pub warmup_bars: u32,
}

/// The universe after resolution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IrUniverse {
    /// The universe's name when the manifest named one (e.g. `NIFTY50`).
    pub named: Option<String>,
    /// The instruments it resolved to, sorted and de-duplicated.
    pub instruments: Vec<InstrumentId>,
}

/// Subscriptions after resolution: one sorted instrument list per channel.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IrSubscriptions {
    /// Instruments whose bars (on the driving timeframe) are delivered.
    pub bars: Vec<InstrumentId>,
    /// Instruments whose top-of-book quotes are delivered.
    pub quotes: Vec<InstrumentId>,
    /// Instruments whose trade prints are delivered.
    pub trades: Vec<InstrumentId>,
}

/// Why a manifest did not compile.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IrError {
    /// The manifest itself is invalid ([`StrategyManifest::validate`]).
    Manifest(ManifestError),
    /// The strategy subscribes to no instrument, so it would receive nothing.
    NoSubscriptions,
    /// A subscribed instrument is outside the explicit universe.
    SubscriptionOutsideUniverse(InstrumentId),
}

impl IrError {
    /// A stable snake_case code for agents and the CLI; manifest errors keep
    /// the codes of `honba.strategies.manifest.ManifestError`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Manifest(e) => match e {
                ManifestError::EmptyName => "empty_name",
                ManifestError::EmptySourceHash => "empty_source_hash",
                ManifestError::UnsupportedApiVersion { .. } => "unsupported_api_version",
                ManifestError::ZeroInterval => "zero_interval",
                ManifestError::NamedUniverseUnresolved => "named_universe_unresolved",
                ManifestError::InvalidParameterRange { .. } => "invalid_parameter_range",
                ManifestError::ParameterDefaultOutOfRange { .. } => {
                    "parameter_default_out_of_range"
                }
                ManifestError::TomlParse(_) => "toml_parse",
                ManifestError::InvalidStep(_) => "invalid_step",
            },
            Self::NoSubscriptions => "no_subscriptions",
            Self::SubscriptionOutsideUniverse(_) => "subscription_outside_universe",
        }
    }
}

impl std::fmt::Display for IrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manifest(e) => write!(f, "{e}"),
            Self::NoSubscriptions => write!(f, "strategy subscribes to no instruments"),
            Self::SubscriptionOutsideUniverse(id) => {
                write!(f, "subscription {id} is outside the strategy's universe")
            }
        }
    }
}

impl std::error::Error for IrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Manifest(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ManifestError> for IrError {
    fn from(e: ManifestError) -> Self {
        Self::Manifest(e)
    }
}

fn sorted(ids: &[InstrumentId]) -> Vec<InstrumentId> {
    let mut out = ids.to_vec();
    out.sort();
    out.dedup();
    out
}

impl StrategyIr {
    /// Verifies `manifest` and resolves it into a runnable IR.
    ///
    /// ```
    /// use honba_messages::{BarAggregation, Exchange, InstrumentId};
    /// use honba_strategy::{StrategyIr, StrategyManifest, Subscriptions, TimeframeSpec, Universe};
    ///
    /// let tcs = InstrumentId::new("TCS", Exchange::new("NSE"));
    /// let manifest = StrategyManifest::new(
    ///     "sma",
    ///     "sha256:abc",
    ///     Universe::Explicit(vec![tcs.clone()]),
    ///     TimeframeSpec::new(1, BarAggregation::Day),
    /// )
    /// .with_subscriptions(Subscriptions { instruments: vec![tcs.clone()], quotes: false, trades: false })
    /// .with_warmup_bars(20);
    /// let ir = StrategyIr::compile(manifest).unwrap();
    /// assert_eq!(ir.subscriptions.bars, vec![tcs]);
    /// assert_eq!(ir.warmup_bars, 20);
    /// ```
    pub fn compile(manifest: StrategyManifest) -> Result<Self, IrError> {
        manifest.validate()?;
        let subscribed = sorted(&manifest.subscriptions.instruments);
        if subscribed.is_empty() {
            return Err(IrError::NoSubscriptions);
        }
        let universe = match &manifest.universe {
            Universe::Explicit(list) => {
                let instruments = sorted(list);
                if let Some(outside) = subscribed.iter().find(|i| !instruments.contains(i)) {
                    return Err(IrError::SubscriptionOutsideUniverse(outside.clone()));
                }
                IrUniverse {
                    named: None,
                    instruments,
                }
            }
            // `validate` guarantees the loader resolved the name into the
            // subscription list; that list is the universe.
            Universe::Named(name) => IrUniverse {
                named: Some(name.clone()),
                instruments: subscribed.clone(),
            },
        };
        let channel = |on: bool| if on { subscribed.clone() } else { Vec::new() };
        let subscriptions = IrSubscriptions {
            bars: subscribed.clone(),
            quotes: channel(manifest.subscriptions.quotes),
            trades: channel(manifest.subscriptions.trades),
        };
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            strategy_api_version: manifest.api_version.clone(),
            universe,
            subscriptions,
            driving_timeframe: manifest.driving_timeframe,
            timeframes: vec![manifest.driving_timeframe],
            warmup_bars: manifest.warmup_bars,
            manifest,
        })
    }
}

fn current_schema_version<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let found = u32::deserialize(deserializer)?;
    if found == SCHEMA_VERSION {
        Ok(found)
    } else {
        Err(serde::de::Error::custom(format!(
            "unsupported schema_version {found}; this build reads {SCHEMA_VERSION}"
        )))
    }
}
