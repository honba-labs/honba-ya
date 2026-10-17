//! Run-configuration schema (docs/archive/plan.md E0-S4).
//!
//! Config is the input to every surface — a backtest, a sweep, a live session,
//! a CI job — so its shape has to be one declared type that the code generator
//! can walk, rather than a TOML file parsed ad hoc in four places.
//!
//! The type lives in its own crate rather than in `honba-cli` so that codegen,
//! the REST layer, and the CLI all read it without depending on the CLI.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use honba_risk::{OrderRateLimit, RiskLimits};
use honba_strategy::{StrategyManifest, Universe};

/// A complete, self-describing description of one backtest run.
///
/// This is the unit a caller submits. It names the strategy by manifest rather
/// than by source, so the run cannot disagree with what was verified.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BacktestRunConfig {
    /// The verified strategy to run.
    pub strategy: StrategyManifest,
    /// Where the market data comes from.
    #[serde(default)]
    pub data: DataSourceConfig,
    /// Simulation assumptions.
    #[serde(default)]
    pub execution: ExecutionConfig,
    /// Starting account state.
    #[serde(default)]
    pub account: AccountConfig,
    /// Seed for every stochastic component. Same seed and data ⇒ identical journal.
    pub seed: u64,
    /// Pre-trade risk limits (ADR 0018). Absent means no limits.
    #[serde(default)]
    pub risk: RiskLimits,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self::Local {
            root: "data/catalog".to_string(),
        }
    }
}

/// Where a run reads market data from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DataSourceConfig {
    /// Parquet files under a local root, read through `honba-feed`.
    Local {
        /// Directory holding the Parquet catalog.
        root: String,
    },
    /// A broker adapter, resolved by name at run time.
    Adapter {
        /// Registered adapter name, e.g. `dhan`.
        name: String,
    },
}

/// How the run simulates fills.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Fill model to use.
    pub fill_model: FillModel,
    /// Commission model version, recorded in the run's assumptions.
    #[serde(default = "default_cost_model_version")]
    pub cost_model_version: String,
    /// Multiplier applied to modelled slippage, for cost-stress reruns.
    #[serde(default = "one")]
    pub slippage_multiplier: f64,
    /// Whether the run applies the India trading calendar and holidays.
    #[serde(default)]
    pub apply_market_calendar: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            fill_model: FillModel::BarFill,
            cost_model_version: default_cost_model_version(),
            slippage_multiplier: 1.0,
            apply_market_calendar: true,
        }
    }
}

fn default_cost_model_version() -> String {
    "latest".to_string()
}

fn one() -> f64 {
    1.0
}

/// The fill model a run uses.
///
/// Named rather than free-form so a result can state exactly how it was filled;
/// an unrecognised model would make results incomparable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FillModel {
    /// Fill at the bar's open, next open, or a fraction of the bar's range.
    #[default]
    BarFill,
    /// Fill only when the bar trades through the order price.
    BarFillThrough,
    /// Top-of-book matching against quote/trade ticks (ADR 0030). The run needs a tick source;
    /// a run whose data source supplies bars only is refused with `l2_requires_ticks`.
    L2,
    /// No fills; used to test the strategy's signal path alone.
    None,
}

/// Starting account state for a run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    /// Starting cash.
    pub starting_cash: f64,
    /// ISO 4217 currency code of `starting_cash`.
    #[serde(default = "default_currency")]
    pub currency: String,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            starting_cash: 1_000_000.0,
            currency: default_currency(),
        }
    }
}

fn default_currency() -> String {
    "INR".to_string()
}

impl BacktestRunConfig {
    /// Checks the config is internally consistent before a run starts.
    ///
    /// A run that starts with an inconsistent config produces a journal nobody
    /// can interpret, so this is checked at the door.
    pub fn validate(&self) -> Result<(), RunConfigError> {
        self.strategy.validate().map_err(RunConfigError::Strategy)?;
        if self.seed == 0 {
            return Err(RunConfigError::ZeroSeed);
        }
        if self.execution.slippage_multiplier < 0.0 {
            return Err(RunConfigError::NegativeSlippage);
        }
        if !(self.account.starting_cash.is_finite() && self.account.starting_cash > 0.0) {
            return Err(RunConfigError::InvalidStartingCash);
        }
        if let DataSourceConfig::Local { root } = &self.data {
            if root.trim().is_empty() {
                return Err(RunConfigError::EmptyDataRoot);
            }
        }
        self.risk.validate().map_err(RunConfigError::Risk)?;
        Ok(())
    }

    /// The instrument set this run covers.
    pub fn instruments(&self) -> Vec<honba_messages::InstrumentId> {
        self.strategy.instruments()
    }
}

/// Why a run config was rejected.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum RunConfigError {
    /// The embedded strategy manifest is invalid.
    Strategy(honba_strategy::ManifestError),
    /// Seed zero would make every run identical and defeat reproducibility checks.
    ZeroSeed,
    /// A negative slippage multiplier would reward worse fills.
    NegativeSlippage,
    /// Starting cash must be a positive, finite number.
    InvalidStartingCash,
    /// The local data root is blank.
    EmptyDataRoot,
    /// The `[risk]` limits are invalid.
    Risk(honba_risk::RiskConfigError),
}

impl std::fmt::Display for RunConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strategy(e) => write!(f, "invalid strategy manifest: {e}"),
            Self::ZeroSeed => write!(f, "seed must be non-zero"),
            Self::NegativeSlippage => write!(f, "slippage_multiplier must be >= 0"),
            Self::InvalidStartingCash => write!(f, "starting_cash must be positive and finite"),
            Self::EmptyDataRoot => write!(f, "local data root must not be empty"),
            Self::Risk(e) => write!(f, "invalid risk limits: {e}"),
        }
    }
}

impl std::error::Error for RunConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Strategy(e) => Some(e),
            Self::Risk(e) => Some(e),
            _ => None,
        }
    }
}

/// A named universe resolved to concrete instruments.
///
/// A run config cannot hold a bare [`Universe::Named`], because the run needs
/// to know which instruments to load. This is the resolved form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvedUniverse {
    /// The name that was resolved, kept for the run's assumptions block.
    pub name: String,
    /// The instruments it resolved to, sorted.
    pub instruments: Vec<honba_messages::InstrumentId>,
}

impl ResolvedUniverse {
    /// Builds a resolved universe, sorting for determinism.
    pub fn new(
        name: impl Into<String>,
        mut instruments: Vec<honba_messages::InstrumentId>,
    ) -> Self {
        instruments.sort();
        instruments.dedup();
        Self {
            name: name.into(),
            instruments,
        }
    }

    /// Folds a resolved universe into a manifest's subscriptions, so the
    /// manifest and the loader agree on the instrument set.
    pub fn apply(&self, manifest: &mut StrategyManifest) {
        manifest.universe = Universe::Explicit(self.instruments.clone());
        manifest.subscriptions.instruments = self.instruments.clone();
    }
}

#[cfg(test)]
mod tests;
