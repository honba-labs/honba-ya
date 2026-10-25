//! Strategy factory for creating strategies from parameter maps.
//!
//! This module provides a trait and registry for building strategies dynamically
//! from [`StrategyManifest`] parameter declarations, enabling optimization
//! workflows (GA, Bayesian, WFO) to instantiate strategies with different
//! parameter combinations without hardcoding constructor calls.

use std::collections::BTreeMap;
use std::sync::Arc;

use honba_core::{InstrumentId, Quantity};
use serde_json::Value;

use crate::dyn_strategy::DynStrategy;
use crate::error::{Result, StrategyError};
use crate::strategy::Strategy;

/// A trait for creating strategies from parameter maps.
///
/// Implementations parse a [`BTreeMap`] of parameter names to JSON values and
/// construct the corresponding strategy instance. This enables the optimizer
/// crates to generate strategy instances for any point in the parameter space
/// declared in a [`StrategyManifest`] without knowing the concrete strategy type.
pub trait StrategyFactory: Send + Sync {
    /// The strategy type this factory creates.
    type Strategy: Strategy;

    /// The strategy name this factory handles (e.g., "sma_crossover").
    fn strategy_name(&self) -> &'static str;

    /// Creates a strategy instance from the given parameters.
    ///
    /// # Parameters
    /// - `params`: Map of parameter name to JSON value. Must contain all required
    ///   parameters for the strategy. Optional parameters may be omitted and will
    ///   use their defaults.
    /// - `instrument`: The instrument to trade.
    /// - `quantity`: The base order quantity.
    ///
    /// # Errors
    /// Returns [`StrategyError`] if required parameters are missing or invalid.
    fn create(
        &self,
        params: &BTreeMap<String, Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> Result<Self::Strategy>;

    /// Returns the list of parameter names this factory expects.
    fn parameter_names(&self) -> Vec<&'static str>;

    /// Validates that all required parameters are present.
    fn validate_params(&self, params: &BTreeMap<String, Value>) -> Result<()> {
        for name in self.parameter_names() {
            if !params.contains_key(name) {
                return Err(StrategyError::other(format!(
                    "missing required parameter: {}",
                    name
                )));
            }
        }
        Ok(())
    }
}

/// Registry of built-in strategy factories.
///
/// Provides a central lookup for creating strategies by name from parameter maps.
/// Used by optimization workflows to instantiate strategies without hardcoding types.
#[derive(Default)]
pub struct StrategyFactoryRegistry {
    factories: BTreeMap<String, Arc<dyn StrategyFactory<Strategy = DynStrategy>>>,
}

impl StrategyFactoryRegistry {
    /// Creates a new registry with built-in factories registered.
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.register_builtin();
        registry
    }

    /// Registers a strategy factory.
    pub fn register<F>(&mut self, factory: F)
    where
        F: StrategyFactory<Strategy = DynStrategy> + 'static,
    {
        self.factories
            .insert(factory.strategy_name().to_string(), Arc::new(factory));
    }

    /// Creates a strategy by name from parameters.
    pub fn create(
        &self,
        name: &str,
        params: &BTreeMap<String, Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> Result<DynStrategy> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| StrategyError::other(format!("unknown strategy: {}", name)))?;
        factory.create(params, instrument, quantity)
    }

    /// Returns the list of registered strategy names.
    pub fn strategy_names(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }

    /// Registers all built-in strategy factories.
    fn register_builtin(&mut self) {
        self.register(BuyAndHoldFactory);
        self.register(SmaCrossoverFactory);
        self.register(RsiReversalFactory);
    }
}

/// Built-in strategy factories for the reference strategies.
#[derive(Debug, Clone, Copy)]
pub enum BuiltinStrategyFactory {
    /// Buy-and-hold strategy.
    BuyAndHold,
    /// SMA crossover strategy.
    SmaCrossover,
    /// RSI mean-reversion strategy.
    RsiReversal,
}

impl BuiltinStrategyFactory {
    /// Creates a boxed factory for the given variant.
    pub fn boxed(self) -> Arc<dyn StrategyFactory<Strategy = DynStrategy>> {
        match self {
            BuiltinStrategyFactory::BuyAndHold => Arc::new(BuyAndHoldFactory),
            BuiltinStrategyFactory::SmaCrossover => Arc::new(SmaCrossoverFactory),
            BuiltinStrategyFactory::RsiReversal => Arc::new(RsiReversalFactory),
        }
    }
}

/// Factory for [`BuyAndHold`] strategy.
struct BuyAndHoldFactory;

impl StrategyFactory for BuyAndHoldFactory {
    type Strategy = DynStrategy;

    fn strategy_name(&self) -> &'static str {
        "buy_and_hold"
    }

    fn parameter_names(&self) -> Vec<&'static str> {
        vec![] // No tunable parameters
    }

    fn create(
        &self,
        _params: &BTreeMap<String, Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> Result<Self::Strategy> {
        Ok(DynStrategy::new(Box::new(crate::BuyAndHold::new(
            instrument, quantity,
        ))))
    }
}

/// Factory for [`SmaCrossover`] strategy.
struct SmaCrossoverFactory;

impl StrategyFactory for SmaCrossoverFactory {
    type Strategy = DynStrategy;

    fn strategy_name(&self) -> &'static str {
        "sma_crossover"
    }

    fn parameter_names(&self) -> Vec<&'static str> {
        vec!["fast", "slow"]
    }

    fn create(
        &self,
        params: &BTreeMap<String, Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> Result<Self::Strategy> {
        let fast = params
            .get("fast")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| StrategyError::other("missing 'fast' parameter"))?
            as usize;

        let slow = params
            .get("slow")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| StrategyError::other("missing 'slow' parameter"))?
            as usize;

        if fast == 0 || slow == 0 {
            return Err(StrategyError::other("SMA periods must be positive"));
        }
        if fast >= slow {
            return Err(StrategyError::other("fast period must be less than slow period"));
        }

        Ok(DynStrategy::new(Box::new(crate::SmaCrossover::new(
            instrument, fast, slow, quantity,
        ))))
    }
}

/// Factory for [`RsiReversal`] strategy.
struct RsiReversalFactory;

impl StrategyFactory for RsiReversalFactory {
    type Strategy = DynStrategy;

    fn strategy_name(&self) -> &'static str {
        "rsi_reversal"
    }

    fn parameter_names(&self) -> Vec<&'static str> {
        vec!["period", "oversold", "overbought"]
    }

    fn create(
        &self,
        params: &BTreeMap<String, Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> Result<Self::Strategy> {
        let period = params
            .get("period")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| StrategyError::other("missing 'period' parameter"))?
            as usize;

        let oversold = params
            .get("oversold")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| StrategyError::other("missing 'oversold' parameter"))?;

        let overbought = params
            .get("overbought")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| StrategyError::other("missing 'overbought' parameter"))?;

        if period == 0 {
            return Err(StrategyError::other("RSI period must be positive"));
        }
        if oversold >= overbought {
            return Err(StrategyError::other("oversold must be less than overbought"));
        }

        Ok(DynStrategy::new(Box::new(crate::RsiReversal::new(
            instrument, period, oversold, overbought, quantity,
        ))))
    }
}

/// Converts a [`StrategyManifest`] parameter space into a list of parameter
/// combinations for sweeping.
///
/// Uses the manifest's `sweep_space_from_manifest` and `parameter_grid_from_manifest`
/// to generate the Cartesian product of all sweepable parameters.
pub fn manifest_to_parameter_grid(
    manifest: &crate::StrategyManifest,
) -> Result<Vec<BTreeMap<String, Value>>> {
    crate::parameter_grid_from_manifest(manifest).map_err(|e| StrategyError::other(e.to_string()))
}

/// Filters a parameter grid to only include sweepable parameters as declared
/// in the [`StepManifest`].
pub fn filter_sweepable_params(
    grid: Vec<BTreeMap<String, Value>>,
    step_manifest: &crate::StepManifest,
) -> Vec<BTreeMap<String, Value>> {
    let sweepable: std::collections::HashSet<_> = step_manifest
        .params
        .iter()
        .filter_map(|(name, param)| {
            if param.sweepable.unwrap_or(false) {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    grid.into_iter()
        .map(|mut params| {
            params.retain(|k, _| sweepable.contains(k));
            params
        })
        .collect()
}