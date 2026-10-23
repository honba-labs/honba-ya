//! The Rust-registered strategies a run can execute (ADR 0017 decision 6).
//!
//! A strategy with no entry here is refused at admission with a typed 422, not at run time.

use honba_api::parse_timeframe;
use honba_api::{ErrorCode, ErrorDetail};
use honba_core::InstrumentId;
use honba_strategy::{
    BuyAndHold, RsiReversal, SmaCrossover, Strategy, StrategyIr, StrategyManifest, Subscriptions,
    TimeframeSpec, Universe,
};
use serde_json::json;

/// Order quantity of every registered strategy until strategy parameters are on the wire.
fn default_quantity() -> honba_core::Quantity {
    honba_core::Quantity::from_u64(10)
}

/// Converts the REST contract's bar-aggregation unit (`honba_api::wire::BarAggregation`) to
/// `honba-nou-strategy`'s own, unrelated `BarAggregation` (a strategy-manifest concept with
/// no `honba_core` or wire equivalent; see its doc comment). That enum has no `Month`
/// variant, so a month-aggregation `bar_spec` is refused as a 422 rather than silently
/// mapped onto some other unit.
fn to_strategy_aggregation(
    aggregation: honba_api::wire::BarAggregation,
) -> Result<honba_strategy::BarAggregation, ErrorDetail> {
    use honba_api::wire::BarAggregation as Api;
    use honba_strategy::BarAggregation as Strategy;
    match aggregation {
        Api::Second => Ok(Strategy::Second),
        Api::Minute => Ok(Strategy::Minute),
        Api::Hour => Ok(Strategy::Hour),
        Api::Day => Ok(Strategy::Day),
        Api::Week => Ok(Strategy::Week),
        Api::Month => Err(ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            "bar_spec of month aggregation has no equivalent strategy timeframe".to_string(),
        )
        .with_context(json!({"field": "bar_spec", "reason": "unsupported_aggregation"}))),
    }
}

/// The set of strategies implemented in Rust, by name.
#[derive(Clone, Debug, Default)]
pub struct StrategyRegistry {
    names: Vec<&'static str>,
}

impl StrategyRegistry {
    /// The strategies shipped in `honba-strategy`.
    pub fn builtin() -> Self {
        Self {
            names: vec!["buy_and_hold", "rsi_reversal", "sma_crossover"],
        }
    }

    /// Registered names, sorted.
    pub fn names(&self) -> Vec<&'static str> {
        self.names.clone()
    }

    /// Whether `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains(&name) || name == "sma_cross"
    }

    /// Builds the strategy `name` over `instrument`. The named strategies have no random
    /// component, so `seed` is accepted for the factory shape and ignored.
    pub fn build(
        &self,
        name: &str,
        instrument: &InstrumentId,
        seed: u64,
    ) -> Option<Box<dyn Strategy>> {
        let _ = seed;
        let instrument = instrument.clone();
        Some(match name {
            "buy_and_hold" => Box::new(BuyAndHold::new(instrument, default_quantity())),
            "rsi_reversal" => {
                Box::new(RsiReversal::new(instrument, 14, 30.0, 70.0, default_quantity()))
            }
            "sma_crossover" | "sma_cross" => {
                Box::new(SmaCrossover::new(instrument, 3, 8, default_quantity()))
            }
            _ => return None,
        })
    }

    /// The IR a registered strategy runs under: an explicit one-instrument universe on the
    /// bar timeframe `bar_spec`.
    pub fn ir_for(
        &self,
        name: &str,
        instrument: &InstrumentId,
        bar_spec: &str,
    ) -> Result<StrategyIr, ErrorDetail> {
        let field_error = |field: &str, reason: &str, message: String| {
            ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
                .with_context(json!({"field": field, "reason": reason}))
        };
        if !self.contains(name) {
            return Err(field_error(
                "strategy",
                "unknown_strategy",
                format!("strategy {name:?} is not registered"),
            ));
        }
        let spec = parse_timeframe(bar_spec)
            .map_err(|e| field_error("bar_spec", "invalid_bar_spec", e.message))?;
        let interval = u32::try_from(spec.step()).map_err(|_| {
            field_error("bar_spec", "invalid_bar_spec", "bar step too large".into())
        })?;
        let aggregation = to_strategy_aggregation(spec.aggregation())?;
        let manifest = StrategyManifest::new(
            name,
            format!("registered:{name}"),
            Universe::Explicit(vec![instrument.clone()]),
            TimeframeSpec::new(interval, aggregation),
        )
        .with_subscriptions(Subscriptions {
            instruments: vec![instrument.clone()],
            quotes: false,
            trades: false,
        });
        StrategyIr::compile(manifest).map_err(|e| {
            ErrorDetail::new(ErrorCode::InternalError, e.to_string())
                .with_context(json!({"reason": "registered_ir"}))
        })
    }
}
