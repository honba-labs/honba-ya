//! Integration tests for the GA optimizer wired to a real [`BacktestEngine`].

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use honba_core::{Exchange, InstrumentId, Price, Quantity, Tick};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_strategy::{
    DynStrategy, Strategy, StrategyContext, StrategyManifest, factory::StrategyFactory,
};
use honba_ya_enhancer::{GaConfig, GaOptimizer};
use rust_decimal::Decimal;

struct ThresholdStrategy {
    instrument: InstrumentId,
    buy_threshold: Price,
    sell_threshold: Price,
    quantity: Quantity,
    bought: bool,
    sold: bool,
}

impl ThresholdStrategy {
    fn new(
        instrument: InstrumentId,
        buy_threshold: Price,
        sell_threshold: Price,
        quantity: Quantity,
    ) -> Self {
        Self {
            instrument,
            buy_threshold,
            sell_threshold,
            quantity,
            bought: false,
            sold: false,
        }
    }
}

impl Strategy for ThresholdStrategy {
    fn name(&self) -> &str {
        "threshold_test"
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        trade: &Tick,
    ) -> honba_strategy::Result<()> {
        if let Some(price) = trade.last_price() {
            if !self.bought && price <= self.buy_threshold {
                self.bought = true;
                ctx.submit(honba_strategy::intent::OrderIntent::limit_buy(
                    self.instrument.clone(),
                    self.quantity,
                    price,
                ));
            } else if self.bought && !self.sold && price >= self.sell_threshold {
                self.sold = true;
                ctx.submit(honba_strategy::intent::OrderIntent::limit_sell(
                    self.instrument.clone(),
                    self.quantity,
                    price,
                ));
            }
        }
        Ok(())
    }
}

struct ThresholdFactory;

impl StrategyFactory for ThresholdFactory {
    type Strategy = DynStrategy;

    fn strategy_name(&self) -> &'static str {
        "threshold_test"
    }

    fn parameter_names(&self) -> Vec<&'static str> {
        vec!["buy_threshold", "sell_threshold"]
    }

    fn create(
        &self,
        params: &BTreeMap<String, serde_json::Value>,
        instrument: InstrumentId,
        quantity: Quantity,
    ) -> honba_strategy::Result<Self::Strategy> {
        let buy_val = params
            .get("buy_threshold")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| honba_strategy::error::StrategyError::other("missing buy_threshold"))?;
        let sell_val = params
            .get("sell_threshold")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| honba_strategy::error::StrategyError::other("missing sell_threshold"))?;
        let buy_threshold = Price::from_rupees(&format!("{buy_val}"))
            .map_err(|e| honba_strategy::error::StrategyError::other(e.to_string()))?;
        let sell_threshold = Price::from_rupees(&format!("{sell_val}"))
            .map_err(|e| honba_strategy::error::StrategyError::other(e.to_string()))?;
        Ok(DynStrategy::new(Box::new(ThresholdStrategy::new(
            instrument,
            buy_threshold,
            sell_threshold,
            quantity,
        ))))
    }
}

fn make_manifest() -> StrategyManifest {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let mut m = StrategyManifest::new(
        "threshold_test",
        "test-hash",
        honba_strategy::Universe::Explicit(vec![inst.clone()]),
        honba_strategy::TimeframeSpec::new(1, honba_strategy::BarAggregation::Minute),
    );
    m = m
        .with_parameter(
            "buy_threshold",
            honba_strategy::ParameterSpec::new(serde_json::json!(1500.0))
                .with_bounds(1400.0, 1600.0, 10.0),
        )
        .with_parameter(
            "sell_threshold",
            honba_strategy::ParameterSpec::new(serde_json::json!(1600.0))
                .with_bounds(1500.0, 1700.0, 10.0),
        );
    m
}

fn make_ticks() -> Vec<Tick> {
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let base = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()));
    let prices = [1550.0, 1480.0, 1450.0, 1420.0, 1520.0, 1580.0, 1620.0];
    prices
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            let ts = base + chrono::Duration::minutes(i as i64);
            Tick::Ltp {
                instrument: InstrumentId::new(Exchange::Nse, "RELIANCE"),
                price: Price::from_rupees(&p.to_string()).unwrap(),
                volume: Quantity::from_u64(100),
                exchange_ts: ts,
                receive_ts: ts,
            }
        })
        .collect()
}

#[test]
fn test_ga_optimizer_runs_and_produces_pareto_frontier() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let quantity = Quantity::from_u64(100);

    let mut registry = honba_strategy::factory::StrategyFactoryRegistry::new();
    registry.register(ThresholdFactory);

    let ga = GaOptimizer::with_registry(
        GaConfig {
            population_size: 8,
            generations: 3,
            ..Default::default()
        },
        Arc::new(registry),
    );

    let manifest = make_manifest();
    let data = make_ticks();
    let limits = RiskLimits {
        max_notional_per_order: Some(Decimal::from(1_000_000)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo("threshold_test");
    let calendar = Arc::new(honba_backtest::NseTradingCalendar::new());

    let result = ga
        .optimize(
            &manifest,
            &data,
            inst.clone(),
            quantity,
            risk_engine,
            calendar,
        )
        .expect("GA optimize should succeed");

    // Best fitness should be finite (a real backtest was run).
    assert!(result.best_individual.fitness.is_finite());
    assert!(!result.best_individual.params.is_empty());
    assert!(result.pareto_frontier.len() >= 1);
    assert_eq!(result.generations_run, 3);
    assert!(result.total_evaluations > 0);
    assert!(result.converged);

    // Best individual params should be finite and positive (GA mutation can
    // push values outside the declared sweep bounds, so we don't clamp here).
    let buy = result.best_individual.params["buy_threshold"]
        .as_f64()
        .unwrap();
    let sell = result.best_individual.params["sell_threshold"]
        .as_f64()
        .unwrap();
    assert!(buy > 0.0 && buy.is_finite());
    assert!(sell > 0.0 && sell.is_finite());
}

#[test]
fn test_ga_empty_parameter_grid_errors() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let quantity = Quantity::from_u64(100);

    let mut registry = honba_strategy::factory::StrategyFactoryRegistry::new();
    registry.register(ThresholdFactory);

    let ga = GaOptimizer::with_registry(GaConfig::default(), Arc::new(registry));

    // Manifest with no parameters -> empty grid.
    let manifest = StrategyManifest::new(
        "threshold_test",
        "test-hash",
        honba_strategy::Universe::Explicit(vec![inst.clone()]),
        honba_strategy::TimeframeSpec::new(1, honba_strategy::BarAggregation::Minute),
    );

    let data = make_ticks();
    let limits = RiskLimits::default();
    let risk_engine = RiskEngine::new(limits, OtrConfig::default());
    let calendar = Arc::new(honba_backtest::NseTradingCalendar::new());

    let err = ga
        .optimize(
            &manifest,
            &data,
            inst.clone(),
            quantity,
            risk_engine,
            calendar,
        )
        .expect_err("empty parameter grid should error");

    assert!(matches!(
        err,
        honba_ya_enhancer::GaError::EmptyParameterGrid
    ));
}
