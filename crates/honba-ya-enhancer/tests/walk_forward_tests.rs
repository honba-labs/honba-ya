//! Integration tests for Walk-Forward Analysis wired to a real BacktestEngine.
//!
//! Uses the built-in `rsi_reversal` strategy (already registered in
//! `StrategyFactoryRegistry`) and `Tick` events, since `run_wfo` takes `&[Tick]`
//! and RSI reversal trades on `on_trade` (Tick::Ltp).

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use honba_core::{Exchange, InstrumentId, Price, Quantity, Tick};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_strategy::{BarAggregation, StrategyManifest, TimeframeSpec, Universe};
use honba_ya_enhancer::{EnhancerEngine, WalkForwardConfig};
use rust_decimal::Decimal;
use serde_json::json;

fn make_manifest() -> StrategyManifest {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    StrategyManifest::new(
        "rsi_reversal",
        "test-hash",
        Universe::Explicit(vec![inst.clone()]),
        TimeframeSpec::new(1, BarAggregation::Minute),
    )
    .with_parameter(
        "period",
        honba_strategy::ParameterSpec::new(json!(14)).with_bounds(5.0, 30.0, 5.0),
    )
    .with_parameter(
        "oversold",
        honba_strategy::ParameterSpec::new(json!(30.0)).with_bounds(20.0, 40.0, 5.0),
    )
    .with_parameter(
        "overbought",
        honba_strategy::ParameterSpec::new(json!(70.0)).with_bounds(60.0, 90.0, 5.0),
    )
}

fn make_ticks() -> Vec<Tick> {
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let base = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()));
    // Oscillating prices so RSI crosses oversold/overbought zones.
    let prices = [
        1500.0, 1490.0, 1480.0, 1470.0, 1460.0, 1470.0, 1480.0, 1490.0, 1500.0, 1510.0, 1520.0,
        1530.0, 1520.0, 1510.0, 1500.0, 1490.0, 1480.0, 1490.0, 1500.0, 1510.0,
    ];
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
fn test_wfo_runs_and_reports_wfe() {
    let manifest = make_manifest();
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let quantity = Quantity::from_u64(100);

    let engine = EnhancerEngine::with_config(
        honba_ya_enhancer::GaConfig::default(),
        honba_ya_enhancer::BayesianConfig::default(),
        WalkForwardConfig {
            in_sample_length: 8,
            out_sample_length: 4,
            step_size: 4,
            compute_wfe: true,
            wfe_threshold: 0.60,
        },
    );

    let data = make_ticks();
    let limits = RiskLimits {
        max_notional_per_order: Some(Decimal::from(1_000_000)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo("rsi_reversal");
    let calendar = Arc::new(honba_backtest::NseTradingCalendar::new());

    // Fixed-parameter optimizer: returns period=14, oversold=30, overbought=70.
    // Exercises the real BacktestEngine evaluation inside run_wfo.
    let result = engine.run_wfo(
        &manifest,
        &data,
        inst.clone(),
        quantity,
        risk_engine,
        calendar,
        |_manifest, _data, _instrument, _quantity, _risk_engine, _calendar| {
            Ok(BTreeMap::from([
                ("period".to_string(), 14.0),
                ("oversold".to_string(), 30.0),
                ("overbought".to_string(), 70.0),
            ]))
        },
    );

    // In-sample and out-sample PnL are recorded once per WFO run (whenever
    // the optimizer succeeds), so the slices must be equal in length. This
    // also proves the real BacktestEngine evaluation path ran (create
    // succeeded; a rejection would yield Decimal::MIN PnL and not be recorded
    // here).
    assert_eq!(result.in_sample_pnl.len(), result.best_params_per_run.len());
    assert_eq!(
        result.out_sample_pnl.len(),
        result.best_params_per_run.len()
    );
    // WFE runs only count iterations where in-sample Sharpe was positive; the
    // loop itself completed all data-splitting iterations without panic.
    assert_eq!(result.wfe_scores.len(), result.n_runs);
}
