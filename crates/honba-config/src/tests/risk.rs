//! The `[risk]` section of `BacktestRunConfig` (ADR 0018 decision 8).

use crate::*;

use honba_messages::{BarAggregation, Exchange, InstrumentId};
use honba_strategy::{StrategyManifest, Subscriptions, TimeframeSpec};

fn base_json(extra: &str) -> String {
    format!(
        r#"{{"strategy": {{
        "api_version": "1.0.0", "name": "sma", "source_hash": "h",
        "universe": {{"explicit": []}},
        "subscriptions": {{"instruments": [], "quotes": false, "trades": false}},
        "driving_timeframe": {{"interval": 1, "aggregation": "day"}},
        "warmup_bars": 0
    }}, "seed": 7{extra}}}"#
    )
}

fn valid_config() -> BacktestRunConfig {
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    BacktestRunConfig {
        strategy: StrategyManifest::new(
            "sma",
            "sha256:abc",
            Universe::Explicit(vec![id.clone()]),
            TimeframeSpec::new(1, BarAggregation::Day),
        )
        .with_subscriptions(Subscriptions {
            instruments: vec![id],
            quotes: false,
            trades: false,
        }),
        data: DataSourceConfig::default(),
        execution: ExecutionConfig::default(),
        account: AccountConfig::default(),
        seed: 42,
        risk: RiskLimits::default(),
    }
}

#[test]
fn config_without_risk_section_parses() {
    let cfg: BacktestRunConfig = serde_json::from_str(&base_json("")).unwrap();
    assert_eq!(cfg.risk, RiskLimits::default());
}

#[test]
fn risk_section_parses() {
    let cfg: BacktestRunConfig = serde_json::from_str(&base_json(
        r#", "risk": {"max_notional": 5.0, "order_rate": {"max_orders": 3, "window_ms": 10}}"#,
    ))
    .unwrap();
    assert_eq!(cfg.risk.max_notional, Some(5.0));
    assert_eq!(
        cfg.risk.order_rate,
        Some(OrderRateLimit {
            max_orders: 3,
            window_ms: 10
        })
    );
}

#[test]
fn risk_unknown_field_rejected() {
    assert!(serde_json::from_str::<BacktestRunConfig>(&base_json(
        r#", "risk": {"max_notinal": 5.0}"#
    ))
    .is_err());
    assert!(serde_json::from_str::<BacktestRunConfig>(&base_json(
        r#", "risk": {"order_rate": {"max_orders": 1, "window_ms": 1, "burst": 1}}"#
    ))
    .is_err());
}

#[test]
fn risk_invalid_limits_rejected() {
    for v in [0.0, -5.0, f64::NAN, f64::INFINITY] {
        let mut cfg = valid_config();
        cfg.risk.max_notional = Some(v);
        assert!(
            matches!(cfg.validate(), Err(RunConfigError::Risk(_))),
            "max_notional {v}"
        );
    }
    for (m, w) in [(0, 1), (1, 0)] {
        let mut cfg = valid_config();
        cfg.risk.order_rate = Some(OrderRateLimit {
            max_orders: m,
            window_ms: w,
        });
        assert!(matches!(cfg.validate(), Err(RunConfigError::Risk(_))));
    }
    let mut ok = valid_config();
    ok.risk.max_notional = Some(1.0);
    assert_eq!(ok.validate(), Ok(()));
}
