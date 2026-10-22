//! The `[risk]` section of `BacktestRunConfig` (ADR 0018 decision 8).

use crate::*;

use honba_core::{Exchange, InstrumentId};
use honba_strategy::{BarAggregation, StrategyManifest, Subscriptions, TimeframeSpec};
use rust_decimal::Decimal;

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
    let id = InstrumentId::new(Exchange::Nse, "TCS");
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
        r#", "risk": {"max_notional_per_order": 5.0, "order_rate_limit": {"max_orders": 3, "window_ms": 10}}"#,
    ))
    .unwrap();
    assert_eq!(cfg.risk.max_notional_per_order, Some(Decimal::from(5)));
    assert_eq!(
        cfg.risk.order_rate_limit,
        Some(OrderRateLimit {
            max_orders: 3,
            window_ms: 10
        })
    );
}

#[test]
fn risk_unknown_order_rate_field_is_ignored() {
    assert!(
        serde_json::from_str::<BacktestRunConfig>(&base_json(
            r#", "risk": {"order_rate_limit": {"max_orders": 1, "window_ms": 1, "burst": 1}}"#
        ))
        .is_ok(),
        "honba_mon::OrderRateLimit has no deny_unknown_fields, so an extra key is ignored"
    );
}

#[test]
fn risk_unknown_top_level_field_is_silently_ignored() {
    // Unlike the deleted `honba-kokoro-risk::RiskLimits` this crate used to depend on,
    // `honba_mon::RiskLimits` does not carry `#[serde(deny_unknown_fields)]`, so an
    // unrecognised key inside `[risk]` is ignored rather than rejected. This test
    // documents that reality instead of asserting a strictness honba-mon doesn't provide.
    let cfg: BacktestRunConfig = serde_json::from_str(&base_json(
        r#", "risk": {"max_notinal_per_order": 5.0}"#,
    ))
    .unwrap();
    assert_eq!(cfg.risk, RiskLimits::default());
}

#[test]
fn risk_invalid_limits_rejected() {
    for v in [Decimal::ZERO, Decimal::from(-5)] {
        let mut cfg = valid_config();
        cfg.risk.max_notional_per_order = Some(v);
        assert!(
            matches!(cfg.validate(), Err(RunConfigError::Risk(_))),
            "max_notional_per_order {v}"
        );
    }
    for (m, w) in [(0, 1), (1, 0)] {
        let mut cfg = valid_config();
        cfg.risk.order_rate_limit = Some(OrderRateLimit {
            max_orders: m,
            window_ms: w,
        });
        assert!(matches!(cfg.validate(), Err(RunConfigError::Risk(_))));
    }
    let mut ok = valid_config();
    ok.risk.max_notional_per_order = Some(Decimal::from(1));
    assert_eq!(ok.validate(), Ok(()));
}
