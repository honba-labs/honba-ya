//! TOML round trip of a full `[risk]` + `[risk.order_rate_limit]` section (ADR 0018 decision 8).

use std::path::PathBuf;

use honba_config::{BacktestRunConfig, OrderRateLimit, RiskLimits};
use rust_decimal::Decimal;

fn base_text() -> String {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/backtest");
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "toml"))
        .collect();
    files.sort();
    std::fs::read_to_string(&files[0]).unwrap()
}

#[test]
fn full_risk_section_round_trips_through_toml() {
    let text = base_text()
        + "\n[risk]\nmax_notional_per_order = \"500000.0\"\n\n[risk.order_rate_limit]\nmax_orders = 30\nwindow_ms = 1000\n";
    let cfg: BacktestRunConfig = toml::from_str(&text).unwrap();
    assert_eq!(
        cfg.risk,
        RiskLimits {
            max_notional_per_order: Some(Decimal::from(500_000)),
            order_rate_limit: Some(OrderRateLimit {
                max_orders: 30,
                window_ms: 1000
            }),
            ..RiskLimits::default()
        }
    );
    cfg.validate().unwrap();
    let back: BacktestRunConfig = toml::from_str(&toml::to_string(&cfg).unwrap()).unwrap();
    assert_eq!(back, cfg);
}

#[test]
fn invalid_risk_section_fails_validation() {
    let text = base_text() + "\n[risk]\nmax_notional_per_order = \"-1.0\"\n";
    let cfg: BacktestRunConfig = toml::from_str(&text).unwrap();
    assert!(cfg.validate().is_err());
}
