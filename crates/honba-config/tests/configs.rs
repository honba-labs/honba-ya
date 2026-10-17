//! Every committed backtest run config parses as `BacktestRunConfig`, passes
//! `validate()`, and compiles its strategy (docs/archive/plan.md E0-S4 follow-up).
//!
//! `configs/backtest/*.toml` is what docs, examples and agents copy; a file
//! that no longer matches the schema is a broken example. Live and AI configs
//! (`configs/live`, `configs/ai`) are other schemas and are not checked here.

use std::path::PathBuf;

use honba_config::BacktestRunConfig;
use honba_strategy::StrategyIr;

fn backtest_configs() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/backtest");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "toml"))
        .collect();
    files.sort();
    files
}

#[test]
fn there_are_backtest_configs_to_check() {
    assert!(!backtest_configs().is_empty());
}

#[test]
fn every_backtest_config_matches_the_run_config_schema() {
    for path in backtest_configs() {
        let name = path.display();
        let text = std::fs::read_to_string(&path).unwrap();
        let cfg: BacktestRunConfig = toml::from_str(&text)
            .unwrap_or_else(|e| panic!("{name}: not a BacktestRunConfig: {e}"));
        cfg.validate().unwrap_or_else(|e| panic!("{name}: {e}"));
        StrategyIr::compile(cfg.strategy.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[test]
fn a_typo_in_a_run_config_is_an_error_not_a_default() {
    // ADR 0012: configs are inputs; an unknown key must fail, never be dropped.
    let path = &backtest_configs()[0];
    let text = std::fs::read_to_string(path).unwrap() + "\nslipage_multiplier = 2.0\n";
    let err = toml::from_str::<BacktestRunConfig>(&text).unwrap_err();
    assert!(err.to_string().contains("slipage_multiplier"), "{err}");
}

#[test]
fn config_without_risk_section_parses() {
    // ADR 0018: `[risk]` is optional; every committed config predates it.
    for path in backtest_configs() {
        let name = path.display();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("[risk"), "{name} already has a risk section");
        let cfg: BacktestRunConfig =
            toml::from_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(cfg.risk, honba_config::RiskLimits::default(), "{name}");
    }
}
