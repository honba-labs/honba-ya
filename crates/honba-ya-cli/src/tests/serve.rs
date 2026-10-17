use crate::serve::{load_risk_limits, non_loopback_warning, parse_trading_state};
use honba_messages::TradingState;
use honba_risk::{OrderRateLimit, RiskLimits};

#[test]
fn loopback_addresses_do_not_warn() {
    assert!(non_loopback_warning("127.0.0.1:8080".parse().unwrap()).is_none());
    assert!(non_loopback_warning("[::1]:8080".parse().unwrap()).is_none());
}

#[test]
fn other_addresses_warn_about_missing_auth_and_tls() {
    for addr in ["0.0.0.0:8080", "192.168.1.5:80", "[::]:80"] {
        let warning = non_loopback_warning(addr.parse().unwrap()).unwrap();
        assert!(warning.contains("auth"), "{warning}");
        assert!(warning.contains("TLS"), "{warning}");
        assert!(warning.contains(addr), "{warning}");
    }
}

#[test]
fn trading_state_parses_the_wire_spellings_and_refuses_others() {
    assert_eq!(parse_trading_state("active").unwrap(), TradingState::Active);
    assert_eq!(
        parse_trading_state("reducing").unwrap(),
        TradingState::Reducing
    );
    assert_eq!(parse_trading_state("halted").unwrap(), TradingState::Halted);
    let err = parse_trading_state("paused").unwrap_err();
    assert!(err.contains("paused"), "{err}");
}

fn risk_file(name: &str, text: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/honba-cli-risk-unit");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn risk_config_parses_the_backtest_risk_shape() {
    let path = risk_file(
        "full.toml",
        "[risk]\nmax_notional = 500000.0\nmax_participation = 0.1\nstale_after_ms = 5000\n\
         price_collar_bps = 200\n\n[risk.order_rate]\nmax_orders = 30\nwindow_ms = 1000\n",
    );
    assert_eq!(
        load_risk_limits(&path).unwrap(),
        RiskLimits {
            max_notional: Some(500000.0),
            order_rate: Some(OrderRateLimit {
                max_orders: 30,
                window_ms: 1000
            }),
            max_participation: Some(0.1),
            stale_after_ms: Some(5000),
            price_collar_bps: Some(200),
        }
    );
}

#[test]
fn risk_config_without_a_risk_table_means_no_limits() {
    let path = risk_file("empty.toml", "");
    assert_eq!(load_risk_limits(&path).unwrap(), RiskLimits::default());
}

#[test]
fn risk_config_errors_name_the_field_and_the_file() {
    let cases = [
        (
            "zero-collar.toml",
            "[risk]\nprice_collar_bps = 0\n",
            "price_collar_bps",
        ),
        (
            "neg-notional.toml",
            "[risk]\nmax_notional = -1.0\n",
            "max_notional",
        ),
        ("typo.toml", "[risk]\nmax_notinal = 1.0\n", "max_notinal"),
        ("syntax.toml", "[risk\n", "syntax.toml"),
    ];
    for (name, text, needle) in cases {
        let path = risk_file(name, text);
        let err = format!("{:#}", load_risk_limits(&path).unwrap_err());
        assert!(err.contains(needle), "{name}: {err}");
        assert!(err.contains(name), "{name}: {err}");
    }
    let err = format!(
        "{:#}",
        load_risk_limits(std::path::Path::new("/nonexistent/risk.toml")).unwrap_err()
    );
    assert!(err.contains("risk.toml"), "{err}");
}
