//! Unit tests for the command-line grammar in `main.rs`.

use std::path::PathBuf;

use clap::Parser;
use honba_messages::TradingState;

use crate::{CalendarCommands, Cli, Commands, DataCommands};

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("honba").chain(args.iter().copied()))
}

#[test]
fn backtest_takes_a_config_and_an_optional_output() {
    match parse(&["backtest", "cfg.toml", "-o", "out.md"])
        .unwrap()
        .command
    {
        Commands::Backtest { config, output } => {
            assert_eq!(config, PathBuf::from("cfg.toml"));
            assert_eq!(output, Some(PathBuf::from("out.md")));
        }
        _ => panic!("expected backtest"),
    }
    match parse(&["backtest", "cfg.toml"]).unwrap().command {
        Commands::Backtest { output, .. } => assert_eq!(output, None),
        _ => panic!("expected backtest"),
    }
}

#[test]
fn calendars_show_requires_a_year() {
    match parse(&["calendars", "show", "--year", "2025"])
        .unwrap()
        .command
    {
        Commands::Calendars {
            command: CalendarCommands::Show { year },
        } => assert_eq!(year, 2025),
        _ => panic!("expected calendars show"),
    }
    assert!(parse(&["calendars", "show"]).is_err());
    assert!(parse(&["calendars", "show", "--year", "twenty"]).is_err());
}

#[test]
fn data_load_takes_source_and_symbol() {
    match parse(&["data", "load", "bars.parquet", "TCS"])
        .unwrap()
        .command
    {
        Commands::Data {
            command:
                DataCommands::Load {
                    source,
                    symbol,
                    exchange,
                },
        } => {
            assert_eq!((source.as_str(), symbol.as_str()), ("bars.parquet", "TCS"));
            assert_eq!(exchange, None);
        }
        _ => panic!("expected data load"),
    }
}

#[test]
fn data_load_accepts_an_exchange_flag() {
    match parse(&["data", "load", "b.parquet", "TCS", "--exchange", "BSE"])
        .unwrap()
        .command
    {
        Commands::Data {
            command: DataCommands::Load { exchange, .. },
        } => assert_eq!(exchange.as_deref(), Some("BSE")),
        _ => panic!("expected data load"),
    }
}

#[test]
fn a_subcommand_is_required_and_unknown_ones_are_rejected() {
    assert!(parse(&[]).is_err());
    assert!(parse(&["optimize"]).is_err());
}

#[test]
fn clap_definition_is_consistent() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}

#[test]
fn serve_defaults_to_loopback_and_needs_a_data_dir() {
    match parse(&["serve", "--data-dir", "bars"]).unwrap().command {
        Commands::Serve {
            data_dir,
            addr,
            cors_origin,
            ..
        } => {
            assert!(cors_origin.is_empty());
            assert_eq!(data_dir, PathBuf::from("bars"));
            assert_eq!(addr, "127.0.0.1:8080".parse().unwrap());
        }
        _ => panic!("expected serve"),
    }
    match parse(&["serve", "--data-dir", "d", "--addr", "127.0.0.1:0"])
        .unwrap()
        .command
    {
        Commands::Serve { addr, .. } => assert_eq!(addr.port(), 0),
        _ => panic!("expected serve"),
    }
    assert!(parse(&["serve"]).is_err());
    assert!(parse(&["serve", "--data-dir", "d", "--addr", "not-an-addr"]).is_err());
}

#[test]
fn serve_cors_origin_is_repeatable() {
    match parse(&[
        "serve",
        "--data-dir",
        "d",
        "--cors-origin",
        "https://a.example",
        "--cors-origin",
        "https://b.example",
    ])
    .unwrap()
    .command
    {
        Commands::Serve { cors_origin, .. } => {
            assert_eq!(cors_origin, vec!["https://a.example", "https://b.example"])
        }
        _ => panic!("expected serve"),
    }
}

#[test]
fn serve_run_flags_default_to_the_adr_values() {
    match parse(&["serve", "--data-dir", "d"]).unwrap().command {
        Commands::Serve { runs, .. } => {
            assert_eq!(runs.journals_dir, PathBuf::from("data/journals"));
            assert_eq!(runs.profiles_dir, PathBuf::from("data/profiles"));
            assert_eq!(runs.portfolios_dir, PathBuf::from("data/portfolios"));
            assert_eq!(runs.max_concurrent_runs, None);
            assert_eq!(runs.max_queued_runs, 64);
            assert_eq!(runs.shutdown_grace_secs, 10);
            assert_eq!(runs.keep_runs, 1_000);
            assert_eq!(runs.keep_days, 30);
        }
        _ => panic!("expected serve"),
    }
}

#[test]
fn serve_run_flags_can_be_overridden() {
    match parse(&[
        "serve",
        "--data-dir",
        "d",
        "--journals-dir",
        "j",
        "--profiles-dir",
        "p",
        "--portfolios-dir",
        "pf",
        "--max-concurrent-runs",
        "3",
        "--max-queued-runs",
        "5",
        "--shutdown-grace-secs",
        "0",
        "--keep-runs",
        "7",
        "--keep-days",
        "2",
    ])
    .unwrap()
    .command
    {
        Commands::Serve { runs, .. } => {
            assert_eq!(runs.journals_dir, PathBuf::from("j"));
            assert_eq!(runs.profiles_dir, PathBuf::from("p"));
            assert_eq!(runs.portfolios_dir, PathBuf::from("pf"));
            assert_eq!(runs.max_concurrent_runs, Some(3));
            assert_eq!(runs.max_queued_runs, 5);
            assert_eq!(runs.shutdown_grace_secs, 0);
            assert_eq!(runs.keep_runs, 7);
            assert_eq!(runs.keep_days, 2);
        }
        _ => panic!("expected serve"),
    }
}

#[test]
fn serve_rejects_a_zero_worker_pool_or_queue() {
    assert!(parse(&["serve", "--data-dir", "d", "--max-concurrent-runs", "0"]).is_err());
    assert!(parse(&["serve", "--data-dir", "d", "--max-queued-runs", "0"]).is_err());
}

#[test]
fn serve_trading_state_defaults_to_active() {
    match parse(&["serve", "--data-dir", "d"]).unwrap().command {
        Commands::Serve { trading_state, .. } => {
            assert_eq!(trading_state, TradingState::Active)
        }
        _ => panic!("expected serve"),
    }
}

#[test]
fn serve_trading_state_accepts_the_wire_spellings() {
    for (raw, expected) in [
        ("active", TradingState::Active),
        ("reducing", TradingState::Reducing),
        ("halted", TradingState::Halted),
    ] {
        match parse(&["serve", "--data-dir", "d", "--trading-state", raw])
            .unwrap()
            .command
        {
            Commands::Serve { trading_state, .. } => {
                assert_eq!(trading_state, expected, "{raw}")
            }
            _ => panic!("expected serve"),
        }
    }
}

#[test]
fn serve_rejects_an_unknown_trading_state() {
    assert!(parse(&["serve", "--data-dir", "d", "--trading-state", "paused"]).is_err());
}

#[test]
fn serve_bar_ingest_is_off_by_default_and_opt_in() {
    match parse(&["serve", "--data-dir", "d"]).unwrap().command {
        Commands::Serve { ingest, .. } => {
            assert!(!ingest.bar_ingest);
            assert_eq!(ingest.bar_ingest_capacity, 1_024);
        }
        _ => panic!("expected serve"),
    }
    match parse(&[
        "serve",
        "--data-dir",
        "d",
        "--bar-ingest",
        "--bar-ingest-capacity",
        "8",
    ])
    .unwrap()
    .command
    {
        Commands::Serve { ingest, .. } => {
            assert!(ingest.bar_ingest);
            assert_eq!(ingest.bar_ingest_capacity, 8);
        }
        _ => panic!("expected serve"),
    }
    assert!(parse(&["serve", "--data-dir", "d", "--bar-ingest-capacity", "0"]).is_err());
}

#[test]
fn serve_risk_config_is_optional_and_takes_a_path() {
    match parse(&["serve", "--data-dir", "d"]).unwrap().command {
        Commands::Serve { risk, .. } => assert_eq!(risk.risk_config, None),
        _ => panic!("expected serve"),
    }
    match parse(&["serve", "--data-dir", "d", "--risk-config", "risk.toml"])
        .unwrap()
        .command
    {
        Commands::Serve { risk, .. } => {
            assert_eq!(risk.risk_config, Some(PathBuf::from("risk.toml")))
        }
        _ => panic!("expected serve"),
    }
    assert!(parse(&["serve", "--data-dir", "d", "--risk-config"]).is_err());
}
