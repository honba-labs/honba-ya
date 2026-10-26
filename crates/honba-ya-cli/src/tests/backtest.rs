//! Unit tests for the pure helpers in `crate::backtest`.

use honba_core::chrono::{DateTime, Utc};
use honba_core::rust_decimal::prelude::*;
use honba_core::uuid::Uuid;
use honba_core::{Exchange, FillReport, InstrumentId, OrderSide, Price, Quantity};

use crate::backtest::{BacktestConfig, equity_curve, pair_fills};

fn any_instrument() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "X")
}

fn ts(n: u64) -> DateTime<Utc> {
    DateTime::from_timestamp(0, n as u32).expect("fixture timestamp in range")
}

fn fill(side: OrderSide, price: f64, n: u64) -> FillReport {
    FillReport::new(
        format!("F-{n}"),
        format!("O-{n}"),
        Uuid::new_v4(),
        any_instrument(),
        side,
        Price::new(Decimal::from_f64_retain(price).expect("fixture price is finite")),
        Quantity::from_u64(1),
        "test-algo",
        ts(n),
    )
}

#[test]
fn pair_fills_pairs_consecutive_entry_exit_fills() {
    let fills = [
        fill(OrderSide::Buy, 100.0, 1),
        fill(OrderSide::Sell, 105.0, 2),
        fill(OrderSide::Sell, 110.0, 3),
        fill(OrderSide::Buy, 104.0, 4),
    ];
    let trips = pair_fills(&fills);
    let pnl: Vec<f64> = trips.iter().map(|t| t.net_pnl.to_f64().unwrap()).collect();
    assert_eq!(pnl, [5.0, 6.0]);
}

#[test]
fn pair_fills_skips_same_side_pairs_and_a_trailing_fill() {
    let fills = [
        fill(OrderSide::Buy, 100.0, 1),
        fill(OrderSide::Buy, 101.0, 2),
        fill(OrderSide::Buy, 102.0, 3),
    ];
    assert!(pair_fills(&fills).is_empty());
    assert!(pair_fills(&[]).is_empty());
}

#[test]
fn equity_curve_accumulates_net_pnl_from_the_start() {
    let trips = pair_fills(&[
        fill(OrderSide::Buy, 100.0, 1),
        fill(OrderSide::Sell, 90.0, 2),
        fill(OrderSide::Buy, 100.0, 3),
        fill(OrderSide::Sell, 130.0, 4),
    ]);
    assert_eq!(equity_curve(1_000.0, &trips), [1_000.0, 990.0, 1_020.0]);
    assert_eq!(equity_curve(1_000.0, &[]), [1_000.0]);
}

#[test]
fn config_fills_defaults_for_omitted_fields() {
    let cfg: BacktestConfig = toml::from_str("[strategy]\nname = \"sma_crossover\"\n").unwrap();
    assert_eq!(cfg.symbol, "NIFTY50");
    assert_eq!(cfg.exchange, "NSE");
    assert_eq!(cfg.starting_equity, 1_000_000.0);
    assert_eq!(cfg.strategy.name, "sma_crossover");
    assert_eq!(
        (
            cfg.strategy.fast,
            cfg.strategy.slow,
            cfg.strategy.trade_size
        ),
        (0, 0, 0.0)
    );
}

#[test]
fn config_reads_explicit_values() {
    let cfg: BacktestConfig = toml::from_str(
        r#"
        symbol = "TCS"
        exchange = "BSE"
        starting_equity = 5000.0
        [strategy]
        name = "sma_crossover"
        fast = 2
        slow = 5
        trade_size = 3.0
        "#,
    )
    .unwrap();
    assert_eq!((cfg.symbol.as_str(), cfg.exchange.as_str()), ("TCS", "BSE"));
    assert_eq!(cfg.starting_equity, 5000.0);
    assert_eq!(
        (
            cfg.strategy.fast,
            cfg.strategy.slow,
            cfg.strategy.trade_size
        ),
        (2, 5, 3.0)
    );
}

#[test]
fn config_requires_a_strategy_table() {
    assert!(toml::from_str::<BacktestConfig>("symbol = \"X\"\n").is_err());
}
