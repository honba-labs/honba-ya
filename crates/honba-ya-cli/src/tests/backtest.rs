//! Unit tests for the pure helpers in `crate::backtest`.

use honba_entities::{Currency, Trade};
use honba_messages::{Exchange, InstrumentId, OrderId, OrderSide, UnixNanos};

use crate::backtest::{equity_curve, pair_fills, BacktestConfig};

fn any_instrument() -> InstrumentId {
    InstrumentId::new("X", Exchange::new("NSE"))
}

fn fill(side: OrderSide, price: f64, ts: u64) -> Trade {
    Trade::new(
        OrderId::new(format!("O-{ts}")),
        any_instrument(),
        side,
        1.0,
        price,
        Currency::Inr,
        UnixNanos::from_u64(ts),
        UnixNanos::from_u64(ts),
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
    let pnl: Vec<f64> = trips.iter().map(|t| t.net_pnl).collect();
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
