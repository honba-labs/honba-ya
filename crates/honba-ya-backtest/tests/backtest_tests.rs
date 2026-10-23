use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use honba_backtest::*;
// `honba_core::*` also exports a (different, live-trading) `MarketEvent`;
// this explicit import resolves the glob ambiguity in favor of the
// backtest-engine's own event type.
use honba_backtest::MarketEvent;
use honba_core::*;
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_nou_agent::{LlmRecord, PromptReplayer};
use honba_strategy::{OrderIntent, Strategy, StrategyContext};

struct MockNseStrategy {
    symbol: InstrumentId,
    buy_threshold: Price,
    sell_threshold: Price,
    bought: bool,
    sold: bool,
}

impl Strategy for MockNseStrategy {
    fn name(&self) -> &str {
        "algo-nse-equity"
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        trade: &Tick,
    ) -> honba_strategy::Result<()> {
        if let Some(price) = trade.last_price() {
            if !self.bought && price <= self.buy_threshold {
                self.bought = true;
                ctx.submit(OrderIntent::limit_buy(
                    self.symbol.clone(),
                    Quantity::from_u64(100),
                    price,
                ));
            } else if self.bought && !self.sold && price >= self.sell_threshold {
                self.sold = true;
                ctx.submit(OrderIntent::limit_sell(
                    self.symbol.clone(),
                    Quantity::from_u64(100),
                    price,
                ));
            }
        }
        Ok(())
    }
}

#[test]
fn test_historical_day_backtest_with_indian_cost_and_settlement() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let buy_price = Price::from_rupees("1500.00").unwrap();
    let sell_price = Price::from_rupees("1550.00").unwrap();

    let strategy = MockNseStrategy {
        symbol: inst.clone(),
        buy_threshold: buy_price,
        sell_threshold: sell_price,
        bought: false,
        sold: false,
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(dec!(1_000_000.00)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo("algo-nse-equity");

    // Scrip-wise price band: 10% around 1500 (1350 to 1650) with reference price 1500
    let band = PriceBand::new(
        Price::from_rupees("1350.00").unwrap(),
        Price::from_rupees("1650.00").unwrap(),
        Price::from_rupees("1500.00").unwrap(),
    );
    risk_engine.set_price_band(inst.clone(), band);

    let config = BacktestConfig {
        starting_cash: dec!(500_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let calendar = NseTradingCalendar::new();

    let mut engine = BacktestEngine::new(strategy, risk_engine, config, calendar);

    // Trade Date: Wednesday, Jan 15, 2025
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let t_open = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()));
    let t_mid = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(11, 30, 0).unwrap()));
    let t_close = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap()));

    let events = vec![
        // 1. Initial tick at ₹1520 (no signal)
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1520.00").unwrap(),
            volume: Quantity::from_u64(100),
            exchange_ts: t_open,
            receive_ts: t_open,
        }),
        // 2. Dip to ₹1500 (triggers BUY 100 shares @ ₹1500 = ₹150,000 notional)
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1500.00").unwrap(),
            volume: Quantity::from_u64(200),
            exchange_ts: t_mid,
            receive_ts: t_mid,
        }),
        // 3. Rally to ₹1550 (triggers SELL 100 shares @ ₹1550 = ₹155,000 notional)
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1550.00").unwrap(),
            volume: Quantity::from_u64(300),
            exchange_ts: t_close,
            receive_ts: t_close,
        }),
    ];

    let result = engine.run_events(events);

    // Validation
    assert_eq!(result.total_signals, 2);
    assert_eq!(result.refused_orders, 0);
    assert_eq!(result.total_trades, 2);

    // Buy notional: ₹150,000. Sell notional: ₹155,000. Gross profit: ₹5,000.
    // Statutory taxes:
    // Buy STT (0.1%): ₹150.00
    // Sell STT (0.1%): ₹155.00
    // Total STT: ₹305.00
    assert_eq!(result.total_fees.stt, dec!(305.00));

    // Stamp duty (buy only 0.015% of 150k): ₹22.50
    assert_eq!(result.total_fees.stamp_duty, dec!(22.50));

    // DP charges on delivery sell: ₹13.50
    assert_eq!(result.total_fees.dp_charge, dec!(13.50));

    // Brokerage: ₹20 on buy + ₹20 on sell = ₹40
    assert_eq!(result.total_fees.brokerage, dec!(40.00));

    // GST, Exchange turnover, and SEBI turnover are strictly positive
    assert!(result.total_fees.exchange_turnover > dec!(5.00));
    assert!(result.total_fees.sebi_turnover > dec!(0.20));
    assert!(result.total_fees.gst > dec!(10.00));

    // Gross PnL is now real FIFO-realized PnL (via the fixed
    // honba_portfolio::apply_fill), not a notional-sum approximation:
    // close a 100-share long bought @1500 with a sell @1550 = 100*(1550-1500).
    assert_eq!(result.gross_pnl, dec!(5000.00));

    // Net PnL is Gross Profit (₹5,000) minus total statutory fees
    let total_cost = result.total_fees.total();
    assert_eq!(result.net_pnl, dec!(5000.00) - total_cost);
    assert!(result.final_equity > dec!(504_000.00));

    assert_eq!(result.fills.len(), result.total_trades);
}

struct PromptReplayStrategy {
    replayer: PromptReplayer,
    algo_id: String,
}

impl Strategy for PromptReplayStrategy {
    fn name(&self) -> &str {
        &self.algo_id
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _trade: &Tick,
    ) -> honba_strategy::Result<()> {
        for order in self.replayer.next_orders() {
            ctx.submit(OrderIntent {
                instrument_id: order.instrument,
                side: order.side,
                quantity: order.quantity,
                order_type: order.order_type,
                time_in_force: order.tif,
            });
        }
        Ok(())
    }
}

#[test]
fn test_deterministic_llm_prompt_replay_backtest() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let algo_id = "SEBI_AI_REPLAY_01";

    // 1. Create recorded LLM decision trail with SHA256 checksums
    let prompt1 = "Tick 1: RELIANCE at 2500, RSI 28 (oversold). Recommendation?";
    let raw1 = r#"{"action": "place_order", "symbol": "RELIANCE", "side": "BUY", "qty": "50", "price_limit": "2500.00", "algo_id": "SEBI_AI_REPLAY_01", "rationale": "Oversold bounce entry"}"#;
    let d1: Decision = serde_json::from_str(raw1).unwrap();

    let prompt2 = "Tick 2: RELIANCE at 2550, RSI 72 (overbought). Recommendation?";
    let raw2 = r#"{"action": "place_order", "symbol": "RELIANCE", "side": "SELL", "qty": "50", "price_limit": "2550.00", "algo_id": "SEBI_AI_REPLAY_01", "rationale": "Overbought profit take"}"#;
    let d2: Decision = serde_json::from_str(raw2).unwrap();

    let r1 = LlmRecord::new(
        prompt1,
        raw1,
        "gpt-4o-2024-08-06",
        Some(100),
        Some(0.0),
        Some(d1),
    );
    let r2 = LlmRecord::new(
        prompt2,
        raw2,
        "gpt-4o-2024-08-06",
        Some(100),
        Some(0.0),
        Some(d2),
    );

    let replayer = PromptReplayer::new(algo_id, vec![r1, r2]).unwrap();
    let strategy = PromptReplayStrategy {
        replayer,
        algo_id: algo_id.to_string(),
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(dec!(1_000_000.00)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo(algo_id);
    let band = PriceBand::new(
        Price::from_rupees("2000.00").unwrap(),
        Price::from_rupees("3000.00").unwrap(),
        Price::from_rupees("2500.00").unwrap(),
    );
    risk_engine.set_price_band(inst.clone(), band);

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 2, 10).unwrap();
    let t1 = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 30, 0).unwrap()));
    let t2 = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(14, 0, 0).unwrap()));

    let events = vec![
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("2500.00").unwrap(),
            volume: Quantity::from_u64(100),
            exchange_ts: t1,
            receive_ts: t1,
        }),
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("2550.00").unwrap(),
            volume: Quantity::from_u64(100),
            exchange_ts: t2,
            receive_ts: t2,
        }),
    ];

    let result = engine.run_events(events);

    // Verify deterministic replay execution
    assert_eq!(result.total_signals, 2);
    assert_eq!(result.refused_orders, 0);
    assert_eq!(result.total_trades, 2);

    // Buy: 50 * 2500 = 125,000. Sell: 50 * 2550 = 127,500.
    // Gross PnL is real FIFO-realized PnL: 50 * (2550 - 2500) = 2,500.
    assert_eq!(result.gross_pnl, dec!(2500.00));
    assert!(result.total_fees.total() > Decimal::ZERO);
    assert!(result.final_equity > dec!(1_000_000.00));
}

/// Numeric parity: the engine's accumulated `total_fees` must equal exactly
/// what `TaxCalculator::compute_charges` (the canonical kokoro entry point
/// the engine calls internally) produces for the same fills, proving the
/// engine isn't quietly applying some other formula or rounding rule.
#[test]
fn test_backtest_fees_match_kokoro_tax_calculator() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let buy_price = Price::from_rupees("1500.00").unwrap();
    let sell_price = Price::from_rupees("1550.00").unwrap();

    let strategy = MockNseStrategy {
        symbol: inst.clone(),
        buy_threshold: buy_price,
        sell_threshold: sell_price,
        bought: false,
        sold: false,
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(dec!(1_000_000.00)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo("algo-nse-equity");
    let band = PriceBand::new(
        Price::from_rupees("1350.00").unwrap(),
        Price::from_rupees("1650.00").unwrap(),
        Price::from_rupees("1500.00").unwrap(),
    );
    risk_engine.set_price_band(inst.clone(), band);

    let config = BacktestConfig {
        starting_cash: dec!(500_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let t_open = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()));
    let t_mid = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(11, 30, 0).unwrap()));
    let t_close = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap()));

    let events = vec![
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1520.00").unwrap(),
            volume: Quantity::from_u64(100),
            exchange_ts: t_open,
            receive_ts: t_open,
        }),
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1500.00").unwrap(),
            volume: Quantity::from_u64(200),
            exchange_ts: t_mid,
            receive_ts: t_mid,
        }),
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price: Price::from_rupees("1550.00").unwrap(),
            volume: Quantity::from_u64(300),
            exchange_ts: t_close,
            receive_ts: t_close,
        }),
    ];

    let result = engine.run_events(events);

    // Recompute expected fees directly from the same canonical
    // TaxCalculator the engine calls internally, for the two known fills:
    // Buy 100 @ 1500 (not the first sell of the day; irrelevant for a buy)
    // and Sell 100 @ 1550 (the first sell of the day for this scrip).
    let fees_buy = TaxCalculator::compute_charges(
        TradeSide::Buy,
        ProductType::Delivery,
        TaxExchange::Nse,
        dec!(1500.00),
        dec!(100),
        false,
    );
    let fees_sell = TaxCalculator::compute_charges(
        TradeSide::Sell,
        ProductType::Delivery,
        TaxExchange::Nse,
        dec!(1550.00),
        dec!(100),
        true,
    );
    let mut expected = TaxBreakdown::default();
    expected.add_assign(&fees_buy);
    expected.add_assign(&fees_sell);

    assert_eq!(result.total_fees, expected);
}

/// `decimal_qty_to_u64` is the explicit, tested boundary conversion between
/// `Decimal` share quantities and the `u64` that
/// `honba_portfolio::apply_fill`/`apply_fill_to_account` require.
#[test]
fn test_decimal_qty_to_u64_truncates_fractional_part() {
    assert_eq!(decimal_qty_to_u64(dec!(10.75)), 10);
    assert_eq!(decimal_qty_to_u64(dec!(10.00)), 10);
    assert_eq!(decimal_qty_to_u64(dec!(0.99)), 0);
}

#[test]
fn test_decimal_qty_to_u64_saturates_non_positive_to_zero() {
    assert_eq!(decimal_qty_to_u64(dec!(-5.00)), 0);
    assert_eq!(decimal_qty_to_u64(Decimal::ZERO), 0);
}

/// Proves T+1 settlement timing is driven by `ClearingCalendar` (which
/// correctly rolls a settlement date past weekends), not a naive
/// calendar-day+1: a Friday buy must NOT show as settled by Saturday's
/// event, but must have settled by Monday's.
#[test]
fn test_t1_settlement_uses_clearing_calendar() {
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    // Thresholds set so the strategy buys unconditionally on the very
    // first trade it sees, and never sells.
    let strategy = MockNseStrategy {
        symbol: inst.clone(),
        buy_threshold: Price::from_rupees("999999.00").unwrap(),
        sell_threshold: Price::from_rupees("999999.00").unwrap(),
        bought: false,
        sold: false,
    };

    let limits = RiskLimits {
        max_notional_per_order: Some(dec!(1_000_000.00)),
        ..Default::default()
    };
    let mut risk_engine = RiskEngine::new(limits, OtrConfig::default());
    risk_engine.register_algo("algo-nse-equity");
    let band = PriceBand::new(
        Price::from_rupees("900.00").unwrap(),
        Price::from_rupees("1100.00").unwrap(),
        Price::from_rupees("1000.00").unwrap(),
    );
    risk_engine.set_price_band(inst.clone(), band);

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    // Friday Jan 17, 2025 -> Saturday Jan 18 -> Monday Jan 20, 2025.
    let friday = NaiveDate::from_ymd_opt(2025, 1, 17).unwrap();
    let saturday = NaiveDate::from_ymd_opt(2025, 1, 18).unwrap();
    let monday = NaiveDate::from_ymd_opt(2025, 1, 20).unwrap();

    let t_fri = Utc.from_utc_datetime(&friday.and_time(NaiveTime::from_hms_opt(9, 20, 0).unwrap()));
    let t_sat = Utc.from_utc_datetime(&saturday.and_time(NaiveTime::from_hms_opt(9, 20, 0).unwrap()));
    let t_mon = Utc.from_utc_datetime(&monday.and_time(NaiveTime::from_hms_opt(9, 20, 0).unwrap()));

    let price = Price::from_rupees("1000.00").unwrap();
    let events = vec![
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price,
            volume: Quantity::from_u64(100),
            exchange_ts: t_fri,
            receive_ts: t_fri,
        }),
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price,
            volume: Quantity::from_u64(100),
            exchange_ts: t_sat,
            receive_ts: t_sat,
        }),
        MarketEvent::Tick(Tick::Ltp {
            instrument: inst.clone(),
            price,
            volume: Quantity::from_u64(100),
            exchange_ts: t_mon,
            receive_ts: t_mon,
        }),
    ];

    let result = engine.run_events(events);

    assert_eq!(result.total_trades, 1); // the one Friday buy
    assert_eq!(result.equity_curve.len(), 3);

    let equity_fri = result.equity_curve[0].1;
    let equity_sat = result.equity_curve[1].1;
    let equity_mon = result.equity_curve[2].1;

    // Between Friday's buy and Saturday's event, nothing has settled yet:
    // a naive calendar-day+1 would have settled by Saturday, but
    // ClearingCalendar correctly treats Saturday as a non-settlement day.
    assert_eq!(equity_fri, equity_sat);

    // By Monday's event the T+1 settlement (correctly rolled past the
    // weekend) has landed: settled cash drops by roughly the trade
    // notional (₹100,000), since the position's mark-to-market value is
    // unchanged (same price throughout).
    assert!(
        equity_mon < equity_sat,
        "settlement must land by Monday, not stay pending forever"
    );
    assert!(
        (equity_sat - equity_mon) > dec!(90_000.00),
        "the settlement drop should be roughly the trade notional, confirming \
         it actually landed on Monday rather than some smaller unrelated change"
    );
}
