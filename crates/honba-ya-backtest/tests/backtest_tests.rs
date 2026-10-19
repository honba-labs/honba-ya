use async_trait::async_trait;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use honba_actors::Strategy;
use honba_backtest::*;
use honba_core::*;
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_nou_agent::{LlmRecord, PromptReplayer};

struct MockNseStrategy {
    symbol: InstrumentId,
    buy_threshold: Price,
    sell_threshold: Price,
    bought: bool,
    sold: bool,
}

#[async_trait]
impl Strategy for MockNseStrategy {
    fn name(&self) -> &str {
        "MockNseStrategy"
    }

    fn algo_id(&self) -> &str {
        "algo-nse-equity"
    }

    async fn on_tick(&mut self, tick: &Tick) -> Vec<OrderRequest> {
        if let Some(price) = tick.last_price() {
            if !self.bought && price <= self.buy_threshold {
                self.bought = true;
                return vec![OrderRequest::new(
                    self.symbol.clone(),
                    OrderSide::Buy,
                    OrderType::Limit(price),
                    Quantity::from_u64(100),
                    self.algo_id(),
                )];
            } else if self.bought && !self.sold && price >= self.sell_threshold {
                self.sold = true;
                return vec![OrderRequest::new(
                    self.symbol.clone(),
                    OrderSide::Sell,
                    OrderType::Limit(price),
                    Quantity::from_u64(100),
                    self.algo_id(),
                )];
            }
        }
        Vec::new()
    }
}

#[tokio::test]
async fn test_historical_day_backtest_with_indian_cost_and_settlement() {
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
    };
    let cost_model = IndianCostModel::new();
    let calendar = NseTradingCalendar::new();

    let mut engine = BacktestEngine::new(strategy, risk_engine, config, cost_model, calendar);

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

    let result = engine.run_events(events).await;

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

    // Net PnL is Gross Profit (₹5,000) minus total statutory fees
    let total_cost = result.total_fees.total();
    assert_eq!(result.net_pnl, dec!(5000.00) - total_cost);
    assert!(result.final_equity > dec!(504_000.00));
}

struct PromptReplayStrategy {
    replayer: PromptReplayer,
    algo_id: String,
}

#[async_trait]
impl Strategy for PromptReplayStrategy {
    fn name(&self) -> &str {
        "PromptReplayStrategy"
    }

    fn algo_id(&self) -> &str {
        &self.algo_id
    }

    async fn on_tick(&mut self, _tick: &Tick) -> Vec<OrderRequest> {
        self.replayer.next_orders()
    }
}

#[tokio::test]
async fn test_deterministic_llm_prompt_replay_backtest() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let algo_id = "SEBI_AI_REPLAY_01";

    // 1. Create recorded LLM decision trail with SHA256 checksums
    let prompt1 = "Tick 1: RELIANCE at 2500, RSI 28 (oversold). Recommendation?";
    let raw1 = r#"{"action": "buy", "symbol": "RELIANCE", "exchange": "NSE", "qty": "50", "price_limit": "2500.00", "algo_id": "SEBI_AI_REPLAY_01", "rationale": "Oversold bounce entry"}"#;
    let d1: Decision = serde_json::from_str(raw1).unwrap();

    let prompt2 = "Tick 2: RELIANCE at 2550, RSI 72 (overbought). Recommendation?";
    let raw2 = r#"{"action": "sell", "symbol": "RELIANCE", "exchange": "NSE", "qty": "50", "price_limit": "2550.00", "algo_id": "SEBI_AI_REPLAY_01", "rationale": "Overbought profit take"}"#;
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
    let band = PriceBand::new(
        Price::from_rupees("2000.00").unwrap(),
        Price::from_rupees("3000.00").unwrap(),
        Price::from_rupees("2500.00").unwrap(),
    );
    risk_engine.set_price_band(inst.clone(), band);

    let config = BacktestConfig {
        starting_cash: dec!(1_000_000.00),
        product_type: ProductType::Delivery,
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        IndianCostModel::new(),
        NseTradingCalendar::new(),
    );

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

    let result = engine.run_events(events).await;

    // Verify deterministic replay execution
    assert_eq!(result.total_signals, 2);
    assert_eq!(result.refused_orders, 0);
    assert_eq!(result.total_trades, 2);

    // Buy: 50 * 2500 = 125,000. Sell: 50 * 2550 = 127,500. Gross: 2,500.
    assert_eq!(result.gross_pnl, dec!(127500.00));
    assert!(result.total_fees.total() > Decimal::ZERO);
    assert!(result.final_equity > dec!(1_000_000.00));
}
