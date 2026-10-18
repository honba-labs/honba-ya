use async_trait::async_trait;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal_macros::dec;

use honba_actors::Strategy;
use honba_backtest::*;
use honba_core::*;
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};

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
