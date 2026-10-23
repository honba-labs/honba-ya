//! Comprehensive Phase 4 integration tests for `honba-ya-backtest`:
//! - 4.1 L2 depth-tier walking, limit order matching, spread-crossing penalty,
//!   and `AdverseFillModel` slippage.
//! - 4.2 Double-entry `honba-kokoro-ledger` journal transactions, exact Indian
//!   statutory cost breakdown (delivery vs MIS intraday), and ICCL T+1 Demat
//!   rolling settlement cycles & margin holdbacks.
//! - 4.3 Zero-copy Arrow/Parquet catalog ingestion (`CatalogLoader`) against
//!   both synthetic multi-partition fixtures and the real `honba-data/data/catalog`.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Decimal128Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use honba_backtest::sim::{
    l2_book::OrderBook,
    latency::NetworkLatency,
    mis::MisSquareOffManager,
    slippage::{AdverseFillModel, SlippageModel},
};
use honba_backtest::{
    BacktestConfig, BacktestEngine, CatalogLoader, MarketEvent, NseTradingCalendar, ProductType,
    post_transaction,
};
use honba_core::{
    Bar, BarReader, BarRequest, DepthLevel, Exchange, InstrumentId, OrderSide, Price, Quantity,
    Tick, Timeframe,
};
use honba_mon::{OtrConfig, RiskEngine, RiskLimits};
use honba_strategy::{OrderIntent, Strategy, StrategyContext};

fn risk_engine_for(algo_id: &str) -> RiskEngine {
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(algo_id);
    risk_engine
}

struct SingleMarketBuyer {
    instrument: InstrumentId,
    qty: Quantity,
    done: bool,
}

impl Strategy for SingleMarketBuyer {
    fn name(&self) -> &str {
        "single_market_buyer"
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _tick: &Tick,
    ) -> honba_strategy::Result<()> {
        if !self.done {
            ctx.submit(OrderIntent::market_buy(self.instrument.clone(), self.qty));
            self.done = true;
        }
        Ok(())
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        if !self.done {
            ctx.submit(OrderIntent::market_buy(self.instrument.clone(), self.qty));
            self.done = true;
        }
        Ok(())
    }
}

struct BuyOnDay1SellOnDay2 {
    instrument: InstrumentId,
    qty: Quantity,
    bar_count: usize,
}

impl Strategy for BuyOnDay1SellOnDay2 {
    fn name(&self) -> &str {
        "buy_day1_sell_day2"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> honba_strategy::Result<()> {
        self.bar_count += 1;
        if self.bar_count == 1 {
            ctx.submit(OrderIntent::market_buy(self.instrument.clone(), self.qty));
        } else if self.bar_count == 2 && !ctx.busy(&self.instrument) {
            ctx.submit(OrderIntent::market_sell(self.instrument.clone(), self.qty));
        }
        Ok(())
    }
}

#[test]
fn test_l2_depth_walking_vwap_and_limit_matching() {
    let mut book = OrderBook::new(0, dec!(1.0));
    let bids = vec![
        DepthLevel {
            price: Price::from_rupees("100.00").unwrap(),
            quantity: Quantity::from_u64(50),
            orders_count: Some(2),
        },
        DepthLevel {
            price: Price::from_rupees("99.00").unwrap(),
            quantity: Quantity::from_u64(50),
            orders_count: Some(1),
        },
    ];
    let asks = vec![
        DepthLevel {
            price: Price::from_rupees("101.00").unwrap(),
            quantity: Quantity::from_u64(40),
            orders_count: Some(2),
        },
        DepthLevel {
            price: Price::from_rupees("102.00").unwrap(),
            quantity: Quantity::from_u64(60),
            orders_count: Some(3),
        },
    ];
    book.set_depth(&bids, &asks);

    assert_eq!(
        book.get_spread(),
        (
            Some(Price::from_rupees("100.00").unwrap()),
            Some(Price::from_rupees("101.00").unwrap())
        )
    );
    assert_eq!(
        book.get_mid_price(),
        Some(Price::from_rupees("100.50").unwrap())
    );

    // Market buy 80 shares: consumes 40 @ 101 and 40 @ 102 => VWAP = 101.50, worst = 102.00
    let (filled, vwap, remaining, worst) = book.execute_market_order(OrderSide::Buy, 80);
    assert_eq!(filled, 80);
    assert_eq!(remaining, 0);
    assert_eq!(vwap, Price::from_rupees("101.5000").unwrap());
    assert_eq!(worst, Some(Price::from_rupees("102.00").unwrap()));

    // Reset depth and test limit buy @ 101.00 for 80 shares: only 40 @ 101.00 can fill
    book.set_depth(&bids, &asks);
    let (l_filled, l_vwap, l_rem) =
        book.execute_limit_order(OrderSide::Buy, 80, Price::from_rupees("101.00").unwrap());
    assert_eq!(l_filled, 40);
    assert_eq!(l_vwap, Price::from_rupees("101.0000").unwrap());
    assert_eq!(l_rem, 40);
}

#[test]
fn test_spread_crossing_penalty_and_adverse_fill_slippage_in_engine() {
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let strategy = SingleMarketBuyer {
        instrument: inst.clone(),
        qty: Quantity::from_u64(60),
        done: false,
    };
    let risk_engine = risk_engine_for(strategy.name());

    let mut slippage = SlippageModel::new();
    slippage.set_daily_range_pct(dec!(1.0));
    slippage.set_avg_spread_pct(dec!(0.002));

    let adverse = AdverseFillModel::new(3, dec!(5.0), true);
    let config = BacktestConfig::with_l2_simulation(
        dec!(1_000_000.00),
        ProductType::Delivery,
        NetworkLatency::new(0, 0, 42),
        slippage,
        MisSquareOffManager::new(NseTradingCalendar::new()),
    )
    .with_adverse_fill(adverse);

    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let ts = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 20, 0).unwrap()));

    let depth_tick = Tick::Depth {
        instrument: inst.clone(),
        bids: vec![DepthLevel {
            price: Price::from_rupees("2499.00").unwrap(),
            quantity: Quantity::from_u64(100),
            orders_count: Some(3),
        }],
        asks: vec![
            DepthLevel {
                price: Price::from_rupees("2500.00").unwrap(),
                quantity: Quantity::from_u64(30),
                orders_count: Some(2),
            },
            DepthLevel {
                price: Price::from_rupees("2502.00").unwrap(),
                quantity: Quantity::from_u64(50),
                orders_count: Some(2),
            },
        ],
        last_price: Some(Price::from_rupees("2500.00").unwrap()),
        exchange_ts: ts,
        receive_ts: ts,
    };

    // Trigger `on_trade` with a zero-volume LTP tick so the L2 ladder installed by `depth_tick` is preserved
    let trigger_ltp = Tick::Ltp {
        instrument: inst,
        price: Price::from_rupees("2500.00").unwrap(),
        volume: Quantity::ZERO,
        exchange_ts: ts,
        receive_ts: ts,
    };

    let result = engine.run_events(vec![
        MarketEvent::Tick(depth_tick),
        MarketEvent::Tick(trigger_ltp),
    ]);

    assert_eq!(result.total_trades, 1);
    assert!(
        result.price_impact > Decimal::ZERO,
        "L2 depth walk + spread crossing + adverse fill model must record positive price_impact"
    );
    // Adverse fill model + 2-level ask walk pushes fill price above 2500.00
    assert!(result.fills[0].price.0 > dec!(2500.00));
}

#[test]
fn test_iccl_t1_demat_settlement_margin_holdbacks_and_double_entry_ledger() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let strategy = BuyOnDay1SellOnDay2 {
        instrument: inst.clone(),
        qty: Quantity::from_u64(100),
        bar_count: 0,
    };
    let risk_engine = risk_engine_for(strategy.name());
    let config = BacktestConfig {
        starting_cash: dec!(500_000.00),
        product_type: ProductType::Delivery,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    // Day 1: Wednesday Jan 15, 2025 (Buy 100 INFY @ 1500)
    // Day 2: Thursday Jan 16, 2025 (T+1 settles Day 1 buy into free Demat, then Sells 100 INFY @ 1550)
    let d1 = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
    let t1 = Utc.from_utc_datetime(&d1.and_time(NaiveTime::from_hms_opt(10, 0, 0).unwrap()));
    let t2 = Utc.from_utc_datetime(&d2.and_time(NaiveTime::from_hms_opt(10, 0, 0).unwrap()));

    let p1 = Price::from_rupees("1500.00").unwrap();
    let p2 = Price::from_rupees("1550.00").unwrap();

    let events_day1 = vec![MarketEvent::Bar(Bar {
        instrument: inst.clone(),
        open: p1,
        high: p1,
        low: p1,
        close: p1,
        volume: Quantity::from_u64(10_000),
        timestamp: t1,
    })];

    let mut engine_step = BacktestEngine::new(
        SingleMarketBuyer {
            instrument: inst.clone(),
            qty: Quantity::from_u64(100),
            done: false,
        },
        risk_engine_for("single_market_buyer"),
        BacktestConfig {
            starting_cash: dec!(500_000.00),
            product_type: ProductType::Delivery,
            ..Default::default()
        },
        NseTradingCalendar::new(),
    );
    let res_day1 = engine_step.run_events(events_day1.clone());
    // After run_events finishes, final flush_settlements(next_day) settles the T+1 receivable into free Demat!
    assert_eq!(res_day1.total_trades, 1);
    assert_eq!(
        engine_step.demat_holdings().free_holdings(&inst),
        Quantity::from_u64(100),
        "T+1 settlement must move 100 bought shares from blocked to free Demat holdings"
    );
    assert_eq!(
        engine_step.margin_blocked(),
        Decimal::ZERO,
        "margin holdback must be released once T+1 settlement completes"
    );

    // Now run the 2-day buy + sell sequence and verify double-entry ledger transactions
    let result = engine.run_events(vec![
        events_day1[0].clone(),
        MarketEvent::Bar(Bar {
            instrument: inst.clone(),
            open: p2,
            high: p2,
            low: p2,
            close: p2,
            volume: Quantity::from_u64(10_000),
            timestamp: t2,
        }),
    ]);

    assert_eq!(result.total_trades, 2);
    assert_eq!(result.ledger_transactions.len(), 2);
    for tx in &result.ledger_transactions {
        assert!(
            post_transaction(tx).is_ok(),
            "every fill must post a balanced double-entry transaction"
        );
    }
    // After Day 1 buy (100) and Day 2 sell (100) + T+1 settlement, free Demat holdings are back to 0
    assert_eq!(engine.demat_holdings().free_holdings(&inst), Quantity::ZERO);
}

#[test]
fn test_intraday_mis_statutory_cost_rates() {
    let inst = InstrumentId::new(Exchange::Nse, "SBIN");
    let strategy = BuyOnDay1SellOnDay2 {
        instrument: inst.clone(),
        qty: Quantity::from_u64(100),
        bar_count: 0,
    };
    let risk_engine = risk_engine_for(strategy.name());
    let config = BacktestConfig {
        starting_cash: dec!(500_000.00),
        product_type: ProductType::Intraday,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strategy, risk_engine, config, NseTradingCalendar::new());

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let t1 = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 30, 0).unwrap()));
    let t2 = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(10, 30, 0).unwrap()));
    let p_buy = Price::from_rupees("800.00").unwrap();
    let p_sell = Price::from_rupees("820.00").unwrap();

    let result = engine.run_events(vec![
        MarketEvent::Bar(Bar {
            instrument: inst.clone(),
            open: p_buy,
            high: p_buy,
            low: p_buy,
            close: p_buy,
            volume: Quantity::from_u64(5_000),
            timestamp: t1,
        }),
        MarketEvent::Bar(Bar {
            instrument: inst.clone(),
            open: p_sell,
            high: p_sell,
            low: p_sell,
            close: p_sell,
            volume: Quantity::from_u64(5_000),
            timestamp: t2,
        }),
    ]);

    assert_eq!(result.total_trades, 2);
    // MIS Intraday STT: 0 on buy, 0.025% on sell (82,000 * 0.00025 = ₹20.50)
    assert_eq!(result.total_fees.stt, dec!(20.50));
    // MIS Intraday Stamp Duty: 0.003% on buy (80,000 * 0.00003 = ₹2.40), 0 on sell
    assert_eq!(result.total_fees.stamp_duty, dec!(2.40));
    // MIS Intraday DP charge: ₹0.00 (no Demat delivery)
    assert_eq!(result.total_fees.dp_charge, Decimal::ZERO);
}

#[test]
fn test_catalog_loader_synthetic_partitions_and_bar_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let sym_dir = tmp
        .path()
        .join("catalog")
        .join("1D")
        .join("NSE")
        .join("INFY");
    std::fs::create_dir_all(&sym_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));

    let d1 = Utc
        .from_utc_datetime(
            &NaiveDate::from_ymd_opt(2025, 1, 15)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()),
        )
        .timestamp_nanos_opt()
        .unwrap();
    let d2 = Utc
        .from_utc_datetime(
            &NaiveDate::from_ymd_opt(2025, 1, 16)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()),
        )
        .timestamp_nanos_opt()
        .unwrap();
    let d3 = Utc
        .from_utc_datetime(
            &NaiveDate::from_ymd_opt(2025, 1, 17)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()),
        )
        .timestamp_nanos_opt()
        .unwrap();

    // Partition 1: d1, d2
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![d1, d2])),
            Arc::new(Float64Array::from(vec![1500.0, 1510.0])),
            Arc::new(Float64Array::from(vec![1520.0, 1530.0])),
            Arc::new(Float64Array::from(vec![1490.0, 1505.0])),
            Arc::new(Float64Array::from(vec![1510.0, 1525.0])),
            Arc::new(Float64Array::from(vec![10000.0, 12000.0])),
        ],
    )
    .unwrap();
    let f1 = std::fs::File::create(sym_dir.join("2025_part1.parquet")).unwrap();
    let mut w1 = ArrowWriter::try_new(f1, schema.clone(), None).unwrap();
    w1.write(&batch1).unwrap();
    w1.close().unwrap();

    // Partition 2: overlapping d2, plus d3
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![d2, d3])),
            Arc::new(Float64Array::from(vec![1510.0, 1525.0])),
            Arc::new(Float64Array::from(vec![1530.0, 1560.0])),
            Arc::new(Float64Array::from(vec![1505.0, 1520.0])),
            Arc::new(Float64Array::from(vec![1525.0, 1550.0])),
            Arc::new(Float64Array::from(vec![12000.0, 15000.0])),
        ],
    )
    .unwrap();
    let f2 = std::fs::File::create(sym_dir.join("2025_part2.parquet")).unwrap();
    let mut w2 = ArrowWriter::try_new(f2, schema, None).unwrap();
    w2.write(&batch2).unwrap();
    w2.close().unwrap();

    let loader = CatalogLoader::new(tmp.path());
    assert_eq!(loader.list_timeframes().unwrap(), vec!["1D".to_string()]);
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    assert_eq!(loader.list_instruments("1d").unwrap(), vec![inst.clone()]);

    // Overlapping boundary row d2 is deduplicated => 3 unique bars
    let bars = loader.load_bars(&inst, "1D", None, None).unwrap();
    assert_eq!(bars.len(), 3);
    assert_eq!(bars[0].close, Price::from_rupees("1510").unwrap());
    assert_eq!(bars[1].close, Price::from_rupees("1525").unwrap());
    assert_eq!(bars[2].close, Price::from_rupees("1550").unwrap());

    // Verify `honba_core::BarReader` trait implementation
    let req = BarRequest {
        instrument: inst.clone(),
        timeframe: Timeframe::D1,
        from: DateTime::<Utc>::from_timestamp_nanos(d1),
        to: DateTime::<Utc>::from_timestamp_nanos(d3),
    };
    let via_trait = loader.read_bars(&req).unwrap();
    assert_eq!(via_trait.len(), 3);

    // Run end-to-end backtest directly via `BacktestEngine::run_catalog`
    let strategy = BuyOnDay1SellOnDay2 {
        instrument: inst.clone(),
        qty: Quantity::from_u64(10),
        bar_count: 0,
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine_for("buy_day1_sell_day2"),
        BacktestConfig::default(),
        NseTradingCalendar::new(),
    );
    let result = engine
        .run_catalog(&loader, &[inst], "1D", None, None)
        .unwrap();
    assert_eq!(result.total_trades, 2);
    // Bought 10 @ 1510, Sold 10 @ 1525 => gross PnL = 150
    assert_eq!(result.gross_pnl, dec!(150.00));
}

#[test]
fn test_catalog_loader_lakehouse_v2_decimal128_schema() {
    let tmp = tempfile::tempdir().unwrap();
    let part_dir = tmp
        .path()
        .join("bars")
        .join("timeframe=1d")
        .join("exchange=NSE")
        .join("year=2025")
        .join("month=01");
    std::fs::create_dir_all(&part_dir).unwrap();

    let dec_field = |name: &str| Field::new(name, DataType::Decimal128(18, 4), false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        dec_field("open"),
        dec_field("high"),
        dec_field("low"),
        dec_field("close"),
        dec_field("volume"),
    ]));

    let t1 = Utc
        .from_utc_datetime(
            &NaiveDate::from_ymd_opt(2025, 1, 15)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(9, 15, 0).unwrap()),
        )
        .timestamp_nanos_opt()
        .unwrap();

    let mk_dec = |v: i128| {
        Arc::new(
            Decimal128Array::from(vec![Some(v)])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        ) as Arc<dyn arrow::array::Array>
    };

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["TCS"])),
            Arc::new(StringArray::from(vec!["NSE"])),
            Arc::new(TimestampNanosecondArray::from(vec![t1]).with_timezone("UTC")),
            mk_dec(3500_0000),
            mk_dec(3550_0000),
            mk_dec(3480_0000),
            mk_dec(3525_5000),
            mk_dec(5000_0000),
        ],
    )
    .unwrap();

    let f = std::fs::File::create(part_dir.join("TCS.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(f, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let loader = CatalogLoader::new(tmp.path());
    let inst = InstrumentId::new(Exchange::Nse, "TCS");
    let bars = loader.load_bars(&inst, "1d", None, None).unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].close, Price(dec!(3525.5000)));
    assert_eq!(bars[0].volume, Quantity(dec!(5000.0000)));
}

#[test]
fn test_real_honba_data_catalog_ingestion_if_present() {
    let workspace_catalog =
        Path::new("/home/pmallapp/Devel/Finance/honba-workspace/honba-data/data");
    if !workspace_catalog.join("catalog").is_dir() {
        return;
    }

    let loader = CatalogLoader::new(workspace_catalog);
    let timeframes = loader.list_timeframes().unwrap();
    assert!(timeframes.iter().any(|tf| tf.eq_ignore_ascii_case("1D")));

    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    let bars_5m = loader.load_bars(&inst, "5M", None, None).unwrap();
    assert!(
        !bars_5m.is_empty(),
        "expected non-empty 5M INFY bars from honba-data/data/catalog"
    );

    // Run a deterministic backtest over the first 20 bars from the real catalog
    let strategy = BuyOnDay1SellOnDay2 {
        instrument: inst.clone(),
        qty: Quantity::from_u64(10),
        bar_count: 0,
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine_for("buy_day1_sell_day2"),
        BacktestConfig::default(),
        NseTradingCalendar::new(),
    );
    let sample_events: Vec<MarketEvent> =
        bars_5m.into_iter().take(20).map(MarketEvent::Bar).collect();
    let result = engine.run_events(sample_events);
    assert_eq!(result.total_trades, 2);
    assert_eq!(result.ledger_transactions.len(), 2);
}
