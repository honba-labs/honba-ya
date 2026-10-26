use chrono::Utc;
use rust_decimal_macros::dec;

use honba_core::{
    Currency, Exchange, InstrumentId, Money, OrderSide, OrderStatus, OrderType, PositionSide,
    Price, Quantity, TimeInForce,
};
use honba_kokoro::repository::{
    AccountCode, AccountId, AccountRecord, AccountRepository, AlgoId, ClientOrderId, FillReport,
    JournalTransaction, LedgerRepository, OrderRecord, OrderRepository, PositionRepository,
    PostingEntry, TaxLot, TransactionId,
};
use honba_persistence::migrations::migrate;
use honba_persistence::{
    Database, SqlxAccountStore, SqlxLedgerStore, SqlxOrderStore, SqlxPositionStore,
};

#[tokio::test]
async fn test_sqlx_order_repository_ddd() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    migrate(&db).await.unwrap();

    let mut store = SqlxOrderStore::new(&db);
    let cid = ClientOrderId::new(uuid::Uuid::new_v4().to_string());
    let algo = AlgoId::new("algo_momentum");
    let acc = AccountId::new("ACC_123");
    let instrument = InstrumentId::new(Exchange::Nse, "RELIANCE");

    let record = OrderRecord {
        client_order_id: cid.clone(),
        broker_order_id: Some("BROKER_999".to_string()),
        algo_id: Some(algo.clone()),
        account_id: acc.clone(),
        instrument: instrument.clone(),
        side: OrderSide::Buy,
        order_type: OrderType::Limit(Price(dec!(2500.00))),
        time_in_force: TimeInForce::Day,
        price: Some(Price(dec!(2500.00))),
        quantity: Quantity::from_u64(10),
        filled_quantity: Quantity::ZERO,
        avg_fill_price: None,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    // 1. Save and retrieve via OrderRepository trait
    store.save_order(&record).unwrap();
    let retrieved = store.get_order(&cid).unwrap().expect("order found");
    assert_eq!(retrieved.client_order_id, cid);
    assert_eq!(retrieved.quantity, Quantity::from_u64(10));

    // 2. Query active orders
    let active = store.list_active_orders(&algo).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].client_order_id, cid);

    // 3. Persist to relational sqlx table
    store.persist_to_db(&record).await.unwrap();
}

#[tokio::test]
async fn test_sqlx_position_repository_ddd() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    migrate(&db).await.unwrap();

    let mut store = SqlxPositionStore::new(&db);
    let acc = AccountId::new("ACC_123");
    let instrument = InstrumentId::new(Exchange::Nse, "TCS");

    let lot = TaxLot {
        lot_id: "LOT_1".to_string(),
        account_id: acc.clone(),
        instrument: instrument.clone(),
        side: PositionSide::Long,
        entry_price: Price(dec!(3500.00)),
        quantity: Quantity::from_u64(20),
        remaining_quantity: Quantity::from_u64(20),
        entry_date: Utc::now().date_naive(),
        created_at: Utc::now(),
    };

    store.save_lot(&acc, &lot).unwrap();
    let positions = store.get_positions(&acc).unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, Quantity::from_u64(20));

    // Match lots on closing sell
    let fill = FillReport {
        trade_id: "TRADE_1".to_string(),
        order_id: ClientOrderId::new("ORD_1"),
        account_id: acc.clone(),
        instrument: instrument.clone(),
        side: OrderSide::Sell,
        price: Price(dec!(3600.00)),
        quantity: Quantity::from_u64(10),
        fee: Money::inr(dec!(15.00)),
        timestamp: Utc::now(),
    };

    let matched = store.match_lots(&fill).unwrap();
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].matched_quantity, Quantity::from_u64(10));
    assert_eq!(matched[0].realized_pnl, Money::inr(dec!(1000.00))); // (3600 - 3500) * 10
}

#[tokio::test]
async fn test_sqlx_account_repository_ddd() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    migrate(&db).await.unwrap();

    let mut store = SqlxAccountStore::new(&db);
    let acc = AccountId::new("ACC_999");
    let algo = AlgoId::new("algo_hft");

    let record = AccountRecord {
        account_id: acc.clone(),
        algo_id: Some(algo.clone()),
        cash: Money::inr(dec!(1_000_000.00)),
        status: "active".to_string(),
        updated_at: Utc::now(),
    };

    store.save_account(&record).unwrap();
    let retrieved = store.get_account(&acc).unwrap().expect("account found");
    assert_eq!(retrieved.cash, Money::inr(dec!(1_000_000.00)));

    let list = store.list_accounts().unwrap();
    assert_eq!(list.len(), 1);

    // Persist to relational table
    store.persist_to_db(&record).await.unwrap();
}

#[tokio::test]
async fn test_sqlx_ledger_repository_ddd() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    migrate(&db).await.unwrap();

    let mut store = SqlxLedgerStore::new(&db, Currency::Inr);
    let tx_id = TransactionId::new("TX_100");

    let tx = JournalTransaction::new(
        tx_id.clone(),
        Utc::now(),
        "Capital funding",
        vec![PostingEntry {
            account: AccountCode::new(AccountCode::CASH_AT_BROKER),
            amount: Money::inr(dec!(500_000.00)),
        }],
        vec![PostingEntry {
            account: AccountCode::new(AccountCode::OPENING_CAPITAL),
            amount: Money::inr(dec!(500_000.00)),
        }],
    )
    .unwrap();

    let posted_id = store.post_entry(&tx).unwrap();
    assert_eq!(posted_id, tx_id);

    let cash_bal = store
        .get_balance(&AccountCode::new(AccountCode::CASH_AT_BROKER))
        .unwrap();
    assert_eq!(cash_bal, Money::inr(dec!(500_000.00)));
}
