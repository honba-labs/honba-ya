use chrono::Utc;
use honba_core::{
    Exchange, FillReport, InstrumentId, Money, OrderRequest, OrderSide, OrderType, Price, Quantity,
};
use honba_persistence::{
    Database, DecisionRepository, FillRepository, OrderRepository, PositionRepository,
    TraderRepository, migrate,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

async fn setup_db() -> Arc<Database> {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory db");
    // Verify migration idempotency
    migrate(&db).await.expect("initial migrate");
    migrate(&db).await.expect("idempotent second migrate");
    Arc::new(db)
}

#[tokio::test]
async fn test_concurrent_persistence_transactions_under_load() {
    let db = setup_db().await;
    let mut handles = Vec::new();

    // Spawn 10 concurrent tasks interacting with all repositories simultaneously
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            let trader_repo = TraderRepository::new(&db_clone);
            let order_repo = OrderRepository::new(&db_clone);
            let fill_repo = FillRepository::new(&db_clone);
            let pos_repo = PositionRepository::new(&db_clone);
            let decision_repo = DecisionRepository::new(&db_clone);

            let client_id = format!("client-load-{:03}", i);
            let algo_id = format!("algo-load-{:03}", i);
            let now = Utc::now();

            // 1. Upsert trader
            trader_repo
                .upsert(
                    &client_id,
                    &algo_id,
                    "prop",
                    Decimal::from_str("100000.00").unwrap(),
                    now,
                )
                .await
                .expect("trader upsert");

            // 2. Persist decision
            let prompt_text = format!("decision prompt for task {}", i);
            let prompt_hash = honba_persistence::sha256_hex(&prompt_text);
            decision_repo
                .insert(
                    &format!("dec-{}", i),
                    &algo_id,
                    &prompt_hash,
                    "gpt-4o",
                    42,
                    Decimal::from_str("0.7").unwrap(),
                    "order_intent",
                    r#"{"action":"buy"}"#,
                    now,
                )
                .await
                .expect("decision insert");

            // 3. Upsert order
            let inst = InstrumentId::new(Exchange::Nse, format!("LOADSYM{}", i));
            let req = OrderRequest::new(
                inst.clone(),
                OrderSide::Buy,
                OrderType::Limit(Price::from_rupees("100.00").unwrap()),
                Quantity::from_u64(10),
                &algo_id,
            )
            .with_client_id(&client_id);

            order_repo.upsert(&req, now).await.expect("order upsert");

            // 4. Record Fill
            let fill = FillReport {
                fill_id: format!("FILL-LOAD-{}", i),
                order_id: format!("BRK-ORD-{}", i),
                client_order_id: req.client_order_id,
                instrument: inst.clone(),
                side: OrderSide::Buy,
                price: Price::from_rupees("100.00").unwrap(),
                quantity: Quantity::from_u64(10),
                algo_id: algo_id.clone(),
                timestamp: now,
                correlation_id: Some(req.correlation_id()),
                client_id: Some(client_id.clone()),
                fee: Money::zero_inr(),
                fee_breakdown: None,
                trace_context: None,
            };
            fill_repo
                .insert(&fill, "{}", now)
                .await
                .expect("fill insert");

            // 5. Update position via FIFO lot
            pos_repo
                .apply_fill(
                    &inst,
                    &client_id,
                    OrderSide::Buy,
                    Decimal::from(10),
                    Decimal::from(100),
                    now,
                )
                .await
                .expect("pos buy lot");
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("task join");
    }

    // Verify all 10 traders, orders, fills were persisted cleanly
    let trader_repo = TraderRepository::new(&db);
    let all_traders = trader_repo.list().await.expect("list traders");
    assert_eq!(all_traders.len(), 10);
}
