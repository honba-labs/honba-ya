//! Fill store integration tests: exact-decimal order aggregation, automatic
//! promotion to `filled`, idempotent inserts on `trade_id`, and the
//! `fills_for_order` hook — SQLite in-memory via `Database::connect` + `migrate`.

use chrono::{DateTime, TimeZone, Utc};
use honba_core::{
    Exchange, FillReport, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price,
    Quantity, TimeInForce,
};
use honba_persistence::{Database, FillStore, OrderStore, migrate};
use rust_decimal_macros::dec;

async fn setup() -> Database {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite::memory:");
    let applied = migrate(&db).await.expect("migrate");
    assert_eq!(applied, vec![1, 2, 3, 4, 5]);
    db
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().unwrap()
}

fn sample_request(algo_id: &str) -> OrderRequest {
    OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2850.50").unwrap()),
        Quantity::from_u64(100),
        algo_id,
    )
    .with_client_id("client-retail-01")
}

fn fill_report(
    fill_id: &str,
    client_order_id: uuid::Uuid,
    qty: u64,
    price: &str,
    algo_id: &str,
) -> FillReport {
    FillReport::new(
        fill_id,
        "BRK-ORDER-1",
        client_order_id,
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        Price::from_rupees(price).unwrap(),
        Quantity::from_u64(qty),
        algo_id,
        ts(1002),
    )
}

#[tokio::test]
async fn partial_then_full_fill_aggregation_and_auto_promotion() {
    let db = setup().await;
    let orders = OrderStore::new(&db);
    let fills = FillStore::new(&db);

    let req = sample_request("algo-fill-01");
    let cid = req.client_order_id.to_string();
    orders.upsert(&req, ts(1000)).await.expect("upsert");
    orders
        .update_status(&cid, OrderStatus::PendingNew, OrderStatus::New, ts(1001))
        .await
        .expect("ack");

    // Partial fill: 40 @ 100.50.
    let f1 = fills
        .insert(
            &fill_report("T-1", req.client_order_id, 40, "100.50", "algo-fill-01"),
            r#"{"stt":"0.60","exchange":"0.05"}"#,
            ts(1002),
        )
        .await
        .expect("insert T-1");
    assert_eq!(f1.trade_id, "T-1");
    assert_eq!(f1.quantity(), dec!(40));
    assert_eq!(f1.price(), dec!(100.5));
    assert_eq!(f1.side(), Some(OrderSide::Buy));
    assert_eq!(f1.fee_breakdown_json, r#"{"stt":"0.60","exchange":"0.05"}"#);

    let order = orders.get_by_client_order_id(&cid).await.unwrap().unwrap();
    assert_eq!(order.filled_quantity(), dec!(40));
    assert_eq!(order.avg_fill_price(), Some(dec!(100.5)));
    // Not complete yet: status untouched by aggregation.
    assert_eq!(order.status(), Some(OrderStatus::New));

    // Idempotent re-insert: same trade_id returns the existing row and does
    // not double-count aggregates (even with a different caller timestamp).
    let f1_again = fills
        .insert(
            &fill_report("T-1", req.client_order_id, 40, "100.50", "algo-fill-01"),
            "{}",
            ts(9999),
        )
        .await
        .expect("idempotent insert T-1");
    assert_eq!(f1, f1_again);
    let order = orders.get_by_client_order_id(&cid).await.unwrap().unwrap();
    assert_eq!(order.filled_quantity(), dec!(40));
    assert_eq!(order.avg_fill_price(), Some(dec!(100.5)));

    // Drive to PartiallyFilled, then complete the order.
    orders
        .update_status(
            &cid,
            OrderStatus::New,
            OrderStatus::PartiallyFilled,
            ts(1003),
        )
        .await
        .expect("partial ack");

    // Full fill: 60 @ 333.33 → total 100, exact weighted average
    // (40 * 100.50 + 60 * 333.33) / 100 = (4020 + 19999.80) / 100 = 240.198.
    let f2 = fills
        .insert(
            &fill_report("T-2", req.client_order_id, 60, "333.33", "algo-fill-01"),
            r#"{"stt":"4.00"}"#,
            ts(1004),
        )
        .await
        .expect("insert T-2");
    assert_eq!(f2.trade_id, "T-2");
    assert_eq!(f2.quantity(), dec!(60));

    let order = orders.get_by_client_order_id(&cid).await.unwrap().unwrap();
    assert_eq!(order.filled_quantity(), dec!(100));
    assert_eq!(order.avg_fill_price(), Some(dec!(240.198)));
    // Auto-promoted: filled_quantity reached the requested quantity.
    assert_eq!(order.status(), Some(OrderStatus::Filled));
}

#[tokio::test]
async fn fills_for_order_returns_inserted_fills_in_order() {
    let db = setup().await;
    let orders = OrderStore::new(&db);
    let fills = FillStore::new(&db);

    let req = sample_request("algo-fill-02");
    let cid = req.client_order_id.to_string();
    orders.upsert(&req, ts(1000)).await.expect("upsert");
    orders
        .update_status(&cid, OrderStatus::PendingNew, OrderStatus::New, ts(1001))
        .await
        .expect("ack");

    let f1 = fills
        .insert(
            &fill_report("T-10", req.client_order_id, 25, "100.00", "algo-fill-02"),
            "{}",
            ts(1002),
        )
        .await
        .expect("T-10");
    let f2 = fills
        .insert(
            &fill_report("T-11", req.client_order_id, 75, "101.25", "algo-fill-02"),
            "{}",
            ts(1003),
        )
        .await
        .expect("T-11");

    let persisted = orders.fills_for_order(&cid).await.expect("fills_for_order");
    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].trade_id, "T-10");
    assert_eq!(persisted[1].trade_id, "T-11");
    assert_eq!(persisted[0].id, f1.id);
    assert_eq!(persisted[1].id, f2.id);
    assert_eq!(persisted[0].executed_at, ts(1002));
    assert_eq!(persisted[1].executed_at, ts(1003));

    // Lookups by trade id.
    assert!(fills.get_by_trade_id("T-10").await.unwrap().is_some());
    assert!(fills.get_by_trade_id("T-11").await.unwrap().is_some());
    assert!(fills.get_by_trade_id("MISSING").await.unwrap().is_none());

    // An unrelated order reports no fills.
    let other = sample_request("algo-fill-03");
    orders.upsert(&other, ts(1010)).await.unwrap();
    let none = orders
        .fills_for_order(&other.client_order_id.to_string())
        .await
        .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn completed_single_fill_promotes_market_order_to_filled() {
    let db = setup().await;
    let orders = OrderStore::new(&db);
    let fills = FillStore::new(&db);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Sell,
        OrderType::Market,
        Quantity::from_u64(10),
        "algo-fill-04",
    );
    let cid = req.client_order_id.to_string();
    orders.upsert(&req, ts(1000)).await.expect("upsert");
    orders
        .update_status(&cid, OrderStatus::PendingNew, OrderStatus::New, ts(1001))
        .await
        .expect("ack");
    assert_eq!(req.tif, TimeInForce::Day);

    fills
        .insert(
            &FillReport::new(
                "T-100",
                "BRK-2",
                req.client_order_id,
                InstrumentId::new(Exchange::Nse, "TCS"),
                OrderSide::Sell,
                Price::from_rupees("3900.00").unwrap(),
                Quantity::from_u64(10),
                "algo-fill-04",
                ts(1002),
            ),
            "{}",
            ts(1002),
        )
        .await
        .expect("single fill");

    let order = orders.get_by_client_order_id(&cid).await.unwrap().unwrap();
    assert_eq!(order.filled_quantity(), dec!(10));
    assert_eq!(order.avg_fill_price(), Some(dec!(3900)));
    assert_eq!(order.status(), Some(OrderStatus::Filled));
}
