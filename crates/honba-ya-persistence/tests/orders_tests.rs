//! Order store integration tests: idempotent upsert, the full legal
//! transition matrix, illegal-transition rejection, and concurrent upsert
//! races — all against SQLite in-memory via `Database::connect` + `migrate`.

use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use honba_core::{
    Exchange, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, Quantity,
};
use honba_persistence::orders::is_legal_transition;
use honba_persistence::{Database, OrderStore, OrderStoreError, PersistedOrder, migrate};
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

async fn count_order(db: &Database, client_order_id: &str) -> i64 {
    match db {
        Database::Sqlite(pool) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM orders WHERE client_order_id = $1")
                .bind(client_order_id)
                .fetch_one(pool)
                .await
                .unwrap()
        }
        Database::Postgres(pool) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM orders WHERE client_order_id = $1")
                .bind(client_order_id)
                .fetch_one(pool)
                .await
                .unwrap()
        }
    }
}

#[tokio::test]
async fn upsert_is_idempotent_on_client_order_id() {
    let db = setup().await;
    let store = OrderStore::new(&db);
    let req = sample_request("algo-order-01");

    let first = store.upsert(&req, ts(1000)).await.expect("first upsert");
    assert_eq!(first.status, "pending_new");
    assert_eq!(first.status(), Some(OrderStatus::PendingNew));
    assert_eq!(first.quantity(), dec!(100));
    assert_eq!(first.filled_quantity(), dec!(0));
    assert_eq!(first.avg_fill_price(), None);
    assert_eq!(first.side(), Some(OrderSide::Buy));
    assert_eq!(first.tif(), Some(honba_core::TimeInForce::Day));
    assert_eq!(
        first.order_type(),
        Some(OrderType::Limit(Price(dec!(2850.50))))
    );
    assert_eq!(
        first.instrument(),
        Some(InstrumentId::new(Exchange::Nse, "RELIANCE"))
    );
    assert_eq!(first.client_order_id_uuid(), Some(req.client_order_id));
    assert_eq!(first.created_at, ts(1000));
    assert_eq!(first.algo_id, "algo-order-01");
    assert_eq!(first.client_id, "client-retail-01");

    // Second upsert with a different timestamp returns the original row
    // unchanged (idempotent, no duplicate, created_at preserved).
    let second = store.upsert(&req, ts(2000)).await.expect("second upsert");
    assert_eq!(first, second);
    assert_eq!(second.created_at, ts(1000));
    assert_eq!(count_order(&db, &req.client_order_id.to_string()).await, 1);
}

#[tokio::test]
async fn full_legal_transition_matrix_and_illegal_rejections() {
    use OrderStatus::*;
    let legal = [
        (PendingNew, New),
        (PendingNew, Cancelled),
        (PendingNew, Rejected),
        (PendingNew, Expired),
        (New, PartiallyFilled),
        (New, Filled),
        (New, Cancelled),
        (New, Rejected),
        (New, Expired),
        (PartiallyFilled, PartiallyFilled),
        (PartiallyFilled, Filled),
        (PartiallyFilled, Cancelled),
        (PartiallyFilled, Rejected),
        (PartiallyFilled, Expired),
    ];
    let illegal = [
        (PendingNew, PendingNew),
        (PendingNew, PartiallyFilled),
        (PendingNew, Filled),
        (New, PendingNew),
        (New, New),
        (PartiallyFilled, PendingNew),
        (PartiallyFilled, New),
        (Filled, PendingNew),
        (Filled, New),
        (Filled, Filled),
        (Filled, Cancelled),
        (Cancelled, New),
        (Cancelled, Cancelled),
        (Rejected, New),
        (Expired, New),
    ];
    for (from, to) in legal {
        assert!(
            is_legal_transition(from, to),
            "{from:?} -> {to:?} must be legal"
        );
    }
    for (from, to) in illegal {
        assert!(
            !is_legal_transition(from, to),
            "{from:?} -> {to:?} must be illegal"
        );
    }

    // Drive a real chain through the database.
    let db = setup().await;
    let store = OrderStore::new(&db);
    let req = sample_request("algo-order-02");
    let cid = req.client_order_id.to_string();
    store.upsert(&req, ts(1000)).await.expect("upsert");

    let after_ack = store
        .update_status(&cid, PendingNew, New, ts(1001))
        .await
        .expect("pending_new -> new");
    assert_eq!(after_ack.status(), Some(OrderStatus::New));
    assert_eq!(after_ack.updated_at, ts(1001));

    let after_partial = store
        .update_status(&cid, New, PartiallyFilled, ts(1002))
        .await
        .expect("new -> partially_filled");
    assert_eq!(after_partial.status(), Some(OrderStatus::PartiallyFilled));

    let after_fill = store
        .update_status(&cid, PartiallyFilled, Filled, ts(1003))
        .await
        .expect("partially_filled -> filled");
    assert_eq!(after_fill.status(), Some(OrderStatus::Filled));

    // Terminal state admits no outgoing transitions (illegal edge).
    match store.update_status(&cid, Filled, Cancelled, ts(1004)).await {
        Err(OrderStoreError::InvalidStateTransition { from, to, .. }) => {
            assert_eq!(from, "filled");
            assert_eq!(to, "cancelled");
        }
        other => panic!("expected InvalidStateTransition, got {other:?}"),
    }

    // Caller-declared `from` mismatching the stored status is rejected even
    // if the edge itself would be legal.
    match store.update_status(&cid, New, Cancelled, ts(1005)).await {
        Err(OrderStoreError::InvalidStateTransition { from, .. }) => {
            assert_eq!(from, "filled");
        }
        other => panic!("expected InvalidStateTransition on from-mismatch, got {other:?}"),
    }

    // Unknown order -> NotFound.
    match store
        .update_status(
            "00000000-0000-0000-0000-000000000000",
            PendingNew,
            New,
            ts(1006),
        )
        .await
    {
        Err(OrderStoreError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }

    // Status unchanged after all rejected attempts.
    let current: PersistedOrder = store.get_by_client_order_id(&cid).await.unwrap().unwrap();
    assert_eq!(current.status(), Some(OrderStatus::Filled));
}

#[tokio::test]
async fn broker_order_id_roundtrip() {
    let db = setup().await;
    let store = OrderStore::new(&db);
    let req = sample_request("algo-order-03");
    let cid = req.client_order_id.to_string();
    store.upsert(&req, ts(1000)).await.expect("upsert");
    assert!(
        store
            .get_by_broker_order_id("BRK-999")
            .await
            .unwrap()
            .is_none()
    );

    let acked = store
        .attach_broker_order_id(&cid, "BRK-999", ts(1001))
        .await
        .expect("attach broker id");
    assert_eq!(acked.broker_order_id.as_deref(), Some("BRK-999"));
    assert_eq!(acked.updated_at, ts(1001));

    let by_broker = store
        .get_by_broker_order_id("BRK-999")
        .await
        .unwrap()
        .expect("lookup by broker id");
    assert_eq!(by_broker.client_order_id, cid);

    match store
        .attach_broker_order_id("missing-uuid", "BRK-1", ts(1002))
        .await
    {
        Err(OrderStoreError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upserts_return_existing_row_without_duplicates() {
    let db = setup().await;
    let req = sample_request("algo-order-race");
    let cid = req.client_order_id.to_string();

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let db = db.clone();
            let req = req.clone();
            tokio::spawn(async move {
                let store = OrderStore::new(&db);
                store.upsert(&req, ts(1000 + i)).await
            })
        })
        .collect();

    let results: Vec<Result<PersistedOrder, OrderStoreError>> = join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    let mut seen_ids = Vec::new();
    for result in results {
        let order = result.expect("concurrent upsert must succeed");
        seen_ids.push(order.client_order_id);
    }
    // Every racer observed the same row.
    assert!(seen_ids.iter().all(|id| *id == cid));
    // Exactly one row persisted — unique violation absorbed, no duplicates.
    assert_eq!(count_order(&db, &cid).await, 1);
}
