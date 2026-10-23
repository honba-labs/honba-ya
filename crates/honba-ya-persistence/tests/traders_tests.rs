use chrono::Utc;
use honba_persistence::{migrate, Database, PersistedTrader, TraderRepository, TraderStore};
use rust_decimal::Decimal;
use std::str::FromStr;

async fn setup_db() -> Database {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory db");
    migrate(&db).await.expect("migrate");
    db
}

#[tokio::test]
async fn test_trader_store_upsert_and_lookups() {
    let db = setup_db().await;
    let store = TraderStore::new(&db);

    let client_id = "client-alpha-001";
    let algo_id = "algo-momentum-01";
    let capital = Decimal::from_str("1000000.50").unwrap();
    let now = Utc::now();

    // 1. Initial upsert
    let trader = store
        .upsert(client_id, algo_id, "prop", capital, now)
        .await
        .expect("upsert trader");

    assert_eq!(trader.client_id, client_id);
    assert_eq!(trader.algo_id, algo_id);
    assert_eq!(trader.risk_profile, "prop");
    assert_eq!(trader.capital, capital);

    // 2. Lookup by client_id
    let by_client = store
        .get_by_client_id(client_id)
        .await
        .expect("query client")
        .expect("found trader");
    assert_eq!(by_client.id, trader.id);
    assert_eq!(by_client.algo_id, algo_id);

    // 3. Lookup by algo_id
    let by_algo = store
        .get_by_algo_id(algo_id)
        .await
        .expect("query algo")
        .expect("found trader");
    assert_eq!(by_algo.id, trader.id);
    assert_eq!(by_algo.client_id, client_id);

    // 4. Idempotent update: increase capital and change risk profile
    let new_capital = Decimal::from_str("2500000.75").unwrap();
    let updated = store
        .upsert(client_id, algo_id, "institutional", new_capital, now)
        .await
        .expect("update trader");

    assert_eq!(updated.id, trader.id);
    assert_eq!(updated.risk_profile, "institutional");
    assert_eq!(updated.capital, new_capital);

    // 5. List traders
    let list = store.list().await.expect("list traders");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].client_id, client_id);
}

#[tokio::test]
async fn test_trader_repository_alias() {
    let db = setup_db().await;
    let repo = TraderRepository::new(&db);

    let client_id = "client-retail-002";
    let algo_id = "algo-mean-rev-02";
    let capital = Decimal::from_str("50000.00").unwrap();
    let now = Utc::now();

    let trader: PersistedTrader = repo
        .upsert(client_id, algo_id, "retail", capital, now)
        .await
        .expect("repo upsert");

    assert_eq!(trader.client_id, client_id);
}
