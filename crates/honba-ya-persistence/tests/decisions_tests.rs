//! Decision store integration tests: SHA-256 prompt checksum helper,
//! idempotent insert, 64-hex validation, and newest-first algo listing —
//! SQLite in-memory via `Database::connect` + `migrate`.

use chrono::{DateTime, TimeZone, Utc};
use honba_persistence::{Database, DecisionStore, DecisionStoreError, migrate, sha256_hex};
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

#[test]
fn sha256_hex_matches_known_vector() {
    assert_eq!(
        sha256_hex("abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    // Stable and 64 lowercase hex characters.
    let digest = sha256_hex("prompt payload");
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(digest, sha256_hex("prompt payload"));
    assert_ne!(digest, sha256_hex("prompt payload "));
}

#[tokio::test]
async fn insert_get_roundtrip_and_idempotent_insert() {
    let db = setup().await;
    let store = DecisionStore::new(&db);

    let prompt_sha = sha256_hex("place BUY RELIANCE limit 2850.50");
    let spec = r#"{"action":"buy","instrument":"NSE:RELIANCE","qty":"10"}"#;

    let first = store
        .insert(
            "DEC-1",
            "algo-dec-01",
            &prompt_sha,
            "test-model-x",
            42,
            dec!(0.7),
            "order_intent",
            spec,
            ts(100),
        )
        .await
        .expect("insert DEC-1");
    assert_eq!(first.decision_id, "DEC-1");
    assert_eq!(first.algo_id, "algo-dec-01");
    assert_eq!(first.prompt_sha256, prompt_sha);
    assert_eq!(first.model_name, "test-model-x");
    assert_eq!(first.seed, "42");
    assert_eq!(first.seed_u64(), Some(42));
    assert_eq!(first.temperature, "0.7");
    assert_eq!(first.temperature_decimal(), dec!(0.7));
    assert_eq!(first.decision_kind, "order_intent");
    assert_eq!(first.spec_json, spec);
    assert_eq!(first.created_at, ts(100));

    let fetched = store
        .get_by_id("DEC-1")
        .await
        .expect("get_by_id")
        .expect("row exists");
    assert_eq!(first, fetched);
    assert!(store.get_by_id("DEC-MISSING").await.unwrap().is_none());

    // Idempotent re-insert with a different payload returns the original row.
    let again = store
        .insert(
            "DEC-1",
            "algo-dec-01",
            &prompt_sha,
            "other-model",
            999,
            dec!(1.5),
            "risk_override",
            "{}",
            ts(999),
        )
        .await
        .expect("idempotent insert DEC-1");
    assert_eq!(first, again);
    assert_eq!(again.created_at, ts(100));
}

#[tokio::test]
async fn invalid_prompt_sha256_is_rejected() {
    let db = setup().await;
    let store = DecisionStore::new(&db);

    // Too short (63 chars).
    let short = "a".repeat(63);
    let err = store
        .insert(
            "DEC-BAD-1",
            "algo-dec-01",
            &short,
            "m",
            1,
            dec!(0.5),
            "order_intent",
            "{}",
            ts(100),
        )
        .await;
    assert!(matches!(
        err,
        Err(DecisionStoreError::InvalidPromptSha256(_))
    ));

    // 64 chars but non-hex characters.
    let non_hex = "g".repeat(64);
    let err = store
        .insert(
            "DEC-BAD-2",
            "algo-dec-01",
            &non_hex,
            "m",
            1,
            dec!(0.5),
            "order_intent",
            "{}",
            ts(100),
        )
        .await;
    assert!(matches!(
        err,
        Err(DecisionStoreError::InvalidPromptSha256(_))
    ));

    // Nothing was persisted by the rejected inserts.
    assert!(store.get_by_id("DEC-BAD-1").await.unwrap().is_none());
    assert!(store.get_by_id("DEC-BAD-2").await.unwrap().is_none());
}

#[tokio::test]
async fn list_by_algo_is_ordered_by_created_at_desc() {
    let db = setup().await;
    let store = DecisionStore::new(&db);

    let sha = sha256_hex("shared prompt");
    for (id, at) in [("DEC-A1", 100), ("DEC-A2", 200), ("DEC-A3", 300)] {
        store
            .insert(
                id,
                "algo-dec-list",
                &sha,
                "m",
                1,
                dec!(0.5),
                "order_intent",
                "{}",
                ts(at),
            )
            .await
            .expect("insert");
    }
    // Another algo's decisions must not leak into the listing.
    store
        .insert(
            "DEC-B1",
            "algo-dec-other",
            &sha256_hex("other prompt"),
            "m",
            2,
            dec!(0.5),
            "order_intent",
            "{}",
            ts(400),
        )
        .await
        .expect("insert other algo");

    let all = store.list_by_algo("algo-dec-list", 10).await.expect("list");
    let ids: Vec<&str> = all.iter().map(|d| d.decision_id.as_str()).collect();
    assert_eq!(ids, vec!["DEC-A3", "DEC-A2", "DEC-A1"]);
    assert_eq!(all[0].created_at, ts(300));
    assert_eq!(all[2].created_at, ts(100));

    let limited = store
        .list_by_algo("algo-dec-list", 1)
        .await
        .expect("limited list");
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].decision_id, "DEC-A3");

    let empty = store
        .list_by_algo("algo-dec-never-used", 5)
        .await
        .expect("empty list");
    assert!(empty.is_empty());
}
