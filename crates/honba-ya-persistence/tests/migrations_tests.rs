//! Migration runner integration tests: idempotent double-migrate, SQLite
//! audit-table ownership (audit.rs owns `sebi_audit_ledger`; no
//! `audit_ledger` in sqlite migrations), and full down-migration rollback —
//! SQLite in-memory via `Database::connect`.

use honba_persistence::{migrate, rollback_last, Database};

async fn setup() -> Database {
    Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite::memory:")
}

async fn table_exists(db: &Database, name: &str) -> bool {
    match db {
        Database::Sqlite(pool) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
            )
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
                > 0
        }
        Database::Postgres(pool) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
            )
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
                > 0
        }
    }
}

async fn applied_count(db: &Database) -> i64 {
    match db {
        Database::Sqlite(pool) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM schema_migrations")
                .fetch_one(pool)
                .await
                .unwrap()
        }
        Database::Postgres(pool) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM schema_migrations")
                .fetch_one(pool)
                .await
                .unwrap()
        }
    }
}

#[tokio::test]
async fn migrate_is_idempotent_and_creates_all_tables() {
    let db = setup().await;

    let first = migrate(&db).await.expect("first migrate");
    assert_eq!(first, vec![1, 2, 3, 4, 5]);
    assert_eq!(applied_count(&db).await, 5);

    // Second run: nothing pending, empty result, no re-apply.
    let second = migrate(&db).await.expect("second migrate");
    assert!(second.is_empty(), "expected no pending migrations");
    assert_eq!(applied_count(&db).await, 5);

    for name in [
        "schema_migrations",
        "traders",
        "orders",
        "fills",
        "decisions",
        "positions",
        "position_lots",
    ] {
        assert!(table_exists(&db, name).await, "table {name} must exist");
    }

    // SQLite audit tables are owned by audit.rs at runtime — the sqlite
    // migration set must not create (or conflict with) them.
    assert!(
        !table_exists(&db, "audit_ledger").await,
        "sqlite migrations must not create audit_ledger"
    );
    assert!(
        !table_exists(&db, "sebi_audit_ledger").await,
        "sebi_audit_ledger is owned by SqliteAuditLedger, not migrations"
    );
}

#[tokio::test]
async fn rollback_last_then_re_migrate_then_full_rollback_to_empty() {
    let db = setup().await;
    assert_eq!(migrate(&db).await.unwrap(), vec![1, 2, 3, 4, 5]);

    // Roll back the highest version (005_positions: drops both position tables).
    let rolled = rollback_last(&db).await.expect("rollback_last");
    assert_eq!(rolled, Some(5));
    assert_eq!(applied_count(&db).await, 4);
    assert!(!table_exists(&db, "positions").await);
    assert!(!table_exists(&db, "position_lots").await);
    // Earlier versions remain intact.
    assert!(table_exists(&db, "orders").await);
    assert!(table_exists(&db, "decisions").await);

    // Re-migrate: only the pending version is re-applied (idempotent).
    let reapplied = migrate(&db).await.expect("re-migrate");
    assert_eq!(reapplied, vec![5]);
    assert_eq!(applied_count(&db).await, 5);
    assert!(table_exists(&db, "positions").await);
    assert!(table_exists(&db, "position_lots").await);

    // Walk all down migrations to the bottom: 5 → 4 → 3 → 2 → 1 → None.
    let mut rolled_back = Vec::new();
    while let Some(version) = rollback_last(&db).await.expect("walk down") {
        rolled_back.push(version);
    }
    assert_eq!(rolled_back, vec![5, 4, 3, 2, 1]);
    assert_eq!(applied_count(&db).await, 0);
    for name in ["traders", "orders", "fills", "decisions", "positions", "position_lots"] {
        assert!(!table_exists(&db, name).await, "table {name} must be dropped");
    }
    // The tracking table itself survives (not part of the migration set).
    assert!(table_exists(&db, "schema_migrations").await);
    // Nothing left to roll back.
    assert_eq!(rollback_last(&db).await.unwrap(), None);

    // Full forward pass again from empty.
    let full = migrate(&db).await.expect("migrate from empty");
    assert_eq!(full, vec![1, 2, 3, 4, 5]);
    assert!(table_exists(&db, "orders").await);
    assert!(table_exists(&db, "positions").await);
    assert!(!table_exists(&db, "sebi_audit_ledger").await);
}
