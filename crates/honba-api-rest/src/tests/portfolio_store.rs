//! `PortfolioStore`: isolation, revisions, id validation and atomic writes.
use crate::{
    PortfolioAccountRecord, PortfolioDocument, PortfolioError, PortfolioHeldPosition,
    PortfolioStore,
};
use honba_entities::{Currency, Money, PortfolioMode, PortfolioSource, Position};
use honba_messages::{Exchange, InstrumentId, Product};

fn store() -> (tempfile::TempDir, PortfolioStore) {
    let tmp = tempfile::tempdir().unwrap();
    let store = PortfolioStore::new(tmp.path().join("portfolios"));
    (tmp, store)
}

fn document_with_trades(n: usize) -> PortfolioDocument {
    PortfolioDocument {
        trades: vec![
            honba_entities::Trade::new(
                honba_messages::OrderId::new("O-1"),
                InstrumentId::new("INFY", Exchange::new("NSE")),
                honba_messages::OrderSide::Buy,
                10.0,
                100.0,
                Currency::Inr,
                honba_messages::UnixNanos::from_u64(1),
                honba_messages::UnixNanos::from_u64(2),
            );
            n
        ],
        ..PortfolioDocument::default()
    }
}

fn account_record(id: &str, currency: Currency) -> PortfolioAccountRecord {
    PortfolioAccountRecord {
        id: id.to_string(),
        name: id.to_string(),
        mode: PortfolioMode::Sandbox,
        source: PortfolioSource::Ledger,
        broker: None,
        currency,
        cash: Money::zero(currency),
        leverage: 1.0,
        margin_used: Money::zero(currency),
        margin_available: Money::zero(currency),
        positions: Vec::new(),
        greeks: None,
    }
}

fn document_with_account() -> PortfolioDocument {
    PortfolioDocument {
        accounts: vec![account_record("main", Currency::Inr)],
        ..PortfolioDocument::default()
    }
}

#[test]
fn a_missing_user_gets_default_document() {
    let (_tmp, store) = store();
    let doc = store.get("default").unwrap();
    assert_eq!(doc, PortfolioDocument::default());
    assert_eq!(doc.revision, 0);
}

#[test]
fn put_bumps_revision_and_stores() {
    let (_tmp, store) = store();
    let first = store.put("alice", 0, document_with_account()).unwrap();
    assert_eq!(first.revision, 1);
    let got = store.get("alice").unwrap();
    assert_eq!(got.revision, 1);
    assert_eq!(got.accounts.len(), 1);
}

#[test]
fn revision_conflict_rejected() {
    let (_tmp, store) = store();
    store.put("alice", 0, document_with_account()).unwrap();
    let err = store
        .put("alice", 0, PortfolioDocument::default())
        .unwrap_err();
    assert!(matches!(
        err,
        PortfolioError::RevisionConflict {
            expected: 0,
            current: 1
        }
    ));
}

#[test]
fn users_isolated() {
    let (_tmp, store) = store();
    store.put("alice", 0, document_with_account()).unwrap();
    store.put("bob", 0, PortfolioDocument::default()).unwrap();
    assert_eq!(store.get("alice").unwrap().accounts.len(), 1);
    assert_eq!(store.get("bob").unwrap().accounts.len(), 0);
}

#[test]
fn update_bumps_once_only_if_changed() {
    let (_tmp, store) = store();
    let after = store
        .update("alice", |d| {
            d.accounts.push(account_record("x", Currency::Inr));
            true
        })
        .unwrap();
    assert_eq!(after.revision, 1);
    let nochange = store.update("alice", |_| false).unwrap();
    assert_eq!(nochange.revision, 1);
}

#[test]
fn invalid_user_id_rejected_on_path_operations() {
    let (_tmp, store) = store();
    assert!(matches!(store.get(""), Err(PortfolioError::InvalidUserId)));
    assert!(matches!(
        store.put("", 0, PortfolioDocument::default()),
        Err(PortfolioError::InvalidUserId)
    ));
    let long = "a".repeat(65);
    assert!(matches!(
        store.get(&long),
        Err(PortfolioError::InvalidUserId)
    ));
    assert!(matches!(
        store.put("bad/id", 0, PortfolioDocument::default()),
        Err(PortfolioError::InvalidUserId)
    ));
}

#[test]
fn too_many_accounts_rejected() {
    let (_tmp, store) = store();
    let mut doc = PortfolioDocument::default();
    for i in 0..(crate::portfolio_store::MAX_ACCOUNTS + 1) {
        doc.accounts
            .push(account_record(&format!("a{i}"), Currency::Inr));
    }
    let err = store.put("alice", 0, doc).unwrap_err();
    assert!(matches!(err, PortfolioError::TooLarge { .. }));
}

#[test]
fn too_many_positions_in_one_account_rejected() {
    let (_tmp, store) = store();
    let mut doc = PortfolioDocument::default();
    let mut account = account_record("main", Currency::Inr);
    for i in 0..(crate::portfolio_store::MAX_POSITIONS_PER_ACCOUNT + 1) {
        account.positions.push(PortfolioHeldPosition {
            position: Position::flat(
                InstrumentId::new(format!("I{i}"), Exchange::new("X")),
                Currency::Inr,
            ),
            product: Product::Delivery,
            group_name: None,
            tags: Vec::new(),
            strategy_id: None,
        });
    }
    doc.accounts.push(account);
    let err = store.put("alice", 0, doc).unwrap_err();
    assert!(matches!(err, PortfolioError::TooLarge { .. }));
}

#[test]
fn too_many_trades_rejected() {
    let (_tmp, store) = store();
    let doc = document_with_trades(crate::portfolio_store::MAX_TRADES + 1);
    let err = store.put("alice", 0, doc).unwrap_err();
    assert!(matches!(err, PortfolioError::TooLarge { .. }));
}

#[test]
fn a_corrupt_file_is_quarantined_and_read_returns_default() {
    let (tmp, store) = store();
    let dir = tmp.path().join("portfolios");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("alice.json"), b"not json").unwrap();
    let got = store.get("alice").unwrap();
    assert_eq!(got, PortfolioDocument::default());
    let mut entries: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    entries.sort_by_key(|e| e.as_ref().unwrap().file_name());
    assert_eq!(entries.len(), 1);
    let name = entries[0].as_ref().unwrap().file_name();
    let name = name.to_string_lossy();
    assert!(name.starts_with("alice.json.corrupt-"));
}

#[test]
fn putting_an_unchanged_portfolio_keeps_revision_and_writes_nothing() {
    let (tmp, store) = store();
    let first = store.put("alice", 0, document_with_account()).unwrap();
    assert_eq!(first.revision, 1);
    let path = tmp.path().join("portfolios/alice.json");
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    let same = store.put("alice", 1, document_with_account()).unwrap();
    assert_eq!(same, first);
    assert_eq!(store.get("alice").unwrap().revision, 1);
    assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    assert_eq!(
        store
            .put("alice", 1, PortfolioDocument::default())
            .unwrap()
            .revision,
        2
    );
}
