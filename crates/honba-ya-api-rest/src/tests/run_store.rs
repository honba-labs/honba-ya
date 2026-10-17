//! `RunStore` pieces that touch no filesystem: ports and refusals before any I/O.

use std::path::PathBuf;

use honba_api::RunKind;
use honba_messages::ErrorCode;

use crate::{OsEntropy, RunClock, RunEntropy, RunStore, SystemClock};

fn missing_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("does-not-exist-run-store-unit")
}

#[test]
fn the_system_clock_reads_a_plausible_wall_time() {
    // 2024-01-01T00:00:00Z in Unix ms.
    assert!(SystemClock.now_unix_ms() > 1_704_067_200_000);
}

#[test]
fn os_entropy_draws_differ() {
    let a = OsEntropy.next_bits().unwrap();
    let b = OsEntropy.next_bits().unwrap();
    assert_ne!(a, b);
}

#[test]
fn ill_formed_ids_are_not_found_without_touching_the_filesystem() {
    let root = missing_root();
    let store = RunStore::system(root.clone());
    for bad in [
        "",
        "../etc",
        "..%2F..%2Fetc",
        "%2e%2e%2f",
        "01hz3x4y5z6a7b8c9d0e1f2g3h",
        "01HZ3X4Y5Z6A7B8C9D0E1F2G3",
        "01HZ3X4Y5Z6A7B8C9D0E1F2G3HJ",
        "01HZ3X4Y5Z6A7B8C9D0E1F2GIL",
        "0000000000000000000000/../",
    ] {
        for kind in [RunKind::Backtest, RunKind::Sweep] {
            let err = store.load(bad, kind).unwrap_err();
            assert_eq!(err.code, ErrorCode::NotFound, "{bad:?}");
            assert!(
                !err.message.contains(bad) || bad.is_empty(),
                "{bad:?} echoed"
            );
        }
        assert_eq!(store.load_any(bad).unwrap_err().code, ErrorCode::NotFound);
        assert_eq!(
            store.read_journal(bad, RunKind::Backtest).unwrap_err().code,
            ErrorCode::NotFound
        );
    }
    assert!(
        !root.exists(),
        "a refused id must not create or touch the root"
    );
}

#[test]
fn a_well_formed_unknown_id_is_not_found() {
    let store = RunStore::system(missing_root());
    let err = store
        .load("0000000000000000000000ABCD", RunKind::Backtest)
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}
