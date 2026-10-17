//! ADR 0017 decision 2: the run id space.

use crate::{ErrorCode, RunId, RunIdGenerator};

const ALPHABET: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

#[test]
fn run_id_format() {
    let id = RunIdGenerator::default()
        .next(1_700_000_000_000, [7, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        .unwrap();
    let text = id.to_string();
    assert_eq!(text.len(), 26);
    assert!(text.chars().all(|c| ALPHABET.contains(c)), "{text}");
    assert_eq!(RunId::parse(&text).unwrap(), id);
    assert_eq!(id.as_str(), text);
    assert_eq!(text.parse::<RunId>().unwrap(), id);
}

#[test]
fn run_id_known_vectors() {
    let mut g = RunIdGenerator::default();
    assert_eq!(
        g.next(0, [0; 10]).unwrap().as_str(),
        "00000000000000000000000000"
    );
    let mut g = RunIdGenerator::default();
    assert_eq!(
        g.next(1, [0; 10]).unwrap().as_str(),
        "00000000010000000000000000"
    );
    let mut g = RunIdGenerator::default();
    assert_eq!(
        g.next(0, [0xFF; 10]).unwrap().as_str(),
        "0000000000ZZZZZZZZZZZZZZZZ"
    );
    let mut g = RunIdGenerator::default();
    let max_ms = (1u64 << 48) - 1;
    assert_eq!(
        g.next(max_ms, [0; 10]).unwrap().as_str(),
        "7ZZZZZZZZZ0000000000000000"
    );
}

#[test]
fn run_id_rejects() {
    let bad = [
        "",
        "../x",
        "..%2F..%2Fetc",
        "%2e%2e",
        "%2e%2e%2f",
        "..",
        "../../../../../../etc/passwd",
        "00000000000000000000000000/",
        "0000000000000000000000000a",  // lowercase
        "0000000000000000000000000",   // 25
        "000000000000000000000000000", // 27
        "0000000000000000000000000I",
        "0000000000000000000000000L",
        "0000000000000000000000000O",
        "0000000000000000000000000U",
        "0000000000000000000000000\n",
        "0000000000000000000000000é",
        " 0000000000000000000000000",
    ];
    for text in bad {
        assert!(RunId::parse(text).is_err(), "{text:?} was accepted");
    }
    assert!(RunId::parse("00000000000000000000000000").is_ok());
}

#[test]
fn run_id_deserialization_validates() {
    assert!(serde_json::from_str::<RunId>("\"../x\"").is_err());
    let ok: RunId = serde_json::from_str("\"01ARZ3NDEKTSV4RRFFQ69G5FAV\"").unwrap();
    assert_eq!(
        serde_json::to_string(&ok).unwrap(),
        "\"01ARZ3NDEKTSV4RRFFQ69G5FAV\""
    );
}

#[test]
fn run_id_monotonic_within_ms() {
    let mut g = RunIdGenerator::default();
    let a = g.next(1000, [1; 10]).unwrap();
    // Fresh entropy is ignored inside the same millisecond: the 80 bits increment.
    let b = g.next(1000, [9; 10]).unwrap();
    let c = g.next(1000, [0; 10]).unwrap();
    assert!(a < b && b < c);
    let next_ms = g.next(1001, [0; 10]).unwrap();
    assert!(c < next_ms);
}

#[test]
fn run_id_increment_carries() {
    let mut g = RunIdGenerator::default();
    let a = g.next(5, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF]).unwrap();
    let b = g.next(5, [0; 10]).unwrap();
    assert_eq!(a.as_str(), "0000000005000000000000007Z");
    assert_eq!(b.as_str(), "00000000050000000000000080");
}

#[test]
fn run_id_monotonic_on_clock_regression() {
    let mut g = RunIdGenerator::default();
    let a = g.next(5000, [3; 10]).unwrap();
    let b = g.next(4000, [9; 10]).unwrap();
    let c = g.next(4999, [9; 10]).unwrap();
    assert!(a < b && b < c, "{a} {b} {c}");
    // The timestamp part stays at the last issued millisecond.
    assert_eq!(&a.as_str()[..10], &b.as_str()[..10]);
}

#[test]
fn run_id_overflow_is_error() {
    let mut g = RunIdGenerator::default();
    g.next(5, [0xFF; 10]).unwrap();
    let err = g.next(5, [0; 10]).unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    let err = g.next(4, [0; 10]).unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
    // A later millisecond recovers: the failed mints did not advance the state.
    assert!(g.next(6, [0; 10]).is_ok());
}

#[test]
fn run_id_timestamp_beyond_48_bits_is_error() {
    let mut g = RunIdGenerator::default();
    let err = g.next(1u64 << 48, [0; 10]).unwrap_err();
    assert_eq!(err.code, ErrorCode::InternalError);
}

#[test]
fn run_ids_differ_for_same_seed() {
    // Ids are opaque and not derived from content: identical inputs still mint distinct ids.
    let mut g = RunIdGenerator::default();
    let a = g.next(42, [5; 10]).unwrap();
    let b = g.next(42, [5; 10]).unwrap();
    assert_ne!(a, b);
}
