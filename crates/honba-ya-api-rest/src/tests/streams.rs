use std::io::Write;
use std::path::Path;

use honba_messages::{Event, Exchange, InstrumentId, Message, QuoteTick, UnixNanos};

use crate::streams::read_journal_from;

fn msg(n: u64) -> Message {
    let quote = QuoteTick::new(
        InstrumentId::new("TCS", Exchange::new("NSE")),
        100.0 + n as f64,
        101.0 + n as f64,
        10.0,
        20.0,
        UnixNanos::from_u64(n),
        UnixNanos::from_u64(n),
    );
    Message::new(Event::Quote(quote), UnixNanos::from_u64(n + 1))
}

fn write_events(path: &Path, msgs: &[Message]) {
    let mut file = std::fs::File::create(path).unwrap();
    for m in msgs {
        let line = serde_json::to_vec(m).unwrap();
        file.write_all(&line).unwrap();
        file.write_all(b"\n").unwrap();
    }
}

#[test]
fn read_from_zero_returns_all_complete_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let msgs: Vec<_> = (0..5).map(msg).collect();
    write_events(&path, &msgs);

    let chunk = read_journal_from(&path, 0).unwrap();
    assert_eq!(chunk.records.len(), 5);
    assert_eq!(chunk.last_seq, 5);
    assert!(chunk.at_eof);
}

#[test]
fn read_from_midpoint_returns_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let msgs: Vec<_> = (0..5).map(msg).collect();
    write_events(&path, &msgs);

    let chunk = read_journal_from(&path, 2).unwrap();
    assert_eq!(chunk.records.len(), 3);
    assert_eq!(chunk.records[0].0, 2);
    assert_eq!(chunk.records[2].0, 4);
    assert!(chunk.at_eof);
}

#[test]
fn read_past_end_returns_empty_chunk_at_eof() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let msgs: Vec<_> = (0..3).map(msg).collect();
    write_events(&path, &msgs);

    let chunk = read_journal_from(&path, 5).unwrap();
    assert!(chunk.records.is_empty());
    // When reading past the end, last_seq reflects the actual lines read (3),
    // not the requested from_seq (5).
    assert_eq!(chunk.last_seq, 3);
    assert!(chunk.at_eof);
}

#[test]
fn trailing_partial_line_is_not_returned_and_byte_offset_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let msgs: Vec<_> = (0..3).map(msg).collect();
    write_events(&path, &msgs);

    // Truncate the last newline.
    let content = std::fs::read(&path).unwrap();
    let truncated = &content[..content.len() - 1];
    std::fs::write(&path, truncated).unwrap();

    // First read: should see only the 2 complete records.
    let chunk1 = read_journal_from(&path, 0).unwrap();
    assert_eq!(chunk1.records.len(), 2);
    assert_eq!(chunk1.last_seq, 2);
    assert!(!chunk1.at_eof);

    // Second read from 2: same file (no new complete lines), returns empty.
    let chunk2 = read_journal_from(&path, 2).unwrap();
    assert!(chunk2.records.is_empty());
    assert_eq!(chunk2.last_seq, 2);
    assert!(!chunk2.at_eof);

    // Now append the newline + a new record.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(b"\n").unwrap();
    let new_msg = msg(3);
    let line = serde_json::to_vec(&new_msg).unwrap();
    file.write_all(&line).unwrap();
    file.write_all(b"\n").unwrap();

    // Third read from 2: should see the previously-partial line (now complete) and the new one.
    let chunk3 = read_journal_from(&path, 2).unwrap();
    assert_eq!(chunk3.records.len(), 2);
    assert_eq!(chunk3.records[0].0, 2);
    assert_eq!(chunk3.records[1].0, 3);
    assert!(chunk3.at_eof);
}
