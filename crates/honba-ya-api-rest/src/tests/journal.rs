//! `NdjsonJournal`: one wire `Message` per line, flush on demand, failures are kept.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use chrono::DateTime;
use honba_api::{ErrorCode, Event, Message, SCHEMA_VERSION};
use honba_api::wire::QuoteTick;
use honba_core::{Exchange, InstrumentId};
use rust_decimal::Decimal;

use crate::{JournalWriter, NdjsonJournal};

#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("disk full at /secret/path"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("disk full at /secret/path"))
    }
}

fn msg(n: u64) -> Message {
    let now = DateTime::from_timestamp(n as i64, 0).unwrap();
    let quote = QuoteTick::new(
        InstrumentId::new(Exchange::Nse, "TCS"),
        Decimal::from(100 + n),
        Decimal::from(101 + n),
        Decimal::from(10),
        Decimal::from(20),
        now,
        now,
    );
    Message::new(Event::Quote(quote), now)
}

#[test]
fn each_message_is_one_newline_terminated_line_in_append_order() {
    let sink = Shared::default();
    let mut journal = NdjsonJournal::new(sink.clone());
    for n in 1..=3 {
        journal.append(&msg(n)).unwrap();
    }
    journal.flush().unwrap();
    let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
    assert!(text.ends_with('\n'));
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3);
    for (i, line) in lines.iter().enumerate() {
        let back: Message = serde_json::from_str(line).unwrap();
        assert_eq!(back, msg(i as u64 + 1));
        assert!(line.contains(&format!("\"schema_version\":\"{SCHEMA_VERSION}\"")));
    }
}

#[test]
fn records_are_buffered_until_flush() {
    let sink = Shared::default();
    let mut journal = NdjsonJournal::new(sink.clone());
    journal.append(&msg(1)).unwrap();
    assert!(sink.0.lock().unwrap().is_empty());
    journal.flush().unwrap();
    assert!(!sink.0.lock().unwrap().is_empty());
}

#[test]
fn a_write_failure_is_kept_and_leaks_no_path() {
    let mut journal = NdjsonJournal::new(Broken);
    journal.append(&msg(1)).unwrap(); // buffered
    let first = journal.flush().unwrap_err();
    assert_eq!(first.code, ErrorCode::InternalError);
    assert_eq!(first.context.as_ref().unwrap()["reason"], "journal_write");
    assert!(!first.message.contains("/secret"));
    // Kept: every later call reports the failure instead of pretending to succeed.
    assert_eq!(journal.append(&msg(2)).unwrap_err(), first);
    assert_eq!(journal.flush().unwrap_err(), first);
}
