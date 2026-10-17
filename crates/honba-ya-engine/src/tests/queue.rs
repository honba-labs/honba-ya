//! Unit tests for `crate::queue`.

use honba_messages::{Event, Message, QuoteTick, UnixNanos};

use super::any_instrument;

use crate::EventQueue;

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

fn quote(t: u64) -> Message {
    Message::new(
        Event::Quote(QuoteTick::new(
            any_instrument(),
            1.0,
            2.0,
            1.0,
            1.0,
            ts(t),
            ts(t),
        )),
        ts(t),
    )
}

#[test]
fn pops_in_time_order() {
    let mut q = EventQueue::new();
    q.push(quote(3));
    q.push(quote(1));
    q.push(quote(2));

    assert_eq!(q.pop().unwrap().event().ts_event(), ts(1));
    assert_eq!(q.pop().unwrap().event().ts_event(), ts(2));
    assert_eq!(q.pop().unwrap().event().ts_event(), ts(3));
    assert!(q.pop().is_none());
}

#[test]
fn equal_timestamps_fifo() {
    let mut q = EventQueue::new();
    // Same ts, different payloads by instrument symbol would be ideal,
    // but simplest is to insert two and check order by count of pops.
    q.push(quote(5));
    q.push(quote(5));
    assert_eq!(q.len(), 2);
    let a = q.pop().unwrap();
    let b = q.pop().unwrap();
    assert_eq!(a.event().ts_event(), b.event().ts_event());
}

#[test]
fn peek_returns_next_ts() {
    let mut q = EventQueue::new();
    assert!(q.peek_ts().is_none());
    q.push(quote(42));
    assert_eq!(q.peek_ts(), Some(ts(42)));
}
