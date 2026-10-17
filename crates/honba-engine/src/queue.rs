//! Priority queue of events ordered by `ts_event`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use honba_messages::{Event, Message, UnixNanos};

/// Wrapper providing min-heap ordering on `Event::ts_event`.
struct Queued {
    ts: UnixNanos,
    seq: u64,
    msg: Message,
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.seq == other.seq
    }
}
impl Eq for Queued {}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; invert to get min-heap on ts.
        // Tie-break on sequence number to preserve insertion order.
        other
            .ts
            .cmp(&self.ts)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// A time-ordered queue of [`Message`] values.
///
/// Events are popped in ascending `ts_event` order. Events with equal
/// timestamps pop in insertion order (FIFO).
#[derive(Default)]
pub struct EventQueue {
    heap: BinaryHeap<Queued>,
    next_seq: u64,
}

impl EventQueue {
    /// Creates an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes a message onto the queue.
    pub fn push(&mut self, msg: Message) {
        let ts = msg.event().ts_event();
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.heap.push(Queued { ts, seq, msg });
    }

    /// Pushes an event with the given `ts_init`.
    pub fn push_event(&mut self, event: Event, ts_init: UnixNanos) {
        self.push(Message::new(event, ts_init));
    }

    /// Pops the next message, if any.
    pub fn pop(&mut self) -> Option<Message> {
        self.heap.pop().map(|q| q.msg)
    }

    /// Peeks at the next message's timestamp without removing it.
    pub fn peek_ts(&self) -> Option<UnixNanos> {
        self.heap.peek().map(|q| q.ts)
    }

    /// Returns the number of queued messages.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}
