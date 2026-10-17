//! Data feed trait.

use honba_messages::Message;

use crate::error::Result;

/// A source of market data events.
///
/// The engine pulls from the feed until it returns `None`. Feeds may be
/// replayed from history, streamed live, or synthesised in tests.
pub trait DataFeed: Send {
    /// Returns the next message, or `None` when exhausted.
    fn next(&mut self) -> Result<Option<Message>>;
}
