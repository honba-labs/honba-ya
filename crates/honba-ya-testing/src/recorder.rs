//! Event recorder handler.

use std::fmt;
use std::sync::{Arc, Mutex};

use honba_core::chrono::{DateTime, Utc};

use crate::ports::FeedItem;

/// Error surfaced by the local [`Handler`] lifecycle hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerError {
    /// Human-readable failure reason.
    pub message: String,
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "handler error: {}", self.message)
    }
}

impl std::error::Error for HandlerError {}

/// Result alias for the [`Handler`] lifecycle hooks.
pub type HandlerResult<T> = Result<T, HandlerError>;

/// A lifecycle target for a feed-driven run.
///
/// The owning loop calls [`on_start`](Handler::on_start) before the first item,
/// [`on_event`](Handler::on_event) for every item, and [`on_stop`](Handler::on_stop) once the
/// feed is exhausted. [`Recorder`] is the reference implementation.
pub trait Handler {
    /// Called once before the first event.
    fn on_start(&mut self) -> HandlerResult<()>;
    /// Called for every event with its event timestamp.
    fn on_event(&mut self, item: &FeedItem, ts: DateTime<Utc>) -> HandlerResult<()>;
    /// Called once after the run ends.
    fn on_stop(&mut self) -> HandlerResult<()>;
}

/// A handler that records every event it receives.
///
/// Cheap to clone; clones share the same underlying buffer, so a test can
/// drive one clone and read from another after the run.
///
/// ```
/// use honba_testing::{Handler, MarketDataFeed, Recorder, VecFeed};
///
/// # tokio::runtime::Builder::new_current_thread()
/// #     .build()
/// #     .unwrap()
/// #     .block_on(async {
/// let rec = Recorder::new();
/// let mut driven = rec.clone();
/// driven.on_start().unwrap();
///
/// let mut feed = VecFeed::new(vec![VecFeed::quote("X", 1.0, 2.0, 5)]);
/// while let Some(item) = feed.next().await.unwrap() {
///     let ts = item.timestamp();
///     driven.on_event(&item, ts).unwrap();
/// }
/// driven.on_stop().unwrap();
///
/// assert_eq!(rec.len(), 1);
/// assert_eq!(rec.timestamps(), vec![5]);
/// assert!(rec.started() && rec.stopped());
/// # });
/// ```
#[derive(Clone, Default)]
pub struct Recorder {
    events: Arc<Mutex<Vec<FeedItem>>>,
    started: Arc<Mutex<bool>>,
    stopped: Arc<Mutex<bool>>,
}

impl Recorder {
    /// Creates an empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Returns `true` if no events were recorded.
    pub fn is_empty(&self) -> bool {
        self.events.lock().unwrap().is_empty()
    }

    /// Returns the recorded events as a clone.
    pub fn events(&self) -> Vec<FeedItem> {
        self.events.lock().unwrap().clone()
    }

    /// Returns the timestamp of every recorded event as epoch nanoseconds, in order.
    pub fn timestamps(&self) -> Vec<i64> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|item| item.timestamp().timestamp_nanos_opt().unwrap_or(0))
            .collect()
    }

    /// Returns `true` if `on_start` was called.
    pub fn started(&self) -> bool {
        *self.started.lock().unwrap()
    }

    /// Returns `true` if `on_stop` was called.
    pub fn stopped(&self) -> bool {
        *self.stopped.lock().unwrap()
    }
}

impl Handler for Recorder {
    fn on_start(&mut self) -> HandlerResult<()> {
        *self.started.lock().unwrap() = true;
        Ok(())
    }

    fn on_event(&mut self, item: &FeedItem, _ts: DateTime<Utc>) -> HandlerResult<()> {
        self.events.lock().unwrap().push(item.clone());
        Ok(())
    }

    fn on_stop(&mut self) -> HandlerResult<()> {
        *self.stopped.lock().unwrap() = true;
        Ok(())
    }
}
