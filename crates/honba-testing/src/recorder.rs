//! Event recorder handler.

use std::sync::{Arc, Mutex};

use honba_engine::{Handler, Result};
use honba_messages::{Event, UnixNanos};

/// A handler that records every event it receives.
///
/// Cheap to clone; clones share the same underlying buffer, so a test can
/// register one clone with the engine and read from another after the run.
///
/// ```
/// use honba_engine::{Engine, Handler};
/// use honba_testing::{Recorder, VecFeed};
///
/// let rec = Recorder::new();
/// let mut engine = Engine::new();
/// engine.add_handler(rec.clone());
///
/// let mut feed = VecFeed::new(vec![VecFeed::quote("X", 1.0, 2.0, 5)]);
/// engine.run(&mut feed).unwrap();
///
/// assert_eq!(rec.len(), 1);
/// assert_eq!(rec.timestamps(), vec![5]);
/// ```
#[derive(Clone, Default)]
pub struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
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
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    /// Returns the `ts_event` of every recorded event, in order.
    pub fn timestamps(&self) -> Vec<u64> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.ts_event().as_u64())
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
    fn on_start(&mut self) -> Result<()> {
        *self.started.lock().unwrap() = true;
        Ok(())
    }

    fn on_event(
        &mut self,
        event: &Event,
        _ts_init: UnixNanos,
    ) -> Result<honba_engine::EngineOutput> {
        self.events.lock().unwrap().push(event.clone());
        Ok(honba_engine::EngineOutput::None)
    }

    fn on_stop(&mut self) -> Result<()> {
        *self.stopped.lock().unwrap() = true;
        Ok(())
    }
}
