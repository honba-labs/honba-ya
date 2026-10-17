#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! The async runtime shell around the synchronous Honba kernel.
//!
//! The event loop is synchronous and single-threaded, and it stays that way:
//! [`honba_engine::Engine`] owns the queue, the clock and the audit trail, and
//! every surface — backtest, paper, live — drives that same kernel. Asynchrony
//! is the exception, so it lives here, at the edges, and nowhere else.
//!
//! [`EngineHandle`] owns exactly one engine in exactly one task. It never lends
//! the engine out, so there is no lock on it and no concurrency hazard to reason
//! about: a command is a message on a bounded channel, and the kernel orders
//! everything else.
//!
//! # The six rules
//!
//! 1. **One engine, one thread.** [`EngineHandle`] is `!Sync` and owns its task.
//!    No `Arc<Mutex<Engine>>` anywhere.
//! 2. **All I/O is async; all kernel CPU work is sync and inline.** The event
//!    loop never awaits: [`Engine::inject`](honba_engine::Engine::inject) and
//!    [`Engine::pump`](honba_engine::Engine::pump) are the only kernel calls the
//!    task makes while dispatching, and both are synchronous. The awaits are on
//!    the feed and on the command channel, never on the dispatch path.
//! 3. **Ordering is preserved by the kernel, not the transport.** Feeds may
//!    burst or arrive out of order; the shell injects in arrival order and pumps
//!    the kernel to empty, so the kernel's
//!    [`EventQueue`](honba_engine::EventQueue) decides the order on `ts_event`.
//!    See [`EngineHandle`] for which branch wins when a command and a message
//!    are both ready.
//! 4. **Backpressure is explicit.** A bounded `mpsc`; a slow consumer stalls its
//!    own feed, not the runtime. [`EngineHandle::spawn_with_capacity`] sets the
//!    bound, [`EngineHandle::submit`] waits for room, and
//!    [`EngineHandle::try_submit`] reports [`AsyncError::Rejected`] instead of
//!    dropping a command.
//! 5. **Cancellation is a command, not an abort.** [`Command::Stop`] drains the
//!    queue and runs `on_stop`, so the audit stream is always complete:
//!    [`EngineHandle::shutdown`] sends it and then waits for
//!    [`Engine::finish`](honba_engine::Engine::finish).
//!    [`JoinHandle::abort`](tokio::task::JoinHandle::abort) is reserved for
//!    panics, which surface as [`AsyncError::Panicked`] through
//!    [`EngineHandle::join`].
//! 6. **`Clock` is the only time source.** [`LiveClock`] wraps
//!    [`tokio::time::Instant`]; [`HistoricClock`] advances with the data. Domain
//!    code never sees either — the kernel keeps its own monotonic clock, and a
//!    handler only ever reads timestamps off the messages it is given.
//!
//! # Shape of a run
//!
//! ```
//! use honba_async::{BoxedFeed, Command, EngineHandle, TradingState};
//! use honba_engine::Engine;
//! use honba_testing::{Recorder, VecFeed, VecMessageFeed};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut engine = Engine::new();
//! engine.add_handler(Recorder::new());
//! let messages = vec![VecFeed::bar("X", 101.0, 1), VecFeed::bar("X", 102.0, 2)];
//!
//! let feed = VecMessageFeed::new(messages);
//! let handle = EngineHandle::spawn(engine, Box::new(feed) as BoxedFeed);
//!
//! handle.submit(Command::State(TradingState::Halted)).await.unwrap();
//! handle.shutdown().await.unwrap();
//! # }
//! ```

pub mod clocks;
pub mod command;
pub mod error;
pub mod handle;

pub use clocks::{HistoricClock, LiveClock};
pub use command::Command;
pub use error::{AsyncError, Result};
pub use handle::{BoxedFeed, EngineHandle, DEFAULT_COMMAND_CAPACITY};
pub use honba_engine::TradingState;

#[cfg(test)]
mod tests;
