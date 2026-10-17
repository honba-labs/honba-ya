//! The single-writer task that owns one [`Engine`].

use std::cell::Cell;
use std::marker::PhantomData;

use honba_engine::{AuditRecord, Engine};
use honba_ports::MarketDataFeed;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::command::Command;
use crate::error::{AsyncError, Result};

/// Default capacity of the command channel.
pub const DEFAULT_COMMAND_CAPACITY: usize = 64;

/// A market-data feed owned by the engine task.
///
/// The feed is a port, so it is `Send` and driven by one task; the shell never
/// lends it out either.
pub type BoxedFeed = Box<dyn MarketDataFeed + Send>;

/// The handle to one running [`Engine`] task.
///
/// # One engine, one thread
///
/// The task this handle owns is the only thing that touches the engine. The
/// engine is moved into the task and never moved out, shared, or locked: there
/// is no `Arc<Mutex<Engine>>` here and no path by which one could be added
/// without giving up this type's guarantees. The handle is therefore `Send` — it
/// can be moved to the thread that supervises a run — and **not** `Sync`: the
/// `PhantomData<Cell<()>>` marker below makes that a compile error rather than a
/// convention. `Cell` is `!Sync` and `Send`, which is exactly the pair of bounds
/// wanted here.
///
/// A static assertion that the type is `!Sync` is not expressible in stable
/// Rust; the marker field is the enforcement, and the `Send` assertions in the
/// unit tests pin the other half.
///
/// # What the task does
///
/// Once started, the task selects between the command channel and the feed. A
/// message from the feed is injected into the kernel and the kernel is then
/// pumped until its queue is empty, so a burst is dispatched in `ts_event` order
/// by the kernel rather than in arrival order by the transport. A command is
/// applied between two dispatched messages. The loop ends on
/// [`Command::Stop`], on feed exhaustion, or on a feed error; whatever the
/// reason, [`Engine::finish`] runs afterwards, so the queue drains and every
/// handler sees `on_stop`.
///
/// # Dropping a handle
///
/// Dropping the handle without [`EngineHandle::shutdown`] detaches the task: it
/// keeps running, its result is discarded, and nobody observes whether the queue
/// drained or `on_stop` ran. The audit stream of a run abandoned that way may be
/// incomplete, which is the whole reason `shutdown` exists. Dropping is the
/// right thing only when the process is going away anyway.
///
/// # Backpressure
///
/// The command channel is bounded. [`EngineHandle::submit`] waits for room;
/// [`EngineHandle::try_submit`] reports [`AsyncError::Rejected`] instead. A
/// caller that will not wait has lost the command, so `Rejected` is an error,
/// not a dropped message.
pub struct EngineHandle {
    tx: mpsc::Sender<Command>,
    join: JoinHandle<Result<Vec<AuditRecord>>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl EngineHandle {
    /// Spawns the single-writer task on the current runtime with the default command capacity.
    ///
    /// The task starts on the next time the runtime polls it, so a handle
    /// returned by this function is already running but has not yet called
    /// `on_start`.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime: `tokio::spawn` has no runtime to
    /// spawn onto. Build the handle inside `#[tokio::main]` or
    /// `#[tokio::test]`.
    pub fn spawn(engine: Engine, feed: BoxedFeed) -> Self {
        Self::spawn_with_capacity(engine, feed, DEFAULT_COMMAND_CAPACITY)
    }

    /// Same, with an explicit bounded command channel capacity.
    ///
    /// A capacity of zero is a rendezvous channel: the task must be waiting on
    /// the channel for any command to be accepted at all, and `try_submit` on it
    /// succeeds only in that window.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime, for the same reason as
    /// [`EngineHandle::spawn`].
    pub fn spawn_with_capacity(engine: Engine, feed: BoxedFeed, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let join = tokio::spawn(engine_task(engine, feed, rx));
        tracing::debug!(capacity, "honba-async: engine task spawned");
        Self {
            tx,
            join,
            _not_sync: PhantomData,
        }
    }

    /// Sends a command, waiting while the channel is full (backpressure, rule 4).
    ///
    /// The wait is on this caller, not on the runtime: no other task is blocked
    /// by a full channel, and the engine keeps dispatching whatever it already
    /// has queued. Returns [`AsyncError::Closed`] once the task has stopped.
    pub async fn submit(&self, cmd: Command) -> Result<()> {
        self.tx.send(cmd).await.map_err(|_| AsyncError::Closed)
    }

    /// Non-blocking send. Fails with [`AsyncError::Closed`] when the task is gone.
    ///
    /// Returns [`AsyncError::Rejected`] when the channel is full. Rejecting is
    /// the honest answer: the command was not queued, so reporting success would
    /// be a lie the caller cannot detect.
    pub fn try_submit(&self, cmd: Command) -> Result<()> {
        self.tx.try_send(cmd).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => {
                AsyncError::Rejected("the command channel is full".to_string())
            }
            mpsc::error::TrySendError::Closed(_) => AsyncError::Closed,
        })
    }

    /// Sends `Command::Stop`, then waits for the task to finish (rule 5). Idempotent.
    ///
    /// The send is best effort: a task that has already stopped is not an error
    /// here, and only the join decides the outcome. Calling this twice is safe —
    /// the second call finds the channel closed, sends nothing, and reports the
    /// same result.
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_and_audit().await.map(|_| ())
    }

    /// Waits for the task to finish without sending `Stop` first.
    ///
    /// Use this for a task whose feed has ended: the engine will finish on its
    /// own and there is nothing to stop.
    pub async fn join(self) -> Result<()> {
        self.join_and_audit().await.map(|_| ())
    }

    /// Waits for the task to finish and returns the audit trail it produced.
    ///
    /// The same as [`EngineHandle::join`], plus the readback. It exists because
    /// the engine is owned by the task and dropped when the task ends: without
    /// it the [`AuditLog`](honba_engine::AuditLog) a run produced would be
    /// unreachable from outside the crate, and a caller could never verify that
    /// the audit stream is complete.
    pub async fn join_and_audit(self) -> Result<Vec<AuditRecord>> {
        let Self { join, .. } = self;
        wait(join).await
    }

    /// Sends `Command::Stop`, then waits and returns the audit trail.
    ///
    /// [`EngineHandle::shutdown`] plus the readback of
    /// [`EngineHandle::join_and_audit`].
    pub async fn shutdown_and_audit(self) -> Result<Vec<AuditRecord>> {
        let Self { tx, join, .. } = self;
        let _ = tx.send(Command::Stop).await;
        wait(join).await
    }

    /// Whether the task is still running.
    ///
    /// False once the task has returned, whether it drained cleanly or failed.
    /// A freshly spawned task counts as running: it has not been polled yet.
    pub fn is_running(&self) -> bool {
        !self.join.is_finished()
    }
}

async fn wait(join: JoinHandle<Result<Vec<AuditRecord>>>) -> Result<Vec<AuditRecord>> {
    match join.await {
        Ok(result) => result,
        Err(err) if err.is_panic() => {
            tracing::error!("honba-async: engine task panicked");
            Err(AsyncError::Panicked)
        }
        Err(_) => {
            tracing::error!("honba-async: engine task was cancelled");
            Err(AsyncError::Closed)
        }
    }
}

/// Builds the single-writer task future: start, drive, finish, read back.
///
/// The order is the whole point. [`Engine::start`] opens the handler lifecycle,
/// [`drive`] runs until the feed ends or a command stops it, and
/// [`Engine::finish`] runs whatever happens — it drains the queue before
/// `on_stop`, so the audit trail is complete even when the loop stopped early.
/// The audit is read back after `finish`, so a caller that gets it back can
/// treat it as the run's complete record.
pub(crate) async fn engine_task(
    mut engine: Engine,
    mut feed: BoxedFeed,
    mut rx: mpsc::Receiver<Command>,
) -> Result<Vec<AuditRecord>> {
    engine.start()?;
    let outcome = drive(&mut engine, &mut rx, feed.as_mut()).await;
    engine.finish()?;
    outcome?;
    Ok(engine.audit().to_vec())
}

/// Drives the loop until the feed ends, the channel closes, or a `Stop` lands.
///
/// # Which branch wins
///
/// The feed branch is polled first and the branch order is **biased**, so when a
/// command and a message are both ready the message is taken. Two properties
/// depend on that:
///
/// - *Determinism.* `tokio::select!` chooses uniformly among ready branches. An
///   unbiased select would make the same feed produce a different audit on every
///   run depending on which branch the scheduler happened to leave ready, which
///   is exactly the nondeterminism the whole design exists to avoid.
/// - *Drain, not discard.* A `Stop` never throws away data the transport
///   already had. Whatever the feed can hand over is injected and dispatched
///   first, so the audit trail of a run that was stopped covers every message
///   the venue had already delivered.
///
/// The cost is that a `Stop` lands at the first gap in the feed rather than the
/// instant it is sent: while a feed keeps producing, the loop keeps consuming.
/// That is the intended trade — a stop issued during a burst is asking for the
/// burst to finish, and the bounded command channel keeps an operator's backlog
/// from growing without limit while it drains.
///
/// # Ordering
///
/// A message is injected in arrival order and the kernel is then pumped to empty,
/// so the kernel orders on `ts_event`. The window it can reorder is what is in
/// the queue when the pump runs: an injected burst, or the fills a handler's
/// orders produce. A feed that delivers a `ts_event` earlier than one already
/// dispatched is a broken feed, not a reordering opportunity, and surfaces as
/// [`AlgoError::ClockRegression`](honba_engine::AlgoError::ClockRegression).
async fn drive(
    engine: &mut Engine,
    rx: &mut mpsc::Receiver<Command>,
    feed: &mut dyn MarketDataFeed,
) -> Result<()> {
    loop {
        let stop = tokio::select! {
            biased;
            item = feed.next() => match item {
                Ok(Some(msg)) => {
                    engine.inject(msg);
                    false
                }
                Ok(None) => true,
                Err(err) => return Err(AsyncError::Feed(err)),
            },
            command = rx.recv() => match command {
                Some(Command::Stop) => true,
                Some(Command::State(next)) => {
                    engine.set_trading_state(next);
                    false
                }
                None => true,
            },
        };
        if stop {
            return Ok(());
        }
        while engine.pump()? {}
    }
}
