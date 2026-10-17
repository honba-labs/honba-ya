//! Bridge from the async [`honba_ports::ExecutionGateway`] to the sync [`ExecutionVenue`]
//! (ADR 0030, decision-log entry "async gateway bridge").
//!
//! The order routes call [`ExecutionEngine::submit`] and `cancel` from async handlers while
//! holding a std mutex, so those calls must never wait on a broker. [`GatewayVenue`] therefore
//! does not call the gateway at all: it owns a *command channel* to one task that owns the
//! gateway. `submit` and `cancel` enqueue a command and return `Ok(())`; the task awaits the
//! gateway, and everything it learns (a refusal, a fill, a cancel acknowledgement) is pushed as
//! an [`ExecutionEvent`] onto a shared queue that [`ExecutionEngine::drain_events`] empties.
//! The lock is held for a channel send or a queue swap only, never across an await.
//!
//! A refusal's `reason` is `<stable ErrorCode>: <the broker's own text>` (bounded to 256 chars):
//! the ledger keeps the code and puts the text in `reject_reason.message`.
//!
//! Ids: the ledger is keyed by the client order id, the gateway answers with its own venue id.
//! The task keeps `venue id -> client order` and rewrites every fill and event back to the client
//! id (a fill already carrying a client id is accepted as is). Fills are tracked per order to
//! report `cum_qty` and `complete`, which the ledger maps to `PartiallyFilled` / `Filled`.
//!
//! Events surface when somebody drains: [`AppState::advance_market`] does (with or without bars),
//! and [`AppState::spawn_venue_drain`] does it on a timer for a venue with no bar feed.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use honba_engine::{AlgoError, ExecutionEngine, Result as EngineResult};
use honba_entities::{ExecutionEvent, Trade};
use honba_messages::ErrorCode;
use honba_messages::{Order, OrderId, UnixNanos};
use honba_ports::{ExecutionGateway, PortError};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::ExecutionVenue;

/// Default interval between polls of the gateway's fill stream.
const DEFAULT_POLL: Duration = Duration::from_millis(50);

/// Tolerance when deciding that cumulative fills cover the order quantity.
const QTY_EPS: f64 = 1e-9;

enum Command {
    Submit(Box<Order>),
    Cancel { order_id: String, now: UnixNanos },
}

type Events = Arc<Mutex<VecDeque<ExecutionEvent>>>;

/// An [`ExecutionVenue`] backed by an async [`ExecutionGateway`] running on its own task.
///
/// Create it inside a tokio runtime with [`GatewayVenue::spawn`]. Dropping it closes the command
/// channel and the task ends after the commands already queued.
pub struct GatewayVenue {
    commands: UnboundedSender<Command>,
    events: Events,
    task: JoinHandle<()>,
}

impl GatewayVenue {
    /// Spawns the gateway task (poll interval 50 ms). Must be called inside a tokio runtime.
    pub fn spawn(gateway: impl ExecutionGateway + 'static) -> Self {
        Self::spawn_with_poll(gateway, DEFAULT_POLL)
    }

    /// Spawns the gateway task, polling the fill stream every `poll` (at least 1 ms).
    pub fn spawn_with_poll(gateway: impl ExecutionGateway + 'static, poll: Duration) -> Self {
        let (commands, rx) = mpsc::unbounded_channel();
        let events = Events::default();
        let task = tokio::spawn(run(
            gateway,
            rx,
            Arc::clone(&events),
            poll.max(Duration::from_millis(1)),
        ));
        Self {
            commands,
            events,
            task,
        }
    }

    fn send(&self, command: Command) -> EngineResult<()> {
        if self.task.is_finished() {
            return Err(AlgoError::Component(
                "execution gateway task stopped".into(),
            ));
        }
        self.commands
            .send(command)
            .map_err(|_| AlgoError::Component("execution gateway task stopped".into()))
    }
}

impl ExecutionEngine for GatewayVenue {
    fn submit(&mut self, order: Order) -> EngineResult<()> {
        self.send(Command::Submit(Box::new(order)))
    }

    fn cancel(&mut self, order_id: &str, now: UnixNanos) -> EngineResult<()> {
        self.send(Command::Cancel {
            order_id: order_id.to_owned(),
            now,
        })
    }

    fn native_events(&self) -> bool {
        true
    }

    fn drain_events(&mut self) -> EngineResult<Vec<ExecutionEvent>> {
        Ok(lock(&self.events).drain(..).collect())
    }

    fn drain_fills(&mut self) -> EngineResult<Vec<Trade>> {
        // Legacy view: the ledger path uses `drain_events`; fills are not buffered twice.
        Ok(Vec::new())
    }
}

impl ExecutionVenue for GatewayVenue {}

fn lock(events: &Events) -> MutexGuard<'_, VecDeque<ExecutionEvent>> {
    events.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A working order the task is tracking.
struct Working {
    order: Order,
    venue_id: OrderId,
    cum: f64,
}

async fn run<G: ExecutionGateway>(
    mut gateway: G,
    mut rx: UnboundedReceiver<Command>,
    events: Events,
    poll: Duration,
) {
    let mut working: HashMap<String, Working> = HashMap::new();
    let mut tick = tokio::time::interval(poll);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = rx.recv() => match command {
                Some(Command::Submit(order)) => {
                    submit(&mut gateway, &mut working, &events, *order).await;
                    poll_fills(&mut gateway, &mut working, &events).await;
                }
                Some(Command::Cancel { order_id, now }) => {
                    // Fills first, so the remainder below excludes whatever already filled.
                    poll_fills(&mut gateway, &mut working, &events).await;
                    cancel(&mut gateway, &mut working, &events, &order_id, now).await;
                }
                None => break,
            },
            _ = tick.tick() => poll_fills(&mut gateway, &mut working, &events).await,
        }
    }
}

async fn submit<G: ExecutionGateway>(
    gateway: &mut G,
    working: &mut HashMap<String, Working>,
    events: &Events,
    order: Order,
) {
    match gateway.submit_order(order.clone()).await {
        Ok(venue_id) => {
            working.insert(
                order.order_id().as_str().to_owned(),
                Working {
                    order,
                    venue_id,
                    cum: 0.0,
                },
            );
        }
        Err(error) => {
            // A timeout or transport error leaves the order's venue state unknown; the ledger
            // has no "unknown" status, so it is rejected and the gap is documented (ADR 0030).
            let code = error_code(&error);
            tracing::warn!(order = %order.order_id(), %error, code = %code, "gateway refused submit");
            lock(events).push_back(ExecutionEvent::Rejected {
                order_id: order.order_id().clone(),
                instrument_id: order.instrument_id().clone(),
                side: order.side(),
                quantity: order.quantity(),
                reason: format!("{}: {}", code.as_str(), error_text(&error)),
                venue_order_id: None,
                ts: order.ts_event(),
            });
        }
    }
}

/// The most characters of a broker's own text kept in a rejection reason.
const MAX_TEXT_CHARS: usize = 256;

/// The broker's own words for `error`, bounded to [`MAX_TEXT_CHARS`] characters (cut on a char
/// boundary, ending in `…`). A venue refusal reads `<code>: <message>`; other errors carry
/// their inner text.
fn error_text(error: &PortError) -> String {
    let text = match error {
        PortError::Rejected { code, message } if message.is_empty() || message == code => {
            code.clone()
        }
        PortError::Rejected { code, message } => format!("{code}: {message}"),
        PortError::Unavailable(t)
        | PortError::Transport(t)
        | PortError::Retriable(t)
        | PortError::Terminal(t)
        | PortError::Unrecoverable(t)
        | PortError::InvalidRequest(t)
        | PortError::Unsupported(t)
        | PortError::Internal(t) => t.clone(),
        other => other.to_string(),
    };
    if text.chars().count() <= MAX_TEXT_CHARS {
        return text;
    }
    let mut cut: String = text.chars().take(MAX_TEXT_CHARS - 1).collect();
    cut.push('…');
    cut
}

async fn cancel<G: ExecutionGateway>(
    gateway: &mut G,
    working: &mut HashMap<String, Working>,
    events: &Events,
    order_id: &str,
    now: UnixNanos,
) {
    let Some(w) = working.get(order_id) else {
        return; // unknown or finished: a no-op, as the engine contract says
    };
    match gateway.cancel_order(w.venue_id.clone()).await {
        Ok(()) => {
            let w = working.remove(order_id).expect("looked up above");
            lock(events).push_back(ExecutionEvent::Cancelled {
                order_id: w.order.order_id().clone(),
                instrument_id: w.order.instrument_id().clone(),
                side: w.order.side(),
                quantity: (w.order.quantity() - w.cum).max(0.0),
                venue_order_id: None,
                ts: now,
            });
        }
        Err(error) => {
            // The order stays working: a refused cancel (already filled or cancelled) is
            // reconciled by the fill stream, an unreachable venue by the caller's retry.
            tracing::warn!(order = order_id, %error, code = %error_code(&error), "gateway refused cancel");
        }
    }
}

/// The stable wire error code a gateway failure is reported under.
///
/// No new codes: a venue refusal on its merits is `order_rejected`, a venue that cannot be
/// reached (unavailable, retriable, terminal, needs the operator) is
/// `order_execution_unavailable`, and the rest keep their transport meaning.
fn error_code(error: &PortError) -> ErrorCode {
    match error {
        PortError::Rejected { .. } => ErrorCode::OrderRejected,
        PortError::InvalidRequest(_) => ErrorCode::ValidationInvalidRequest,
        PortError::Unsupported(_) => ErrorCode::Unsupported,
        PortError::Unavailable(_)
        | PortError::Retriable(_)
        | PortError::Terminal(_)
        | PortError::Unrecoverable(_) => ErrorCode::OrderExecutionUnavailable,
        PortError::Timeout => ErrorCode::Timeout,
        PortError::Transport(_) => ErrorCode::TransportError,
        _ => ErrorCode::InternalError,
    }
}

async fn poll_fills<G: ExecutionGateway>(
    gateway: &mut G,
    working: &mut HashMap<String, Working>,
    events: &Events,
) {
    loop {
        let trade = match gateway.next_fill().await {
            Ok(Some(trade)) => trade,
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, retryable = error.is_retryable(), "gateway fill stream error");
                break;
            }
        };
        let key = working
            .iter()
            .find(|(client, w)| {
                trade.order_id() == &w.venue_id || trade.order_id().as_str() == client.as_str()
            })
            .map(|(client, _)| client.clone());
        let Some(client) = key else {
            tracing::warn!(order = %trade.order_id(), "gateway fill for an unknown order dropped");
            continue;
        };
        let w = working.get_mut(&client).expect("key just found");
        w.cum += trade.quantity();
        let complete = w.cum >= w.order.quantity() - QTY_EPS;
        let cum_qty = w.cum;
        let mapped = Trade::new(
            w.order.order_id().clone(),
            trade.instrument_id().clone(),
            trade.side(),
            trade.quantity(),
            trade.price(),
            trade.costs().currency(),
            trade.ts_event(),
            trade.ts_init(),
        )
        .with_costs(trade.costs());
        let venue_order_id = None;
        lock(events).push_back(ExecutionEvent::Fill {
            trade: mapped,
            cum_qty,
            complete,
            venue_order_id,
        });
        if complete {
            working.remove(&client);
        }
    }
}

impl crate::AppState {
    /// Spawns a task that drains the execution venue every `every` (at least 1 ms), so events a
    /// venue produces on its own clock (a broker's fills, a [`GatewayVenue`]'s refusals) reach
    /// the order ledger without waiting for a bar. It calls `advance_market` with no bars, so
    /// it never moves simulated time; it runs until the handle is aborted.
    ///
    /// Must be called inside a tokio runtime.
    pub fn spawn_venue_drain(&self, every: Duration) -> JoinHandle<()> {
        let state = self.clone();
        let every = every.max(Duration::from_millis(1));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = state.advance_market(&[]) {
                    tracing::warn!(error = %e.message, "venue drain failed");
                }
            }
        })
    }
}
