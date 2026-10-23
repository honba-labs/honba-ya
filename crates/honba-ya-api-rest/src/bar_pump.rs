//! The live bar feed behind the simulated market (E3-S7 follow-up).
//!
//! [`AppState::spawn_bar_pump`] consumes a `tokio::sync::mpsc` receiver of [`Bar`]s and calls
//! [`AppState::advance_market`] for each, so a running server fills orders as bars arrive.
//! Event time is the bars' own `ts_event`; the pump never reads the wall clock. It ends when
//! every sender is dropped (the graceful shutdown: the composition root drops its sender, or
//! aborts the task). An `advance_market` error (an out-of-order bar) is logged and counted,
//! and the pump keeps going.

use std::sync::PoisonError;

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use honba_core::rust_decimal::prelude::ToPrimitive;
use honba_core::Bar;

use crate::AppState;

/// Converts a canonical bar to `honba-feed`'s own wire shape for
/// [`LiveQuoteStore::put_bar`](honba_feed::LiveQuoteStore::put_bar) (ADR 0030; see
/// `honba-me-feed::wire`'s module doc for why the two `Bar` types differ: that crate's
/// columnar engine is still `f64`/dual-timestamp and has not been ported onto `Decimal`).
/// `put_bar` only reads the instrument, close and `ts_event`, so the bar specification
/// built here is a placeholder — a live-fed [`honba_core::Bar`] carries no timeframe of its
/// own to put in it, and nothing downstream of `put_bar` reads it back.
fn to_feed_bar(bar: &Bar) -> honba_feed::wire::Bar {
    let spec = honba_feed::wire::BarSpecification::new(
        1,
        honba_feed::wire::BarAggregation::Minute,
        honba_feed::wire::PriceType::Last,
    );
    let ts = honba_feed::wire::UnixNanos::from_datetime(bar.timestamp);
    honba_feed::wire::Bar::new(
        honba_feed::wire::BarType::new(bar.instrument.clone(), spec),
        bar.open.0.to_f64().unwrap_or_default(),
        bar.high.0.to_f64().unwrap_or_default(),
        bar.low.0.to_f64().unwrap_or_default(),
        bar.close.0.to_f64().unwrap_or_default(),
        bar.volume.0.to_f64().unwrap_or_default(),
        ts,
        ts,
    )
}

/// What a finished bar pump did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BarPumpReport {
    /// Bars received from the source.
    pub bars: u64,
    /// Bars whose `advance_market` call failed (logged, then skipped).
    pub errors: u64,
}

impl AppState {
    /// Spawns a task feeding every bar from `bars` to [`AppState::advance_market`].
    ///
    /// Must be called inside a tokio runtime. The task ends when the channel closes and then
    /// returns its [`BarPumpReport`].
    pub fn spawn_bar_pump(&self, bars: Receiver<Bar>) -> JoinHandle<BarPumpReport> {
        let state = self.clone();
        tokio::spawn(async move { state.run_bar_pump(bars).await })
    }

    /// Starts the live bar ingest behind `POST /bars`: a bounded channel of `capacity` bars (at
    /// least 1) whose sender the state keeps and whose receiver a [`spawn_bar_pump`] task drains.
    ///
    /// Must be called inside a tokio runtime. Starting again replaces the sender, which ends the
    /// previous pump. Call [`AppState::stop_bar_ingest`] to shut down: it drops the sender, the
    /// pump drains what is queued and then returns its [`BarPumpReport`].
    ///
    /// [`spawn_bar_pump`]: AppState::spawn_bar_pump
    pub fn start_bar_ingest(&self, capacity: usize) -> JoinHandle<BarPumpReport> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        self.attach_bar_ingest(tx);
        self.spawn_bar_pump(rx)
    }

    /// Makes `POST /bars` send into `sender`; whoever owns the receiver drains it (usually
    /// [`AppState::spawn_bar_pump`]). [`AppState::start_bar_ingest`] is this plus the pump.
    pub fn attach_bar_ingest(&self, sender: Sender<Bar>) {
        *self
            .bar_ingest
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(sender);
    }

    /// Detaches the ingest sender so `POST /bars` answers `bar_ingest_disabled` again. The pump
    /// ends once every other sender clone (an in-flight request) is gone and the queue is drained.
    pub fn stop_bar_ingest(&self) {
        self.bar_ingest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    /// A clone of the ingest sender, if ingest is on.
    pub(crate) fn bar_ingest_sender(&self) -> Option<Sender<Bar>> {
        self.bar_ingest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Runs the bar pump on the current task until `bars` closes.
    pub async fn run_bar_pump(&self, mut bars: Receiver<Bar>) -> BarPumpReport {
        let mut report = BarPumpReport::default();
        while let Some(bar) = bars.recv().await {
            report.bars += 1;
            if let Some(live) = self.live_quotes() {
                live.put_bar(&to_feed_bar(&bar));
            }
            if let Err(e) = self.advance_market(std::slice::from_ref(&bar)) {
                report.errors += 1;
                tracing::warn!(ts_event = ?bar.timestamp, error = %e.message, "bar pump: advance_market failed; bar skipped");
            }
        }
        tracing::info!(
            bars = report.bars,
            errors = report.errors,
            "bar pump: source closed"
        );
        report
    }
}
