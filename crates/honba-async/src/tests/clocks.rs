//! Unit tests for `crate::clocks`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use honba_messages::UnixNanos;
use honba_ports::{Clock, PortError};

use crate::{HistoricClock, LiveClock};

fn ts(nanos: u64) -> UnixNanos {
    UnixNanos::from_u64(nanos)
}

#[tokio::test]
async fn a_historic_clock_starts_at_the_value_it_was_given() {
    let clock = HistoricClock::new(ts(1_700_000_000_000_000_000));
    assert_eq!(clock.now().await, ts(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn a_historic_clock_sleep_advances_instead_of_waiting() {
    let clock = HistoricClock::new(ts(1_000));
    clock.sleep(Duration::from_millis(5)).await.unwrap();
    assert_eq!(clock.now().await, ts(1_000 + 5_000_000));
}

#[tokio::test]
async fn a_historic_clock_advancing_forward_moves_now() {
    let clock = HistoricClock::new(ts(10));
    clock.advance_to(ts(500)).unwrap();
    assert_eq!(clock.now().await, ts(500));
}

#[tokio::test]
async fn a_historic_clock_advancing_to_the_same_value_is_allowed() {
    let clock = HistoricClock::new(ts(10));
    clock.advance_to(ts(10)).unwrap();
    assert_eq!(clock.now().await, ts(10));
}

#[tokio::test]
async fn a_historic_clock_refuses_to_go_backwards() {
    let clock = HistoricClock::new(ts(500));
    let err = clock.advance_to(ts(499)).unwrap_err();
    assert_eq!(
        err,
        PortError::InvalidRequest(
            "cannot advance a historic clock backwards: 500 -> 499".to_string()
        )
    );
    assert_eq!(
        clock.now().await,
        ts(500),
        "a refused advance must not move time"
    );
}

/// Slack allowed when comparing a live clock's reading against a wall clock the
/// test read itself.
///
/// The two `SystemTime` reads are a few hundred nanoseconds apart on an idle
/// machine and further apart on a loaded one, so this only ever has to cover
/// that gap, never a behaviour.
const READ_SLOP_NANOS: u64 = 1_000_000;

#[tokio::test(start_paused = true)]
async fn a_live_clock_tracks_virtual_time_exactly() {
    let clock = LiveClock::new();
    let start = clock.now().await;

    tokio::time::advance(Duration::from_secs(30)).await;

    assert_eq!(
        clock.now().await.as_u64() - start.as_u64(),
        30_000_000_000,
        "virtual time must show up in now() exactly, with no wall-clock drift"
    );
}

#[tokio::test(start_paused = true)]
async fn a_live_clock_reads_the_wall_clock_plus_the_virtual_elapsed_time() {
    let wall_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after the epoch")
        .as_nanos() as u64;
    let clock = LiveClock::new();

    tokio::time::advance(Duration::from_secs(10)).await;

    let since_wall_epoch = clock.now().await.as_u64() - wall_epoch;
    assert!(
        since_wall_epoch + READ_SLOP_NANOS >= 10_000_000_000,
        "now() must be the wall clock plus the advance, got {since_wall_epoch}ns"
    );
    assert!(
        since_wall_epoch < 60_000_000_000,
        "now() must not jump a minute on a ten second advance, got {since_wall_epoch}ns"
    );
}

#[tokio::test(start_paused = true)]
async fn a_live_clock_sleep_returns_exactly_at_its_virtual_deadline() {
    let clock = LiveClock::new();
    let before = clock.now().await;

    clock.sleep(Duration::from_secs(5)).await.unwrap();

    assert_eq!(
        clock.now().await.as_u64() - before.as_u64(),
        5_000_000_000,
        "sleep must return at its deadline, neither before nor after"
    );
}

#[tokio::test(start_paused = true)]
async fn a_live_clock_anchored_in_the_past_still_tracks_virtual_time() {
    let clock = LiveClock::new_at(std::time::Instant::now());
    let start = clock.now().await;

    tokio::time::advance(Duration::from_secs(3)).await;

    assert_eq!(
        clock.now().await.as_u64() - start.as_u64(),
        3_000_000_000,
        "anchoring in the past must not make the clock drift"
    );
}

#[tokio::test(start_paused = true)]
async fn a_live_clock_never_moves_backwards() {
    let clock = LiveClock::new();
    let first = clock.now().await;
    let second = clock.now().await;
    assert!(second >= first, "{} went backwards to {}", first, second);
}
