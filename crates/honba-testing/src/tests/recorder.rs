//! Unit tests for `crate::recorder`.

use honba_engine::Handler;
use honba_messages::UnixNanos;

use crate::{Recorder, VecFeed};

#[test]
fn new_recorder_is_empty_and_idle() {
    let r = Recorder::new();
    assert!(r.is_empty());
    assert_eq!(r.len(), 0);
    assert!(!r.started() && !r.stopped());
}

#[test]
fn records_lifecycle_and_events_in_arrival_order() {
    let mut r = Recorder::new();
    r.on_start().unwrap();
    for ts in [3, 1, 2] {
        let msg = VecFeed::bar("X", 1.0, ts);
        r.on_event(msg.event(), UnixNanos::from_u64(ts)).unwrap();
    }
    r.on_stop().unwrap();
    assert!(r.started() && r.stopped());
    assert_eq!(r.len(), 3);
    assert_eq!(r.timestamps(), [3, 1, 2]);
    assert_eq!(r.events()[0], VecFeed::bar("X", 1.0, 3).event().clone());
}

#[test]
fn clones_share_the_same_log() {
    let observer = Recorder::new();
    let mut handler = observer.clone();
    handler
        .on_event(VecFeed::bar("X", 1.0, 1).event(), UnixNanos::from_u64(1))
        .unwrap();
    assert_eq!(observer.len(), 1);
}
