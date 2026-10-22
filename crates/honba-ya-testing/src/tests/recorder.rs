//! Unit tests for `crate::recorder`.

use crate::ports::MarketDataFeed;
use crate::{Handler, Recorder, VecFeed};

#[test]
fn new_recorder_is_empty_and_idle() {
    let r = Recorder::new();
    assert!(r.is_empty());
    assert_eq!(r.len(), 0);
    assert!(!r.started() && !r.stopped());
}

#[tokio::test]
async fn records_lifecycle_and_events_in_arrival_order() {
    let mut r = Recorder::new();
    r.on_start().unwrap();
    for ts in [3, 1, 2] {
        let item = VecFeed::bar("X", 1.0, ts);
        let at = item.timestamp();
        r.on_event(&item, at).unwrap();
    }
    r.on_stop().unwrap();
    assert!(r.started() && r.stopped());
    assert_eq!(r.len(), 3);
    assert_eq!(r.timestamps(), [3, 1, 2]);
    assert_eq!(r.events()[0], VecFeed::bar("X", 1.0, 3));
}

#[tokio::test]
async fn clones_share_the_same_log() {
    let observer = Recorder::new();
    let mut handler = observer.clone();
    let item = VecFeed::bar("X", 1.0, 1);
    let at = item.timestamp();
    handler.on_event(&item, at).unwrap();
    assert_eq!(observer.len(), 1);
}
