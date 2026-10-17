//! Unit tests for `crate::clock`.

use honba_messages::UnixNanos;

use crate::{AlgoError, Clock};

fn ts(n: u64) -> UnixNanos {
    UnixNanos::from_u64(n)
}

#[test]
fn advance_forward_ok() {
    let mut c = Clock::default();
    c.advance_to(ts(100)).unwrap();
    assert_eq!(c.now(), ts(100));
}

#[test]
fn advance_backward_errors() {
    let mut c = Clock::new(ts(100));
    match c.advance_to(ts(50)) {
        Err(AlgoError::ClockRegression { current, requested }) => {
            assert_eq!(current, 100);
            assert_eq!(requested, 50);
        }
        other => panic!("expected ClockRegression, got {other:?}"),
    }
}

#[test]
fn advance_to_same_time_ok() {
    let mut c = Clock::new(ts(100));
    c.advance_to(ts(100)).unwrap();
}
