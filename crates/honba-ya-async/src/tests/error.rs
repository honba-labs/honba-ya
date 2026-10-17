//! Unit tests for `crate::error`.

use honba_engine::AlgoError;
use honba_ports::PortError;

use crate::AsyncError;

#[test]
fn kernel_display_is_the_kernel_error_verbatim() {
    let err = AsyncError::from(AlgoError::ClockRegression {
        current: 10,
        requested: 5,
    });
    assert_eq!(
        err.to_string(),
        "clock cannot go backwards: current=10, requested=5"
    );
}

#[test]
fn feed_display_is_the_port_error_verbatim() {
    let err = AsyncError::from(PortError::Transport("socket closed".to_string()));
    assert_eq!(err.to_string(), "transport error: socket closed");
}

#[test]
fn closed_says_the_task_has_stopped() {
    assert_eq!(
        AsyncError::Closed.to_string(),
        "the engine task has stopped"
    );
}

#[test]
fn panicked_says_the_task_panicked() {
    assert_eq!(AsyncError::Panicked.to_string(), "the engine task panicked");
}

#[test]
fn rejected_quotes_the_reason() {
    let err = AsyncError::Rejected("trading state machine is full".to_string());
    assert_eq!(
        err.to_string(),
        "command rejected: trading state machine is full"
    );
}

#[test]
fn from_algo_error_wraps_the_kernel_failure_without_retyping_it() {
    let err = AsyncError::from(AlgoError::Component("handler exploded".to_string()));
    assert_eq!(
        err,
        AsyncError::Kernel(AlgoError::Component("handler exploded".to_string()))
    );
}

#[test]
fn from_port_error_wraps_the_feed_failure_without_retyping_it() {
    let port = PortError::Timeout;
    let err = AsyncError::from(port);
    assert_eq!(err, AsyncError::Feed(PortError::Timeout));
}

#[test]
fn every_variant_compares_by_value() {
    assert_eq!(AsyncError::Closed, AsyncError::Closed);
    assert_eq!(AsyncError::Panicked, AsyncError::Panicked);
    assert_ne!(AsyncError::Closed, AsyncError::Panicked);
    assert_ne!(AsyncError::Closed, AsyncError::Rejected("x".to_string()));
    assert_ne!(
        AsyncError::Rejected("a".to_string()),
        AsyncError::Rejected("b".to_string())
    );
    assert_ne!(
        AsyncError::Kernel(AlgoError::DataFeedExhausted),
        AsyncError::Kernel(AlgoError::Component("x".to_string()))
    );
    assert_ne!(
        AsyncError::Feed(PortError::Timeout),
        AsyncError::Feed(PortError::Transport("x".to_string()))
    );
}

#[test]
fn a_port_error_converts_through_from_rather_than_being_stringified() {
    let err: AsyncError = PortError::Unavailable("session expired".to_string()).into();
    assert_eq!(
        err,
        AsyncError::Feed(PortError::Unavailable("session expired".to_string()))
    );
    assert_eq!(
        err.to_string(),
        "port unavailable: session expired",
        "the port message must reach the edge verbatim"
    );
}
