//! Unit tests for the run event stream frame and its resume DTO (E11-S4, ADR 0021).

use honba_messages::{
    ErrorCode, ErrorDetail, Event, Exchange, InstrumentId, Message, QuoteTick, UnixNanos,
    SCHEMA_VERSION,
};
use serde_json::json;

use crate::{BacktestEventsQuery, RunStatus, StreamFrame};

fn quote_message(n: u64) -> Message {
    let quote = QuoteTick::new(
        InstrumentId::new("TCS", Exchange::new("NSE")),
        100.0,
        101.0,
        10.0,
        20.0,
        UnixNanos::from_u64(n),
        UnixNanos::from_u64(n),
    );
    Message::new(Event::Quote(quote), UnixNanos::from_u64(n))
}

#[test]
fn status_frame_serializes_with_kind_tag_and_snake_case() {
    let frame = StreamFrame::Status {
        status: RunStatus::Running,
        seq: 4,
        error: None,
    };
    let value = serde_json::to_value(&frame).unwrap();
    assert_eq!(
        value,
        json!({"kind": "status", "status": "running", "seq": 4})
    );
    let back: StreamFrame = serde_json::from_value(value).unwrap();
    assert_eq!(
        back,
        StreamFrame::Status {
            status: RunStatus::Running,
            seq: 4,
            error: None
        }
    );
}

#[test]
fn status_frame_error_is_present_only_when_failed() {
    let with_error = StreamFrame::Status {
        status: RunStatus::Failed,
        seq: 9,
        error: Some(ErrorDetail {
            code: ErrorCode::InternalError,
            message: "boom".to_string(),
            retryable: false,
            context: Some(json!({"reason": "panic"})),
        }),
    };
    let value = serde_json::to_value(&with_error).unwrap();
    assert_eq!(value["error"]["code"], ErrorCode::InternalError.as_str());

    let without_error = StreamFrame::Status {
        status: RunStatus::Failed,
        seq: 9,
        error: None,
    };
    let value = serde_json::to_value(&without_error).unwrap();
    assert!(value.get("error").is_none());
}

#[test]
fn record_frame_carries_the_journal_message_verbatim() {
    let message = quote_message(3);
    let frame = StreamFrame::Record {
        seq: 3,
        message: message.clone(),
    };
    let value = serde_json::to_value(&frame).unwrap();
    assert_eq!(value["kind"], "record");
    assert_eq!(value["seq"], 3);
    let back: StreamFrame = serde_json::from_value(value).unwrap();
    match back {
        StreamFrame::Record { seq, message: m } => {
            assert_eq!(seq, 3);
            assert_eq!(m, message);
        }
        other => panic!("expected a record frame, got {other:?}"),
    }
}

#[test]
fn a_round_tripped_frame_carries_the_wire_schema_version() {
    let message = quote_message(0);
    assert_eq!(message.schema_version(), SCHEMA_VERSION);
    let frame = StreamFrame::Record { seq: 0, message };
    let text = serde_json::to_string(&frame).unwrap();
    let back: StreamFrame = serde_json::from_str(&text).unwrap();
    assert!(matches!(back, StreamFrame::Record { seq: 0, .. }));
}

#[test]
fn an_unknown_kind_is_a_decode_error() {
    // Strict decode (ADR 0012 rule 3): an unknown kind is not silently ignored.
    let err = serde_json::from_value::<StreamFrame>(json!({"kind": "bogus", "seq": 1}));
    assert!(err.is_err());
}

#[test]
fn events_query_defaults_to_none_and_parses() {
    let query: BacktestEventsQuery = serde_json::from_value(json!({})).unwrap();
    assert_eq!(query.resume_from_seq, None);
    let query: BacktestEventsQuery = serde_json::from_value(json!({"resume_from_seq": 7})).unwrap();
    assert_eq!(query.resume_from_seq, Some(7));
}

#[test]
fn events_query_rejects_unknown_fields() {
    let err = serde_json::from_value::<BacktestEventsQuery>(json!({"resume": 1}));
    assert!(err.is_err());
}
