//! `POST /bars` batch validation.

use crate::{check_ingest_batch, BarsIngestRequest, ErrorCode, MAX_INGEST_BARS};

fn reason(len: usize) -> Option<String> {
    check_ingest_batch(len).err().map(|d| {
        assert_eq!(d.code, ErrorCode::ValidationInvalidRequest);
        d.context.unwrap()["reason"].as_str().unwrap().to_owned()
    })
}

#[test]
fn a_batch_must_be_non_empty_and_bounded() {
    assert_eq!(reason(0).as_deref(), Some("bar_batch_empty"));
    assert_eq!(reason(1), None);
    assert_eq!(reason(MAX_INGEST_BARS), None);
    assert_eq!(
        reason(MAX_INGEST_BARS + 1).as_deref(),
        Some("bar_batch_too_large")
    );
}

#[test]
fn the_request_rejects_unknown_fields_and_invalid_bars() {
    assert!(serde_json::from_value::<BarsIngestRequest>(
        serde_json::json!({"bars": [], "extra": 1})
    )
    .is_err());
    assert!(
        serde_json::from_value::<BarsIngestRequest>(serde_json::json!({"bars": [{}]})).is_err()
    );
}
