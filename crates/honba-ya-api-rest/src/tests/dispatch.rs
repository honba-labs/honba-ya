//! Unit tests for `crate::dispatch`: target building and argument rejection.

use crate::dispatch::{build_target, DispatchError};

#[test]
fn a_target_without_a_query_is_the_path() {
    assert_eq!(build_target("/health", None).unwrap(), "/health");
    assert_eq!(build_target("/health", Some("{}")).unwrap(), "/health");
    assert_eq!(build_target("/health", Some("null")).unwrap(), "/health");
}

#[test]
fn a_query_object_is_form_encoded_and_scalars_are_stringified() {
    let target = build_target(
        "/bars/TCS.NSE",
        Some(r#"{"from": "2024-01-01T00:00:00Z", "n": 5, "flag": true, "skip": null}"#),
    )
    .unwrap();
    let (path, query) = target.split_once('?').unwrap();
    assert_eq!(path, "/bars/TCS.NSE");
    let mut parts: Vec<&str> = query.split('&').collect();
    parts.sort_unstable();
    assert_eq!(parts, ["flag=true", "from=2024-01-01T00%3A00%3A00Z", "n=5"]);
}

#[test]
fn a_query_that_is_not_a_flat_object_is_rejected() {
    for bad in [
        "[]",
        "1",
        r#"{"a": [1]}"#,
        r#"{"a": {"b": 1}}"#,
        "{not json",
    ] {
        assert!(
            matches!(
                build_target("/x", Some(bad)),
                Err(DispatchError::InvalidQuery(_))
            ),
            "{bad}"
        );
    }
}

#[test]
fn a_path_must_be_absolute() {
    assert!(matches!(
        build_target("health", None),
        Err(DispatchError::InvalidTarget(_))
    ));
}
