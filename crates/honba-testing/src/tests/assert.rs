//! Unit tests for `crate::assert`.

use crate::{assert_close, assert_close_slice};

#[test]
fn assert_close_accepts_differences_up_to_eps() {
    assert_close(1.0, 1.0, 0.0);
    assert_close(1.0, 1.25, 0.25);
    assert_close(-2.0, -2.0 + 1e-10, 1e-9);
}

#[test]
#[should_panic(expected = "assert_close failed")]
fn assert_close_panics_beyond_eps() {
    assert_close(1.0, 1.5, 0.25);
}

#[test]
#[should_panic(expected = "assert_close failed")]
fn assert_close_treats_nan_as_never_close() {
    assert_close(f64::NAN, f64::NAN, 1.0);
}

#[test]
fn assert_close_slice_accepts_elementwise_matches() {
    assert_close_slice(&[], &[], 0.0);
    assert_close_slice(&[1.0, 2.0], &[1.0 + 1e-12, 2.0], 1e-9);
}

#[test]
#[should_panic(expected = "slice lengths differ")]
fn assert_close_slice_panics_on_length_mismatch() {
    assert_close_slice(&[1.0], &[1.0, 2.0], 0.0);
}

#[test]
#[should_panic(expected = "at index 1")]
fn assert_close_slice_reports_the_first_bad_index() {
    assert_close_slice(&[1.0, 2.0, 3.0], &[1.0, 9.0, 9.0], 0.1);
}
