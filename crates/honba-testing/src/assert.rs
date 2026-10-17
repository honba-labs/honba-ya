//! Assertion helpers for numeric tests.

/// Asserts that two `f64` values are within `eps` of each other.
///
/// ```
/// use honba_testing::assert_close;
///
/// assert_close(0.1 + 0.2, 0.3, 1e-12);
/// ```
///
/// # Panics
///
/// Panics if the difference exceeds `eps`.
pub fn assert_close(a: f64, b: f64, eps: f64) {
    let diff = (a - b).abs();
    assert!(
        diff <= eps,
        "assert_close failed: |{a} - {b}| = {diff} > {eps}"
    );
}

/// Asserts that two sequences are element-wise close within `eps`.
///
/// # Panics
///
/// Panics if the lengths differ or any element pair exceeds `eps`.
pub fn assert_close_slice(a: &[f64], b: &[f64], eps: f64) {
    assert_eq!(a.len(), b.len(), "slice lengths differ");
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        let diff = (x - y).abs();
        assert!(
            diff <= eps,
            "assert_close_slice failed at index {i}: |{x} - {y}| = {diff} > {eps}"
        );
    }
}
