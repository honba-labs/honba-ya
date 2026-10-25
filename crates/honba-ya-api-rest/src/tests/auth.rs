use crate::auth::{AuthLayer, sha256_hex};

#[test]
fn test_sha256_hex() {
    let hash = sha256_hex("test");
    assert_eq!(hash.len(), 64);
}

#[test]
fn test_required_scope() {
    assert_eq!(AuthLayer::required_scope("GET", "/capabilities"), None);
    assert_eq!(
        AuthLayer::required_scope("GET", "/instruments"),
        Some("read")
    );
    assert_eq!(
        AuthLayer::required_scope("GET", "/bars/ABC"),
        Some("read")
    );
    assert_eq!(
        AuthLayer::required_scope("POST", "/orders"),
        Some("trade")
    );
    assert_eq!(
        AuthLayer::required_scope("PUT", "/trading-state"),
        Some("admin")
    );
    assert_eq!(AuthLayer::required_scope("GET", "/unknown"), None);
}
