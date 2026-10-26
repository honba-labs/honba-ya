use crate::auth::{AuthLayer, JwtClaims, JwtConfig, JwtError, sha256_hex, validate_jwt};
use chrono::Utc;
use jsonwebtoken::{EncodingKey, Header};

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
    assert_eq!(AuthLayer::required_scope("GET", "/bars/ABC"), Some("read"));
    assert_eq!(AuthLayer::required_scope("POST", "/orders"), Some("trade"));
    assert_eq!(
        AuthLayer::required_scope("PUT", "/trading-state"),
        Some("admin")
    );
    assert_eq!(AuthLayer::required_scope("GET", "/unknown"), None);
}

#[test]
fn test_jwt_hmac_sha256_valid_and_rbac_scopes() {
    let secret = b"super-secret-hmac-key-for-testing-only-32bytes!";
    let now = Utc::now().timestamp() as usize;

    let claims = JwtClaims {
        sub: "trader-42".to_string(),
        scopes: vec!["read".to_string(), "trade".to_string()],
        iss: "honba".to_string(),
        aud: "honba-api".to_string(),
        exp: now + 3600,
        iat: now - 10,
        jti: "jwt-id-123".to_string(),
    };

    let token = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let config = JwtConfig {
        hmac_secret: Some(secret.to_vec()),
        rsa_public_key: None,
        issuer: "honba".to_string(),
        audience: "honba-api".to_string(),
    };

    let info = validate_jwt(&token, &config).expect("valid jwt should succeed");
    assert_eq!(info.token_id, "trader-42");
    assert!(info.scopes.contains(&"read".to_string()));
    assert!(info.scopes.contains(&"trade".to_string()));
    assert!(!info.scopes.contains(&"admin".to_string()));
}

#[test]
fn test_jwt_hmac_sha256_expired() {
    let secret = b"super-secret-hmac-key-for-testing-only-32bytes!";
    let now = Utc::now().timestamp() as usize;

    let claims = JwtClaims {
        sub: "trader-expired".to_string(),
        scopes: vec!["read".to_string()],
        iss: "honba".to_string(),
        aud: "honba-api".to_string(),
        exp: now - 10, // Expired
        iat: now - 3600,
        jti: "jwt-id-expired".to_string(),
    };

    let token = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let config = JwtConfig {
        hmac_secret: Some(secret.to_vec()),
        rsa_public_key: None,
        issuer: "honba".to_string(),
        audience: "honba-api".to_string(),
    };

    let err = validate_jwt(&token, &config).expect_err("expired token should fail");
    assert!(matches!(err, JwtError::Expired));
}

#[test]
fn test_jwt_hmac_sha256_invalid_issuer_and_audience() {
    let secret = b"super-secret-hmac-key-for-testing-only-32bytes!";
    let now = Utc::now().timestamp() as usize;

    let claims = JwtClaims {
        sub: "trader-attacker".to_string(),
        scopes: vec!["admin".to_string()],
        iss: "untrusted-issuer".to_string(),
        aud: "honba-api".to_string(),
        exp: now + 3600,
        iat: now,
        jti: "jwt-attacker".to_string(),
    };

    let token = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let config = JwtConfig {
        hmac_secret: Some(secret.to_vec()),
        rsa_public_key: None,
        issuer: "honba".to_string(),
        audience: "honba-api".to_string(),
    };

    let err = validate_jwt(&token, &config).expect_err("wrong issuer should fail");
    assert!(matches!(err, JwtError::InvalidToken));
}

#[test]
fn test_jwt_admin_rbac_scope() {
    let secret = b"super-secret-hmac-key-for-testing-only-32bytes!";
    let now = Utc::now().timestamp() as usize;

    let claims = JwtClaims {
        sub: "admin-user".to_string(),
        scopes: vec!["admin".to_string()],
        iss: "honba".to_string(),
        aud: "honba-api".to_string(),
        exp: now + 3600,
        iat: now,
        jti: "jwt-admin-1".to_string(),
    };

    let token = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .unwrap();

    let config = JwtConfig {
        hmac_secret: Some(secret.to_vec()),
        rsa_public_key: None,
        issuer: "honba".to_string(),
        audience: "honba-api".to_string(),
    };

    let info = validate_jwt(&token, &config).expect("admin token valid");
    assert_eq!(info.token_id, "admin-user");
    assert_eq!(info.scopes, vec!["admin"]);
}
