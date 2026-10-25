use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use honba_api::{ApiResponse, ErrorCode, ErrorDetail, ResponseEnvelope};
use serde::{Deserialize, Serialize};
use tracing::info;
use chrono::Utc;

/// Hex-encoded SHA-256 digest of `s`.
///
/// `honba-ya-persistence::decisions::sha256_hex` has the same helper, but this
/// crate doesn't depend on `honba-ya-persistence`, so it's kept local here on
/// top of the `sha2`/`hex` deps this crate already has.
pub(crate) fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

/// Token info returned by successful validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    /// Unique identifier for the token (first 8 chars of SHA-256)
    pub token_id: String,
    /// Scopes granted to this token (e.g., "read:market", "write:orders", "research", "operator")
    pub scopes: Vec<String>,
}

/// Auth error codes (matching Python AuthErrorCode)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthErrorCode {
    MissingToken,
    InvalidToken,
    Expired,
    Forbidden,
}

impl std::fmt::Display for AuthErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthErrorCode::MissingToken => write!(f, "missing_token"),
            AuthErrorCode::InvalidToken => write!(f, "invalid_token"),
            AuthErrorCode::Expired => write!(f, "expired"),
            AuthErrorCode::Forbidden => write!(f, "forbidden"),
        }
    }
}

/// Auth configuration
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Path to tokens.toml file; when Some, Bearer token auth is enabled
    pub tokens_file: Option<PathBuf>,
    /// Path to audit log file; when Some, auth decisions are logged here
    pub audit_log_path: Option<PathBuf>,
    /// JWT configuration; when Some, JWT bearer tokens are validated
    pub jwt_config: Option<JwtConfig>,
}

/// Token record as stored in the token file
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct TokenRecord {
    token_id: String,
    token_hash: String, // "sha256:<hex>"
    scopes: Vec<String>,
    label: String,
    expires_at: Option<String>,
    created_at: String,
}

impl TokenRecord {
    fn matches_token(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let computed = sha256_hex(token);
        let expected = self
            .token_hash
            .strip_prefix("sha256:")
            .unwrap_or(&self.token_hash);
        computed == expected
    }

    fn is_expired(&self) -> bool {
        if let Some(expires_at) = &self.expires_at {
            if let Ok(expiry) = chrono::DateTime::parse_from_rfc3339(expires_at) {
                return chrono::Utc::now() >= expiry;
            }
            true
        } else {
            false
        }
    }
}

/// JWT (RFC 7518) claims for Honba API token authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (token identifier)
    pub sub: String,
    /// Scopes granted to this token (e.g., "read", "trade", "admin")
    pub scopes: Vec<String>,
    /// Issuer identifier
    pub iss: String,
    /// Audience identifier
    pub aud: String,
    /// Expiration time (UNIX timestamp)
    pub exp: usize,
    /// Issued-at time (UNIX timestamp)
    pub iat: usize,
    /// JWT ID for tracking
    pub jti: String,
}

/// JWT authentication configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC secret key for token signing/verification
    pub hmac_secret: Option<Vec<u8>>,
    /// RSA public key for token verification (PEM-encoded)
    pub rsa_public_key: Option<String>,
    /// Token issuer
    pub issuer: String,
    /// Token audience
    pub audience: String,
}

/// JWT authentication error codes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JwtError {
    MissingToken,
    InvalidToken,
    Expired,
    InvalidSignature,
    MissingHmacSecret,
    MissingRsaKey,
    UnknownAlgorithm,
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JwtError::MissingToken => write!(f, "missing_token"),
            JwtError::InvalidToken => write!(f, "invalid_token"),
            JwtError::Expired => write!(f, "expired"),
            JwtError::InvalidSignature => write!(f, "invalid_signature"),
            JwtError::MissingHmacSecret => write!(f, "missing_hmac_secret"),
            JwtError::MissingRsaKey => write!(f, "missing_rsa_key"),
            JwtError::UnknownAlgorithm => write!(f, "unknown_algorithm"),
        }
    }
}

impl std::error::Error for JwtError {}

/// File-backed token store
#[derive(Debug, Clone)]
pub struct FileTokenStore {
    path: PathBuf,
    records: Arc<RwLock<Vec<TokenRecord>>>,
    last_mtime: Arc<RwLock<std::time::SystemTime>>,
}

impl FileTokenStore {
    /// Creates a new file-backed token store and loads the initial token file.
    pub fn new(path: PathBuf) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let store = Self {
            path,
            records: Arc::new(RwLock::new(Vec::new())),
            last_mtime: Arc::new(RwLock::new(std::time::SystemTime::UNIX_EPOCH)),
        };
        store.reload()?;
        Ok(store)
    }

    fn reload(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mtime = std::fs::metadata(&self.path)?.modified()?;
        {
            let mut last = self.last_mtime.write().unwrap();
            if *last == mtime {
                return Ok(());
            }
            *last = mtime;
        }

        let content = std::fs::read_to_string(&self.path)?;
        let data: HashMap<String, Vec<TokenRecord>> = toml::from_str(&content)?;
        let records = data.get("tokens").cloned().unwrap_or_default();
        info!(
            "Loaded {} token(s) from {}",
            records.len(),
            self.path.display()
        );
        *self.records.write().unwrap() = records;
        Ok(())
    }

    /// Validates a plain token and returns its info.
    ///
    /// Returns `Ok(TokenInfo)` on success, or an `AuthErrorCode` on failure
    /// (missing, invalid, or expired token).
    pub fn validate(&self, token: &str) -> Result<TokenInfo, AuthErrorCode> {
        self.reload().ok(); // Best effort

        if token.is_empty() {
            return Err(AuthErrorCode::MissingToken);
        }

        let records = self.records.read().unwrap();
        for record in records.iter() {
            if record.matches_token(token) {
                if record.is_expired() {
                    return Err(AuthErrorCode::Expired);
                }
                return Ok(TokenInfo {
                    token_id: record.token_id.clone(),
                    scopes: record.scopes.clone(),
                });
            }
        }
        Err(AuthErrorCode::InvalidToken)
    }
}

/// The scope a route requires, or that it is public (E5-S3, ADR 0023 §2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteScope {
    /// No token is required; the route is health or metadata.
    Public,
    /// The caller's token must carry this scope.
    Required(&'static str),
    /// Any valid token is enough; no particular scope. The handler acts for the token's own
    /// `token_id` (the `/profile` routes: a token manages only its own profile).
    Authenticated,
}

/// The canonical route-to-scope map (ADR 0023 §2).
///
/// Every route the router serves has exactly one row, so scope enforcement is a
/// lookup rather than a set of prefix rules that can silently miss a new route.
/// Paths use the `{param}` spelling of [`honba_api::ENDPOINTS`], so the drift
/// guard can compare the two by key. The five scopes are `read:market`,
/// `read:account`, `write:orders`, `research` and `operator`; `read:account` governs the
/// per-user portfolio reads (ADR 0032). Scopes are explicit: none implies another.
/// RBAC scopes (Phase 6.2): coarse-grained roles replacing the previous fine-grained
/// `read:market`/`write:orders`/`research`/`operator`/`read:account` hierarchy.
///
/// - `read`: general read access (market data, quotes, instruments, depth, bars, portfolio)
/// - `trade`: general trade access (place, modify, cancel orders, close positions)
/// - `admin`: administrative access (operator routes, approval queue, system config)
///
/// Legacy fine-grained scopes (`read:market`, `write:orders`, etc.) are still supported
/// for backward compatibility but new routes should use these RBAC roles.
pub const ROUTE_SCOPES: &[(&str, &str, RouteScope)] = &[
    // Public routes (no auth required)
    ("GET", "/capabilities", RouteScope::Public),
    ("GET", "/health", RouteScope::Public),
    ("GET", "/schema", RouteScope::Public),

    // Read routes: `read` RBAC scope
    ("GET", "/instruments", RouteScope::Required("read")),
    ("GET", "/instruments/{id}", RouteScope::Required("read")),
    ("GET", "/quotes", RouteScope::Required("read")),
    ("GET", "/bars/{id}", RouteScope::Required("read")),
    ("GET", "/depth/{id}", RouteScope::Required("read")),

    // Write/Trade routes: `trade` RBAC scope
    ("POST", "/bars", RouteScope::Required("trade")),
    ("POST", "/orders", RouteScope::Required("trade")),
    ("DELETE", "/orders/{id}", RouteScope::Required("trade")),
    ("POST", "/positions/close", RouteScope::Required("trade")),

    // Operator / Admin routes: `admin` RBAC scope
    ("GET", "/trading-state", RouteScope::Required("admin")),
    ("PUT", "/trading-state", RouteScope::Required("admin")),
    ("GET", "/screener/scan", RouteScope::Required("read")),
    // Profile routes: any valid token, scoped to its own `token_id` by the handlers.
    ("GET", "/profile", RouteScope::Authenticated),
    ("PUT", "/profile", RouteScope::Authenticated),
    ("GET", "/profile/layouts", RouteScope::Authenticated),
    ("PUT", "/profile/layouts/{name}", RouteScope::Authenticated),
    (
        "DELETE",
        "/profile/layouts/{name}",
        RouteScope::Authenticated,
    ),
    ("GET", "/profile/modules", RouteScope::Authenticated),
    ("PUT", "/profile/modules/{id}", RouteScope::Authenticated),
    ("DELETE", "/profile/modules/{id}", RouteScope::Authenticated),
    // Portfolio routes: `read` scope for read-only portfolio access (ADR 0032 §1, §6)
    ("GET", "/portfolio", RouteScope::Required("read")),
    (
        "GET",
        "/portfolio/accounts",
        RouteScope::Required("read"),
    ),
    (
        "GET",
        "/portfolio/positions",
        RouteScope::Required("read"),
    ),
    (
        "GET",
        "/portfolio/holdings",
        RouteScope::Required("read"),
    ),
    (
        "GET",
        "/portfolio/trades",
        RouteScope::Required("read"),
    ),

    // Research routes: still use `research` scope for backward compatibility
    ("GET", "/strategies", RouteScope::Required("research")),
    ("POST", "/strategies", RouteScope::Required("research")),
    (
        "POST",
        "/strategies/verify",
        RouteScope::Required("research"),
    ),
    ("POST", "/backtests", RouteScope::Required("research")),
    ("GET", "/backtests/{id}", RouteScope::Required("research")),
    (
        "GET",
        "/backtests/{id}/journal",
        RouteScope::Required("research"),
    ),
    (
        "GET",
        "/backtests/{id}/events",
        RouteScope::Required("research"),
    ),
    ("POST", "/sweeps", RouteScope::Required("research")),
    ("GET", "/sweeps/{id}", RouteScope::Required("research")),
    (
        "GET",
        "/sweeps/{id}/journal",
        RouteScope::Required("research"),
    ),
    ("GET", "/journals/{id}", RouteScope::Required("research")),

    // Approval queue routes: `admin` scope
    ("GET", "/approval/queue", RouteScope::Required("admin")),
    ("POST", "/approval/approve", RouteScope::Required("admin")),
    ("POST", "/approval/reject", RouteScope::Required("admin")),

    // WebSocket routes: public
    ("GET", "/ws", RouteScope::Public),
    ("GET", "/ws/market", RouteScope::Public),

    // Additional research / run routes
    ("GET", "/backtests", RouteScope::Required("research")),
    ("GET", "/sweeps", RouteScope::Required("research")),

    // Additional orders & positions & portfolio routes
    ("GET", "/orders", RouteScope::Required("trade")),
    ("GET", "/orders/{id}/fills", RouteScope::Required("read")),
    ("GET", "/positions", RouteScope::Required("read")),
    ("GET", "/positions/audit", RouteScope::Required("read")),
    ("GET", "/trades", RouteScope::Required("read")),
    ("GET", "/portfolio/chart", RouteScope::Required("read")),
    ("GET", "/portfolio/concentration", RouteScope::Required("read")),
    ("GET", "/portfolio/wac", RouteScope::Required("read")),
    ("GET", "/ledger", RouteScope::Required("read")),
    ("GET", "/tax/report", RouteScope::Required("read")),
    ("GET", "/fees", RouteScope::Required("read")),

    // Additional reports routes
    ("GET", "/reports/ledger", RouteScope::Required("read")),
    ("GET", "/reports/tax", RouteScope::Required("read")),
    ("GET", "/reports/snapshots", RouteScope::Required("read")),
    ("GET", "/reports/audit", RouteScope::Required("read")),
    ("GET", "/reports/concentration", RouteScope::Required("read")),
    ("GET", "/reports/wac", RouteScope::Required("read")),
    ("GET", "/reports/fees", RouteScope::Required("read")),
    ("GET", "/reports/equity-curve", RouteScope::Required("read")),
    ("GET", "/reports/fills/{order_id}", RouteScope::Required("read")),

    // Documentation / OpenAPI routes
    ("GET", "/schema/openapi/v1.json", RouteScope::Public),
    ("GET", "/schema/openapi/v2.json", RouteScope::Public),
    ("GET", "/schema/openapi", RouteScope::Public),
    ("GET", "/docs", RouteScope::Public),
    ("GET", "/swagger-ui", RouteScope::Public),
];

/// The scope required for a concrete request, or `None` when the route is not in
/// [`ROUTE_SCOPES`].
///
/// `None` means the request matched no row: the router answers 404/405 for it,
/// but the drift guard (`unscoped_routes`) forbids a registered endpoint from
/// ever landing here.
///
/// `HEAD` is looked up as `GET`: the router answers it from the `GET` handler, so it needs the
/// same token and scope.
pub fn scope_for(method: &str, path: &str) -> Option<RouteScope> {
    let method = if method.eq_ignore_ascii_case("HEAD") {
        "GET"
    } else {
        method
    };
    let unversioned_path = path
        .strip_prefix("/api/v1")
        .or_else(|| path.strip_prefix("/api/v2"))
        .unwrap_or(path);
    ROUTE_SCOPES
        .iter()
        .find(|(m, pattern, _)| m.eq_ignore_ascii_case(method) && path_matches(pattern, unversioned_path))
        .map(|(_, _, scope)| *scope)
}

/// Whether concrete request `path` matches a route `pattern` whose `{param}`
/// segments are single-segment wildcards.
fn path_matches(pattern: &str, path: &str) -> bool {
    let mut want = pattern.split('/');
    let mut got = path.split('/');
    loop {
        match (want.next(), got.next()) {
            (None, None) => return true,
            (Some(pat), Some(seg)) => {
                let wildcard = pat.starts_with('{') && pat.ends_with('}');
                if wildcard {
                    if seg.is_empty() {
                        return false;
                    }
                } else if pat != seg {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// The routes in `routes` (given as `(METHOD, path)` pairs) with no row in
/// [`ROUTE_SCOPES`].
///
/// The drift guard asserts this is empty for [`honba_api::ENDPOINTS`] and that a
/// route added without a scope makes it non-empty, so a new endpoint cannot ship
/// as an accidental public route.
pub fn unscoped_routes(routes: &[(&str, &str)]) -> Vec<String> {
    routes
        .iter()
        .filter(|(method, path)| scope_for(method, path).is_none())
        .map(|(method, path)| format!("{method} {path}"))
        .collect()
}

/// The auth layer that wraps a token store
#[derive(Clone)]
pub struct AuthLayer {
    store: FileTokenStore,
    audit_log_path: Option<PathBuf>,
    jwt_config: Option<JwtConfig>,
}

impl AuthLayer {
    /// Creates a new auth layer with the given token store and audit log path.
    pub fn new(store: FileTokenStore, audit_log_path: Option<PathBuf>, jwt_config: Option<JwtConfig>) -> Self {
        Self {
            store,
            audit_log_path,
            jwt_config,
        }
    }

    pub(crate) fn required_scope(method: &str, path: &str) -> Option<&'static str> {
        match scope_for(method, path) {
            Some(RouteScope::Required(scope)) => Some(scope),
            // Public, a route the table does not know (the router answers 404/405 for it, and
            // the drift guard keeps it that way), or one that needs a token but no scope.
            Some(RouteScope::Authenticated) | Some(RouteScope::Public) | None => None,
        }
    }
}

/// Audit log entry for auth decisions
#[derive(Debug)]
struct AuditEntry<'a> {
    token_id: Option<&'a str>,
    route: &'a str,
    method: &'a str,
    decision: &'a str,
    reason: Option<&'a str>,
    required_scope: Option<&'a str>,
    token_scopes: Option<&'a [String]>,
    client_ip: Option<&'a str>,
}

impl AuthLayer {
    fn audit(&self, entry: AuditEntry<'_>) {
        if let Some(audit_path) = &self.audit_log_path {
            let json = serde_json::json!({
                "ts": chrono::Utc::now().to_rfc3339(),
                "event": "auth.decision",
                "token_id": entry.token_id.unwrap_or("unknown"),
                "route": entry.route,
                "method": entry.method,
                "decision": entry.decision,
                "reason": entry.reason,
                "required_scope": entry.required_scope,
                "token_scopes": entry.token_scopes,
                "client_ip": entry.client_ip,
            });
            let line = serde_json::to_string(&json).unwrap();
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(audit_path)
                .and_then(|mut f| {
                    std::io::Write::write_all(&mut f, format!("{}\n", line).as_bytes())
                });
        }
    }

    /// Extract token info from request, enforcing scope.
    /// Returns None if route is public.
    pub async fn extract(
        &self,
        method: &str,
        path: &str,
        headers: &HeaderMap,
        client_ip: Option<&str>,
    ) -> Result<Option<TokenInfo>, (StatusCode, Json<ResponseEnvelope<serde_json::Value>>)> {
        let required = Self::required_scope(method, path);
        let any_token = scope_for(method, path) == Some(RouteScope::Authenticated);
        if required.is_none() && !any_token {
            return Ok(None);
        }

        let auth_header = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(|s| s.trim());

        let token = match auth_header {
            Some(t) if !t.is_empty() => t,
            _ => {
                self.audit(AuditEntry {
                    token_id: None,
                    route: path,
                    method,
                    decision: "denied",
                    reason: Some("missing_token"),
                    required_scope: required,
                    token_scopes: None,
                    client_ip,
                });
                let detail = ErrorDetail::new(
                    ErrorCode::Unauthorized,
                    "Authorization header missing or not Bearer",
                );
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(ApiResponse::error(detail)),
                ));
            }
        };

        // First, try file-based token validation (backward compatible)
        if let Ok(info) = self.store.validate(token) {
            if required.is_some_and(|r| !info.scopes.iter().any(|s| s == r)) {
                let required = required.unwrap_or_default();
                self.audit(AuditEntry {
                    token_id: Some(&info.token_id),
                    route: path,
                    method,
                    decision: "denied",
                    reason: Some("forbidden"),
                    required_scope: Some(required),
                    token_scopes: Some(&info.scopes),
                    client_ip,
                });
                let detail = ErrorDetail::new(
                    ErrorCode::Unauthorized,
                    format!("Token does not have required scope: {}", required),
                )
                .with_context(serde_json::json!({
                    "required_scope": required,
                    "token_scopes": info.scopes,
                    "token_id": info.token_id,
                }));
                return Err((StatusCode::FORBIDDEN, Json(ApiResponse::error(detail))));
            }

            self.audit(AuditEntry {
                token_id: Some(&info.token_id),
                route: path,
                method,
                decision: "granted",
                reason: None,
                required_scope: required,
                token_scopes: Some(&info.scopes),
                client_ip,
            });
            return Ok(Some(info));
        }

        // Fall back to JWT validation if file token failed and JWT config is available
        if let Some(config) = &self.jwt_config {
            match validate_jwt(token, config) {
                Ok(info) => {
                    if required.is_some_and(|r| !info.scopes.iter().any(|s| s == r)) {
                        let required = required.unwrap_or_default();
                        self.audit(AuditEntry {
                            token_id: Some(&info.token_id),
                            route: path,
                            method,
                            decision: "denied",
                            reason: Some("forbidden"),
                            required_scope: Some(required),
                            token_scopes: Some(&info.scopes),
                            client_ip,
                        });
                        let detail = ErrorDetail::new(
                            ErrorCode::Unauthorized,
                            format!("Token does not have required scope: {}", required),
                        )
                        .with_context(serde_json::json!({
                            "required_scope": required,
                            "token_scopes": info.scopes,
                            "token_id": info.token_id,
                        }));
                        return Err((StatusCode::FORBIDDEN, Json(ApiResponse::error(detail))));
                    }

                    self.audit(AuditEntry {
                        token_id: Some(&info.token_id),
                        route: path,
                        method,
                        decision: "granted",
                        reason: None,
                        required_scope: required,
                        token_scopes: Some(&info.scopes),
                        client_ip,
                    });
                    return Ok(Some(info));
                }
                Err(code) => {
                    let (reason, status) = match code {
                        JwtError::MissingToken => ("missing_token", StatusCode::UNAUTHORIZED),
                        JwtError::InvalidToken => ("invalid_token", StatusCode::UNAUTHORIZED),
                        JwtError::Expired => ("expired", StatusCode::UNAUTHORIZED),
                        JwtError::InvalidSignature => ("invalid_signature", StatusCode::UNAUTHORIZED),
                        JwtError::MissingHmacSecret => {
                            ("missing_hmac_secret", StatusCode::INTERNAL_SERVER_ERROR)
                        }
                        JwtError::MissingRsaKey => {
                            ("missing_rsa_key", StatusCode::INTERNAL_SERVER_ERROR)
                        }
                        JwtError::UnknownAlgorithm => {
                            ("unknown_algorithm", StatusCode::INTERNAL_SERVER_ERROR)
                        }
                    };
                    self.audit(AuditEntry {
                        token_id: None,
                        route: path,
                        method,
                        decision: "denied",
                        reason: Some(reason),
                        required_scope: required,
                        token_scopes: None,
                        client_ip,
                    });
                    let detail = ErrorDetail::new(
                        ErrorCode::Unauthorized,
                        format!("Token validation failed: {}", code),
                    );
                    return Err((status, Json(ApiResponse::error(detail))));
                }
            }
        }

        // No JWT config and file token failed
        let detail = ErrorDetail::new(
            ErrorCode::Unauthorized,
            "Authorization header missing or not Bearer",
        );
        Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiResponse::error(detail)),
        ))
    }
}

/// Extract client IP from request
fn client_ip(req: &Request) -> Option<String> {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            req.extensions()
                .get::<std::net::SocketAddr>()
                .map(|addr| addr.ip().to_string())
        })
}

/// Request marker the auth middleware sets on every request it sees, so a handler can tell
/// "auth is off" (no marker) from "auth is on but no token reached me" (marker, no
/// [`TokenInfo`]), which must never run as a default user.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuthEnforced;

/// Axum middleware that enforces auth
pub async fn auth_middleware(
    State(auth_layer): State<Arc<AuthLayer>>,
    mut req: Request,
    next: Next,
) -> Response {
    req.extensions_mut().insert(AuthEnforced);
    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();
    let ip = client_ip(&req);

    // Check auth before proceeding
    match auth_layer
        .extract(&method, &path, &headers, ip.as_deref())
        .await
    {
        Ok(Some(token_info)) => {
            // Insert token info into extensions for handlers that need it
            let mut req = req;
            req.extensions_mut().insert(token_info);
            next.run(req).await
        }
        Ok(None) => {
            // Public route
            next.run(req).await
        }
        Err((status, json)) => (status, json).into_response(),
    }
}

/// Extension trait to add auth to router
pub trait AuthRouterExt {
    fn with_auth(self, auth_layer: Arc<AuthLayer>) -> Self;
}

impl<S: Clone + Send + Sync + 'static> AuthRouterExt for Router<S> {
    fn with_auth(self, auth_layer: Arc<AuthLayer>) -> Self {
        self.layer(axum::middleware::from_fn_with_state(
            auth_layer,
            auth_middleware,
        ))
    }
}

/// Validate a JWT bearer token.
    ///
    /// Supports two signing methods:
    /// - `HS256` with an HMAC secret key
    /// - `RS256` with an RSA public key
    ///
    /// Returns `Ok(TokenInfo)` on success, or `JwtError` on failure.
    pub fn validate_jwt(token: &str, config: &JwtConfig) -> Result<TokenInfo, JwtError> {
        if token.is_empty() {
            return Err(JwtError::MissingToken);
        }

        // Both branches below check `iss`/`aud`/`exp`/`iat` manually against `config`, so the
        // library's own claim validation is disabled here to avoid double (and differently
        // configured) enforcement; `jsonwebtoken` still verifies the signature.
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();

        // Try HS256 first (HMAC)
        if let Some(hmac_secret) = &config.hmac_secret {
            let key = jsonwebtoken::DecodingKey::from_secret(hmac_secret);
            match jsonwebtoken::decode::<JwtClaims>(token, &key, &validation) {
                Ok(data) => {
                    // Verify issuer and audience
                    if data.claims.iss != config.issuer {
                        return Err(JwtError::InvalidToken);
                    }
                    if data.claims.aud != config.audience {
                        return Err(JwtError::InvalidToken);
                    }

                    // Check expiration
                    let now = Utc::now().timestamp() as usize;
                    if now < data.claims.iat || now > data.claims.exp {
                        return Err(JwtError::Expired);
                    }

                    return Ok(TokenInfo {
                        token_id: data.claims.sub.clone(),
                        scopes: data.claims.scopes.clone(),
                    });
                }
                Err(_) => {
                    // Not a valid HS256 token (wrong key, signature, or shape); try RSA.
                }
            }
        }

        // Try RS256 (RSA public key verification)
        if let Some(rsa_pem) = &config.rsa_public_key {
            let key = match jsonwebtoken::DecodingKey::from_rsa_pem(rsa_pem.as_bytes()) {
                Ok(key) => key,
                Err(_) => return Err(JwtError::MissingRsaKey),
            };
            let mut rsa_validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
            rsa_validation.validate_exp = false;
            rsa_validation.validate_aud = false;
            rsa_validation.required_spec_claims.clear();

            match jsonwebtoken::decode::<JwtClaims>(token, &key, &rsa_validation) {
                Ok(data) => {
                    // Verify issuer and audience
                    if data.claims.iss != config.issuer {
                        return Err(JwtError::InvalidToken);
                    }
                    if data.claims.aud != config.audience {
                        return Err(JwtError::InvalidToken);
                    }

                    // Check expiration
                    let now = Utc::now().timestamp() as usize;
                    if now < data.claims.iat || now > data.claims.exp {
                        return Err(JwtError::Expired);
                    }

                    return Ok(TokenInfo {
                        token_id: data.claims.sub.clone(),
                        scopes: data.claims.scopes.clone(),
                    });
                }
                Err(_) => {
                    return Err(JwtError::InvalidSignature);
                }
            }
        }

        // No suitable key configured
        if config.hmac_secret.is_none() && config.rsa_public_key.is_none() {
            return Err(JwtError::MissingHmacSecret);
        }

        Err(JwtError::MissingRsaKey)
    }
