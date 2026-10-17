use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
    Router,
};
use honba_api::ApiResponse;
use honba_messages::{ErrorCode, ErrorDetail, ResponseEnvelope};
use serde::{Deserialize, Serialize};
use tracing::info;
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
pub const ROUTE_SCOPES: &[(&str, &str, RouteScope)] = &[
    ("GET", "/capabilities", RouteScope::Public),
    ("GET", "/health", RouteScope::Public),
    ("GET", "/schema", RouteScope::Public),
    ("GET", "/instruments", RouteScope::Required("read:market")),
    (
        "GET",
        "/instruments/{id}",
        RouteScope::Required("read:market"),
    ),
    ("GET", "/quotes", RouteScope::Required("read:market")),
    ("GET", "/bars/{id}", RouteScope::Required("read:market")),
    ("GET", "/depth/{id}", RouteScope::Required("read:market")),
    ("POST", "/bars", RouteScope::Required("write:orders")),
    // Operator routes: the dedicated `operator` scope (ADR 0023). It implies nothing, and
    // `write:orders` no longer reaches these routes.
    ("GET", "/trading-state", RouteScope::Required("operator")),
    ("PUT", "/trading-state", RouteScope::Required("operator")),
    ("GET", "/screener/scan", RouteScope::Required("read:market")),
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
    // Portfolio routes: the reserved `read:account` scope, finally used (ADR 0032 §1, §6). The
    // handlers scope the portfolio to the token's own `token_id`, like the profile routes.
    ("GET", "/portfolio", RouteScope::Required("read:account")),
    (
        "GET",
        "/portfolio/accounts",
        RouteScope::Required("read:account"),
    ),
    (
        "GET",
        "/portfolio/positions",
        RouteScope::Required("read:account"),
    ),
    (
        "GET",
        "/portfolio/holdings",
        RouteScope::Required("read:account"),
    ),
    (
        "GET",
        "/portfolio/trades",
        RouteScope::Required("read:account"),
    ),
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
    ("POST", "/orders", RouteScope::Required("write:orders")),
    ("GET", "/orders", RouteScope::Required("write:orders")),
    (
        "DELETE",
        "/orders/{id}",
        RouteScope::Required("write:orders"),
    ),
    (
        "POST",
        "/positions/close",
        RouteScope::Required("write:orders"),
    ),
    (
        "GET",
        "/approval/queue",
        RouteScope::Required("write:orders"),
    ),
    (
        "POST",
        "/approval/approve",
        RouteScope::Required("write:orders"),
    ),
    (
        "POST",
        "/approval/reject",
        RouteScope::Required("write:orders"),
    ),
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
    ROUTE_SCOPES
        .iter()
        .find(|(m, pattern, _)| m.eq_ignore_ascii_case(method) && path_matches(pattern, path))
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
}

impl AuthLayer {
    /// Creates a new auth layer with the given token store and audit log path.
    pub fn new(store: FileTokenStore, audit_log_path: Option<PathBuf>) -> Self {
        Self {
            store,
            audit_log_path,
        }
    }

    fn required_scope(method: &str, path: &str) -> Option<&'static str> {
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
                    Json(ResponseEnvelope::error(detail)),
                ));
            }
        };

        match self.store.validate(token) {
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
                        ErrorCode::Forbidden,
                        format!("Token does not have required scope: {}", required),
                    )
                    .with_context(serde_json::json!({
                        "required_scope": required,
                        "token_scopes": info.scopes,
                        "token_id": info.token_id,
                    }));
                    return Err((StatusCode::FORBIDDEN, Json(ResponseEnvelope::error(detail))));
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
                Ok(Some(info))
            }
            Err(code) => {
                let (reason, status) = match code {
                    AuthErrorCode::Expired => ("expired", StatusCode::UNAUTHORIZED),
                    AuthErrorCode::InvalidToken => ("invalid_token", StatusCode::UNAUTHORIZED),
                    _ => ("invalid_token", StatusCode::UNAUTHORIZED),
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
                Err((status, Json(ResponseEnvelope::error(detail))))
            }
        }
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

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Some("read:market")
        );
        assert_eq!(
            AuthLayer::required_scope("GET", "/bars/ABC"),
            Some("read:market")
        );
        assert_eq!(
            AuthLayer::required_scope("POST", "/orders"),
            Some("write:orders")
        );
        assert_eq!(
            AuthLayer::required_scope("PUT", "/trading-state"),
            Some("operator")
        );
        assert_eq!(AuthLayer::required_scope("GET", "/unknown"), None);
    }
}
