#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! REST API server for Honba platform using axum.
//!
//! Provides multi-version API routing (`/api/v1`, `/api/v2`, and root fallback)
//! as specified in Phase 1 of the Versioning Design Plan.
//! All responses are wrapped in the versioned envelope from `honba-api`.

use crate::auth::AuthRouterExt;
use axum::http::{HeaderName, HeaderValue, Method};
use axum::{
    async_trait,
    extract::{rejection::JsonRejection, FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use honba_api::{ApiResponse, ErrorCode, ErrorDetail};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

mod approval;
mod auth;
mod bar_pump;
mod caller;
mod catalog;
mod dispatch;
mod exec;
mod executor;
mod ingest;
mod journal;
mod market;
mod openapi;
mod operator_journal;
pub mod orders;
mod portfolio;
mod portfolio_store;
mod profile;
mod profile_store;
mod registry;
mod reports;
mod retention;
mod run_store;
mod runs;
mod screener;
mod service;
mod state;
mod streams;
mod trades;
mod trading_state;
mod user_doc;
pub mod v1;
pub mod v2;
pub mod ws;

pub use approval::ApprovalQueue;
pub use auth::{
    scope_for, unscoped_routes, AuthConfig, AuthLayer, FileTokenStore, RouteScope, TokenInfo,
    ROUTE_SCOPES,
};
pub use bar_pump::BarPumpReport;
pub use catalog::{load_catalog, CatalogBars, CatalogLoadError, CATALOG_DIR};
pub use dispatch::{build_target, dispatch, dispatch_stream, DispatchError};
pub use exec::NextOpenSim;
pub use executor::BacktestExecutor;
pub use honba_api::JournalWriter;
pub use journal::NdjsonJournal;
pub use market::{ApiQuery, ApiQueryRejection, MAX_BAR_ROWS};
pub use orders::OrderLedger;
pub use portfolio_store::{
    PortfolioAccountRecord, PortfolioDocument, PortfolioError, PortfolioHeldPosition,
    PortfolioStore,
};
pub use profile_store::{DEFAULT_USER_ID, Module, Profile, ProfileError, ProfileStore, Source};
pub use registry::StrategyRegistry;
pub use retention::{evictions, RetentionPolicy};
pub use run_store::{OsEntropy, RecoveryReport, RunClock, RunEntropy, RunStore, SystemClock};
pub use service::{
    RunService, RunServiceConfig, ShutdownReport, DEFAULT_MAX_QUEUED, DEFAULT_SHUTDOWN_GRACE,
};
pub use state::AppState;
pub use streams::get_backtest_events;
pub use ws::ws_handler;

pub use v1::{
    delete_order, get_capabilities, get_health, get_orders, get_schema, get_strategies,
    get_sweep_by_id, post_close_positions, post_orders, post_strategies, post_sweeps,
    post_verify_strategy,
};

/// JSON body extractor whose rejections are the standard error envelope
/// (`validation_invalid_request`) rather than axum's plain-text message.
#[derive(Debug)]
pub struct ApiJson<T>(pub T);

/// Rejection of [`ApiJson`]: keeps axum's status, wraps its message in an envelope.
#[derive(Debug)]
pub struct ApiJsonRejection(JsonRejection);

impl IntoResponse for ApiJsonRejection {
    fn into_response(self) -> Response {
        let status = self.0.status();
        let detail = ErrorDetail::new(ErrorCode::ValidationInvalidRequest, self.0.body_text());
        (
            status,
            Json(ApiResponse::error::<serde_json::Value>(detail)),
        )
            .into_response()
    }
}

#[async_trait]
impl<S, T> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(v)| Self(v))
            .map_err(ApiJsonRejection)
    }
}

/// Server configuration beyond the data: today only the CORS allow-list and auth.
#[derive(Clone, Debug, Default)]
pub struct ApiConfig {
    cors_origins: Vec<HeaderValue>,
    /// Auth configuration; when `tokens_file` is Some, Bearer token auth is enforced
    pub auth: AuthConfig,
}

impl ApiConfig {
    /// The allowed CORS origins; empty means CORS is off.
    pub fn cors_origins(&self) -> &[HeaderValue] {
        &self.cors_origins
    }

    /// Allows exactly these origins (e.g. `https://app.example`); an entry that cannot be a
    /// header value, or a bare `*`, is an error.
    pub fn with_cors_origins<I, S>(mut self, origins: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for origin in origins {
            let origin = origin.as_ref();
            if origin == "*" {
                return Err("cors origin '*' is not allowed; list explicit origins".to_owned());
            }
            let value = HeaderValue::from_str(origin)
                .map_err(|_| format!("cors origin {origin:?} is not a valid header value"))?;
            self.cors_origins.push(value);
        }
        Ok(self)
    }
}

/// Middleware providing RFC 8594 deprecation and canonical link headers for legacy un-prefixed requests.
/// Supports content negotiation via `Accept: application/vnd.honba.v2+json` or `X-Honba-Version: 2`.
pub async fn legacy_v1_deprecation_notice(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let wants_v2 = req
        .headers()
        .get("x-honba-version")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "2")
        .unwrap_or(false)
        || req
            .headers()
            .get(axum::http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("application/vnd.honba.v2+json"))
            .unwrap_or(false);

    let res = next.run(req).await;
    let (mut parts, body) = res.into_parts();

    if wants_v2 {
        parts.headers.insert(
            HeaderName::from_static("x-honba-version"),
            HeaderValue::from_static("2"),
        );
        let link = format!("</api/v2{path}>; rel=\"canonical\"");
        if let Ok(val) = HeaderValue::from_str(&link) {
            parts.headers.insert(axum::http::header::LINK, val);
        }
        let bytes = match axum::body::to_bytes(body, usize::MAX).await {
            Ok(b) => b,
            Err(_) => return Response::from_parts(parts, axum::body::Body::empty()),
        };
        if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if val.is_object() && val.get("api_version").is_some() {
                val["api_version"] = serde_json::json!("v2");
                let new_body = serde_json::to_vec(&val).unwrap_or_default();
                parts.headers.insert(
                    axum::http::header::CONTENT_LENGTH,
                    HeaderValue::from(new_body.len()),
                );
                return Response::from_parts(parts, axum::body::Body::from(new_body));
            }
        }
        Response::from_parts(parts, axum::body::Body::from(bytes))
    } else {
        parts.headers.insert(
            HeaderName::from_static("deprecation"),
            HeaderValue::from_static("true"),
        );
        parts.headers.insert(
            HeaderName::from_static("x-honba-version"),
            HeaderValue::from_static("1"),
        );
        let link = format!("</api/v1{path}>; rel=\"canonical\"");
        if let Ok(val) = HeaderValue::from_str(&link) {
            parts.headers.insert(axum::http::header::LINK, val);
        }
        Response::from_parts(parts, body)
    }
}

/// Create the API router over an empty catalogue.
pub fn api_router() -> Router {
    api_router_with(AppState::default())
}

/// Create the API router serving `state`.
pub fn api_router_with(state: AppState) -> Router {
    api_router_with_config(state, &ApiConfig::default())
}

/// Create the API router serving `state` under `config`.
pub fn api_router_with_config(state: AppState, config: &ApiConfig) -> Router {
    let state = Arc::new(state);

    let v1_router = v1::router();
    let v2_router = v2::router();

    let router = Router::new()
        .nest("/api/v1", v1_router.clone())
        .nest("/api/v2", v2_router)
        // Root fallback defaults to v1 for existing un-prefixed clients with deprecation warning
        .nest(
            "/",
            v1_router.layer(axum::middleware::from_fn(legacy_v1_deprecation_notice)),
        )
        // OpenAPI specifications per version
        .route("/schema/openapi/v1.json", get(openapi::get_openapi_v1))
        .route("/schema/openapi/v2.json", get(openapi::get_openapi_v2))
        .route("/schema/openapi", get(openapi::get_openapi_index))
        .route("/docs", get(openapi::get_swagger_ui))
        .route("/swagger-ui", get(openapi::get_swagger_ui))
        .fallback(unknown_route)
        .method_not_allowed_fallback(unsupported_method)
        .with_state(state)
        .layer(CompressionLayer::new())
        .layer(cors_layer(config))
        .layer(TraceLayer::new_for_http());

    // Add auth middleware if configured
    if let Some(tokens_file) = &config.auth.tokens_file {
        let audit_log_path = config.auth.audit_log_path.clone();
        let store = FileTokenStore::new(tokens_file.clone()).expect("failed to load token file");
        let auth_layer =
            Arc::new(AuthLayer::new(store, audit_log_path, config.auth.jwt_config.clone()));
        router.with_auth(auth_layer)
    } else {
        router
    }
}

/// An unknown route is a 404 `not_found` envelope, never an empty body.
async fn unknown_route(uri: axum::http::Uri) -> Response {
    market::failure(
        StatusCode::NOT_FOUND,
        ErrorDetail::new(
            ErrorCode::NotFound,
            format!("no such route: {}", uri.path()),
        ),
    )
}

/// A known path with a method it does not serve is a 405 `unsupported` envelope.
async fn unsupported_method(method: Method, uri: axum::http::Uri) -> Response {
    market::failure(
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorDetail::new(
            ErrorCode::NotImplemented,
            format!("{method} is not supported on {}", uri.path()),
        ),
    )
}

/// No CORS headers unless origins are listed; then only for those, read methods only.
fn cors_layer(config: &ApiConfig) -> CorsLayer {
    if config.cors_origins.is_empty() {
        return CorsLayer::new();
    }
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(config.cors_origins.clone()))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE])
}

/// Serves [`api_router_with`]`(state)` on `listener` until `shutdown` completes.
pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_with_config(listener, state, &ApiConfig::default(), shutdown).await
}

/// [`serve`] under an explicit [`ApiConfig`].
pub async fn serve_with_config(
    listener: tokio::net::TcpListener,
    state: AppState,
    config: &ApiConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let router = api_router_with_config(state, config);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

/// Routes still answering 501 `not_implemented`.
pub const NOT_IMPLEMENTED_ENDPOINTS: &[(&str, &str)] = &[];

fn endpoint_key((method, path): &(&str, &str)) -> String {
    format!("{method} {path}")
}

/// The capability manifest: endpoints come from the registry, never a hand-kept list.
pub(crate) fn capability_manifest() -> honba_api::CapabilityManifest {
    honba_api::CapabilityManifest {
        crates: vec![
            "honba-api".to_string(),
            "honba-api-rest".to_string(),
            "honba-messages".to_string(),
            "honba-entities".to_string(),
        ],
        market_packs: vec!["india".to_string(), "null".to_string()],
        endpoints: honba_api::ENDPOINTS.iter().map(endpoint_key).collect(),
        not_implemented: NOT_IMPLEMENTED_ENDPOINTS.iter().map(endpoint_key).collect(),
        toolsets: vec!["strategies".to_string(), "indicators".to_string()],
        adapters: vec![],
        features: std::collections::BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests;
