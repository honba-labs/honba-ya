#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! REST API server for Honba platform using axum.
//!
//! Provides read-only endpoints as specified in Phase 2 (E11-S3, E11-S4).
//! All responses are wrapped in the versioned envelope from `honba-api`.

use crate::auth::AuthRouterExt;
use axum::handler::Handler as _;
use axum::http::{HeaderValue, Method};
use axum::{
    async_trait,
    extract::{rejection::JsonRejection, FromRequest, Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post, put},
    Router,
};
use honba_api::{
    ApiResponse, Capabilities, CapabilitiesResponse, CompiledStrategy, ErrorCode, ErrorDetail,
    OrdersRequest, ResponseEnvelope, StrategiesResponse, SweepRequest, VerifyStrategyRequest,
    VerifyStrategyResponse,
};
use std::sync::{Arc, PoisonError};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};

mod approval;
mod auth;
mod bar_pump;
mod caller;
mod dispatch;
mod exec;
mod executor;
mod gateway_venue;
mod ingest;
mod journal;
mod market;
mod operator_journal;
mod orders;
mod portfolio;
mod portfolio_store;
mod profile;
mod profile_store;
mod registry;
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

pub use approval::ApprovalQueue;
pub use auth::{
    scope_for, unscoped_routes, AuthConfig, AuthLayer, FileTokenStore, RouteScope, TokenInfo,
    ROUTE_SCOPES,
};
pub use bar_pump::BarPumpReport;
pub use dispatch::{build_target, dispatch, dispatch_stream, DispatchError};
pub use exec::ExecutionVenue;
pub use executor::BacktestExecutor;
pub use gateway_venue::GatewayVenue;
pub use honba_api::JournalWriter;
pub use journal::NdjsonJournal;
pub use market::{ApiQuery, ApiQueryRejection, MAX_BAR_ROWS};
pub use orders::OrderLedger;
pub use portfolio_store::{
    PortfolioAccountRecord, PortfolioDocument, PortfolioError, PortfolioHeldPosition,
    PortfolioStore,
};
pub use profile_store::{Module, Profile, ProfileError, ProfileStore, Source, DEFAULT_USER_ID};
pub use registry::StrategyRegistry;
pub use retention::{evictions, RetentionPolicy};
pub use run_store::{OsEntropy, RecoveryReport, RunClock, RunEntropy, RunStore, SystemClock};
pub use service::{
    RunService, RunServiceConfig, ShutdownReport, DEFAULT_MAX_QUEUED, DEFAULT_SHUTDOWN_GRACE,
};
pub use state::AppState;
pub use streams::get_backtest_events;

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
            Json(ResponseEnvelope::<serde_json::Value>::error(detail)),
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
///
/// The default sends no CORS headers at all, so browsers refuse cross-origin reads; origins
/// are opt-in, one explicit entry each (never a wildcard).
/// Auth is optional: when `tokens_file` is provided, Bearer token auth is enforced.
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
    let router = Router::new()
        .route("/capabilities", get(get_capabilities))
        .route("/health", get(get_health))
        .route("/schema", get(get_schema))
        .route("/instruments", get(market::get_instruments))
        .route("/instruments/:id", get(market::get_instrument_by_id))
        .route("/quotes", get(market::get_quotes))
        .route("/bars/:id", get(market::get_bars))
        .route("/depth/:id", get(market::get_depth))
        .route("/bars", post(ingest::post_bars))
        .route(
            "/trading-state",
            get(trading_state::get_trading_state).put(trading_state::put_trading_state),
        )
        .route(
            "/profile",
            get(profile::get_profile).put(profile::put_profile.layer(profile::body_limit())),
        )
        .route("/profile/layouts", get(profile::get_layouts))
        .route(
            "/profile/layouts/:name",
            put(profile::put_layout.layer(profile::body_limit())).delete(profile::delete_layout),
        )
        .route("/profile/modules", get(profile::get_modules))
        .route(
            "/profile/modules/:id",
            put(profile::put_module.layer(profile::body_limit())).delete(profile::delete_module),
        )
        .route("/portfolio", get(portfolio::get_portfolio))
        .route("/portfolio/accounts", get(portfolio::get_accounts))
        .route("/portfolio/positions", get(portfolio::get_positions))
        .route("/portfolio/holdings", get(portfolio::get_holdings))
        .route("/portfolio/trades", get(portfolio::get_trades))
        .route("/strategies", post(post_strategies).get(get_strategies))
        .route("/strategies/verify", post(post_verify_strategy))
        .route("/backtests", post(runs::post_backtests))
        .route("/backtests/:id", get(runs::get_backtest_by_id))
        .route("/backtests/:id/journal", get(runs::get_backtest_journal))
        .route("/backtests/:id/events", get(get_backtest_events))
        .route("/sweeps", post(post_sweeps))
        .route("/sweeps/:id", get(get_sweep_by_id))
        .route("/sweeps/:id/journal", get(runs::get_sweep_journal))
        .route("/orders", post(post_orders).get(get_orders))
        .route("/orders/:id", delete(delete_order))
        .route("/positions/close", post(post_close_positions))
        .route("/approval/queue", get(orders::get_approval_queue))
        .route("/approval/approve", post(orders::post_approve_order))
        .route("/approval/reject", post(orders::post_reject_order))
        .route("/screener/scan", get(screener::get_screener_scan))
        .route("/journals/:id", get(runs::get_journal_by_id))
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
        let auth_layer = Arc::new(AuthLayer::new(store, audit_log_path));
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
            ErrorCode::Unsupported,
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
///
/// In-flight requests finish before this returns. The caller owns the listener, so a test can
/// bind `127.0.0.1:0` and read the chosen port; the future owns the stop signal (ctrl-c in the
/// CLI, a channel in tests). Handlers read no wall clock.
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
///
/// ADR 0017 decision 7 shrinks the list to the trading rows once the sweeps
/// (E4-S5) land; ADR 0018 decision 7 (E2-S2 r5b) risk-gates `POST /orders`,
/// `DELETE /orders/{id}` and `POST /positions/close`, so those left the list.
/// E11-S7 built the write ledger, so no route is left: the empty list is the
/// honest signal that every registry endpoint answers for real.
///
/// Remove a row when its handler is built; `tests/capabilities.rs` probes the router and fails
/// if this list and the real answers disagree.
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

/// Get API capabilities.
async fn get_capabilities(
    State(_state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<CapabilitiesResponse>> {
    Json(ApiResponse::success(Capabilities {
        capabilities: capability_manifest(),
    }))
}

/// Get health status.
async fn get_health() -> Json<ResponseEnvelope<serde_json::Value>> {
    Json(ApiResponse::success(serde_json::json!({"status": "ok"})))
}

/// Get schema bundle.
async fn get_schema() -> Json<ResponseEnvelope<serde_json::Value>> {
    // Return basic schema info - codegen will populate full schema
    Json(ApiResponse::success(serde_json::json!({
        "openapi": "3.1.0",
        "version": honba_api::API_VERSION
    })))
}

/// Lists the strategies compiled in this process, ordered by id.
async fn get_strategies(
    State(state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<StrategiesResponse>> {
    let catalog = state
        .strategies
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Json(ApiResponse::success(honba_api::list_strategies(&catalog)))
}

/// Compiles a manifest (the same step as verify) and keeps it in the session catalog.
///
/// The body is read as raw JSON first so a `code`/`source` field gets its own 422 reason; a
/// manifest that does not verify, or a full catalog, is a 422 carrying the reason code.
async fn post_strategies(
    State(state): State<Arc<AppState>>,
    ApiJson(body): ApiJson<serde_json::Value>,
) -> (StatusCode, Json<ResponseEnvelope<CompiledStrategy>>) {
    let compiled = honba_api::parse_compile_request(body).and_then(|req| {
        let mut catalog = state
            .strategies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        honba_api::compile_strategy(&mut catalog, req.manifest)
    });
    match compiled {
        Ok(strategy) => (StatusCode::OK, Json(ApiResponse::success(strategy))),
        Err(detail) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiResponse::error(detail)),
        ),
    }
}

/// Verifies a manifest and returns its IR; a manifest that does not verify is
/// a 422 carrying `validation_invalid_request` and the reason code.
async fn post_verify_strategy(
    ApiJson(manifest): ApiJson<VerifyStrategyRequest>,
) -> (StatusCode, Json<ResponseEnvelope<VerifyStrategyResponse>>) {
    match honba_api::verify_strategy(manifest) {
        Ok(ir) => (StatusCode::OK, Json(ApiResponse::success(ir))),
        Err(detail) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiResponse::error(detail)),
        ),
    }
}

async fn post_sweeps(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<SweepRequest>,
) -> Response {
    runs::post_sweeps(State(state), ApiJson(request)).await
}

async fn get_sweep_by_id(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    runs::get_sweep_by_id(State(state), Path(id)).await
}

async fn get_orders(
    State(state): State<Arc<AppState>>,
    query: ApiQuery<honba_api::OrdersQuery>,
) -> Response {
    orders::get_orders(State(state), query).await
}

async fn post_orders(State(state): State<Arc<AppState>>, req: ApiJson<OrdersRequest>) -> Response {
    orders::post_orders(State(state), req, honba_messages::OrderId::new("rest")).await
}

async fn delete_order(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    orders::delete_order(State(state), Path(id)).await
}

async fn post_close_positions(State(state): State<Arc<AppState>>) -> Response {
    orders::post_close_positions(State(state)).await
}

#[cfg(test)]
mod tests;
