//! REST API v1 routes and handlers (Maintained for existing software).

use std::sync::{Arc, PoisonError};

use axum::handler::Handler as _;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Json, Response};
use axum::routing::{delete, get, post, put};
use axum::Router;
use honba_api::{
    ApiResponse, Capabilities, CapabilitiesResponse, CompiledStrategy,
    OrdersRequest, ResponseEnvelope, StrategiesResponse, SweepRequest, VerifyStrategyRequest,
    VerifyStrategyResponse,
};

use crate::market::{self, ApiQuery};
use crate::reports::{
    get_concentration_risk, get_equity_curve, get_fees_report, get_ledger, get_order_fills,
    get_portfolio_snapshots, get_position_audit, get_tax_report, get_weighted_average_cost,
};
use crate::ws::ws_handler;
use crate::{
    capability_manifest, ingest, openapi, orders, portfolio, profile, runs, screener,
    streams::get_backtest_events, trading_state, ApiJson, AppState,
};

/// Get API capabilities for v1.
pub async fn get_capabilities(
    State(_state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<CapabilitiesResponse>> {
    Json(ApiResponse::success(Capabilities {
        capabilities: capability_manifest(),
    }))
}

/// Get health status.
pub async fn get_health() -> Json<ResponseEnvelope<serde_json::Value>> {
    Json(ApiResponse::success(serde_json::json!({"status": "ok"})))
}

/// Get schema bundle — full OpenAPI 3.1 document.
pub async fn get_schema() -> Json<ResponseEnvelope<serde_json::Value>> {
    let doc = openapi::OpenApiDocument::generate_v1();
    Json(ApiResponse::success(serde_json::to_value(&doc).unwrap()))
}

/// Lists the strategies compiled in this process, ordered by id.
pub async fn get_strategies(
    State(state): State<Arc<AppState>>,
) -> Json<ResponseEnvelope<StrategiesResponse>> {
    let catalog = state
        .strategies
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Json(ApiResponse::success(honba_api::list_strategies(&catalog)))
}

/// Compiles a manifest (the same step as verify) and keeps it in the session catalog.
pub async fn post_strategies(
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

/// Verifies a manifest and returns its IR.
pub async fn post_verify_strategy(
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

/// Post sweeps.
pub async fn post_sweeps(
    State(state): State<Arc<AppState>>,
    ApiJson(request): ApiJson<SweepRequest>,
) -> Response {
    runs::post_sweeps(State(state), ApiJson(request)).await
}

/// Get sweep by id.
pub async fn get_sweep_by_id(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    runs::get_sweep_by_id(State(state), Path(id)).await
}

/// Get orders.
pub async fn get_orders(
    State(state): State<Arc<AppState>>,
    query: ApiQuery<honba_api::OrdersQuery>,
) -> Response {
    orders::get_orders(State(state), query).await
}

/// Post orders.
pub async fn post_orders(State(state): State<Arc<AppState>>, req: ApiJson<OrdersRequest>) -> Response {
    orders::post_orders(State(state), req, "rest".to_string()).await
}

/// Delete order.
pub async fn delete_order(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    orders::delete_order(State(state), Path(id)).await
}

/// Close positions.
pub async fn post_close_positions(State(state): State<Arc<AppState>>) -> Response {
    orders::post_close_positions(State(state)).await
}

/// Adds v1 version header to response.
pub async fn v1_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        axum::http::HeaderName::from_static("x-honba-version"),
        axum::http::HeaderValue::from_static("1"),
    );
    res
}

/// Build the v1 router with attached state.
pub fn routes(state: Arc<AppState>) -> Router {
    router().with_state(state)
}

/// Build the v1 router expecting State<Arc<AppState>>.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
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
        .route("/portfolio/chart", get(get_equity_curve))
        .route("/portfolio/concentration", get(get_concentration_risk))
        .route("/portfolio/wac", get(get_weighted_average_cost))
        .route("/ledger", get(get_ledger))
        .route("/tax/report", get(get_tax_report))
        .route("/fees", get(get_fees_report))
        .route("/positions", get(portfolio::get_positions))
        .route("/positions/audit", get(get_position_audit))
        .route("/trades", get(portfolio::get_trades))
        .route("/reports/ledger", get(get_ledger))
        .route("/reports/tax", get(get_tax_report))
        .route("/reports/snapshots", get(get_portfolio_snapshots))
        .route("/reports/audit", get(get_position_audit))
        .route("/reports/concentration", get(get_concentration_risk))
        .route("/reports/wac", get(get_weighted_average_cost))
        .route("/reports/fees", get(get_fees_report))
        .route("/reports/equity-curve", get(get_equity_curve))
        .route("/reports/fills/:order_id", get(get_order_fills))
        .route("/strategies", post(post_strategies).get(get_strategies))
        .route("/strategies/verify", post(post_verify_strategy))
        .route("/backtests", post(runs::post_backtests).get(runs::get_backtests))
        .route("/backtests/:id", get(runs::get_backtest_by_id))
        .route("/backtests/:id/journal", get(runs::get_backtest_journal))
        .route("/backtests/:id/events", get(get_backtest_events))
        .route("/sweeps", post(post_sweeps).get(runs::get_sweeps))
        .route("/sweeps/:id", get(get_sweep_by_id))
        .route("/sweeps/:id/journal", get(runs::get_sweep_journal))
        .route("/orders", post(post_orders).get(get_orders))
        .route("/orders/:id", delete(delete_order))
        .route("/orders/:id/fills", get(get_order_fills))
        .route("/positions/close", post(post_close_positions))
        .route("/approval/queue", get(orders::get_approval_queue))
        .route("/approval/approve", post(orders::post_approve_order))
        .route("/approval/reject", post(orders::post_reject_order))
        .route("/screener/scan", get(screener::get_screener_scan))
        .route("/journals/:id", get(runs::get_journal_by_id))
        .route("/ws", get(ws_handler))
        .route("/ws/market", get(ws_handler))
        .layer(axum::middleware::from_fn(v1_headers))
}
