//! Axum REST API.
use axum::{routing::get, Router};

pub fn router() -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/traders", get(|| async { "[]" }))
        .route("/orders", get(|| async { "[]" }))
        .route("/pnl", get(|| async { r#"{"inr":"0.00"}"# }))
}
