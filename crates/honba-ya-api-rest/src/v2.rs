//! REST API v2 routes and handlers (Latest & Recommended).

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderValue;
use axum::response::Response;

use crate::{AppState, v1};

/// Build the v2 router with attached state.
pub fn routes(state: Arc<AppState>) -> Router {
    router().with_state(state)
}

/// Build the v2 router expecting State<Arc<AppState>>.
pub fn router() -> Router<Arc<AppState>> {
    v1::router().layer(axum::middleware::from_fn(v2_response_envelope))
}

/// Middleware that updates the response envelope and headers for API v2.
pub async fn v2_response_envelope(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let res = next.run(req).await;
    let (mut parts, body) = res.into_parts();
    parts.headers.insert(
        axum::http::HeaderName::from_static("x-honba-version"),
        HeaderValue::from_static("2"),
    );

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
}
