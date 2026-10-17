//! In-process driver of the router: one request in, `(status, body)` out, no socket.
//!
//! This is how a host that embeds the API (the Python SDK's in-process transport) calls the
//! very same [`Router`] that [`crate::serve`] exposes, so both give identical statuses and
//! envelopes. The query arrives as a flat JSON object and is form-encoded here, once, so a
//! caller never hand-builds a query string.

use std::fmt;

use axum::{
    body::Body,
    http::{header, Method, Request},
    Router,
};
use serde_json::Value;
use tower::util::ServiceExt;

/// Why a request could not be handed to the router. A request the router
/// *rejects* is not an error here: it is an ordinary `(status, body)` answer.
#[derive(Debug, PartialEq, Eq)]
pub enum DispatchError {
    /// The method is not a valid HTTP method token.
    InvalidMethod(String),
    /// The path is not an absolute path, or the request could not be built from it.
    InvalidTarget(String),
    /// The query is not a flat JSON object of scalars.
    InvalidQuery(String),
    /// The response body could not be read.
    Body(String),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMethod(m) => write!(f, "invalid HTTP method {m:?}"),
            Self::InvalidTarget(m) => write!(f, "invalid request target: {m}"),
            Self::InvalidQuery(m) => write!(f, "invalid query: {m}"),
            Self::Body(m) => write!(f, "unreadable response body: {m}"),
        }
    }
}

impl std::error::Error for DispatchError {}

/// Joins `path` and a JSON query object into a request target.
///
/// `query_json` is `None`, `null`, or a flat object whose values are strings, numbers or
/// booleans (`null` values are skipped). Keys and values are form-encoded.
pub fn build_target(path: &str, query_json: Option<&str>) -> Result<String, DispatchError> {
    if !path.starts_with('/') {
        return Err(DispatchError::InvalidTarget(format!(
            "path {path:?} must start with '/'"
        )));
    }
    let Some(text) = query_json else {
        return Ok(path.to_owned());
    };
    let invalid = |m: String| DispatchError::InvalidQuery(m);
    let value: Value = serde_json::from_str(text).map_err(|e| invalid(e.to_string()))?;
    let object = match value {
        Value::Null => return Ok(path.to_owned()),
        Value::Object(object) => object,
        _ => return Err(invalid("expected a JSON object".to_owned())),
    };
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (key, value) in object {
        match value {
            Value::Null => {}
            Value::String(s) => pairs.push((key, s)),
            Value::Number(n) => pairs.push((key, n.to_string())),
            Value::Bool(b) => pairs.push((key, b.to_string())),
            _ => return Err(invalid(format!("value of {key:?} is not a scalar"))),
        }
    }
    if pairs.is_empty() {
        return Ok(path.to_owned());
    }
    let encoded = serde_urlencoded::to_string(&pairs).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("{path}?{encoded}"))
}

/// Sends one request through `router` and returns the status and the raw body text.
///
/// `target` is a path with an optional query (see [`build_target`]); `body`, when present, is
/// sent as `application/json`. No wall clock, no network.
pub async fn dispatch(
    router: Router,
    method: &str,
    target: &str,
    body: Option<&str>,
) -> Result<(u16, String), DispatchError> {
    if !target.starts_with('/') {
        return Err(DispatchError::InvalidTarget(format!(
            "target {target:?} must start with '/'"
        )));
    }
    let method = Method::from_bytes(method.as_bytes())
        .map_err(|_| DispatchError::InvalidMethod(method.to_owned()))?;
    let mut request = Request::builder().method(method).uri(target);
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    let request = request
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
        .map_err(|e| DispatchError::InvalidTarget(e.to_string()))?;
    let response = router.oneshot(request).await.map_err(|e| match e {})?; // `Router`'s error type is `Infallible`.
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|e| DispatchError::Body(e.to_string()))?;
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

/// Sends one SSE streaming request through `router` and invokes `callback` for each
/// `Event` (a `StreamFrame` line). The callback receives the raw SSE frame bytes.
/// Returns `Ok(())` when the stream ends, or `Err` if the callback returns an error
/// or the response is an error status.
///
/// This is used by the in-process Python transport (`honba._honba.api_request_stream`)
/// to stream run events without a network socket.
pub async fn dispatch_stream<F>(
    router: Router,
    method: &str,
    target: &str,
    body: Option<&str>,
    mut callback: F,
) -> Result<(), DispatchError>
where
    F: FnMut(&[u8]) -> Result<(), String> + Send + 'static,
{
    if !target.starts_with('/') {
        return Err(DispatchError::InvalidTarget(format!(
            "target {target:?} must start with '/'"
        )));
    }
    let method = Method::from_bytes(method.as_bytes())
        .map_err(|_| DispatchError::InvalidMethod(method.to_owned()))?;
    let mut request = Request::builder().method(method).uri(target);
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    request = request.header(header::ACCEPT, "text/event-stream");
    let request = request
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
        .map_err(|e| DispatchError::InvalidTarget(e.to_string()))?;

    let response = router.oneshot(request).await.map_err(|e| match e {})?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|e| DispatchError::Body(e.to_string()))?;
        let text = String::from_utf8_lossy(&bytes).into_owned();
        return Err(DispatchError::Body(text));
    }

    // For SSE, we need to stream. The response body from a oneshot is already fully buffered.
    // Instead, we'll read the full body and parse it as SSE events.
    // Note: This is a simplification for the in-process transport; the real HTTP path uses Sse.
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|e| DispatchError::Body(e.to_string()))?;
    let text = String::from_utf8_lossy(&bytes).into_owned();

    // Parse SSE format: each event is separated by blank line, with "data: " prefix.
    for event_block in text.split("\n\n") {
        let event_text = event_block.trim();
        if event_text.is_empty() {
            continue;
        }
        // Extract data: lines
        for line in event_text.lines() {
            if let Some(data) = line.strip_prefix("data: ") {
                callback(data.as_bytes()).map_err(DispatchError::Body)?;
            }
        }
    }
    Ok(())
}
