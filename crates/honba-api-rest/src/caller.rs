//! The `Caller` extractor: the user a request acts for.
//!
//! Auth on: the validated bearer token's `token_id`. Auth off: [`DEFAULT_USER_ID`]. Auth on but
//! no token (the middleware let the request through some other way): 401, never the default
//! user. Both `/profile` and `/portfolio` scope their per-user documents by this value, so they
//! share this extractor rather than each resolving the owner their own way (ADR 0032 §1).

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::Response;
use honba_messages::{ErrorCode, ErrorDetail};

use crate::auth::{AuthEnforced, TokenInfo};
use crate::market::failure;
use crate::profile_store::DEFAULT_USER_ID;

/// The user a request acts for, resolved from the auth middleware's output.
///
/// Auth on: the validated token's `token_id`. Auth off: [`DEFAULT_USER_ID`]. Auth on but no
/// token: 401.
pub(crate) struct Caller(pub(crate) String);

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> std::result::Result<Self, Response> {
        if let Some(token) = parts.extensions.get::<TokenInfo>() {
            return Ok(Self(token.token_id.clone()));
        }
        if parts.extensions.get::<AuthEnforced>().is_some() {
            return Err(failure(
                StatusCode::UNAUTHORIZED,
                ErrorDetail::new(ErrorCode::Unauthorized, "a valid bearer token is required"),
            ));
        }
        Ok(Self(DEFAULT_USER_ID.to_owned()))
    }
}
