//! `/profile` routes: the caller's own profile, its named layouts and its modules.
//!
//! The user is the validated token's `token_id` when auth is on, else
//! [`DEFAULT_USER_ID`](crate::profile_store::DEFAULT_USER_ID). A
//! token can therefore only ever reach its own profile; there is no user id in any path. Sub-
//! resource writes go through [`ProfileStore::update`], so each is one atomic read-modify-write
//! that bumps the revision once. `PUT /profile` is a compare-and-swap on the body's `revision`.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use honba_api::{LayoutDocument, LayoutsResponse, ModulesResponse, ProfileRevision};
use honba_messages::{ErrorCode, ErrorDetail};
use serde::Serialize;
use serde_json::json;
use tracing::error;

use crate::market::{failure, success};
use crate::profile_store::Result;
use crate::{ApiJson, AppState, Module, Profile, ProfileError, ProfileStore};

pub(crate) use crate::caller::Caller;

/// Longest accepted layout name or module id, in characters.
const MAX_NAME_LEN: usize = 128;

/// Largest request body accepted on the `PUT /profile*` routes, in bytes. Smaller than
/// [`MAX_PROFILE_BYTES`](crate::profile_store::MAX_PROFILE_BYTES) so oversized bodies are refused
/// (413) before they are buffered and parsed.
pub(crate) const MAX_BODY_BYTES: usize = 1024 * 1024;

/// The body-size layer of the `PUT /profile*` routes.
pub(crate) fn body_limit() -> DefaultBodyLimit {
    DefaultBodyLimit::max(MAX_BODY_BYTES)
}

/// Runs `work` for the caller's profile on the blocking pool and renders the result.
async fn with_store<T: Serialize + Send + 'static>(
    state: Arc<AppState>,
    Caller(user): Caller,
    work: impl FnOnce(&ProfileStore, &str) -> Result<T> + Send + 'static,
) -> Response {
    let Some(store) = state.profiles.clone() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorDetail::new(
                ErrorCode::Unsupported,
                "no profiles directory is configured",
            )
            .with_context(json!({"reason": "no_profiles_dir"})),
        );
    };
    match tokio::task::spawn_blocking(move || work(&store, &user)).await {
        Ok(Ok(data)) => success(data),
        Ok(Err(e)) => profile_failure(e),
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorDetail::new(ErrorCode::InternalError, "profile task failed"),
        ),
    }
}

fn profile_failure(e: ProfileError) -> Response {
    match e {
        // The id comes from a token record, never from the request: refuse, don't build a path.
        ProfileError::InvalidUserId => failure(
            StatusCode::FORBIDDEN,
            ErrorDetail::new(ErrorCode::Forbidden, "this token id cannot own a profile")
                .with_context(json!({"reason": "invalid_user_id"})),
        ),
        ProfileError::RevisionConflict { expected, current } => failure(
            StatusCode::CONFLICT,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("revision conflict: expected {expected}, current {current}"),
            )
            .with_context(json!({
                "reason": "revision_conflict",
                "expected": expected,
                "current": current,
            })),
        ),
        ProfileError::TooLarge { what } => failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("profile too large: {what}"),
            )
            .with_context(json!({"reason": "profile_too_large"})),
        ),
        ProfileError::Io(_) | ProfileError::Json(_) => {
            error!(error = %e, "profile store failure");
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorDetail::new(ErrorCode::InternalError, "profile store failure"),
            )
        }
    }
}

/// Why `name` is not a valid layout name, module id or watchlist name (1..=128 characters with
/// no control character), or `None` when it is fine. One rule for the sub-resource routes and
/// for the keys of `PUT /profile`.
fn name_problem(kind: &str, name: &str) -> Option<String> {
    let ok = !name.is_empty()
        && name.chars().count() <= MAX_NAME_LEN
        && !name.chars().any(char::is_control);
    (!ok).then(|| {
        format!("{kind} must be 1 to {MAX_NAME_LEN} characters with no control characters")
    })
}

/// The 400 response for an invalid name, or `None` when it is fine.
fn check_name(kind: &str, name: &str) -> Option<Response> {
    name_problem(kind, name).map(|message| bad_request(message, "invalid_name"))
}

/// The 400 response for a whole profile whose map keys are not valid names or whose module ids
/// differ from their keys, or `None` when it is fine.
fn check_profile(profile: &Profile) -> Option<Response> {
    let layouts = profile.layouts.keys().map(|k| ("layout name", k));
    let modules = profile.modules.keys().map(|k| ("module id", k));
    let lists = profile.watchlists.keys().map(|k| ("watchlist name", k));
    for (kind, name) in layouts.chain(modules).chain(lists) {
        if let Some(response) = check_name(kind, name) {
            return Some(response);
        }
    }
    let mismatch = profile.modules.iter().find(|(key, m)| m.id != **key)?;
    Some(bad_request(
        format!(
            "module id {:?} does not match its key {:?}",
            mismatch.1.id, mismatch.0
        ),
        "id_mismatch",
    ))
}

fn bad_request(message: String, reason: &str) -> Response {
    failure(
        StatusCode::BAD_REQUEST,
        ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
            .with_context(json!({"reason": reason})),
    )
}

/// `GET /profile`.
pub(crate) async fn get_profile(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    with_store(state, caller, |store, user| store.get(user)).await
}

/// `PUT /profile`: replace the profile; the body's `revision` is the one the client last read.
pub(crate) async fn put_profile(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    ApiJson(profile): ApiJson<Profile>,
) -> Response {
    if let Some(response) = check_profile(&profile) {
        return response;
    }
    let expected = profile.revision;
    with_store(state, caller, move |store, user| {
        store.put(user, expected, profile)
    })
    .await
}

/// `GET /profile/layouts`.
pub(crate) async fn get_layouts(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    with_store(state, caller, |store, user| {
        store
            .get(user)
            .map(|p| LayoutsResponse { layouts: p.layouts })
    })
    .await
}

/// `PUT /profile/layouts/{name}`: the body is the layout, any JSON.
pub(crate) async fn put_layout(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Path(name): Path<String>,
    ApiJson(LayoutDocument(layout)): ApiJson<LayoutDocument>,
) -> Response {
    if let Some(response) = check_name("layout name", &name) {
        return response;
    }
    with_store(state, caller, move |store, user| {
        store
            .update(user, |p| {
                p.layouts.insert(name, layout.clone()).as_ref() != Some(&layout)
            })
            .map(revision_of)
    })
    .await
}

/// `DELETE /profile/layouts/{name}`: deleting a missing layout is a no-op.
pub(crate) async fn delete_layout(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Path(name): Path<String>,
) -> Response {
    if let Some(response) = check_name("layout name", &name) {
        return response;
    }
    with_store(state, caller, move |store, user| {
        store
            .update(user, |p| p.layouts.remove(&name).is_some())
            .map(revision_of)
    })
    .await
}

/// `GET /profile/modules`.
pub(crate) async fn get_modules(State(state): State<Arc<AppState>>, caller: Caller) -> Response {
    with_store(state, caller, |store, user| {
        store
            .get(user)
            .map(|p| ModulesResponse { modules: p.modules })
    })
    .await
}

/// `PUT /profile/modules/{id}`: the body's `id` must equal the path id.
pub(crate) async fn put_module(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Path(id): Path<String>,
    ApiJson(module): ApiJson<Module>,
) -> Response {
    if let Some(response) = check_name("module id", &id) {
        return response;
    }
    if module.id != id {
        return bad_request(
            format!(
                "module id {:?} does not match the path id {id:?}",
                module.id
            ),
            "id_mismatch",
        );
    }
    with_store(state, caller, move |store, user| {
        store
            .update(user, |p| {
                p.modules.insert(id, module.clone()).as_ref() != Some(&module)
            })
            .map(revision_of)
    })
    .await
}

/// `DELETE /profile/modules/{id}`: deleting a missing module is a no-op.
pub(crate) async fn delete_module(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = check_name("module id", &id) {
        return response;
    }
    with_store(state, caller, move |store, user| {
        store
            .update(user, |p| p.modules.remove(&id).is_some())
            .map(revision_of)
    })
    .await
}

fn revision_of(profile: Profile) -> ProfileRevision {
    ProfileRevision {
        revision: profile.revision,
    }
}
