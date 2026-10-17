//! `/profile` routes: per-user isolation, revisions, sub-resources and the "any valid token"
//! auth rule. The user is the token's `token_id`, or `default` when auth is off.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use honba_api_rest::{api_router_with, api_router_with_config, ApiConfig, AppState};
use serde_json::{json, Value};
use tower::util::ServiceExt;

// sha256("test") and sha256("other")
const HASH_A: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const HASH_B: &str = "d9298a10d1b0735837dc4bd85dac641b0f3cef27a47e5d53a54f2f3f5b2fcffa";

/// A router with a profiles dir; `tokens` is `(token_id, hash, scopes)` rows, empty for auth off.
fn router(tokens: &[(&str, &str, &str)]) -> (Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::default().with_profiles_dir(dir.path().join("profiles"));
    if tokens.is_empty() {
        return (api_router_with(state), dir);
    }
    let mut toml = String::new();
    for (id, hash, scopes) in tokens {
        toml.push_str(&format!(
            "[[tokens]]\ntoken_id = \"{id}\"\ntoken_hash = \"sha256:{hash}\"\n\
             scopes = [{scopes}]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ));
    }
    let path = dir.path().join("tokens.toml");
    std::fs::write(&path, toml).unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    (api_router_with_config(state, &config), dir)
}

fn two_users() -> (Router, tempfile::TempDir) {
    router(&[("alice", HASH_A, ""), ("bob", HASH_B, "\"read:market\"")])
}

async fn send(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let res = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON body {:?}", String::from_utf8_lossy(&bytes)));
    (status, parsed)
}

fn module(id: &str) -> Value {
    json!({"id": id, "name": "Core", "sources": [{"kind": "broker", "reference": "dhan"}],
           "enabled": true})
}

#[tokio::test]
async fn auth_off_serves_the_default_user() {
    let (app, dir) = router(&[]);
    let (status, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["revision"], json!(0));

    let put = json!({"revision": 0, "shortlist": ["INFY"], "alerts": {"rules": []}});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(put), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["revision"], json!(1));
    assert!(dir.path().join("profiles/default.json").exists());

    let (_, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(body["data"]["shortlist"], json!(["INFY"]));
    assert_eq!(body["data"]["alerts"], json!({"rules": []}));
}

#[tokio::test]
async fn no_profiles_dir_is_503() {
    let app = api_router_with(AppState::default());
    let (status, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["context"]["reason"], json!("no_profiles_dir"));
}

#[tokio::test]
async fn auth_on_needs_a_token_but_no_scope() {
    let (app, _dir) = two_users();
    for (method, uri) in [
        (Method::GET, "/profile"),
        (Method::PUT, "/profile"),
        (Method::GET, "/profile/layouts"),
        (Method::PUT, "/profile/layouts/x"),
        (Method::DELETE, "/profile/layouts/x"),
        (Method::GET, "/profile/modules"),
        (Method::PUT, "/profile/modules/x"),
        (Method::DELETE, "/profile/modules/x"),
    ] {
        let (status, body) = send(&app, method.clone(), uri, Some(json!({})), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}: {body}");
        assert_eq!(body["error"]["code"], json!("unauthorized"));
        let (status, _) = send(&app, method.clone(), uri, Some(json!({})), Some("wrong")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
    // alice's token carries no scopes at all and still reaches her profile.
    let (status, body) = send(&app, Method::GET, "/profile", None, Some("test")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn head_needs_the_same_token_as_get() {
    let (app, dir) = two_users();
    for uri in ["/profile", "/profile/layouts", "/orders"] {
        let req = |token: Option<&str>| {
            let mut b = Request::builder().method(Method::HEAD).uri(uri);
            if let Some(t) = token {
                b = b.header("authorization", format!("Bearer {t}"));
            }
            b.body(Body::empty()).unwrap()
        };
        let res = app.clone().oneshot(req(None)).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "HEAD {uri}");
        let res = app.clone().oneshot(req(Some("wrong"))).await.unwrap();
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "HEAD {uri} bad token"
        );
    }
    // A valid token still works, and no request ran as the default user.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri("/profile")
                .header("authorization", "Bearer test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(!dir.path().join("profiles/default.json").exists());
}

#[tokio::test]
async fn users_are_isolated_over_http() {
    let (app, dir) = two_users();
    let put = json!({"revision": 0, "shortlist": ["A"]});
    let (status, _) = send(&app, Method::PUT, "/profile", Some(put), Some("test")).await;
    assert_eq!(status, StatusCode::OK);
    send(
        &app,
        Method::PUT,
        "/profile/layouts/main",
        Some(json!({"grid": 1})),
        Some("test"),
    )
    .await;
    send(
        &app,
        Method::PUT,
        "/profile/modules/m1",
        Some(module("m1")),
        Some("test"),
    )
    .await;

    // bob sees none of it, and his own write does not disturb alice.
    let (_, body) = send(&app, Method::GET, "/profile", None, Some("other")).await;
    assert_eq!(body["data"]["revision"], json!(0));
    assert_eq!(body["data"]["shortlist"], json!([]));
    let (_, body) = send(&app, Method::GET, "/profile/layouts", None, Some("other")).await;
    assert_eq!(body["data"]["layouts"], json!({}));
    let (_, body) = send(&app, Method::GET, "/profile/modules", None, Some("other")).await;
    assert_eq!(body["data"]["modules"], json!({}));
    let put = json!({"revision": 0, "shortlist": ["B"]});
    let (status, _) = send(&app, Method::PUT, "/profile", Some(put), Some("other")).await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = send(&app, Method::GET, "/profile", None, Some("test")).await;
    assert_eq!(body["data"]["shortlist"], json!(["A"]));
    assert_eq!(body["data"]["revision"], json!(3));
    assert!(dir.path().join("profiles/alice.json").exists());
    assert!(dir.path().join("profiles/bob.json").exists());
    assert!(!dir.path().join("profiles/default.json").exists());
}

#[tokio::test]
async fn a_stale_revision_is_409_in_the_error_envelope() {
    let (app, _dir) = router(&[]);
    let (status, _) = send(
        &app,
        Method::PUT,
        "/profile",
        Some(json!({"shortlist": ["first"]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let put = json!({"revision": 0, "shortlist": ["late"]});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(put), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("revision_conflict")
    );
    assert_eq!(body["error"]["context"]["current"], json!(1));
    let (_, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(body["data"]["shortlist"], json!(["first"]));
}

#[tokio::test]
async fn layouts_crud() {
    let (app, _dir) = router(&[]);
    let (status, body) = send(
        &app,
        Method::PUT,
        "/profile/layouts/intraday",
        Some(json!({"panes": [1, 2]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["revision"], json!(1));

    // The same value again changes nothing and keeps the revision.
    let (_, body) = send(
        &app,
        Method::PUT,
        "/profile/layouts/intraday",
        Some(json!({"panes": [1, 2]})),
        None,
    )
    .await;
    assert_eq!(body["data"]["revision"], json!(1));

    let (_, body) = send(&app, Method::GET, "/profile/layouts", None, None).await;
    assert_eq!(
        body["data"]["layouts"],
        json!({"intraday": {"panes": [1, 2]}})
    );

    let (status, body) = send(
        &app,
        Method::DELETE,
        "/profile/layouts/intraday",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["revision"], json!(2));
    let (_, body) = send(&app, Method::GET, "/profile/layouts", None, None).await;
    assert_eq!(body["data"]["layouts"], json!({}));

    // Deleting what is not there succeeds without a new revision.
    let (status, body) = send(
        &app,
        Method::DELETE,
        "/profile/layouts/intraday",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["revision"], json!(2));
}

#[tokio::test]
async fn modules_crud_and_id_mismatch() {
    let (app, _dir) = router(&[]);
    let (status, body) = send(
        &app,
        Method::PUT,
        "/profile/modules/m1",
        Some(module("m1")),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = send(
        &app,
        Method::PUT,
        "/profile/modules/m1",
        Some(module("m2")),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["context"]["reason"], json!("id_mismatch"));

    let (_, body) = send(&app, Method::GET, "/profile/modules", None, None).await;
    assert_eq!(body["data"]["modules"], json!({"m1": module("m1")}));
    // Modules are part of the whole profile too.
    let (_, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(body["data"]["modules"]["m1"]["name"], json!("Core"));

    let (status, _) = send(&app, Method::DELETE, "/profile/modules/m1", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = send(&app, Method::GET, "/profile/modules", None, None).await;
    assert_eq!(body["data"]["modules"], json!({}));
}

#[tokio::test]
async fn bad_names_are_400() {
    let (app, dir) = router(&[]);
    let long = "n".repeat(129);
    for name in [long.as_str(), "a%00b", "a%0Ab", "a%7Fb"] {
        for uri in [
            format!("/profile/layouts/{name}"),
            format!("/profile/modules/{name}"),
        ] {
            let body = if uri.contains("modules") {
                module(name)
            } else {
                json!({})
            };
            let (status, resp) = send(&app, Method::PUT, &uri, Some(body), None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "PUT {uri}: {resp}");
            assert_eq!(resp["error"]["code"], json!("validation_invalid_request"));
            let (status, _) = send(&app, Method::DELETE, &uri, None, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "DELETE {uri}");
        }
    }
    assert!(!dir.path().join("profiles").exists());
    // 128 characters is fine, and the limit counts characters, not bytes.
    let edge = "é".repeat(128);
    let (status, body) = send(
        &app,
        Method::PUT,
        &format!("/profile/layouts/{edge}"),
        Some(json!({})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn a_hostile_token_id_is_refused_and_touches_no_path() {
    let (app, dir) = router(&[("../evil", HASH_A, "")]);
    for (method, uri) in [
        (Method::GET, "/profile"),
        (Method::PUT, "/profile"),
        (Method::PUT, "/profile/layouts/x"),
        (Method::DELETE, "/profile/modules/x"),
    ] {
        let (status, body) = send(&app, method.clone(), uri, Some(json!({})), Some("test")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {body}");
        assert_eq!(body["error"]["code"], json!("forbidden"));
    }
    assert!(!dir.path().join("profiles").exists());
    assert!(!dir.path().join("evil.json").exists());
}

#[tokio::test]
async fn oversized_bodies_and_profiles_are_413_with_a_reason() {
    let (app, _dir) = router(&[]);
    // Over the route body limit: refused before it is parsed.
    let huge = json!({"revision": 0, "preferences": "x".repeat(1024 * 1024 + 1)});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(huge.clone()), None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
    let (status, _) = send(&app, Method::PUT, "/profile/layouts/x", Some(huge), None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    // Under the body limit but over a count limit: 413 with the store's reason.
    let layouts: Value = (0..201).map(|i| (format!("l{i}"), json!(1))).collect();
    let put = json!({"revision": 0, "layouts": layouts});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(put), None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("profile_too_large")
    );
    let (_, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(body["data"]["revision"], json!(0));
}

#[tokio::test]
async fn put_profile_validates_map_keys_like_the_sub_resources() {
    let (app, dir) = router(&[]);
    let long = "n".repeat(129);
    let bad_bodies = [
        json!({"layouts": {"": {}}}),
        json!({"layouts": {long.clone(): {}}}),
        json!({"modules": {"a\0b": module("a\0b")}}),
        json!({"watchlists": {"tab\there": ["INFY"]}}),
        json!({"watchlists": {"": []}}),
    ];
    for body in bad_bodies {
        let (status, resp) = send(&app, Method::PUT, "/profile", Some(body.clone()), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {resp}");
        assert_eq!(resp["error"]["context"]["reason"], json!("invalid_name"));
    }
    let mismatch = json!({"modules": {"m1": module("m2")}});
    let (status, resp) = send(&app, Method::PUT, "/profile", Some(mismatch), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
    assert_eq!(resp["error"]["context"]["reason"], json!("id_mismatch"));
    assert!(!dir.path().join("profiles").exists());

    let good = json!({"layouts": {"main": {}}, "modules": {"m1": module("m1")},
                      "watchlists": {"core": ["INFY"]}});
    let (status, resp) = send(&app, Method::PUT, "/profile", Some(good), None).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
}

#[tokio::test]
async fn unknown_profile_fields_survive_a_get_put_round_trip() {
    let (app, _dir) = router(&[]);
    let put = json!({"revision": 0, "futureThing": {"k": 1}});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(put), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["futureThing"], json!({"k": 1}));
    let (_, body) = send(&app, Method::GET, "/profile", None, None).await;
    assert_eq!(body["data"]["futureThing"], json!({"k": 1}));
}

#[tokio::test]
async fn a_no_op_put_profile_keeps_the_revision() {
    let (app, _dir) = router(&[]);
    let put = json!({"revision": 0, "shortlist": ["INFY"]});
    let (_, body) = send(&app, Method::PUT, "/profile", Some(put), None).await;
    assert_eq!(body["data"]["revision"], json!(1));
    let again = json!({"revision": 1, "shortlist": ["INFY"]});
    let (status, body) = send(&app, Method::PUT, "/profile", Some(again), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["revision"], json!(1));
    assert_eq!(body["data"]["shortlist"], json!(["INFY"]));
}

#[tokio::test]
async fn a_layout_may_be_any_json_value() {
    let (app, _dir) = router(&[]);
    for (name, layout) in [
        ("arr", json!([1, 2])),
        ("str", json!("grid")),
        ("nul", Value::Null),
    ] {
        let uri = format!("/profile/layouts/{name}");
        let (status, body) = send(&app, Method::PUT, &uri, Some(layout.clone()), None).await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
        let (_, body) = send(&app, Method::GET, "/profile/layouts", None, None).await;
        assert_eq!(body["data"]["layouts"][name], layout);
    }
}
