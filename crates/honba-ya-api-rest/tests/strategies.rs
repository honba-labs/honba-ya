//! `POST /strategies` (compile) and `GET /strategies` (list) through the real router.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use honba_api::{CompiledStrategy, StrategiesResponse, StrategyCatalog};
use honba_api_rest::{api_router, api_router_with, AppState};
use serde_json::{json, Value};
use tower::util::ServiceExt;

fn manifest(name: &str, hash: &str) -> Value {
    let tcs = json!({"symbol": "TCS", "exchange": "NSE"});
    json!({
        "api_version": "1.0.0",
        "name": name,
        "source_hash": hash,
        "universe": {"explicit": [tcs]},
        "subscriptions": {"instruments": [tcs]},
        "driving_timeframe": {"interval": 1, "aggregation": "day"},
        "warmup_bars": 20
    })
}

async fn call(router: &Router, method: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri("/strategies");
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = router
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn compile(router: &Router, m: Value) -> (StatusCode, Value) {
    call(router, "POST", Some(json!({ "manifest": m }))).await
}

async fn listed(router: &Router) -> Vec<CompiledStrategy> {
    let (status, body) = call(router, "GET", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data: StrategiesResponse = serde_json::from_value(body["data"].clone()).unwrap();
    data.strategies
}

#[tokio::test]
async fn compiling_a_manifest_returns_an_id_and_its_ir() {
    let router = api_router();
    let (status, body) = compile(&router, manifest("sma", "sha256:abc")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let compiled: CompiledStrategy = serde_json::from_value(body["data"].clone()).unwrap();
    assert!(compiled.id.starts_with("sha256:"));
    assert_eq!(compiled.ir.warmup_bars, 20);
    assert!(body["error"].is_null());
}

#[tokio::test]
async fn an_empty_catalog_lists_nothing() {
    assert!(listed(&api_router()).await.is_empty());
}

#[tokio::test]
async fn the_same_manifest_twice_gives_the_same_id_and_is_listed_once() {
    let router = api_router();
    let (_, first) = compile(&router, manifest("sma", "sha256:abc")).await;
    let (status, second) = compile(&router, manifest("sma", "sha256:abc")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["data"]["id"], second["data"]["id"]);
    let all = listed(&router).await;
    assert_eq!(all.len(), 1);
    assert_eq!(json!(all[0].id), first["data"]["id"]);
}

#[tokio::test]
async fn the_catalog_is_shared_by_every_request_to_one_router_and_not_across_routers() {
    let router = api_router();
    compile(&router, manifest("sma", "sha256:abc")).await;
    assert_eq!(listed(&router).await.len(), 1);
    assert!(listed(&api_router()).await.is_empty());
}

#[tokio::test]
async fn the_list_is_ordered_by_id() {
    let router = api_router();
    for n in ["d", "b", "e", "a", "c"] {
        compile(&router, manifest("s", &format!("sha256:{n}"))).await;
    }
    let ids: Vec<String> = listed(&router).await.into_iter().map(|s| s.id).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids.len(), 5);
    assert_eq!(ids, sorted);
}

#[tokio::test]
async fn an_unverifiable_manifest_gets_the_verify_error_envelope_and_is_not_stored() {
    let router = api_router();
    let mut bad = manifest("sma", "sha256:abc");
    bad["subscriptions"]["instruments"] = json!([]);
    let (status, body) = compile(&router, bad).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("no_subscriptions")
    );
    assert!(body["data"].is_null());
    assert!(listed(&router).await.is_empty());
}

#[tokio::test]
async fn source_code_is_a_422_with_reason_source_unsupported() {
    let (status, body) = call(
        &api_router(),
        "POST",
        Some(json!({"name": "x", "code": "class S: pass"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], json!("validation_invalid_request"));
    assert_eq!(
        body["error"]["context"]["reason"],
        json!("source_unsupported")
    );
}

#[tokio::test]
async fn a_body_without_a_manifest_or_with_a_typo_is_a_422() {
    let router = api_router();
    for body in [
        json!({}),
        json!({"manifest": manifest("a", "h"), "extra": 1}),
    ] {
        let (status, parsed) = call(&router, "POST", Some(body.clone())).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(parsed["error"]["code"], json!("validation_invalid_request"));
    }
}

#[tokio::test]
async fn a_malformed_json_body_is_still_the_standard_envelope() {
    let res = api_router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/strategies")
                .header("content-type", "application/json")
                .body(Body::from("{not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(res.status().is_client_error());
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["error"]["code"], json!("validation_invalid_request"));
}

#[tokio::test]
async fn a_full_catalog_rejects_a_new_strategy_and_still_accepts_a_known_one() {
    let state = AppState::default().with_strategy_catalog(StrategyCatalog::with_limit(2));
    let router = api_router_with(state);
    assert_eq!(
        compile(&router, manifest("a", "sha256:a")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        compile(&router, manifest("b", "sha256:b")).await.0,
        StatusCode::OK
    );
    let (status, body) = compile(&router, manifest("c", "sha256:c")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["context"]["reason"], json!("catalog_full"));
    assert_eq!(body["error"]["context"]["limit"], json!(2));
    assert_eq!(
        compile(&router, manifest("a", "sha256:a")).await.0,
        StatusCode::OK
    );
    assert_eq!(listed(&router).await.len(), 2);
}

#[tokio::test]
async fn verify_and_compile_agree_on_the_ir() {
    let router = api_router();
    let m = manifest("sma", "sha256:abc");
    let (_, compiled) = compile(&router, m.clone()).await;
    let res = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/strategies/verify")
                .header("content-type", "application/json")
                .body(Body::from(m.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let verified: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(compiled["data"]["ir"], verified["data"]);
}
