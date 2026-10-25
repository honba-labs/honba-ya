//! OpenAPI 3.1 schema generation for the Honba REST API.
//!
//! Generates a comprehensive OpenAPI 3.1 document describing all REST endpoints,
//! request/response schemas, authentication schemes, and error responses.
//! Uses `schemars` for JSON Schema generation from Rust types.

use axum::response::IntoResponse;
use honba_core::{
    InstrumentId, OrderSide, OrderStatus, OrderType, Price, Quantity, TimeInForce,
};
use honba_mon::{RiskLimits, RiskRefusal};
use serde::{Deserialize, Serialize};
use schemars::schema::*;
use schemars::schema_for;
use std::collections::BTreeMap;

/// OpenAPI 3.1 info block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiInfo {
    pub title: String,
    pub version: String,
    pub description: String,
}

/// OpenAPI 3.1 server object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiServer {
    pub url: String,
    pub description: String,
}

/// OpenAPI 3.1 security scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SecurityScheme {
    Http { scheme: String, bearer_format: Option<String> },
    ApiKey { name: String, in_: String },
}

/// OpenAPI 3.1 security requirement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityRequirement {
    #[serde(flatten)]
    pub scheme: BTreeMap<String, Vec<String>>,
}

/// Complete OpenAPI 3.1 document for the Honba REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenApiDocument {
    pub openapi: String,
    pub info: OpenApiInfo,
    pub servers: Vec<OpenApiServer>,
    pub paths: BTreeMap<String, PathItem>,
    pub components: Components,
    pub tags: Vec<Tag>,
}

/// A single path item in the OpenAPI document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PathItem {
    pub get: Option<Operation>,
    pub post: Option<Operation>,
    pub put: Option<Operation>,
    pub delete: Option<Operation>,
}

/// An OpenAPI operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub tags: Vec<String>,
    pub summary: String,
    pub operation_id: String,
    pub parameters: Vec<Parameter>,
    pub request_body: Option<RequestBody>,
    pub responses: BTreeMap<String, Response>,
    pub security: Vec<SecurityRequirement>,
}

/// An OpenAPI parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameter {
    pub name: String,
    pub r#in: String,
    pub required: bool,
    pub schema: Schema,
}

/// An OpenAPI request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestBody {
    pub required: bool,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI media type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaType {
    pub schema: Schema,
}

/// An OpenAPI response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub description: String,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub name: String,
    pub description: String,
}

/// OpenAPI components (schemas, security schemes, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Components {
    pub schemas: BTreeMap<String, Schema>,
    pub security_schemes: BTreeMap<String, SecurityScheme>,
}

impl OpenApiDocument {
    /// Generate the complete OpenAPI 3.1 document for the Honba REST API.
    pub fn generate() -> Self {
        let mut paths = BTreeMap::new();

        // Market data endpoints
        paths.insert(
            "/instruments".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["market".to_string()],
                    summary: "List all instruments".to_string(),
                    operation_id: "getInstruments".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "List of instruments".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType { schema: schema_for!(Vec<InstrumentId>).schema.into() },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                delete: None,
            },
        );

        paths.insert(
            "/quotes".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["market".to_string()],
                    summary: "Get latest quotes".to_string(),
                    operation_id: "getQuotes".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Latest quotes".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType { schema: schema_for!(serde_json::Value).schema.into() },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                delete: None,
            },
        );

        paths.insert(
            "/orders".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "List orders".to_string(),
                    operation_id: "getOrders".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "List of orders".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType { schema: schema_for!(serde_json::Value).schema.into() },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
                post: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "Place a new order".to_string(),
                    operation_id: "postOrders".to_string(),
                    parameters: vec![],
                    request_body: Some(RequestBody {
                        required: true,
                        content: BTreeMap::from([(
                            "application/json".to_string(),
                            MediaType {
                                schema: schema_for!(serde_json::Value).schema.into(),
                            },
                        )]),
                    }),
                    responses: BTreeMap::from([
                        (
                            "200".to_string(),
                            Response {
                                description: "Order placed".to_string(),
                                content: BTreeMap::from([(
                                    "application/json".to_string(),
                                    MediaType {
                                        schema: schema_for!(serde_json::Value).schema.into(),
                                    },
                                )]),
                            },
                        ),
                        (
                            "422".to_string(),
                            Response {
                                description: "Risk refusal or validation error".to_string(),
                                content: BTreeMap::from([(
                                    "application/json".to_string(),
                                    MediaType {
                                        schema: schema_for!(serde_json::Value).schema.into(),
                                    },
                                )]),
                            },
                        ),
                    ]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
                put: None,
                delete: None,
            },
        );

        paths.insert(
            "/orders/{id}".to_string(),
            PathItem {
                get: None,
                post: None,
                put: None,
                delete: Some(Operation {
                    tags: vec!["trading".to_string()],
                    summary: "Cancel an order".to_string(),
                    operation_id: "deleteOrder".to_string(),
                    parameters: vec![Parameter {
                        name: "id".to_string(),
                        r#in: "path".to_string(),
                        required: true,
                        schema: schema_for!(String).schema.into(),
                    }],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Order cancelled".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value).schema.into(),
                                },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["trade".to_string()])]),
                    }],
                }),
            },
        );

        paths.insert(
            "/portfolio".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["portfolio".to_string()],
                    summary: "Get portfolio".to_string(),
                    operation_id: "getPortfolio".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Portfolio state".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType { schema: schema_for!(serde_json::Value).schema.into() },
                            )]),
                        },
                    )]),
                    security: vec![SecurityRequirement {
                        scheme: BTreeMap::from([("Bearer".to_string(), vec!["read".to_string()])]),
                    }],
                }),
                post: None,
                put: None,
                delete: None,
            },
        );

        paths.insert(
            "/schema".to_string(),
            PathItem {
                get: Some(Operation {
                    tags: vec!["meta".to_string()],
                    summary: "Get OpenAPI schema".to_string(),
                    operation_id: "getSchema".to_string(),
                    parameters: vec![],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "OpenAPI 3.1 schema".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType { schema: schema_for!(serde_json::Value).schema.into() },
                            )]),
                        },
                    )]),
                    security: vec![],
                }),
                post: None,
                put: None,
                delete: None,
            },
        );

        let mut schemas = BTreeMap::new();
        schemas.insert(
            "InstrumentId".to_string(),
            schema_for!(InstrumentId).schema.into(),
        );
        schemas.insert("OrderSide".to_string(), schema_for!(OrderSide).schema.into());
        schemas.insert(
            "OrderStatus".to_string(),
            schema_for!(OrderStatus).schema.into(),
        );
        schemas.insert("OrderType".to_string(), schema_for!(OrderType).schema.into());
        schemas.insert("Price".to_string(), schema_for!(Price).schema.into());
        schemas.insert("Quantity".to_string(), schema_for!(Quantity).schema.into());
        schemas.insert(
            "TimeInForce".to_string(),
            schema_for!(TimeInForce).schema.into(),
        );
        schemas.insert(
            "RiskLimits".to_string(),
            schema_for!(RiskLimits).schema.into(),
        );
        schemas.insert(
            "RiskRefusal".to_string(),
            schema_for!(RiskRefusal).schema.into(),
        );

        let mut security_schemes = BTreeMap::new();
        security_schemes.insert(
            "Bearer".to_string(),
            SecurityScheme::Http {
                scheme: "bearer".to_string(),
                bearer_format: Some("JWT".to_string()),
            },
        );

        Self {
            openapi: "3.1.0".to_string(),
            info: OpenApiInfo {
                title: "Honba REST API".to_string(),
                version: honba_api::API_VERSION.to_string(),
                description: "Honba trading platform REST API with JWT authentication and OpenAPI 3.1".to_string(),
            },
            servers: vec![OpenApiServer {
                url: "http://localhost:8080".to_string(),
                description: "Honba API server".to_string(),
            }],
            paths,
            components: Components {
                schemas,
                security_schemes,
            },
            tags: vec![
                Tag {
                    name: "market".to_string(),
                    description: "Market data endpoints (instruments, quotes, bars, depth)".to_string(),
                },
                Tag {
                    name: "trading".to_string(),
                    description: "Trading endpoints (orders, positions)".to_string(),
                },
                Tag {
                    name: "portfolio".to_string(),
                    description: "Portfolio endpoints (holdings, accounts, trades)".to_string(),
                },
                Tag {
                    name: "meta".to_string(),
                    description: "Meta endpoints (schema, health, capabilities)".to_string(),
                },
            ],
        }
    }

    /// Generate the OpenAPI 3.1 document for v1 (legacy & supported).
    pub fn generate_v1() -> Self {
        let mut doc = Self::generate();
        doc.info.title = "Honba REST API v1".to_string();
        doc.info.version = "1.0.0".to_string();
        doc.info.description = "Honba trading platform REST API v1 (Supported for legacy software)".to_string();
        doc.servers = vec![
            OpenApiServer {
                url: "/api/v1".to_string(),
                description: "Honba REST API v1 (Supported for legacy software)".to_string(),
            },
            OpenApiServer {
                url: "/".to_string(),
                description: "Legacy un-prefixed root (Deprecated)".to_string(),
            },
        ];
        doc
    }

    /// Generate the OpenAPI 3.1 document for v2 (latest & recommended).
    pub fn generate_v2() -> Self {
        let mut doc = Self::generate();
        doc.info.title = "Honba REST API v2".to_string();
        doc.info.version = "2.0.0".to_string();
        doc.info.description = "Honba trading platform REST API v2 (Latest & Recommended)".to_string();
        doc.servers = vec![
            OpenApiServer {
                url: "/api/v2".to_string(),
                description: "Honba REST API v2 (Latest & Recommended)".to_string(),
            },
        ];
        doc
    }
}

/// Handler for `GET /schema/openapi/v1.json`.
pub async fn get_openapi_v1() -> axum::response::Response {
    let doc = OpenApiDocument::generate_v1();
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&doc).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /schema/openapi/v2.json`.
pub async fn get_openapi_v2() -> axum::response::Response {
    let doc = OpenApiDocument::generate_v2();
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&doc).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /schema/openapi`.
pub async fn get_openapi_index() -> axum::response::Response {
    let index = serde_json::json!({
        "openapi_versions": [
            {
                "version": "v1",
                "status": "supported_legacy",
                "url": "/schema/openapi/v1.json",
                "description": "Honba REST API v1 (Supported for legacy software)"
            },
            {
                "version": "v2",
                "status": "latest_recommended",
                "url": "/schema/openapi/v2.json",
                "description": "Honba REST API v2 (Latest & Recommended)"
            }
        ],
        "latest": "v2",
        "documentation_url": "/docs"
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string_pretty(&index).unwrap_or_default(),
    )
        .into_response()
}

/// Handler for `GET /docs` and `GET /swagger-ui`.
pub async fn get_swagger_ui() -> axum::response::Html<&'static str> {
    axum::response::Html(SWAGGER_UI_HTML)
}

/// Swagger UI HTML page with multi-version selector.
pub const SWAGGER_UI_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Honba REST API Documentation</title>
  <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    body { margin: 0; background: #fafafa; font-family: sans-serif; }
    .topbar { display: block; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        urls: [
          { url: "/schema/openapi/v2.json", name: "v2 (Latest & Recommended)" },
          { url: "/schema/openapi/v1.json", name: "v1 (Supported / Legacy)" }
        ],
        "urls.primaryName": "v2 (Latest & Recommended)",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>"#;
