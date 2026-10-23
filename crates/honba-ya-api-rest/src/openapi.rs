//! OpenAPI 3.1 schema generation for the Honba REST API.
//!
//! Generates a comprehensive OpenAPI 3.1 document describing all REST endpoints,
//! request/response schemas, authentication schemes, and error responses.
//! Uses `schemars` for JSON Schema generation from Rust types.

use honba_core::{
    InstrumentId, OrderSide, OrderStatus, OrderType, Price, Quantity, TimeInForce,
};
use honba_mon::{RiskLimits, RiskRefusal};
use serde::{Deserialize, Serialize};
use serde_json::json;
use schemars::schema::*;
use schemars::{JsonSchema, schema_for};
use std::collections::BTreeMap;

/// OpenAPI 3.1 info block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OpenApiInfo {
    pub title: String,
    pub version: String,
    pub description: String,
}

/// OpenAPI 3.1 server object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OpenApiServer {
    pub url: String,
    pub description: String,
}

/// OpenAPI 3.1 security scheme.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SecurityScheme {
    Http { scheme: String, bearer_format: Option<String> },
    ApiKey { name: String, in_: String },
}

/// OpenAPI 3.1 security requirement.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SecurityRequirement {
    #[serde(flatten)]
    pub scheme: BTreeMap<String, Vec<String>>,
}

/// Complete OpenAPI 3.1 document for the Honba REST API.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OpenApiDocument {
    pub openapi: String,
    pub info: OpenApiInfo,
    pub servers: Vec<OpenApiServer>,
    pub paths: BTreeMap<String, PathItem>,
    pub components: Components,
    pub tags: Vec<Tag>,
}

/// A single path item in the OpenAPI document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PathItem {
    pub get: Option<Operation>,
    pub post: Option<Operation>,
    pub put: Option<Operation>,
    pub delete: Option<Operation>,
}

/// An OpenAPI operation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Parameter {
    pub name: String,
    pub r#in: String,
    pub required: bool,
    pub schema: Schema,
}

/// An OpenAPI request body.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestBody {
    pub required: bool,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI media type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaType {
    pub schema: Schema,
}

/// An OpenAPI response.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub description: String,
    pub content: BTreeMap<String, MediaType>,
}

/// An OpenAPI tag.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Tag {
    pub name: String,
    pub description: String,
}

/// OpenAPI components (schemas, security schemes, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
                                MediaType { schema: schema_for!(Vec<InstrumentId>) },
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
                                MediaType { schema: schema_for!(serde_json::Value) },
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
                                MediaType { schema: schema_for!(serde_json::Value) },
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
                                schema: schema_for!(serde_json::Value),
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
                                        schema: schema_for!(serde_json::Value),
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
                                        schema: schema_for!(serde_json::Value),
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
                        schema: schema_for!(String),
                    }],
                    request_body: None,
                    responses: BTreeMap::from([(
                        "200".to_string(),
                        Response {
                            description: "Order cancelled".to_string(),
                            content: BTreeMap::from([(
                                "application/json".to_string(),
                                MediaType {
                                    schema: schema_for!(serde_json::Value),
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
                                MediaType { schema: schema_for!(serde_json::Value) },
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
                                MediaType { schema: schema_for!(serde_json::Value) },
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
            schema_for!(InstrumentId),
        );
        schemas.insert("OrderSide".to_string(), schema_for!(OrderSide));
        schemas.insert(
            "OrderStatus".to_string(),
            schema_for!(OrderStatus),
        );
        schemas.insert("OrderType".to_string(), schema_for!(OrderType));
        schemas.insert("Price".to_string(), schema_for!(Price));
        schemas.insert("Quantity".to_string(), schema_for!(Quantity));
        schemas.insert(
            "TimeInForce".to_string(),
            schema_for!(TimeInForce),
        );
        schemas.insert(
            "RiskLimits".to_string(),
            schema_for!(RiskLimits),
        );
        schemas.insert(
            "RiskRefusal".to_string(),
            schema_for!(RiskRefusal),
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
}
