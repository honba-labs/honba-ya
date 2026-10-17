#![allow(missing_docs)]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Capability manifest describing what features are available.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CapabilityManifest {
    /// Crates compiled in.
    pub crates: Vec<String>,
    /// Market packs available.
    pub market_packs: Vec<String>,
    /// Endpoints in the contract, as `METHOD /path` in registry order.
    pub endpoints: Vec<String>,
    /// The subset of `endpoints` that currently answer 501 `not_implemented`.
    #[serde(default)]
    pub not_implemented: Vec<String>,
    /// Toolsets available (strategies, indicators, etc.).
    pub toolsets: Vec<String>,
    /// Registered adapters.
    pub adapters: Vec<String>,
    /// Features flags.
    pub features: BTreeMap<String, bool>,
}

/// Top-level capabilities response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Capabilities {
    /// Capability manifest.
    pub capabilities: CapabilityManifest,
}
