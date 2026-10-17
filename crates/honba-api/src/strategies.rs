//! `POST /strategies` and `GET /strategies`: compile a manifest and keep it.
//!
//! "Compile" is the same step as `POST /strategies/verify` ([`verify_strategy`]): a manifest in,
//! its [`StrategyIr`](honba_strategy::StrategyIr) out. What this adds is a session-scoped catalog, so a later request (a
//! backtest naming `strategy`) can refer to the result by a stable id.
//!
//! Everything here is pure. The catalog is a plain value; the transport decides how to share it
//! (the REST crate keeps one behind a lock). Nothing is persisted: a catalog lives as long as the
//! process that holds it.
//!
//! Capacity: a catalog holds at most [`MAX_COMPILED_STRATEGIES`] strategies. A new strategy past
//! the limit is rejected (422, `reason = catalog_full`) rather than evicting an older one, so an
//! id a caller was given never silently disappears. Re-submitting a manifest already held is not
//! new and always succeeds.

use std::collections::BTreeMap;

use honba_messages::{ErrorCode, ErrorDetail};
use honba_strategy::StrategyManifest;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::requests::StrategiesRequest;
use crate::responses::{CompiledStrategy, StrategiesResponse};
use crate::verify::verify_strategy;

/// Default capacity of a [`StrategyCatalog`].
pub const MAX_COMPILED_STRATEGIES: usize = 1_000;

/// The compiled strategies of one process, keyed by content id (so ordered by id).
#[derive(Clone, Debug)]
pub struct StrategyCatalog {
    by_id: BTreeMap<String, CompiledStrategy>,
    limit: usize,
}

impl Default for StrategyCatalog {
    fn default() -> Self {
        Self::with_limit(MAX_COMPILED_STRATEGIES)
    }
}

impl StrategyCatalog {
    /// An empty catalog that holds at most `limit` strategies.
    pub fn with_limit(limit: usize) -> Self {
        Self {
            by_id: BTreeMap::new(),
            limit,
        }
    }

    /// Number of strategies held.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// True when nothing has been compiled yet.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// The capacity this catalog was built with.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The strategy stored under `id`, if any.
    pub fn get(&self, id: &str) -> Option<&CompiledStrategy> {
        self.by_id.get(id)
    }
}

/// Reads a `POST /strategies` body.
///
/// `code` or `source` is refused with `reason = source_unsupported` before anything else, so a
/// caller sending author code learns why instead of seeing a missing-field message. Any other
/// malformed body is a plain `validation_invalid_request`.
pub fn parse_compile_request(body: Value) -> Result<StrategiesRequest, ErrorDetail> {
    if let Some(object) = body.as_object() {
        if object.contains_key("code") || object.contains_key("source") {
            return Err(ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                "compiling source code is not supported; submit a strategy manifest",
            )
            .with_context(json!({"reason": "source_unsupported"})));
        }
    }
    serde_json::from_value(body)
        .map_err(|e| ErrorDetail::new(ErrorCode::ValidationInvalidRequest, e.to_string()))
}

/// Verifies `manifest`, stores the compiled strategy and returns it.
///
/// The id is the SHA-256 of the manifest's canonical JSON, so the same manifest gets the same id
/// in every process, and two manifests that merely share a `source_hash` do not collide. A
/// manifest already held returns the stored entry unchanged. A manifest that does not verify
/// returns [`verify_strategy`]'s error and stores nothing.
pub fn compile_strategy(
    catalog: &mut StrategyCatalog,
    manifest: StrategyManifest,
) -> Result<CompiledStrategy, ErrorDetail> {
    let ir = verify_strategy(manifest)?;
    let id = content_id(&ir.manifest)?;
    if let Some(known) = catalog.by_id.get(&id) {
        return Ok(known.clone());
    }
    if catalog.by_id.len() >= catalog.limit {
        return Err(ErrorDetail::new(
            ErrorCode::ValidationInvalidRequest,
            format!(
                "the strategy catalog is full ({} strategies)",
                catalog.limit
            ),
        )
        .with_context(json!({"reason": "catalog_full", "limit": catalog.limit})));
    }
    let compiled = CompiledStrategy { id: id.clone(), ir };
    catalog.by_id.insert(id, compiled.clone());
    Ok(compiled)
}

/// Every strategy held, ordered by id.
pub fn list_strategies(catalog: &StrategyCatalog) -> StrategiesResponse {
    StrategiesResponse {
        strategies: catalog.by_id.values().cloned().collect(),
    }
}

fn content_id(manifest: &StrategyManifest) -> Result<String, ErrorDetail> {
    // Struct fields serialize in declaration order and `schedules` is a `BTreeMap`, so the
    // bytes are canonical.
    let bytes = serde_json::to_vec(manifest)
        .map_err(|e| ErrorDetail::new(ErrorCode::InternalError, e.to_string()))?;
    let digest = Sha256::digest(&bytes);
    let mut id = String::from("sha256:");
    for byte in digest {
        id.push_str(&format!("{byte:02x}"));
    }
    Ok(id)
}
