use serde_json::json;

use crate::CapabilityManifest;

#[test]
fn a_manifest_from_an_older_server_without_not_implemented_still_parses() {
    let manifest: CapabilityManifest = serde_json::from_value(json!({
        "crates": [], "market_packs": [], "endpoints": ["GET /health"],
        "toolsets": [], "adapters": [], "features": {}
    }))
    .unwrap();
    assert!(manifest.not_implemented.is_empty());
}
