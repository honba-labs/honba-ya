use honba_api::ENDPOINTS;

use crate::{capability_manifest, NOT_IMPLEMENTED_ENDPOINTS};

#[test]
fn the_manifest_is_derived_from_the_registry() {
    let manifest = capability_manifest();
    assert_eq!(manifest.endpoints.len(), ENDPOINTS.len());
    assert_eq!(
        manifest.endpoints[0],
        format!("{} {}", ENDPOINTS[0].0, ENDPOINTS[0].1)
    );
    assert_eq!(
        manifest.not_implemented.len(),
        NOT_IMPLEMENTED_ENDPOINTS.len()
    );
}

#[test]
fn every_not_implemented_route_is_in_the_registry() {
    for route in NOT_IMPLEMENTED_ENDPOINTS {
        assert!(
            ENDPOINTS.contains(route),
            "{route:?} is not a registry endpoint"
        );
    }
}
