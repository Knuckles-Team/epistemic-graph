//! A development / test composition root for the SQL store's scope grants.
//!
//! RF-RULING-004 puts the scope-grant proof authority in the composition root:
//! only it may decide that a principal is entitled to serve a given SQL scope.
//! This crate's own tests and integration tests have no such root, so they use
//! the verifier here.
//!
//! It is compiled ONLY under `cfg(test)` or the off-by-default `dev-scope-grant`
//! feature, so a production build of `eg-query` cannot link it. It is not a
//! permissive stub: it checks the layout, the principal and the proof bytes it
//! is given, so a store opened with the wrong layout or principal still fails
//! closed.

use std::sync::Arc;

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_types::mutation_batch::MutationScopeIdentity;

/// The one principal a development SQL store serves its scopes as.
pub const DEV_PRINCIPAL: &str =
    "principal:sha256:2f7d1c5b9e0a4638d5c1b7a9e3f0d2c4b6a8e0f1d3c5b7a9e1f3d5c7b9a1e3f5";
pub const DEV_PROOF: &[u8] = b"eg-query-dev-scope-grant";

pub struct DevScopeVerifier;

impl ScopeGrantVerifier for DevScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        _identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::Sql || principal != DEV_PRINCIPAL || proof != DEV_PROOF {
            return Err("development scope authority rejected".to_string());
        }
        Ok(())
    }
}

/// The verifier a development caller hands to `TableStore::open*`.
pub fn dev_verifier() -> Arc<dyn ScopeGrantVerifier> {
    Arc::new(DevScopeVerifier)
}
