//! A development / test composition root for the time-series store's scope grants.
//!
//! RF-RULING-004 puts the scope-grant proof authority in the composition root:
//! only it may decide that a principal is entitled to serve a given series scope.
//! The benchmark binary and this crate's own tests have no such root, so they use
//! the verifier here.
//!
//! It is compiled ONLY under `cfg(test)` or the off-by-default `dev-scope-grant`
//! feature, so a production build of `eg-tsdb` cannot link it. It is not a
//! permissive stub: it checks the layout, the principal and the proof bytes it is
//! given, so a store opened with the wrong layout or principal still fails closed.

use std::path::Path;
use std::sync::Arc;

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};

use crate::store::{SeriesStore, TsError};

/// The one principal a development store serves its series scopes as.
pub const DEV_PRINCIPAL: &str =
    "principal:sha256:9c1f0e6b2a7d4c3e8f5b0a1d2c3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e";
pub const DEV_PROOF: &[u8] = b"eg-tsdb-dev-scope-grant";

pub struct DevScopeVerifier;

impl ScopeGrantVerifier for DevScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        _identity: &eg_types::MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::TimeSeries || principal != DEV_PRINCIPAL || proof != DEV_PROOF {
            return Err("development scope authority rejected".to_string());
        }
        Ok(())
    }
}

/// Open a series store the way a composition root would.
pub fn open_dev_store(path: &Path) -> Result<SeriesStore, TsError> {
    SeriesStore::open(path, Arc::new(DevScopeVerifier), DEV_PRINCIPAL, DEV_PROOF)
}
