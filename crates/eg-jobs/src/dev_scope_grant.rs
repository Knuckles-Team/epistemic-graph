//! A development / test composition root for the job store's scope grant.
//!
//! RF-RULING-004 puts the scope-grant proof authority in the composition root:
//! only it may decide that a principal is entitled to serve the fixed
//! `analytics-jobs` scope. This crate's tests, the replay-determinism property
//! test and `eg-quantum-jobs`' fixtures have no such root, so they use the
//! verifier here.
//!
//! It is compiled ONLY under `cfg(test)` or the off-by-default
//! `dev-scope-grant` feature, so a production build of `eg-jobs` cannot link it.
//! It is not a permissive stub: it checks the layout, the principal and the
//! proof bytes, so a store opened for the wrong layout or principal still fails
//! closed.

use std::path::Path;

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};

use crate::store::{JobError, JobStore};

/// The one principal a development store serves the `analytics-jobs` scope as.
pub const DEV_PRINCIPAL: &str =
    "principal:sha256:4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c";
pub const DEV_PROOF: &[u8] = b"eg-jobs-dev-scope-grant";

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
        if layout != OwnerLayout::Jobs || principal != DEV_PRINCIPAL || proof != DEV_PROOF {
            return Err("development scope authority rejected".to_string());
        }
        Ok(())
    }
}

/// Open a job store at an exact path the way a composition root would.
pub fn open_dev_store(path: &Path) -> Result<JobStore, JobError> {
    JobStore::open(path, &DevScopeVerifier, DEV_PRINCIPAL, DEV_PROOF)
}

/// Open `{persist_dir}/jobs.redb` the way a composition root would.
pub fn open_dev_store_in_dir(persist_dir: &Path) -> Result<JobStore, JobError> {
    JobStore::open_in_dir(persist_dir, &DevScopeVerifier, DEV_PRINCIPAL, DEV_PROOF)
}
