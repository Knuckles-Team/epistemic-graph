//! Held Blob-CAS references: every array a record depends on must already be
//! a committed blob of exactly the declared length, owned by the caller.
//!
//! The caller uploads (`BlobBegin`/`BlobChunkPut`/`BlobCommit`) and so holds
//! its own owner reference before it commits the record; the engine checks
//! that reference here rather than trusting the digest. A build without the
//! blob store cannot hold anything, so it fails closed.

use eg_types::policy_evolution::{HeldBlobRef, PolicyEvolutionRecord, PolicyRefusal};

use super::*;

/// Every held reference `record` carries.
fn held_blobs(record: &PolicyEvolutionRecord) -> Vec<&HeldBlobRef> {
    match record {
        PolicyEvolutionRecord::Capability { .. }
        | PolicyEvolutionRecord::ModelPolicyVersion { .. } => Vec::new(),
        PolicyEvolutionRecord::Capture { record } => record.held_blobs(),
        PolicyEvolutionRecord::TrainingRun { record } => record.log.iter().collect(),
        PolicyEvolutionRecord::PolicyEvaluation { record } => record.report.iter().collect(),
    }
}

fn blob_missing(blob: &HeldBlobRef) -> PolicyRefusal {
    PolicyRefusal::BlobMissing(blob.digest.to_hex())
}

/// Check every held reference against the Blob CAS.
#[cfg(feature = "blob")]
pub(super) async fn verify_held_blobs(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    record: &PolicyEvolutionRecord,
) -> Result<(), PolicyRefusal> {
    let blobs: Vec<HeldBlobRef> = held_blobs(record).into_iter().copied().collect();
    if blobs.is_empty() {
        return Ok(());
    }
    let carrier = crate::server::access::CarrierAuthority::from_verified(verified)
        .map_err(PolicyRefusal::InvalidRecord)?;
    let owner_scope = carrier.owner_scope().to_string();
    let store = timed_read(state)
        .await
        .blob
        .as_ref()
        .map(|cursors| Arc::clone(&cursors.store))
        .ok_or_else(|| blob_missing(&blobs[0]))?;
    tokio::task::spawn_blocking(move || {
        blobs.iter().try_for_each(|blob| {
            let manifest = store
                .get_manifest(&blob.digest.to_hex())
                .map_err(|_| blob_missing(blob))?;
            let held = manifest.is_some_and(|manifest| {
                manifest.owner_scope == owner_scope && manifest.len == blob.length
            });
            held.then_some(()).ok_or_else(|| blob_missing(blob))
        })
    })
    .await
    .map_err(|error| PolicyRefusal::InvalidRecord(format!("blob check failed: {error}")))?
}

/// Without the blob store no reference can be held: refuse any record that
/// names one.
#[cfg(not(feature = "blob"))]
pub(super) async fn verify_held_blobs(
    _state: &Arc<RwLock<ServerState>>,
    _verified: &VerifiedRequestContext,
    record: &PolicyEvolutionRecord,
) -> Result<(), PolicyRefusal> {
    match held_blobs(record).first() {
        Some(blob) => Err(blob_missing(blob)),
        None => Ok(()),
    }
}
