//! Offline, read-and-verify first phase of the per-tenant semantic owner lift.
//!
//! The returned evidence is deliberately not an activation token. A v3 writer
//! must copy *all* owner, scope-binding, replay-ledger and outbox tables, then
//! prove its destination against these exact source fingerprints while holding
//! the same engine lease. Ordinary v2 opens remain fenced by the v3 marker.

use super::{
    legacy_semantic_owner_dir, sanitize_owner_segment, tenant_semantic_migration_fence_file,
    tenant_semantic_owner_file,
};
use eg_storage::{
    adopt_recovery, layout_digest_hex, merge_semantic_owner_files, open_read_only, open_recovery,
    prove_scope_bindings_reanchored_read_only, prove_semantic_owner_partition_read_only,
    prove_semantic_owner_union_read_only, strict_recovery_evidence,
    strict_recovery_evidence_read_only, OwnerLayout, PhysicalStoreIdentity, StrictRecoveryEvidence,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAX_BINDINGS: usize = 256;

#[derive(Debug)]
pub(super) struct InspectedBindingOwner {
    pub binding_id: String,
    pub source_file: PathBuf,
    pub bytes: u64,
    pub evidence: StrictRecoveryEvidence,
}

#[derive(Debug)]
pub(super) struct TenantOwnerUpgradeInspection {
    pub tenant_id: String,
    pub sources: Vec<InspectedBindingOwner>,
    pub total_bytes: u64,
    /// Domain-separated digest of the complete, ordered source census.
    pub source_census_digest: [u8; 32],
    /// The destination is reserved; this phase never opens or creates it.
    pub destination: PathBuf,
}

#[derive(Debug)]
pub(super) struct TenantOwnerCandidate {
    pub candidate: PathBuf,
    pub source_census_digest: [u8; 32],
    pub source_file_sha256: [u8; 32],
    pub copied_file_sha256: [u8; 32],
    pub target_evidence: StrictRecoveryEvidence,
}

#[derive(Debug)]
pub(super) struct TenantOwnerMergedCandidate {
    pub candidate: PathBuf,
    pub source_census_digest: [u8; 32],
    pub source_file_sha256: Vec<[u8; 32]>,
    pub target_evidence: StrictRecoveryEvidence,
}

/// Offline proof that the canonical v3 name is installed and all retained v2
/// rows reconcile to it. This is not a serving activation receipt.
#[derive(Debug)]
pub(super) struct TenantOwnerPromotion {
    pub destination: PathBuf,
    pub source_census_digest: [u8; 32],
    pub target_evidence: StrictRecoveryEvidence,
}

/// EG-derived half of `rf019-tenant-activation-claim/v1`. Global signed
/// receipt IDs/digests are deliberately absent: the checker must supply those
/// from its fixed trust authority, never from this owner observer or a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VerifiedTenantMigrationObservation {
    pub tenant_id: String,
    pub binding_ids: Vec<String>,
    pub owner_path: PathBuf,
    pub owner_device: u64,
    pub owner_inode: u64,
    pub owner_size: u64,
    pub owner_sha256: String,
    pub owner_layout_sha256: String,
    pub source_census_sha256: String,
    pub migration_proof_sha256: String,
}

/// Inspect every declared v2 binding under an exclusive engine lock.
///
/// `binding_ids` must be the complete catalog set. The directory census
/// refuses an omitted owner or an unexpected file; a caller cannot silently
/// migrate only a subset. `max_total_bytes` bounds the amount of live source
/// state inspected. The engine must be stopped because it holds this lock for
/// its lifetime. No source is modified by this function's own logic.
pub(super) fn inspect_tenant_owner_upgrade(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_total_bytes: u64,
) -> Result<TenantOwnerUpgradeInspection, String> {
    // This is the same process-wide engine.lock as normal EG startup. Keep it
    // until the final source fingerprint is captured. The later copy/activation
    // phase must take and retain it through the destination rename too.
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    inspect_under_lease(persist_dir, tenant, binding_ids, max_total_bytes)
}

fn inspect_under_lease(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_total_bytes: u64,
) -> Result<TenantOwnerUpgradeInspection, String> {
    inspect_under_lease_with_destination(persist_dir, tenant, binding_ids, max_total_bytes, false)
}

fn inspect_under_lease_with_destination(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_total_bytes: u64,
    destination_installed: bool,
) -> Result<TenantOwnerUpgradeInspection, String> {
    let unique = validate_request(tenant, binding_ids, max_total_bytes)?;
    let destination =
        validate_directory_census(persist_dir, tenant, binding_ids, destination_installed)?;

    let mut sources = Vec::with_capacity(binding_ids.len());
    let mut total_bytes = 0_u64;
    for binding in unique {
        let source = inspect_binding(persist_dir, tenant, binding)?;
        total_bytes = total_bytes
            .checked_add(source.bytes)
            .ok_or_else(|| "semantic upgrade byte count overflowed".to_string())?;
        if total_bytes > max_total_bytes {
            return Err("semantic upgrade source byte budget exceeded".to_string());
        }
        sources.push(source);
    }
    let source_census_digest = census_digest(tenant, &sources);
    Ok(TenantOwnerUpgradeInspection {
        tenant_id: tenant.to_string(),
        sources,
        total_bytes,
        source_census_digest,
        destination,
    })
}

/// Build a proved *private candidate* for a tenant with exactly one v2
/// binding. This does not install the canonical v3 path or change normal
/// serving. A tenant with multiple v2 files needs a separate ledger-key,
/// replay and outbox merge proof and is refused here.
pub(super) fn copy_single_binding_tenant_candidate(
    persist_dir: &Path,
    expected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
) -> Result<TenantOwnerCandidate, String> {
    if expected.sources.len() != 1 {
        return Err("semantic tenant copy requires exactly one v2 binding".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let bindings = vec![expected.sources[0].binding_id.clone()];
    let current =
        inspect_under_lease(persist_dir, &expected.tenant_id, &bindings, max_total_bytes)?;
    if current.source_census_digest != expected.source_census_digest
        || current.destination != expected.destination
    {
        return Err("semantic tenant source changed after inspection".to_string());
    }
    let source = &current.sources[0];
    let candidate = current.destination.with_extension("candidate.redb");
    require_candidate_headroom(persist_dir, current.total_bytes)?;
    let mut cleanup = CandidateCleanup::new(candidate.clone());
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&candidate)
        .map_err(|error| error.to_string())?;
    cleanup.armed = true;
    copy_file_contents(&source.source_file, &mut output, source.bytes)?;
    drop(output);
    let source_file_sha256 = file_sha256(&source.source_file)?;
    let copied_file_sha256 = file_sha256(&candidate)?;
    if source_file_sha256 != copied_file_sha256 {
        return Err("semantic tenant candidate differs from its v2 source bytes".to_string());
    }
    let source_identity = legacy_physical_identity(&current.tenant_id, &source.binding_id)?;
    let validated = open_recovery(
        &candidate,
        source_identity,
        None,
        OwnerLayout::SemanticIndex,
    )?;
    let target_identity = tenant_physical_identity(&current.destination)?;
    let adopted = adopt_recovery(validated, target_identity.clone())?;
    let target_evidence = strict_recovery_evidence(&adopted)?;
    drop(adopted);
    let target = open_read_only(&candidate, None)?;
    let reopened =
        strict_recovery_evidence_read_only(&target, &target_identity, OwnerLayout::SemanticIndex)?;
    if target_evidence != reopened {
        return Err("semantic tenant candidate changed after durable reopen".to_string());
    }
    let source_read = open_read_only(&source.source_file, None)?;
    prove_scope_bindings_reanchored_read_only(&source_read, &target)?;
    drop(source_read);
    drop(target);
    prove_single_binding_copy(&source.evidence, &target_evidence)?;
    let source_after = inspect_binding(persist_dir, &current.tenant_id, &source.binding_id)?;
    if source_after.bytes != source.bytes
        || source_after.evidence != source.evidence
        || file_sha256(&source.source_file)? != source_file_sha256
    {
        return Err("semantic v2 source changed during candidate copy".to_string());
    }
    cleanup.armed = false;
    Ok(TenantOwnerCandidate {
        candidate,
        source_census_digest: current.source_census_digest,
        source_file_sha256,
        copied_file_sha256,
        target_evidence,
    })
}

/// Abort an unactivated candidate after re-proving both source and candidate.
/// The canonical v3 path has never been installed, so this removes only the
/// private copy and leaves the original v2 serving authority intact.
pub(super) fn abort_single_binding_tenant_candidate(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    candidate: &TenantOwnerCandidate,
    max_total_bytes: u64,
) -> Result<(), String> {
    if inspected.sources.len() != 1
        || candidate.source_census_digest != inspected.source_census_digest
        || candidate.candidate != inspected.destination.with_extension("candidate.redb")
    {
        return Err("semantic tenant abort does not match the inspected source".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let binding = inspected.sources[0].binding_id.clone();
    let current = inspect_under_lease(
        persist_dir,
        &inspected.tenant_id,
        &[binding],
        max_total_bytes,
    )?;
    if current.source_census_digest != inspected.source_census_digest
        || file_sha256(&current.sources[0].source_file)? != candidate.source_file_sha256
    {
        return Err("semantic tenant source changed before candidate abort".to_string());
    }
    let target = open_read_only(&candidate.candidate, None)?;
    let target_identity = tenant_physical_identity(&inspected.destination)?;
    let evidence =
        strict_recovery_evidence_read_only(&target, &target_identity, OwnerLayout::SemanticIndex)?;
    if evidence != candidate.target_evidence {
        return Err("semantic tenant candidate changed before abort".to_string());
    }
    prove_single_binding_copy(&current.sources[0].evidence, &evidence)?;
    let source = open_read_only(&current.sources[0].source_file, None)?;
    prove_scope_bindings_reanchored_read_only(&source, &target)?;
    drop(source);
    drop(target);
    std::fs::remove_file(&candidate.candidate).map_err(|error| error.to_string())?;
    Ok(())
}

/// Build a private tenant candidate from two or more independent v2 owners.
/// The kernel refuses every colliding key and proves the union of all owner,
/// replay, ledger and outbox rows, including logical scope-binding identities.
pub(super) fn merge_multi_binding_tenant_candidate(
    persist_dir: &Path,
    expected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
) -> Result<TenantOwnerMergedCandidate, String> {
    if expected.sources.len() < 2 {
        return Err("semantic multi-owner merge requires at least two bindings".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let bindings: Vec<_> = expected
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    let current =
        inspect_under_lease(persist_dir, &expected.tenant_id, &bindings, max_total_bytes)?;
    if current.source_census_digest != expected.source_census_digest
        || current.destination != expected.destination
    {
        return Err("semantic tenant sources changed after inspection".to_string());
    }
    let source_file_sha256: Vec<_> = current
        .sources
        .iter()
        .map(|source| file_sha256(&source.source_file))
        .collect::<Result<_, _>>()?;
    let source_files: Vec<_> = current
        .sources
        .iter()
        .map(|source| {
            Ok((
                source.source_file.clone(),
                legacy_physical_identity(&current.tenant_id, &source.binding_id)?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let candidate = current.destination.with_extension("candidate.redb");
    require_candidate_headroom(persist_dir, current.total_bytes)?;
    match std::fs::symlink_metadata(&candidate) {
        Ok(_) => return Err("semantic tenant candidate already exists".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("semantic tenant candidate is unavailable: {error}")),
    }
    let mut cleanup = CandidateCleanup::new(candidate.clone());
    cleanup.armed = true;
    let merged = merge_semantic_owner_files(
        &source_files,
        &candidate,
        tenant_physical_identity(&current.destination)?,
    )?;
    if merged.sources.len() != current.sources.len()
        || merged
            .sources
            .iter()
            .zip(&current.sources)
            .any(|(copied, source)| copied != &source.evidence)
    {
        return Err("semantic merge source evidence changed during copy".to_string());
    }
    for (source, digest) in current.sources.iter().zip(&source_file_sha256) {
        let rechecked = inspect_binding(persist_dir, &current.tenant_id, &source.binding_id)?;
        if rechecked.evidence != source.evidence
            || rechecked.bytes != source.bytes
            || file_sha256(&source.source_file)? != *digest
        {
            return Err("semantic v2 source changed during tenant merge".to_string());
        }
    }
    cleanup.armed = false;
    Ok(TenantOwnerMergedCandidate {
        candidate,
        source_census_digest: current.source_census_digest,
        source_file_sha256,
        target_evidence: merged.target,
    })
}

fn require_candidate_headroom(persist_dir: &Path, source_bytes: u64) -> Result<(), String> {
    // A redb merge may allocate fresh pages beyond the sum of its inputs.
    // Keep at least 256 MiB of unrelated workspace headroom as well. This
    // bounded check precedes the first candidate file creation.
    let reserve = (source_bytes / 2).max(256 * 1024 * 1024);
    let required = source_bytes
        .checked_add(reserve)
        .ok_or_else(|| "semantic upgrade disk budget overflowed".to_string())?;
    let available = fs4::available_space(persist_dir).map_err(|error| error.to_string())?;
    if available < required {
        return Err("semantic upgrade lacks disk space for a private candidate".to_string());
    }
    Ok(())
}

/// Remove an unactivated multi-binding candidate only after its exact source
/// set and the candidate's reopened recovery evidence still match the copy
/// receipt. Normal v2 serving remains the authority throughout.
pub(super) fn abort_multi_binding_tenant_candidate(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    candidate: &TenantOwnerMergedCandidate,
    max_total_bytes: u64,
) -> Result<(), String> {
    if inspected.sources.len() < 2
        || inspected.sources.len() != candidate.source_file_sha256.len()
        || candidate.source_census_digest != inspected.source_census_digest
        || candidate.candidate != inspected.destination.with_extension("candidate.redb")
    {
        return Err("semantic tenant merge abort has a mismatched receipt".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let bindings: Vec<_> = inspected
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    let current = inspect_under_lease(
        persist_dir,
        &inspected.tenant_id,
        &bindings,
        max_total_bytes,
    )?;
    if current.source_census_digest != inspected.source_census_digest {
        return Err("semantic tenant sources changed before merge abort".to_string());
    }
    for (source, digest) in current.sources.iter().zip(&candidate.source_file_sha256) {
        if file_sha256(&source.source_file)? != *digest {
            return Err("semantic tenant source bytes changed before merge abort".to_string());
        }
    }
    let target = open_read_only(&candidate.candidate, None)?;
    let evidence = strict_recovery_evidence_read_only(
        &target,
        &tenant_physical_identity(&inspected.destination)?,
        OwnerLayout::SemanticIndex,
    )?;
    if evidence != candidate.target_evidence {
        return Err("semantic tenant candidate changed before merge abort".to_string());
    }
    drop(target);
    std::fs::remove_file(&candidate.candidate).map_err(|error| error.to_string())
}

pub(super) fn promote_single_binding_tenant_candidate(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    candidate: &TenantOwnerCandidate,
    max_total_bytes: u64,
) -> Result<TenantOwnerPromotion, String> {
    if inspected.sources.len() != 1
        || candidate.source_census_digest != inspected.source_census_digest
    {
        return Err("semantic singleton promotion has a mismatched source".to_string());
    }
    promote_checked_candidate(
        persist_dir,
        inspected,
        &candidate.candidate,
        &[candidate.source_file_sha256],
        &candidate.target_evidence,
        max_total_bytes,
    )
}

pub(super) fn promote_multi_binding_tenant_candidate(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    candidate: &TenantOwnerMergedCandidate,
    max_total_bytes: u64,
) -> Result<TenantOwnerPromotion, String> {
    if inspected.sources.len() < 2
        || candidate.source_census_digest != inspected.source_census_digest
    {
        return Err("semantic multi-owner promotion has a mismatched source".to_string());
    }
    promote_checked_candidate(
        persist_dir,
        inspected,
        &candidate.candidate,
        &candidate.source_file_sha256,
        &candidate.target_evidence,
        max_total_bytes,
    )
}

/// Install a proved private candidate under the same exclusive engine lease
/// that fences normal startup. Hard-link creation refuses an existing v3 name
/// atomically and preserves the file's device/inode physical root. If the
/// process dies between link and unlink, both names point at the same valid
/// owner; `recover_promoted_tenant_owner` validates and removes the alias.
fn promote_checked_candidate(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    candidate: &Path,
    source_sha256: &[[u8; 32]],
    target_evidence: &StrictRecoveryEvidence,
    max_total_bytes: u64,
) -> Result<TenantOwnerPromotion, String> {
    if source_sha256.len() != inspected.sources.len()
        || candidate != inspected.destination.with_extension("candidate.redb")
    {
        return Err("semantic promotion candidate does not match its source set".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let bindings: Vec<_> = inspected
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    let current = inspect_under_lease(
        persist_dir,
        &inspected.tenant_id,
        &bindings,
        max_total_bytes,
    )?;
    if current.source_census_digest != inspected.source_census_digest
        || current.destination != inspected.destination
    {
        return Err("semantic promotion source changed after candidate copy".to_string());
    }
    let sources = source_files_with_identities(&current)?;
    for (source, expected_sha256) in current.sources.iter().zip(source_sha256) {
        if file_sha256(&source.source_file)? != *expected_sha256 {
            return Err("semantic promotion source bytes changed after candidate copy".to_string());
        }
    }
    require_regular_file(candidate)?;
    let identity = tenant_physical_identity(&current.destination)?;
    let candidate_proof = prove_semantic_owner_union_read_only(&sources, candidate, &identity)?;
    if candidate_proof.target != *target_evidence
        || candidate_proof
            .sources
            .iter()
            .zip(&current.sources)
            .any(|(proven, source)| proven != &source.evidence)
    {
        return Err("semantic promotion candidate or source proof changed".to_string());
    }
    std::fs::File::open(candidate)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    // Persist the authority fence before publishing the v3 name. A crash in
    // this interval leaves the tenant unavailable, never silently back on v2.
    install_migration_fence(persist_dir, &current.tenant_id, &current.destination)?;
    std::fs::hard_link(candidate, &current.destination).map_err(|error| error.to_string())?;
    sync_parent_directory(&current.destination)?;
    // The canonical name now fences v2, even if the unlink or subsequent
    // verification fails. Recovery re-proves the alias instead of guessing.
    std::fs::remove_file(candidate).map_err(|error| error.to_string())?;
    sync_parent_directory(&current.destination)?;
    verify_promoted_under_lease(persist_dir, &current.tenant_id, &bindings, max_total_bytes)
}

/// Restart path after a candidate was linked to the canonical v3 name. The
/// complete catalog set is required again; an omitted v2 directory is refused.
/// This proves migration state only and never enables the serving route.
pub(super) fn recover_promoted_tenant_owner(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_total_bytes: u64,
) -> Result<TenantOwnerPromotion, String> {
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    verify_promoted_under_lease(persist_dir, tenant, binding_ids, max_total_bytes)
}

/// Produce the native migration observation for a checker-owned signer. The
/// only inputs are the tenant/catalog selector and explicit I/O budgets. All
/// digests, row dispositions and owner file facts are recomputed while EG's
/// exclusive engine lease is held; there is no caller-supplied observation.
/// The returned value is evidence, never a serving authorization token.
pub(super) fn observe_promoted_tenant_owner(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_source_bytes: u64,
    max_owner_bytes: u64,
) -> Result<VerifiedTenantMigrationObservation, String> {
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    observe_promoted_tenant_owner_under_existing_lease(
        persist_dir,
        tenant,
        binding_ids,
        max_source_bytes,
        max_owner_bytes,
    )
}

/// Called only during server startup after its process-lifetime engine.lock is
/// held. Recompute the complete source census, row proof and owner fingerprint
/// without recursively acquiring the same exclusive lease.
pub(super) fn observe_promoted_tenant_owner_under_existing_lease(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_source_bytes: u64,
    max_owner_bytes: u64,
) -> Result<VerifiedTenantMigrationObservation, String> {
    if binding_ids.is_empty()
        || binding_ids.len() > 32
        || !claim_token(tenant)
        || binding_ids.iter().any(|binding| !claim_token(binding))
        || max_source_bytes == 0
        || max_source_bytes > 8 * 1024 * 1024 * 1024
        || max_owner_bytes == 0
        || max_owner_bytes > 8 * 1024 * 1024 * 1024
    {
        return Err("semantic activation observation exceeds the closed claim bounds".to_string());
    }
    let promoted = verify_promoted_under_lease(persist_dir, tenant, binding_ids, max_source_bytes)?;
    let current = inspect_under_lease_with_destination(
        persist_dir,
        tenant,
        binding_ids,
        max_source_bytes,
        true,
    )?;
    if current.source_census_digest != promoted.source_census_digest
        || current.destination != promoted.destination
    {
        return Err("semantic activation source changed after promotion proof".to_string());
    }
    for source in &current.sources {
        prove_semantic_owner_partition_read_only(
            &source.source_file,
            &legacy_physical_identity(tenant, &source.binding_id)?,
            tenant,
            &source.binding_id,
        )?;
        for table in [
            "semantic_bindings",
            "mutation_replay_operations",
            "ledger_outbox",
        ] {
            if source
                .evidence
                .tables
                .iter()
                .find(|entry| entry.table_id == table)
                .map_or(true, |entry| entry.rows == 0)
            {
                return Err(format!(
                    "semantic activation source {} lacks a populated {table}",
                    source.binding_id
                ));
            }
        }
    }
    let sources = source_files_with_identities(&current)?;
    let proof = prove_semantic_owner_union_read_only(
        &sources,
        &current.destination,
        &tenant_physical_identity(&current.destination)?,
    )?;
    if proof.target != promoted.target_evidence
        || proof
            .sources
            .iter()
            .zip(&current.sources)
            .any(|(observed, source)| observed != &source.evidence)
    {
        return Err("semantic activation row proof changed during observation".to_string());
    }
    let canonical =
        std::fs::canonicalize(&current.destination).map_err(|error| error.to_string())?;
    if canonical != current.destination {
        return Err("semantic activation owner path is not canonical".to_string());
    }
    if canonical.to_str().is_none() {
        return Err("semantic activation owner path is not UTF-8".to_string());
    }
    let owner = fingerprint_owner_file(&canonical, max_owner_bytes)?;
    let ordered: Vec<_> = current
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    if ordered.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("semantic activation binding set is not sorted and unique".to_string());
    }
    let layout = layout_digest_hex(OwnerLayout::SemanticIndex);
    let mut proof_digest = Sha256::new();
    proof_digest.update(b"eg/rf019-tenant-migration-observation/v1\0");
    hash_field(&mut proof_digest, tenant.as_bytes());
    proof_digest.update((ordered.len() as u64).to_be_bytes());
    for binding in &ordered {
        hash_field(&mut proof_digest, binding.as_bytes());
    }
    hash_field(&mut proof_digest, canonical.as_os_str().as_encoded_bytes());
    proof_digest.update(owner.device.to_be_bytes());
    proof_digest.update(owner.inode.to_be_bytes());
    proof_digest.update(owner.size.to_be_bytes());
    proof_digest.update(owner.sha256);
    proof_digest.update(current.source_census_digest);
    proof_digest.update(proof.row_proof_sha256);
    proof_digest.update(proof.target.fingerprint);
    hash_field(&mut proof_digest, layout.as_bytes());
    Ok(VerifiedTenantMigrationObservation {
        tenant_id: tenant.to_string(),
        binding_ids: ordered,
        owner_path: canonical,
        owner_device: owner.device,
        owner_inode: owner.inode,
        owner_size: owner.size,
        owner_sha256: hex::encode(owner.sha256),
        owner_layout_sha256: layout,
        source_census_sha256: hex::encode(current.source_census_digest),
        migration_proof_sha256: hex::encode(proof_digest.finalize()),
    })
}

fn claim_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(byte))
}

struct OwnerFingerprint {
    device: u64,
    inode: u64,
    size: u64,
    sha256: [u8; 32],
}

#[cfg(target_os = "linux")]
fn fingerprint_owner_file(path: &Path, max_bytes: u64) -> Result<OwnerFingerprint, String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    // Linux O_NOFOLLOW. EG's normal engine lease fences cooperative writers;
    // this descriptor check also refuses a changed/symlinked owner pathname.
    const O_NOFOLLOW: i32 = 0o400000;
    let named_before = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
        .map_err(|error| error.to_string())?;
    let before = file.metadata().map_err(|error| error.to_string())?;
    if !named_before.file_type().is_file()
        || !before.is_file()
        || named_before.nlink() != 1
        || before.nlink() != 1
        || before.len() > max_bytes
    {
        return Err("semantic activation owner file is not a bounded single-link file".to_string());
    }
    let mut digest = Sha256::new();
    let mut count = 0_u64;
    let mut block = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut block).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        count = count
            .checked_add(read as u64)
            .ok_or_else(|| "semantic activation owner byte count overflowed".to_string())?;
        if count > max_bytes {
            return Err("semantic activation owner byte budget exceeded".to_string());
        }
        digest.update(&block[..read]);
    }
    let after = file.metadata().map_err(|error| error.to_string())?;
    let named_after = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    let stable = (
        before.dev(),
        before.ino(),
        before.len(),
        before.mtime(),
        before.mtime_nsec(),
        before.ctime(),
        before.ctime_nsec(),
    ) == (
        after.dev(),
        after.ino(),
        after.len(),
        after.mtime(),
        after.mtime_nsec(),
        after.ctime(),
        after.ctime_nsec(),
    ) && (before.dev(), before.ino(), before.len(), before.nlink())
        == (
            named_after.dev(),
            named_after.ino(),
            named_after.len(),
            named_after.nlink(),
        )
        && named_after.file_type().is_file()
        && (before.dev(), before.ino(), before.len())
            == (named_before.dev(), named_before.ino(), named_before.len());
    if !stable || count != before.len() {
        return Err("semantic activation owner changed while observed".to_string());
    }
    Ok(OwnerFingerprint {
        device: before.dev(),
        inode: before.ino(),
        size: count,
        sha256: digest.finalize().into(),
    })
}

#[cfg(not(target_os = "linux"))]
fn fingerprint_owner_file(_path: &Path, _max_bytes: u64) -> Result<OwnerFingerprint, String> {
    Err("semantic activation owner observation requires Linux no-follow open".to_string())
}

fn verify_promoted_under_lease(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    max_total_bytes: u64,
) -> Result<TenantOwnerPromotion, String> {
    let current = inspect_under_lease_with_destination(
        persist_dir,
        tenant,
        binding_ids,
        max_total_bytes,
        true,
    )?;
    let candidate = current.destination.with_extension("candidate.redb");
    let alias_exists = match std::fs::symlink_metadata(&candidate) {
        Ok(metadata) if metadata.file_type().is_file() => {
            if !same_file_identity(&candidate, &current.destination)? {
                return Err("semantic promotion alias is a different owner file".to_string());
            }
            true
        }
        Ok(_) => return Err("semantic promotion alias has the wrong file type".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(format!("semantic promotion alias is unavailable: {error}")),
    };
    let sources = source_files_with_identities(&current)?;
    let proven = prove_semantic_owner_union_read_only(
        &sources,
        &current.destination,
        &tenant_physical_identity(&current.destination)?,
    )?;
    if proven
        .sources
        .iter()
        .zip(&current.sources)
        .any(|(source, current)| source != &current.evidence)
    {
        return Err("semantic promoted source evidence changed during proof".to_string());
    }
    if alias_exists {
        std::fs::remove_file(&candidate).map_err(|error| error.to_string())?;
        sync_parent_directory(&current.destination)?;
    }
    Ok(TenantOwnerPromotion {
        destination: current.destination,
        source_census_digest: current.source_census_digest,
        target_evidence: proven.target,
    })
}

fn source_files_with_identities(
    inspected: &TenantOwnerUpgradeInspection,
) -> Result<Vec<(PathBuf, PhysicalStoreIdentity)>, String> {
    inspected
        .sources
        .iter()
        .map(|source| {
            Ok((
                source.source_file.clone(),
                legacy_physical_identity(&inspected.tenant_id, &source.binding_id)?,
            ))
        })
        .collect()
}

fn require_regular_file(path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err("semantic promotion candidate is not an ordinary file".to_string())
    }
}

#[cfg(unix)]
fn same_file_identity(first: &Path, second: &Path) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let first = std::fs::metadata(first).map_err(|error| error.to_string())?;
    let second = std::fs::metadata(second).map_err(|error| error.to_string())?;
    Ok(first.dev() == second.dev() && first.ino() == second.ino())
}

#[cfg(not(unix))]
fn same_file_identity(_first: &Path, _second: &Path) -> Result<bool, String> {
    Err("semantic owner promotion requires Unix file identity".to_string())
}

fn sync_parent_directory(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "semantic promotion has no parent directory".to_string())?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn migration_fence_bytes(tenant: &str, destination: &Path) -> Result<Vec<u8>, String> {
    let basename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("semantic migration destination has no UTF-8 basename")?;
    let mut digest = Sha256::new();
    digest.update(b"eg/rf019/tenant-migration-fence/v1\0");
    digest.update((tenant.len() as u64).to_be_bytes());
    digest.update(tenant.as_bytes());
    digest.update((basename.len() as u64).to_be_bytes());
    digest.update(basename.as_bytes());
    Ok(format!(
        "eg/rf019/tenant-migration-fence/v1\n{:x}\n",
        digest.finalize()
    )
    .into_bytes())
}

#[cfg(target_os = "linux")]
fn require_migration_fence(
    persist_dir: &Path,
    tenant: &str,
    destination: &Path,
) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    const O_NOFOLLOW: i32 = 0o400000;
    let path = tenant_semantic_migration_fence_file(persist_dir, tenant);
    let named = std::fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(&path)
        .map_err(|error| error.to_string())?;
    let opened = file.metadata().map_err(|error| error.to_string())?;
    let expected = migration_fence_bytes(tenant, destination)?;
    if !named.file_type().is_file()
        || !opened.is_file()
        || named.nlink() != 1
        || opened.nlink() != 1
        || (named.dev(), named.ino()) != (opened.dev(), opened.ino())
        || opened.len() != expected.len() as u64
    {
        return Err("semantic migration fence identity is invalid".to_string());
    }
    let mut actual = Vec::with_capacity(expected.len());
    file.take((expected.len() + 1) as u64)
        .read_to_end(&mut actual)
        .map_err(|error| error.to_string())?;
    if actual != expected {
        return Err("semantic migration fence content is invalid".to_string());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn require_migration_fence(
    _persist_dir: &Path,
    _tenant: &str,
    _destination: &Path,
) -> Result<(), String> {
    Err("semantic migration fence requires Linux no-follow open".to_string())
}

fn install_migration_fence(
    persist_dir: &Path,
    tenant: &str,
    destination: &Path,
) -> Result<(), String> {
    let path = tenant_semantic_migration_fence_file(persist_dir, tenant);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => return require_migration_fence(persist_dir, tenant, destination),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("semantic migration fence unavailable: {error}")),
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|error| error.to_string())?;
    file.write_all(&migration_fence_bytes(tenant, destination)?)
        .and_then(|()| file.sync_all())
        .map_err(|error| error.to_string())?;
    sync_parent_directory(&path)?;
    require_migration_fence(persist_dir, tenant, destination)
}

fn copy_file_contents(source: &Path, output: &mut std::fs::File, bytes: u64) -> Result<(), String> {
    let mut input = std::fs::File::open(source).map_err(|error| error.to_string())?;
    let copied = std::io::copy(
        &mut (&mut input).take(bytes.saturating_add(1)),
        &mut *output,
    )
    .map_err(|error| error.to_string())?;
    if copied != bytes {
        return Err("semantic v2 source length changed during copy".to_string());
    }
    output.flush().map_err(|error| error.to_string())?;
    output.sync_all().map_err(|error| error.to_string())
}

fn file_sha256(path: &Path) -> Result<[u8; 32], String> {
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut block = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut block).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&block[..read]);
    }
    Ok(hasher.finalize().into())
}

fn legacy_physical_identity(tenant: &str, binding: &str) -> Result<PhysicalStoreIdentity, String> {
    PhysicalStoreIdentity::new(format!(
        "eg-core:semantic-index:v2:{}",
        legacy_store_file_name(tenant, binding)
    ))
}

fn tenant_physical_identity(destination: &Path) -> Result<PhysicalStoreIdentity, String> {
    let filename = destination
        .file_name()
        .ok_or_else(|| "semantic tenant destination has no filename".to_string())?
        .to_string_lossy();
    PhysicalStoreIdentity::new(format!("eg-core:semantic-index:v3:{filename}"))
}

fn prove_single_binding_copy(
    source: &StrictRecoveryEvidence,
    target: &StrictRecoveryEvidence,
) -> Result<(), String> {
    if source.ledger_rows != target.ledger_rows
        || source.owner_rows != target.owner_rows
        || source.tables.len() != target.tables.len()
    {
        return Err("semantic tenant copy changed row totals or table census".to_string());
    }
    for (before, after) in source.tables.iter().zip(&target.tables) {
        if before.table_id != after.table_id || before.rows != after.rows {
            return Err("semantic tenant copy changed a table identity or row count".to_string());
        }
        if !matches!(
            before.table_id.as_str(),
            "mutation_store_root" | "mutation_scope_bindings" | "mutation_owner_manifest"
        ) && before.fingerprint != after.fingerprint
        {
            return Err("semantic tenant copy changed a ledger, outbox or owner row".to_string());
        }
    }
    Ok(())
}

struct CandidateCleanup {
    path: PathBuf,
    armed: bool,
}

impl CandidateCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: false }
    }
}

impl Drop for CandidateCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn validate_request<'a>(
    tenant: &str,
    binding_ids: &'a [String],
    max_total_bytes: u64,
) -> Result<BTreeSet<&'a String>, String> {
    if tenant.is_empty() || binding_ids.is_empty() || binding_ids.len() > MAX_BINDINGS {
        return Err("semantic upgrade requires a tenant and 1..=256 bindings".to_string());
    }
    if max_total_bytes == 0 {
        return Err("semantic upgrade requires a positive source byte budget".to_string());
    }
    let unique: BTreeSet<_> = binding_ids.iter().collect();
    if unique.len() != binding_ids.len() || binding_ids.iter().any(|id| id.is_empty()) {
        return Err("semantic upgrade binding ids must be nonempty and unique".to_string());
    }
    Ok(unique)
}

fn validate_directory_census(
    persist_dir: &Path,
    tenant: &str,
    binding_ids: &[String],
    destination_installed: bool,
) -> Result<PathBuf, String> {
    let destination = tenant_semantic_owner_file(persist_dir, tenant);
    match std::fs::symlink_metadata(&destination) {
        Ok(metadata) if destination_installed && metadata.file_type().is_file() => {}
        Ok(_) => return Err("semantic tenant destination has the wrong state".to_string()),
        Err(error) if !destination_installed && error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "semantic tenant destination is unavailable: {error}"
            ))
        }
    }
    let candidate = destination.with_extension("candidate.redb");
    let fence = tenant_semantic_migration_fence_file(persist_dir, tenant);
    match std::fs::symlink_metadata(&fence) {
        Ok(_) => require_migration_fence(persist_dir, tenant, &destination)?,
        Err(error) if !destination_installed && error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("semantic migration fence unavailable: {error}")),
    }
    match std::fs::symlink_metadata(&candidate) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err("semantic tenant candidate has the wrong file type".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("semantic tenant candidate is unavailable: {error}")),
    }
    let tenant_dir = persist_dir
        .join("semantic-index")
        .join(sanitize_owner_segment(tenant));
    require_directory(&tenant_dir)?;
    let expected: BTreeSet<_> = binding_ids
        .iter()
        .map(|id| legacy_semantic_owner_dir(persist_dir, tenant, id))
        .collect();
    let mut actual: BTreeSet<_> = std::fs::read_dir(&tenant_dir)
        .map_err(|error| error.to_string())?
        .map(|entry| {
            entry
                .map(|item| item.path())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<_, _>>()?;
    // These are the two named stages of this migration, not v2 binding
    // directories. Their presence and type are checked above; recovery later
    // proves a surviving candidate name is the SAME inode as canonical.
    actual.remove(&destination);
    actual.remove(&candidate);
    actual.remove(&fence);
    if actual != expected {
        return Err("semantic upgrade catalog does not cover every v2 owner directory".to_string());
    }
    Ok(destination)
}

fn inspect_binding(
    persist_dir: &Path,
    tenant: &str,
    binding: &str,
) -> Result<InspectedBindingOwner, String> {
    let directory = legacy_semantic_owner_dir(persist_dir, tenant, binding);
    require_directory(&directory)?;
    let filename = legacy_store_file_name(tenant, binding);
    let source_file = directory.join(&filename);
    require_single_regular_file(&directory, &source_file)?;
    let bytes = std::fs::metadata(&source_file)
        .map_err(|error| error.to_string())?
        .len();
    let physical = PhysicalStoreIdentity::new(format!("eg-core:semantic-index:v2:{filename}"))?;
    // The storage kernel validates the exact manifest, replay/outbox ledger
    // and declared owner-table set without opening the file for write.
    let store = open_read_only(&source_file, None)?;
    let evidence =
        strict_recovery_evidence_read_only(&store, &physical, OwnerLayout::SemanticIndex)?;
    Ok(InspectedBindingOwner {
        binding_id: binding.to_string(),
        source_file,
        bytes,
        evidence,
    })
}

fn require_directory(path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err("semantic upgrade source directory is not an ordinary directory".to_string())
    }
}

fn require_single_regular_file(directory: &Path, expected: &Path) -> Result<(), String> {
    let entries: Vec<_> = std::fs::read_dir(directory)
        .map_err(|error| error.to_string())?
        .map(|entry| {
            entry
                .map(|item| item.path())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<_, _>>()?;
    if entries.len() != 1 || entries[0] != expected {
        return Err("semantic upgrade source has an unexpected file".to_string());
    }
    let metadata = std::fs::symlink_metadata(expected).map_err(|error| error.to_string())?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err("semantic upgrade source is not an ordinary file".to_string())
    }
}

fn legacy_store_file_name(tenant: &str, binding: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-file/v1\0");
    hasher.update(tenant.as_bytes());
    hasher.update([0]);
    hasher.update(binding.as_bytes());
    format!("semantic_index-{}.redb", hex::encode(hasher.finalize()))
}

fn census_digest(tenant: &str, sources: &[InspectedBindingOwner]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-owner-source-census/v1\0");
    hash_field(&mut hasher, tenant.as_bytes());
    hasher.update((sources.len() as u64).to_be_bytes());
    for source in sources {
        hash_field(&mut hasher, source.binding_id.as_bytes());
        hasher.update(source.bytes.to_be_bytes());
        hasher.update(source.evidence.fingerprint);
        hasher.update(source.evidence.ledger_rows.to_be_bytes());
        hasher.update(source.evidence.owner_rows.to_be_bytes());
        hasher.update((source.evidence.tables.len() as u64).to_be_bytes());
        for table in &source.evidence.tables {
            hash_field(&mut hasher, table.table_id.as_bytes());
            hasher.update(table.rows.to_be_bytes());
            hasher.update(table.fingerprint);
        }
    }
    hasher.finalize().into()
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn populated_binding(tenant: &str, binding: &str) -> eg_types::semantic_index::SemanticBinding {
        use eg_types::semantic_index::{
            SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBinding, SemanticBindingDraft,
            SemanticLexicalIndexSpec, SemanticModelIdentity, SemanticPolicyComponents,
            SemanticSourceSelector, SemanticVectorMetric, SqlColumnRef, SEMANTIC_SQL_CATALOG_ID,
        };
        SemanticBinding::create(SemanticBindingDraft {
            binding_id: binding.to_string(),
            tenant_id: tenant.to_string(),
            actor_scope: "semantic:index-maintainer".to_string(),
            effective_actor_scope: "semantic:agent:index-maintainer".to_string(),
            purpose_id: "retrieval".to_string(),
            policy: SemanticPolicyComponents {
                rbac_policy_revision: 1,
                rbac_policy_digest: "sha256:rbac".to_string(),
                row_policy_revision: 1,
                row_policy_digest: "sha256:row-policy".to_string(),
                source_acl_revision: 7,
                source_acl_digest: "sha256:sql-acl-state".to_string(),
            },
            source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
                catalog_id: SEMANTIC_SQL_CATALOG_ID.to_string(),
                schema_id: "public".to_string(),
                table_id: "articles".to_string(),
                column_id: "body".to_string(),
            }),
            source_schema_digest: "sha256:schema".to_string(),
            source_revision: "source-v1".to_string(),
            source_field_set_digest: "sha256:field-set".to_string(),
            dimension: 3,
            metric: SemanticVectorMetric::Cosine,
            model: SemanticModelIdentity {
                model_id: "embedding-model".to_string(),
                model_revision: "revision-1".to_string(),
                preprocess_digest: "sha256:preprocess".to_string(),
                model_digest: "sha256:model".to_string(),
            },
            generation: 1,
            maintenance_policy_id: "semantic-maintenance".to_string(),
            lexical_index: SemanticLexicalIndexSpec {
                analyzer_id: "standard".to_string(),
                analyzer_revision: "1".to_string(),
                analyzer_config_digest: "sha256:analyzer".to_string(),
            },
            ann_index: SemanticAnnIndexSpec {
                method: SemanticAnnIndexMethod::IvfPq,
                parameters_digest: "sha256:ann-parameters".to_string(),
            },
            created_at: "2026-09-08T00:00:00Z".to_string(),
        })
        .unwrap()
    }

    #[test]
    fn populated_two_binding_observation_is_exact_and_source_mutation_refused() {
        use eg_core::compute::semantic_index_service::SemanticIndexService;
        use eg_types::contract::Nonce;
        use std::io::Write;
        use std::sync::Arc;

        let root = std::env::temp_dir().join(format!("semantic-observe-{}", uuid::Uuid::new_v4()));
        let tenant = format!("tenant:{}", uuid::Uuid::new_v4());
        let (grant_proof, _) = *super::super::semantic_server_secrets();
        for (index, binding) in ["a", "b"].into_iter().enumerate() {
            let service = SemanticIndexService::open(
                &legacy_semantic_owner_dir(&root, &tenant, binding),
                Arc::new(super::super::TenantScopedSemanticVerifier {
                    tenant: tenant.clone(),
                    proof: grant_proof,
                }),
                super::super::semantic_owner_principal(),
                &grant_proof,
                &tenant,
                binding,
            )
            .unwrap();
            let stored = service
                .admit_binding_operation(
                    &populated_binding(&tenant, binding),
                    1,
                    "semantic-index-observer-test",
                    &format!("observe-{index}"),
                    Nonce::from_bytes([index as u8 + 1; 32]),
                )
                .unwrap();
            assert!(!stored.replayed);
            drop(service);
        }
        let bindings = ["b".to_string(), "a".to_string()];
        let inspected =
            inspect_tenant_owner_upgrade(&root, &tenant, &bindings, 64 * 1024 * 1024).unwrap();
        let candidate =
            merge_multi_binding_tenant_candidate(&root, &inspected, 64 * 1024 * 1024).unwrap();
        promote_multi_binding_tenant_candidate(&root, &inspected, &candidate, 64 * 1024 * 1024)
            .unwrap();
        let first = observe_promoted_tenant_owner(
            &root,
            &tenant,
            &bindings,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap();
        let second = observe_promoted_tenant_owner(
            &root,
            &tenant,
            &bindings,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.binding_ids, vec!["a", "b"]);
        assert_eq!(first.owner_path, inspected.destination);
        assert_eq!(first.owner_sha256.len(), 64);
        assert_eq!(first.owner_layout_sha256.len(), 64);
        assert_eq!(
            first.source_census_sha256,
            hex::encode(inspected.source_census_digest)
        );
        assert_eq!(first.migration_proof_sha256.len(), 64);
        assert!(super::super::open_semantic_service(&root, &tenant, "a").is_err());
        std::fs::OpenOptions::new()
            .append(true)
            .open(&inspected.sources[0].source_file)
            .unwrap()
            .write_all(b"source changed")
            .unwrap();
        assert!(observe_promoted_tenant_owner(
            &root,
            &tenant,
            &bindings,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn complete_two_binding_census_is_stable_and_read_only() {
        use eg_core::compute::semantic_index_service::SemanticIndexService;
        use std::sync::Arc;

        let root = std::env::temp_dir().join(format!("semantic-upgrade-{}", uuid::Uuid::new_v4()));
        let tenant = format!("tenant:{}", uuid::Uuid::new_v4());
        let (proof, _) = *super::super::semantic_server_secrets();
        let mut source_files = Vec::new();
        for binding in ["a", "b"] {
            let directory = legacy_semantic_owner_dir(&root, &tenant, binding);
            let service = SemanticIndexService::open(
                &directory,
                Arc::new(super::super::TenantScopedSemanticVerifier {
                    tenant: tenant.clone(),
                    proof,
                }),
                super::super::semantic_owner_principal(),
                &proof,
                &tenant,
                binding,
            )
            .unwrap();
            drop(service);
            source_files.push(directory.join(legacy_store_file_name(&tenant, binding)));
        }
        let before: Vec<_> = source_files
            .iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect();
        let report = inspect_tenant_owner_upgrade(
            &root,
            &tenant,
            &["b".into(), "a".into()],
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(report.sources.len(), 2);
        assert_eq!(report.sources[0].binding_id, "a");
        assert_eq!(report.sources[1].binding_id, "b");
        assert!(report
            .sources
            .iter()
            .all(|source| source.evidence.ledger_rows > 0));
        assert!(!report.destination.exists());
        assert!(copy_single_binding_tenant_candidate(&root, &report, 64 * 1024 * 1024).is_err());
        let merged = merge_multi_binding_tenant_candidate(&root, &report, 64 * 1024 * 1024)
            .expect("distinct scope and owner keys merge without overwriting");
        assert!(merged.candidate.is_file());
        assert!(!report.destination.exists());
        assert_eq!(merged.source_census_digest, report.source_census_digest);
        assert_eq!(merged.source_file_sha256.len(), 2);
        assert_eq!(
            merged.target_evidence.owner_rows,
            report
                .sources
                .iter()
                .map(|source| source.evidence.owner_rows)
                .sum::<u64>()
        );
        for (path, original) in source_files.iter().zip(before) {
            assert_eq!(std::fs::read(path).unwrap(), original);
        }
        abort_multi_binding_tenant_candidate(&root, &report, &merged, 64 * 1024 * 1024).unwrap();
        let restored = SemanticIndexService::open(
            &legacy_semantic_owner_dir(&root, &tenant, "a"),
            Arc::new(super::super::TenantScopedSemanticVerifier {
                tenant: tenant.clone(),
                proof,
            }),
            super::super::semantic_owner_principal(),
            &proof,
            &tenant,
            "a",
        )
        .unwrap();
        drop(restored);
        let restaged =
            merge_multi_binding_tenant_candidate(&root, &report, 64 * 1024 * 1024).unwrap();
        // Simulate a process death after the durable promotion fence and
        // canonical link, but before the private candidate alias is removed.
        install_migration_fence(&root, &tenant, &report.destination).unwrap();
        std::fs::hard_link(&restaged.candidate, &report.destination).unwrap();
        let recovered = recover_promoted_tenant_owner(
            &root,
            &tenant,
            &["a".into(), "b".into()],
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(recovered.destination, report.destination);
        assert_eq!(recovered.target_evidence, restaged.target_evidence);
        assert!(!restaged.candidate.exists());
        assert!(super::super::refuse_split_semantic_owner(&root, &tenant).is_err());
        std::fs::write(&restaged.candidate, b"different candidate inode").unwrap();
        assert!(recover_promoted_tenant_owner(
            &root,
            &tenant,
            &["a".into(), "b".into()],
            64 * 1024 * 1024,
        )
        .is_err());
        assert!(report.destination.exists());
        std::fs::remove_file(&restaged.candidate).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ordinary_two_binding_promotion_preserves_sources_and_recovers() {
        use eg_core::compute::semantic_index_service::SemanticIndexService;
        use std::sync::Arc;

        let root = std::env::temp_dir().join(format!("semantic-promote-{}", uuid::Uuid::new_v4()));
        let tenant = format!("tenant:{}", uuid::Uuid::new_v4());
        let (proof, _) = *super::super::semantic_server_secrets();
        for binding in ["a", "b"] {
            let service = SemanticIndexService::open(
                &legacy_semantic_owner_dir(&root, &tenant, binding),
                Arc::new(super::super::TenantScopedSemanticVerifier {
                    tenant: tenant.clone(),
                    proof,
                }),
                super::super::semantic_owner_principal(),
                &proof,
                &tenant,
                binding,
            )
            .unwrap();
            drop(service);
        }
        let inspected = inspect_tenant_owner_upgrade(
            &root,
            &tenant,
            &["a".into(), "b".into()],
            64 * 1024 * 1024,
        )
        .unwrap();
        let sources: Vec<_> = inspected
            .sources
            .iter()
            .map(|source| std::fs::read(&source.source_file).unwrap())
            .collect();
        let candidate =
            merge_multi_binding_tenant_candidate(&root, &inspected, 64 * 1024 * 1024).unwrap();
        let promoted =
            promote_multi_binding_tenant_candidate(&root, &inspected, &candidate, 64 * 1024 * 1024)
                .unwrap();
        assert!(promoted.destination.is_file());
        assert!(super::super::tenant_semantic_migration_fence_file(&root, &tenant).is_file());
        assert!(!candidate.candidate.exists());
        // The real canonical file still leaves both public resolution paths
        // closed; the offline promotion proof is not an activation receipt.
        assert!(super::super::open_semantic_service(&root, &tenant, "a").is_err());
        assert!(super::super::existing_semantic_service(&root, &tenant, "b").is_err());
        assert!(observe_promoted_tenant_owner(
            &root,
            &tenant,
            &["a".into(), "b".into()],
            64 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .is_err());
        for (source, original) in inspected.sources.iter().zip(sources) {
            assert_eq!(std::fs::read(&source.source_file).unwrap(), original);
        }
        let reopened = recover_promoted_tenant_owner(
            &root,
            &tenant,
            &["b".into(), "a".into()],
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(reopened.target_evidence, promoted.target_evidence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn singleton_copy_preserves_source_and_can_be_rolled_back() {
        use eg_core::compute::semantic_index_service::SemanticIndexService;
        use std::sync::Arc;

        let root = std::env::temp_dir().join(format!("semantic-upgrade-{}", uuid::Uuid::new_v4()));
        let tenant = format!("tenant:{}", uuid::Uuid::new_v4());
        let binding = "only-binding";
        let directory = legacy_semantic_owner_dir(&root, &tenant, binding);
        let (proof, _) = *super::super::semantic_server_secrets();
        let verifier = || {
            Arc::new(super::super::TenantScopedSemanticVerifier {
                tenant: tenant.clone(),
                proof,
            })
        };
        let source = SemanticIndexService::open(
            &directory,
            verifier(),
            super::super::semantic_owner_principal(),
            &proof,
            &tenant,
            binding,
        )
        .unwrap();
        drop(source);
        let path = directory.join(legacy_store_file_name(&tenant, binding));
        let before = std::fs::read(&path).unwrap();
        let inspected =
            inspect_tenant_owner_upgrade(&root, &tenant, &[binding.into()], 64 * 1024 * 1024)
                .unwrap();
        let copied =
            copy_single_binding_tenant_candidate(&root, &inspected, 64 * 1024 * 1024).unwrap();
        assert!(copied.candidate.is_file());
        assert!(!inspected.destination.exists());
        assert_eq!(copied.source_census_digest, inspected.source_census_digest);
        assert_eq!(copied.source_file_sha256, copied.copied_file_sha256);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        prove_single_binding_copy(&inspected.sources[0].evidence, &copied.target_evidence).unwrap();
        // No canonical v3 marker was installed, so deleting the private
        // candidate and reopening v2 is a complete rollback.
        abort_single_binding_tenant_candidate(&root, &inspected, &copied, 64 * 1024 * 1024)
            .unwrap();
        let reopened = SemanticIndexService::open(
            &directory,
            verifier(),
            super::super::semantic_owner_principal(),
            &proof,
            &tenant,
            binding,
        )
        .unwrap();
        drop(reopened);
        // Opening the v2 redb source may update its store bytes. Promotion
        // must preserve the source as it stood immediately before restaging.
        let before_restaging = std::fs::read(&path).unwrap();
        let restaged =
            copy_single_binding_tenant_candidate(&root, &inspected, 64 * 1024 * 1024).unwrap();
        let promoted =
            promote_single_binding_tenant_candidate(&root, &inspected, &restaged, 64 * 1024 * 1024)
                .unwrap();
        assert_eq!(promoted.destination, inspected.destination);
        assert!(!restaged.candidate.exists());
        assert_eq!(std::fs::read(&path).unwrap(), before_restaging);
        assert!(super::super::refuse_split_semantic_owner(&root, &tenant).is_err());
        let recovered =
            recover_promoted_tenant_owner(&root, &tenant, &[binding.into()], 64 * 1024 * 1024)
                .unwrap();
        assert_eq!(recovered.target_evidence, promoted.target_evidence);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn partial_catalog_and_symlink_sources_are_refused_before_open() {
        let root = std::env::temp_dir().join(format!("semantic-upgrade-{}", uuid::Uuid::new_v4()));
        let tenant = "tenant/a";
        let a = legacy_semantic_owner_dir(&root, tenant, "a");
        let b = legacy_semantic_owner_dir(&root, tenant, "b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let result = inspect_tenant_owner_upgrade(&root, tenant, &["a".into()], 1_000_000);
        assert!(result.unwrap_err().contains("every v2 owner directory"));
        std::fs::remove_dir(&b).unwrap();
        #[cfg(unix)]
        {
            let target = root.join("target");
            std::fs::write(&target, b"not redb").unwrap();
            std::os::unix::fs::symlink(&target, a.join(legacy_store_file_name(tenant, "a")))
                .unwrap();
            let result = inspect_tenant_owner_upgrade(&root, tenant, &["a".into()], 1_000_000);
            assert!(result.unwrap_err().contains("ordinary file"));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn zero_budget_and_duplicate_binding_are_refused() {
        let root = Path::new("/tmp/semantic-upgrade-missing");
        assert!(inspect_tenant_owner_upgrade(root, "t", &["a".into()], 0).is_err());
        assert!(inspect_tenant_owner_upgrade(root, "t", &["a".into(), "a".into()], 1).is_err());
    }
}
