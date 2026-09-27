//! Private candidate creation, abort, and promotion under the engine lease.

use super::*;

fn prove_single_candidate(
    candidate: &Path,
    current: &TenantOwnerUpgradeInspection,
    source: &InspectedBindingOwner,
) -> Result<StrictRecoveryEvidence, String> {
    let source_identity = legacy_physical_identity(&current.tenant_id, &source.binding_id)?;
    let validated = open_recovery(candidate, source_identity, None, OwnerLayout::SemanticIndex)?;
    let target_identity = tenant_physical_identity(&current.destination)?;
    let adopted = adopt_recovery(validated, target_identity.clone())?;
    let target_evidence = strict_recovery_evidence(&adopted)?;
    drop(adopted);
    let target = open_read_only(candidate, None)?;
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
    Ok(target_evidence)
}

/// Build a proved *private candidate* for a tenant with exactly one v2
/// binding. This does not install the canonical v3 path or change normal
/// serving. A tenant with multiple v2 files needs a separate ledger-key,
/// replay and outbox merge proof and is refused here.
pub(crate) fn copy_single_binding_tenant_candidate(
    persist_dir: &Path,
    expected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
) -> Result<TenantOwnerCandidate, String> {
    if expected.sources.len() != 1 {
        return Err("semantic tenant copy requires exactly one v2 binding".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let current = inspect_unchanged_sources_under_lease(
        persist_dir,
        expected,
        max_total_bytes,
        "semantic tenant source changed after inspection",
    )?;
    let source = &current.sources[0];
    let candidate = current.destination.with_extension("candidate.redb");
    require_candidate_headroom(persist_dir, current.total_bytes)?;
    let mut cleanup = CandidateCleanup::new(candidate.clone());
    let mut output =
        eg_core::fs::create_private_new_file(&candidate).map_err(|error| error.to_string())?;
    cleanup.armed = true;
    copy_file_contents(&source.source_file, &mut output, source.bytes)?;
    drop(output);
    let source_file_sha256 = file_sha256(&source.source_file)?;
    let copied_file_sha256 = file_sha256(&candidate)?;
    if source_file_sha256 != copied_file_sha256 {
        return Err("semantic tenant candidate differs from its v2 source bytes".to_string());
    }
    let target_evidence = prove_single_candidate(&candidate, &current, source)?;
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
pub(crate) fn abort_single_binding_tenant_candidate(
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
    let current = inspect_expected_sources_under_lease(persist_dir, inspected, max_total_bytes)?;
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
pub(crate) fn merge_multi_binding_tenant_candidate(
    persist_dir: &Path,
    expected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
) -> Result<TenantOwnerMergedCandidate, String> {
    if expected.sources.len() < 2 {
        return Err("semantic multi-owner merge requires at least two bindings".to_string());
    }
    let _lease = eg_core::persist_lock::acquire(&persist_dir.to_string_lossy())?;
    let current = inspect_unchanged_sources_under_lease(
        persist_dir,
        expected,
        max_total_bytes,
        "semantic tenant sources changed after inspection",
    )?;
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

/// Reinspect the original complete source census while the caller holds the
/// engine lease. Both abort and promotion must derive the binding list from
/// the inspected set, never from a new caller-supplied subset.
fn inspect_expected_sources_under_lease(
    persist_dir: &Path,
    inspected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
) -> Result<TenantOwnerUpgradeInspection, String> {
    let bindings: Vec<_> = inspected
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    inspect_under_lease(
        persist_dir,
        &inspected.tenant_id,
        &bindings,
        max_total_bytes,
    )
}

/// Verify the complete source census and reserved destination while a
/// candidate-producing caller still holds the exclusive engine lease.
fn inspect_unchanged_sources_under_lease(
    persist_dir: &Path,
    expected: &TenantOwnerUpgradeInspection,
    max_total_bytes: u64,
    changed_error: &'static str,
) -> Result<TenantOwnerUpgradeInspection, String> {
    let current = inspect_expected_sources_under_lease(persist_dir, expected, max_total_bytes)?;
    if current.source_census_digest != expected.source_census_digest
        || current.destination != expected.destination
    {
        return Err(changed_error.to_string());
    }
    Ok(current)
}

/// Remove an unactivated multi-binding candidate only after its exact source
/// set and the candidate's reopened recovery evidence still match the copy
/// receipt. Normal v2 serving remains the authority throughout.
pub(crate) fn abort_multi_binding_tenant_candidate(
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
    let current = inspect_expected_sources_under_lease(persist_dir, inspected, max_total_bytes)?;
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

pub(crate) fn promote_single_binding_tenant_candidate(
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

pub(crate) fn promote_multi_binding_tenant_candidate(
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
    let current = inspect_unchanged_sources_under_lease(
        persist_dir,
        inspected,
        max_total_bytes,
        "semantic promotion source changed after candidate copy",
    )?;
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
    let bindings: Vec<_> = current
        .sources
        .iter()
        .map(|source| source.binding_id.clone())
        .collect();
    verify_promoted_under_lease(persist_dir, &current.tenant_id, &bindings, max_total_bytes)
}
