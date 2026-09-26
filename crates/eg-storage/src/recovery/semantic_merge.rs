//! Offline merge of exact SemanticIndex owner files into one private tenant
//! candidate. The caller must hold the engine's exclusive persist-dir lock.
//! This function never activates a candidate or deletes a source.

use super::evidence::{
    copy_bindings, copy_ledger_rows, strict_evidence_of, strict_recovery_evidence_read_only,
    strict_snapshot_read, strict_snapshot_write, StrictRecoveryEvidence,
};
use crate::codec::{decode_ledger_record, encode_bounded};
use crate::kernel::create_physical;
use crate::owner::{copy_declared_owner_tables, prove_declared_owner_rows, validate_manifest_read};
use crate::physical::binding::ScopeBinding;
use crate::physical::manifest::OwnerManifest;
use crate::tables::{visit_ledger_content_tables, OWNER_MANIFEST, SCOPE_BINDINGS};
use crate::{open_read_only, OwnerLayout, PhysicalStoreIdentity};
use redb::{
    Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, TableHandle, Value,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SemanticOwnerMergeEvidence {
    pub sources: Vec<StrictRecoveryEvidence>,
    pub target: StrictRecoveryEvidence,
    /// Domain-separated digest of the exact source→target row disposition
    /// proof, ordered by source path, closed table ordinal and redb key.
    pub row_proof_sha256: [u8; 32],
}

/// Copy all declared owner and mutation-ledger rows from nonempty, distinct
/// current-format source files. Any duplicate key in any table is refused
/// before commit; no last-writer-wins merge is legal for replay or outbox.
///
/// Every source is validated through a read-only physical open. The target is
/// a new file with its own root and an authority epoch higher than every
/// predecessor. Scope bindings are reanchored to that target root, while
/// their logical identities and every other row's bytes are preserved.
pub fn merge_semantic_owner_files(
    source_files: &[(PathBuf, PhysicalStoreIdentity)],
    candidate: &Path,
    target_identity: PhysicalStoreIdentity,
) -> Result<SemanticOwnerMergeEvidence, String> {
    if source_files.is_empty() || source_files.len() > 256 {
        return Err("semantic merge requires 1..=256 source files".to_string());
    }
    if candidate.exists() {
        return Err("semantic merge candidate already exists".to_string());
    }
    let mut source_paths = std::collections::BTreeSet::new();
    let mut source_evidence = Vec::with_capacity(source_files.len());
    let mut max_epoch = 0_u64;
    for (path, identity) in source_files {
        if !source_paths.insert(path) || path.as_path() == candidate {
            return Err("semantic merge source paths are not distinct".to_string());
        }
        let store = open_read_only(path, None)?;
        let evidence =
            strict_recovery_evidence_read_only(&store, identity, OwnerLayout::SemanticIndex)?;
        let read = store
            .database()
            .begin_read()
            .map_err(|error| error.to_string())?;
        let manifest = validate_manifest_read(&read, identity, OwnerLayout::SemanticIndex)?;
        max_epoch = max_epoch.max(manifest.authority_epoch);
        source_evidence.push(evidence);
    }
    let epoch = max_epoch
        .checked_add(1)
        .ok_or_else(|| "semantic merge authority epoch exhausted".to_string())?;
    let target = create_physical(
        candidate,
        target_identity.clone(),
        None,
        OwnerLayout::SemanticIndex,
    )?;
    let mut write = target
        .database()
        .begin_write()
        .map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let mut manifest: OwnerManifest = target.manifest().clone();
    manifest.authority_epoch = epoch;
    manifest.validate()?;
    let bytes = encode_bounded(&manifest, "semantic merged owner manifest")?;
    write
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .insert("manifest", bytes.as_slice())
        .map_err(|error| error.to_string())?;
    for ((path, identity), expected) in source_files.iter().zip(&source_evidence) {
        let store = open_read_only(path, None)?;
        let read = store
            .database()
            .begin_read()
            .map_err(|error| error.to_string())?;
        // Revalidate each exact source before copying after target creation.
        validate_manifest_read(&read, identity, OwnerLayout::SemanticIndex)?;
        if strict_snapshot_read(&read, OwnerLayout::SemanticIndex)? != *expected {
            return Err("semantic merge source changed before table copy".to_string());
        }
        copy_bindings(&read, &write, target.incarnation())?;
        copy_ledger_rows(&read, &write)?;
        copy_declared_owner_tables(&read, &write, OwnerLayout::SemanticIndex)?;
    }
    let staged = strict_snapshot_write(&write, OwnerLayout::SemanticIndex)?;
    prove_union_counts(&source_evidence, &staged)?;
    write.commit().map_err(|error| error.to_string())?;
    drop(target);
    let reopened = crate::StorageKernel::open_owner::<crate::SemanticIndexOwner>(
        candidate,
        target_identity.clone(),
        None,
    )?;
    let verified = strict_evidence_of(reopened.store())?;
    if verified != staged {
        return Err("semantic merge evidence changed after durable reopen".to_string());
    }
    drop(reopened);
    let read_only = open_read_only(candidate, None)?;
    let final_evidence = strict_recovery_evidence_read_only(
        &read_only,
        &target_identity,
        OwnerLayout::SemanticIndex,
    )?;
    if final_evidence != verified {
        return Err("semantic merge evidence changed after closing the writer".to_string());
    }
    drop(read_only);
    let proof = prove_semantic_owner_union_read_only(source_files, candidate, &target_identity)?;
    if proof.sources != source_evidence || proof.target != verified {
        return Err("semantic merge row proof changed the recovered census".to_string());
    }
    Ok(proof)
}

/// Reconstruct the exact union proof after a restart or a path reanchor.
/// Every source row must have identical bytes in the destination, except
/// scope bindings, whose only permitted change is their physical-root digest.
/// Per-table cardinality then rules out extra, lost or collided rows.
pub fn prove_semantic_owner_union_read_only(
    source_files: &[(PathBuf, PhysicalStoreIdentity)],
    destination: &Path,
    target_identity: &PhysicalStoreIdentity,
) -> Result<SemanticOwnerMergeEvidence, String> {
    if source_files.is_empty() || source_files.len() > 256 {
        return Err("semantic union requires 1..=256 source files".to_string());
    }
    let mut distinct_paths = std::collections::BTreeSet::new();
    let target = open_read_only(destination, None)?;
    let target_evidence =
        strict_recovery_evidence_read_only(&target, target_identity, OwnerLayout::SemanticIndex)?;
    let target_read = target
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let target_table = target_read
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let mut source_evidence = Vec::with_capacity(source_files.len());
    let mut source_proofs = Vec::with_capacity(source_files.len());
    for (path, identity) in source_files {
        if !distinct_paths.insert(path) || path.as_path() == destination {
            return Err("semantic union source paths are not distinct".to_string());
        }
        let source = open_read_only(path, None)?;
        source_evidence.push(strict_recovery_evidence_read_only(
            &source,
            identity,
            OwnerLayout::SemanticIndex,
        )?);
        let source_read = source
            .database()
            .begin_read()
            .map_err(|error| error.to_string())?;
        let source_table = source_read
            .open_table(SCOPE_BINDINGS)
            .map_err(|error| error.to_string())?;
        let mut row_proof = Sha256::new();
        row_proof.update(b"eg/semantic-owner-row-proof/v1\0");
        proof_field(&mut row_proof, path.as_os_str().as_encoded_bytes());
        proof_field(&mut row_proof, identity.digest());
        for row in source_table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let original: ScopeBinding = decode_ledger_record(value.value())?;
            let merged = target_table
                .get(key.value())
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "semantic merge omitted a scope binding".to_string())?;
            let adopted: ScopeBinding = decode_ledger_record(merged.value())?;
            if original.schema_version != adopted.schema_version
                || original.identity != adopted.identity
                || original.initial_version != adopted.initial_version
                || adopted.store_identity_digest != target.incarnation().identity_digest()
            {
                return Err("semantic merge changed a logical scope binding".to_string());
            }
            prove_row(
                &mut row_proof,
                SCOPE_BINDINGS.name(),
                key.value().as_bytes(),
                value.value(),
                merged.value(),
                b"reanchored",
            );
        }
        macro_rules! prove_content {
            ($table:expr) => {{
                prove_table_rows(&source_read, &target_read, $table, &mut row_proof)?;
            }};
        }
        visit_ledger_content_tables!(prove_content);
        prove_declared_owner_rows(
            &source_read,
            &target_read,
            OwnerLayout::SemanticIndex,
            &mut row_proof,
        )?;
        let digest: [u8; 32] = row_proof.finalize().into();
        source_proofs.push((path, digest));
    }
    prove_union_counts(&source_evidence, &target_evidence)?;
    source_proofs.sort_by(|left, right| left.0.cmp(right.0));
    let mut proof = Sha256::new();
    proof.update(b"eg/semantic-owner-union-proof/v1\0");
    proof.update((source_proofs.len() as u64).to_be_bytes());
    for (path, digest) in source_proofs {
        proof_field(&mut proof, path.as_os_str().as_encoded_bytes());
        proof_field(&mut proof, &digest);
    }
    proof_field(&mut proof, &target_evidence.fingerprint);
    Ok(SemanticOwnerMergeEvidence {
        sources: source_evidence,
        target: target_evidence,
        row_proof_sha256: proof.finalize().into(),
    })
}

/// Refuse a legacy per-binding file whose SemanticIndex owner rows name a
/// different tenant or binding. This is separate from physical recovery
/// validation, which binds ledger rows to scopes but does not interpret the
/// first two components of domain-specific semantic table keys.
pub fn prove_semantic_owner_partition_read_only(
    path: &Path,
    identity: &PhysicalStoreIdentity,
    tenant: &str,
    binding: &str,
) -> Result<(), String> {
    let source = open_read_only(path, None)?;
    strict_recovery_evidence_read_only(&source, identity, OwnerLayout::SemanticIndex)?;
    let read = source
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    crate::owner::registry::prove_semantic_partition(&read, tenant, binding)
}

pub(crate) fn prove_table_rows<K, V>(
    source: &ReadTransaction,
    target: &ReadTransaction,
    table: TableDefinition<'static, K, V>,
    proof: &mut Sha256,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    let source_table = source
        .open_table(table)
        .map_err(|error| error.to_string())?;
    let target_table = target
        .open_table(table)
        .map_err(|error| error.to_string())?;
    for row in source_table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let target_value = target_table
            .get(key.value())
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("semantic union omitted a row in {}", table.name()))?;
        if V::as_bytes(&value.value()).as_ref() != V::as_bytes(&target_value.value()).as_ref() {
            return Err(format!("semantic union changed a row in {}", table.name()));
        }
        prove_row(
            proof,
            table.name(),
            K::as_bytes(&key.value()).as_ref(),
            V::as_bytes(&value.value()).as_ref(),
            V::as_bytes(&target_value.value()).as_ref(),
            b"preserved",
        );
    }
    Ok(())
}

fn prove_row(
    proof: &mut Sha256,
    table: &str,
    key: &[u8],
    source_value: &[u8],
    target_value: &[u8],
    disposition: &[u8],
) {
    proof_field(proof, table.as_bytes());
    proof_field(proof, key);
    proof_field(proof, &Sha256::digest(source_value));
    proof_field(proof, &Sha256::digest(target_value));
    proof_field(proof, disposition);
}

fn proof_field(proof: &mut Sha256, field: &[u8]) {
    proof.update((field.len() as u64).to_be_bytes());
    proof.update(field);
}

fn prove_union_counts(
    sources: &[StrictRecoveryEvidence],
    target: &StrictRecoveryEvidence,
) -> Result<(), String> {
    let first = &sources[0];
    if sources
        .iter()
        .any(|source| source.tables.len() != first.tables.len())
        || target.tables.len() != first.tables.len()
    {
        return Err("semantic merge table census differs across owners".to_string());
    }
    for (index, target_table) in target.tables.iter().enumerate() {
        let expected = if matches!(
            target_table.table_id.as_str(),
            "mutation_store_root" | "mutation_owner_manifest"
        ) {
            1
        } else {
            sources.iter().try_fold(0_u64, |sum, source| {
                if source.tables[index].table_id != target_table.table_id {
                    return Err("semantic merge table order changed".to_string());
                }
                sum.checked_add(source.tables[index].rows)
                    .ok_or_else(|| "semantic merge row count overflowed".to_string())
            })?
        };
        if target_table.rows != expected {
            return Err("semantic merge lost or duplicated a table row".to_string());
        }
    }
    let owner_rows = sources.iter().try_fold(0_u64, |sum, source| {
        sum.checked_add(source.owner_rows)
            .ok_or_else(|| "semantic merge owner row count overflowed".to_string())
    })?;
    let ledger_rows = sources.iter().try_fold(0_u64, |sum, source| {
        sum.checked_add(source.ledger_rows)
            .ok_or_else(|| "semantic merge ledger row count overflowed".to_string())
    })?;
    let ledger_rows = ledger_rows
        .checked_sub(2 * (sources.len() as u64 - 1))
        .ok_or_else(|| "semantic merge ledger row count underflowed".to_string())?;
    if target.owner_rows != owner_rows || target.ledger_rows != ledger_rows {
        return Err("semantic merge changed owner or ledger row totals".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner::registry::SEMANTIC_BINDINGS;

    #[test]
    fn owner_partition_rejects_a_cross_tenant_semantic_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("semantic.redb");
        let identity = PhysicalStoreIdentity::new("semantic-partition-test").unwrap();
        let store =
            create_physical(&path, identity.clone(), None, OwnerLayout::SemanticIndex).unwrap();
        let write = store.database().begin_write().unwrap();
        write
            .open_table(SEMANTIC_BINDINGS)
            .unwrap()
            .insert(("tenant-b", "binding-a", 1), b"planted".as_slice())
            .unwrap();
        write.commit().unwrap();
        drop(store);
        assert!(prove_semantic_owner_partition_read_only(
            &path,
            &identity,
            "tenant-a",
            "binding-a"
        )
        .is_err());
        assert!(prove_semantic_owner_partition_read_only(
            &path,
            &identity,
            "tenant-b",
            "binding-a"
        )
        .is_ok());
    }
}
