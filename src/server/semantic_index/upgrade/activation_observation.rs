//! Read-only RF-019 activation evidence checks under the existing engine lease.

use super::*;
use eg_storage::SemanticOwnerMergeEvidence;

pub(super) fn validate_activation_sources(
    current: &TenantOwnerUpgradeInspection,
    tenant: &str,
) -> Result<(), String> {
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
                .is_none_or(|entry| entry.rows == 0)
            {
                return Err(format!(
                    "semantic activation source {} lacks a populated {table}",
                    source.binding_id
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn activation_observation_digest(
    tenant: &str,
    ordered: &[String],
    canonical: &Path,
    owner: &OwnerFingerprint,
    current: &TenantOwnerUpgradeInspection,
    proof: &SemanticOwnerMergeEvidence,
    layout: &str,
) -> String {
    let mut proof_digest = Sha256::new();
    proof_digest.update(b"eg/rf019-tenant-migration-observation/v1\0");
    hash_field(&mut proof_digest, tenant.as_bytes());
    proof_digest.update((ordered.len() as u64).to_be_bytes());
    for binding in ordered {
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
    hex::encode(proof_digest.finalize())
}
