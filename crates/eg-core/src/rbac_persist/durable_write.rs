use std::collections::{BTreeMap, BTreeSet};

use eg_storage::RbacOwner;
use eg_transaction::{AdmittedOwnerWrite, Begin};
use sha2::{Digest, Sha256};

use crate::acl::AgentIdentity;
use crate::rbac::RbacPolicy;

use super::{
    IdentityBootstrapState, RbacPersistError, RbacStore, BOOTSTRAP_KEY, IDENTITIES_KEY, POLICY_KEY,
    RBAC_TABLE,
};

struct EncodedAuthorityState {
    policy: Vec<u8>,
    identities: Vec<u8>,
    bootstrap: Vec<u8>,
    digest: String,
}

pub(super) fn save_authority_state(
    store: &RbacStore,
    policy: &RbacPolicy,
    identities: &BTreeMap<String, AgentIdentity>,
    bootstrap: IdentityBootstrapState,
) -> Result<(), RbacPersistError> {
    let encoded = encode_authority_state(policy, identities, bootstrap)?;
    if is_current(store, &encoded)? {
        return Ok(());
    }
    let expected = store.current_version()?;
    let target = expected.checked_add(1).ok_or_else(|| {
        RbacPersistError::Redb("identity/RBAC state version overflow".to_string())
    })?;
    let batch = authority_batch(store, expected, target, &encoded.digest);
    persist_authority_state(store, &batch, &encoded)
}

fn encode_authority_state(
    policy: &RbacPolicy,
    identities: &BTreeMap<String, AgentIdentity>,
    bootstrap: IdentityBootstrapState,
) -> Result<EncodedAuthorityState, RbacPersistError> {
    let policy = serde_json::to_vec(policy)?;
    let identities = serde_json::to_vec(identities)?;
    let bootstrap = serde_json::to_vec(&bootstrap)?;
    let mut state_digest = Sha256::new();
    state_digest.update(&policy);
    state_digest.update([0]);
    state_digest.update(&identities);
    state_digest.update([0]);
    state_digest.update(&bootstrap);
    Ok(EncodedAuthorityState {
        policy,
        identities,
        bootstrap,
        digest: hex::encode(state_digest.finalize()),
    })
}

fn is_current(
    store: &RbacStore,
    encoded: &EncodedAuthorityState,
) -> Result<bool, RbacPersistError> {
    let read = store.scoped_read()?;
    let table = read.open_table(RBAC_TABLE).map_err(RbacPersistError::Redb)?;
    let policy_matches = table
        .get(POLICY_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .is_some_and(|value| value.value() == encoded.policy.as_slice());
    let identities_match = table
        .get(IDENTITIES_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .is_some_and(|value| value.value() == encoded.identities.as_slice());
    let bootstrap_matches = table
        .get(BOOTSTRAP_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .is_some_and(|value| value.value() == encoded.bootstrap.as_slice());
    Ok(policy_matches && identities_match && bootstrap_matches)
}

fn authority_batch(
    store: &RbacStore,
    expected: u64,
    target: u64,
    digest: &str,
) -> eg_types::MutationBatch {
    let batch_id = format!("rbac:{target}:{digest}");
    eg_types::MutationBatch {
        schema_version: eg_types::MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: eg_types::MutationRequestContext {
            request_id: 0,
            // The batch actor must be the principal the storage kernel
            // authenticated this store's serving scope for; the mutation kernel
            // rejects any batch whose context names a different one.
            principal: store.owner.principal().to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // OCC supplies the expected version, so this path claims no
            // unversioned-system-mutation capability.
            verified_capabilities: BTreeSet::new(),
        },
        identity: store.owner.identity().clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        version_expectation: eg_types::VersionExpectation::Native(expected),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![eg_types::MutationOperation {
            ordinal: 0,
            surface: eg_types::MutationSurface::Other,
            domain: eg_types::mutation_batch::MutationDomain::ControlPlane,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: "security_state_snapshot".to_string(),
                query: format!("sha256:{digest}"),
            },
        }],
        outbox: vec![eg_types::MutationOutboxIntent {
            topic: "engine.security.committed".to_string(),
            key: batch_id,
            payload: digest.as_bytes().to_vec(),
            headers: BTreeMap::new(),
        }],
        created_at_ms: 0,
    }
}

fn persist_authority_state(
    store: &RbacStore,
    batch: &eg_types::MutationBatch,
    encoded: &EncodedAuthorityState,
) -> Result<(), RbacPersistError> {
    let outcome = rmp_serde::to_vec_named(&true)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    commit_authority_mutation(store, batch, Some(outcome), |owner_write| {
        let mut table = owner_write
            .open_table(RBAC_TABLE)
            .map_err(RbacPersistError::Redb)?;
        table
            .insert(POLICY_KEY, encoded.policy.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        table
            .insert(IDENTITIES_KEY, encoded.identities.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        table
            .insert(BOOTSTRAP_KEY, encoded.bootstrap.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        Ok(())
    })
}

/// Remove one mandatory record. The ONLY way this crate can write, so a test
/// that needs a partial durable image still goes through admission, ordering
/// and commit rather than becoming a second physical authority.
#[cfg(test)]
pub(super) fn remove_authority_record(
    store: &RbacStore,
    key: &'static str,
) -> Result<(), RbacPersistError> {
    let expected = store.current_version()?;
    let target = expected
        .checked_add(1)
        .ok_or_else(|| RbacPersistError::Redb("identity/RBAC state version overflow".to_string()))?;
    let batch = authority_batch(store, expected, target, &format!("remove-{key}"));
    commit_authority_mutation(store, &batch, None, |owner_write| {
        owner_write
            .open_table(RBAC_TABLE)
            .map_err(RbacPersistError::Redb)?
            .remove(key)
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        Ok(())
    })
}

/// Admit `batch`, apply `apply_rows` to the `rbac_v1` owner table inside that
/// one admitted write, and commit. The write transaction is minted by the
/// storage kernel's single mutation authority and is never reachable here: the
/// only handle this crate is given is the layout-bounded
/// [`AdmittedOwnerWrite`], valid between `owner_rows` and `finish_owner`.
fn commit_authority_mutation<F>(
    store: &RbacStore,
    batch: &eg_types::MutationBatch,
    result_msgpack: Option<Vec<u8>>,
    apply_rows: F,
) -> Result<(), RbacPersistError>
where
    F: FnOnce(&AdmittedOwnerWrite<'_, RbacOwner>) -> Result<(), RbacPersistError>,
{
    let (write, begun) = store
        .mutations
        .admit(&store.owner, batch)
        .map_err(RbacPersistError::Redb)?;
    let source_version = match begun {
        // The idempotency key already names a terminally committed receipt:
        // this exact state was persisted by an earlier attempt, so the write is
        // discarded rather than reapplied.
        Begin::Replay(_) => return write.abort().map_err(RbacPersistError::Redb),
        Begin::Apply { source_version } => source_version,
    };
    let owner_write = write
        .owner_rows(&store.owner, batch)
        .map_err(RbacPersistError::Redb)?;
    apply_rows(&owner_write)?;
    owner_write.finish_owner().map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .finish(&write, batch, result_msgpack, 0, source_version)
        .map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .commit(write, batch)
        .map_err(RbacPersistError::Redb)
}
