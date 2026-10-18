use std::collections::BTreeMap;

use eg_storage::RbacOwner;
use eg_transaction::{AdmittedOwnerWrite, Begin};
use redb::ReadableTable;
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
    expected_revision: u64,
    policy: &RbacPolicy,
    identities: &BTreeMap<String, AgentIdentity>,
    bootstrap: IdentityBootstrapState,
) -> Result<u64, RbacPersistError> {
    let encoded = encode_authority_state(policy, identities, bootstrap)?;
    persist_authority_state(store, expected_revision, &encoded, false)
}

pub(super) fn initialize_authority_state(store: &RbacStore) -> Result<(), RbacPersistError> {
    let expected_revision = store.current_version()?;
    let encoded = encode_authority_state(
        &RbacPolicy::new(),
        &BTreeMap::new(),
        IdentityBootstrapState::Pending,
    )?;
    persist_authority_state(store, expected_revision, &encoded, true).map(|_| ())
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
        // An RBAC authority write has no caller: it publishes the whole
        // policy/identity/bootstrap image the process already authorized, so it
        // is a MAINTENANCE mutation (RF-RULING-005) with no operation replay
        // identity and no attempt nonce. Its serving principal is the one the
        // storage kernel authenticated this store's scope for; the mutation
        // kernel rejects any batch naming a different one.
        envelope: eg_types::mutation_batch::MutationEnvelope::maintenance(
            store.owner.principal(),
            "rbac_authority",
            digest,
            &batch_id,
        )
        .expect("an RBAC authority digest is a canonical resource id"),
        identity: store.owner.identity().clone(),
        placement_epoch: 0,
        version_expectation: eg_types::VersionExpectation::Native(expected),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![eg_types::MutationOperation {
            ordinal: 0,
            surface: eg_types::MutationSurface::Other,
            domain: eg_types::mutation_batch::DurabilityDomain::ControlPlane,
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
    expected_revision: u64,
    encoded: &EncodedAuthorityState,
    initialize: bool,
) -> Result<u64, RbacPersistError> {
    let outcome = rmp_serde::to_vec_named(&true)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    commit_authority_mutation(
        store,
        expected_revision,
        &encoded.digest,
        Some(outcome),
        |owner_write| {
            let mut table = owner_write
                .open_table(RBAC_TABLE)
                .map_err(RbacPersistError::Redb)?;
            // Initialization requires absence of ALL records in this same write.
            // Ordinary writes must not repair an incomplete image.
            for key in [POLICY_KEY, IDENTITIES_KEY, BOOTSTRAP_KEY] {
                let present = table
                    .get(key)
                    .map_err(|error| RbacPersistError::Redb(error.to_string()))?
                    .is_some();
                if present == initialize {
                    return Err(RbacPersistError::IncompleteState(
                        "authority image does not match initialization/write precondition",
                    ));
                }
            }
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
        },
    )
}

/// Remove one mandatory record. The ONLY way this crate can write, so a test
/// that needs a partial durable image still goes through admission, ordering
/// and commit rather than becoming a second physical authority.
#[cfg(test)]
pub(super) fn remove_authority_record(
    store: &RbacStore,
    key: &'static str,
) -> Result<(), RbacPersistError> {
    commit_authority_mutation(
        store,
        store.current_version()?,
        &format!("remove-{key}"),
        None,
        |owner_write| {
            owner_write
                .open_table(RBAC_TABLE)
                .map_err(RbacPersistError::Redb)?
                .remove(key)
                .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
            Ok(())
        },
    )
    .map(|_| ())
}

/// Mint and admit the maintenance batch, apply `apply_rows` to the `rbac` owner
/// table inside that one admitted write, and commit. The write transaction is
/// minted by the storage kernel's single mutation authority and is never
/// reachable here: the only handle this crate is given is the layout-bounded
/// [`AdmittedOwnerWrite`], valid between `owner_rows` and `finish_owner`.
fn commit_authority_mutation<F>(
    store: &RbacStore,
    expected_revision: u64,
    digest: &str,
    result_msgpack: Option<Vec<u8>>,
    apply_rows: F,
) -> Result<u64, RbacPersistError>
where
    F: FnOnce(&AdmittedOwnerWrite<'_, RbacOwner>) -> Result<(), RbacPersistError>,
{
    // Compare the caller's preimage before admission/replay under the SAME
    // owner write lock. Never upgrade a stale image to the latest revision.
    let mut stale = None;
    let (write, batch, begun) = store
        .mutations
        .admit_current(&store.owner, |expected| {
            if expected != expected_revision {
                stale = Some(RbacPersistError::StaleRevision {
                    expected: expected_revision,
                    actual: expected,
                });
                return Err("RBAC_STALE_REVISION".to_string());
            }
            let target = expected
                .checked_add(1)
                .ok_or_else(|| "identity/RBAC state version overflow".to_string())?;
            Ok(authority_batch(store, expected, target, digest))
        })
        .map_err(|error| stale.unwrap_or(RbacPersistError::Redb(error)))?;
    let source_version = match begun {
        // A current preimage always creates the next revision, never a prior
        // receipt. Do not acknowledge a replay without a committed revision.
        Begin::Replay(_) => {
            write.abort().map_err(RbacPersistError::Redb)?;
            return Err(RbacPersistError::IncompleteState(
                "unexpected authority write replay",
            ));
        }
        Begin::Apply { source_version } => source_version,
    };
    let owner_write = write
        .owner_rows(&store.owner, &batch)
        .map_err(RbacPersistError::Redb)?;
    apply_rows(&owner_write)?;
    owner_write.finish_owner().map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .finish(&write, &batch, result_msgpack, 0, source_version)
        .map_err(RbacPersistError::Redb)?;
    store
        .mutations
        .commit(write, &batch)
        .map_err(RbacPersistError::Redb)?;
    Ok(expected_revision + 1) // checked for overflow while building the batch
}
