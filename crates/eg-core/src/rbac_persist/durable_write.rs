use std::collections::{BTreeMap, BTreeSet};

use redb::ReadableDatabase;
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
    let expected = eg_mutation_store::version(&store.mutation_store, &store.identity)
        .map_err(RbacPersistError::Redb)?;
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
    let transaction = store
        .mutation_store
        .database()
        .begin_read()
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    let table = transaction
        .open_table(RBAC_TABLE)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
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
            principal:
                "principal:sha256:d70d97fc35a6e2dfbef26a2bca76a96c6dd2c4142ae2a14850deaf61b478bba0"
                    .to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // OCC supplies the expected version, so this path claims no
            // unversioned-system-mutation capability.
            verified_capabilities: BTreeSet::new(),
        },
        identity: store.identity.clone(),
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
    let write = store
        .mutation_store
        .write()
        .map_err(RbacPersistError::Redb)?;
    let source_version =
        match eg_mutation_store::begin(&write, batch).map_err(RbacPersistError::Redb)? {
            eg_mutation_store::Begin::Replay(_) => return Ok(()),
            eg_mutation_store::Begin::Apply { source_version } => source_version,
        };
    {
        let mut table = write
            .owner_rows()
            .open_table(RBAC_TABLE)
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        table
            .insert(POLICY_KEY, encoded.policy.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        table
            .insert(IDENTITIES_KEY, encoded.identities.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        table
            .insert(BOOTSTRAP_KEY, encoded.bootstrap.as_slice())
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    }
    let outcome = rmp_serde::to_vec_named(&true)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    eg_mutation_store::finish(&write, batch, Some(outcome), 0, source_version)
        .map_err(RbacPersistError::Redb)?;
    eg_mutation_store::commit(write, batch).map_err(RbacPersistError::Redb)
}
