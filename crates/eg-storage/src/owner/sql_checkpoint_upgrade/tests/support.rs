//! Exact old-image fixtures with genuine bound scopes and linked replay rows.

use super::*;
use crate::codec::{decode_ledger_record, encode_bounded};
use crate::owner::grant::ScopeGrantVerifier;
use crate::tables::{
    MutationClass, MutationClassRow, OperationReplayRow, RecordedOperation, BATCHES, CLASSES,
    FENCES, OUTBOX, PRIVATE_PAYLOADS, REPLAY_NONCES, REPLAY_OPERATIONS, VERSIONS,
};
use eg_types::contract::{Digest256, MethodId, Nonce};
use eg_types::mutation_batch::{
    method_schema_id, CommittedVersion, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatch, MutationBatchRecord, MutationBatchStatus, MutationEnvelope, MutationOperation,
    MutationOutboxIntent, MutationOutboxRecord, MutationSurface, VersionExpectation,
    BATCH_COMPILED_METHODS, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;
use eg_types::MutationScopeIdentity;
use std::collections::BTreeMap;

pub(super) const ACTOR: &str =
    "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const USER_ROWS: redb::TableDefinition<(&str, u64), &[u8]> =
    redb::TableDefinition::new("__sql_rows__");

struct SqlFixtureGrant;
impl ScopeGrantVerifier for SqlFixtureGrant {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout == OwnerLayout::Sql
            && identity.tenant().as_str() == "tenant-a"
            && principal == ACTOR
            && proof == b"verified"
        {
            Ok(())
        } else {
            Err("fixture grant rejected".to_string())
        }
    }
}

/// Inspection stages beside the database, and private direct-state staging
/// requires a mode-0700 parent regardless of the process umask.
pub(super) fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

pub(super) fn physical() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("eg-query:sql-user-tables").unwrap()
}

pub(super) fn scope() -> MutationScopeIdentity {
    MutationScopeIdentity::fixed_native(
        "tenant-a",
        DurabilityDomain::SqlCatalog,
        "source-fixture",
        eg_types::mutation_batch::COMPILED_BATCH_INCARNATION,
    )
    .unwrap()
}

pub(super) fn create_predecessor(path: &Path, prepared: bool) -> MutationBatchRecord {
    let options = StoreOpenOptions::default()
        .with_cache_bytes(UPGRADE_CACHE_BYTES)
        .unwrap();
    let kernel =
        StorageKernel::create_owner_with::<SqlOwner>(path, physical(), None, options).unwrap();
    let identity = scope();
    let grant = kernel
        .authenticate_scope::<SqlOwner>(
            &SqlFixtureGrant,
            identity.clone(),
            ACTOR.to_string(),
            b"verified",
        )
        .unwrap();
    kernel.bind_serving_scope(grant, 0).unwrap();
    let database = kernel.store().database();
    let write = database.begin_write().unwrap();
    let record = seed_operation(&write, &identity, prepared);
    let mut manifest = kernel.store().manifest().clone();
    manifest.layout_digest = contract::PRE_CHECKPOINT_LAYOUT;
    manifest.tables = contract::predecessor_contracts().unwrap();
    write.delete_table(SQL_SOURCE_CHECKPOINTS).unwrap();
    write
        .open_table(OWNER_MANIFEST)
        .unwrap()
        .insert(
            "manifest",
            encode_bounded(&manifest, "old fixture manifest")
                .unwrap()
                .as_slice(),
        )
        .unwrap();
    write
        .open_table(USER_ROWS)
        .unwrap()
        .insert(("tenant-a:source-table", 7), b"typed row".as_slice())
        .unwrap();
    let mut source_authority = [0_u8; 40];
    source_authority[..32].copy_from_slice(&[9_u8; 32]);
    source_authority[32..].copy_from_slice(&11_u64.to_be_bytes());
    write
        .open_table(super::super::super::registry::SQL_SOURCE_AUTHORITY)
        .unwrap()
        .insert("current", source_authority.as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(kernel);
    record
}

fn seed_operation(
    write: &redb::WriteTransaction,
    identity: &MutationScopeIdentity,
    prepared: bool,
) -> MutationBatchRecord {
    let batch = fixture_batch(identity, prepared);
    let key = identity.binding_digest().to_hex();
    let record = MutationBatchRecord {
        batch: batch.clone(),
        identity: identity.clone(),
        status: if prepared {
            MutationBatchStatus::Prepared
        } else {
            MutationBatchStatus::Committed
        },
        committed_version: if prepared {
            CommittedVersion::None
        } else {
            CommittedVersion::checked_native(0).unwrap()
        },
        result_msgpack: (!prepared).then(|| rmp_serde::to_vec_named(&17_u64).unwrap()),
        committed_at_ms: 12,
    };
    record.validate().unwrap();
    write
        .open_table(BATCHES)
        .unwrap()
        .insert(
            (key.as_str(), batch.batch_id.as_str()),
            encode_bounded(&record, "fixture batch").unwrap().as_slice(),
        )
        .unwrap();
    let class = MutationClassRow {
        identity: identity.clone(),
        batch_id: batch.batch_id.clone(),
        class: MutationClass::Operation,
    };
    write
        .open_table(CLASSES)
        .unwrap()
        .insert(
            (key.as_str(), batch.batch_id.as_str()),
            encode_bounded(&class, "fixture class").unwrap().as_slice(),
        )
        .unwrap();
    seed_replay(write, &batch);
    write
        .open_table(VERSIONS)
        .unwrap()
        .insert(key.as_str(), if prepared { 0 } else { 1 })
        .unwrap();
    let fence = crate::tables::ScopeFence {
        identity: identity.clone(),
        placement_epoch: 0,
        fencing_token: 1,
    };
    write
        .open_table(FENCES)
        .unwrap()
        .insert(
            key.as_str(),
            encode_bounded(&fence, "fixture fence").unwrap().as_slice(),
        )
        .unwrap();
    if prepared {
        write
            .open_table(PRIVATE_PAYLOADS)
            .unwrap()
            .insert((key.as_str(), batch.batch_id.as_str()), SEALED)
            .unwrap();
    } else {
        seed_outbox(write, &record);
    }
    record
}

fn fixture_batch(identity: &MutationScopeIdentity, prepared: bool) -> MutationBatch {
    let method = MethodId::new(BATCH_COMPILED_METHODS).unwrap();
    let envelope = MutationEnvelope::for_scope(
        CompiledScope {
            identity,
            actor: ACTOR,
            serving_principal: ACTOR,
            request_id: 7,
            idempotency_key: "source-fixture-key",
            nonce: Nonce::from_bytes([7; 32]),
            now_ms: 10,
        },
        CompiledOperation {
            method_schema_id: method_schema_id(&method).unwrap(),
            method,
            method_schema_digest: Digest256::from_bytes([1; 32]),
            canonical_payload_digest: Digest256::from_bytes([2; 32]),
        },
    )
    .unwrap();
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Query,
        domain: DurabilityDomain::SqlCatalog,
        method: Method::ApplyMutation {
            event_type: if prepared {
                eg_types::mutation_batch::PRIVATE_PAYLOAD_EVENT_TYPES[0].to_string()
            } else {
                "sql_source_fixture".to_string()
            },
            query: if prepared {
                format!("sha256:{}", SEALED_DIGEST)
            } else {
                "source batch".to_string()
            },
        },
    };
    let mut batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: "source-fixture-batch".to_string(),
        envelope,
        identity: identity.clone(),
        placement_epoch: 0,
        version_expectation: VersionExpectation::Native(0),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation],
        outbox: Vec::new(),
        created_at_ms: 10,
    };
    if !prepared {
        batch.outbox.push(MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: batch.batch_id.clone(),
            payload: vec![3],
            headers: BTreeMap::from([("actor".to_string(), ACTOR.to_string())]),
        });
    }
    batch
        .reseal_envelope(Digest256::from_bytes([1; 32]))
        .unwrap();
    batch.validate().unwrap();
    batch
}

fn seed_replay(write: &redb::WriteTransaction, batch: &MutationBatch) {
    let envelope = batch.envelope.operation().unwrap();
    let operation_replay_digest = envelope.operation_identity().unwrap().digest().unwrap();
    let nonce_replay_digest = envelope.nonce_replay_key().unwrap().digest().unwrap();
    let row = OperationReplayRow {
        identity: batch.identity.clone(),
        idempotency_key: batch.idempotency_key().to_string(),
        operation_replay_digest,
        nonce_replay_digest,
        batch_id: batch.batch_id.clone(),
        recorded: RecordedOperation::Batch(batch.batch_id.clone()),
    };
    let scope = batch.identity.binding_digest().to_hex();
    write
        .open_table(REPLAY_OPERATIONS)
        .unwrap()
        .insert(
            (scope.as_str(), batch.idempotency_key()),
            encode_bounded(&row, "fixture replay").unwrap().as_slice(),
        )
        .unwrap();
    write
        .open_table(REPLAY_NONCES)
        .unwrap()
        .insert(
            (scope.as_str(), nonce_replay_digest.to_hex().as_str()),
            batch.idempotency_key(),
        )
        .unwrap();
}

fn seed_outbox(write: &redb::WriteTransaction, record: &MutationBatchRecord) {
    let outbox = MutationOutboxRecord {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: record.batch.batch_id.clone(),
        ordinal: 0,
        identity: record.identity.clone(),
        committed_version: record.committed_version,
        commit_sequence: Some(1),
        intent: record.batch.outbox[0].clone(),
        created_at_ms: record.batch.created_at_ms,
    };
    let scope = record.identity.binding_digest().to_hex();
    write
        .open_table(OUTBOX)
        .unwrap()
        .insert(
            (scope.as_str(), record.batch.batch_id.as_str(), 0),
            encode_bounded(&outbox, "fixture outbox")
                .unwrap()
                .as_slice(),
        )
        .unwrap();
}

pub(super) const SEALED: &[u8] = b"authenticated fixture ciphertext";
pub(super) const SEALED_DIGEST: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub(super) struct FixtureIntegrity;
impl PrivatePayloadIntegrity for FixtureIntegrity {
    fn authenticate(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        if sealed == SEALED && digest == SEALED_DIGEST {
            Ok(())
        } else {
            Err("fixture private authentication rejected".to_string())
        }
    }
}

pub(super) fn snapshot_old(path: &Path) -> StrictRecoveryEvidence {
    let database = upgrade_builder().open_read_only(path).unwrap();
    let read = database.begin_read().unwrap();
    sql_pre_checkpoint_evidence(HashSnapshot::Read(&read)).unwrap()
}

pub(super) fn read_record(kernel: &StorageKernel) -> MutationBatchRecord {
    let read = kernel.store().begin_read().unwrap();
    let table = read.open_table(BATCHES).unwrap();
    let scope = scope().binding_digest().to_hex();
    decode_ledger_record(
        table
            .get((scope.as_str(), "source-fixture-batch"))
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
}
