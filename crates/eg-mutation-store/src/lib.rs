//! Atomic mutation-ledger persistence for graph and native authoritative stores.
//!
//! A physical store has one immutable [`StoreIncarnation`]. Each logical
//! [`MutationScopeIdentity`] is bound once to that root and receives its own
//! idempotency, OCC, fence, receipt, and outbox keyspace.

use eg_types::{
    mutation_batch::MutationCommitPhase, MutationBatch, MutationBatchRecord, MutationBatchStatus,
    MutationOutboxRecord, MutationScopeIdentity,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use sha2::{Digest, Sha256};

const MAX_MUTATION_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_MUTATION_RECORD_ITEMS: usize = 1_000_000;
const MAX_MUTATION_COLLECTION_ROWS: usize = 100_000;
const MAX_MUTATION_COLLECTION_BYTES: usize = 512 * 1024 * 1024;

pub(crate) const STORE_ROOT: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_store_root_v1");
pub(crate) const SCOPE_BINDINGS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_scope_bindings_v1");
pub(crate) const OWNER_MANIFEST: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_owner_manifest_v1");
pub(crate) const BATCHES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_batches_v1");
pub(crate) const IDEMPOTENCY: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_idempotency_v1");
pub(crate) const VERSIONS: TableDefinition<'static, &str, u64> =
    TableDefinition::new("mutation_versions_v1");
pub(crate) const FENCES: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_fences_v1");
pub(crate) const OUTBOX: TableDefinition<'static, (&str, &str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox_v1");
pub(crate) const OUTBOX_TOPIC_INDEX: TableDefinition<
    'static,
    (&str, &str, u64, u64, &str, u32),
    (),
> = TableDefinition::new("mutation_outbox_topic_index_v1");
pub(crate) const PRIVATE_PAYLOADS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_private_payloads_v1");
pub(crate) const OUTBOX_CONSUMERS: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_outbox_consumers_v1");
pub(crate) const OUTBOX_DELIVERIES: TableDefinition<'static, (&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox_deliveries_v1");
pub(crate) const OUTBOX_CURSORS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_cursors_v1");
pub(crate) const OUTBOX_CLAIM_CURSORS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_claim_cursors_v1");
pub(crate) const OUTBOX_FAIRNESS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_fairness_v1");

fn decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_MUTATION_RECORD_BYTES,
            MAX_MUTATION_RECORD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "stored mutation record is invalid or exceeds resource limits".to_string())
}

fn decode_batch_record(bytes: &[u8]) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_record(bytes)?;
    record.validate()?;
    Ok(record)
}

fn decode_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_record(bytes)?;
    record.validate()?;
    Ok(record)
}

#[derive(Default)]
struct CollectionBudget {
    rows: usize,
    bytes: usize,
}

impl CollectionBudget {
    fn account(&mut self, added: usize) -> Result<(), String> {
        self.rows = self
            .rows
            .checked_add(1)
            .filter(|count| *count <= MAX_MUTATION_COLLECTION_ROWS)
            .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
        self.bytes = self
            .bytes
            .checked_add(added)
            .filter(|count| *count <= MAX_MUTATION_COLLECTION_BYTES)
            .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Fence {
    placement_epoch: u64,
    fencing_token: u64,
}

/// Result of validating a proposed batch within its owner transaction.
#[derive(Debug, Clone)]
pub enum Begin {
    Apply { source_version: Option<u64> },
    Replay(Box<MutationBatchRecord>),
}

#[derive(Debug, Clone)]
pub enum SagaBegin {
    Execute,
    Resume(MutationBatchRecord),
    Committed(MutationBatchRecord),
}

#[path = "store/identity.rs"]
mod identity;
pub use identity::{
    adopt_restored_store, adopt_restored_store_if_mutation_store, bind_scope, initialize,
    open_read_only, MutationStore, MutationWrite, PrivatePayloadIntegrity, ReadOnlyMutationStore,
    StoreIdentityDigest, StoreIncarnation, MUTATION_STORE_SCHEMA_VERSION,
};
pub(crate) use identity::{
    binding_for_read, binding_for_write, decode_binding, reject_prototype_names,
    require_persisted_root, scope_identity_key, ScopeBinding, STORE_ROOT_KEY,
};

#[path = "store/admission.rs"]
mod admission;
pub(crate) use admission::AdmissionState;

#[path = "store/authority.rs"]
mod authority;
pub(crate) use authority::{reanchor_staged_store_authority, rewrite_store_authority};

#[path = "store/strict_recovery.rs"]
mod strict_recovery;
pub use strict_recovery::{
    backup_strict_recovery_store, strict_recovery_evidence, StrictRecoveryEvidence,
    StrictTableEvidence,
};
pub(crate) use strict_recovery::{strict_snapshot_read, strict_snapshot_write};

#[path = "store/owner_table_api.rs"]
mod owner_table_api;
pub use owner_table_api::*;

#[path = "store/blob_shared_access.rs"]
mod blob_shared_access;
pub use blob_shared_access::*;

#[path = "store/owner.rs"]
mod owner;
#[path = "store/owner_manifest_types.rs"]
mod owner_manifest_types;
pub use owner_manifest_types::OwnerManifestDigest;
#[path = "store/recovery_authority.rs"]
mod recovery_authority;
pub use recovery_authority::{
    adopt_recovery, adopt_staged_mutation_store, classify_recovery_store,
    inspect_staged_mutation_store, open_recovery, ClassifiedRecoveryStore, RecoveryExpectation,
    ValidatedPlainRecoveryStore, ValidatedRecoveryStore, ValidatedStagedMutationStore,
};
#[path = "store/owner_registry.rs"]
mod owner_registry;
pub use owner::{
    bind_serving_scope, create, open, AdmittedOwnerWrite, AuthenticatedScopeGrant, BlobOwner,
    JobsOwner, KvOwner, LedgerOnlyOwner, OwnerDomain, OwnerHandle, OwnerLayout,
    PhysicalStoreIdentity, RbacOwner, ScopeGrantVerifier, SemanticIndexOwner, StatechartOwner,
    TimeSeriesOwner,
};
pub(crate) use owner::{
    copy_declared_owner_tables, hash_declared_owner_tables, open_declared_owner_tables,
    read_current_manifest, validate_declared_owner_tables, validate_declared_tables_write,
    validate_manifest_read, validate_manifest_write, OwnerManifest,
};
pub use owner_registry::declared_table_names;

#[path = "store/ledger.rs"]
mod ledger;
pub(crate) use ledger::{
    encode_bounded, idempotency_batch_id, open_product_tables, persist_idempotency,
    persist_private, persist_record, private_payload_digest, read_private_in_write,
    read_record_in_write, recovery_plan_digest, remove_private, source_version,
    verify_replay_identity,
};

#[path = "store/persist.rs"]
mod persist;
pub(crate) use persist::validate_recovery_content;
pub use persist::{
    read_outbox, read_private_payload, read_record, validate_recovery_store,
    validate_recovery_store_read_only, version, RecoveryStoreCounts,
};

#[path = "store/recovery.rs"]
mod recovery;
pub use recovery::{backup_recovery_store, recovery_store_fingerprint};

#[path = "store/apply.rs"]
mod apply;
pub use apply::{begin, commit, finish, purge_scope};

#[path = "store/saga.rs"]
mod saga;
pub use saga::{commit_saga, prepare_saga, prepare_saga_with_private_payload};

#[cfg(test)]
#[path = "store/tests.rs"]
mod tests;
