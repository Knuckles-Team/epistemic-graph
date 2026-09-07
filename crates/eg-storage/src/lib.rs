//! Sole physical-state authority for the epistemic-graph workspace.
//!
//! [`StorageKernelV1`] alone opens and identifies durable stores, owns the
//! closed physical owner-table registry, issues scoped read, snapshot and write
//! capabilities, and validates adoption, backup, restore and recovery. No
//! server handler, domain crate, provider, sidecar, or wrapper may open a
//! database or become a second physical authority.
//!
//! Writes are reachable only through [`MutationOwnerAuthority`], the move-once
//! token [`StorageKernelV1::into_read_and_mutation_authority`] issues exactly
//! once per kernel. No code outside this crate can name one otherwise, so no
//! domain crate can obtain a write capability.

mod capability;
mod codec;
pub mod direct_state;
mod kernel;
mod owner;
mod payload;
mod physical;
mod recovery;
mod scoped;
mod tables;

pub use capability::{
    OwnerPayloadRetirement, PhysicalWriteCapability, ScopedRead, ScopedSnapshot,
};
pub use codec::{
    decode_batch_record, decode_ledger_record, decode_outbox_record, encode_bounded,
    CollectionBudget,
};
pub use kernel::{MutationOwnerAuthority, StorageKernelV1, StoreOpenOptions};
pub use owner::blob_shared::{
    BlobSharedRead, BlobSharedServiceHandle, BlobSharedServiceVerifier, BlobSharedTable,
    BlobSharedWrite, CasChunkRows, CasRefcountRows,
};
pub use owner::domain::{
    BlobOwner, ClusterHierarchyOwner, ColdTierOwner, GraphShardOwner, JobsOwner, KvOwner,
    LedgerOnlyOwner, NodeInfoOwner, OwnerDomain, PathIndexOwner, RbacOwner, RequestReplayOwner,
    SemanticIndexOwner, SqlOwner, StatechartOwner, TenantCatalogOwner, TimeSeriesOwner,
    VizProvenanceOwner,
};
pub use owner::grant::{AuthenticatedScopeGrant, ScopeGrantVerifier};
pub use owner::handle::OwnedStoreHandle;
pub use owner::identity::PhysicalStoreIdentity;
pub use owner::layout::OwnerLayout;
pub use owner::row_key::{
    is_control_scope, owner_row_key, reserved_control_graph, OwnerRowScope, RowKey,
    GRAPH_SHARD_CONTROL_GRAPH,
};
pub use owner::registry::{
    declared_table_names, owner_table_names, ANN_CODES, SEMANTIC_POINTERS, SEMANTIC_STATES,
};
pub use owner::table_api::*;
pub use payload::private_payload_digest;
pub use physical::binding::ledger_scope_key;
pub use scoped::{
    OwnerReadTable, ScopedOwnerTable, ScopedOwnerTableMut, ScopedTable, ScopedTableMut,
};
pub use physical::incarnation::{
    StoreIdentityDigest, StoreIncarnation, STORAGE_KERNEL_SCHEMA_VERSION,
};
pub use physical::integrity::PrivatePayloadIntegrity;
pub use physical::manifest::OwnerManifestDigest;
pub use physical::read_only::{open_read_only, ReadOnlyStore};
pub use recovery::adopt::{
    adopt_recovery, adopt_staged_mutation_store, classify_recovery_store,
    inspect_staged_mutation_store, open_recovery, ClassifiedRecoveryStore, RecoveryExpectation,
    ValidatedPlainRecoveryStore, ValidatedRecoveryStore, ValidatedStagedMutationStore,
};
pub use recovery::backup::{backup_recovery_store, recovery_store_fingerprint};
pub use recovery::evidence::{
    backup_strict_recovery_store, strict_recovery_evidence, StrictRecoveryEvidence,
    StrictTableEvidence,
};
pub use recovery::validate::{
    validate_recovery_store, validate_recovery_store_read_only, RecoveryStoreCounts,
};
pub use tables::{
    LedgerRowScope, MutationClass, MutationClassRow, OperationReplayRow, ScopeFence,
};
