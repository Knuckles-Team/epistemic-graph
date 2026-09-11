//! Pure redb durable-row machinery (CONCEPT:EG-KG.storage.kg-kg / KG-2.195 / KG-2.216).
//!
//! This is the SERVER-INDEPENDENT half of the redb durable tier: the on-disk
//! table layout, the `Method → redb rows` apply, the group-commit, and the
//! full checkpoint/load read-back. It has NO Tokio and NO `ServerState`
//! dependency, so it compiles under `--features redb` ALONE (no `server`).
//!
//! Two callers share it — ONE durable format, never duplicated:
//!   * the out-of-process server's `server::persistence::redb_backend::RedbBackend`
//!     (gated on `server`), which wraps these in its off-reactor group-commit
//!     writer thread + the `PersistenceBackend` async trait; and
//!   * the in-process [`crate::embedded::EmbeddedEngine`] (gated on `embedded`),
//!     which commits through them DIRECTLY (the caller is the writer — durable,
//!     commit-before-return, no Tokio runtime).
//!
//! The redb `Database` and every table key/value shape here are byte-identical to
//! what the server writes, so a graph written by the embedded API reopens in the
//! server and vice-versa.
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> identity + integrity-policy blob`

use eg_storage::{GraphShardOwner, OwnedStoreHandle, ScopedOwnerTableMut};
use eg_transaction::{AdmittedGroup, AdmittedOwnerWrite, Begin, OwnerPayloadWrite};
use redb::{ReadableTable, TableDefinition};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::change_envelope::{
    ChangeCursor, ChangeEnvelope, ChangeEnvelopeCommit, ChangeEnvelopeRecord, ContentVersion,
    MaterialOperation,
};
use crate::epistemic_operations::{
    ClaimWorkItemResult, ClaimWorkItemResultReason, ClaimWorkItemResultSchemaVersion,
    ResourceCapacity, ResourceCapacitySnapshot, ResourceHostUpdateCapacitySnapshot,
    ResourceHostUpdateDiskPolicySnapshot, ResourceHostUpdateRequest,
    ResourceHostUpdateRequestTargetKind, ResourceHostUpdateResult, ResourceHostUpdateResultReason,
    ResourceHostUpdateResultSchemaVersion, ResourceHostUpdateSnapshot,
    ResourceHostUpdateSnapshotTargetKind, ResourceRequirement,
    ResourceReservationDiskPolicySnapshot, ResourceReservationHostCapacitySnapshot,
    ResourceReservationHostSnapshot, ResourceReservationHostSnapshotTargetKind,
    ResourceReservationRecord, ResourceReservationRecordState, ResourceReservationRecordTargetKind,
    ResourceReservationRequest, ResourceReservationRequestTargetKind, ResourceReservationResult,
    ResourceReservationResultDecision, ResourceReservationResultSchemaVersion,
    ResourceReservationResultState, ResourceReservationStatusRequest,
    ResourceReservationStatusResult, ResourceReservationStatusResultSchemaVersion,
    ResourceReservationSummary, ResourceReservationSummaryState, ResourceTargetSnapshot,
    ResourceTargetSnapshotKind,
};
#[cfg(test)]
use crate::mutation_batch::MUTATION_BATCH_VERSION;
use crate::mutation_batch::{
    CommittedVersion, DurabilityDomain, LogicalName, MutationBatch, MutationBatchCommit,
    MutationBatchRecord, MutationBatchStatus, MutationOperation, MutationOutboxIntent,
    MutationOutboxRecord, MutationProjectionCursor, MutationScope, MutationSurface,
    VersionExpectation,
};
use crate::protocol::{GraphType, Method};

/// Durable rows are outside the native RPC frame validator and may be supplied
/// by a corrupted, restored, or otherwise untrusted database file. Keep one
/// format-wide ceiling aligned with the native protocol's hard request budget,
/// then structurally preflight every MessagePack row before serde can honor an
/// attacker-controlled collection size hint.
const MAX_DURABLE_MSGPACK_BYTES: usize = 384 * 1024 * 1024;
const MAX_DURABLE_STORED_BYTES: usize = MAX_DURABLE_MSGPACK_BYTES + 1024;
const MAX_DURABLE_MSGPACK_ITEMS: usize = 4_000_000;
const INITIAL_GRAPH_VERSION: u64 = 0;

fn durable_msgpack_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        MAX_DURABLE_MSGPACK_BYTES,
        MAX_DURABLE_MSGPACK_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

/// Return a native WorkItem/resource mutation with only authority-owned
/// lifecycle fields normalized. All other request fields remain serialized and
/// therefore participate in exact idempotency comparison. This is intentionally
/// a dedicated native replay seam, not a generic byte-comparison relaxation for
/// MutationBatch.
fn native_retry_method(method: &Method) -> Option<Method> {
    match method {
        Method::ReserveWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReserveWorkItemResources { request })
        }
        Method::ReleaseWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReleaseWorkItemResources { request })
        }
        Method::ReclaimWorkItemResources { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::ReclaimWorkItemResources { request })
        }
        Method::UpdateResourceHost { request } => {
            let mut request = request.clone();
            request.now_ms = 0;
            Some(Method::UpdateResourceHost { request })
        }
        Method::SubmitWorkItem { request } => {
            let mut request = request.clone();
            normalize_submit_context(&mut request.context);
            Some(Method::SubmitWorkItem { request })
        }
        Method::SubmitWorkItems { request } => {
            let mut request = request.clone();
            normalize_submit_context(&mut request.context);
            for child in &mut request.requests {
                normalize_submit_context(&mut child.context);
            }
            Some(Method::SubmitWorkItems { request })
        }
        _ => None,
    }
}

/// The transport/request context is provenance, but a retry may legitimately
/// carry a fresh request/trace/expiry window. Keep the security-bearing scope
/// and subject fields in the idempotency comparison while normalizing only the
/// authority-issued temporal/correlation fields.
fn normalize_submit_context(context: &mut crate::epistemic_operations::RequestContext) {
    context.request_id.clear();
    context.trace_id.clear();
    context.issued_at_ms = 0;
    context.expires_at_ms = 0;
    context.placement_epoch = None;
}

fn native_retry_method_key(method: &Method) -> Result<Option<Vec<u8>>, String> {
    native_retry_method(method)
        .map(|method| rmp_serde::to_vec_named(&method).map_err(|error| error.to_string()))
        .transpose()
}

fn native_retry_operations(operations: &[MutationOperation]) -> Option<Vec<MutationOperation>> {
    if operations.is_empty()
        || operations
            .iter()
            .any(|operation| !is_native_retry_method(&operation.method))
    {
        return None;
    }
    let methods = operations
        .iter()
        .map(|operation| native_retry_method(&operation.method))
        .collect::<Option<Vec<_>>>()?;
    Some(
        operations
            .iter()
            .zip(methods)
            .map(|(operation, method)| MutationOperation {
                ordinal: operation.ordinal,
                surface: operation.surface,
                domain: operation.domain,
                method,
            })
            .collect(),
    )
}

/// Encode the derived projection wake-up for an immutable operation list.
///
/// Native resource retries compare this derived intent after normalizing the
/// authority-owned lifecycle timestamp. Keeping the digest construction here
/// prevents the retry path from drifting from the producer in
/// `server::mutation_batch::finish_batch`.
///
/// BUG-PE-037: moved here from `server::mutation_batch` (which imports Tokio
/// and is gated on `server`) -- this and `projection_payload_for_operations`
/// below are pure (sha2/rmp_serde/serde_json only) and this module's own doc
/// comment above is explicit that it "compiles under `--features redb`
/// ALONE (no `server`)"; the prior location broke exactly that contract for
/// `native_retry_outbox_match`, below, one of this module's own callers.
/// `server::mutation_batch::finish_batch` now calls
/// `crate::redb_store::projection_payload_for_operations` instead of
/// keeping a second copy.
pub(crate) fn projection_summary_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    let encoded_operations = rmp_serde::to_vec_named(operations).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    rmp_serde::to_vec_named(&serde_json::json!({
        "schema": "epistemic.mutation.projection.v1",
        "operations": operations.len(),
        "operations_sha256": hex::encode(Sha256::digest(&encoded_operations)),
    }))
    .map_err(|e| e.to_string())
}

/// Encode the feature-aware projection wake-up payload for an operation list.
///
/// `epistemic-tms` replaces the ordinary summary with a typed
/// `ReasoningProjectionWakeup`. Retry reconciliation must derive the same
/// payload as the producer, including that feature-specific shape.
pub(crate) fn projection_payload_for_operations(
    operations: &[MutationOperation],
) -> Result<Vec<u8>, String> {
    #[cfg(feature = "epistemic-tms")]
    {
        use sha2::{Digest, Sha256};

        let encoded_operations =
            rmp_serde::to_vec_named(operations).map_err(|error| error.to_string())?;
        let methods = operations
            .iter()
            .map(|operation| operation.method.clone())
            .collect::<Vec<_>>();
        let wakeup = eg_epistemic::ReasoningProjectionWakeup::new(
            operations.len(),
            hex::encode(Sha256::digest(encoded_operations)),
            eg_epistemic::ReasoningProjectionWakeup::events_for_methods(&methods),
        )?;
        rmp_serde::to_vec_named(&wakeup).map_err(|error| error.to_string())
    }

    #[cfg(not(feature = "epistemic-tms"))]
    {
        projection_summary_for_operations(operations)
    }
}

/// Compare the derived projection wake-up for a retry.  Native resource
/// operations contain one authority-owned `now_ms`, so the raw outbox digest
/// changes when a retry is admitted at a later leader time even though the
/// immutable operation is identical.  Rebuild the digest from the same
/// normalized operation list used by the operation comparator, while keeping
/// topic/key/header metadata exact and rejecting arbitrary payload changes.
fn native_retry_outbox_match(
    stored_operations: &[MutationOperation],
    proposed_operations: &[MutationOperation],
    stored_outbox: &[MutationOutboxIntent],
    proposed_outbox: &[MutationOutboxIntent],
    operations_match: bool,
) -> Result<bool, String> {
    let Some(stored_normalized) = native_retry_operations(stored_operations) else {
        return Ok(stored_outbox == proposed_outbox);
    };
    let Some(proposed_normalized) = native_retry_operations(proposed_operations) else {
        return Ok(false);
    };
    if !operations_match || stored_outbox.len() != proposed_outbox.len() {
        return Ok(false);
    }
    let stored_normalized_payload = projection_payload_for_operations(&stored_normalized)?;
    let proposed_normalized_payload = projection_payload_for_operations(&proposed_normalized)?;
    if stored_normalized_payload != proposed_normalized_payload {
        return Ok(false);
    }
    // The producer hashes the original operation list, including its historical
    // authority-owned timestamp. Authenticate each stored/proposed intent against
    // its own operation list before comparing the normalized retry meaning; never
    // require an original intent to equal a digest that the producer did not emit.
    let stored_original_payload = projection_payload_for_operations(stored_operations)?;
    let proposed_original_payload = projection_payload_for_operations(proposed_operations)?;
    Ok(stored_outbox
        .iter()
        .zip(proposed_outbox)
        .all(|(stored, proposed)| {
            stored.topic == proposed.topic
                && stored.key == proposed.key
                && stored.headers == proposed.headers
                && stored.payload == stored_original_payload
                && proposed.payload == proposed_original_payload
        }))
}

fn is_native_retry_method(method: &Method) -> bool {
    matches!(
        method,
        Method::ReserveWorkItemResources { .. }
            | Method::ReleaseWorkItemResources { .. }
            | Method::ReclaimWorkItemResources { .. }
            | Method::UpdateResourceHost { .. }
            | Method::SubmitWorkItem { .. }
            | Method::SubmitWorkItems { .. }
    )
}

/// A resource batch may be replayed after its placement leader changes.  The
/// durable operation/idempotency key remains the retry identity; placement
/// metadata is historical routing proof and may advance monotonically for that
/// exact replay.  A backwards route is never accepted.
fn native_resource_placement_replay_match(
    stored: &MutationBatch,
    proposed: &MutationBatch,
    operations_match: bool,
) -> bool {
    if !operations_match
        || stored.operations.len() != 1
        || proposed.operations.len() != 1
        || !is_native_retry_method(&stored.operations[0].method)
        || !is_native_retry_method(&proposed.operations[0].method)
    {
        return false;
    }
    let stored_epoch = stored.placement_epoch;
    let proposed_epoch = proposed.placement_epoch;
    if proposed_epoch > stored_epoch {
        // A failover advances the catalog epoch but keeps the placement group's
        // fencing token.  Requiring the exact prior token prevents a caller from
        // manufacturing a higher epoch (or swapping in an unrelated group) at
        // the persistence boundary; dispatch supplies the current route proof.
        return stored.fencing_token.is_some() && proposed.fencing_token == stored.fencing_token;
    }
    proposed_epoch == stored_epoch && proposed.fencing_token == stored.fencing_token
}

/// One positional operation pair of a retry comparison.  Ordinal/surface/domain
/// must agree first (unchanged order), then the methods: both retry-keyed and
/// equal, or both unkeyed with byte-identical msgpack encodings.
fn mutation_operation_retry_match(
    stored: &MutationOperation,
    proposed: &MutationOperation,
) -> Result<bool, String> {
    if stored.ordinal != proposed.ordinal
        || stored.surface != proposed.surface
        || stored.domain != proposed.domain
    {
        return Ok(false);
    }
    match (
        native_retry_method_key(&stored.method)?,
        native_retry_method_key(&proposed.method)?,
    ) {
        (Some(stored_key), Some(proposed_key)) => Ok(stored_key == proposed_key),
        (None, None) => {
            let stored_bytes =
                rmp_serde::to_vec_named(&stored.method).map_err(|error| error.to_string())?;
            let proposed_bytes =
                rmp_serde::to_vec_named(&proposed.method).map_err(|error| error.to_string())?;
            Ok(stored_bytes == proposed_bytes)
        }
        (Some(_), None) | (None, Some(_)) => Ok(false),
    }
}

fn mutation_operations_retry_match(
    stored: &[MutationOperation],
    proposed: &[MutationOperation],
) -> Result<bool, String> {
    if stored.len() != proposed.len() {
        return Ok(false);
    }
    for (stored, proposed) in stored.iter().zip(proposed) {
        if !mutation_operation_retry_match(stored, proposed)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The operation envelope a graph-shard test fixture batch carries.
///
/// Minted through the one public constructor, so a fixture cannot become a
/// second minting path: everything it does not name is this deployment's
/// documented constant, exactly as a producer with no verified request carrier
/// gets. The attempt nonce is server-minted, so rebuilding a fixture for the
/// same key is a FRESH attempt over the same stable operation -- which is the
/// case the kernel must replay rather than refuse.
#[cfg(test)]
pub(crate) fn fixture_operation_envelope(
    identity: &eg_types::MutationScopeIdentity,
    actor: &str,
    request_id: u64,
    idempotency_key: &str,
) -> eg_types::mutation_batch::MutationEnvelope {
    let method =
        eg_types::contract::MethodId::new(eg_types::mutation_batch::BATCH_COMPILED_METHODS)
            .expect("the reserved batch method id is canonical");
    eg_types::mutation_batch::MutationEnvelope::for_scope(
        eg_types::mutation_batch::CompiledScope {
            identity,
            actor,
            serving_principal: actor,
            request_id,
            idempotency_key,
            nonce: eg_types::contract::Nonce::minted(),
            now_ms: 0,
        },
        eg_types::mutation_batch::CompiledOperation {
            method_schema_id: eg_types::mutation_batch::method_schema_id(&method)
                .expect("a reserved batch method has a derived schema id"),
            method,
            method_schema_digest: eg_types::contract::Digest256::from_bytes([0_u8; 32]),
            canonical_payload_digest: eg_types::contract::Digest256::from_bytes([1_u8; 32]),
        },
    )
    .expect("a fixture scope mints a valid operation envelope")
}

#[cfg(test)]
mod resource_reservation_tests;

pub(crate) mod capacity_lease;
#[cfg(feature = "redb")]
pub(crate) mod development_lane;
/// The graph shard as a kernel-owned store (RF-RULING-004 step 8).
#[cfg(feature = "redb")]
pub(crate) mod shard;
pub(crate) mod work_item_capability;

pub(crate) mod audit;
pub(crate) mod checkpoint;
pub(crate) mod control;
pub(crate) mod crossmodal;
pub(crate) mod dump;
pub(crate) mod resource;
pub(crate) mod work_item;

// The decomposition keeps the physical file as one `redb_store` API surface.
// Keep the root's imports and its server/embedded callers on that surface
// explicitly: child modules use `super::*`, while persistence code reaches the
// durable machinery through `crate::redb_store`, never through a second store.
#[cfg(feature = "security")]
pub(crate) use audit::{
    append_audit_entry, prove_inclusion, provenance_anchor_commit, provenance_leaf_hashes,
    verify_audit, AuditTailCache, ProvenanceAnchorCache,
};
pub(crate) use checkpoint::apply_checkpoint;
pub(crate) use control::{
    clear_xshard_decision, clear_xshard_prepare, get_xshard_decision, get_xshard_decision_retain,
    get_xshard_prepare, put_xshard_decision, put_xshard_prepare, put_xshard_recoverable_pending,
    scan_xshard_decisions, scan_xshard_prepares,
};
#[cfg(feature = "matview")]
pub(crate) use control::{
    delete_matview_operator_state, delete_plan_matview, put_matview_operator_state,
    put_plan_matview, scan_matview_operator_state, scan_plan_matviews,
};
#[cfg(feature = "compute-dist")]
pub(crate) use control::{put_matview, scan_matviews};
pub(crate) use crossmodal::{commit_crossmodal, CrossModalStaged};
pub(crate) use crossmodal::{BlobRefRow, VectorUpsert};
pub(crate) use dump::{
    decode_graph_meta_identity, decode_meta_record, encode_meta_record,
    encode_meta_with_incarnation, graph_meta_schema_version, new_incarnation_id, read_all_dumps,
    read_all_graph_meta, read_graph_dump, read_graph_dump_page, upgrade_legacy_graph_meta,
    GraphDumpPage, PageCursorRef,
};
pub(crate) use resource::{
    read_resource_reservation, read_resource_reservation_status, resource_decode, resource_encode,
    resource_host_update_snapshot_kind, resource_metadata_maps, resource_record_target_kind,
    resource_record_work_item_live, resource_request_from_record,
    resource_reservation_snapshot_kind, resource_text, resource_validate_work_item,
    MAX_RESOURCE_CLEAR_SCAN, MAX_RESOURCE_HOST_DISK_POLICIES,
};
pub(crate) use shard::{Shard, ShardWrite};
pub(crate) use work_item::{
    apply_submit_work_item_rows, apply_submit_work_items_rows, apply_work_item_rows,
    WorkItemCommitScope,
};

use self::crossmodal::apply_crossmodal_projection_rows;
use self::resource::{
    apply_resource_reservation_rows, resource_load_host, resource_target_selection_matches,
};

fn decode_durable<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(bytes, durable_msgpack_limits())
        .map_err(|_| "durable value is invalid or exceeds resource limits".to_string())
}

fn decode_mutation_batch_record(
    bytes: &[u8],
    expected_graph_fname: &str,
    expected_batch_id: &str,
) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_durable(bytes)?;
    record.validate()?;
    validate_graph_mutation_record_binding(&record, expected_graph_fname, expected_batch_id)?;
    Ok(record)
}

fn validate_graph_mutation_record_binding(
    record: &MutationBatchRecord,
    expected_graph_fname: &str,
    expected_batch_id: &str,
) -> Result<(), String> {
    let graph_name = match (
        record.status,
        record.identity.scope(),
        record.committed_version,
    ) {
        (
            MutationBatchStatus::Committed,
            MutationScope::Graph { graph, .. },
            CommittedVersion::Graph { .. },
        ) => graph.as_str(),
        _ => return Err("graph mutation store contains a non-graph committed record".to_string()),
    };
    if sanitize(graph_name) != expected_graph_fname {
        return Err("graph mutation record does not match its requested graph route".to_string());
    }
    if record.batch.batch_id != expected_batch_id {
        return Err("graph mutation record does not match its physical lookup key".to_string());
    }
    Ok(())
}

fn decode_mutation_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_durable(bytes)?;
    record.validate()?;
    if !matches!(record.committed_version, CommittedVersion::Graph { source, .. } if source != 0) {
        return Err("graph mutation store contains a non-graph outbox record".to_string());
    }
    Ok(record)
}

fn decode_mutation_projection_cursor(bytes: &[u8]) -> Result<MutationProjectionCursor, String> {
    let cursor: MutationProjectionCursor = decode_durable(bytes)?;
    cursor.validate()?;
    if !matches!(cursor.committed_version, CommittedVersion::Graph { source, .. } if source != 0) {
        return Err("graph mutation store contains a non-graph projection cursor".to_string());
    }
    Ok(cursor)
}

fn validate_graph_mutation_record_sizes(plaintext: &[u8], stored: &[u8]) -> Result<(), String> {
    if plaintext.len() > MAX_DURABLE_MSGPACK_BYTES {
        return Err("graph mutation record exceeds durable resource limits".to_string());
    }
    if stored.len() > MAX_DURABLE_STORED_BYTES {
        return Err("sealed graph mutation record exceeds durable resource limits".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct DurableOutboxDelivery {
    consumer: String,
    lease_epoch: u64,
    lease_until_ms: u64,
    attempt: u32,
    delivered_at_ms: Option<u64>,
}

/// Authoritative reservation row.  The immutable WorkItem admission record is
/// retained after release/reclaim; held totals and fairness debt are updated only
/// by this transaction, never by telemetry or a local scheduler mirror.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DurableResourceReservation {
    record: ResourceReservationRecord,
    held_cpu_weight: u64,
    held_memory_mib: u64,
    held_disk_mib: u64,
    held_process_slots: u64,
    fairness_debt: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DurableResourceHost {
    tenant_ref: String,
    host_ref: String,
    revision: u64,
    capacity: ResourceCapacity,
    observed: ResourceCapacity,
    heartbeat_at_ms: u64,
    heartbeat_ttl_ms: u64,
    now_ms: u64,
    draining: bool,
    quarantined: bool,
    labels: Vec<String>,
    target_kind: String,
    target_alias: Option<String>,
    disk_used_mib: u64,
    disk_capacity_mib: u64,
    held_cpu_weight: u64,
    held_memory_mib: u64,
    held_disk_mib: u64,
    held_process_slots: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct DurableResourceFairness {
    debt: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DurableResourceDiskPolicy {
    blocked: bool,
    low_watermark_mib: Option<u64>,
    high_watermark_mib: Option<u64>,
    revision: u64,
}

fn resource_reservation_host_capacity_snapshot(
    value: &ResourceCapacity,
) -> ResourceReservationHostCapacitySnapshot {
    ResourceReservationHostCapacitySnapshot {
        cpu_weight: value.cpu_weight,
        memory_mib: value.memory_mib,
        disk_mib: value.disk_mib,
        process_slots: value.process_slots,
    }
}

fn resource_host_update_capacity_snapshot(
    value: &ResourceCapacity,
) -> ResourceHostUpdateCapacitySnapshot {
    ResourceHostUpdateCapacitySnapshot {
        cpu_weight: value.cpu_weight,
        memory_mib: value.memory_mib,
        disk_mib: value.disk_mib,
        process_slots: value.process_slots,
    }
}

fn resource_reservation_disk_policy_snapshot(
    policy_key: String,
    policy: &DurableResourceDiskPolicy,
) -> ResourceReservationDiskPolicySnapshot {
    ResourceReservationDiskPolicySnapshot {
        policy_key,
        blocked: policy.blocked,
        low_watermark_mib: policy.low_watermark_mib,
        high_watermark_mib: policy.high_watermark_mib,
        revision: policy.revision,
    }
}

fn resource_host_update_disk_policy_snapshot(
    policy_key: String,
    policy: &DurableResourceDiskPolicy,
) -> ResourceHostUpdateDiskPolicySnapshot {
    ResourceHostUpdateDiskPolicySnapshot {
        policy_key,
        blocked: policy.blocked,
        low_watermark_mib: policy.low_watermark_mib,
        high_watermark_mib: policy.high_watermark_mib,
        revision: policy.revision,
    }
}

fn resource_reservation_host_snapshot(
    host: &DurableResourceHost,
    policies: &[(String, DurableResourceDiskPolicy)],
) -> Result<ResourceReservationHostSnapshot, String> {
    let mut labels = host.labels.clone();
    labels.sort();
    let mut policies = policies.to_vec();
    policies.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(ResourceReservationHostSnapshot {
        host_ref: host.host_ref.clone(),
        revision: host.revision,
        capacity: resource_reservation_host_capacity_snapshot(&host.capacity),
        observed: resource_reservation_host_capacity_snapshot(&host.observed),
        heartbeat_at_ms: host.heartbeat_at_ms,
        heartbeat_ttl_ms: host.heartbeat_ttl_ms,
        draining: host.draining,
        quarantined: host.quarantined,
        labels,
        target_kind: resource_reservation_snapshot_kind(&host.target_kind)?,
        target_alias: host.target_alias.clone(),
        disk_used_mib: host.disk_used_mib,
        disk_capacity_mib: host.disk_capacity_mib,
        held_cpu_weight: host.held_cpu_weight,
        held_memory_mib: host.held_memory_mib,
        held_disk_mib: host.held_disk_mib,
        held_process_slots: host.held_process_slots,
        disk_policies: policies
            .into_iter()
            .map(|(key, policy)| resource_reservation_disk_policy_snapshot(key, &policy))
            .collect(),
    })
}

fn resource_host_update_snapshot(
    host: &DurableResourceHost,
    policies: &[(String, DurableResourceDiskPolicy)],
) -> Result<ResourceHostUpdateSnapshot, String> {
    let mut labels = host.labels.clone();
    labels.sort();
    let mut policies = policies.to_vec();
    policies.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(ResourceHostUpdateSnapshot {
        host_ref: host.host_ref.clone(),
        revision: host.revision,
        capacity: resource_host_update_capacity_snapshot(&host.capacity),
        observed: resource_host_update_capacity_snapshot(&host.observed),
        heartbeat_at_ms: host.heartbeat_at_ms,
        heartbeat_ttl_ms: host.heartbeat_ttl_ms,
        draining: host.draining,
        quarantined: host.quarantined,
        labels,
        target_kind: resource_host_update_snapshot_kind(&host.target_kind)?,
        target_alias: host.target_alias.clone(),
        disk_used_mib: host.disk_used_mib,
        disk_capacity_mib: host.disk_capacity_mib,
        held_cpu_weight: host.held_cpu_weight,
        held_memory_mib: host.held_memory_mib,
        held_disk_mib: host.held_disk_mib,
        held_process_slots: host.held_process_slots,
        disk_policies: policies
            .into_iter()
            .map(|(key, policy)| resource_host_update_disk_policy_snapshot(key, &policy))
            .collect(),
    })
}

fn resource_collect_disk_policy_rows(
    policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    host_ref: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, DurableResourceDiskPolicy)>, String> {
    let prefix = format!("{host_ref}\0");
    let mut rows = Vec::new();
    for row in policies.scope_rows().map_err(|error| error.to_string())? {
        if rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
            return Err("resource disk-policy scan exceeds native bound".to_string());
        }
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, policy_key) = key.value();
        if row_graph != graph {
            return Err("resource disk-policy row escaped its scope".to_string());
        }
        if !policy_key.starts_with(&prefix) {
            if policy_key < prefix.as_str() {
                continue;
            }
            break;
        }
        let policy_key = policy_key
            .strip_prefix(&prefix)
            .ok_or_else(|| "resource disk-policy key escaped host scope".to_string())?;
        resource_text(policy_key, "resource disk_policy_key")?;
        let policy = resource_decode::<DurableResourceDiskPolicy>(value.value(), crypto)?;
        rows.push((policy_key.to_string(), policy));
    }
    Ok(rows)
}

pub(crate) const NODES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("nodes");
pub(crate) const EDGES: TableDefinition<(&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("edges");
pub(crate) const LEDGER: TableDefinition<(&str, u64), &str> = TableDefinition::new("ledger");
pub(crate) const SEMANTIC: TableDefinition<&str, &[u8]> = TableDefinition::new("semantic_store");
// Tamper-evident hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security, feature `security`). One
// row per durable mutation, keyed `(graph, seq)`, value = `prev_hash | entry_hash |
// line` (see `crate::audit`). Appended in the SAME WriteTransaction as the mutation
// it records, so the audit entry and the data it audits are durable together. The
// table const is always defined (so the layout is stable) but only WRITTEN/READ under
// `security`.
pub(crate) const AUDIT: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("audit_chain");
// Provenance-anchor MEMBER list (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring): `(graph,
// audit_seq) -> msgpack Vec<(node_id, leaf_hash_bytes)>` for the `:ToolCall`/`:RunTrace`
// window folded into the audit-chain entry at that exact `seq` (a
// `PROVENANCE_ANCHOR|...` line, see `crate::audit::provenance_anchor_line`). Keyed
// by the SAME seq as its audit entry so the two correlate with no extra index. The
// anchored ROOT itself is never trusted from this table -- only from the
// tamper-evident AUDIT entry at that seq -- so tampering this side table cannot
// forge a passing inclusion proof; it can only make an otherwise-valid proof fail
// closed (see `crate::redb_store::prove_inclusion`).
pub(crate) const PROVENANCE_ANCHOR_MEMBERS: TableDefinition<(&str, u64), &[u8]> =
    TableDefinition::new("provenance_anchor_members");
pub(crate) const GRAPH_META: TableDefinition<&str, &[u8]> = TableDefinition::new("graph_meta");
/// Monotonic native WorkItem command sequence, scoped by graph.  The sequence
/// is allocated in the same transaction as the submitted WorkItem row and its
/// mutation/outbox record, so replay never allocates a second command number.
pub(crate) const WORK_ITEM_COMMAND_SEQUENCE: TableDefinition<&str, u64> =
    TableDefinition::new("work_item_command_sequence");
/// Native reservation identity, keyed `(graph, reservation_id)`.
pub(crate) const RESOURCE_RESERVATIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("resource_reservations");
/// Tenant-scoped reservation keyset for bounded status pagination. The value
/// repeats the reservation id so a repaired index can be validated without
/// trusting a caller-provided cursor.
pub(crate) const RESOURCE_RESERVATION_TENANT_INDEX: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("resource_reservation_tenant_index");
/// One immutable reservation winner per `(graph, WorkItem, attempt)`.
pub(crate) const RESOURCE_RESERVATION_ATTEMPTS: TableDefinition<(&str, &str, u64), &str> =
    TableDefinition::new("resource_reservation_attempts");
/// Native host capacity/telemetry and held accounting.
pub(crate) const RESOURCE_HOSTS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("resource_hosts");
/// Global repository/branch/concurrency ownership keys.
pub(crate) const RESOURCE_EXCLUSIVITY: TableDefinition<(&str, &str), &str> =
    TableDefinition::new("resource_exclusivity");
/// Native fairness service debt by tenant/group.
pub(crate) const RESOURCE_FAIRNESS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("resource_fairness");
/// Exact active count for each global concurrency key.  The count is updated
/// with the reservation row, so admission never scans an unbounded history.
pub(crate) const RESOURCE_CONCURRENCY: TableDefinition<(&str, &str), u64> =
    TableDefinition::new("resource_concurrency");
/// Host-local anti-affinity tag counts, updated with reservation lifecycle rows.
pub(crate) const RESOURCE_ANTI_AFFINITY: TableDefinition<(&str, &str, &str), u64> =
    TableDefinition::new("resource_anti_affinity");
/// Per-host/profile disk hysteresis.  The blocked bit is native policy state,
/// never a caller-provided telemetry field, and is retained across restarts.
pub(crate) const RESOURCE_DISK_POLICIES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("resource_disk_policies");
/// Complete governed envelope retained for replay/audit reconciliation.
pub(crate) const CHANGE_ENVELOPES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("change_envelopes");
/// Current content version, scoped by verified tenant and owning graph.
pub(crate) const CONTENT_VERSIONS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("content_versions");
/// Current typed source cursor. No component is converted into a filename.
pub(crate) const CHANGE_CURSORS: TableDefinition<(&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("change_cursors");
pub(crate) const CHANGE_BLOBS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("change_blobs");
pub(crate) const CHANGE_FEATURES: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("change_features");
pub(crate) const CHANGE_EVIDENCE: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("change_evidence");
pub(crate) const CHANGE_POLICIES: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("change_policies");
pub(crate) const CHANGE_LINEAGE: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("change_lineage");
// Durable Raft log table (CONCEPT:EG-KG.storage.one-fsync-covers-raft). Defined here so `commit_ops` (shared
// with the server's group-commit writer, which folds replicated log appends into
// the SAME `WriteTransaction` as graph mutations) is self-contained. The embedded
// path never appends log ops, so this table stays empty for an embedded-only DB —
// the const costs nothing and keeps the two callers on one durable layout.
pub(crate) const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");
// Cross-shard 2PC prepare records (CONCEPT:EG-KG.storage.lane-n-increment). One row per participant group
// of an in-flight cross-shard transaction, keyed by `(txn_id, group_id)`, holding
// that group's PREPARED-but-not-applied slice (its staged write-set). Durable so an
// in-doubt txn survives a coordinator/participant crash between PREPARE and COMMIT
// and is resolved on restart. Lives in the authoritative shard for the same-file reason
// as the Raft log; the PURE put/clear/scan logic lives here (shared store) next to
// NODES/EDGES/purge_graph_rows, while the writer-thread `Cmd` arms in `redb_backend`
// call into it (mirrors how the graph-row machinery is shared, CONCEPT:EG-KG.backend.engine-modes).
pub(crate) const XSHARD_PREPARE: TableDefinition<(&str, u64), &[u8]> =
    TableDefinition::new("xshard_prepare");
// The coordinator's durable DECISION record for a cross-shard txn, keyed by `txn_id`
// (CONCEPT:EG-KG.storage.lane-n-increment). Values `0/1` are ordinary ABORT/COMMIT;
// `2/3` are ABORT/COMMIT retained for a separate MutationBatch parent; `4` is a
// recoverable attempt started before phase 1 but not yet decided. Writing a terminal
// value is the ATOMIC COMMIT POINT: once it reads COMMIT every participant applies on
// recovery; absent/pending/ABORT ⇒ no new participant applies (presumed-abort).
pub(crate) const XSHARD_DECISION: TableDefinition<&str, u8> =
    TableDefinition::new("xshard_decision");
// Named distributed-compute MATERIALIZED VIEWS (CONCEPT:EG-KG.storage.feature). One row per matview
// keyed by `name`, holding the MessagePack-serialized `MatView` (its definition +
// current result rows). Durable so a matview survives restart; the handler reloads the
// in-RAM `MatViewStore` from this table on boot and refreshes incrementally on a delta.
// Lives in the authoritative shard for the same-file reason as the Raft log + xshard rows.
pub(crate) const MATVIEWS: TableDefinition<&str, &[u8]> = TableDefinition::new("matviews");

// Named PLAN-BACKED materialized views (CONCEPT:EG-KG.storage.plan-backed-matview). One
// row per matview keyed by `name`, holding the MessagePack-serialized plan-backed
// DEFINITION (name + target graph + `wire::Plan` bytes + reorder hint) — NOT the result
// rows, which ride the version-keyed result cache. DISJOINT table from the algo-only
// `matviews` table above (and from Lane D's secondary-index tables): a distinct redb
// table name, so the two matview families and the index rows never collide. Reloaded into
// the in-RAM plan-matview manager on boot.
pub(crate) const PLAN_MATVIEWS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("plan_matviews");

// DBSP INCREMENTAL operator state (CONCEPT:EG-KG.storage.incremental-matview): the durable
// snapshot of an incrementally-maintained plan-backed matview's circuit state (its
// membership map / per-bucket accumulators + CDC watermark), keyed by view name. The
// direct analogue of turso's `dbsp_state` btree, scoped down to redb. DISJOINT from
// `plan_matviews` (that table holds the DEFINITION; this holds the maintained STATE).
// Written when an incremental view is defined and dropped with it.
pub(crate) const MATVIEW_OPERATOR_STATE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("matview_operator_state");

/// Retire one graph's owner rows when its scope is retired.
///
/// The payload half of a scope retirement: `MutationKernel::purge_scope_with`
/// and the graft both require it, for the same reason -- retiring a scope's
/// ledger authority while leaving its rows behind would hand the retired
/// generation's data to the next binding of the same logical graph name.
///
/// Every sweep goes through `scoped_owner_table_mut` + `purge_scope_rows`,
/// whose scope comes from the CAPABILITY and never from an argument, so
/// holding one graph's handle can never wipe another's rows in the file they
/// share. It is deliberately not `PhysicalWriteCapability::purge_scoped_rows`:
/// that is the LEDGER sweep, keyed by the 64-hex binding digest, and an owner
/// key leads with the graph NAME -- on these tables it would match nothing and
/// return `Ok(())` having removed no row.
///
/// The twelve file-wide tables are deliberately NOT swept: they belong to the
/// file, not to any one graph, and a retired graph owns no row in them. The
/// catalog row is removed by [`remove_graph_catalog_row`] on the control scope.
pub(crate) struct GraphShardRetirement;

/// Sweep one scope-prefixed owner table for the capability's own scope.
fn retire_scoped_table<K, V>(
    write: &eg_storage::PhysicalWriteCapability<'_, GraphShardOwner>,
    definition: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
    V: redb::Value + 'static,
{
    write.scoped_owner_table_mut(definition)?.purge_scope_rows()
}

impl eg_storage::OwnerPayloadRetirement<GraphShardOwner> for GraphShardRetirement {
    fn retire_owner_payload(
        &self,
        write: &eg_storage::PhysicalWriteCapability<'_, GraphShardOwner>,
        _scope: &eg_types::MutationScopeIdentity,
    ) -> Result<(), String> {
        retire_scoped_table(write, NODES)?;
        retire_scoped_table(write, EDGES)?;
        retire_scoped_table(write, LEDGER)?;
        retire_scoped_table(write, SEMANTIC)?;
        retire_scoped_table(write, AUDIT)?;
        retire_scoped_table(write, PROVENANCE_ANCHOR_MEMBERS)?;
        retire_scoped_table(write, WORK_ITEM_COMMAND_SEQUENCE)?;
        retire_scoped_table(write, RESOURCE_RESERVATIONS)?;
        retire_scoped_table(write, RESOURCE_RESERVATION_TENANT_INDEX)?;
        retire_scoped_table(write, RESOURCE_RESERVATION_ATTEMPTS)?;
        retire_scoped_table(write, RESOURCE_HOSTS)?;
        retire_scoped_table(write, RESOURCE_EXCLUSIVITY)?;
        retire_scoped_table(write, RESOURCE_FAIRNESS)?;
        retire_scoped_table(write, RESOURCE_CONCURRENCY)?;
        retire_scoped_table(write, RESOURCE_ANTI_AFFINITY)?;
        retire_scoped_table(write, RESOURCE_DISK_POLICIES)?;
        retire_scoped_table(write, CHANGE_ENVELOPES)?;
        retire_scoped_table(write, CONTENT_VERSIONS)?;
        retire_scoped_table(write, CHANGE_CURSORS)?;
        retire_scoped_table(write, CHANGE_BLOBS)?;
        retire_scoped_table(write, CHANGE_FEATURES)?;
        retire_scoped_table(write, CHANGE_EVIDENCE)?;
        retire_scoped_table(write, CHANGE_POLICIES)?;
        retire_scoped_table(write, CHANGE_LINEAGE)?;
        capacity_lease::retire_graph_rows(write)?;
        work_item_capability::retire_graph_rows(write)?;
        development_lane::retire_graph_rows(write)?;
        Ok(())
    }
}

/// In-doubt cross-shard prepare records `(txn_id, group_id, slice-blob)` returned by
/// the recovery scan (CONCEPT:EG-KG.storage.lane-n-increment).
pub(crate) type XshardPrepareScan = Result<Vec<(String, u64, Vec<u8>)>, String>;
/// Durable 2PC decisions `(opaque_parent_id, outcome, retained_for_parent)`.
/// `outcome=None` is the recoverable protocol-start marker (not yet decided).
pub(crate) type XshardDecisionScan = Result<Vec<(String, Option<bool>, bool)>, String>;

/// Persisted materialized views `(name, blob)` returned by the boot reload scan
/// (CONCEPT:EG-KG.storage.feature). Shared by the algo-only distributed matview
/// (`compute-dist`) and the plan-backed incremental matview (`matview`) scan surfaces.
#[cfg(any(feature = "compute-dist", feature = "matview"))]
pub(crate) type MatViewScanResult = Result<Vec<(String, Vec<u8>)>, String>;

/// Encryption-at-rest cipher handle threaded through the durable read/write paths
/// (CONCEPT:EG-KG.sharding.row-level-security). A thin wrapper so the SAME function signatures carry it
/// whether or not the `security` feature is compiled: without `security` it is a
/// zero-sized no-op (every `seal`/`unseal` is the identity), so the durable format
/// and code path are byte-for-byte unchanged; with `security` + a configured key it
/// holds the AEAD that seals value blobs on write and unseals on read.
#[derive(Clone, Copy, Default)]
pub struct DurableCrypto<'a> {
    #[cfg(feature = "security")]
    cipher: Option<&'a crate::crypto::ValueCipher>,
    #[cfg(not(feature = "security"))]
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> DurableCrypto<'a> {
    /// A no-op handle (encryption off / not compiled).
    pub fn none() -> Self {
        DurableCrypto::default()
    }

    /// Wrap an optional cipher (the `security` path).
    #[cfg(feature = "security")]
    pub fn new(cipher: Option<&'a crate::crypto::ValueCipher>) -> Self {
        DurableCrypto { cipher }
    }

    /// Seal a value blob for storage. Identity when no cipher is active.
    ///
    /// Returns `Cow` so the **encryption-OFF** path (the default — no key configured)
    /// BORROWS the caller's plaintext with ZERO allocation/copy (CONCEPT:EG-KG.storage.redb-store #4): the
    /// bytes are handed straight to redb's `insert` via `as_ref()`, byte-for-byte
    /// identical to what `plaintext.to_vec()` produced before, so the on-disk format is
    /// unchanged. Only when a cipher IS active does it allocate the owned ciphertext
    /// (seal + encrypt, behavior unchanged). This removes a per-op heap allocation +
    /// memcpy of every node/edge/property value blob from inside the held write txn on
    /// the CPU-bound writer thread, for the common at-rest-encryption-off deployment.
    #[inline]
    fn seal<'b>(&self, plaintext: &'b [u8]) -> Cow<'b, [u8]> {
        #[cfg(feature = "security")]
        if let Some(c) = self.cipher {
            return Cow::Owned(c.seal(plaintext));
        }
        Cow::Borrowed(plaintext)
    }

    /// Unseal a stored value blob. Plaintext is accepted only in a deployment whose
    /// configured current format is plaintext. A sealed blob without its cipher and
    /// an unsealed blob with an active cipher both fail closed.
    #[inline]
    pub(crate) fn unseal(&self, stored: &[u8]) -> Result<Vec<u8>, String> {
        if stored.len() > MAX_DURABLE_STORED_BYTES {
            return Err("durable value exceeds resource limits".to_string());
        }
        #[cfg(feature = "security")]
        if let Some(c) = self.cipher {
            let plaintext = c.unseal(stored)?;
            if plaintext.len() > MAX_DURABLE_MSGPACK_BYTES {
                return Err("durable value exceeds resource limits".to_string());
            }
            return Ok(plaintext);
        }
        #[cfg(feature = "security")]
        if crate::crypto::is_sealed(stored) {
            return Err("encrypted durable value requires configured key material".to_string());
        }
        Ok(stored.to_vec())
    }
}

#[cfg(test)]
mod shard_control_tests;

/// The shard file's own control scope.
///
/// `OwnerLayout::GraphShard` declares `DurabilityDomain::GraphRows`, which may
/// never own a native scope (`may_own_native_scope() == false`), so EVERY scope
/// bound to a shard file is a graph scope -- including the one the file's own
/// file-wide rows are written under. RF-RULING-008's group admission needs
/// exactly one such control scope per shard write transaction: it is the member
/// that may reach the Raft log/metadata, the cross-shard prepare/decision rows,
/// the materialized views, the encryption canary and the cross-modal series
/// rows, none of which belong to any one graph.
///
/// The literal has ONE owner, `eg_storage::GRAPH_SHARD_CONTROL_GRAPH`: the
/// storage kernel refuses the name on any layout that does not reserve it and
/// reads the group's control class off the identity, so the kernel guard and the
/// durable chokepoints below cannot disagree about which name it is.
///
/// It is therefore a REAL graph name that no user may ever hold. The name is
/// bracketed like `__commons__` (a real, user-visible graph) but is refused at
/// every durable chokepoint below, so a tenant cannot create it, write to it,
/// register its identity, or purge it -- and a shard that already carries a user
/// graph under this name cannot exist, because no path could have created one.
pub const SHARD_CONTROL_GRAPH: &str = eg_storage::GRAPH_SHARD_CONTROL_GRAPH;

/// Refuse a durable operation that names the shard's own control scope.
///
/// Enforced at the CHOKEPOINTS rather than at the entrypoints: the durable
/// identity writer ([`write_graph_meta_with_incarnation`]), the coalesced write
/// path ([`commit_ops`]), the cross-modal commit's implicit identity backfill
/// ([`backfill_crossmodal_graph_meta`]) and the whole-graph teardown
/// ([`purge_graph_rows`]). Every server, embedded, Raft-apply and checkpoint
/// entrypoint reaches the durable tier through one of those four, so guarding
/// them covers paths this module does not own.
pub fn reject_reserved_graph(graph: &str) -> Result<(), String> {
    if graph == SHARD_CONTROL_GRAPH || sanitize(graph) == SHARD_CONTROL_GRAPH {
        return Err(format!(
            "'{SHARD_CONTROL_GRAPH}' is the shard's reserved control scope and cannot be used as a graph"
        ));
    }
    Ok(())
}

/// Map a logical graph name to the bounded durable key used by the served and
/// embedded paths. Escaping avoids collisions caused by lossy path replacement.
pub fn sanitize(name: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut key = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        use std::fmt::Write as _;
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            key.push(char::from(byte));
        } else {
            write!(&mut key, "~{byte:02x}").expect("writing to String cannot fail");
        }
    }
    if key.len() <= 200 {
        return key;
    }
    let digest = Sha256::digest(name.as_bytes());
    let mut bounded = String::with_capacity(66);
    bounded.push_str("~h");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut bounded, "{byte:02x}").expect("writing to String cannot fail");
    }
    bounded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphDumpKind {
    InPlaceCoreCheckpoint,
    DurableReadOnlyMaterialization,
}

/// The complete in-memory `GraphCore` image accepted by an in-place checkpoint.
///
/// Keeping this input distinct from [`GraphDump`] makes the authority boundary
/// structural: callers cannot accidentally attach durable native rows or reuse a
/// read-only materialization while constructing an ordinary checkpoint.
pub(crate) struct InPlaceCoreCheckpoint {
    pub graph: String,
    pub name: String,
    pub graph_type: GraphType,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub ledger: Vec<String>,
    pub semantic: Vec<u8>,
}

/// An owned, off-lock view of one graph used by checkpoint and materialization paths.
///
/// This is deliberately not a cross-store transfer image. Durable reads include
/// diagnostic native rows but omit other graph-scoped authorities. Complete moves
/// must use the fenced `RedbBackend::reshard_graph` / `RawGraphRows` protocol.
pub struct GraphDump {
    kind: GraphDumpKind,
    pub graph: String,
    pub name: String,
    pub graph_type: GraphType,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub ledger: Vec<String>,
    pub semantic: Vec<u8>,
    /// Diagnostic/read-only native `development_lane_*`/`resource_*` rows for
    /// this graph (BUG-CX-096). They make omissions observable to readers; they
    /// are not a complete authority-transfer contract and are never applied by
    /// an ordinary in-place checkpoint.
    pub native: NativeOperationDumpRows,
}

impl GraphDump {
    pub(crate) fn in_place_core_checkpoint(checkpoint: InPlaceCoreCheckpoint) -> Self {
        Self {
            kind: GraphDumpKind::InPlaceCoreCheckpoint,
            graph: checkpoint.graph,
            name: checkpoint.name,
            graph_type: checkpoint.graph_type,
            incarnation_id: checkpoint.incarnation_id,
            source_snapshot_version: checkpoint.source_snapshot_version,
            integrity_policy: checkpoint.integrity_policy,
            nodes: checkpoint.nodes,
            edges: checkpoint.edges,
            ledger: checkpoint.ledger,
            semantic: checkpoint.semantic,
            native: NativeOperationDumpRows::default(),
        }
    }

    fn validate_in_place_checkpoint(&self) -> Result<(), String> {
        if self.kind != GraphDumpKind::InPlaceCoreCheckpoint {
            return Err(
                "checkpoint refused a durable read-only dump; use RedbBackend::reshard_graph for cross-store transfer"
                    .to_string(),
            );
        }
        if !self.native.is_empty() {
            return Err(
                "checkpoint refused native authority rows; use RedbBackend::reshard_graph for cross-store transfer"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// The graph-scoped row set of every `development_lane_*` (10 tables) and
/// `resource_*` (9 tables) redb table, keyed by the NON-graph part of each
/// table's real key (the dump's own `graph` field already carries the graph
/// component). Every value blob that is sealed at rest in redb (an `&[u8]`
/// column, decoded via `resource_decode`/`DurableLaneHold`/
/// `DurableResourceReservation`-style wrappers elsewhere in this module) is
/// stored here UNSEALED — the plaintext form, the same convention
/// [`read_graph_dump`] already uses for `nodes`/`edges`/`semantic`. A plain
/// text or numeric index column is stored as-is (never sealed on disk).
#[derive(Default)]
pub struct NativeOperationDumpRows {
    /// `development_lane_holds`: `(graph, hold_id) -> sealed DurableLaneHold`.
    pub development_lane_holds: Vec<(String, Vec<u8>)>,
    /// `development_lane_tenant_index`: `(graph, tenant, hold_id) -> hold_id`.
    pub development_lane_tenant_index: Vec<((String, String), String)>,
    /// `development_lane_lane_index`: `(graph, tenant, lane_id) -> hold_id`.
    pub development_lane_lane_index: Vec<((String, String), String)>,
    /// `development_lane_repository_branch_index`: `(graph, tenant, branch) -> hold_id`.
    pub development_lane_repository_branch_index: Vec<((String, String), String)>,
    /// `development_lane_worktree_index`: `(graph, worktree) -> hold_id`.
    pub development_lane_worktree_index: Vec<(String, String)>,
    /// `development_lane_work_item_index`: `(graph, tenant, attempt) -> hold_id`.
    pub development_lane_work_item_index: Vec<((String, u64), String)>,
    /// `development_lane_counters`: `(graph, scope) -> sealed counters blob`.
    pub development_lane_counters: Vec<(String, Vec<u8>)>,
    /// `development_lane_pressure_index`: `(graph, tenant, scope, metric, value, counter_key) -> marker`.
    #[allow(clippy::type_complexity)]
    pub development_lane_pressure_index: Vec<((String, String, String, u64, String), u8)>,
    /// `development_lane_policies`: `(graph, tenant) -> sealed policy blob`.
    pub development_lane_policies: Vec<(String, Vec<u8>)>,
    /// `development_lane_invocations`: `(graph, tenant, key) -> sealed replay blob`.
    pub development_lane_invocations: Vec<((String, String), Vec<u8>)>,
    /// `resource_reservations`: `(graph, reservation_id) -> sealed DurableResourceReservation`.
    pub resource_reservations: Vec<(String, Vec<u8>)>,
    /// `resource_reservation_tenant_index`: `(graph, tenant, reservation_id) -> reservation_id`.
    pub resource_reservation_tenant_index: Vec<((String, String), String)>,
    /// `resource_reservation_attempts`: `(graph, work_item, attempt) -> reservation_id`.
    pub resource_reservation_attempts: Vec<((String, u64), String)>,
    /// `resource_hosts`: `(graph, host_ref) -> sealed DurableResourceHost`.
    pub resource_hosts: Vec<(String, Vec<u8>)>,
    /// `resource_exclusivity`: `(graph, key) -> reservation_id`.
    pub resource_exclusivity: Vec<(String, String)>,
    /// `resource_fairness`: `(graph, group) -> sealed fairness blob`.
    pub resource_fairness: Vec<(String, Vec<u8>)>,
    /// `resource_concurrency`: `(graph, key) -> active count`.
    pub resource_concurrency: Vec<(String, u64)>,
    /// `resource_anti_affinity`: `(graph, host, tag) -> count`.
    pub resource_anti_affinity: Vec<((String, String), u64)>,
    /// `resource_disk_policies`: `(graph, key) -> sealed disk-policy blob`.
    pub resource_disk_policies: Vec<(String, Vec<u8>)>,
}

impl NativeOperationDumpRows {
    fn is_empty(&self) -> bool {
        [
            self.development_lane_holds.len(),
            self.development_lane_tenant_index.len(),
            self.development_lane_lane_index.len(),
            self.development_lane_repository_branch_index.len(),
            self.development_lane_worktree_index.len(),
            self.development_lane_work_item_index.len(),
            self.development_lane_counters.len(),
            self.development_lane_pressure_index.len(),
            self.development_lane_policies.len(),
            self.development_lane_invocations.len(),
            self.resource_reservations.len(),
            self.resource_reservation_tenant_index.len(),
            self.resource_reservation_attempts.len(),
            self.resource_hosts.len(),
            self.resource_exclusivity.len(),
            self.resource_fairness.len(),
            self.resource_concurrency.len(),
            self.resource_anti_affinity.len(),
            self.resource_disk_policies.len(),
        ]
        .into_iter()
        .sum::<usize>()
            == 0
    }
}

/// Commit one drained burst -- every buffered mutation plus any Raft log
/// appends -- as ONE admitted scope group (CONCEPT:EG-KG.storage.one-fsync-covers-raft,
/// RF-RULING-008).
///
/// One group is one physical write transaction and one fsync, so a graph
/// mutation and the Raft log entry that replicated it are durable together and
/// N buffered writers are notified off one flush. Each touched graph is its own
/// member, confined to its own rows by the capability rather than by an
/// argument; the Raft log, and the catalog row a first-touch graph needs, ride
/// the control member. The embedded path passes an empty `raft_log_ops`.
///
/// A burst touching more than `MAX_SHARD_GROUP_GRAPHS` distinct graphs flushes
/// in chunks, deterministically and in order, so replicas draining the same
/// burst apply the same chunks in the same sequence.
///
/// `drain_id` must be unique per ATTEMPT. This path has no replay requirement --
/// before the cutover it carried no batch identity, no idempotency row and no
/// receipt -- and exactly-once for a replicated entry is already carried by the
/// Raft applied index, which is persisted after the effect lands and never
/// regresses. Deriving the id from `(raft_group, index)` instead would
/// manufacture a conflicting replay out of a path that never needed one.
pub(crate) fn commit_ops(
    shard: &Shard,
    ops: &mut Vec<(String, Method)>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    drain_id: &str,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), owned by the caller across batches.
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    if ops.is_empty() && raft_log_ops.is_empty() {
        return Ok(());
    }
    // Group by graph BEFORE anything else: a `BTreeMap` keeps the member order
    // deterministic across replicas, which is what makes the chunking below
    // reproducible, and it is also the order `ShardWrite` hands the members
    // back in.
    let mut by_graph: BTreeMap<String, Vec<Method>> = BTreeMap::new();
    for (graph, method) in ops.drain(..) {
        reject_reserved_graph(&graph)?;
        by_graph.entry(graph).or_default().push(method);
    }
    let graphs: Vec<String> = by_graph.keys().cloned().collect();
    let chunks = shard::chunk_graphs(&graphs);
    // Raft entries ride the FIRST chunk's control member: they are one member's
    // rows, not per-graph rows, and splitting them across chunks would break the
    // "one fsync covers Raft" property for every chunk but one.
    let mut raft_pending = std::mem::take(raft_log_ops);

    if chunks.is_empty() {
        return commit_drained_chunk(
            shard,
            &[],
            &mut by_graph,
            &mut raft_pending,
            drain_id,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        );
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let chunk_id = format!("{drain_id}/{index}");
        commit_drained_chunk(
            shard,
            chunk,
            &mut by_graph,
            &mut raft_pending,
            &chunk_id,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        )?;
    }
    Ok(())
}

/// One chunk of a drained burst: one admitted group, one commit, one fsync.
#[allow(clippy::too_many_arguments)]
fn commit_drained_chunk(
    shard: &Shard,
    graphs: &[String],
    by_graph: &mut BTreeMap<String, Vec<Method>>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    drain_id: &str,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    // Cold graphs bind FIRST, in their own transactions: binding opens and
    // commits its own write and redb admits one writer, so it cannot happen
    // inside the group.
    let members = shard.graph_members(graphs)?;
    let (group, batches) = shard.admit_drain(&members, drain_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = apply_drained_chunk(
        &write,
        graphs,
        by_graph,
        raft_log_ops,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    );
    // The row gate closes whether or not the rows landed: dropping a member's
    // owner-row admission unfinished poisons the shared transaction, so the
    // failure must not skip it.
    let finished = write.finish();
    match (applied, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, committed_at_ms),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

/// Every row one chunk writes, member by member.
///
/// One graph's tables at a time: `redb` refuses a second open of a table whose
/// first handle is still alive, and every graph member writes the same `nodes`.
/// The per-graph loop is what keeps that legal, and it is also the natural
/// shape -- a member's rows are exactly one graph's.
fn apply_drained_chunk(
    write: &ShardWrite<'_>,
    graphs: &[String],
    by_graph: &mut BTreeMap<String, Vec<Method>>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    for graph in graphs {
        let Some(methods) = by_graph.remove(graph) else {
            continue;
        };
        apply_graph_methods(
            write,
            graph,
            &methods,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        )?;
        backfill_graph_meta_row(write, graph)?;
    }
    if !raft_log_ops.is_empty() {
        let mut log = write.control().open_table(RAFT_LOG)?;
        for (gid, idx, blob) in raft_log_ops.drain(..) {
            // Consensus entries carry Method payloads. When the deployment data
            // key is active, seal them just like authoritative value rows so
            // source properties are not exposed by the local Raft log.
            let sealed = crypto.seal(&blob);
            log.insert((gid, idx), sealed.as_ref())
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

/// One graph member's rows for one drained chunk.
fn apply_graph_methods(
    write: &ShardWrite<'_>,
    graph: &str,
    methods: &[Method],
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    let mut tables = GraphRowTables::open(member)?;
    for method in methods {
        if matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }) {
            tables.command_sequences.remove(graph)?;
            clear_resource_rows_in_wtx(write, graph, crypto)?;
            development_lane::clear_native_graph_rows_in_wtx(write, graph, crypto)?;
            capacity_lease::clear_graph_rows(write, graph)?;
            work_item_capability::clear_graph_rows_with_native(
                write,
                graph,
                &mut tables.native_work_items,
            )?;
        }
        apply_method_rows(graph, method, &mut tables, crypto)?;
        #[cfg(feature = "security")]
        append_audit_entry(&mut tables.audit, audit_tail, graph, method)?;
    }
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph, crypto)
}

/// Backfill the catalog row of a graph that received writes but was never
/// explicitly registered (the pre-created `__commons__`, for instance), so
/// authoritative `load_all` recovers it with no checkpoint.
///
/// The catalog is FILE-WIDE (RF-ADR-006 row-class correction, G2): it is the
/// file's list of which graphs it hosts, and the boot scan has to enumerate it
/// to learn those names before any graph scope can be bound. So the row is the
/// control member's to write, in the same admitted group as the graph's own.
fn backfill_graph_meta_row(write: &ShardWrite<'_>, graph: &str) -> Result<(), String> {
    let mut meta = write.control().open_table(GRAPH_META)?;
    if meta
        .get(graph)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let incarnation_id = new_incarnation_id(graph);
    let encoded = encode_meta_with_incarnation(graph, GraphType::Global, &incarnation_id)?;
    meta.insert(graph, encoded.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Deterministic failure injection points around the authoritative batch commit.
/// Production always calls with `None`; unit tests use these boundaries to prove
/// that restart observes either no batch or one complete committed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationBatchCrashpoint {
    BeforeRows,
    AfterRowsBeforeMetadata,
    BeforeCommit,
    AfterCommitBeforeAck,
}

/// Atomically apply one canonical mutation batch to graph rows, durable status,
/// idempotency index, and transactional outbox.
///
/// This is deliberately separate from [`commit_ops`]: a batch is already the
/// caller's all-or-nothing unit and must never be folded into a partially-acked
/// queue group.  One immediate redb `WriteTransaction` is its commit point.  An
/// exact retry returns the stored result; cross-modal retries may re-derive only
/// the OCC version from the current authoritative observation while the durable
/// record retains the original. Reusing an idempotency key for different work
/// fails closed.
pub(crate) fn commit_mutation_batch(
    shard: &Shard,
    graph_fname: &str,
    batch: &MutationBatch,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname,
            batch,
            change: None,
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack,
            committed_at_ms,
            // Compact-row batches never carry `authoritative_state`, so this is
            // inert (see `commit_mutation_batch_inner`).
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Commit authenticated staged graph material through the same batch kernel. The
/// digest/version descriptor lives in `batch`; complete snapshots or affected-row
/// deltas are supplied out-of-line so status/outbox do not duplicate them.
///
/// `audited` is the CALLER's already-resolved `MutationPlan::audited` (or
/// equivalent `eg_capabilities::policy(method).audited`) for the ORIGINAL,
/// pre-opaque-wrapped method -- see the doc comment on `commit_mutation_batch_inner`
/// for why this cannot be re-derived downstream once the operation is compiled.
/// Borrowed carrier for [`commit_mutation_batch_state`]'s graph-identifying and
/// coordinator-record inputs, bundled so the function stays under the clippy
/// argument-count ceiling.
pub(crate) struct StateCommitInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) authoritative_state_msgpack: &'a [u8],
    pub(crate) result_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
    pub(crate) audited: bool,
}

pub(crate) fn commit_mutation_batch_state(
    shard: &Shard,
    input: StateCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname: input.graph_fname,
            batch: input.batch,
            change: None,
            authoritative_state_msgpack: Some(input.authoritative_state_msgpack),
            crossmodal: None,
            result_msgpack: input.result_msgpack,
            committed_at_ms: input.committed_at_ms,
            audited: input.audited,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// Engine-native ChangeEnvelope commit. Graph rows, every material/governance
/// projection, version/cursor fences, terminal batch/envelope records, and the
/// CDC outbox are written by one redb transaction and one durability barrier.
pub(crate) fn commit_change_envelope(
    shard: &Shard,
    graph_fname: &str,
    envelope: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<ChangeEnvelopeCommit, String> {
    envelope.validate()?;
    let mutation = commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname,
            batch: &envelope.mutation,
            change: Some(envelope),
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack: None,
            committed_at_ms,
            // No `authoritative_state`, so `audited` is inert here.
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )?;
    let outbox_count = envelope
        .mutation
        .operations
        .len()
        .checked_add(envelope.mutation.outbox.len())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| "change envelope outbox count overflow".to_string())?;
    Ok(ChangeEnvelopeCommit {
        envelope_id: envelope.envelope_id.clone(),
        batch_id: envelope.mutation.batch_id.clone(),
        content_version: envelope.content_version.clone(),
        cursor: envelope.cursor.clone(),
        outbox_count,
        replayed: mutation.replayed,
    })
}

/// A ChangeEnvelope batch aborted at `index` (the first envelope that failed its
/// idempotency/version/cursor/fence check or row projection). Because every envelope
/// for one graph shares ONE atomic transaction, the abort rolls back the whole
/// group — no envelope in it commits — and the caller reports the batch outcome per
/// envelope honestly.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEnvelopesError {
    pub(crate) index: usize,
    pub(crate) error: String,
}

/// Engine-native BATCH ChangeEnvelope commit: apply EVERY envelope in `envelopes`
/// (all of which must target `graph_fname`) into ONE redb transaction and one
/// durability barrier (CONCEPT:EG-KG.ingest.batched-change-envelopes). The envelopes
/// are applied in order; read-your-writes inside the shared transaction chains each
/// envelope's content-version, cursor, and +1 graph-version onto the previous one,
/// so a page of records built with sequential `expected_graph_version`s commits as a
/// single fsync instead of N.
///
/// Atomicity is per graph-batch: the first envelope that fails a check aborts the
/// whole transaction (nothing in this group commits) and returns [`ChangeEnvelopesError`]
/// naming the offending index. An idempotent replay with a fresh attempt nonce is
/// NOT a failure — it is reported per envelope via `ChangeEnvelopeCommit::replayed` and the
/// transaction still commits its non-replayed siblings. When every envelope is a
/// replay, nothing was written and the transaction is dropped without an fsync,
/// exactly like the single-envelope path.
pub(crate) fn commit_change_envelopes(
    shard: &Shard,
    graph_fname: &str,
    envelopes: &[ChangeEnvelope],
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
    let max_batch = crate::change_envelope::MAX_ENVELOPES_PER_BATCH;
    if envelopes.len() > max_batch {
        return Err(ChangeEnvelopesError {
            index: 0,
            error: format!(
                "CHANGE_BATCH_TOO_LARGE: {} envelopes exceed the {max_batch} cap",
                envelopes.len()
            ),
        });
    }
    let at = |index: usize, error: String| ChangeEnvelopesError { index, error };
    if envelopes.is_empty() {
        return Ok(Vec::new());
    }

    // Structural validation and the one serving-scope rebind happen before the
    // shared write is admitted.  Domain preconditions, replay resolution, row
    // application, receipts, versions and outbox rows remain inside that one
    // admitted transaction below.
    let handle = shard.graph(graph_fname).map_err(|error| at(0, error))?;
    let bound = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            envelope.validate().map_err(|error| at(index, error))?;
            shard::bind_caller_batch(handle.as_ref(), graph_fname, &envelope.mutation)
                .map_err(|error| at(index, error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let members = vec![(graph_fname.to_string(), Arc::clone(&handle))];

    // Precompute only bounded result metadata before opening the shared write.  The
    // durable replay/OCC/domain decisions still happen under the admitted group.
    let outbox_counts = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            envelope
                .mutation
                .operations
                .len()
                .checked_add(envelope.mutation.outbox.len())
                .and_then(|count| count.checked_add(1))
                .and_then(|count| u32::try_from(count).ok())
                .ok_or_else(|| at(index, "change envelope outbox count overflow".to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // A leading replay is classified in a short-lived group and then discarded:
    // it must not force a control receipt or an fsync.  The first fresh envelope
    // opens the one shared group that carries every later fresh/replayed member.
    let mut first_fresh = 0usize;
    let mut commits = Vec::with_capacity(envelopes.len());
    let (group, first_batches, first_control_begin, first_graph_begin) = loop {
        let (group, batches) = shard
            .admit_batch(
                graph_fname,
                &handle,
                &bound[first_fresh],
                &bound[first_fresh].batch_id,
            )
            .map_err(|error| at(first_fresh, error))?;
        let control_begin = group
            .begun(0)
            .map_err(|error| at(first_fresh, error))?
            .clone();
        let graph_begin = group
            .begun(1)
            .map_err(|error| at(first_fresh, error))?
            .clone();
        match graph_begin {
            Begin::Replay(_) => {
                shard
                    .commit_drain(group, &batches, committed_at_ms)
                    .map_err(|error| at(first_fresh, error))?;
                commits.push(ChangeEnvelopeCommit {
                    envelope_id: envelopes[first_fresh].envelope_id.clone(),
                    batch_id: envelopes[first_fresh].mutation.batch_id.clone(),
                    content_version: envelopes[first_fresh].content_version.clone(),
                    cursor: envelopes[first_fresh].cursor.clone(),
                    outbox_count: outbox_counts[first_fresh],
                    replayed: true,
                });
                first_fresh += 1;
                if first_fresh == envelopes.len() {
                    return Ok(commits);
                }
            }
            Begin::Apply { .. } => {
                if !matches!(&control_begin, Begin::Apply { .. }) {
                    shard
                        .mutations()
                        .abort_group(group)
                        .map_err(|error| at(first_fresh, error))?;
                    return Err(at(
                        first_fresh,
                        "the shard control member unexpectedly replayed for a fresh envelope"
                            .to_string(),
                    ));
                }
                break (group, batches, control_begin, graph_begin);
            }
        }
    };

    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    let mut control_version = match &first_control_begin {
        Begin::Apply { source_version } => source_version.unwrap_or(0).saturating_add(1),
        Begin::Replay(_) => unreachable!("a fresh graph member requires a fresh control member"),
    };
    let mut final_control_batch = first_batches[0].clone();
    let mut final_graph_batch = first_batches[1].clone();

    for (index, (envelope, batch)) in envelopes
        .iter()
        .zip(bound.iter())
        .enumerate()
        .skip(first_fresh)
    {
        let graph_begin = if index == first_fresh {
            first_graph_begin.clone()
        } else {
            match group.member(1).and_then(|member| member.begin(batch)) {
                Ok(begun) => begun,
                Err(error) => {
                    shard
                        .mutations()
                        .abort_group(group)
                        .map_err(|abort| at(index, format!("{error}; abort failed: {abort}")))?;
                    return Err(at(index, error));
                }
            }
        };

        if matches!(graph_begin, Begin::Replay(_)) {
            // This graph member has a durable receipt already. It contributes
            // no rows and therefore no control maintenance member. Keep the
            // last fresh batch as the final graph reference for commit_group.
            commits.push(ChangeEnvelopeCommit {
                envelope_id: envelope.envelope_id.clone(),
                batch_id: envelope.mutation.batch_id.clone(),
                content_version: envelope.content_version.clone(),
                cursor: envelope.cursor.clone(),
                outbox_count: outbox_counts[index],
                replayed: true,
            });
            continue;
        }

        let graph_source = match graph_begin {
            Begin::Apply { source_version } => source_version,
            Begin::Replay(_) => unreachable!("replay handled above"),
        };
        let (control_batch, control_source) =
            if index == first_fresh {
                let source_version = match &first_control_begin {
                    Begin::Apply { source_version } => *source_version,
                    Begin::Replay(_) => unreachable!("fresh graph member requires fresh control"),
                };
                (first_batches[0].clone(), source_version)
            } else {
                let control_batch = match shard.maintenance_batch_at(
                    &format!(
                        "change-envelope/{graph_fname}/{}",
                        envelope.mutation.batch_id
                    ),
                    control_version,
                ) {
                    Ok(batch) => batch,
                    Err(error) => {
                        shard.mutations().abort_group(group).map_err(|abort| {
                            at(index, format!("{error}; abort failed: {abort}"))
                        })?;
                        return Err(at(index, error));
                    }
                };
                let control_begin = match group.control().begin(&control_batch) {
                    Ok(begun) => begun,
                    Err(error) => {
                        shard.mutations().abort_group(group).map_err(|abort| {
                            at(index, format!("{error}; abort failed: {abort}"))
                        })?;
                        return Err(at(index, error));
                    }
                };
                let source_version = match control_begin {
                    Begin::Apply { source_version } => source_version,
                    Begin::Replay(_) => {
                        shard
                            .mutations()
                            .abort_group(group)
                            .map_err(|error| at(index, error))?;
                        return Err(at(
                            index,
                            "the shard control member unexpectedly replayed for a fresh envelope"
                                .to_string(),
                        ));
                    }
                };
                (control_batch, source_version)
            };
        let current_batches = vec![control_batch, batch.clone()];
        let staged = match stage_mutation_batch_rows(
            shard,
            &group,
            &members,
            &current_batches,
            StagedRowInput {
                graph_fname,
                batch: &current_batches[1],
                change: Some(envelope),
                authoritative_state_msgpack: None,
                crossmodal: None,
                committed_at_ms,
                audited: true,
                crashpoint: None,
            },
            crypto,
            #[cfg(feature = "security")]
            &mut staged_audit_tail,
        ) {
            Ok(staged) => staged,
            Err(error) => {
                shard
                    .mutations()
                    .abort_group(group)
                    .map_err(|abort| at(index, format!("{error}; abort failed: {abort}")))?;
                return Err(at(index, error));
            }
        };

        if let Err(error) = shard.mutations().finish(
            group.control(),
            &current_batches[0],
            None,
            committed_at_ms,
            control_source,
        ) {
            shard
                .mutations()
                .abort_group(group)
                .map_err(|abort| at(index, format!("{error}; abort failed: {abort}")))?;
            return Err(at(index, error));
        }
        if let Err(error) = shard.mutations().finish(
            group.member(1).map_err(|error| at(index, error))?,
            &current_batches[1],
            staged.generated_result,
            committed_at_ms,
            graph_source,
        ) {
            shard
                .mutations()
                .abort_group(group)
                .map_err(|abort| at(index, format!("{error}; abort failed: {abort}")))?;
            return Err(at(index, error));
        }

        control_version = control_source.unwrap_or(0).saturating_add(1);
        final_control_batch = current_batches[0].clone();
        final_graph_batch = current_batches[1].clone();
        commits.push(ChangeEnvelopeCommit {
            envelope_id: envelope.envelope_id.clone(),
            batch_id: envelope.mutation.batch_id.clone(),
            content_version: envelope.content_version.clone(),
            cursor: envelope.cursor.clone(),
            outbox_count: outbox_counts[index],
            replayed: false,
        });
    }

    let final_batches = [final_control_batch, final_graph_batch];
    let commit_refs: Vec<&MutationBatch> = final_batches.iter().collect();
    if let Err(error) = shard.mutations().commit_group(group, &commit_refs) {
        return Err(at(first_fresh, error));
    }
    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }
    Ok(commits)
}

/// Non-graph rows that participate in an authoritative cross-modal
/// [`MutationBatch`] commit. The coordinator metadata, graph rows, semantic/blob
/// projections, time-series batches, result and outbox are written by the same redb
/// transaction; this borrowed carrier is never serialized as a second authority.
pub(crate) struct CrossModalBatchRows<'a> {
    pub(crate) methods: &'a [Method],
    pub(crate) vectors: &'a [VectorUpsert],
    pub(crate) blob_refs: &'a [BlobRefRow],
    pub(crate) measurements: &'a [crate::MeasurementBatch],
}

/// Authenticated graph material supplied by a complex MutationBatch. Callers may
/// provide a complete snapshot or persist only the affected rows.
enum AuthoritativeGraphState {
    Snapshot(Box<crate::graph::GraphSnapshot>),
    RowDelta(crate::graph_delta::GraphRowDelta),
}

/// Borrowed carrier for [`commit_mutation_batch_crossmodal`]'s graph-identifying
/// and coordinator-record inputs, bundled alongside the row material so the
/// function itself stays under the clippy argument-count ceiling.
pub(crate) struct CrossModalCommitInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) rows: CrossModalBatchRows<'a>,
    pub(crate) result_msgpack: Option<&'a [u8]>,
    pub(crate) committed_at_ms: u64,
}

/// Commit a canonical cross-modal batch through the universal status/fence/
/// idempotency/outbox kernel. Public mutation surfaces use this canonical path;
/// [`commit_crossmodal`] remains the low-level atomic projection primitive.
pub(crate) fn commit_mutation_batch_crossmodal(
    shard: &Shard,
    input: CrossModalCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname: input.graph_fname,
            batch: input.batch,
            change: None,
            authoritative_state_msgpack: None,
            crossmodal: Some(input.rows),
            result_msgpack: input.result_msgpack,
            committed_at_ms: input.committed_at_ms,
            // No `authoritative_state`, so `audited` is inert here.
            audited: true,
            crashpoint: None,
        },
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )
}

/// The inputs every batch-commit entry point shares, bundled so the phases
/// below stay inside the argument cap without positional strings.
struct BatchCommitInput<'a> {
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    change: Option<&'a ChangeEnvelope>,
    authoritative_state_msgpack: Option<&'a [u8]>,
    crossmodal: Option<CrossModalBatchRows<'a>>,
    result_msgpack: Option<&'a [u8]>,
    committed_at_ms: u64,
    /// Whether THIS commit appends tamper-evident audit-chain entries. Only
    /// consulted on the authoritative-state branches; see the note below on why
    /// it cannot be re-derived from `batch.operations`.
    audited: bool,
    crashpoint: Option<MutationBatchCrashpoint>,
}

/// Commit ONE caller batch: its graph rows, its governance material, its
/// catalog entry, and the kernel's terminal metadata, in ONE transaction.
///
/// The batch is admitted through [`Shard::admit_batch`], which puts the caller's
/// batch verbatim on its graph member and the shard's own bookkeeping on the
/// control member. Everything that used to be checked here first is the
/// kernel's now and is checked INSIDE the transaction rather than before it:
/// binding, exact idempotency, OCC against the authoritative version, and route
/// fencing are `commit::begin`; the receipt, the idempotency row, the class row,
/// the version bump and the outbox rows are `commit::finish`. That is the whole
/// of the deleted `check_idempotency_replay` / `check_batch_id_uniqueness` /
/// `check_occ_version_and_fence` / `write_mutation_batch_*` family, and it
/// closes the window those checks could only narrow: they read before the write
/// lock was held.
///
/// A replay is therefore an ANSWER, not a pre-check. An exact retry resolves to
/// `Begin::Replay` at admission and its durable receipt is the result; nothing
/// is written for it, and there is no second idempotency authority to consult.
///
/// `audited`: whether THIS commit appends tamper-evident audit-chain entries for
/// its operations. Only consulted when `authoritative_state_msgpack` is `Some`.
/// It cannot be re-derived from `batch.operations`: `compile_methods`'s
/// `opaque_state_operation` rewrites EVERY state-backed operation into the same
/// opaque digest receipt (`Method::ApplyMutation{event_type:
/// "authoritative_state_operation", ..}`) by design, so sensitive row payloads
/// never enter the durable batch/audit/outbox record -- and by the time this
/// function sees the operation, the original method's own
/// `eg_capabilities::policy(..).audited` answer is unrecoverable. Callers pass
/// their already-resolved `MutationPlan::audited`.
fn commit_mutation_batch_inner(
    shard: &Shard,
    input: BatchCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    let BatchCommitInput {
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        result_msgpack,
        committed_at_ms,
        audited,
        crashpoint,
    } = input;
    // Audit-tail updates are staged alongside the transaction. Advancing the
    // process cache before the commit would create a false tail when an
    // injected or real failure drops it.
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();

    // The compiler preserves the verified caller scope on the batch because it
    // is part of the request authority. Bind exactly once at the physical
    // shard boundary: the ledger identity and serving principal belong to the
    // shard, while the caller authority and outbox attribution remain in the
    // rebound batch. Binding a cold graph opens its own transaction, so it must
    // happen before this group's admission.
    let handle = shard.graph(graph_fname)?;
    let bound = shard::bind_caller_batch(handle.as_ref(), graph_fname, batch)?;
    let members = vec![(graph_fname.to_string(), Arc::clone(&handle))];
    let (group, batches) = shard.admit_batch(graph_fname, &handle, &bound, &bound.batch_id)?;

    if matches!(group.begun(1)?, Begin::Replay(_)) {
        // A byte-identical retry still has to commit the admitted group: the
        // replay member's fresh attempt nonce is consumed only by the commit
        // finalizer. `commit_batch` finishes the control member and seals the
        // replay member without reapplying owner rows.
        let Begin::Replay(_record) = group.begun(1)?.clone() else {
            return Err("admitted replay lost its receipt".to_string());
        };
        let committed = shard.commit_batch(
            group,
            &batches,
            result_msgpack.map(ToOwned::to_owned),
            committed_at_ms,
        )?;
        let commit = MutationBatchCommit {
            record: committed.record,
            identity: bound.identity.clone(),
            replayed: true,
        };
        commit.validate()?;
        return Ok(commit);
    }

    let staged = match stage_mutation_batch_rows(
        shard,
        &group,
        &members,
        &batches,
        StagedRowInput {
            graph_fname,
            batch: &bound,
            change,
            authoritative_state_msgpack,
            crossmodal: crossmodal.as_ref(),
            committed_at_ms,
            audited,
            crashpoint,
        },
        crypto,
        #[cfg(feature = "security")]
        &mut staged_audit_tail,
    ) {
        Ok(staged) => staged,
        Err(error) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };

    run_mutation_batch_crashpoint(&bound, crashpoint, MutationBatchCrashpoint::BeforeCommit)?;
    crate::mutation_batch::apply_certification_fault(
        &bound,
        crate::mutation_batch::MutationCommitPhase::BeforeCommit,
    )?;

    let result = staged
        .generated_result
        .or_else(|| result_msgpack.map(ToOwned::to_owned));
    let committed = shard.commit_batch(group, &batches, result, committed_at_ms)?;

    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }

    run_mutation_batch_crashpoint(
        &bound,
        crashpoint,
        MutationBatchCrashpoint::AfterCommitBeforeAck,
    )?;
    crate::mutation_batch::apply_certification_fault(
        &bound,
        crate::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck,
    )?;

    let commit = MutationBatchCommit {
        record: committed.record,
        identity: bound.identity.clone(),
        replayed: committed.replayed,
    };
    commit.validate()?;
    Ok(commit)
}

/// What one batch's row phase produced.
struct StagedMutationRows {
    generated_result: Option<Vec<u8>>,
}

/// The row-phase inputs, bundled out of [`stage_mutation_batch_rows`]'s
/// parameter list.
struct StagedRowInput<'a> {
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    change: Option<&'a ChangeEnvelope>,
    authoritative_state_msgpack: Option<&'a [u8]>,
    crossmodal: Option<&'a CrossModalBatchRows<'a>>,
    committed_at_ms: u64,
    audited: bool,
    crashpoint: Option<MutationBatchCrashpoint>,
}

/// Every OWNER row one batch writes, inside the admitted group.
///
/// The row gate closes whether or not the rows landed: dropping a member's
/// owner-row admission unfinished poisons the shared transaction, so a failure
/// here must not skip it.
fn stage_mutation_batch_rows(
    shard: &Shard,
    group: &AdmittedGroup<'_, GraphShardOwner>,
    members: &[(String, Arc<OwnedStoreHandle<GraphShardOwner>>)],
    batches: &[MutationBatch],
    input: StagedRowInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<StagedMutationRows, String> {
    let write = ShardWrite::open(shard, group, members, batches)?;
    let staged = stage_rows_in(
        &write,
        input,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
    );
    let finished = write.finish();
    match (staged, finished) {
        (Ok(staged), Ok(())) => Ok(staged),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

/// The row phases themselves, in order, with the row gate already open.
fn stage_rows_in(
    write: &ShardWrite<'_>,
    input: StagedRowInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<StagedMutationRows, String> {
    let StagedRowInput {
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        committed_at_ms,
        audited,
        crashpoint,
    } = input;

    // Domain preconditions the kernel cannot know: an envelope must not already
    // be committed, and a content version / cursor precondition must hold. These
    // are the ONLY checks that survived the cut -- the idempotency, OCC and
    // fence checks beside them are the kernel's now.
    let plan = prepare_and_validate_mutation_batch(
        write,
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        crossmodal.is_some(),
        crypto,
    )?;

    run_mutation_batch_crashpoint(batch, crashpoint, MutationBatchCrashpoint::BeforeRows)?;

    let generated_result = apply_state_dispatch_rows(
        write,
        graph_fname,
        plan.staged_state.as_ref(),
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        audited,
        crossmodal.is_some(),
    )?;

    apply_crossmodal_rows_phase(write, graph_fname, crossmodal, crypto)?;

    apply_post_row_cleanup(
        write,
        graph_fname,
        batch,
        plan.lifecycle.as_ref(),
        authoritative_state_msgpack.is_none(),
    )?;

    run_mutation_batch_crashpoint(
        batch,
        crashpoint,
        MutationBatchCrashpoint::AfterRowsBeforeMetadata,
    )?;

    // The governance rows and the catalog entry. The receipt, idempotency row,
    // class row, version bump and outbox rows that used to sit beside them are
    // written by `commit::finish` from the batch itself.
    if let Some(change) = change {
        apply_change_envelope_commit_rows(write, graph_fname, change, committed_at_ms, crypto)?;
    }
    write_mutation_batch_graph_meta_row(
        write,
        graph_fname,
        batch,
        plan.lifecycle,
        plan.integrity_policy_update,
    )?;

    Ok(StagedMutationRows { generated_result })
}

fn compute_native_terminal_work_item_cas(batch: &MutationBatch) -> bool {
    batch.authoritative_state.is_none()
        && match batch.operations.first() {
            Some(first)
                if first.domain == DurabilityDomain::ControlPlane
                    && first.surface == MutationSurface::Job
                    && matches!(&first.method, Method::CommitWorkItemResult { .. }) =>
            {
                batch.operations[1..]
                    .iter()
                    .all(|op| matches!(&op.method, Method::AddNode { .. }))
            }
            Some(first) => {
                batch.operations.len() == 1
                    && first.domain == DurabilityDomain::ControlPlane
                    && first.surface == MutationSurface::Job
                    && matches!(
                        &first.method,
                        Method::CancelWorkItem { .. }
                            | Method::DeferWorkItem { .. }
                            | Method::ReserveWorkItemResources { .. }
                            | Method::ReleaseWorkItemResources { .. }
                            | Method::ReclaimWorkItemResources { .. }
                            | Method::UpdateResourceHost { .. }
                            | Method::SubmitWorkItem { .. }
                            | Method::SubmitWorkItems { .. }
                    )
            }
            None => false,
        }
}

fn resolve_mutation_authoritative_state(
    batch: &MutationBatch,
    authoritative_state_msgpack: Option<&[u8]>,
) -> Result<Option<AuthoritativeGraphState>, String> {
    Ok(
        match (&batch.authoritative_state, authoritative_state_msgpack) {
            (Some(descriptor), Some(bytes)) => {
                use sha2::{Digest, Sha256};
                let digest = hex::encode(Sha256::digest(bytes));
                if digest != descriptor.digest {
                    return Err(
                        "authoritative state digest does not match MutationBatch".to_string()
                    );
                }
                let state = match descriptor.algorithm.as_str() {
                    "sha256" => AuthoritativeGraphState::Snapshot(Box::new(
                        decode_durable::<crate::graph::GraphSnapshot>(bytes).map_err(|_| {
                            "authoritative graph state is invalid or exceeds resource limits"
                                .to_string()
                        })?,
                    )),
                    crate::graph_delta::ROW_DELTA_ALGORITHM => {
                        let delta = decode_durable::<crate::graph_delta::GraphRowDelta>(bytes)
                        .map_err(|_| {
                            "authoritative graph row delta is invalid or exceeds resource limits"
                                .to_string()
                        })?;
                        delta.validate()?;
                        AuthoritativeGraphState::RowDelta(delta)
                    }
                    _ => return Err("unsupported authoritative state algorithm".to_string()),
                };
                Some(state)
            }
            (None, None) => None,
            (Some(_), None) => {
                return Err("MutationBatch state descriptor has no authoritative bytes".to_string())
            }
            (None, Some(_)) => {
                return Err(
                    "authoritative bytes require a MutationBatch state descriptor".to_string(),
                )
            }
        },
    )
}

// `None` means this mutation does not change graph control state. `Some(None)`
// is an explicit policy-free snapshot, while `Some(Some(_))` installs a new
// validated policy. Keeping the outer option is necessary for exact snapshot
// replacement without confusing "unchanged" with "absent".
fn resolve_integrity_policy_update(
    staged_state: Option<&AuthoritativeGraphState>,
) -> Option<Option<crate::graph::IntegrityPolicy>> {
    match staged_state {
        Some(AuthoritativeGraphState::Snapshot(snapshot)) => {
            Some(snapshot.integrity_policy.clone())
        }
        Some(AuthoritativeGraphState::RowDelta(delta)) => {
            delta.integrity_policy_update().cloned().map(Some)
        }
        None => None,
    }
}

/// The graph name of a graph-scoped `MutationBatch`. Every commit path in this
/// module is indexed by `graph_fname` and only ever handles
/// `MutationScope::Graph` batches -- `batch.validate()` (run before any of
/// these call this) structurally requires `MutationScope::Graph` to pair with
/// `VersionExpectation::Graph(_)`, so a native scope can never reach here in
/// practice. A native scope reports no graph name at all; fail closed instead
/// of ever substituting "" for it.
fn mutation_batch_graph_name(batch: &MutationBatch) -> Result<&str, String> {
    batch
        .identity
        .scope()
        .graph_name()
        .map(LogicalName::as_str)
        .ok_or_else(|| "mutation batch is not graph-scoped".to_string())
}

fn validate_mutation_batch_route_and_lowering(
    batch: &MutationBatch,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    staged_state_is_none: bool,
) -> Result<(), String> {
    let batch_graph_name = mutation_batch_graph_name(batch)?;
    // `Shard::bind_caller_batch` has already proved the caller's logical graph
    // against `sanitize(...)` and rebound this batch to the physical shard
    // scope.  This validator therefore sees the bound physical spelling; a
    // second sanitization would turn `acme~3aa` into `acme~7e3aa` and reject a
    // valid escaped route.  Keep the comparison exact here so the one
    // logical-to-physical conversion remains at the caller admission seam.
    if graph_fname != batch_graph_name {
        return Err(format!(
            "mutation batch graph route mismatch: bound batch '{}' does not match '{}'",
            batch_graph_name, graph_fname
        ));
    }
    for operation in &batch.operations {
        let crossmodal_sentinel = crossmodal.is_some()
            && matches!(
                &operation.method,
                Method::ApplyMutation { event_type, .. }
                    if event_type == "crossmodal_operation"
            );
        if staged_state_is_none
            && !supports_atomic_batch_rows(&operation.method)
            && !crossmodal_sentinel
        {
            return Err(format!(
                "MutationBatch operation {} is not lowered to the atomic graph-row kernel",
                operation.ordinal
            ));
        }
    }
    if let Some(rows) = crossmodal {
        for method in rows.methods {
            if !supports_atomic_batch_rows(method) {
                return Err(
                    "cross-modal graph method is not lowered to the atomic row kernel".to_string(),
                );
            }
        }
    }
    Ok(())
}

fn detect_and_validate_lifecycle(
    batch: &MutationBatch,
    graph_fname: &str,
) -> Result<Option<(bool, String, Option<GraphType>)>, String> {
    let lifecycle = batch
        .operations
        .iter()
        .find_map(|operation| match &operation.method {
            Method::CreateGraph {
                graph_name,
                graph_type,
            } => Some((true, graph_name.clone(), Some(*graph_type))),
            Method::DeleteGraph { graph_name } => Some((false, graph_name.clone(), None)),
            _ => None,
        });
    if let Some((_, ref graph_name, _)) = lifecycle {
        // Lifecycle methods retain their logical target in the operation while
        // the surrounding bound batch carries the physical shard key.
        if batch.operations.len() != 1 || sanitize(graph_name) != graph_fname {
            return Err(
                "lifecycle MutationBatch must contain exactly one operation for its target graph"
                    .to_string(),
            );
        }
    }
    Ok(lifecycle)
}

/// An envelope replay must reproduce the committed envelope byte-for-byte.
fn check_replay_envelope_matches(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
    graph_name: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let stored = envelopes
        .get((graph_fname, change.envelope_id.as_str()))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!(
                "STALE_FENCE: committed envelope '{}' is no longer current for graph '{}'",
                change.envelope_id, graph_name
            )
        })?;
    let bytes = crypto.unseal(stored.value())?;
    let record: ChangeEnvelopeRecord = decode_durable(&bytes)?;
    let stored_bytes = rmp_serde::to_vec_named(&record.envelope).map_err(|e| e.to_string())?;
    let proposed_bytes = rmp_serde::to_vec_named(change).map_err(|e| e.to_string())?;
    if stored_bytes != proposed_bytes {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: envelope '{}' does not match its committed batch",
            change.envelope_id
        ));
    }
    Ok(())
}

/// Precondition 1: the envelope id must not already be committed for this graph.
fn check_change_envelope_not_committed(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
) -> Result<(), String> {
    let envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    if envelopes
        .get((graph_fname, change.envelope_id.as_str()))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: envelope_id '{}' is already committed",
            change.envelope_id
        ));
    }
    Ok(())
}

/// Precondition 2: the envelope's content version must chain off the durable
/// one -- matching previous digest AND a strictly advancing source version --
/// or, with no durable row, must not claim a previous digest.
fn check_change_content_version_precondition(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let versions = write
        .graph(graph_fname)?
        .open_scoped_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    let version_key = (
        graph_fname,
        tenant,
        change.content_version.object_id.as_str(),
    );
    let current = versions
        .get(version_key)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable::<ContentVersion>(&bytes)
        })
        .transpose()?;
    match current {
        Some(current) => {
            if change.content_version.previous_digest.as_deref() != Some(current.digest.as_str()) {
                return Err(format!(
                    "STALE_CONTENT_VERSION: object '{}' expected previous digest does not match",
                    change.content_version.object_id
                ));
            }
            if !change
                .content_version
                .source_version
                .advances(&current.source_version)
            {
                return Err(format!(
                    "STALE_CONTENT_VERSION: object '{}' source version did not advance",
                    change.content_version.object_id
                ));
            }
        }
        None if change.content_version.previous_digest.is_some() => {
            return Err(format!(
                "STALE_CONTENT_VERSION: object '{}' has no prior version",
                change.content_version.object_id
            ));
        }
        None => {}
    }
    Ok(())
}

/// Precondition 3: the same chaining rule for the envelope's source cursor.
fn check_change_cursor_precondition(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let cursors = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    let cursor_key = (
        graph_fname,
        tenant,
        cursor.source.as_str(),
        cursor.partition.as_str(),
    );
    let current = cursors
        .get(cursor_key)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable::<ChangeCursor>(&bytes)
        })
        .transpose()?;
    match current {
        Some(current) => {
            if cursor.expected_previous.as_ref() != Some(&current.position) {
                return Err(format!(
                    "STALE_CURSOR: source '{}' partition '{}' expected position does not match",
                    cursor.source, cursor.partition
                ));
            }
            if !cursor.position.advances(&current.position) {
                return Err(format!(
                    "STALE_CURSOR: source '{}' partition '{}' did not advance",
                    cursor.source, cursor.partition
                ));
            }
        }
        None if cursor.expected_previous.is_some() => {
            return Err(format!(
                "STALE_CURSOR: source '{}' partition '{}' has no prior position",
                cursor.source, cursor.partition
            ));
        }
        None => {}
    }
    Ok(())
}

/// The three preconditions run in the original order -- envelope idempotency,
/// then content version, then cursor -- so a change that violates more than one
/// still reports the same first violation it did before the split.
fn validate_change_envelope_preconditions(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: Option<&ChangeEnvelope>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(change) = change else {
        return Ok(());
    };
    check_change_envelope_not_committed(write, graph_fname, change)?;
    check_change_content_version_precondition(write, graph_fname, tenant, change, crypto)?;
    if let Some(cursor) = &change.cursor {
        check_change_cursor_precondition(write, graph_fname, tenant, cursor, crypto)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_snapshot_state(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    snapshot: &crate::graph::GraphSnapshot,
    batch: &MutationBatch,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    // Non-`security` builds never read `audited` (it is only consulted inside
    // the `#[cfg(feature = "security")]` block below); this keeps such a build
    // warning-free without cfg-gating the parameter itself.
    let _ = audited;
    let incoming_nodes = snapshot
        .nodes
        .iter()
        .map(|(node_id, properties)| (node_id.clone(), properties.as_ref().clone()))
        .collect::<Vec<_>>();
    // A snapshot replaces the native WorkItem image.  Validate that a
    // generic restore cannot manufacture an active lease, then purge all
    // private claim state atomically before installing the replacement.
    work_item_capability::validate_snapshot_nodes(&incoming_nodes)?;
    work_item_capability::clear_graph_rows(write, graph_fname)?;
    development_lane::validate_lane_links_in_wtx(write, graph_fname, &incoming_nodes, crypto)?;
    let mut nodes = write
        .graph(graph_fname)?
        .open_scoped_table(NODES)
        .map_err(|e| e.to_string())?;
    let mut edges = write
        .graph(graph_fname)?
        .open_scoped_table(EDGES)
        .map_err(|e| e.to_string())?;
    let mut ledger = write
        .graph(graph_fname)?
        .open_scoped_table(LEDGER)
        .map_err(|e| e.to_string())?;
    clear_graph_rows(graph_fname, &mut nodes, &mut edges, &mut ledger)?;
    for (node_id, properties) in &snapshot.nodes {
        let sealed = crypto.seal(properties.as_ref());
        nodes
            .insert((graph_fname, node_id.as_str()), sealed.as_ref())
            .map_err(|e| e.to_string())?;
    }
    for (source, target, properties) in &snapshot.edges {
        let ordinal = next_edge_ordinal(&edges, graph_fname, source.as_str(), target.as_str())?;
        let sealed = crypto.seal(properties.as_ref());
        edges
            .insert(
                (graph_fname, source.as_str(), target.as_str(), ordinal),
                sealed.as_ref(),
            )
            .map_err(|e| e.to_string())?;
    }
    for (sequence, line) in snapshot.ledger.iter().enumerate() {
        ledger
            .insert((graph_fname, sequence as u64), line.as_str())
            .map_err(|e| e.to_string())?;
    }
    drop(nodes);
    drop(edges);
    drop(ledger);
    let semantic_bytes =
        rmp_serde::to_vec_named(&snapshot.semantic_store).map_err(|e| e.to_string())?;
    let sealed_semantic = crypto.seal(&semantic_bytes);
    let mut semantic = write
        .graph(graph_fname)?
        .open_scoped_table(SEMANTIC)
        .map_err(|e| e.to_string())?;
    semantic
        .insert(graph_fname, sealed_semantic.as_ref())
        .map_err(|e| e.to_string())?;

    #[cfg(feature = "security")]
    if audited {
        let mut audit = write
            .graph(graph_fname)?
            .open_scoped_table(AUDIT)
            .map_err(|e| e.to_string())?;
        for operation in &batch.operations {
            append_audit_entry(
                &mut audit,
                staged_audit_tail,
                graph_fname,
                &operation.method,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_row_delta_state(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    delta: &crate::graph_delta::GraphRowDelta,
    batch: &MutationBatch,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    let _ = audited;
    let mut tables = GraphRowTables::open(write.graph(graph_fname)?)?;
    for method in delta.operations() {
        apply_method_rows(graph_fname, method, &mut tables, crypto)?;
    }
    if let Some((_, retain, append)) = delta.ledger_patch() {
        let suffix_keys: Vec<u64> = tables
            .ledger
            .scope_rows()
            .map_err(|error| error.to_string())?
            .map(|row| {
                let (key, _) = row.map_err(|error| error.to_string())?;
                let (row_graph, sequence) = key.value();
                if row_graph != graph_fname {
                    return Err("graph row delta ledger escaped its scope".to_string());
                }
                Ok(sequence)
            })
            .filter_map(|row| match row {
                Ok(sequence) if sequence >= retain => Some(Ok(sequence)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<_, _>>()?;
        for sequence in suffix_keys {
            tables
                .ledger
                .remove((graph_fname, sequence))
                .map_err(|error| error.to_string())?;
        }
        for (offset, line) in append.iter().enumerate() {
            let sequence = retain
                .checked_add(offset as u64)
                .ok_or_else(|| "graph row delta ledger sequence overflow".to_string())?;
            tables
                .ledger
                .insert((graph_fname, sequence), line.as_str())
                .map_err(|error| error.to_string())?;
        }
    }
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;

    // The delta is an authenticated projection detail. Audit the original
    // opaque operation receipt so sensitive row properties are not copied
    // into audit/status/outbox surfaces -- but only when the ORIGINAL,
    // pre-opaque-wrapped method's policy actually calls for it (`audited`,
    // resolved by the caller before the operation was compiled; see this
    // function's doc comment). `TouchNodes` is the standing example of a
    // state-backed method that is durable but intentionally unaudited: the
    // opaque receipt shape here is identical to an audited method's, so this
    // flag -- not the operation's (rewritten) method -- is what tells the two
    // apart.
    #[cfg(feature = "security")]
    if audited {
        let mut audit = write
            .graph(graph_fname)?
            .open_scoped_table(AUDIT)
            .map_err(|e| e.to_string())?;
        for operation in &batch.operations {
            append_audit_entry(
                &mut audit,
                staged_audit_tail,
                graph_fname,
                &operation.method,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn open_native_operation_graph_tables<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, &'static str, &'static [u8]>,
        ScopedOwnerTableMut<'txn, &'static str, u64>,
    ),
    String,
> {
    let nodes = write
        .graph(graph_fname)?
        .open_scoped_table(NODES)
        .map_err(|e| e.to_string())?;
    let native_work_items = write
        .graph(graph_fname)?
        .open_scoped_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let edges = write
        .graph(graph_fname)?
        .open_scoped_table(EDGES)
        .map_err(|e| e.to_string())?;
    let ledger = write
        .graph(graph_fname)?
        .open_scoped_table(LEDGER)
        .map_err(|e| e.to_string())?;
    let semantic = write
        .graph(graph_fname)?
        .open_scoped_table(SEMANTIC)
        .map_err(|e| e.to_string())?;
    let command_sequences = write
        .graph(graph_fname)?
        .open_scoped_table(WORK_ITEM_COMMAND_SEQUENCE)
        .map_err(|e| e.to_string())?;
    Ok((
        nodes,
        native_work_items,
        edges,
        ledger,
        semantic,
        command_sequences,
    ))
}

#[allow(clippy::type_complexity)]
fn open_native_operation_resource_tables_a<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static str>,
    ),
    String,
> {
    let resource_reservations = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATIONS)
        .map_err(|e| e.to_string())?;
    let resource_tenant_index = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
        .map_err(|e| e.to_string())?;
    let resource_attempts = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
        .map_err(|e| e.to_string())?;
    let resource_hosts = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_HOSTS)
        .map_err(|e| e.to_string())?;
    let resource_exclusivity = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_EXCLUSIVITY)
        .map_err(|e| e.to_string())?;
    Ok((
        resource_reservations,
        resource_tenant_index,
        resource_attempts,
        resource_hosts,
        resource_exclusivity,
    ))
}

#[allow(clippy::type_complexity)]
fn open_native_operation_resource_tables_b<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), u64>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), u64>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let resource_fairness = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_FAIRNESS)
        .map_err(|e| e.to_string())?;
    let resource_concurrency = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_CONCURRENCY)
        .map_err(|e| e.to_string())?;
    let resource_anti_affinity = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_ANTI_AFFINITY)
        .map_err(|e| e.to_string())?;
    let resource_disk_policies = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_DISK_POLICIES)
        .map_err(|e| e.to_string())?;
    Ok((
        resource_fairness,
        resource_concurrency,
        resource_anti_affinity,
        resource_disk_policies,
    ))
}

#[allow(clippy::type_complexity)]
fn open_native_operation_lane_tables<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<
            'txn,
            (
                &'static str,
                &'static str,
                &'static str,
                &'static str,
                u64,
                &'static str,
            ),
            u8,
        >,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let lane_holds = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::HOLDS)
        .map_err(|e| e.to_string())?;
    let lane_work_item_index = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::WORK_ITEM_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_counters = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::COUNTERS)
        .map_err(|e| e.to_string())?;
    let lane_pressure_index = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::PRESSURE_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_policies = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::POLICIES)
        .map_err(|e| e.to_string())?;
    Ok((
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
    ))
}

/// GOC-19/GOC-20 (BUG-015 "B9"): a `CommitWorkItemResult` batch may
/// additionally carry co-committed `AddNode` provenance operations
/// (RunTrace/ToolCall/OutcomeEvaluation) so a WorkItem's terminal status
/// and its provenance land in the SAME redb write transaction -- never
/// one durable without the other. Validated ONCE, before any row is
/// touched below, so a disallowed shape is refused without partially
/// applying anything in this transaction. Every other WorkItem method
/// (Claim/Renew/Cancel/Defer/CasMetadata) keeps the original strict
/// single-operation rule enforced further down -- this relaxation is
/// deliberately narrow to `CommitWorkItemResult` alone, which is the one
/// terminal transition `crates/eg-types/src/outcome_bundle.rs`'s
/// `CommitOutcomeBundle` (GOC-20) is designed to accompany.
fn validate_native_operations_commit_work_item_result_shape(
    batch: &MutationBatch,
) -> Result<(), String> {
    let work_item_result_ops = batch
        .operations
        .iter()
        .filter(|op| matches!(&op.method, Method::CommitWorkItemResult { .. }))
        .count();
    if work_item_result_ops > 1 {
        return Err(
            "a MutationBatch may contain at most one CommitWorkItemResult operation".to_string(),
        );
    }
    if work_item_result_ops == 1 {
        let Some((terminal_index, extension)) =
            batch
                .operations
                .iter()
                .enumerate()
                .find_map(|(index, operation)| match &operation.method {
                    Method::CommitWorkItemResult {
                        outcome_extension, ..
                    } => Some((index, outcome_extension.as_ref())),
                    _ => None,
                })
        else {
            return Err("terminal operation shape could not be resolved".to_string());
        };
        if terminal_index != 0 {
            return Err("CommitWorkItemResult must be the first operation".to_string());
        }
        if let Some(extension) = extension {
            if batch.operations.len() != 1 {
                return Err(
                    "a terminal outcome extension owns receipt rows and must be the only operation"
                        .to_string(),
                );
            }
            let event_intents: Vec<_> = batch
                .outbox
                .iter()
                .filter(|intent| intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC)
                .collect();
            if event_intents.len() != 1 {
                return Err(
                    "a terminal outcome extension must carry exactly one run-event outbox intent"
                        .to_string(),
                );
            }
            let event_intent = event_intents[0];
            if event_intent.key != batch.batch_id {
                return Err(
                    "terminal run-event outbox key must equal the mutation batch id".to_string(),
                );
            }
            let event: eg_types::outcome_bundle::RunEvent =
                rmp_serde::from_slice(&event_intent.payload).map_err(|_| {
                    "terminal run-event outbox payload is not a valid RunEvent".to_string()
                })?;
            event.validate_for_bundle(&extension.outcome_bundle)?;
            let fence_token = extension.outcome_bundle.fence_token.to_string();
            let completeness = serde_json::to_value(extension.outcome_bundle.completeness)
                .map_err(|error| format!("outcome completeness encoding failed: {error}"))?
                .as_str()
                .ok_or_else(|| "outcome completeness encoding was not a string".to_string())?
                .to_string();
            let missing_refs = serde_json::to_string(&extension.outcome_bundle.missing_refs)
                .map_err(|error| format!("outcome missing_refs encoding failed: {error}"))?;
            use sha2::{Digest, Sha256};
            let actor = batch
                .envelope
                .operation()
                .ok_or_else(|| "terminal batch envelope has no operation actor".to_string())?
                .authority
                .actor
                .as_str();
            let graph = batch
                .identity
                .scope()
                .graph_name()
                .ok_or_else(|| "terminal batch identity is not graph-scoped".to_string())?
                .as_str();
            let mut scope_digest = Sha256::new();
            scope_digest.update(batch.identity.tenant().as_str().as_bytes());
            scope_digest.update([0]);
            scope_digest.update(graph.as_bytes());
            let scope_digest = hex::encode(scope_digest.finalize());
            for (field, expected) in [
                ("batch_id", batch.batch_id.as_str()),
                (
                    "delegation_id",
                    extension.outcome_bundle.delegation_id.as_str(),
                ),
                (
                    "delegator_id",
                    extension.outcome_bundle.delegator_id.as_str(),
                ),
                (
                    "selected_agent_id",
                    extension.outcome_bundle.selected_agent_id.as_str(),
                ),
                (
                    "executor_lease_actor",
                    extension.outcome_bundle.executor_lease_actor.as_str(),
                ),
                ("outcome", extension.outcome_bundle.outcome.as_str()),
                (
                    "work_item_id",
                    extension.outcome_bundle.work_item_id.as_str(),
                ),
                ("run_id", extension.outcome_bundle.run_id.as_str()),
                ("fence_token", fence_token.as_str()),
                (
                    "capability_digest",
                    extension.outcome_bundle.capability_digest.as_str(),
                ),
                (
                    "catalog_digest",
                    extension.outcome_bundle.catalog_digest.as_str(),
                ),
                (
                    "policy_digest",
                    extension.outcome_bundle.policy_digest.as_str(),
                ),
                (
                    "model_digest",
                    extension.outcome_bundle.model_digest.as_str(),
                ),
                ("completeness", completeness.as_str()),
                ("missing_refs", missing_refs.as_str()),
                ("actor", actor),
                ("scope_sha256", scope_digest.as_str()),
            ] {
                if event_intent.headers.get(field).map(String::as_str) != Some(expected) {
                    return Err(format!(
                        "terminal run-event outbox header '{field}' is not bound"
                    ));
                }
            }
            if extension.outcome_bundle.result_ref.as_deref()
                != event_intent.headers.get("result_ref").map(String::as_str)
            {
                return Err("terminal run-event outbox result_ref header is not bound".to_string());
            }
        } else if batch.operations.iter().any(|op| {
            !matches!(
                &op.method,
                Method::CommitWorkItemResult { .. } | Method::AddNode { .. }
            )
        }) {
            return Err(
                "a CommitWorkItemResult MutationBatch may only carry additional AddNode \
                 (provenance) operations"
                    .to_string(),
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_native_clear_or_delete_graph_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    resource_reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    resource_attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    resource_hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    resource_fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    resource_anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    resource_disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_work_item_index: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    lane_counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    clear_graph_rows(graph_fname, nodes, edges, ledger)?;
    command_sequences
        .remove(graph_fname)
        .map_err(|e| e.to_string())?;
    clear_resource_rows(
        graph_fname,
        resource_reservations,
        resource_tenant_index,
        resource_attempts,
        resource_hosts,
        resource_exclusivity,
        resource_fairness,
        resource_concurrency,
        resource_anti_affinity,
        resource_disk_policies,
        crypto,
    )?;
    development_lane::clear_native_graph_rows_in_wtx_with_lane_tables(
        write,
        graph_fname,
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
        crypto,
    )?;
    capacity_lease::clear_graph_rows(write, graph_fname)?;
    work_item_capability::clear_graph_rows_with_native(write, graph_fname, native_work_items)?;
    Ok(())
}

/// The batch-level context both native WorkItem-submit operations need: the
/// batch being applied (its `batch_id` is the outbox id, and its operation count
/// carries the one-result-producing-operation invariant), the authoritative
/// commit timestamp, and the sealing handle. Bundled so the two operations stay
/// inside clippy's parameter cap; each field is the value the caller already
/// passed positionally.
#[derive(Clone, Copy)]
struct NativeSubmitScope<'a> {
    batch: &'a MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'a>,
}

/// The shared inputs for the native row phase of one admitted graph member.
///
/// The table capability is already tied to the member's `ShardWrite`; keeping
/// it here prevents the operation loop from manufacturing a second transaction
/// or accidentally mixing a graph name with another member's tables.
#[derive(Clone, Copy)]
struct MutationRowCtx<'a> {
    write: &'a ShardWrite<'a>,
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    crypto: DurableCrypto<'a>,
}

/// The tail every native submit-work-item operation shares once its row
/// applier has returned a result: refuse to overwrite an already-produced
/// result or co-commit alongside a second operation (a `SubmitWorkItem(s)`
/// batch is only ever admitted as the sole operation), then encode the result
/// as the batch's raw response payload. `label` names the request kind in the
/// refusal message, so `SubmitWorkItem` and `SubmitWorkItems` keep their own
/// distinct wording even though the mechanics are identical.
fn finish_native_submit_work_item_operation<T: serde::Serialize>(
    result: T,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    label: &str,
) -> Result<(), String> {
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(format!(
            "{label} MutationBatch must contain exactly one result-producing operation"
        ));
    }
    let payload = crate::protocol::ResultPayload::raw(&result)?;
    *generated_result = Some(rmp_serde::to_vec_named(&payload).map_err(|e| e.to_string())?);
    Ok(())
}

fn apply_native_submit_work_item_operation(
    graph_fname: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    scope: NativeSubmitScope<'_>,
    generated_result: &mut Option<Vec<u8>>,
) -> Result<(), String> {
    let NativeSubmitScope {
        batch,
        committed_at_ms,
        crypto,
    } = scope;
    let result = apply_submit_work_item_rows(
        graph_fname,
        request,
        nodes,
        edges,
        command_sequences,
        WorkItemCommitScope {
            crypto,
            authoritative_now_ms: committed_at_ms,
            outbox_id: &batch.batch_id,
        },
    )?;
    finish_native_submit_work_item_operation(result, batch, generated_result, "SubmitWorkItem")
}

fn apply_native_submit_work_items_operation(
    graph_fname: &str,
    request: &eg_types::native_control::SubmitWorkItemsRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    scope: NativeSubmitScope<'_>,
    generated_result: &mut Option<Vec<u8>>,
) -> Result<(), String> {
    let NativeSubmitScope {
        batch,
        committed_at_ms,
        crypto,
    } = scope;
    let result = apply_submit_work_items_rows(
        graph_fname,
        request,
        nodes,
        edges,
        command_sequences,
        WorkItemCommitScope {
            crypto,
            authoritative_now_ms: committed_at_ms,
            outbox_id: &batch.batch_id,
        },
    )?;
    finish_native_submit_work_item_operation(result, batch, generated_result, "SubmitWorkItems")
}

#[allow(clippy::too_many_arguments)]
fn apply_native_work_item_family_operation(
    graph_fname: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_work_item_index: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    lane_counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(
        graph_fname,
        batch.batch_id.as_str(),
        method,
        nodes,
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
        native_work_items,
        crypto,
    )?
    .ok_or_else(|| "WorkItem mutation produced no durable result".to_string())?;
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "WorkItem MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

// GOC-19/GOC-20 (BUG-015 "B9"): split out from the shared WorkItem-family
// helper above -- this is the ONE WorkItem terminal transition allowed to
// co-commit additional `AddNode` provenance operations in the same batch
// (validated once, before the operation loop, by
// `validate_native_operations_commit_work_item_result_shape`). The
// `batch.operations.len() != 1` sub-check is deliberately dropped here;
// `generated_result.is_some()` alone still guarantees at most one
// result-producing operation applies.
#[allow(clippy::too_many_arguments)]
fn apply_native_commit_work_item_result_operation(
    graph_fname: &str,
    batch_id: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_work_item_index: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    lane_counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(
        graph_fname,
        batch_id,
        method,
        nodes,
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
        native_work_items,
        crypto,
    )?
    .ok_or_else(|| "WorkItem mutation produced no durable result".to_string())?;
    if generated_result.is_some() {
        return Err(
            "WorkItem MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_native_resource_reservation_operation(
    graph_fname: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    resource_attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    resource_hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    resource_fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    resource_anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    resource_disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_resource_reservation_rows(
        graph_fname,
        method,
        nodes,
        resource_reservations,
        resource_tenant_index,
        resource_attempts,
        resource_hosts,
        resource_exclusivity,
        resource_fairness,
        resource_concurrency,
        resource_anti_affinity,
        resource_disk_policies,
        crypto,
    )?
    .ok_or_else(|| "resource reservation mutation produced no durable result".to_string())?;
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "resource reservation MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    *generated_result = Some(rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?);
    Ok(())
}

/// True for the `ApplyMutation` row that merely carries a crossmodal projection
/// in a batch that already staged one; the projection itself is applied by
/// `apply_crossmodal_projection_rows`.
fn native_operation_is_crossmodal_carrier(method: &Method, crossmodal_present: bool) -> bool {
    crossmodal_present
        && matches!(
            method,
            Method::ApplyMutation { event_type, .. } if event_type == "crossmodal_operation"
        )
}

#[allow(clippy::too_many_arguments)]
fn apply_one_native_operation_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    operation: &MutationOperation,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    generated_result: &mut Option<Vec<u8>>,
    crossmodal_present: bool,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    resource_reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    resource_attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    resource_hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    resource_fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    resource_anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    resource_disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_work_item_index: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    lane_counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
) -> Result<(), String> {
    // Hoisted out of the match below (it was the arm immediately before the
    // wildcard, and no earlier arm can match `ApplyMutation`, so the dispatch
    // order is unchanged): a crossmodal envelope's carrier row is applied by
    // `apply_crossmodal_projection_rows`, not here.
    if native_operation_is_crossmodal_carrier(&operation.method, crossmodal_present) {
        return Ok(());
    }
    match &operation.method {
        Method::CreateGraph { .. } => Ok(()),
        Method::DeleteGraph { .. } | Method::ClearGraph => apply_native_clear_or_delete_graph_rows(
            write,
            graph_fname,
            nodes,
            edges,
            ledger,
            command_sequences,
            resource_reservations,
            resource_tenant_index,
            resource_attempts,
            resource_hosts,
            resource_exclusivity,
            resource_fairness,
            resource_concurrency,
            resource_anti_affinity,
            resource_disk_policies,
            lane_holds,
            lane_work_item_index,
            lane_counters,
            lane_pressure_index,
            lane_policies,
            native_work_items,
            crypto,
        ),
        Method::ClearLedger => clear_ledger_rows(graph_fname, ledger),
        Method::SubmitWorkItem { request } => apply_native_submit_work_item_operation(
            graph_fname,
            request,
            nodes,
            edges,
            command_sequences,
            NativeSubmitScope {
                batch,
                committed_at_ms,
                crypto,
            },
            generated_result,
        ),
        Method::SubmitWorkItems { request } => apply_native_submit_work_items_operation(
            graph_fname,
            request,
            nodes,
            edges,
            command_sequences,
            NativeSubmitScope {
                batch,
                committed_at_ms,
                crypto,
            },
            generated_result,
        ),
        method @ (Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }) => apply_native_work_item_family_operation(
            graph_fname,
            method,
            nodes,
            lane_holds,
            lane_work_item_index,
            lane_counters,
            lane_pressure_index,
            lane_policies,
            native_work_items,
            batch,
            generated_result,
            crypto,
        ),
        method @ Method::CommitWorkItemResult { .. } => {
            apply_native_commit_work_item_result_operation(
                graph_fname,
                batch.batch_id.as_str(),
                method,
                nodes,
                lane_holds,
                lane_work_item_index,
                lane_counters,
                lane_pressure_index,
                lane_policies,
                native_work_items,
                generated_result,
                crypto,
            )
        }
        method @ (Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }) => apply_native_resource_reservation_operation(
            graph_fname,
            method,
            nodes,
            resource_reservations,
            resource_tenant_index,
            resource_attempts,
            resource_hosts,
            resource_exclusivity,
            resource_fairness,
            resource_concurrency,
            resource_anti_affinity,
            resource_disk_policies,
            batch,
            generated_result,
            crypto,
        ),
        method => {
            let mut tables = GraphRowTablesRef {
                nodes,
                edges,
                ledger,
                semantic,
                native_work_items,
            };
            apply_method_rows_ref(graph_fname, method, &mut tables, crypto)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_native_operation_rows_loop(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    generated_result: &mut Option<Vec<u8>>,
    crossmodal_present: bool,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    command_sequences: &mut ScopedOwnerTableMut<'_, &str, u64>,
    resource_reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    resource_attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    resource_hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    resource_fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    resource_concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    resource_anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    resource_disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_holds: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_work_item_index: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    lane_counters: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    lane_pressure_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    #[cfg(feature = "security")] audit: &mut ScopedOwnerTableMut<'_, (&str, u64), &[u8]>,
) -> Result<(), String> {
    for operation in &batch.operations {
        apply_one_native_operation_row(
            write,
            graph_fname,
            batch,
            operation,
            committed_at_ms,
            crypto,
            generated_result,
            crossmodal_present,
            nodes,
            native_work_items,
            edges,
            ledger,
            semantic,
            command_sequences,
            resource_reservations,
            resource_tenant_index,
            resource_attempts,
            resource_hosts,
            resource_exclusivity,
            resource_fairness,
            resource_concurrency,
            resource_anti_affinity,
            resource_disk_policies,
            lane_holds,
            lane_work_item_index,
            lane_counters,
            lane_pressure_index,
            lane_policies,
        )?;
        #[cfg(feature = "security")]
        append_audit_entry(audit, staged_audit_tail, graph_fname, &operation.method)?;
    }
    Ok(())
}

fn apply_native_operations(
    ctx: &MutationRowCtx<'_>,
    committed_at_ms: u64,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
    generated_result: &mut Option<Vec<u8>>,
    crossmodal_present: bool,
) -> Result<(), String> {
    let MutationRowCtx {
        write,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    let _ = audited;
    let (
        mut nodes,
        mut native_work_items,
        mut edges,
        mut ledger,
        mut semantic,
        mut command_sequences,
    ) = open_native_operation_graph_tables(write, graph_fname)?;
    let (
        mut resource_reservations,
        mut resource_tenant_index,
        mut resource_attempts,
        mut resource_hosts,
        mut resource_exclusivity,
    ) = open_native_operation_resource_tables_a(write, graph_fname)?;
    let (
        mut resource_fairness,
        mut resource_concurrency,
        mut resource_anti_affinity,
        mut resource_disk_policies,
    ) = open_native_operation_resource_tables_b(write, graph_fname)?;
    let (
        mut lane_holds,
        mut lane_work_item_index,
        mut lane_counters,
        mut lane_pressure_index,
        mut lane_policies,
    ) = open_native_operation_lane_tables(write, graph_fname)?;
    #[cfg(feature = "security")]
    let mut audit = write
        .graph(graph_fname)?
        .open_scoped_table(AUDIT)
        .map_err(|e| e.to_string())?;

    validate_native_operations_commit_work_item_result_shape(batch)?;

    apply_native_operation_rows_loop(
        write,
        graph_fname,
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        generated_result,
        crossmodal_present,
        &mut nodes,
        &mut native_work_items,
        &mut edges,
        &mut ledger,
        &mut semantic,
        &mut command_sequences,
        &mut resource_reservations,
        &mut resource_tenant_index,
        &mut resource_attempts,
        &mut resource_hosts,
        &mut resource_exclusivity,
        &mut resource_fairness,
        &mut resource_concurrency,
        &mut resource_anti_affinity,
        &mut resource_disk_policies,
        &mut lane_holds,
        &mut lane_work_item_index,
        &mut lane_counters,
        &mut lane_pressure_index,
        &mut lane_policies,
        #[cfg(feature = "security")]
        &mut audit,
    )?;

    // The compact graph/control path can replace or remove a linked
    // WorkItem just as a snapshot/row-delta can.  Release the ordinary
    // table guards, then run the same lane lifecycle validator inside this
    // write transaction before status/outbox metadata is staged.
    drop(nodes);
    drop(edges);
    drop(ledger);
    drop(semantic);
    drop(command_sequences);
    drop(native_work_items);
    drop(lane_holds);
    drop(lane_work_item_index);
    drop(lane_counters);
    drop(lane_pressure_index);
    drop(lane_policies);
    #[cfg(feature = "security")]
    drop(audit);
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;
    Ok(())
}

/// The compact graph/control path can replace or remove a linked WorkItem
/// just as a snapshot/row-delta can (`clears_semantic`); a lifecycle DeleteGraph
/// additionally purges the PRIOR incarnation's mutation-authority rows before
/// this delete writes its own fresh tombstone record.
fn apply_post_row_cleanup(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    lifecycle: Option<&(bool, String, Option<GraphType>)>,
    authoritative_state_msgpack_is_none: bool,
) -> Result<(), String> {
    let clears_semantic = authoritative_state_msgpack_is_none
        && (matches!(lifecycle, Some((false, _, _)))
            || batch
                .operations
                .iter()
                .any(|operation| matches!(&operation.method, Method::ClearGraph)));
    if clears_semantic {
        let mut semantic = write
            .graph(graph_fname)?
            .open_scoped_table(SEMANTIC)
            .map_err(|e| e.to_string())?;
        semantic.remove(graph_fname).map_err(|e| e.to_string())?;
    }
    if matches!(lifecycle, Some((false, _, _))) {
        clear_change_material_rows(write, graph_fname)?;
        // The mutation authority is retired by the kernel when the scope is
        // deleted. Its ledger, replay, outbox, cursor, fence, and version rows
        // must not be swept by this payload transaction: doing so would create
        // a second authority and would race the kernel's retirement proof.
    }
    Ok(())
}

/// Everything one batch's row phase needs that was resolved before the rows.
struct MutationBatchPlan {
    staged_state: Option<AuthoritativeGraphState>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
}

/// The DOMAIN validation a batch must pass before a single row is touched.
///
/// This is what is left of the old prelude. The idempotency, batch-id
/// uniqueness, OCC and fence checks that used to stand beside these are the
/// kernel's, run inside the transaction by `commit::begin`; a `Replayed`
/// outcome is likewise the kernel's answer, resolved at admission, so this no
/// longer returns one. What remains is what only the domain knows: whether the
/// batch's route and lowering are coherent, whether it is a lifecycle
/// operation, and whether the change envelope's preconditions -- not already
/// committed, content version and cursor as expected -- hold.
fn prepare_and_validate_mutation_batch(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    change: Option<&ChangeEnvelope>,
    authoritative_state_msgpack: Option<&[u8]>,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    crossmodal_present: bool,
    crypto: DurableCrypto<'_>,
) -> Result<MutationBatchPlan, String> {
    let _ = crossmodal_present;
    batch.validate_write_budget()?;
    let staged_state = resolve_mutation_authoritative_state(batch, authoritative_state_msgpack)?;
    let integrity_policy_update = resolve_integrity_policy_update(staged_state.as_ref());
    validate_mutation_batch_route_and_lowering(
        batch,
        graph_fname,
        crossmodal,
        staged_state.is_none(),
    )?;
    let lifecycle = detect_and_validate_lifecycle(batch, graph_fname)?;
    // Governance material remains caller-scoped even though the physical
    // mutation ledger is rebound to the shard scope. Preserve the validated
    // ChangeEnvelope mutation tenant for its content/cursor/material keys.
    let change_tenant = change.map_or(batch.identity.tenant().as_str(), |change| {
        change.mutation.identity.tenant().as_str()
    });
    validate_change_envelope_preconditions(write, graph_fname, change_tenant, change, crypto)?;
    Ok(MutationBatchPlan {
        staged_state,
        integrity_policy_update,
        lifecycle,
    })
}

/// A single injected-crashpoint check + the matching certification-fault hook,
/// for the two crashpoints `apply_mutation_batch_in_wtx` itself observes
/// (`BeforeCommit`/`AfterCommitBeforeAck` are the CALLER's, not this
/// function's -- see `commit_mutation_batch_inner`).
fn run_mutation_batch_crashpoint(
    batch: &MutationBatch,
    crashpoint: Option<MutationBatchCrashpoint>,
    at: MutationBatchCrashpoint,
) -> Result<(), String> {
    if crashpoint == Some(at) {
        let message = match at {
            MutationBatchCrashpoint::BeforeRows => "injected crash before mutation rows",
            MutationBatchCrashpoint::AfterRowsBeforeMetadata => {
                "injected crash after mutation rows"
            }
            _ => "injected crash",
        };
        return Err(message.to_string());
    }
    let phase = match at {
        MutationBatchCrashpoint::BeforeRows => {
            crate::mutation_batch::MutationCommitPhase::BeforeRows
        }
        MutationBatchCrashpoint::AfterRowsBeforeMetadata => {
            crate::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata
        }
        _ => return Ok(()),
    };
    crate::mutation_batch::apply_certification_fault(batch, phase)
}

/// Dispatch to whichever of the three row-application shapes this batch uses
/// (authoritative Snapshot / authoritative RowDelta / native per-Method rows),
/// returning the native path's `generated_result` (`None` for the other two
/// shapes, exactly as the pre-decomposition code left `generated_result`
/// untouched outside the native/`else` branch).
#[allow(clippy::too_many_arguments)]
fn apply_state_dispatch_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    staged_state: Option<&AuthoritativeGraphState>,
    batch: &MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
    crossmodal_present: bool,
) -> Result<Option<Vec<u8>>, String> {
    let mut generated_result: Option<Vec<u8>> = None;
    match staged_state {
        Some(AuthoritativeGraphState::Snapshot(snapshot)) => {
            apply_snapshot_state(
                write,
                graph_fname,
                snapshot,
                batch,
                crypto,
                #[cfg(feature = "security")]
                staged_audit_tail,
                audited,
            )?;
        }
        Some(AuthoritativeGraphState::RowDelta(delta)) => {
            apply_row_delta_state(
                write,
                graph_fname,
                delta,
                batch,
                crypto,
                #[cfg(feature = "security")]
                staged_audit_tail,
                audited,
            )?;
        }
        None => {
            apply_native_operations(
                &MutationRowCtx {
                    write,
                    graph_fname,
                    batch,
                    crypto,
                },
                committed_at_ms,
                #[cfg(feature = "security")]
                staged_audit_tail,
                audited,
                &mut generated_result,
                crossmodal_present,
            )?;
        }
    }
    Ok(generated_result)
}

fn apply_crossmodal_rows_phase(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if let Some(rows) = crossmodal {
        if !rows.methods.is_empty() {
            let mut tables = GraphRowTables::open(write.graph(graph_fname)?)?;
            let mut resource_reservations = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_RESERVATIONS)
                .map_err(|e| e.to_string())?;
            let mut resource_tenant_index = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .map_err(|e| e.to_string())?;
            let mut resource_attempts = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .map_err(|e| e.to_string())?;
            let mut resource_hosts = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_HOSTS)
                .map_err(|e| e.to_string())?;
            let mut resource_exclusivity = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_EXCLUSIVITY)
                .map_err(|e| e.to_string())?;
            let mut resource_fairness = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_FAIRNESS)
                .map_err(|e| e.to_string())?;
            let mut resource_concurrency = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_CONCURRENCY)
                .map_err(|e| e.to_string())?;
            let mut resource_anti_affinity = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_ANTI_AFFINITY)
                .map_err(|e| e.to_string())?;
            let mut resource_disk_policies = write
                .graph(graph_fname)?
                .open_scoped_table(RESOURCE_DISK_POLICIES)
                .map_err(|e| e.to_string())?;
            if rows
                .methods
                .iter()
                .any(|method| matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }))
            {
                clear_resource_rows(
                    graph_fname,
                    &mut resource_reservations,
                    &mut resource_tenant_index,
                    &mut resource_attempts,
                    &mut resource_hosts,
                    &mut resource_exclusivity,
                    &mut resource_fairness,
                    &mut resource_concurrency,
                    &mut resource_anti_affinity,
                    &mut resource_disk_policies,
                    crypto,
                )?;
                development_lane::clear_native_graph_rows_in_wtx(write, graph_fname, crypto)?;
                capacity_lease::clear_graph_rows(write, graph_fname)?;
                work_item_capability::clear_graph_rows_with_native(
                    write,
                    graph_fname,
                    &mut tables.native_work_items,
                )?;
            }
            for method in rows.methods {
                apply_method_rows(graph_fname, method, &mut tables, crypto)?;
            }
            drop(tables);
            development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;
        }
        if rows
            .methods
            .iter()
            .any(|method| matches!(method, Method::ClearGraph))
        {
            let mut semantic = write
                .graph(graph_fname)?
                .open_scoped_table(SEMANTIC)
                .map_err(|e| e.to_string())?;
            semantic.remove(graph_fname).map_err(|e| e.to_string())?;
        }
        apply_crossmodal_projection_rows(
            write,
            graph_fname,
            rows.vectors,
            rows.blob_refs,
            rows.measurements,
            crypto,
        )?;
        // Blob/vector projection is also an in-transaction node/semantic
        // replacement surface.  Re-run the lane policy after it so the final
        // image, not only the pre-projection graph rows, is what can commit.
        development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;
    }
    Ok(())
}

fn write_change_envelope_and_content_version_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let envelope_record = ChangeEnvelopeRecord {
        envelope: change.clone(),
        committed_at_ms,
    };
    let bytes = rmp_serde::to_vec_named(&envelope_record).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    envelopes
        .insert((graph_fname, change.envelope_id.as_str()), sealed.as_ref())
        .map_err(|e| e.to_string())?;

    let bytes = rmp_serde::to_vec_named(&change.content_version).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut versions = write
        .graph(graph_fname)?
        .open_scoped_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    versions
        .insert(
            (
                graph_fname,
                tenant,
                change.content_version.object_id.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn write_change_cursor_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(cursor).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut cursors = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    cursors
        .insert(
            (
                graph_fname,
                tenant,
                cursor.source.as_str(),
                cursor.partition.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn write_change_material_blobs(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut blobs = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_BLOBS)
        .map_err(|e| e.to_string())?;
    for blob in &change.blobs {
        let key = (graph_fname, tenant, blob.blob_id.as_str());
        match blob.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(blob).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                blobs
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                blobs.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

fn write_change_material_features(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut features = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_FEATURES)
        .map_err(|e| e.to_string())?;
    for feature in &change.features {
        let key = (graph_fname, tenant, feature.feature_id.as_str());
        match feature.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(feature).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                features
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                features.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

fn write_change_material_evidence(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut evidence = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_EVIDENCE)
        .map_err(|e| e.to_string())?;
    for item in &change.evidence {
        let key = (graph_fname, tenant, item.evidence_id.as_str());
        match item.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(item).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                evidence
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                evidence.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

fn write_change_material_policies(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut policies = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_POLICIES)
        .map_err(|e| e.to_string())?;
    for policy in &change.policies {
        let key = (graph_fname, tenant, policy.policy_id.as_str());
        match policy.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(policy).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                policies
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                policies.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

fn write_change_material_lineage(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut lineage = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_LINEAGE)
        .map_err(|e| e.to_string())?;
    for item in &change.lineage {
        let key = (graph_fname, tenant, item.lineage_id.as_str());
        match item.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(item).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                lineage
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                lineage.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

/// The governed change envelope's own rows: the retained envelope, the content
/// version, the source cursor and the five material families.
///
/// The `epistemic.change.committed.v1` OUTBOX event that used to be written
/// here by hand is gone from this function, not from the system: outbox rows
/// are written by `commit::finish` from `batch.outbox`, so the event is
/// compiled onto the batch as an intent (see
/// `server::mutation_batch::compile`). One writer, one ordinal space, one
/// order -- where before an ordinal counter was threaded through four writers
/// and asserted at the end.
fn apply_change_envelope_commit_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let tenant = change.mutation.identity.tenant().as_str();
    write_change_envelope_and_content_version_rows(
        write,
        graph_fname,
        tenant,
        change,
        committed_at_ms,
        crypto,
    )?;

    if let Some(cursor) = &change.cursor {
        write_change_cursor_row(write, graph_fname, tenant, cursor, crypto)?;
    }

    write_change_material_blobs(write, graph_fname, tenant, change, crypto)?;
    write_change_material_features(write, graph_fname, tenant, change, crypto)?;
    write_change_material_evidence(write, graph_fname, tenant, change, crypto)?;
    write_change_material_policies(write, graph_fname, tenant, change, crypto)?;
    write_change_material_lineage(write, graph_fname, tenant, change, crypto)
}

fn resolve_default_graph_meta_update(
    meta: &redb::Table<'_, &str, &[u8]>,
    graph_fname: &str,
    batch: &MutationBatch,
    integrity_policy_update: Option<&Option<crate::graph::IntegrityPolicy>>,
) -> Result<Option<Vec<u8>>, String> {
    let existing = meta
        .get(graph_fname)
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_vec());
    let encoded = match (existing, integrity_policy_update) {
        (Some(existing), Some(policy)) => {
            let record = decode_meta_record(graph_fname, &existing)?;
            Some(encode_meta_record(
                &record.name,
                record.graph_type,
                &record.incarnation_id,
                policy.as_ref(),
            )?)
        }
        (Some(_), None) => None,
        (None, policy) => Some(encode_meta_record(
            mutation_batch_graph_name(batch)?,
            GraphType::Global,
            &batch.batch_id,
            policy.and_then(Option::as_ref),
        )?),
    };
    Ok(encoded)
}

fn write_mutation_batch_graph_meta_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
) -> Result<(), String> {
    let mut meta = write
        .control()
        .open_table(GRAPH_META)
        .map_err(|e| e.to_string())?;
    match lifecycle {
        Some((true, graph_name, Some(graph_type))) => {
            let encoded = encode_meta_record(
                &graph_name,
                graph_type,
                &batch.batch_id,
                integrity_policy_update.as_ref().and_then(Option::as_ref),
            )?;
            meta.insert(graph_fname, encoded.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Some((false, _, _)) => {
            meta.remove(graph_fname).map_err(|e| e.to_string())?;
        }
        _ => {
            let encoded = resolve_default_graph_meta_update(
                &meta,
                graph_fname,
                batch,
                integrity_policy_update.as_ref(),
            )?;
            if let Some(encoded) = encoded {
                meta.insert(graph_fname, encoded.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

/// Methods whose complete authoritative effect is represented by the NODES/EDGES/
/// LEDGER row transaction below.  Anything else must first be lowered by its
/// surface adapter; accepting it and letting `apply_method_rows`'s non-applicable arm
/// run would create a committed status with missing state.
fn supports_atomic_batch_rows(method: &Method) -> bool {
    matches!(
        method,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::BatchUpdate { .. }
            | Method::AddEmbedding { .. }
            | Method::ClearGraph
            | Method::ClearLedger
            | Method::CreateGraph { .. }
            | Method::DeleteGraph { .. }
            | Method::ClaimWorkItem { .. }
            | Method::SubmitWorkItem { .. }
            | Method::SubmitWorkItems { .. }
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
            | Method::CasWorkItemMetadata { .. }
            | Method::ReserveWorkItemResources { .. }
            | Method::ReleaseWorkItemResources { .. }
            | Method::ReclaimWorkItemResources { .. }
            | Method::UpdateResourceHost { .. }
    )
}

fn property_f64(props: &serde_json::Map<String, serde_json::Value>, key: &str) -> f64 {
    props
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0)
}

fn property_u64(props: &serde_json::Map<String, serde_json::Value>, key: &str) -> u64 {
    props
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn property_string<'a>(
    props: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> &'a str {
    props
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

fn write_work_item_props(
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    node_id: &str,
    props: &serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(props).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    nodes
        .insert((graph, node_id), sealed.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Drive the phase-1 statechart MIRROR for one WorkItem transition (ADR-5 / W2.2) and
/// fold its durable `MachineInstance` projection back INTO the same `props` map — so the
/// mirror commits in the SAME redb write transaction as the authoritative lifecycle
/// `status` on the SAME shard. That co-location is what makes a `kill -9` mid-transition
/// unable to split the row from its mirror (both land, or neither does), and it keeps the
/// mirror state Cypher-queryable as ordinary node properties (`machine_state`).
///
/// The redb row's `status` REMAINS the authority in phase 1; this only compares the
/// chart's independently-computed next state against it and raises the divergence alarm on
/// a mismatch (a chart bug caught before the phase-2 authority flip). `pre_status` is the
/// row status BEFORE the authority mutated it; `authoritative_next` is `Some(status)` the
/// authority persisted (the firing handlers always transition, so it is always `Some`).
#[cfg(feature = "statechart")]
fn apply_work_item_mirror(
    props: &mut serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    pre_status: &str,
    event: &str,
    payload: serde_json::Value,
    authoritative_next: Option<&str>,
) {
    let outcome =
        crate::work_item_statechart::mirror_outcome(pre_status, event, payload, authoritative_next);
    if outcome.diverged {
        crate::work_item_statechart::emit_divergence(
            work_item_id,
            pre_status,
            event,
            authoritative_next,
            &outcome.next_state,
        );
    }
    let prior_version = property_u64(props, "machine_version");
    let next_version = if outcome.fired {
        prior_version.saturating_add(1)
    } else {
        prior_version
    };
    props.insert(
        "machine_state".into(),
        serde_json::Value::String(outcome.next_state),
    );
    props.insert(
        "machine_version".into(),
        serde_json::Value::from(next_version),
    );
    props.insert(
        "machine_def_id".into(),
        serde_json::Value::String(crate::work_item_statechart::WORK_ITEM_DEF_ID.clone()),
    );
}

/// Read and decode one encrypted durable row while preserving each table's
/// typed key and decoder.  A missing table is the same as a missing row: older
/// stores may not have introduced every table yet, so callers must see a
/// typed absence rather than a schema error.
/// Read one scope-prefixed owner row of one graph, unsealed and decoded.
///
/// The read half of the shard's own rows. Every table it serves leads its key
/// with the graph name, so the bound is the capability's scope rather than the
/// key the caller passes: a reader for one graph cannot address another's rows
/// in the file they share.
fn read_typed_graph_row<K, T, Decode>(
    shard: &Shard,
    graph_fname: &str,
    table_definition: TableDefinition<'static, K, &[u8]>,
    key: K::SelfType<'_>,
    crypto: DurableCrypto<'_>,
    decode: Decode,
) -> Result<Option<T>, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
    Decode: FnOnce(&[u8]) -> Result<T, String>,
{
    let handle = shard.graph(graph_fname)?;
    let read = shard.read(&handle)?;
    read.scoped_owner_table(table_definition)?
        .get(key)?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode(&bytes)
        })
        .transpose()
}

/// Read one durable batch receipt of one graph.
///
/// The receipt is the kernel's `ledger_batches` row now, not a shard table, so
/// the route binding this used to re-check by decoding is enforced before the
/// read: `Shard::graph` binds the scope derived from `graph_fname`, and a
/// `ScopedRead` on that scope can only see that scope's receipts.
pub(crate) fn read_mutation_batch_for_graph(
    shard: &Shard,
    graph_fname: &str,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::read_ledger(&shard.read(&handle)?, batch_id)
}

pub(crate) fn read_change_envelope(
    shard: &Shard,
    graph_fname: &str,
    envelope_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeEnvelopeRecord>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CHANGE_ENVELOPES,
        (graph_fname, envelope_id),
        crypto,
        decode_durable,
    )
}

pub(crate) fn read_content_version(
    shard: &Shard,
    tenant: &str,
    graph_fname: &str,
    object_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ContentVersion>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CONTENT_VERSIONS,
        (graph_fname, tenant, object_id),
        crypto,
        decode_durable,
    )
}

pub(crate) fn read_change_cursor(
    shard: &Shard,
    tenant: &str,
    graph_fname: &str,
    source: &str,
    partition: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeCursor>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CHANGE_CURSORS,
        (graph_fname, tenant, source, partition),
        crypto,
        decode_durable,
    )
}

/// Every immutable outbox row of one batch, in ordinal order.
pub(crate) fn read_mutation_outbox(
    shard: &Shard,
    graph_fname: &str,
    batch_id: &str,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::read_outbox(&shard.read(&handle)?, batch_id)
}

/// The authoritative version of one graph.
///
/// One counter, one owner. The shard's `mutation_graph_version` table is
/// retired: the kernel's `ledger_versions` row for this graph's bound scope IS
/// the authoritative version, it is what `admit_batch` resolves inside the write
/// transaction, and it is what every admitted batch advances by exactly one.
/// Two counters over one file in one transaction is the dual authority
/// RF-RULING-004 forbids, which is why this is a rename of the reader and a
/// deletion of the table rather than a migration.
pub(crate) fn read_mutation_graph_version(shard: &Shard, graph_fname: &str) -> Result<u64, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::version(&shard.read(&handle)?)
}

/// Durably write/overwrite a graph_meta identity row in its OWN transaction.
pub(crate) fn write_graph_meta(
    shard: &Shard,
    graph: &str,
    name: &str,
    graph_type: GraphType,
) -> Result<(), String> {
    {
        let read = shard.control_read()?;
        let meta = read
            .open_owner_table(GRAPH_META)
            .map_err(|e| e.to_string())?;
        if let Some(existing) = meta.get(graph).map_err(|e| e.to_string())? {
            let record = decode_meta_record(graph, existing.value())?;
            return if record.name == name && record.graph_type == graph_type {
                Ok(())
            } else {
                Err("graph metadata conflict".to_string())
            };
        }
    }
    let incarnation_id = new_incarnation_id(graph);
    write_graph_meta_with_incarnation(shard, graph, name, graph_type, &incarnation_id)
}

/// Durably register an exact lifecycle incarnation. Repeating the same identity
/// is idempotent; attempting to overwrite a live same-name incarnation fails
/// closed so stale work cannot silently retarget itself.
pub(crate) fn write_graph_meta_with_incarnation(
    shard: &Shard,
    graph: &str,
    name: &str,
    graph_type: GraphType,
    incarnation_id: &str,
) -> Result<(), String> {
    reject_reserved_graph(graph)?;
    if incarnation_id.trim().is_empty() {
        return Err("graph incarnation id must not be empty".to_string());
    }
    let op_id = format!("graph_meta/{graph}/{incarnation_id}");
    let (group, batches) = shard.admit_maintenance(&[], &op_id)?;
    let write = ShardWrite::open(shard, &group, &[], &batches)?;
    let result = (|| {
        let mut meta = write
            .control()
            .open_table(GRAPH_META)
            .map_err(|e| e.to_string())?;
        let existing = meta
            .get(graph)
            .map_err(|e| e.to_string())?
            .map(|value| value.value().to_vec());
        if let Some(existing) = existing {
            let record = decode_meta_record(graph, &existing)?;
            if record.incarnation_id != incarnation_id {
                return Err("graph incarnation conflict".to_string());
            }
        } else {
            let encoded = encode_meta_with_incarnation(name, graph_type, incarnation_id)?;
            meta.insert(graph, encoded.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    })();
    let finished = write.finish();
    match (result, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, 0),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

/// Point-read a single node's stored properties (read-through path).
pub(crate) fn read_one_node(
    shard: &Shard,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES).map_err(|e| e.to_string())?;
    let v = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|g| crypto.unseal(g.value()))
        .transpose()?;
    Ok(v)
}

/// Test a batch of node ids against one MVCC snapshot. Eviction needs presence,
/// not decrypted properties, so this avoids N transactions and N payload copies.
/// The returned vector is positionally aligned with `node_ids`.
pub(crate) fn durable_node_presence(
    shard: &Shard,
    graph: &str,
    node_ids: &[String],
) -> Result<Vec<bool>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read
        .scoped_owner_table(NODES)
        .map_err(|error| error.to_string())?;
    let mut present = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        present.push(
            nodes
                .get((graph, node_id.as_str()))
                .map_err(|error| error.to_string())?
                .is_some(),
        );
    }
    Ok(present)
}

fn read_semantic_store(
    semantic: &ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::compute::semantic::SemanticStore>, String> {
    semantic
        .get(graph)
        .map_err(|error| error.to_string())?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode_durable(&bytes)
        })
        .transpose()
}

fn write_semantic_store(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    store: &crate::compute::semantic::SemanticStore,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(store).map_err(|error| error.to_string())?;
    let sealed = crypto.seal(&bytes);
    semantic
        .insert(graph, sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn upsert_durable_embedding(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    node_id: &str,
    embedding: &[f32],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut store = read_semantic_store(semantic, graph, crypto)?.unwrap_or_default();
    store
        .add_embedding(node_id.to_string(), embedding.to_vec())
        .map_err(|error| error.to_string())?;
    write_semantic_store(semantic, graph, &store, crypto)
}

fn remove_durable_embedding(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(mut store) = read_semantic_store(semantic, graph, crypto)? else {
        return Ok(());
    };
    if store.remove_embedding(node_id) {
        write_semantic_store(semantic, graph, &store, crypto)?;
    }
    Ok(())
}

fn remove_durable_edge_pair(
    graph: &str,
    source: &str,
    target: &str,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    let ordinals: Vec<u32> = edges
        .range_inclusive(
            (graph, source, target, 0u32),
            (graph, source, target, u32::MAX),
        )?
        .map(|row| {
            let (key, _) = row.map_err(|error| error.to_string())?;
            Ok(key.value().3)
        })
        .collect::<Result<_, String>>()?;
    for ordinal in ordinals {
        edges
            .remove((graph, source, target, ordinal))
            .map_err(|error| error.to_string())?;
    }
    invalidate_edge_ord(graph, source, target);
    Ok(())
}

fn remove_durable_node(
    graph: &str,
    node_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    remove_durable_node_rows(graph, node_id, nodes, edges)?;
    remove_durable_embedding(semantic, graph, node_id, crypto)
}

fn remove_durable_node_rows(
    graph: &str,
    node_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    nodes
        .remove((graph, node_id))
        .map_err(|error| error.to_string())?;
    // The edge key is `(graph, source, target, ordinal)`: outgoing edges form a
    // prefix, while incoming edges require one bounded scan of this graph.
    let incident: Vec<(String, String, u32)> = edges
        .scope_rows()
        .map_err(|error| error.to_string())?
        .filter_map(|row| match row {
            Ok((key, _)) => {
                let (row_graph, source, target, ordinal) = key.value();
                if row_graph != graph {
                    return Some(Err("graph edge row escaped its scope".to_string()));
                }
                (source == node_id || target == node_id)
                    .then(|| Ok((source.to_string(), target.to_string(), ordinal)))
            }
            Err(error) => Some(Err(error.to_string())),
        })
        .collect::<Result<_, String>>()?;
    for (source, target, ordinal) in incident {
        edges
            .remove((graph, source.as_str(), target.as_str(), ordinal))
            .map_err(|error| error.to_string())?;
        invalidate_edge_ord(graph, &source, &target);
    }
    invalidate_node_edge_ords(graph, node_id);
    Ok(())
}

/// `Method::CompareAndSetNodeFields` row effect.  Evaluates and merges the CAS
/// against the durable pre-image inside the held transaction; persisting
/// `updates_msgpack` by itself discarded every untouched property and could
/// diverge from GraphCore.  A missing node is a silent no-op, as before.
fn apply_cas_node_fields_row(
    graph: &str,
    node_id: &str,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(current) = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
    else {
        return Ok(());
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&current)?;
    let conditions: serde_json::Map<String, serde_json::Value> =
        decode_durable(conditions_msgpack)?;
    let updates: serde_json::Map<String, serde_json::Value> = decode_durable(updates_msgpack)?;
    let matches = conditions.iter().all(|(key, expected)| {
        props.get(key).cloned().unwrap_or(serde_json::Value::Null) == *expected
    });
    if matches {
        props.extend(updates);
        let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
        let blob = crypto.seal(&bytes);
        nodes
            .insert((graph, node_id), blob.as_ref())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// `Method::AddEdge` row effect: both endpoints must already be durable, then the
/// edge is appended at the next ordinal for the pair.
fn apply_add_edge_row(
    graph: &str,
    source_id: &str,
    target_id: &str,
    properties_msgpack: &[u8],
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let source_exists = nodes
        .get((graph, source_id))
        .map_err(|e| e.to_string())?
        .is_some();
    let target_exists = nodes
        .get((graph, target_id))
        .map_err(|e| e.to_string())?
        .is_some();
    if !source_exists || !target_exists {
        return Err(format!(
            "AddEdge requires durable endpoints: source '{}' present={}, target '{}' present={}",
            source_id, source_exists, target_id, target_exists
        ));
    }
    let ord = next_edge_ordinal(edges, graph, source_id, target_id)?;
    let blob = crypto.seal(properties_msgpack);
    edges
        .insert((graph, source_id, target_id, ord), blob.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Translate ONE applied method into redb row writes inside an open transaction.
/// Mirrors `crate::mutation_apply::apply`'s method set: the durable DATA mutations only.
// crate-internal only (no external callers): each parameter is a distinct
// borrowed redb table handle the write touches, plus the method/graph being
// applied -- bundling the table handles into a struct would just move the
// same borrows behind one more layer of indirection.
#[allow(clippy::too_many_arguments)]
/// The row handles every graph-row writer of one member needs, opened once.
///
/// `redb` refuses a second open of a table whose first handle is still alive,
/// and every graph member of a group writes the same `nodes`, so these are
/// opened per member and dropped before the next member's. Bundling them is
/// what keeps the writers below inside the argument cap without threading six
/// same-shaped handles positionally through every one of them.
pub(crate) struct GraphRowTables<'g> {
    pub(crate) nodes: ScopedOwnerTableMut<'g, (&'static str, &'static str), &'static [u8]>,
    pub(crate) edges:
        ScopedOwnerTableMut<'g, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
    pub(crate) ledger: ScopedOwnerTableMut<'g, (&'static str, u64), &'static str>,
    pub(crate) semantic: ScopedOwnerTableMut<'g, &'static str, &'static [u8]>,
    pub(crate) command_sequences: ScopedOwnerTableMut<'g, &'static str, u64>,
    pub(crate) native_work_items:
        ScopedOwnerTableMut<'g, (&'static str, &'static str), &'static [u8]>,
    #[cfg(feature = "security")]
    pub(crate) audit: ScopedOwnerTableMut<'g, (&'static str, u64), &'static [u8]>,
}

/// Borrowed form used by the native operation loop, which already has several
/// table guards open for resource and lane operations. It lets the shared graph
/// row applier enforce the same scope boundary without opening a second handle
/// to any table that the caller owns.
struct GraphRowTablesRef<'a, 'n, 'e, 'l, 's, 'w>
where
    'n: 'a,
    'e: 'a,
    'l: 'a,
    's: 'a,
    'w: 'a,
{
    nodes: &'a mut ScopedOwnerTableMut<'n, (&'static str, &'static str), &'static [u8]>,
    edges: &'a mut ScopedOwnerTableMut<
        'e,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    ledger: &'a mut ScopedOwnerTableMut<'l, (&'static str, u64), &'static str>,
    semantic: &'a mut ScopedOwnerTableMut<'s, &'static str, &'static [u8]>,
    native_work_items: &'a mut ScopedOwnerTableMut<'w, (&'static str, &'static str), &'static [u8]>,
}

impl<'g> GraphRowTables<'g> {
    /// Open one member's graph-row tables, bounded to that member's own scope.
    pub(crate) fn open(
        member: &'g AdmittedOwnerWrite<'g, GraphShardOwner>,
    ) -> Result<Self, String> {
        Ok(Self {
            nodes: member.open_scoped_table(NODES)?,
            edges: member.open_scoped_table(EDGES)?,
            ledger: member.open_scoped_table(LEDGER)?,
            semantic: member.open_scoped_table(SEMANTIC)?,
            command_sequences: member.open_scoped_table(WORK_ITEM_COMMAND_SEQUENCE)?,
            native_work_items: member.open_scoped_table(work_item_capability::NATIVE_WORK_ITEMS)?,
            #[cfg(feature = "security")]
            audit: member.open_scoped_table(AUDIT)?,
        })
    }
}

pub(crate) fn apply_method_rows(
    graph: &str,
    method: &Method,
    tables: &mut GraphRowTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables = GraphRowTablesRef {
        nodes: &mut tables.nodes,
        edges: &mut tables.edges,
        ledger: &mut tables.ledger,
        semantic: &mut tables.semantic,
        native_work_items: &mut tables.native_work_items,
    };
    apply_method_rows_ref(graph, method, &mut tables, crypto)
}

fn apply_method_rows_ref(
    graph: &str,
    method: &Method,
    tables: &mut GraphRowTablesRef<'_, '_, '_, '_, '_, '_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let GraphRowTablesRef {
        nodes,
        edges,
        ledger,
        semantic,
        native_work_items,
        ..
    } = tables;
    work_item_capability::validate_generic_method(graph, method, nodes, native_work_items, crypto)?;
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let blob = crypto.seal(properties_msgpack);
            nodes
                .insert((graph, node_id.as_str()), blob.as_ref())
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveNode { node_id } => {
            remove_durable_node(graph, node_id, nodes, edges, semantic, crypto)?;
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            apply_cas_node_fields_row(
                graph,
                node_id,
                conditions_msgpack,
                updates_msgpack,
                nodes,
                crypto,
            )?;
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            apply_add_edge_row(
                graph,
                source_id,
                target_id,
                properties_msgpack,
                nodes,
                edges,
                crypto,
            )?;
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            remove_durable_edge_pair(graph, source_id, target_id, edges)?;
        }
        Method::BatchUpdate { operations_msgpack } => {
            apply_batch_rows(graph, operations_msgpack, nodes, edges, semantic, crypto)?;
        }
        // ClearGraph and DeleteGraph share one arm because their row effect is
        // byte-identical.  For DeleteGraph the lifecycle caller performs the
        // native/resource drain guard before entering this row applier; keeping
        // its ordinary graph effect here as well means no low-level path
        // (including cross-modal and checkpoint pending methods) can silently
        // commit a no-op graph delete.
        Method::ClearGraph | Method::DeleteGraph { .. } => {
            clear_graph_rows(graph, nodes, edges, ledger)?;
            semantic.remove(graph).map_err(|error| error.to_string())?;
        }
        Method::AddEmbedding { node_id, embedding } => {
            upsert_durable_embedding(semantic, graph, node_id, embedding, crypto)?;
        }
        Method::MintWorkItemClaimCapability { .. }
        | Method::VerifyWorkItemClaimCapability { .. } => {
            return Err(
                "WorkItem claim capabilities require their native authority operation".to_string(),
            );
        }
        _ => {}
    }
    Ok(())
}

// ── O(1) edge-ordinal counter (CONCEPT:EG-KG.storage.redb-store #3) ────────────────────────────
//
// **Why this exists (profiling rationale).** Assigning an edge's ordinal used to
// RANGE-SCAN that (graph,src,tgt)'s existing edge rows on EVERY `AddEdge` to find
// `max+1` — O(degree) B-tree walks inside the held `WriteTransaction`. On a
// high-degree node every insert got slower as the node fan-out grew, burning the
// now-CPU-bound writer (post EG-024). This mirrors the EG-025 audit-tail fix: keep
// an in-memory per-(graph,src,tgt) next-ordinal counter and chain off it with NO
// per-op scan.
//
// **Why an in-memory counter is authoritative.** EG-026 gives each shard a single
// dedicated writer thread (`eg-redb-writer*`), and a graph routes deterministically
// to exactly one shard — so that thread is the ONLY mutator of its EDGES rows.
// Nothing can advance an ordinal behind our back, so a counter living in that
// thread's storage is correct. We hold it in a `thread_local` rather than a threaded
// parameter because the shared `commit_ops`/`commit_crossmodal` signatures are fixed
// by an out-of-scope caller (`redb_backend`); a thread-local is naturally scoped to
// the one writer thread and lives for its whole lifetime (= the `Pending` lifetime
// that holds the EG-025 audit cache). On any OTHER thread (the embedded one-op-per-
// txn path, tests, tooling) the counter is NOT authoritative — another thread could
// be the real writer — so those contexts fall back to an exact bounded B-tree tail
// seek.
//
// **Restart / correctness.** A fresh process ⇒ fresh writer thread ⇒ empty cache ⇒
// the first touch of each (graph,src,tgt) re-seeds from one bounded tail seek (max+1,
// or 0 when none), then advances in RAM. Edge removals on the writer thread
// (RemoveEdge/RemoveNode/ClearGraph/checkpoint-clear) INVALIDATE the relevant cache
// entries so a later AddEdge re-seeds from the post-removal state — preserving the
// exact "reset to 0 once all edges of a pair are gone" behavior of the old scan.
// Because the counter is seeded at the true `max+1` and only ever increments within
// the sole writer, an assigned ordinal can never collide with an existing row and is
// strictly monotonic per (graph,src,tgt).
/// `graph -> source -> target -> next ordinal to assign`, the shape of
/// [`EDGE_ORD_CACHE`]'s thread-local map.
type EdgeOrdCache = HashMap<String, HashMap<String, HashMap<String, u64>>>;

thread_local! {
    /// True iff this thread is a dedicated redb group-commit writer (`eg-redb-writer`
    /// / `eg-redb-writer-<i>`, CONCEPT:EG-KG.backend.sharded-k-way-durable). Computed once per thread; gates whether
    /// the in-RAM edge-ordinal counter below is authoritative.
    static IS_REDB_WRITER: bool = std::thread::current()
        .name()
        .map(|n| n.starts_with("eg-redb-writer"))
        .unwrap_or(false);

    /// `graph -> source -> target -> next ordinal to assign`. The u64 counter can
    /// represent `u32::MAX + 1` as an explicit exhausted sentinel after assigning
    /// the final valid durable ordinal; it is never serialized.
    /// The hierarchy keeps
    /// hot pair lookup expected O(1) while making whole-graph and whole-source
    /// invalidation O(1), rather than retaining over every cached pair.
    static EDGE_ORD_CACHE: RefCell<EdgeOrdCache> = RefCell::new(HashMap::new());
}

/// Next free edge ordinal for a (src,tgt) pair in this graph.
///
/// O(1) on the dedicated writer thread (EG-026/EG-029): the in-RAM counter, seeded once
/// per (graph,src,tgt) from one bounded tail seek. Off the writer thread it is NOT
/// authoritative, so it performs the same exact O(log E) seek on every call.
fn next_edge_ordinal(
    edges: &ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    if !IS_REDB_WRITER.with(|w| *w) {
        return scan_next_edge_ordinal(edges, graph, src, tgt);
    }
    EDGE_ORD_CACHE.with(|c| -> Result<u32, String> {
        let mut cache = c.borrow_mut();
        let targets = cache
            .entry(graph.to_string())
            .or_default()
            .entry(src.to_string())
            .or_default();
        let next = match targets.get(tgt) {
            // Hot path: the cached counter — NO scan inside the held write txn.
            Some(&n) => n,
            // Cold path: first touch since open / restart — seed from one scan.
            None => u64::from(scan_next_edge_ordinal(edges, graph, src, tgt)?),
        };
        let ordinal =
            u32::try_from(next).map_err(|_| "edge ordinal space exhausted".to_string())?;
        targets.insert(tgt.to_string(), next + 1);
        Ok(ordinal)
    })
}

/// Seek the highest existing ordinal for `(graph, src, tgt)` and add one. The
/// composite key range is ordered by ordinal, so `next_back` makes this O(log E)
/// instead of walking all parallel rows for the pair. Used to seed the writer cache
/// and as the off-writer-thread fallback.
fn scan_next_edge_ordinal(
    edges: &ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    let max = edges
        .range_inclusive((graph, src, tgt, 0u32), (graph, src, tgt, u32::MAX))?
        .next_back()
        .transpose()
        .map_err(|e| e.to_string())?
        .map(|(key, _)| key.value().3);
    max.map_or(Ok(0), |ordinal| {
        ordinal
            .checked_add(1)
            .ok_or_else(|| "edge ordinal space exhausted".to_string())
    })
}

/// Drop the cached next-ordinal for ONE (graph,src,tgt) (RemoveEdge). No-op off the
/// writer thread / when the key was never cached.
fn invalidate_edge_ord(graph: &str, src: &str, tgt: &str) {
    EDGE_ORD_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let mut remove_graph = false;
        if let Some(sources) = cache.get_mut(graph) {
            let remove_source = sources.get_mut(src).is_some_and(|targets| {
                targets.remove(tgt);
                targets.is_empty()
            });
            if remove_source {
                sources.remove(src);
            }
            remove_graph = sources.is_empty();
        }
        if remove_graph {
            cache.remove(graph);
        }
    });
}

/// Drop every cached next-ordinal whose SOURCE is `node` in `graph` (RemoveNode sweeps
/// exactly that node's outgoing edges).
fn invalidate_node_edge_ords(graph: &str, node: &str) {
    EDGE_ORD_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let remove_graph = cache.get_mut(graph).is_some_and(|sources| {
            sources.remove(node);
            sources.is_empty()
        });
        if remove_graph {
            cache.remove(graph);
        }
    });
}

/// Drop every cached next-ordinal for `graph` (ClearGraph / purge / checkpoint re-seed).
fn invalidate_graph_edge_ords(graph: &str) {
    EDGE_ORD_CACHE.with(|c| {
        c.borrow_mut().remove(graph);
    });
}

/// Bounded exact-binary proof seam for the cold edge-ordinal seed and the
/// writer-local invalidation path. It writes only opaque synthetic rows into the
/// caller-owned private probe database and returns raw semantic outcomes; release
/// evidence never includes the supplied path.
#[doc(hidden)]
pub fn exact_performance_probe_edge_ordinal(
    database_path: &std::path::Path,
    parallel_rows: usize,
) -> Result<(u32, u32, u32), String> {
    if parallel_rows == 0 || parallel_rows > 100_000 || parallel_rows > u32::MAX as usize {
        return Err("edge-ordinal probe scale is outside its bound".to_string());
    }
    let path = database_path.to_path_buf();
    std::thread::Builder::new()
        .name("eg-redb-writer-g37".to_string())
        .spawn(move || -> Result<(u32, u32, u32), String> {
            let shard = Shard::open(&path)?;
            let graph = "g37".to_string();
            let members = shard.graph_members(std::slice::from_ref(&graph))?;
            let (group, batches) = shard.admit_maintenance(&members, "probe/g37")?;
            let write = ShardWrite::open(&shard, &group, &members, &batches)?;
            let result = (|| {
                let mut edges = write.graph(&graph)?.open_scoped_table(EDGES)?;
                let value = [0u8];
                for ordinal in 0..parallel_rows as u32 {
                    edges
                        .insert(
                            (graph.as_str(), "source", "target", ordinal),
                            value.as_slice(),
                        )
                        .map_err(|error| error.to_string())?;
                }
                let cold = next_edge_ordinal(&edges, &graph, "source", "target")?;
                let hot = next_edge_ordinal(&edges, &graph, "source", "target")?;
                invalidate_edge_ord(&graph, "source", "target");
                let reseeded = next_edge_ordinal(&edges, &graph, "source", "target")?;
                Ok((cold, hot, reseeded))
            })();
            let finished = write.finish();
            let result = match (result, finished) {
                (Ok(value), Ok(())) => Ok(value),
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            };
            shard.mutations().abort_group(group)?;
            result
        })
        .map_err(|error| error.to_string())?
        .join()
        .map_err(|_| "edge-ordinal probe worker panicked".to_string())?
}

fn apply_batch_add_node_row(
    graph: &str,
    index: usize,
    id: &str,
    mut properties_msgpack: Vec<u8>,
    upsert: bool,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if upsert {
        let current = nodes
            .get((graph, id))
            .map_err(|error| error.to_string())?
            .map(|stored| crypto.unseal(stored.value()))
            .transpose()?;
        if let Some(current) = current {
            properties_msgpack =
                crate::algorithms::merge_batch_node_properties(&current, &properties_msgpack)
                    .map_err(|reason| {
                        format!("BatchUpdate op[{index}] cannot upsert node '{id}': {reason}")
                    })?;
        }
    }
    let sealed = crypto.seal(&properties_msgpack);
    nodes
        .insert((graph, id), sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// `BatchOperation::AddEdge`.  Both endpoints must exist at this point in the
/// batch; an upsert first drops the existing pair.
#[allow(clippy::too_many_arguments)]
fn apply_batch_add_edge_row(
    graph: &str,
    index: usize,
    source: &str,
    target: &str,
    properties_msgpack: &[u8],
    upsert: bool,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let source_exists = nodes
        .get((graph, source))
        .map_err(|error| error.to_string())?
        .is_some();
    let target_exists = nodes
        .get((graph, target))
        .map_err(|error| error.to_string())?
        .is_some();
    if !source_exists || !target_exists {
        return Err(format!(
            "BatchUpdate op[{index}] edge endpoints must exist at that point in the batch"
        ));
    }
    if upsert {
        remove_durable_edge_pair(graph, source, target, edges)?;
    }
    let ordinal = next_edge_ordinal(edges, graph, source, target)?;
    let sealed = crypto.seal(properties_msgpack);
    edges
        .insert((graph, source, target, ordinal), sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// `BatchOperation::AddEmbedding`.
///
/// CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007): `semantic_store` is
/// a scratch decode written back durably ONLY after the whole batch loop returns
/// `Ok` (`if semantic_dirty { write_semantic_store(...) }` in the caller), and
/// that caller's caller drops the enclosing `WriteTransaction` without
/// committing on any `Err` -- so, exactly like the "node does not exist" check
/// here, a rejected write never partially lands durably.
fn apply_batch_add_embedding_row(
    graph: &str,
    index: usize,
    id: String,
    embedding: Vec<f32>,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    semantic_store: &mut crate::compute::semantic::SemanticStore,
) -> Result<(), String> {
    if nodes
        .get((graph, id.as_str()))
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err(format!(
            "BatchUpdate op[{index}] embedding node '{id}' does not exist"
        ));
    }
    semantic_store
        .add_embedding(id, embedding)
        .map_err(|error| format!("BatchUpdate op[{index}] {error}"))?;
    Ok(())
}

/// Apply a decoded `BatchUpdate` op-list as row writes.
/// `BatchOperation::AddNode`.  An upsert merges over the durable pre-image
/// first; a plain add replaces the row outright.
fn apply_batch_rows(
    graph: &str,
    operations_msgpack: &[u8],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    use crate::algorithms::BatchOperation;

    // The compute crate owns the public schema. Decode it here too instead of
    // maintaining a second set of field aliases at the durability boundary.
    // Any error aborts the enclosing redb transaction: never acknowledge an
    // opaque or partially applied batch.
    let operations = crate::algorithms::decode_batch_operations(operations_msgpack)?;
    let has_semantic_operations = operations.iter().any(|operation| {
        matches!(
            operation,
            BatchOperation::RemoveNode { .. } | BatchOperation::AddEmbedding { .. }
        )
    });
    // Load the graph's vector store at most once. A large embedding batch must not
    // repeatedly deserialize and reserialize the whole semantic blob per element.
    let mut semantic_store = has_semantic_operations
        .then(|| read_semantic_store(semantic, graph, crypto))
        .transpose()?
        .flatten()
        .unwrap_or_default();
    let mut semantic_dirty = false;
    for (index, operation) in operations.into_iter().enumerate() {
        match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert,
            } => {
                apply_batch_add_node_row(
                    graph,
                    index,
                    id.as_str(),
                    properties_msgpack,
                    upsert,
                    nodes,
                    crypto,
                )?;
            }
            BatchOperation::RemoveNode { id } => {
                remove_durable_node_rows(graph, &id, nodes, edges)?;
                semantic_dirty |= semantic_store.remove_embedding(&id);
            }
            BatchOperation::AddEdge {
                source,
                target,
                properties_msgpack,
                upsert,
            } => {
                apply_batch_add_edge_row(
                    graph,
                    index,
                    source.as_str(),
                    target.as_str(),
                    &properties_msgpack,
                    upsert,
                    nodes,
                    edges,
                    crypto,
                )?;
            }
            BatchOperation::RemoveEdge { source, target } => {
                remove_durable_edge_pair(graph, &source, &target, edges)?;
            }
            BatchOperation::AddEmbedding { id, embedding } => {
                apply_batch_add_embedding_row(
                    graph,
                    index,
                    id,
                    embedding,
                    nodes,
                    &mut semantic_store,
                )?;
                semantic_dirty = true;
            }
        }
    }
    if semantic_dirty {
        write_semantic_store(semantic, graph, &semantic_store, crypto)?;
    }
    Ok(())
}

/// Drop every row for `graph` across nodes/edges/ledger (ClearGraph).
pub(crate) fn clear_graph_rows(
    graph: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
) -> Result<(), String> {
    let node_keys: Vec<String> = nodes
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, node_id) = key.value();
            if row_graph != graph {
                return Err("graph node row escaped its scope".to_string());
            }
            Ok(node_id.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in node_keys {
        let _ = nodes.remove((graph, id.as_str()));
    }
    let edge_keys: Vec<(String, String, u32)> = edges
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, source, target, ordinal) = key.value();
            if row_graph != graph {
                return Err("graph edge row escaped its scope".to_string());
            }
            Ok((source.to_string(), target.to_string(), ordinal))
        })
        .collect::<Result<_, String>>()?;
    for (s, t, o) in edge_keys {
        let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
    }
    let seqs: Vec<u64> = ledger
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, sequence) = key.value();
            if row_graph != graph {
                return Err("graph ledger row escaped its scope".to_string());
            }
            Ok(sequence)
        })
        .collect::<Result<_, String>>()?;
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    // EG-029: every edge for `graph` is gone — drop all cached ordinals so a later
    // AddEdge (incl. checkpoint re-population, which clears then re-adds) re-seeds from
    // the post-clear state. Covers ClearGraph, purge_graph_rows, and apply_checkpoint.
    invalidate_graph_edge_ords(graph);
    Ok(())
}

/// `Method::ClearLedger`'s durable table-row effect: remove every durable
/// `LEDGER` row for `graph`, leaving `nodes`/`edges`/resources untouched --
/// the row-scoped sibling of [`clear_graph_rows`]'s ledger-clearing loop
/// (factored out rather than shared, since `ClearGraph` legitimately clears
/// nodes/edges/resources TOO and this method must not).
pub(crate) fn clear_ledger_rows(
    graph: &str,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
) -> Result<(), String> {
    let seqs: Vec<u64> = ledger
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, sequence) = key.value();
            if row_graph != graph {
                return Err("graph ledger row escaped its scope".to_string());
            }
            Ok(sequence)
        })
        .collect::<Result<_, String>>()?;
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    Ok(())
}

// Terminal reservation rows are retained as exact lifecycle tombstones,
// but they no longer hold capacity.  A graph clear/delete may remove that
// terminal history atomically; a live Reserved row still requires an
// explicit release/reclaim drain so it cannot silently strand capacity.
fn resource_reservation_row_is_active(stored: &DurableResourceReservation) -> bool {
    stored.record.state == ResourceReservationRecordState::Reserved
        || stored.held_cpu_weight != 0
        || stored.held_memory_mib != 0
        || stored.held_disk_mib != 0
        || stored.held_process_slots != 0
}

fn check_resource_reservations_active(
    graph: &str,
    reservations: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let mut has_active_rows = false;
    let rows = reservations
        .scope_rows()
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, row_reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource reservation row escaped its scope".into());
        }
        let stored: DurableResourceReservation = resource_decode(value.value(), crypto)?;
        if row_reservation_id != stored.record.reservation_id {
            return Err("resource reservation key/index consistency check failed".into());
        }
        let index_value = tenant_index
            .get((
                graph,
                stored.record.tenant_ref.as_str(),
                stored.record.reservation_id.as_str(),
            ))
            .map_err(|error| error.to_string())?;
        if index_value.as_ref().map(|entry| entry.value())
            != Some(stored.record.reservation_id.as_str())
        {
            return Err("resource tenant index consistency check failed".into());
        }
        // A Reserved row, or any row that still carries held capacity, is
        // a live claim even if a corrupted or partially written value also
        // carries the tombstone bit.  Refuse the destructive lifecycle
        // operation for either representation; never infer that held
        // capacity is safe to drop from one flag.
        if resource_reservation_row_is_active(&stored) {
            has_active_rows = true;
        }
    }
    Ok(has_active_rows)
}

fn check_resource_tenant_index_consistency(
    graph: &str,
    tenant_index: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    reservations: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let rows = tenant_index
        .scope_rows()
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource tenant index row escaped its scope".into());
        }
        if value.value() != reservation_id {
            return Err("resource tenant index key/value consistency check failed".into());
        }
        let reservation = reservations
            .get((graph, reservation_id))
            .map_err(|error| error.to_string())?;
        let Some(reservation) = reservation else {
            return Err("resource tenant index references missing reservation".into());
        };
        let stored: DurableResourceReservation = resource_decode(reservation.value(), crypto)?;
        if stored.record.tenant_ref != tenant {
            return Err("resource tenant index tenant mismatch".into());
        }
    }
    Ok(())
}

fn collect_resource_two_part_clear_keys<V: redb::Value + 'static>(
    table: &ScopedOwnerTableMut<'_, (&str, &str), V>,
    graph: &str,
    cursor: &Option<String>,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, key_part) = key.value();
        if row_graph != graph {
            return Err("resource row escaped its scope".into());
        }
        if cursor
            .as_deref()
            .is_some_and(|cursor_key| key_part <= cursor_key)
        {
            continue;
        }
        keys.push(key_part.to_string());
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

/// Clear every `(graph, key)` row of a two-part-key table for `graph`, in
/// bounded `MAX_RESOURCE_CLEAR_SCAN`-sized passes so the caller's open
/// `WriteTransaction` never has to allocate an unbounded key list.
/// One bounded scan pass of `clear_resource_two_part_table`: collects at most
/// `MAX_RESOURCE_CLEAR_SCAN` second-key parts for `graph`, starting after
/// `cursor`.  Mirrors `collect_resource_attempts_clear_keys`.
fn clear_resource_two_part_table<V: redb::Value + 'static>(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str), V>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<String> = None;
    loop {
        let keys = collect_resource_two_part_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for key in &keys {
            table
                .remove((graph, key.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

fn collect_resource_attempts_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    graph: &str,
    cursor: &Option<(String, u64)>,
) -> Result<Vec<(String, u64)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, work_item, attempt) = key.value();
        if row_graph != graph {
            return Err("resource attempt row escaped its scope".into());
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_work_item, cursor_attempt)| {
                (work_item, attempt) <= (cursor_work_item.as_str(), *cursor_attempt)
            })
        {
            continue;
        }
        keys.push((work_item.to_string(), attempt));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

fn clear_resource_attempts_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<(String, u64)> = None;
    loop {
        let keys = collect_resource_attempts_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (work_item, attempt) in &keys {
            table
                .remove((graph, work_item.as_str(), *attempt))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

fn collect_resource_tenant_index_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource tenant-index row escaped its scope".into());
        }
        if value.value() != reservation_id {
            return Err("resource tenant index key/value escaped clear scope".into());
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_tenant, cursor_reservation)| {
                (tenant, reservation_id) <= (cursor_tenant.as_str(), cursor_reservation.as_str())
            })
        {
            continue;
        }
        keys.push((tenant.to_string(), reservation_id.to_string()));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

/// One bounded scan-and-remove pass over a three-part-key owner table,
/// generalized over the row value type `V`. `collect` supplies the
/// table-specific row validation and key extraction — the tenant-index and
/// anti-affinity tables enforce different invariants on their differently
/// shaped values (the tenant index redundantly stores the reservation id as
/// its value and checks it against the third key part; anti-affinity's value
/// is an unrelated `u64` weight), so that half stays table-specific. This
/// helper owns only the shared cursor-pagination and delete loop around it:
/// collect up to `MAX_RESOURCE_CLEAR_SCAN` keys past the resume cursor,
/// delete them, and resume from the last one, until a pass collects none.
/// Mirrors `clear_resource_two_part_table` for the two-part-key tables.
fn clear_resource_three_part_table<V: redb::Value + 'static>(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), V>,
    graph: &str,
    collect: fn(
        &ScopedOwnerTableMut<'_, (&str, &str, &str), V>,
        &str,
        &Option<(String, String)>,
    ) -> Result<Vec<(String, String)>, String>,
) -> Result<(), String> {
    let mut cursor: Option<(String, String)> = None;
    loop {
        let keys = collect(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (a, b) in &keys {
            table
                .remove((graph, a.as_str(), b.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

fn clear_resource_tenant_index_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    graph: &str,
) -> Result<(), String> {
    clear_resource_three_part_table(table, graph, collect_resource_tenant_index_clear_keys)
}

fn collect_resource_anti_affinity_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, host, tag) = key.value();
        if row_graph != graph {
            return Err("resource anti-affinity row escaped its scope".into());
        }
        if cursor.as_ref().is_some_and(|(cursor_host, cursor_tag)| {
            (host, tag) <= (cursor_host.as_str(), cursor_tag.as_str())
        }) {
            continue;
        }
        keys.push((host.to_string(), tag.to_string()));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

fn clear_resource_anti_affinity_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
) -> Result<(), String> {
    clear_resource_three_part_table(table, graph, collect_resource_anti_affinity_clear_keys)
}

fn clear_resource_reservation_side_tables(
    graph: &str,
    reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
) -> Result<(), String> {
    clear_resource_two_part_table(reservations, graph)?;
    clear_resource_tenant_index_table(tenant_index, graph)?;
    clear_resource_attempts_table(attempts, graph)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn clear_resource_host_side_tables(
    graph: &str,
    hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
) -> Result<(), String> {
    clear_resource_two_part_table(hosts, graph)?;
    clear_resource_two_part_table(exclusivity, graph)?;
    clear_resource_two_part_table(fairness, graph)?;
    clear_resource_two_part_table(concurrency, graph)?;
    clear_resource_anti_affinity_table(anti_affinity, graph)?;
    clear_resource_two_part_table(disk_policies, graph)?;
    Ok(())
}

/// Clear all native reservation indexes together with a graph image.  These
/// rows are not a cache: retaining one across DeleteGraph/recreate would leak
/// held capacity into the new incarnation.  The caller invokes this inside the
/// same WriteTransaction as the graph clear/purge, so no half-cleared resource
/// authority is observable.
///
/// The clear/delete operation is itself the governed administrative
/// continuation for terminal history: every range pass below handles at
/// most MAX_RESOURCE_CLEAR_SCAN keys, then resumes from the last key while
/// the same write transaction remains open.  This keeps allocation bounded
/// without imposing a lifetime bound on retained tombstones, so a graph
/// cannot become uncleareable merely because its terminal history is large.
/// The active-row validation remains a complete streaming pass and happens
/// before any removal; no active hold is silently deleted.
#[allow(clippy::too_many_arguments)]
pub(crate) fn clear_resource_rows(
    graph: &str,
    reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    attempts: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    exclusivity: &mut ScopedOwnerTableMut<'_, (&str, &str), &str>,
    fairness: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    concurrency: &mut ScopedOwnerTableMut<'_, (&str, &str), u64>,
    anti_affinity: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    disk_policies: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let has_active_rows =
        check_resource_reservations_active(graph, reservations, tenant_index, crypto)?;
    check_resource_tenant_index_consistency(graph, tenant_index, reservations, crypto)?;
    if has_active_rows {
        return Err("resource graph clear requires native reservation rows to be drained".into());
    }
    clear_resource_reservation_side_tables(graph, reservations, tenant_index, attempts)?;
    clear_resource_host_side_tables(
        graph,
        hosts,
        exclusivity,
        fairness,
        concurrency,
        anti_affinity,
        disk_policies,
    )?;
    Ok(())
}

/// Open the complete resource table family for a graph-member clear. The
/// compact and cross-modal paths already hold these tables and call
/// [`clear_resource_rows`] directly; the ordinary graph-method path only has
/// its core row bundle open, so this adapter opens the resource rows once and
/// releases them before the member is finished.
pub(crate) fn clear_resource_rows_in_wtx(
    write: &ShardWrite<'_>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut reservations = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_RESERVATIONS)?;
    let mut tenant_index = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)?;
    let mut attempts = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)?;
    let mut hosts = write.graph(graph)?.open_scoped_table(RESOURCE_HOSTS)?;
    let mut exclusivity = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_EXCLUSIVITY)?;
    let mut fairness = write.graph(graph)?.open_scoped_table(RESOURCE_FAIRNESS)?;
    let mut concurrency = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_CONCURRENCY)?;
    let mut anti_affinity = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_ANTI_AFFINITY)?;
    let mut disk_policies = write
        .graph(graph)?
        .open_scoped_table(RESOURCE_DISK_POLICIES)?;
    clear_resource_rows(
        graph,
        &mut reservations,
        &mut tenant_index,
        &mut attempts,
        &mut hosts,
        &mut exclusivity,
        &mut fairness,
        &mut concurrency,
        &mut anti_affinity,
        &mut disk_policies,
        crypto,
    )
}

/// Remove every current ChangeEnvelope projection for a graph inside the caller's
/// open transaction. The immutable MutationBatch/outbox audit ledger is retained;
/// current object/material/governance state cannot leak into a same-name graph.
pub(crate) fn clear_change_material_rows(
    write: &ShardWrite<'_>,
    graph: &str,
) -> Result<(), String> {
    let mut envelopes = write
        .graph(graph)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let envelope_keys: Vec<String> = envelopes
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            Ok(key.value().1.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in envelope_keys {
        envelopes
            .remove((graph, id.as_str()))
            .map_err(|e| e.to_string())?;
    }

    macro_rules! purge_graph_three_part_table {
        ($definition:expr) => {{
            let mut table = write
                .graph(graph)?
                .open_scoped_table($definition)
                .map_err(|e| e.to_string())?;
            let keys: Vec<(String, String)> = table
                .scope_rows()
                .map_err(|e| e.to_string())?
                .map(|row| {
                    let (key, _) = row.map_err(|e| e.to_string())?;
                    let (_, tenant, id) = key.value();
                    Ok((tenant.to_string(), id.to_string()))
                })
                .collect::<Result<_, String>>()?;
            for (tenant, id) in keys {
                table
                    .remove((graph, tenant.as_str(), id.as_str()))
                    .map_err(|e| e.to_string())?;
            }
        }};
    }
    purge_graph_three_part_table!(CONTENT_VERSIONS);
    purge_graph_three_part_table!(CHANGE_BLOBS);
    purge_graph_three_part_table!(CHANGE_FEATURES);
    purge_graph_three_part_table!(CHANGE_EVIDENCE);
    purge_graph_three_part_table!(CHANGE_POLICIES);
    purge_graph_three_part_table!(CHANGE_LINEAGE);

    let mut cursors = write
        .graph(graph)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    let cursor_keys: Vec<(String, String, String)> = cursors
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (_, tenant, source, partition) = key.value();
            Ok((
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
            ))
        })
        .collect::<Result<_, String>>()?;
    for (tenant, source, partition) in cursor_keys {
        cursors
            .remove((graph, tenant.as_str(), source.as_str(), partition.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Admitted-member variant used by online reshard imports.  The member's bound
/// graph identity supplies the scope; the graph argument is checked before any
/// table is opened, then every removal goes through the scoped owner handles.
pub(crate) fn clear_change_material_rows_in_wtx(
    write: &impl OwnerPayloadWrite,
    graph: &str,
) -> Result<(), String> {
    if write.scope().graph_name().map(|name| name.as_str()) != Some(graph) {
        return Err("change material clear graph does not match admitted scope".to_string());
    }
    let mut envelopes = write.open_scoped_table(CHANGE_ENVELOPES)?;
    let envelope_keys: Vec<String> = envelopes
        .scope_rows()?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            Ok(key.value().1.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in envelope_keys {
        envelopes.remove((graph, id.as_str()))?;
    }

    macro_rules! purge_graph_three_part_table {
        ($definition:expr) => {{
            let mut table = write.open_scoped_table($definition)?;
            let keys: Vec<(String, String)> = table
                .scope_rows()?
                .map(|row| {
                    let (key, _) = row.map_err(|e| e.to_string())?;
                    let (_, tenant, id) = key.value();
                    Ok((tenant.to_string(), id.to_string()))
                })
                .collect::<Result<_, String>>()?;
            for (tenant, id) in keys {
                table.remove((graph, tenant.as_str(), id.as_str()))?;
            }
        }};
    }
    purge_graph_three_part_table!(CONTENT_VERSIONS);
    purge_graph_three_part_table!(CHANGE_BLOBS);
    purge_graph_three_part_table!(CHANGE_FEATURES);
    purge_graph_three_part_table!(CHANGE_EVIDENCE);
    purge_graph_three_part_table!(CHANGE_POLICIES);
    purge_graph_three_part_table!(CHANGE_LINEAGE);

    let mut cursors = write.open_scoped_table(CHANGE_CURSORS)?;
    let cursor_keys: Vec<(String, String, String)> = cursors
        .scope_rows()?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (_, tenant, source, partition) = key.value();
            Ok((
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
            ))
        })
        .collect::<Result<_, String>>()?;
    for (tenant, source, partition) in cursor_keys {
        cursors.remove((graph, tenant.as_str(), source.as_str(), partition.as_str()))?;
    }
    Ok(())
}

/// Retire one graph entirely: its authority and every row it owns, in ONE
/// transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same, the tenant-DELETE path).
///
/// Unlike `clear_graph_rows`, which empties a LIVE graph's data and keeps its
/// identity, this ends the graph's durable existence: the scope's binding is
/// retired, so the generation can never be authenticated again, and its owner
/// rows go with it. A recreate of the same name binds a NEW incarnation and
/// starts clean.
///
/// This is the whole of what the retired `clear_mutation_authority_rows` used to
/// hand-sweep. Replay keys, receipts, outbox rows, delivery leases, projection
/// cursors, the version and the fence are ledger rows now, and the kernel
/// removes them with the binding; the payload half is
/// [`GraphShardRetirement`], which sweeps the 41 scope-prefixed tables through
/// the capability's own scope. The retired binding is also what replaces
/// `mutation_lifecycle_head`: a request carrying the old incarnation's scope is
/// refused at the kernel before any replay or admission question arises
/// (RF-RULING-004 application note 3).
///
/// The catalog row is the exception and is removed separately: `graph_meta` is
/// FILE-WIDE, so it belongs to the control scope, not to the graph being
/// retired.
pub(crate) fn purge_graph_rows(shard: &Shard, graph: &str) -> Result<(), String> {
    reject_reserved_graph(graph)?;
    let handle = shard.graph(graph)?;
    let identity = handle.identity().clone();
    shard
        .mutations()
        .purge_scope_with(&handle, &identity, &GraphShardRetirement)?;
    shard.forget_graph(graph, &identity)?;
    remove_graph_catalog_row(shard, graph)
}

/// Drop one graph's catalog entry on the control scope.
fn remove_graph_catalog_row(shard: &Shard, graph: &str) -> Result<(), String> {
    let op_id = format!("graph_purge/{graph}");
    let (group, batches) = shard.admit_maintenance(&[], &op_id)?;
    let write = ShardWrite::open(shard, &group, &[], &batches)?;
    let removed = write
        .control()
        .open_table(GRAPH_META)?
        .remove(graph)
        .map(|_| ())
        .map_err(|error| error.to_string());
    let finished = write.finish();
    match (removed, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, 0),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

// ── Cross-shard 2PC durable rows (CONCEPT:EG-KG.storage.lane-n-increment) — pure, server-INDEPENDENT ──
// Shared store helpers (mirroring NODES/EDGES/purge_graph_rows): the `Cmd` arms in
// `redb_backend`'s off-reactor writer thread call straight into these.

#[cfg(all(test, feature = "security"))]
mod security_tests {
    //! Encryption-at-rest + tamper-evident audit proofs over the durable store
    //! (CONCEPT:EG-KG.sharding.row-level-security), exercised through the SAME `commit_ops`/read/`verify_audit`
    //! the server + embedded engine use.
    use super::*;
    use crate::crypto::ValueCipher;

    fn open_db(dir: &std::path::Path) -> Shard {
        let path = dir.join("graph-0.redb");
        Shard::open(&path).unwrap()
    }

    fn add_node_method(node_id: &str, props: serde_json::Value) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&props).unwrap(),
        }
    }

    fn add_edge_method(src: &str, tgt: &str) -> Method {
        Method::AddEdge {
            source_id: src.to_string(),
            target_id: tgt.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        }
    }

    /// Read back the stored ordinals for one (graph,src,tgt) in ascending order.
    fn edge_ords(shard: &Shard, graph: &str, src: &str, tgt: &str) -> Vec<u32> {
        let handle = shard.graph(graph).unwrap();
        let read = shard.read(&handle).unwrap();
        let edges = read.scoped_owner_table(EDGES).unwrap();
        edges
            .scope_rows()
            .unwrap()
            .map(|row| row.expect("read durable edge row"))
            .filter(|(k, _)| {
                let (g, s, t, _) = k.value();
                g == graph && s == src && t == tgt
            })
            .map(|(k, _)| k.value().3)
            .collect()
    }

    fn next_drain_id(tag: &str) -> String {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "{tag}-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    fn tamper_audit_row(shard: &Shard, graph: &str, sequence: u64) {
        let members = shard.graph_members(&[graph]).unwrap();
        let op_id = next_drain_id("tamper-audit");
        let (group, batches) = shard.admit_maintenance(&members, &op_id).unwrap();
        let write = ShardWrite::open(shard, &group, &members, &batches).unwrap();
        {
            let mut audit = write
                .graph(graph)
                .unwrap()
                .open_scoped_table(AUDIT)
                .unwrap();
            let original = audit
                .get((graph, sequence))
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            let mut mutated = original;
            let last = mutated.len() - 1;
            mutated[last] ^= 0xFF;
            audit.insert((graph, sequence), mutated.as_slice()).unwrap();
        }
        write.finish().unwrap();
        shard.commit_drain(group, &batches, 0).unwrap();
    }

    #[test]
    fn hierarchical_edge_ordinal_cache_invalidates_only_the_requested_scope() {
        EDGE_ORD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.clear();
            cache
                .entry("g1".into())
                .or_default()
                .entry("a".into())
                .or_default()
                .extend([("b".into(), 2), ("c".into(), 3)]);
            cache
                .entry("g1".into())
                .or_default()
                .entry("x".into())
                .or_default()
                .insert("y".into(), 4);
            cache
                .entry("g2".into())
                .or_default()
                .entry("a".into())
                .or_default()
                .insert("b".into(), 5);
        });

        invalidate_edge_ord("g1", "a", "b");
        EDGE_ORD_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert!(!cache["g1"]["a"].contains_key("b"));
            assert_eq!(cache["g1"]["a"]["c"], 3);
            assert_eq!(cache["g2"]["a"]["b"], 5);
        });

        invalidate_node_edge_ords("g1", "a");
        EDGE_ORD_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert!(!cache["g1"].contains_key("a"));
            assert_eq!(cache["g1"]["x"]["y"], 4);
        });

        invalidate_graph_edge_ords("g1");
        EDGE_ORD_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            assert!(!cache.contains_key("g1"));
            assert_eq!(cache["g2"]["a"]["b"], 5);
            cache.clear();
        });
    }

    #[test]
    fn edge_ordinal_cache_assigns_u32_max_once_then_fails_closed() {
        let dir = tempdir();
        std::thread::Builder::new()
            .name("eg-redb-writer-exhaustion-test".to_string())
            .spawn(move || {
                let db = open_db(&dir);
                let members = db.graph_members(&["g"]).unwrap();
                let op_id = next_drain_id("edge-space");
                let (group, batches) = db.admit_maintenance(&members, &op_id).unwrap();
                let write = ShardWrite::open(&db, &group, &members, &batches).unwrap();
                let edges = write.graph("g").unwrap().open_scoped_table(EDGES).unwrap();
                EDGE_ORD_CACHE.with(|cache| {
                    cache
                        .borrow_mut()
                        .entry("g".into())
                        .or_default()
                        .entry("a".into())
                        .or_default()
                        .insert("b".into(), u64::from(u32::MAX));
                });

                assert_eq!(next_edge_ordinal(&edges, "g", "a", "b").unwrap(), u32::MAX);
                assert_eq!(
                    next_edge_ordinal(&edges, "g", "a", "b").unwrap_err(),
                    "edge ordinal space exhausted"
                );
                drop(edges);
                write.finish().unwrap();
                db.commit_drain(group, &batches, 0).unwrap();
                EDGE_ORD_CACHE.with(|cache| cache.borrow_mut().clear());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// CONCEPT:EG-KG.storage.redb-store #3 — the O(1) edge-ordinal counter assigns CORRECT, strictly
    /// monotonic ordinals across many `AddEdge` to one node (per-op across SEPARATE commit
    /// batches on the dedicated writer thread — the hot path that used to range-scan every
    /// time), and a FRESH writer thread (the restart case) RE-SEEDS each (src,tgt) from one
    /// bounded tail seek and continues with no gap, reset, or collision. `RemoveEdge` invalidates the
    /// counter so a re-add resets to 0, matching the old scan behavior exactly.
    #[test]
    fn edge_ordinals_monotonic_o1_counter_and_reseed_after_restart() {
        let dir = tempdir();

        // PHASE 1 — on a dedicated `eg-redb-writer*` thread so the EG-029 counter is active.
        let d1 = dir.clone();
        std::thread::Builder::new()
            .name("eg-redb-writer-egtest".to_string())
            .spawn(move || {
                let crypto = DurableCrypto::none();
                let db = open_db(&d1);
                let mut tail = AuditTailCache::new();
                let mut commit = |m: Method| {
                    let mut ops = vec![("g".to_string(), m)];
                    let mut log = Vec::new();
                    commit_ops(
                        &db,
                        &mut ops,
                        &mut log,
                        &next_drain_id("security-edge"),
                        0,
                        crypto,
                        &mut tail,
                    )
                    .unwrap();
                };
                // AddEdge now requires both endpoints to already be durable nodes
                // (redb_store.rs's "AddEdge requires durable endpoints" guard) --
                // seed the three nodes this test's edges reference before adding
                // any edge between them.
                commit(add_node_method("a", serde_json::json!({})));
                commit(add_node_method("b", serde_json::json!({})));
                commit(add_node_method("c", serde_json::json!({})));

                // 6 multi-edges a->b across SEPARATE batches (cross-batch in-RAM counter),
                // interleaved with 2 a->c.
                for _ in 0..6 {
                    commit(add_edge_method("a", "b"));
                }
                commit(add_edge_method("a", "c"));
                commit(add_edge_method("a", "c"));
                assert_eq!(edge_ords(&db, "g", "a", "b"), vec![0, 1, 2, 3, 4, 5]);
                assert_eq!(edge_ords(&db, "g", "a", "c"), vec![0, 1]);

                // RemoveEdge invalidates the counter → re-add resets to 0 (old behavior).
                commit(Method::RemoveEdge {
                    source_id: "a".into(),
                    target_id: "c".into(),
                });
                assert_eq!(edge_ords(&db, "g", "a", "c"), Vec::<u32>::new());
                commit(add_edge_method("a", "c"));
                assert_eq!(edge_ords(&db, "g", "a", "c"), vec![0]);
            })
            .unwrap()
            .join()
            .unwrap();

        // PHASE 2 — RESTART: reopen the SAME file on a NEW writer thread (fresh thread-local
        // counter). Adding 3 more a->b must RE-SEED from one scan (max was 5) and continue
        // 6,7,8 — monotonic, no reset, no collision.
        let d2 = dir.clone();
        std::thread::Builder::new()
            .name("eg-redb-writer-egtest".to_string())
            .spawn(move || {
                let crypto = DurableCrypto::none();
                let db = open_db(&d2);
                let mut tail = AuditTailCache::new();
                for _ in 0..3 {
                    let mut ops = vec![("g".to_string(), add_edge_method("a", "b"))];
                    let mut log = Vec::new();
                    commit_ops(
                        &db,
                        &mut ops,
                        &mut log,
                        &next_drain_id("security-edge-restart"),
                        0,
                        crypto,
                        &mut tail,
                    )
                    .unwrap();
                }
                assert_eq!(
                    edge_ords(&db, "g", "a", "b"),
                    vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
                    "re-seeded counter must continue monotonically after restart"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// CONCEPT:EG-KG.storage.redb-store #4 — with encryption OFF, `seal` returns `Cow::Borrowed` and the
    /// stored value blob is BYTE-FOR-BYTE the caller's plaintext (zero clone, no format
    /// change). Proven by reading the stored bytes back and comparing to the input.
    #[test]
    fn seal_off_stores_plaintext_bytes_byte_identical() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);
        let pbytes = rmp_serde::to_vec_named(&serde_json::json!({"k": "v-plain-123"})).unwrap();
        let mut ops = vec![(
            "g".to_string(),
            Method::AddNode {
                node_id: "n".to_string(),
                properties_msgpack: pbytes.clone(),
            },
        )];
        let mut log = Vec::new();
        let mut tail = AuditTailCache::new();
        commit_ops(
            &db,
            &mut ops,
            &mut log,
            &next_drain_id("security-plaintext"),
            0,
            crypto,
            &mut tail,
        )
        .unwrap();

        let handle = db.graph("g").unwrap();
        let read = db.read(&handle).unwrap();
        let nodes = read.scoped_owner_table(NODES).unwrap();
        let stored = nodes.get(("g", "n")).unwrap().unwrap().value().to_vec();
        assert_eq!(
            stored, pbytes,
            "encryption-off stored bytes must equal the input plaintext (seal = identity)"
        );
    }

    #[test]
    fn encryption_no_plaintext_on_disk_round_trips_and_wrong_key_fails() {
        let dir = tempdir();
        let db_path = dir.join("graph-0.redb");
        let cipher = ValueCipher::from_key_material(b"correct-horse-battery-staple");
        let crypto = DurableCrypto::new(Some(&cipher));

        // Write a node carrying a recognizable SECRET via the durable write path.
        {
            let db = open_db(&dir);
            let mut ops = vec![(
                "g".to_string(),
                add_node_method("n1", serde_json::json!({"ssn": "SECRET-123-45-6789"})),
            )];
            let mut log = Vec::new();
            let mut audit_tail = AuditTailCache::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-encrypted"),
                0,
                crypto,
                &mut audit_tail,
            )
            .unwrap();
        }

        // The raw on-disk redb bytes must NOT contain the plaintext secret.
        let raw = std::fs::read(&db_path).unwrap();
        let needle = b"SECRET-123-45-6789";
        assert!(
            !raw.windows(needle.len()).any(|w| w == needle),
            "plaintext node property leaked into raw redb file"
        );

        // It round-trips with the right key.
        {
            let db = open_db(&dir);
            let dumps = read_all_dumps(&db, crypto).unwrap();
            let g = dumps.iter().find(|d| d.graph == "g").expect("graph g");
            let (_, props) = &g.nodes[0];
            let m: serde_json::Value = rmp_serde::from_slice(props).unwrap();
            assert_eq!(m["ssn"], "SECRET-123-45-6789");
        }

        // A WRONG key fails to decrypt (never silent plaintext).
        {
            let db = open_db(&dir);
            let wrong = ValueCipher::from_key_material(b"totally-different-key");
            let res = read_all_dumps(&db, DurableCrypto::new(Some(&wrong)));
            assert!(res.is_err(), "wrong key must not decrypt");
        }
    }

    #[test]
    fn audit_chain_verifies_clean_and_detects_tampering() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);

        // Three durable mutations → three chained audit entries.
        let mut audit_tail = AuditTailCache::new();
        for (i, m) in [
            add_node_method("a", serde_json::json!({"v": 1})),
            add_node_method("b", serde_json::json!({"v": 2})),
            Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let _ = i;
            let mut ops = vec![("g".to_string(), m)];
            let mut log = Vec::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-audit"),
                0,
                crypto,
                &mut audit_tail,
            )
            .unwrap();
        }

        // A clean chain verifies.
        let report = verify_audit(&db, "g").unwrap();
        assert!(report.ok, "{report:?}");
        assert_eq!(report.entries, 3);

        // Tamper entry seq=1: flip its stored line/hash bytes directly in the table.
        tamper_audit_row(&db, "g", 1);

        let broken = verify_audit(&db, "g").unwrap();
        assert!(!broken.ok, "tamper undetected");
        assert_eq!(broken.first_broken_seq, Some(1), "wrong break position");
    }

    /// CONCEPT:EG-KG.storage.embedded-store — the O(1) tail-cache append produces an IDENTICAL, verifiable
    /// chain to the old per-op scan across: (1) many ops in ONE commit batch
    /// (intra-batch chaining off RAM), (2) several commit batches reusing the cache
    /// (inter-batch), and (3) a fresh cache that must RE-SEED the tail from one scan
    /// (the restart case) and continue the chain without a gap. Two interleaved graphs
    /// prove per-graph isolation of the cache.
    #[test]
    fn audit_tail_cache_o1_append_builds_verifiable_chain_across_batches_and_restart() {
        let dir = tempdir();
        let crypto = DurableCrypto::none();
        let db = open_db(&dir);

        // Helper: commit a batch of (graph, node) AddNode ops through commit_ops with a
        // caller-owned cache (mirrors the writer thread's persistent cache).
        let commit_batch = |shard: &Shard, cache: &mut AuditTailCache, batch: &[(&str, &str)]| {
            let mut ops: Vec<(String, Method)> = batch
                .iter()
                .map(|(g, n)| {
                    (
                        g.to_string(),
                        add_node_method(n, serde_json::json!({"n": n})),
                    )
                })
                .collect();
            let mut log = Vec::new();
            commit_ops(
                shard,
                &mut ops,
                &mut log,
                &next_drain_id("security-cache"),
                0,
                crypto,
                cache,
            )
            .unwrap();
        };

        // Batch 1: 5 ops for "g1" + 3 ops for "g2" in ONE commit (intra-batch chaining,
        // interleaved graphs). The cache seeds each graph once (genesis) then chains in RAM.
        let mut cache = AuditTailCache::new();
        commit_batch(
            &db,
            &mut cache,
            &[
                ("g1", "a"),
                ("g2", "x"),
                ("g1", "b"),
                ("g1", "c"),
                ("g2", "y"),
                ("g1", "d"),
                ("g2", "z"),
                ("g1", "e"),
            ],
        );
        // Cache must reflect the in-RAM tails: g1 saw 5 ops (seq 0..4), g2 saw 3 (seq 0..2).
        assert_eq!(cache.get("g1").unwrap().0, 4, "g1 tail seq");
        assert_eq!(cache.get("g2").unwrap().0, 2, "g2 tail seq");

        // Batch 2: REUSE the same cache (inter-batch). No scan should be needed; the
        // chain must continue seamlessly.
        commit_batch(&db, &mut cache, &[("g1", "f"), ("g2", "w"), ("g1", "g")]);

        // Batch 3: simulate a WRITER RESTART — a brand-new empty cache. The first touch
        // of each graph must RE-SEED the tail from one range-scan and continue with NO gap.
        let mut cache_after_restart = AuditTailCache::new();
        commit_batch(&db, &mut cache_after_restart, &[("g1", "h"), ("g2", "v")]);
        assert_eq!(
            cache_after_restart.get("g1").unwrap().0,
            7,
            "g1 re-seeded tail continues (5+2 prior ⇒ next seq 7)"
        );
        assert_eq!(
            cache_after_restart.get("g2").unwrap().0,
            4,
            "g2 re-seeded tail continues (3+1+1 prior ⇒ seq 4)"
        );

        // The FULL chains must verify clean (tamper-evidence intact, no gaps/breaks).
        let r1 = verify_audit(&db, "g1").unwrap();
        assert!(r1.ok, "g1 chain broken: {r1:?}");
        assert_eq!(r1.entries, 8, "g1 entry count (5+2+1)");
        let r2 = verify_audit(&db, "g2").unwrap();
        assert!(r2.ok, "g2 chain broken: {r2:?}");
        assert_eq!(r2.entries, 5, "g2 entry count (3+1+1)");

        // And tamper-evidence still fires on the cache-built chain.
        tamper_audit_row(&db, "g1", 3);
        let broken = verify_audit(&db, "g1").unwrap();
        assert!(!broken.ok, "tamper on cache-built chain undetected");
        assert_eq!(broken.first_broken_seq, Some(3));
    }

    /// CONCEPT:EG-KG.storage.embedded-store — the cold-seed tail lookup is a BOUNDED reverse seek
    /// (`(graph, 0)..=(graph, u64::MAX)` + `next_back`), not a forward walk to the end
    /// of the chain. Proven by comparing the wall-clock cost of re-seeding (fresh
    /// cache, i.e. after a simulated writer restart) a chain of 200,000 prior entries
    /// against re-seeding a chain of 5: the chains differ 40,000x in length, but a
    /// single bounded reverse seek's cost is independent of that (only the B-tree's
    /// O(log n) depth differs — a small constant next to the `*50 + 20ms` slack
    /// below). The old `.range((graph, 0u64)..)` + `.last()` forward walk (O(chain
    /// length)) would blow well past this bound on the long chain.
    #[test]
    fn audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan() {
        let crypto = DurableCrypto::none();

        // Build a chain of `len` AddNode entries for `graph` in ONE commit batch (the
        // cache stays warm for the whole build, so construction cost is irrelevant —
        // only the POST-RESTART re-seed below is timed), then re-seed from a FRESH
        // cache (simulating a writer restart) and time just that one call.
        let reseed_cost = |graph: &str, len: usize| -> u64 {
            let dir = tempdir();
            let db = open_db(&dir);
            let mut ops: Vec<(String, Method)> = (0..len)
                .map(|i| {
                    (
                        graph.to_string(),
                        add_node_method(&format!("n{i}"), serde_json::json!({"i": i})),
                    )
                })
                .collect();
            let mut log = Vec::new();
            let mut warm_cache = AuditTailCache::new();
            commit_ops(
                &db,
                &mut ops,
                &mut log,
                &next_drain_id("security-cold-build"),
                0,
                crypto,
                &mut warm_cache,
            )
            .unwrap();

            // Simulated restart: a brand-new empty cache forces the NEXT append to
            // cold-seed the tail from durable state instead of chaining off RAM.
            let mut cold_cache = AuditTailCache::new();
            let mut restart_ops = vec![(
                graph.to_string(),
                add_node_method(&format!("n{len}"), serde_json::json!({"i": len})),
            )];
            let mut restart_log = Vec::new();
            let _ = super::audit::cold_seed_rows_touched_take(); // discard anything the build left
            commit_ops(
                &db,
                &mut restart_ops,
                &mut restart_log,
                &next_drain_id("security-cold-restart"),
                0,
                crypto,
                &mut cold_cache,
            )
            .unwrap();
            let rows_touched = super::audit::cold_seed_rows_touched_take();

            // Correctness: the re-seeded tail must continue the chain with no gap, and
            // the full (len + 1)-entry chain must still verify clean.
            assert_eq!(
                cold_cache.get(graph).unwrap().0,
                len as u64,
                "re-seeded tail must continue at seq {len}"
            );
            let report = verify_audit(&db, graph).unwrap();
            assert!(report.ok, "{report:?}");
            assert_eq!(report.entries, (len + 1) as u64);

            rows_touched
        };

        let short = reseed_cost("g-short", 5);
        let long = reseed_cost("g-long", 200_000);

        // The property under test is STRUCTURAL — "a bounded seek, not a forward
        // scan" — so assert it structurally: the number of audit rows the cold seed
        // pulls must not depend on how long the chain is. A bounded reverse seek
        // pulls exactly one row from a 5-entry chain and exactly one from a
        // 200,000-entry chain; a forward walk pulls 5 and 200,000.
        //
        // This replaces a wall-clock ratio (`long <= short*50 + 20ms`). That budget
        // was BOTH flaky and weak: it failed reproducibly on a shared build host
        // purely from a concurrent job's disk contention (measured 5/5 at 1.6-2.0s
        // against a 0.5-1.1s budget with the mechanism provably unchanged), and it
        // would equally have PASSED a genuine forward-scan regression on a fast
        // enough machine. Counting the rows is deterministic, machine-independent,
        // and strictly stronger.
        assert_eq!(
            short, 1,
            "cold seed of a 5-entry chain should pull exactly one audit row"
        );
        assert_eq!(
            long, short,
            "cold-seed cost must be INDEPENDENT of chain length: a 200,000-entry \
             chain pulled {long} audit row(s) vs {short} for a 5-entry chain — the \
             O(1) bounded reverse seek has regressed to an O(chain length) forward scan"
        );
    }

    /// A throwaway temp dir under the scratch space.
    fn tempdir() -> std::path::PathBuf {
        let base = crate::test_support::temp_dir("eg-sec", "test");
        std::fs::create_dir_all(&base).unwrap();
        base
    }
}

/// Build a unique process-local `.redb` fixture path for a kernel-owned-store
/// test: `<prefix>-<tag>-<pid>-<nanos>.redb` under the OS temp directory.
/// Unlike [`test_support::temp_dir`], this hands back a *file* path meant to
/// go straight to `redb::Database::create`/`Shard::open`, not a directory the
/// fixture owns end-to-end — so there is no matching stale-path cleanup here;
/// each caller is responsible for its own fixture's lifecycle, exactly as it
/// was before this helper had a single home. The four kernel-owned-store test
/// modules that build these paths (mutation-batch replay here, the keyset-page
/// dump tests, the shard bootstrap/graft tests, and the online-reshard tests)
/// used to hand-roll this same construction independently, varying only the
/// prefix; they now call through this one definition instead.
#[cfg(test)]
pub(crate) fn temp_path(prefix: &str, tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{tag}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[cfg(test)]
mod mutation_batch_tests {
    use super::*;
    use crate::change_envelope::{
        ChangeCursor, ChangeEnvelope, ContentVersion, ContentVersionPosition, CursorPosition,
        PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
    };
    use crate::mutation_batch::{
        DurabilityDomain, IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent,
        MutationScopeIdentity, MutationSurface, ScopeTenantId, MUTATION_BATCH_VERSION,
    };
    use eg_transaction::OutboxClaimBudget;
    use eg_types::outcome_bundle::{
        CommitOutcomeBundle, OutcomeCompleteness, ReceiptNode, ReceiptNodeKind, RunEvent,
        TerminalOutcomeExtension, OUTCOME_BUNDLE_VERSION, RUN_EVENT_OUTBOX_TOPIC,
    };
    use sha2::{Digest, Sha256};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        super::temp_path("eg-mutation-batch", tag)
    }

    fn open(path: &std::path::Path) -> Shard {
        let shard = Shard::open(path).unwrap();
        // The old fixture seeded the retired graph-version table at 3.  Advance
        // the kernel-owned ledger through three real maintenance admissions so
        // every test keeps the same OCC starting point without recreating a
        // second version authority.
        let members = shard.graph_members(&["graph-a"]).unwrap();
        // Reopening an existing fixture must not re-admit the same maintenance
        // seed keys: the kernel correctly resolves those members as Replay, and
        // replayed members are forbidden from opening owner rows. Seed only a
        // graph whose authoritative ledger version row is absent.
        // Freshness cannot be probed by "is the version row absent?", because this
        // read is what CREATES that row: `read_mutation_graph_version` opens the
        // graph, and `Shard::graph` binds a cold scope (`bind_scope`) which inserts
        // `INITIAL_GRAPH_VERSION`. The absent-row arm below is therefore
        // unreachable through this path, and reading it as "already seeded" left
        // every fixture at version 0 while 47 of them expect the seeded base of 3
        // -- 42 tests failing closed with `STALE_VERSION: expected version 3 but
        // authoritative version is 0`.
        //
        // An unadvanced ledger is the real freshness signal: only seeding moves
        // graph-a off `INITIAL_GRAPH_VERSION`, so a reopened fixture is at 3 or
        // more and is still correctly left alone.
        let seed_required = match read_mutation_graph_version(&shard, "graph-a") {
            Ok(version) => version == INITIAL_GRAPH_VERSION,
            Err(error) if error == "mutation scope binding is missing its version row" => true,
            Err(error) => panic!("unexpected graph version read failure: {error}"),
        };
        if seed_required {
            for index in 0..3 {
                let op_id = format!("mutation-test-seed/{index}");
                let (group, batches) = shard.admit_maintenance(&members, &op_id).unwrap();
                let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
                write.finish().unwrap();
                shard.commit_drain(group, &batches, 0).unwrap();
            }
        }
        shard
    }

    /// Reopen an EXISTING fixture database without seeding anything.
    ///
    /// [`open`] deliberately seeds `MUTATION_GRAPH_VERSION["graph-a"] = 3` when
    /// that row is absent, which 47 fixtures depend on. That makes it the wrong
    /// tool for asserting a row was durably DELETED: it re-inserts the very row
    /// under test between the deletion and the assertion, so such a test can only
    /// ever fail — it reports as coverage of durability while being incapable of
    /// observing it.
    fn reopen(path: &std::path::Path) -> Shard {
        Shard::open(path).unwrap()
    }

    fn node(id: &str, value: i64) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": value}))
                .unwrap(),
        }
    }

    fn batch(batch_id: &str, key: &str) -> MutationBatch {
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope: super::fixture_operation_envelope(
                &identity,
                &format!("principal:sha256:{}", "a".repeat(64)),
                42,
                &key.to_string(),
            ),
            identity,
            placement_epoch: 7,
            version_expectation: VersionExpectation::Graph(3),
            fencing_token: Some(9),
            authoritative_state: None,
            operations: vec![
                MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: node("a", 1),
                },
                MutationOperation {
                    ordinal: 1,
                    surface: MutationSurface::Transaction,
                    domain: DurabilityDomain::GraphRows,
                    method: node("b", 2),
                },
            ],
            outbox: vec![MutationOutboxIntent {
                topic: "projection.test".to_string(),
                key: batch_id.to_string(),
                payload: vec![1, 2, 3],
                headers: Default::default(),
            }],
            created_at_ms: 100,
        };
        batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        batch
    }

    #[test]
    fn graph_record_decoder_validates_the_complete_receipt() {
        let batch = batch("decode-record", "decode-record-key");
        let mut record = MutationBatchRecord {
            identity: batch.identity.clone(),
            batch,
            status: MutationBatchStatus::Committed,
            committed_version: CommittedVersion::Graph {
                source: 3,
                target: 4,
            },
            result_msgpack: None,
            committed_at_ms: 101,
        };
        let encoded = rmp_serde::to_vec_named(&record).unwrap();
        decode_mutation_batch_record(&encoded, "graph-a", "decode-record").unwrap();
        assert!(decode_mutation_batch_record(&encoded, "graph-b", "decode-record").is_err());
        assert!(decode_mutation_batch_record(&encoded, "graph-a", "moved-record").is_err());

        for status in [MutationBatchStatus::Prepared, MutationBatchStatus::Aborted] {
            record.status = status;
            record.committed_version = CommittedVersion::None;
            record.validate().unwrap();
            let non_terminal = rmp_serde::to_vec_named(&record).unwrap();
            assert!(
                decode_mutation_batch_record(&non_terminal, "graph-a", "decode-record").is_err()
            );
        }

        let native_identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            DurabilityDomain::SqlCatalog,
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        )
        .unwrap();
        record.batch.identity = native_identity.clone();
        record.identity = native_identity;
        record.batch.version_expectation = VersionExpectation::Native(3);
        for operation in &mut record.batch.operations {
            operation.domain = DurabilityDomain::SqlCatalog;
        }
        record.batch.envelope = super::fixture_operation_envelope(
            &record.batch.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            42,
            "decode-record-key",
        );
        record
            .batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("native receipt fixture reseals its final body");
        record.status = MutationBatchStatus::Committed;
        record.committed_version = CommittedVersion::Native {
            source: 3,
            target: 4,
        };
        record.validate().unwrap();
        let wrong_store = rmp_serde::to_vec_named(&record).unwrap();
        assert!(decode_mutation_batch_record(&wrong_store, "graph-a", "decode-record").is_err());
    }

    fn ready_work_item_method(work_item_id: &str, max_attempts: u64) -> Method {
        Method::AddNode {
            node_id: work_item_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "WorkItem",
                "tenant": "tenant-a",
                "status": "ready",
                "max_attempts": max_attempts,
            }))
            .unwrap(),
        }
    }

    fn delegated_work_item_method(work_item_id: &str, max_attempts: u64) -> Method {
        delegated_work_item_method_with_status(work_item_id, max_attempts, "ready")
    }

    fn delegated_work_item_method_with_status(
        work_item_id: &str,
        max_attempts: u64,
        status: &str,
    ) -> Method {
        Method::AddNode {
            node_id: work_item_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "WorkItem",
                "tenant": "tenant-a",
                "status": status,
                "state": "ready",
                "kind": "agent.execute",
                "queue": "agent.execute",
                "prio_bucket": 0,
                "created_at": 1.0,
                "next_retry_at": 0.0,
                "resource_class": "",
                "fairness_group": "",
                "lease_owner": null,
                "last_lease_owner": null,
                "lease_epoch": 0,
                "fencing_token": 0,
                "lease_expires_at": null,
                "work_item_fence": "",
                "attempt": 0,
                "max_attempts": max_attempts,
                "backoff_base_s": 1.0,
                "downstream_ids": [],
                "dep_count": 0,
                "metadata": {
                    "delegation_id": "delegation:terminal",
                    "run_id": "run:terminal",
                    "agent_id": "agent:selected-b",
                    "capability_digest": digest_for('b')
                },
                "context": {"agent_id": "agent:delegator-a"},
                "catalog_digest": digest_for('c'),
                "policy_digest": digest_for('d'),
                "model_digest": digest_for('e')
            }))
            .unwrap(),
        }
    }

    fn digest_for(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn terminal_receipt_node(
        bundle: &CommitOutcomeBundle,
        kind: ReceiptNodeKind,
        node_id: &str,
    ) -> ReceiptNode {
        let kind_name = match kind {
            ReceiptNodeKind::RunTrace => "run_trace",
            ReceiptNodeKind::ToolCall => "tool_call",
            ReceiptNodeKind::OutcomeEvaluation => "outcome_evaluation",
        };
        let payload_ref = format!("cas:receipt:{node_id}");
        let properties = serde_json::json!({
            "node_id": node_id,
            "kind": kind_name,
            "delegation_id": bundle.delegation_id,
            "delegator_id": bundle.delegator_id,
            "selected_agent_id": bundle.selected_agent_id,
            "executor_lease_actor": bundle.executor_lease_actor,
            "outcome": bundle.outcome,
            "work_item_id": bundle.work_item_id,
            "run_id": bundle.run_id,
            "fence_token": bundle.fence_token,
            "result_ref": bundle.result_ref,
            "result_digest": bundle.result_digest,
            "event_sequence": bundle.event_sequence,
            "completeness": bundle.completeness,
            "missing_refs": bundle.missing_refs,
            "outbox_id": bundle.outbox_id,
            "payload_ref": payload_ref,
            "capability_digest": bundle.capability_digest,
            "catalog_digest": bundle.catalog_digest,
            "policy_digest": bundle.policy_digest,
            "model_digest": bundle.model_digest,
            "payload": {"fixture": true}
        });
        let properties_msgpack = rmp_serde::to_vec_named(&properties).unwrap();
        ReceiptNode {
            node_id: node_id.to_string(),
            kind,
            delegation_id: bundle.delegation_id.clone(),
            work_item_id: bundle.work_item_id.clone(),
            run_id: bundle.run_id.clone(),
            fence_token: bundle.fence_token,
            result_ref: bundle.result_ref.clone(),
            outbox_id: bundle.outbox_id.clone(),
            payload_ref,
            payload_digest: hex::encode(Sha256::digest(&properties_msgpack)),
            properties_msgpack,
        }
    }

    fn terminal_extension(
        batch_id: &str,
        work_item_id: &str,
        fencing_token: u64,
        outcome: &str,
        worker_id: &str,
    ) -> TerminalOutcomeExtension {
        let bundle = CommitOutcomeBundle {
            schema_version: OUTCOME_BUNDLE_VERSION,
            delegation_id: "delegation:terminal".into(),
            delegator_id: "agent:delegator-a".into(),
            selected_agent_id: "agent:selected-b".into(),
            executor_lease_actor: worker_id.into(),
            outcome: outcome.into(),
            work_item_id: work_item_id.into(),
            fence_token: fencing_token,
            run_id: "run:terminal".into(),
            result_ref: Some("cas:result:terminal".into()),
            result_digest: Some(digest_for('a')),
            artifacts: Vec::new(),
            trace_ref: "trace:terminal".into(),
            tool_call_refs: vec!["toolcall:terminal:0".into()],
            outcome_ref: "outcome:terminal".into(),
            capability_digest: digest_for('b'),
            catalog_digest: digest_for('c'),
            policy_digest: digest_for('d'),
            model_digest: digest_for('e'),
            event_sequence: 1,
            completeness: OutcomeCompleteness::Complete,
            missing_refs: Vec::new(),
            outbox_id: batch_id.into(),
            langfuse_observation_refs: Vec::new(),
        };
        let receipt_nodes = vec![
            terminal_receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
            terminal_receipt_node(
                &bundle,
                ReceiptNodeKind::ToolCall,
                &bundle.tool_call_refs[0],
            ),
            terminal_receipt_node(
                &bundle,
                ReceiptNodeKind::OutcomeEvaluation,
                &bundle.outcome_ref,
            ),
        ];
        TerminalOutcomeExtension {
            outcome_bundle: bundle,
            receipt_nodes,
            run_event: RunEvent {
                schema_version: OUTCOME_BUNDLE_VERSION,
                delegation_id: "delegation:terminal".into(),
                delegator_id: "agent:delegator-a".into(),
                selected_agent_id: "agent:selected-b".into(),
                executor_lease_actor: worker_id.into(),
                outcome: outcome.into(),
                work_item_id: work_item_id.into(),
                run_id: "run:terminal".into(),
                fence_token: fencing_token,
                outbox_id: batch_id.into(),
                result_ref: Some("cas:result:terminal".into()),
                capability_digest: digest_for('b'),
                catalog_digest: digest_for('c'),
                policy_digest: digest_for('d'),
                model_digest: digest_for('e'),
                event_sequence: 1,
                completeness: OutcomeCompleteness::Complete,
                missing_refs: Vec::new(),
                kind: "outcome".into(),
                tool_call_ref: None,
                outcome_ref: Some("outcome:terminal".into()),
                payload_digest: digest_for('f'),
                timestamp_ms: 10,
                cursor_token: "cursor:terminal:1".into(),
                carrier_digest: digest_for('0'),
            },
        }
    }

    fn terminal_extension_batch(
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        work_item_id: &str,
        worker_id: &str,
        lease_epoch: u64,
        fencing_token: u64,
        outcome: &str,
        retryable: bool,
    ) -> MutationBatch {
        let mut terminal = batch(batch_id, idempotency_key);
        terminal.version_expectation = VersionExpectation::Graph(expected_graph_version);
        let extension =
            terminal_extension(batch_id, work_item_id, fencing_token, outcome, worker_id);
        terminal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::CommitWorkItemResult {
                tenant: "tenant-a".into(),
                work_item_id: work_item_id.into(),
                worker_id: worker_id.into(),
                lease_epoch,
                fencing_token,
                idempotency_key: idempotency_key.into(),
                outcome: outcome.into(),
                result_ref: Some("cas:result:terminal".into()),
                outcome_extension: Some(Box::new(extension.clone())),
                error_ref: None,
                retryable,
                now_ms: 1_000,
            },
        }];
        let bundle = &extension.outcome_bundle;
        let completeness = serde_json::to_value(bundle.completeness)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let missing_refs = serde_json::to_string(&bundle.missing_refs).unwrap();
        let actor = terminal
            .envelope
            .operation()
            .expect("terminal fixture has an operation envelope")
            .authority
            .actor
            .clone();
        let mut scope_digest = Sha256::new();
        scope_digest.update(terminal.identity.tenant().as_str().as_bytes());
        scope_digest.update([0]);
        scope_digest.update(
            terminal
                .identity
                .scope()
                .graph_name()
                .expect("terminal fixture is graph-scoped")
                .as_str()
                .as_bytes(),
        );
        let scope_digest = hex::encode(scope_digest.finalize());
        let mut headers = BTreeMap::from([
            ("batch_id".to_string(), batch_id.to_string()),
            ("delegation_id".to_string(), bundle.delegation_id.clone()),
            ("delegator_id".to_string(), bundle.delegator_id.clone()),
            (
                "selected_agent_id".to_string(),
                bundle.selected_agent_id.clone(),
            ),
            (
                "executor_lease_actor".to_string(),
                bundle.executor_lease_actor.clone(),
            ),
            ("outcome".to_string(), bundle.outcome.clone()),
            ("work_item_id".to_string(), bundle.work_item_id.clone()),
            ("run_id".to_string(), bundle.run_id.clone()),
            ("fence_token".to_string(), bundle.fence_token.to_string()),
            (
                "capability_digest".to_string(),
                bundle.capability_digest.clone(),
            ),
            ("catalog_digest".to_string(), bundle.catalog_digest.clone()),
            ("policy_digest".to_string(), bundle.policy_digest.clone()),
            ("model_digest".to_string(), bundle.model_digest.clone()),
            ("completeness".to_string(), completeness),
            ("missing_refs".to_string(), missing_refs),
            ("actor".to_string(), actor.as_str().to_string()),
            ("scope_sha256".to_string(), scope_digest),
        ]);
        if let Some(result_ref) = &bundle.result_ref {
            headers.insert("result_ref".to_string(), result_ref.clone());
        }
        terminal.outbox.push(MutationOutboxIntent {
            topic: RUN_EVENT_OUTBOX_TOPIC.into(),
            key: batch_id.into(),
            payload: rmp_serde::to_vec_named(&extension.run_event).unwrap(),
            headers,
        });
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        terminal
    }

    fn assert_persisted_terminal_currency(
        shard: &Shard,
        batch_id: &str,
        outcome: &str,
        completeness: OutcomeCompleteness,
        missing_refs: &[&str],
    ) {
        let outbox = read_mutation_outbox(shard, "graph-a", batch_id).unwrap();
        assert_eq!(outbox.len(), 1);
        let event: RunEvent = rmp_serde::from_slice(&outbox[0].intent.payload).unwrap();
        assert_eq!(event.outcome, outcome);
        assert_eq!(event.completeness, completeness);
        assert_eq!(
            event.missing_refs,
            missing_refs
                .iter()
                .map(|reference| (*reference).to_string())
                .collect::<Vec<_>>()
        );
        let expected_actor = format!("principal:sha256:{}", "a".repeat(64));
        assert_eq!(
            outbox[0].intent.headers.get("actor").map(String::as_str),
            Some(expected_actor.as_str())
        );
        let admitted_scope = shard::graph_scope_identity("graph-a")
            .expect("terminal assertions use the canonical admitted graph scope");
        let mut expected_scope_digest = Sha256::new();
        expected_scope_digest.update(admitted_scope.tenant().as_str().as_bytes());
        expected_scope_digest.update([0]);
        expected_scope_digest.update(
            admitted_scope
                .scope()
                .graph_name()
                .expect("canonical terminal assertion scope is graph-scoped")
                .as_str()
                .as_bytes(),
        );
        let expected_scope = hex::encode(expected_scope_digest.finalize());
        assert_eq!(
            outbox[0]
                .intent
                .headers
                .get("scope_sha256")
                .map(String::as_str),
            Some(expected_scope.as_str())
        );
        for node_id in ["trace:terminal", "toolcall:terminal:0", "outcome:terminal"] {
            let receipt = read_one_node(shard, "graph-a", node_id, DurableCrypto::none()).unwrap();
            if missing_refs.contains(&node_id) {
                assert!(receipt.is_none(), "missing receipt {node_id} was persisted");
                continue;
            }
            let receipt = receipt.expect("terminal receipt should be persisted");
            let receipt: serde_json::Value = decode_durable(&receipt).unwrap();
            assert_eq!(receipt["outcome"], outcome);
            assert_eq!(
                receipt["completeness"],
                serde_json::to_value(completeness).unwrap()
            );
            assert_eq!(receipt["missing_refs"], serde_json::json!(missing_refs));
        }
    }

    fn seed_and_claim_terminal_work_item(
        shard: &Shard,
        tag: &str,
        work_item_id: &str,
    ) -> ClaimWorkItemResult {
        let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: delegated_work_item_method(work_item_id, 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(shard, &seed, None).unwrap();
        commit_native_claim(
            shard,
            &format!("{tag}-claim"),
            &format!("{tag}-claim-key"),
            4,
            Some(work_item_id),
            "worker-a",
            0,
            60_000,
            64,
        )
    }

    // Test-only fixture builder: every parameter is an independent field of
    // the `ClaimWorkItemRequest` under construction; no natural grouping.
    #[allow(clippy::too_many_arguments)]
    fn native_claim_batch(
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        work_item_id: Option<&str>,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        max_tenant_in_flight: u64,
    ) -> MutationBatch {
        let mut claim = batch(batch_id, idempotency_key);
        claim.version_expectation = VersionExpectation::Graph(expected_graph_version);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: crate::epistemic_operations::ClaimWorkItemRequest {
                    schema_version:
                        crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: work_item_id.map(str::to_string),
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: worker_id.into(),
                    now_ms,
                    lease_ms,
                    max_tenant_in_flight,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("native claim fixture reseals its final body");
        claim
    }

    // Test-only fixture builder; mirrors `native_claim_batch`'s justification
    // above plus the `db` handle it commits the built batch against.
    #[allow(clippy::too_many_arguments)]
    fn commit_native_claim(
        shard: &Shard,
        batch_id: &str,
        idempotency_key: &str,
        expected_graph_version: u64,
        work_item_id: Option<&str>,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        max_tenant_in_flight: u64,
    ) -> ClaimWorkItemResult {
        let committed = commit_at(
            shard,
            &native_claim_batch(
                batch_id,
                idempotency_key,
                expected_graph_version,
                work_item_id,
                worker_id,
                now_ms,
                lease_ms,
                max_tenant_in_flight,
            ),
            None,
        )
        .unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        decode_durable(&bytes).unwrap()
    }

    fn public_batch_method(operations: serde_json::Value) -> Method {
        Method::BatchUpdate {
            operations_msgpack: rmp_serde::to_vec_named(&operations).unwrap(),
        }
    }

    fn commit_at_graph(
        shard: &Shard,
        graph_fname: &str,
        batch: &MutationBatch,
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            shard,
            BatchCommitInput {
                graph_fname,
                batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: None,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
                crashpoint: point,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn commit_at(
        shard: &Shard,
        batch: &MutationBatch,
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        commit_at_graph(shard, "graph-a", batch, point)
    }

    fn commit_with_result(
        shard: &Shard,
        batch: &MutationBatch,
        result: &[u8],
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch(
            shard,
            "graph-a",
            batch,
            Some(result),
            101,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn commit_crossmodal_at(
        shard: &Shard,
        batch: &MutationBatch,
        methods: &[Method],
        vectors: &[VectorUpsert],
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            shard,
            BatchCommitInput {
                graph_fname: "graph-a",
                batch,
                change: None,
                authoritative_state_msgpack: None,
                crossmodal: Some(CrossModalBatchRows {
                    methods,
                    vectors,
                    blob_refs: &[],
                    measurements: &[],
                }),
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
                crashpoint: point,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn assert_absent_after_reopen(path: &std::path::Path, batch_id: &str) {
        let reopened = open(path);
        assert!(
            read_one_node(&reopened, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_one_node(&reopened, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_batch_for_graph(&reopened, "graph-a", batch_id)
                .unwrap()
                .is_none()
        );
        assert!(read_mutation_outbox(&reopened, "graph-a", batch_id)
            .unwrap()
            .is_empty());
    }

    /// Deterministic kill points before the redb commit all reopen as NO mutation:
    /// never one node, status without rows, or an orphan outbox record.
    #[test]
    fn precommit_crashpoints_reopen_with_no_partial_batch() {
        for point in [
            MutationBatchCrashpoint::BeforeRows,
            MutationBatchCrashpoint::AfterRowsBeforeMetadata,
            MutationBatchCrashpoint::BeforeCommit,
        ] {
            let path = temp_path(&format!("pre-{point:?}"));
            {
                let db = open(&path);
                let b = batch("batch-pre", "idem-pre");
                assert!(commit_at(&db, &b, Some(point)).is_err());
            }
            assert_absent_after_reopen(&path, "batch-pre");
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn mutation_batch_write_budgets_leave_no_graph_or_receipt_effects() {
        let oversized_path = temp_path("oversized-result");
        {
            let db = open(&oversized_path);
            let mutation = batch("batch-oversized-result", "idem-oversized-result");
            let result = vec![0; (64 * 1024 * 1024) + 1];
            assert!(commit_with_result(&db, &mutation, &result).is_err());
            assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        }
        assert_absent_after_reopen(&oversized_path, "batch-oversized-result");
        let _ = std::fs::remove_file(oversized_path);

        let collection_path = temp_path("excessive-collection");
        {
            let db = open(&collection_path);
            let mut mutation = batch("batch-excessive-collection", "idem-excessive-collection");
            mutation.outbox = vec![mutation.outbox[0].clone(); 100_001];
            mutation
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("oversized collection fixture reseals its final body");
            assert!(commit_at(&db, &mutation, None).is_err());
            assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        }
        assert_absent_after_reopen(&collection_path, "batch-excessive-collection");
        let _ = std::fs::remove_file(collection_path);
    }

    #[test]
    fn missing_graph_version_is_initial_zero_not_caller_seeded() {
        let path = temp_path("missing-version");
        let db = reopen(&path);
        let error = commit_at(&db, &batch("batch-version", "idem-version"), None).unwrap_err();
        assert!(error.contains("authoritative version is 0"));
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// A crash after commit but before acknowledgement reopens as one COMPLETE
    /// committed mutation.  Retrying returns its stored result and does not append
    /// duplicate rows or outbox events.
    #[test]
    fn postcommit_crash_restarts_and_replays_idempotently() {
        let path = temp_path("postcommit");
        let b = batch("batch-post", "idem-post");
        {
            let db = open(&path);
            assert!(
                commit_at(&db, &b, Some(MutationBatchCrashpoint::AfterCommitBeforeAck)).is_err()
            );
        }
        {
            let db = open(&path);
            assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .is_some());
            assert!(read_one_node(&db, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .is_some());
            let record = read_mutation_batch_for_graph(&db, "graph-a", "batch-post")
                .unwrap()
                .unwrap();
            assert_eq!(record.status, MutationBatchStatus::Committed);
            let outbox = read_mutation_outbox(&db, "graph-a", "batch-post").unwrap();
            assert_eq!(
                outbox.len(),
                3,
                "two canonical events + one explicit intent"
            );

            let mut retry = b.clone();
            retry.envelope = fixture_operation_envelope(
                &retry.identity,
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                42,
                "idem-post",
            );
            retry
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .unwrap();
            assert_ne!(
                retry
                    .envelope
                    .operation()
                    .expect("retry carries an operation envelope")
                    .authority
                    .nonce,
                b.envelope
                    .operation()
                    .expect("original carries an operation envelope")
                    .authority
                    .nonce,
                "the lost-ack retry must use a fresh attempt nonce"
            );
            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", "batch-post")
                    .unwrap()
                    .len(),
                3
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn caller_scoped_ingress_binds_shard_identity_and_replays_once() {
        let path = temp_path("caller-ingress-binding");
        let mut first = batch("batch-caller-ingress", "idem-caller-ingress");
        let caller_authority = first
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .authority
            .clone();
        let caller_actor = caller_authority.actor.as_str().to_string();
        let schema_digest = first
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        for intent in &mut first.outbox {
            intent.headers.insert(
                crate::mutation_batch::MUTATION_ACTOR_HEADER.to_string(),
                caller_actor.clone(),
            );
        }
        first.reseal_envelope(schema_digest).unwrap();

        let expected_identity = shard::graph_scope_identity("graph-a").unwrap();
        let mut retry = batch("batch-caller-ingress", "idem-caller-ingress");
        let retry_schema_digest = retry
            .envelope
            .operation()
            .expect("fixture carries an operation envelope")
            .method_schema_digest;
        for intent in &mut retry.outbox {
            intent.headers.insert(
                crate::mutation_batch::MUTATION_ACTOR_HEADER.to_string(),
                caller_actor.clone(),
            );
        }
        retry.reseal_envelope(retry_schema_digest).unwrap();
        assert_ne!(
            caller_authority.nonce,
            retry
                .envelope
                .operation()
                .expect("retry carries an operation envelope")
                .authority
                .nonce,
            "the replay regression must use a fresh attempt nonce"
        );

        {
            let db = open(&path);
            let committed = commit_at(&db, &first, None).unwrap();
            assert!(!committed.replayed);
            assert_eq!(committed.identity, expected_identity);

            let record = read_mutation_batch_for_graph(&db, "graph-a", first.batch_id.as_str())
                .unwrap()
                .unwrap();
            assert_eq!(record.identity, expected_identity);
            assert_eq!(record.batch.identity, expected_identity);
            let operation = record
                .batch
                .envelope
                .operation()
                .expect("durable record retains operation authority");
            assert_eq!(
                operation.serving_principal,
                crate::mutation_apply::ENGINE_LEDGER_PRINCIPAL
            );
            assert_eq!(operation.authority, caller_authority);
            assert_eq!(record.committing_actor().unwrap(), caller_actor);

            let outbox = read_mutation_outbox(&db, "graph-a", first.batch_id.as_str()).unwrap();
            assert!(!outbox.is_empty());
            for row in &outbox {
                assert_eq!(row.identity, expected_identity);
                assert_eq!(
                    row.intent
                        .headers
                        .get(crate::mutation_batch::MUTATION_ACTOR_HEADER),
                    Some(&caller_actor)
                );
            }
            let version = read_mutation_graph_version(&db, "graph-a").unwrap();
            let outbox_count = outbox.len();

            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.identity, expected_identity);
            assert_eq!(
                read_mutation_graph_version(&db, "graph-a").unwrap(),
                version
            );
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", first.batch_id.as_str())
                    .unwrap()
                    .len(),
                outbox_count
            );
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn caller_route_and_authority_proofs_precede_shard_rebind() {
        let path = temp_path("caller-route-authority-proof");
        let db = open(&path);

        // A batch authorized and compiled for graph-a must not become a graph-b
        // write merely because the persistence caller supplied graph-b as its
        // routing key. The graph-b scope may be lazily bound by the read/write
        // path, but no mutation receipt, row, outbox entry, or version advance
        // may be produced.
        let wrong_route = batch("batch-wrong-route", "idem-wrong-route");
        let error = commit_at_graph(&db, "graph-b", &wrong_route, None).unwrap_err();
        assert!(
            error.contains("caller mutation scope graph"),
            "got: {error}"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        assert_eq!(read_mutation_graph_version(&db, "graph-b").unwrap(), 0);
        for graph in ["graph-a", "graph-b"] {
            assert!(read_one_node(&db, graph, "a", DurableCrypto::none())
                .unwrap()
                .is_none());
            assert!(
                read_mutation_batch_for_graph(&db, graph, "batch-wrong-route")
                    .unwrap()
                    .is_none()
            );
            assert!(read_mutation_outbox(&db, graph, "batch-wrong-route")
                .unwrap()
                .is_empty());
        }

        // Re-sealing after changing the caller identity makes the batch body
        // self-consistent, but its authority still names the original tenant
        // and authority scope. That mismatch must be rejected before the
        // identity is replaced by the reserved shard identity.
        let mut wrong_authority = batch("batch-wrong-authority", "idem-wrong-authority");
        wrong_authority.identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-b").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        wrong_authority
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let error = commit_at_graph(&db, "graph-a", &wrong_authority, None).unwrap_err();
        assert!(
            error.contains("caller mutation authority scope"),
            "got: {error}"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "batch-wrong-authority")
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", "batch-wrong-authority")
                .unwrap()
                .is_empty()
        );

        // Logical graph names are routed through the same escaping used by
        // the persistence boundary. The pre-bind proof compares the sanitized
        // route key, while the caller's logical identity remains the authority
        // evidence used to derive that key.
        let logical_graph = "graph:a";
        let physical_graph = crate::redb_store::sanitize(logical_graph);
        let logical_identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new(logical_graph).unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let mut logical_batch = batch("batch-sanitized-route", "idem-sanitized-route");
        logical_batch.identity = logical_identity.clone();
        logical_batch.envelope = fixture_operation_envelope(
            &logical_identity,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "idem-sanitized-route",
        );
        // This is a new physical graph member on the fixture shard, so its
        // in-lock OCC version starts at zero; the route assertion is the
        // behavior under test rather than a carry-over from graph-a.
        logical_batch.version_expectation = VersionExpectation::Graph(0);
        logical_batch
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let committed = commit_at_graph(&db, &physical_graph, &logical_batch, None).unwrap();
        assert!(!committed.replayed);
        let mut retry = logical_batch.clone();
        retry.envelope = fixture_operation_envelope(
            &logical_identity,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            42,
            "idem-sanitized-route",
        );
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        assert!(
            commit_at_graph(&db, &physical_graph, &retry, None)
                .unwrap()
                .replayed
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn public_batch_reopens_and_replays_edges_vectors_and_tombstones_once() {
        let path = temp_path("public-batch-replay");
        let operations = serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "alpha"}},
            {"op": "add_node", "id": "b", "properties": {"text": "beta"}},
            {"op": "add_node", "id": "c", "properties": {"text": "gamma"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "old"}},
            {"op": "add_edge", "source": "a", "target": "b", "properties": {"kind": "also old"}},
            {"op": "upsert_edge", "source": "a", "target": "b", "properties": {"kind": "new"}},
            {"op": "add_edge", "source": "c", "target": "a", "properties": {"kind": "incoming"}},
            {"op": "add_embedding", "id": "a", "embedding": [0.25, 0.75]}
        ]);
        let mut initial = batch("batch-public", "idem-public");
        initial.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(operations),
        }];
        initial
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        {
            let db = open(&path);
            let committed = commit_at(&db, &initial, None).unwrap();
            assert!(!committed.replayed);
        }
        {
            let db = open(&path);
            let mut retry = initial.clone();
            retry.envelope = fixture_operation_envelope(
                &retry.identity,
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                42,
                "idem-public",
            );
            retry
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .unwrap();
            assert_ne!(
                retry
                    .envelope
                    .operation()
                    .expect("retry carries an operation envelope")
                    .authority
                    .nonce,
                initial
                    .envelope
                    .operation()
                    .expect("original carries an operation envelope")
                    .authority
                    .nonce,
                "the reopen retry must use a fresh attempt nonce"
            );
            let replay = commit_at(&db, &retry, None).unwrap();
            assert!(replay.replayed, "retry must use the stored batch result");
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            assert_eq!(dump.nodes.len(), 3);
            assert_eq!(dump.edges.len(), 2, "upsert must not duplicate the pair");
            let (_, _, properties) = dump
                .edges
                .iter()
                .find(|(source, target, _)| source == "a" && target == "b")
                .expect("upserted edge");
            let properties: serde_json::Value = rmp_serde::from_slice(properties).unwrap();
            assert_eq!(properties["kind"], "new");
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), Some(vec![0.25, 0.75]));
        }

        let mut removal = batch("batch-remove-a", "idem-remove-a");
        removal.version_expectation = VersionExpectation::Graph(4);
        removal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([
                {"op": "remove_node", "id": "a"}
            ])),
        }];
        removal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public removal fixture reseals its final body");
        {
            let db = open(&path);
            commit_at(&db, &removal, None).unwrap();
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            assert_eq!(dump.nodes.len(), 2);
            assert!(
                dump.edges.is_empty(),
                "outgoing and incoming edges must tombstone"
            );
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), None);
        }
        let _ = std::fs::remove_file(path);
    }

    /// BUG-CX-096: `read_graph_dump` used to expose ONLY `GRAPH_META` /
    /// `MUTATION_GRAPH_VERSION` / `NODES` / `EDGES` / `LEDGER` / `SEMANTIC`, making
    /// the 10 `development_lane_*` and 9 `resource_*` tables invisible to dump
    /// diagnostics. They are now observable but explicitly read-only: `GraphDump`
    /// remains incomplete for transfer, and `apply_checkpoint` rejects this origin.
    /// Seed one row directly into EVERY one of those 19 tables (raw redb inserts,
    /// bypassing every native-operation precondition — the same technique
    /// `redb_backend::tests::seed_raw_two_str_row` uses for the sibling BUG-CX-016/054
    /// reshard/backup coverage tests), then prove every row is present on
    /// `dump.native`. `read_graph_dump`'s scans copy these blobs through unsealed but
    /// otherwise UNDECODED (no typed `DurableLaneHold`/`DurableResourceReservation`
    /// deserialization — see `NativeOperationDumpRows`'s doc), so an arbitrary
    /// byte/text/int payload is a legitimate, minimal seed here.
    ///
    /// Confirmed FAILING before this fix: `GraphDump` had no `native` field at all
    /// (`cargo check` errors `no field \`native\` on type \`GraphDump\``) — the
    /// absence of the field IS the observability defect this test closes.
    #[test]
    fn read_graph_dump_carries_every_native_lane_and_resource_table() {
        let path = temp_path("native-dump-coverage");
        let db = open(&path);
        let seed = batch("batch-native-dump-seed", "idem-native-dump-seed");
        commit_at(&db, &seed, None).unwrap();

        {
            let members = db.graph_members(&["graph-a"]).unwrap();
            let (group, batches) = db.admit_maintenance(&members, "native-dump-seed").unwrap();
            let write = ShardWrite::open(&db, &group, &members, &batches).unwrap();
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::HOLDS)
                    .unwrap();
                t.insert(("graph-a", "hold-1"), b"hold-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::TENANT_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "hold-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::LANE_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "lane-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::REPOSITORY_BRANCH_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "branch-1"), "hold-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::WORKTREE_INDEX)
                    .unwrap();
                t.insert(("graph-a", "worktree-1"), "hold-1").unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::WORK_ITEM_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", 1u64), "hold-1").unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::COUNTERS)
                    .unwrap();
                t.insert(("graph-a", "scope-1"), b"counter-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::PRESSURE_INDEX)
                    .unwrap();
                t.insert(
                    (
                        "graph-a",
                        "tenant-1",
                        "scope-1",
                        "metric-1",
                        5u64,
                        "counter-1",
                    ),
                    1u8,
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::POLICIES)
                    .unwrap();
                t.insert(("graph-a", "tenant-1"), b"policy-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(development_lane::INVOCATIONS)
                    .unwrap();
                t.insert(
                    ("graph-a", "tenant-1", "invocation-1"),
                    b"invocation-bytes".as_slice(),
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATIONS)
                    .unwrap();
                t.insert(
                    ("graph-a", "reservation-1"),
                    b"reservation-bytes".as_slice(),
                )
                .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                    .unwrap();
                t.insert(("graph-a", "tenant-1", "reservation-1"), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                    .unwrap();
                t.insert(("graph-a", "work-item-1", 1u64), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_HOSTS)
                    .unwrap();
                t.insert(("graph-a", "host-1"), b"host-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_EXCLUSIVITY)
                    .unwrap();
                t.insert(("graph-a", "exclusivity-1"), "reservation-1")
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_FAIRNESS)
                    .unwrap();
                t.insert(("graph-a", "group-1"), b"fairness-bytes".as_slice())
                    .unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_CONCURRENCY)
                    .unwrap();
                t.insert(("graph-a", "key-1"), 3u64).unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_ANTI_AFFINITY)
                    .unwrap();
                t.insert(("graph-a", "host-1", "tag-1"), 2u64).unwrap();
            }
            {
                let mut t = write
                    .graph("graph-a")
                    .unwrap()
                    .open_scoped_table(RESOURCE_DISK_POLICIES)
                    .unwrap();
                t.insert(("graph-a", "policy-1"), b"disk-policy-bytes".as_slice())
                    .unwrap();
            }
            write.finish().unwrap();
            db.commit_drain(group, &batches, 0).unwrap();
        }

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert_eq!(dump.kind, GraphDumpKind::DurableReadOnlyMaterialization);

        assert_eq!(
            dump.native.development_lane_holds,
            vec![("hold-1".to_string(), b"hold-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_tenant_index,
            vec![(
                ("tenant-1".to_string(), "hold-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_lane_index,
            vec![(
                ("tenant-1".to_string(), "lane-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_repository_branch_index,
            vec![(
                ("tenant-1".to_string(), "branch-1".to_string()),
                "hold-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.development_lane_worktree_index,
            vec![("worktree-1".to_string(), "hold-1".to_string())]
        );
        assert_eq!(
            dump.native.development_lane_work_item_index,
            vec![(("tenant-1".to_string(), 1u64), "hold-1".to_string())]
        );
        assert_eq!(
            dump.native.development_lane_counters,
            vec![("scope-1".to_string(), b"counter-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_pressure_index,
            vec![(
                (
                    "tenant-1".to_string(),
                    "scope-1".to_string(),
                    "metric-1".to_string(),
                    5u64,
                    "counter-1".to_string()
                ),
                1u8
            )]
        );
        assert_eq!(
            dump.native.development_lane_policies,
            vec![("tenant-1".to_string(), b"policy-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.development_lane_invocations,
            vec![(
                ("tenant-1".to_string(), "invocation-1".to_string()),
                b"invocation-bytes".to_vec()
            )]
        );
        assert_eq!(
            dump.native.resource_reservations,
            vec![("reservation-1".to_string(), b"reservation-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_reservation_tenant_index,
            vec![(
                ("tenant-1".to_string(), "reservation-1".to_string()),
                "reservation-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.resource_reservation_attempts,
            vec![(
                ("work-item-1".to_string(), 1u64),
                "reservation-1".to_string()
            )]
        );
        assert_eq!(
            dump.native.resource_hosts,
            vec![("host-1".to_string(), b"host-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_exclusivity,
            vec![("exclusivity-1".to_string(), "reservation-1".to_string())]
        );
        assert_eq!(
            dump.native.resource_fairness,
            vec![("group-1".to_string(), b"fairness-bytes".to_vec())]
        );
        assert_eq!(
            dump.native.resource_concurrency,
            vec![("key-1".to_string(), 3u64)]
        );
        assert_eq!(
            dump.native.resource_anti_affinity,
            vec![(("host-1".to_string(), "tag-1".to_string()), 2u64)]
        );
        assert_eq!(
            dump.native.resource_disk_policies,
            vec![("policy-1".to_string(), b"disk-policy-bytes".to_vec())]
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn durable_read_dump_is_rejected_before_checkpoint_mutation() {
        let path = temp_path("read-dump-checkpoint-refusal");
        let db = open(&path);
        let seed = batch("batch-read-dump-refusal", "idem-read-dump-refusal");
        commit_at(&db, &seed, None).unwrap();

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("seed graph identity");
        assert_eq!(dump.kind, GraphDumpKind::DurableReadOnlyMaterialization);
        assert!(
            dump.native.is_empty(),
            "the origin marker must reject even a read with no native rows"
        );
        let incarnation_id = dump.incarnation_id.clone();
        let source_snapshot_version = dump.source_snapshot_version;
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(&db, &mut pending, vec![dump], DurableCrypto::none())
            .expect_err("a durable read is not a complete transfer image");
        assert!(error.contains("RedbBackend::reshard_graph"));
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        assert!(matches!(pending[0].1, Method::ClearGraph));

        let after = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("rejection must preserve destination rows");
        assert_eq!(after.incarnation_id, incarnation_id);
        assert_eq!(after.source_snapshot_version, source_snapshot_version);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn duplicate_checkpoint_graph_ids_are_rejected_before_pending_mutation() {
        let path = temp_path("duplicate-checkpoint-graph-id");
        let db = open(&path);
        let dump = || {
            GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
                graph: "graph-a".to_string(),
                name: "graph-a".to_string(),
                graph_type: GraphType::Global,
                incarnation_id: "incarnation:test:duplicate-checkpoint".to_string(),
                source_snapshot_version: 1,
                integrity_policy: None,
                nodes: Vec::new(),
                edges: Vec::new(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            })
        };
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(
            &db,
            &mut pending,
            vec![dump(), dump()],
            DurableCrypto::none(),
        )
        .expect_err("duplicate graph images are ambiguous");
        assert_eq!(error, "checkpoint contains duplicate graph id");
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn in_place_checkpoint_rejects_attached_native_authority_before_mutation() {
        let path = temp_path("native-authority-checkpoint-refusal");
        let db = open(&path);
        let mut dump = GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
            graph: "graph-a".to_string(),
            name: "graph-a".to_string(),
            graph_type: GraphType::Global,
            incarnation_id: "incarnation:test:native-authority-refusal".to_string(),
            source_snapshot_version: 1,
            integrity_policy: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic: Vec::new(),
        });
        dump.native
            .resource_hosts
            .push(("host-a".to_string(), Vec::new()));
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];

        let error = apply_checkpoint(&db, &mut pending, vec![dump], DurableCrypto::none())
            .expect_err("ordinary checkpoints cannot carry native authority rows");
        assert!(error.contains("RedbBackend::reshard_graph"));
        assert_eq!(pending.len(), 1, "rejection must not consume pending work");
        assert!(matches!(pending[0].1, Method::ClearGraph));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn public_batch_upsert_node_merges_durable_fields_across_reopen() {
        let path = temp_path("public-batch-node-upsert");
        let mut seed = batch("batch-upsert-seed", "idem-upsert-seed");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([{
                "op": "add_node",
                "id": "existing",
                "properties": {
                    "retained": "yes",
                    "overwritten": "old",
                    "nested": {"left": 1, "right": 2}
                }
            }])),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public seed fixture reseals its final body");
        let mut upsert = batch("batch-upsert-merge", "idem-upsert-merge");
        upsert.version_expectation = VersionExpectation::Graph(4);
        upsert.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: public_batch_method(serde_json::json!([
                {
                    "op": "upsert_node",
                    "id": "existing",
                    "properties": {
                        "overwritten": "new",
                        "added": true,
                        "nested": {"left": 9}
                    }
                },
                {"op": "upsert_node", "id": "created", "properties": {"created": true}}
            ])),
        }];
        upsert
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("public upsert fixture reseals its final body");
        {
            let db = open(&path);
            commit_at(&db, &seed, None).unwrap();
            commit_at(&db, &upsert, None).unwrap();
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            let existing = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "existing")
                .expect("existing node");
            let existing: serde_json::Value = rmp_serde::from_slice(&existing.1).unwrap();
            assert_eq!(existing["retained"], "yes");
            assert_eq!(existing["overwritten"], "new");
            assert_eq!(existing["added"], true);
            assert_eq!(existing["nested"], serde_json::json!({"left": 9}));
            let created = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "created")
                .expect("created node");
            let created: serde_json::Value = rmp_serde::from_slice(&created.1).unwrap();
            assert_eq!(created, serde_json::json!({"created": true}));
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn malformed_or_state_invalid_public_batch_rolls_back_all_rows() {
        for (tag, method) in [
            (
                "opaque",
                Method::BatchUpdate {
                    operations_msgpack: vec![0xc1],
                },
            ),
            (
                "missing-endpoint",
                public_batch_method(serde_json::json!([
                    {"op": "add_node", "id": "partial", "properties": {}},
                    {"op": "add_edge", "source": "partial", "target": "missing", "properties": {}}
                ])),
            ),
        ] {
            let path = temp_path(tag);
            let mut mutation = batch("batch-invalid", "idem-invalid");
            mutation.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method,
            }];
            mutation
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("malformed public fixture reseals its final body");
            {
                let db = open(&path);
                assert!(commit_at(&db, &mutation, None).is_err());
            }
            let db = open(&path);
            assert!(
                read_one_node(&db, "graph-a", "partial", DurableCrypto::none())
                    .unwrap()
                    .is_none(),
                "redb must discard earlier rows when a later operation fails"
            );
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", "batch-invalid")
                    .unwrap()
                    .is_none()
            );
            drop(db);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn terminal_work_item_retry_replays_and_conflicting_payload_fails_closed() {
        let path = temp_path("work-item-terminal-replay");
        let db = open(&path);

        let mut seed = batch("work-item-terminal-seed", "work-item-terminal-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-terminal-claim",
            "work-item-terminal-claim-key",
            4,
            Some("work-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);
        assert_eq!(claimed.lease_epoch, Some(1));
        assert_eq!(claimed.fencing_token, Some(1));

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: 1,
            fencing_token: 1,
            idempotency_key: "terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:one".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch(
            "work:terminal-stable-batch",
            "work-idem:terminal-stable-key",
        );
        // The attempt metadata this used to re-stamp -- request id, purpose,
        // policy fingerprint, trace id -- is either gone or structurally outside
        // the stable replay identity now, so a fixture that wants a distinct
        // request simply re-mints the envelope for it.
        terminal.envelope = fixture_operation_envelope(
            &terminal.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            777,
            "work-idem:terminal-stable-key",
        );
        let remint_attempt = |batch: &mut MutationBatch, nonce_byte: u8, created_at_ms: u64| {
            let eg_types::mutation_batch::MutationEnvelope::Operation(operation) =
                &mut batch.envelope
            else {
                panic!("authenticated replay fixture must carry an operation envelope");
            };
            operation.authority.nonce = eg_types::contract::Nonce::from_bytes([nonce_byte; 32]);
            operation.authority.context_digest = operation
                .authority
                .recompute_context_digest()
                .expect("fixture authority context remains valid after nonce rotation");
            batch.created_at_ms = created_at_ms;
        };
        // `CommitWorkItemResult` is a `native_terminal_work_item_cas` batch:
        // `check_occ_version_and_fence` never checks its expectation against
        // the authoritative version (the WorkItem lease/fencing token is its
        // real CAS guard), but `VersionExpectation` no longer has a "none"
        // arm to encode that -- 5 is simply the actual current graph version
        // at this point (seed 3->4, claim 4->5), matching real state rather
        // than an invented placeholder.
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: terminal_method,
        }];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal fixture reseals its final body");

        let first = commit_at(&db, &terminal, None).unwrap();
        assert!(!first.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let consumed_nonce = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            consumed_nonce.contains("REPLAY_NONCE_CONSUMED"),
            "{consumed_nonce}"
        );

        // A fresh transport request is normalized to the same durable request id
        // before this kernel sees it. Re-mint its attempt nonce while preserving
        // the stable operation key and body, then replay the stored result.
        let mut retry = terminal.clone();
        retry.envelope = fixture_operation_envelope(
            &retry.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            778,
            "work-idem:terminal-stable-key",
        );
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("terminal retry fixture reseals its final body");
        assert_ne!(
            retry
                .envelope
                .operation()
                .expect("retry carries an operation envelope")
                .authority
                .nonce,
            terminal
                .envelope
                .operation()
                .expect("original carries an operation envelope")
                .authority
                .nonce,
            "a retry must use a fresh attempt nonce"
        );
        retry.created_at_ms = 200;
        let replay = commit_at(&db, &retry, None).unwrap();
        assert!(replay.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let mut conflicting_payload = retry.clone();
        let Method::CommitWorkItemResult { result_ref, .. } =
            &mut conflicting_payload.operations[0].method
        else {
            unreachable!();
        };
        *result_ref = Some("result:sha256:different".into());
        remint_attempt(&mut conflicting_payload, 0x43, 300);
        conflicting_payload
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let error = commit_at(&db, &conflicting_payload, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));

        // Planted known-bad input: the same key under a DIFFERENT actor. The
        // actor is inside the stable operation identity, so this is a named
        // conflict rather than a silent replay -- the cross-actor
        // replay-ownership property the M1 review raised as a P1.
        let mut conflicting_authority = retry;
        let key = conflicting_authority.idempotency_key().to_string();
        let identity = conflicting_authority.identity.clone();
        conflicting_authority.envelope = fixture_operation_envelope(
            &identity,
            &format!("principal:sha256:{}", "b".repeat(64)),
            42,
            &key,
        );
        conflicting_authority
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("conflicting authority fixture reseals its final body");
        let error = commit_at(&db, &conflicting_authority, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 6);

        let stored = read_one_node(&db, "graph-a", "work-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "succeeded");
        assert_eq!(stored["result_ref"], "result:sha256:one");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // GOC-19/GOC-20 (BUG-015 "B9"): a `CommitWorkItemResult` batch may
    // co-commit provenance `AddNode` operations (RunTrace/ToolCall/
    // OutcomeEvaluation) in the SAME redb write transaction as the WorkItem's
    // terminal status -- proven here against a real redb-backed database, not
    // just the pure-Rust admission logic in `eg-types::work_item_command_log`.
    #[test]
    fn commit_work_item_result_co_commits_provenance_add_node_operations() {
        let path = temp_path("work-item-outcome-bundle-fusion");
        let db = open(&path);

        let mut seed = batch("work-item-bundle-seed", "work-item-bundle-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-bundle-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("bundle seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-bundle-claim",
            "work-item-bundle-claim-key",
            4,
            Some("work-bundle-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-bundle-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: claimed.lease_epoch.unwrap(),
            fencing_token: claimed.fencing_token.unwrap(),
            idempotency_key: "bundle-terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:bundled".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch("work:bundle-batch", "work-idem:bundle-key");
        // See the identical comment in
        // `terminal_work_item_retry_replays_and_conflicting_payload_fails_closed`:
        // this is a `native_terminal_work_item_cas` batch whose expectation is
        // never checked; 5 is the real current graph version here too (seed
        // 3->4, claim 4->5).
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: terminal_method,
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: node("trace:bundle-1", 11),
            },
            MutationOperation {
                ordinal: 2,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: node("outcome:bundle-1", 22),
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("bundle terminal fixture reseals its final body");

        commit_at(&db, &terminal, None).unwrap();

        let work_item = read_one_node(&db, "graph-a", "work-bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "succeeded");
        assert_eq!(work_item["result_ref"], "result:sha256:bundled");

        // The KNOWN-BAD half of this proof lives in the two tests below: this
        // establishes the PASS-on-good baseline -- both provenance nodes are
        // durable, in the SAME commit that landed the terminal status.
        let trace = read_one_node(&db, "graph-a", "trace:bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let trace: serde_json::Value = decode_durable(&trace).unwrap();
        assert_eq!(trace["value"], 11);

        let outcome = read_one_node(&db, "graph-a", "outcome:bundle-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let outcome: serde_json::Value = decode_durable(&outcome).unwrap();
        assert_eq!(outcome["value"], 22);

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // KNOWN-BAD: a CommitWorkItemResult batch may NOT carry an arbitrary
    // accompanying method (only AddNode provenance operations are allowed) --
    // and rejecting it must leave NEITHER the WorkItem status NOR the
    // disallowed operation's row durable (no partial commit).
    #[test]
    fn commit_work_item_result_batch_rejects_a_disallowed_accompanying_method() {
        let path = temp_path("work-item-outcome-bundle-disallowed");
        let db = open(&path);

        let mut seed = batch("work-item-disallowed-seed", "work-item-disallowed-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-disallowed-1", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("disallowed seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-disallowed-claim",
            "work-item-disallowed-claim-key",
            4,
            Some("work-disallowed-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        let terminal_method = Method::CommitWorkItemResult {
            tenant: "tenant-a".into(),
            work_item_id: "work-disallowed-1".into(),
            worker_id: "worker-a".into(),
            lease_epoch: claimed.lease_epoch.unwrap(),
            fencing_token: claimed.fencing_token.unwrap(),
            idempotency_key: "disallowed-terminal-key".into(),
            outcome: "succeeded".into(),
            result_ref: Some("result:sha256:disallowed".into()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch("work:disallowed-batch", "work-idem:disallowed-key");
        // A disallowed accompanying method makes `native_terminal_work_item_cas`
        // false (it re-validates every operation, not just `len()`), so this
        // batch is no longer exempt from graph-wide OCC -- supply the real
        // current version (seed 3->4, claim 4->5) so the batch reaches the
        // per-operation shape guard this test targets, instead of failing
        // earlier on a mismatched/missing `expected_graph_version`.
        terminal.version_expectation = VersionExpectation::Graph(5);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: terminal_method,
            },
            // Disallowed: only AddNode may ride alongside CommitWorkItemResult.
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::GraphSnapshot,
                method: Method::RemoveNode {
                    node_id: "work-disallowed-1".into(),
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("disallowed terminal fixture reseals its final body");

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("may only carry additional AddNode"),
            "got: {error}"
        );

        // No partial effect: the WorkItem is still `leased`, not `succeeded`.
        let work_item = read_one_node(&db, "graph-a", "work-disallowed-1", DurableCrypto::none())
            .unwrap()
            .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_ne!(work_item["status"], "succeeded");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    // KNOWN-BAD: a batch carrying TWO CommitWorkItemResult operations must be
    // rejected outright, never applying either.
    #[test]
    fn commit_work_item_result_batch_rejects_more_than_one_terminal_operation() {
        let path = temp_path("work-item-outcome-bundle-double-terminal");
        let db = open(&path);

        let mut seed = batch("work-item-double-seed", "work-item-double-seed-key");
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("work-double-1", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("work-double-2", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("double terminal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        // Seed is ONE batch creating two ready WorkItems (3->4); each claim is
        // its own batch and bumps the version once more (4->5, then 5->6).
        let claimed_1 = commit_native_claim(
            &db,
            "work-item-double-claim-1",
            "work-item-double-claim-1-key",
            4,
            Some("work-double-1"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed_1.claimed);
        let claimed_2 = commit_native_claim(
            &db,
            "work-item-double-claim-2",
            "work-item-double-claim-2-key",
            5,
            Some("work-double-2"),
            "worker-b",
            0,
            60_000,
            64,
        );
        assert!(claimed_2.claimed);

        let mut terminal = batch("work:double-batch", "work-idem:double-key");
        // Two CommitWorkItemResult operations also make
        // `native_terminal_work_item_cas` false (not all-AddNode after the
        // first), so -- same reasoning as the disallowed-method test above --
        // supply the real current version (6) to reach the per-operation shape
        // guard rather than failing earlier on OCC.
        terminal.version_expectation = VersionExpectation::Graph(6);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-1".into(),
                    worker_id: "worker-a".into(),
                    lease_epoch: claimed_1.lease_epoch.unwrap(),
                    fencing_token: claimed_1.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-1".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-one".into()),
                    outcome_extension: None,
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: DurabilityDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-2".into(),
                    worker_id: "worker-b".into(),
                    lease_epoch: claimed_2.lease_epoch.unwrap(),
                    fencing_token: claimed_2.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-2".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-two".into()),
                    outcome_extension: None,
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("double terminal fixture reseals its final body");

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("at most one CommitWorkItemResult"),
            "got: {error}"
        );

        for work_item_id in ["work-double-1", "work-double-2"] {
            let work_item = read_one_node(&db, "graph-a", work_item_id, DurableCrypto::none())
                .unwrap()
                .unwrap();
            let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
            assert_ne!(work_item["status"], "succeeded");
        }

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn generic_work_item_claim_enforces_tenant_in_flight_limit() {
        let path = temp_path("work-item-quota");
        let db = open(&path);
        let mut seed = batch("work-item-seed", "work-item-seed-key");
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("leased", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("quota seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-native-lease",
            "work-item-native-lease-key",
            4,
            Some("leased"),
            "worker-a",
            1_000,
            10_000,
            64,
        );
        assert!(claimed.claimed);

        let mut claim = batch("work-item-claim", "work-item-claim-key");
        claim.version_expectation = VersionExpectation::Graph(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: crate::epistemic_operations::ClaimWorkItemRequest {
                    schema_version:
                        crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: None,
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: "worker-a".into(),
                    now_ms: 1_000,
                    lease_ms: 10_000,
                    max_tenant_in_flight: 1,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("quota claim fixture reseals its final body");
        let committed = commit_at(&db, &claim, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        // `Raw` is the one canonical MessagePack-bin result representation.
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        let result: ClaimWorkItemResult = decode_durable(&bytes).unwrap();
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::TenantQuota);
        assert_eq!(result.tenant_in_flight, Some(1));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn exact_work_item_claim_cannot_bypass_tenant_in_flight_limit() {
        use crate::epistemic_operations::{
            ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion,
        };

        let path = temp_path("work-item-exact-quota");
        let db = open(&path);
        let mut seed = batch(
            "work-item-exact-quota-seed",
            "work-item-exact-quota-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("live", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("exact quota seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-exact-native-lease",
            "work-item-exact-native-lease-key",
            4,
            Some("live"),
            "worker-a",
            1_000,
            10_000,
            64,
        );
        assert!(claimed.claimed);

        let mut claim = batch(
            "work-item-exact-quota-claim",
            "work-item-exact-quota-claim-key",
        );
        claim.version_expectation = VersionExpectation::Graph(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::ClaimWorkItem {
                request: ClaimWorkItemRequest {
                    schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: Some("ready".into()),
                    queue_ref: None,
                    resource_class: None,
                    fairness_group: None,
                    worker_ref: "worker-a".into(),
                    now_ms: 1_000,
                    lease_ms: 10_000,
                    max_tenant_in_flight: 1,
                },
            },
        }];
        claim
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("exact quota claim fixture reseals its final body");
        let committed = commit_at(&db, &claim, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        let result: ClaimWorkItemResult = decode_durable(&bytes).unwrap();
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::TenantQuota);
        assert_eq!(result.tenant_in_flight, Some(1));

        let ready = read_one_node(&db, "graph-a", "ready", DurableCrypto::none())
            .unwrap()
            .expect("exact candidate remains inspectable");
        let ready: serde_json::Value = decode_durable(&ready).unwrap();
        assert_eq!(ready["status"], "ready");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn expired_exhausted_work_item_is_terminalized_without_an_over_ceiling_claim() {
        let path = temp_path("work-item-expired-attempt-ceiling");
        let db = open(&path);
        let mut seed = batch("work-item-expired-seed", "work-item-expired-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("exhausted", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("expired seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let first = commit_native_claim(
            &db,
            "work-item-expired-first",
            "work-item-expired-first-key",
            4,
            Some("exhausted"),
            "dead-worker",
            0,
            10_000,
            64,
        );
        assert!(first.claimed);
        let second = commit_native_claim(
            &db,
            "work-item-expired-second",
            "work-item-expired-second-key",
            5,
            Some("exhausted"),
            "dead-worker",
            100_000,
            10_000,
            64,
        );
        assert!(second.claimed);
        assert_eq!(second.attempt, Some(2));
        let third = commit_native_claim(
            &db,
            "work-item-expired-third",
            "work-item-expired-third-key",
            6,
            Some("exhausted"),
            "dead-worker",
            200_000,
            10_000,
            64,
        );
        assert!(third.claimed);
        assert_eq!(third.attempt, Some(3));
        let result = commit_native_claim(
            &db,
            "work-item-expired-claim",
            "work-item-expired-claim-key",
            7,
            Some("exhausted"),
            "replacement-worker",
            300_000,
            10_000,
            64,
        );
        assert!(!result.claimed);
        assert_eq!(result.reason, ClaimWorkItemResultReason::Empty);
        assert_eq!(result.changed_work_item_ids, vec!["exhausted"]);

        let stored = read_one_node(&db, "graph-a", "exhausted", DurableCrypto::none())
            .unwrap()
            .expect("expired work item remains inspectable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "dead_letter");
        assert_eq!(
            stored["attempt"], 3,
            "the exhausted attempt is never incremented"
        );
        assert_eq!(stored["max_attempts"], 3);
        assert_eq!(stored["error_ref"], "lease_exhausted");
        assert!(stored["lease_owner"].is_null());
        assert!(stored["lease_expires_at"].is_null());
        assert_eq!(stored["lease_epoch"], 6, "the dead holder is fenced out");
        assert_eq!(stored["fencing_token"], 6);

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn generic_claim_reaps_exhausted_lease_then_claims_a_different_ready_item() {
        let path = temp_path("work-item-generic-expired-attempt-ceiling");
        let db = open(&path);
        let mut seed = batch(
            "work-item-generic-expired-seed",
            "work-item-generic-expired-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("exhausted", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("runnable", 3),
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("generic expiry seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-first",
                "work-item-generic-expired-first-key",
                4,
                Some("exhausted"),
                "dead-worker",
                0,
                10_000,
                64,
            )
            .claimed
        );
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-second",
                "work-item-generic-expired-second-key",
                5,
                Some("exhausted"),
                "dead-worker",
                100_000,
                10_000,
                64,
            )
            .claimed
        );
        assert!(
            commit_native_claim(
                &db,
                "work-item-generic-expired-third",
                "work-item-generic-expired-third-key",
                6,
                Some("exhausted"),
                "dead-worker",
                200_000,
                10_000,
                64,
            )
            .claimed
        );
        let result = commit_native_claim(
            &db,
            "work-item-generic-expired-claim",
            "work-item-generic-expired-claim-key",
            7,
            None,
            "worker-b",
            300_000,
            10_000,
            64,
        );
        assert!(result.claimed);
        assert_eq!(result.work_item_id.as_deref(), Some("runnable"));
        assert_eq!(result.attempt, Some(1));
        assert!(result
            .changed_work_item_ids
            .iter()
            .any(|id| id == "exhausted"));
        assert!(result
            .changed_work_item_ids
            .iter()
            .any(|id| id == "runnable"));

        let exhausted = read_one_node(&db, "graph-a", "exhausted", DurableCrypto::none())
            .unwrap()
            .expect("expired work item remains inspectable");
        let exhausted: serde_json::Value = decode_durable(&exhausted).unwrap();
        assert_eq!(exhausted["status"], "dead_letter");
        assert_eq!(exhausted["attempt"], 3);
        assert_eq!(exhausted["error_ref"], "lease_exhausted");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// Regression for INCIDENT-kg-readonly-2026-07-31 / D-INC-1 / D-SH-5: a
    /// `RenewWorkItemLease` against a work item that no longer exists MUST still
    /// carry `changed_work_item_ids` (even if empty) in its committed result. If it
    /// doesn't, `commit_work_item` (`src/server/mutation_batch.rs`) can no longer
    /// read that field after the durable commit has already advanced the
    /// authoritative graph version — the serving projection is stranded one
    /// version behind for good, and `authoritative_graph_version` then fails
    /// closed on every later write, taking the whole graph read-only. This test
    /// exercises the REAL redb dispatch path, not a hand-built JSON fixture, so it
    /// fails on the pre-fix shape (`{"renewed": false, "reason": "missing"}`) and
    /// passes once the field is always present.
    #[test]
    fn renew_lease_on_a_missing_work_item_still_carries_changed_work_item_ids() {
        let path = temp_path("work-item-renew-missing");
        let db = open(&path);

        let mut renew = batch("work-item-renew-missing", "work-item-renew-missing-key");
        renew.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::RenewWorkItemLease {
                tenant: "tenant-a".into(),
                work_item_id: "does-not-exist".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 1,
                now_ms: 1_000,
                lease_ms: 10_000,
            },
        }];
        renew
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("missing renewal fixture reseals its final body");
        let committed = commit_at(&db, &renew, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("renew result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("RenewWorkItemLease must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["renewed"], false);
        assert_eq!(value["reason"], "missing");
        assert_eq!(
            value.get("changed_work_item_ids"),
            Some(&serde_json::json!([])),
            "a missing-work-item renewal must still carry changed_work_item_ids so \
             commit_work_item can call core.mark_dirty() and keep the serving \
             projection from stranding behind the authoritative graph version; \
             full result was: {value}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// Same incident, the other bricking shape: a lease renewal that is FENCED
    /// (wrong fencing token/epoch/owner/status) must also carry
    /// `changed_work_item_ids` in its result.
    #[test]
    fn renew_lease_that_is_fenced_still_carries_changed_work_item_ids() {
        let path = temp_path("work-item-renew-fenced");
        let db = open(&path);

        let mut seed = batch(
            "work-item-renew-fenced-seed",
            "work-item-renew-fenced-seed-key",
        );
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("leased", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("fenced renewal seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-renew-fenced-claim",
            "work-item-renew-fenced-claim-key",
            4,
            Some("leased"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);

        // Same work item, but the caller's fencing token is stale (2 vs the
        // durable row's 1) — this must be rejected as "fenced", not applied.
        let mut renew = batch("work-item-renew-fenced", "work-item-renew-fenced-key");
        renew.version_expectation = VersionExpectation::Graph(5);
        renew.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::RenewWorkItemLease {
                tenant: "tenant-a".into(),
                work_item_id: "leased".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 2,
                now_ms: 1_000,
                lease_ms: 10_000,
            },
        }];
        renew
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("fenced renewal fixture reseals its final body");
        let committed = commit_at(&db, &renew, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("renew result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("RenewWorkItemLease must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["renewed"], false);
        assert_eq!(value["reason"], "fenced");
        assert_eq!(
            value.get("changed_work_item_ids"),
            Some(&serde_json::json!([])),
            "a fenced renewal must still carry changed_work_item_ids so \
             commit_work_item can call core.mark_dirty() and keep the serving \
             projection from stranding behind the authoritative graph version; \
             full result was: {value}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn last_permitted_reclaim_survives_restart_but_the_next_reclaim_dead_letters() {
        let path = temp_path("work-item-attempt-boundary-restart");
        {
            let db = open(&path);
            let mut seed = batch("work-item-boundary-seed", "work-item-boundary-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("boundary", 3),
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("attempt boundary seed fixture reseals its final body");
            commit_at(&db, &seed, None).unwrap();
            assert!(
                commit_native_claim(
                    &db,
                    "work-item-boundary-first",
                    "work-item-boundary-first-key",
                    4,
                    Some("boundary"),
                    "dead-worker",
                    0,
                    10_000,
                    64,
                )
                .claimed
            );
            let second = commit_native_claim(
                &db,
                "work-item-boundary-second",
                "work-item-boundary-second-key",
                5,
                Some("boundary"),
                "dead-worker",
                100_000,
                10_000,
                64,
            );
            assert!(second.claimed);
            assert_eq!(second.attempt, Some(2));
            let third = commit_native_claim(
                &db,
                "work-item-boundary-last",
                "work-item-boundary-last-key",
                6,
                Some("boundary"),
                "last-permitted-worker",
                200_000,
                10_000,
                64,
            );
            assert!(third.claimed);
            assert_eq!(third.attempt, Some(3));
        }

        let db = open(&path);
        let result = commit_native_claim(
            &db,
            "work-item-boundary-over",
            "work-item-boundary-over-key",
            7,
            Some("boundary"),
            "would-be-fourth-worker",
            300_000,
            10_000,
            64,
        );
        assert!(!result.claimed);
        let stored = read_one_node(&db, "graph-a", "boundary", DurableCrypto::none())
            .unwrap()
            .expect("boundary work item remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["status"], "dead_letter");
        assert_eq!(stored["attempt"], 3);
        assert_eq!(stored["error_ref"], "lease_exhausted");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: deterministic proof that a `CasWorkItemMetadata` CONFLICT is a
    /// real, distinct outcome, not a silent overwrite. Two "contenders" race
    /// for the same field the same way ANY real race would -- both derive
    /// their request from the SAME pre-claim read (`expected_checkpoint_id:
    /// None`) -- but the race is constructed DETERMINISTICALLY (two
    /// sequential `commit_at` calls against one synchronous db, never a
    /// spawned/sleeping thread; GOC-70) rather than hoped into existence.
    /// The winner's `commit_at` call happens-before the loser's by
    /// construction, so this is not a flaky "usually the first one wins" --
    /// it is the exact same interleaving on every run.
    #[test]
    fn cas_work_item_metadata_deterministic_conflict_never_silently_overwrites() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-conflict");
        let db = open(&path);

        let mut seed = batch("cas-metadata-seed", "cas-metadata-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("cas-a", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("CAS seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let claim = commit_native_claim(
            &db,
            "cas-metadata-claim",
            "cas-metadata-claim-key",
            4,
            Some("cas-a"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claim.claimed);
        let lease = CasWorkItemMetadataLeaseFence {
            worker_ref: claim.lease_holder_ref.clone().unwrap(),
            lease_epoch: claim.lease_epoch.unwrap(),
            fencing_token: claim.fencing_token.unwrap(),
        };

        let cas_request = |expected_checkpoint_id: Option<&str>,
                           set_checkpoint_id: &str,
                           expected_graph_version: u64| {
            let mut op = batch(
                &format!("cas-metadata-{set_checkpoint_id}"),
                &format!("cas-metadata-{set_checkpoint_id}-key"),
            );
            op.version_expectation = VersionExpectation::Graph(expected_graph_version);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-a".into(),
                        expected_lease: Some(lease.clone()),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: expected_checkpoint_id.map(str::to_string),
                        set_checkpoint_id: Some(set_checkpoint_id.to_string()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("CAS request fixture reseals its final body");
            op
        };

        let decode_result = |committed: &MutationBatchCommit| -> CasWorkItemMetadataResult {
            let payload: crate::protocol::ResultPayload = decode_durable(
                committed
                    .record
                    .result_msgpack
                    .as_deref()
                    .expect("cas result"),
            )
            .unwrap();
            let bytes = match payload {
                crate::protocol::ResultPayload::Raw(inner) => inner,
                other => {
                    panic!(
                        "CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}"
                    )
                }
            };
            decode_durable(&bytes).unwrap()
        };

        // Contender A: reads checkpoint_id == None, wins.
        let winner = commit_at(&db, &cas_request(None, "checkpoint:1", 5), None).unwrap();
        let winner_result = decode_result(&winner);
        assert_eq!(winner_result.outcome, CasWorkItemMetadataOutcome::Applied);
        assert_eq!(
            winner_result.changed_work_item_ids,
            vec!["cas-a".to_string()]
        );

        // Contender B: derived its request from the SAME pre-claim read
        // (checkpoint_id == None) -- now stale, because A already committed.
        // It must be told CONFLICT, distinctly from both Applied and NotFound.
        let loser = commit_at(&db, &cas_request(None, "checkpoint:2", 6), None).unwrap();
        let loser_result = decode_result(&loser);
        assert_eq!(loser_result.outcome, CasWorkItemMetadataOutcome::Conflict);
        assert_eq!(loser_result.changed_work_item_ids, Vec::<String>::new());

        // The loser's write never landed: the durable row still carries the
        // WINNER's value, not the loser's, and not some third corrupted value.
        let stored = read_one_node(&db, "graph-a", "cas-a", DurableCrypto::none())
            .unwrap()
            .expect("cas-a remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["checkpoint_id"], "checkpoint:1");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// GOC-19/BUG-111: the SAME no-silent-overwrite property proven
    /// deterministically above, but under REAL concurrency -- two genuine OS
    /// threads, synchronized to start racing at the same instant via a
    /// `Barrier` (GOC-70 rule 3: construct the pile-up deterministically
    /// through a barrier, never a sleep-based hope), both submitting a
    /// `CasWorkItemMetadata` commit derived from the identical pre-race
    /// state, exactly like `mutation_batch_same_attempt_race_has_one_
    /// durable_winner_and_replay` in `resource_reservation_tests.rs`. This
    /// exercises the ACTUAL storage-layer mutual exclusion -- redb's
    /// exclusive write transaction plus the in-transaction
    /// `expected_graph_version`/lease/status checks -- rather than a
    /// hand-simulated interleaving, proving the guard is atomic at the
    /// storage layer and not a read-then-write race.
    #[test]
    fn cas_work_item_metadata_real_concurrent_race_has_exactly_one_winner() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-concurrent");
        let db = std::sync::Arc::new(open(&path));

        let mut seed = batch(
            "cas-metadata-concurrent-seed",
            "cas-metadata-concurrent-seed-key",
        );
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("cas-race", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("concurrent CAS seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let claim = commit_native_claim(
            &db,
            "cas-metadata-concurrent-claim",
            "cas-metadata-concurrent-claim-key",
            4,
            Some("cas-race"),
            "worker-race",
            0,
            60_000,
            64,
        );
        assert!(
            claim.claimed,
            "setup: claim must win to reach the claimed state"
        );
        let lease = CasWorkItemMetadataLeaseFence {
            worker_ref: claim.lease_holder_ref.clone().unwrap(),
            lease_epoch: claim.lease_epoch.unwrap(),
            fencing_token: claim.fencing_token.unwrap(),
        };

        let make_request = |label: &str, set_checkpoint_id: &str| {
            let mut op = batch(
                &format!("cas-metadata-concurrent-{label}"),
                &format!("cas-metadata-concurrent-{label}-key"),
            );
            // Both racers derive from the SAME pre-race graph version (5,
            // the version immediately after the claim above) -- exactly
            // what two real callers who both read state before either wrote
            // would carry. Only the transaction that actually lands first
            // can have this match; the other's `expected_graph_version`
            // is stale by construction, not by chance.
            op.version_expectation = VersionExpectation::Graph(5);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-race".into(),
                        expected_lease: Some(lease.clone()),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: None,
                        set_checkpoint_id: Some(set_checkpoint_id.to_string()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("concurrent CAS request fixture reseals its final body");
            op
        };

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for (label, checkpoint) in [("a", "race:A"), ("b", "race:B")] {
            let db = db.clone();
            let barrier = barrier.clone();
            let request = make_request(label, checkpoint);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                commit_at(&db, &request, None)
            }));
        }
        let results: Vec<Result<MutationBatchCommit, String>> = handles
            .into_iter()
            .map(|handle| handle.join().expect("concurrent CAS worker"))
            .collect();

        let decode_result = |committed: &MutationBatchCommit| -> CasWorkItemMetadataResult {
            let payload: crate::protocol::ResultPayload = decode_durable(
                committed
                    .record
                    .result_msgpack
                    .as_deref()
                    .expect("cas result"),
            )
            .unwrap();
            let bytes = match payload {
                crate::protocol::ResultPayload::Raw(inner) => inner,
                other => {
                    panic!(
                        "CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}"
                    )
                }
            };
            decode_durable(&bytes).unwrap()
        };

        let applied_count = results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .map(|committed| {
                        decode_result(committed).outcome == CasWorkItemMetadataOutcome::Applied
                    })
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            applied_count, 1,
            "exactly one racing thread's CAS must apply under real concurrency, never zero \
             (lost write) and never two (silent double-apply): {results:?}"
        );

        let loser_explicitly_rejected = results.iter().any(|result| match result {
            Err(message) => message.contains("STALE_VERSION"),
            Ok(committed) => {
                decode_result(committed).outcome == CasWorkItemMetadataOutcome::Conflict
            }
        });
        assert!(
            loser_explicitly_rejected,
            "the losing thread must receive an explicit, distinct rejection (STALE_VERSION at \
             the batch envelope or Conflict from the CAS handler itself) -- never silently \
             dropped, never silently merged with the winner: {results:?}"
        );

        // The durable row reflects EXACTLY the winner's write -- never both,
        // never neither, never a corrupted mix of the two.
        let stored = read_one_node(&db, "graph-a", "cas-race", DurableCrypto::none())
            .unwrap()
            .expect("cas-race remains durable");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        let checkpoint = stored["checkpoint_id"].as_str().unwrap();
        assert!(
            checkpoint == "race:A" || checkpoint == "race:B",
            "stored checkpoint must be exactly one racer's value, got {checkpoint:?}"
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: a WorkItem row that does not exist is `not_found`, a THIRD
    /// distinct outcome from `applied`/`conflict` -- never collapsed into
    /// either.
    #[test]
    fn cas_work_item_metadata_missing_row_is_not_found() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
        };

        let path = temp_path("cas-metadata-missing");
        let db = open(&path);

        let mut op = batch("cas-metadata-missing", "cas-metadata-missing-key");
        op.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: Method::CasWorkItemMetadata {
                request: CasWorkItemMetadataRequest {
                    schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                    tenant_ref: "tenant-a".into(),
                    work_item_id: "does-not-exist".into(),
                    expected_lease: None,
                    expected_status: vec!["leased".into(), "running".into()],
                    expected_checkpoint_id: None,
                    set_checkpoint_id: Some("checkpoint:1".into()),
                    expected_metadata_msgpack: None,
                    set_metadata_msgpack: None,
                    expected_prio_bucket: None,
                    set_prio_bucket: None,
                    now_ms: 1_000,
                },
            },
        }];
        op.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("missing CAS fixture reseals its final body");
        let committed = commit_at(&db, &op, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("cas result"),
        )
        .unwrap();
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner) => inner,
            other => {
                panic!("CasWorkItemMetadata must return a bin-encoded typed result, got {other:?}")
            }
        };
        let result: CasWorkItemMetadataResult = decode_durable(&bytes).unwrap();
        assert_eq!(result.outcome, CasWorkItemMetadataOutcome::NotFound);
        assert_eq!(result.changed_work_item_ids, Vec::<String>::new());

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// BUG-111: an ACKED CAS survives a restart. The db handle standing in
    /// for the engine process is dropped and the SAME on-disk file reopened
    /// (exactly `last_permitted_reclaim_survives_restart_but_the_next_reclaim_
    /// dead_letters`'s established restart idiom) -- the durable redb commit
    /// already fsync'd before this test ever saw the "applied" result, so if
    /// the RPC used a side path instead of the same durable WorkItem
    /// transaction, this is where it would show up as a lost write.
    #[test]
    fn cas_work_item_metadata_applied_write_survives_restart() {
        use crate::epistemic_operations_ext::{
            CasWorkItemMetadataLeaseFence, CasWorkItemMetadataRequest,
            CasWorkItemMetadataRequestSchemaVersion,
        };

        let path = temp_path("cas-metadata-restart");
        {
            let db = open(&path);
            let mut seed = batch("cas-metadata-restart-seed", "cas-metadata-restart-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: ready_work_item_method("cas-restart", 3),
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("restart CAS seed fixture reseals its final body");
            commit_at(&db, &seed, None).unwrap();

            let claim = commit_native_claim(
                &db,
                "cas-metadata-restart-claim",
                "cas-metadata-restart-claim-key",
                4,
                Some("cas-restart"),
                "worker-a",
                0,
                60_000,
                64,
            );
            assert!(claim.claimed);

            let mut apply = batch(
                "cas-metadata-restart-apply",
                "cas-metadata-restart-apply-key",
            );
            apply.version_expectation = VersionExpectation::Graph(5);
            apply.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::CasWorkItemMetadata {
                    request: CasWorkItemMetadataRequest {
                        schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: "cas-restart".into(),
                        expected_lease: Some(CasWorkItemMetadataLeaseFence {
                            worker_ref: claim.lease_holder_ref.clone().unwrap(),
                            lease_epoch: claim.lease_epoch.unwrap(),
                            fencing_token: claim.fencing_token.unwrap(),
                        }),
                        expected_status: vec!["leased".into(), "running".into()],
                        expected_checkpoint_id: None,
                        set_checkpoint_id: Some("checkpoint:durable".into()),
                        expected_metadata_msgpack: None,
                        set_metadata_msgpack: None,
                        expected_prio_bucket: None,
                        set_prio_bucket: None,
                        now_ms: 1_000,
                    },
                },
            }];
            apply
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("restart CAS fixture reseals its final body");
            commit_at(&db, &apply, None).unwrap();
            drop(db);
        }

        // Reopen the SAME on-disk file as a fresh handle -- standing in for
        // a process restart. Nothing from the dropped `db`'s in-memory state
        // can leak forward; only what actually committed to disk is here.
        let db = open(&path);
        let stored = read_one_node(&db, "graph-a", "cas-restart", DurableCrypto::none())
            .unwrap()
            .expect("cas-restart remains durable across restart");
        let stored: serde_json::Value = decode_durable(&stored).unwrap();
        assert_eq!(stored["checkpoint_id"], "checkpoint:durable");
        assert_eq!(stored["status"], "leased");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    /// ADR-5 / W2.2 acceptance: a `kill -9` mid-transition resumes correctly. The
    /// WorkItem lifecycle `status` and its co-located statechart MIRROR (`machine_state`)
    /// are one row written in ONE redb write transaction, so a crash BEFORE the commit
    /// rolls both back and a crash AFTER the commit lands both — they can never split.
    #[cfg(feature = "statechart")]
    #[test]
    fn work_item_status_and_statechart_mirror_commit_atomically_across_kill9() {
        use crate::epistemic_operations::{
            ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion,
        };

        let seed_ready = |shard: &Shard| {
            let mut seed = batch("wi-seed", "wi-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "wi".into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "node_type": "WorkItem",
                        "tenant": "tenant-a",
                        "status": "ready",
                    }))
                    .unwrap(),
                },
            }];
            seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("work-item seed fixture reseals its final body");
            commit_at(shard, &seed, None).unwrap();
        };

        let claim_batch = || {
            let mut claim = batch("wi-claim", "wi-claim-key");
            claim.version_expectation = VersionExpectation::Graph(4);
            claim.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::ClaimWorkItem {
                    request: ClaimWorkItemRequest {
                        schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".into(),
                        work_item_id: Some("wi".into()),
                        queue_ref: None,
                        resource_class: None,
                        fairness_group: None,
                        worker_ref: "worker-a".into(),
                        now_ms: 1_000,
                        lease_ms: 10_000,
                        max_tenant_in_flight: 64,
                    },
                },
            }];
            claim
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("work-item claim fixture reseals its final body");
            claim
        };

        let read_pair = |shard: &Shard| -> (Option<String>, Option<String>) {
            match read_one_node(shard, "graph-a", "wi", DurableCrypto::none()).unwrap() {
                None => (None, None),
                Some(b) => {
                    let props: serde_json::Map<String, serde_json::Value> =
                        decode_durable(&b).unwrap();
                    let get = |k: &str| props.get(k).and_then(|v| v.as_str()).map(str::to_string);
                    (get("status"), get("machine_state"))
                }
            }
        };

        // Scenario A — crash BEFORE the redb commit: NEITHER status nor its mirror persist.
        {
            let path = temp_path("wi-kill9-precommit");
            {
                let db = open(&path);
                seed_ready(&db);
                assert!(commit_at(
                    &db,
                    &claim_batch(),
                    Some(MutationBatchCrashpoint::BeforeCommit)
                )
                .is_err());
            }
            let db = open(&path);
            let (status, machine) = read_pair(&db);
            assert_eq!(status.as_deref(), Some("ready"), "status must not advance");
            assert_eq!(machine, None, "the mirror must not advance either");
            let _ = std::fs::remove_file(path);
        }

        // Scenario B — crash AFTER the redb commit (before ack): BOTH status AND its
        // mirror are already durably on disk, together.
        {
            let path = temp_path("wi-kill9-postcommit");
            {
                let db = open(&path);
                seed_ready(&db);
                assert!(commit_at(
                    &db,
                    &claim_batch(),
                    Some(MutationBatchCrashpoint::AfterCommitBeforeAck)
                )
                .is_err());
            }
            let db = open(&path);
            let (status, machine) = read_pair(&db);
            assert_eq!(
                status.as_deref(),
                Some("leased"),
                "status committed durably"
            );
            assert_eq!(
                machine.as_deref(),
                Some("leased"),
                "mirror committed durably and atomically with status"
            );
            let _ = std::fs::remove_file(path);
        }
    }

    /// ADR-5 / W2.2 acceptance: the dual-write divergence alarm fires on an induced
    /// divergence. Drives the redb integration point (`apply_work_item_mirror`) with an
    /// authoritative next state the chart would never compute, and asserts the
    /// `epistemic_graph_statechart_divergence_total` counter increments while the agreeing
    /// case does not.
    #[cfg(all(feature = "statechart", feature = "metrics"))]
    #[test]
    fn work_item_mirror_divergence_raises_the_alarm() {
        fn divergence_count() -> u64 {
            for line in crate::metrics::render().lines() {
                if line.starts_with(
                    "epistemic_graph_statechart_divergence_total{machine=\"work_item\"}",
                ) {
                    return line
                        .rsplit(' ')
                        .next()
                        .and_then(|v| v.parse::<f64>().ok())
                        .map(|f| f as u64)
                        .unwrap_or(0);
                }
            }
            0
        }

        // Induced divergence: `ready --claim-->` the chart decides `leased`, but the
        // (hypothetically buggy) authority claims it landed `succeeded`.
        let before = divergence_count();
        let mut props = serde_json::Map::new();
        apply_work_item_mirror(
            &mut props,
            "wi",
            "ready",
            crate::work_item_statechart::EV_CLAIM,
            serde_json::json!({}),
            Some("succeeded"),
        );
        assert_eq!(
            divergence_count(),
            before + 1,
            "an induced divergence must increment the alarm counter"
        );
        // The mirror still records ITS OWN decision, so the divergence is queryable at rest.
        assert_eq!(
            props.get("machine_state").and_then(|v| v.as_str()),
            Some("leased")
        );

        // The agreeing case does NOT alarm.
        let steady = divergence_count();
        let mut props2 = serde_json::Map::new();
        apply_work_item_mirror(
            &mut props2,
            "wi2",
            "ready",
            crate::work_item_statechart::EV_CLAIM,
            serde_json::json!({}),
            Some("leased"),
        );
        assert_eq!(divergence_count(), steady, "agreement must not alarm");
        assert_eq!(
            props2.get("machine_state").and_then(|v| v.as_str()),
            Some("leased")
        );
    }

    #[test]
    fn crossmodal_batch_recovers_rows_status_vector_and_outbox_together() {
        let path = temp_path("crossmodal-postcommit");
        let mut mutation = batch("batch-crossmodal", "idem-crossmodal");
        let methods = mutation
            .operations
            .iter()
            .map(|operation| operation.method.clone())
            .collect::<Vec<_>>();
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::CrossModal,
            method: Method::ApplyMutation {
                event_type: "crossmodal_operation".to_string(),
                query: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            },
        }];
        let vectors = vec![("a".to_string(), vec![0.25, 0.75])];
        {
            let db = open(&path);
            assert!(commit_crossmodal_at(
                &db,
                &mutation,
                &methods,
                &vectors,
                Some(MutationBatchCrashpoint::AfterCommitBeforeAck),
            )
            .is_err());
        }
        {
            let db = open(&path);
            let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap();
            let semantic: crate::compute::semantic::SemanticStore =
                rmp_serde::from_slice(&dump.semantic).unwrap();
            assert_eq!(semantic.get_embedding("a"), Some(vec![0.25, 0.75]));
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", "batch-crossmodal")
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", "batch-crossmodal")
                    .unwrap()
                    .len(),
                2,
            );
            let replay = commit_crossmodal_at(&db, &mutation, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.version_expectation,
                VersionExpectation::Graph(3),
                "replay must retain the original OCC observation in the durable identity"
            );

            // A retry reconstructed after the acknowledgement-lost crash may
            // carry the now-current graph version.  It is still the same
            // cross-modal request and must replay without applying rows again.
            let mut rederived = mutation.clone();
            rederived.version_expectation = VersionExpectation::Graph(4);
            let replay = commit_crossmodal_at(&db, &rederived, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.version_expectation,
                VersionExpectation::Graph(3),
                "a derived retry version must never overwrite the original durable version"
            );

            // The expected version is the only re-derived field permitted for
            // this replay shape.  Changing the operation under the same key is
            // a genuine idempotency conflict.
            let mut conflict = rederived.clone();
            conflict.operations[0].method = Method::ApplyMutation {
                event_type: "crossmodal_operation".to_string(),
                query: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            };
            let error = commit_crossmodal_at(&db, &conflict, &methods, &vectors, None).unwrap_err();
            assert!(error.contains("IDEMPOTENCY_CONFLICT"));
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn outbox_claim_ack_is_ordered_fenced_and_reconcilable() {
        let path = temp_path("outbox-lease");
        let db = open(&path);
        let mutation = batch("batch-outbox", "idem-outbox");
        commit_at(&db, &mutation, None).unwrap();
        let mut middle = batch("batch-outbox-middle", "idem-outbox-middle");
        middle.version_expectation = VersionExpectation::Graph(4);
        commit_at(&db, &middle, None).unwrap();
        let mut tail = batch("batch-outbox-tail", "idem-outbox-tail");
        tail.version_expectation = VersionExpectation::Graph(5);
        commit_at(&db, &tail, None).unwrap();
        for batch_id in ["batch-outbox", "batch-outbox-middle", "batch-outbox-tail"] {
            assert_eq!(
                read_mutation_outbox(&db, "graph-a", batch_id)
                    .unwrap()
                    .len(),
                1,
                "one explicit logical intent writes one physical outbox row"
            );
        }
        db.outbox_subscribe("graph-a", "projection-worker", "projection.test")
            .unwrap();

        let mut budget = OutboxClaimBudget::new(10, 100, 1_000).unwrap();
        let outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let leases = outcome.claims;
        assert_eq!(leases.len(), 3);
        let gap = db.outbox_ack("graph-a", &leases[1], 1_001).unwrap_err();
        assert!(gap.contains("OUTBOX_ORDER_GAP"));

        for lease in &leases {
            db.outbox_ack("graph-a", lease, 1_001).unwrap();
        }
        let mut budget = OutboxClaimBudget::new(10, 100, 2_000).unwrap();
        let outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        assert!(outcome.claims.is_empty());
        let cursor = db
            .outbox_cursor("graph-a", "projection-worker")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.batch_id, "batch-outbox-tail");
        assert_eq!(cursor.outbox_ordinal, 0);
        assert_eq!(cursor.schema_version, MUTATION_BATCH_VERSION);
        assert_eq!(
            cursor.committed_version,
            CommittedVersion::Graph {
                source: 5,
                target: 6
            }
        );

        let mut next = batch("batch-outbox-next", "idem-outbox-next");
        next.version_expectation = VersionExpectation::Graph(6);
        commit_at(&db, &next, None).unwrap();
        let mut budget = OutboxClaimBudget::new(10, 100, 2_100).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "projection-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let next_lease = outcome.claims.remove(0);
        let advanced = db.outbox_ack("graph-a", &next_lease, 2_101).unwrap();
        assert_eq!(
            advanced.committed_version,
            CommittedVersion::Graph {
                source: 6,
                target: 7
            }
        );
        assert!(db
            .outbox_ack("graph-a", &leases[2], 2_102)
            .unwrap_err()
            .contains("STALE_OUTBOX_LEASE"));

        db.outbox_subscribe("graph-a", "lease-fence-worker", "projection.test")
            .unwrap();
        let mut budget = OutboxClaimBudget::new(1, 10, 3_000).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "lease-fence-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let first = outcome.claims.remove(0);
        let mut budget = OutboxClaimBudget::new(1, 10, 3_011).unwrap();
        let mut outcome = db
            .outbox_claim("graph-a", "lease-fence-worker", &mut budget)
            .unwrap();
        assert_eq!(outcome.deferred, None);
        let replacement = outcome.claims.remove(0);
        assert!(replacement.lease_epoch > first.lease_epoch);
        assert!(db
            .outbox_ack("graph-a", &first, 3_012)
            .unwrap_err()
            .contains("STALE_OUTBOX_LEASE"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn staged_state_commit_replaces_rows_and_replays_without_reexecution() {
        use sha2::{Digest, Sha256};

        let path = temp_path("authoritative-state");
        let db = open(&path);
        let staged = crate::graph::GraphCore::new();
        staged.add_node(
            "replacement".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 7})).unwrap(),
        );
        let state = staged.snapshot().to_msgpack().unwrap();
        let mut mutation = batch("batch-state", "idem-state");
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::ApplyMutation {
                event_type: "authoritative_state_operation".to_string(),
                query: "sha256:opaque".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: "sha256".to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 3,
            target_graph_version: 4,
        });
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        let committed = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();
        assert!(!committed.replayed);
        assert!(
            read_one_node(&db, "graph-a", "replacement", DurableCrypto::none())
                .unwrap()
                .is_some()
        );
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);

        let consumed_nonce = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap_err();
        assert!(
            consumed_nonce.contains("REPLAY_NONCE_CONSUMED"),
            "{consumed_nonce}"
        );

        let mut retry = batch("batch-state", "idem-state");
        retry.operations = mutation.operations.clone();
        retry.authoritative_state = mutation.authoritative_state.clone();
        retry
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        assert_ne!(
            retry
                .envelope
                .operation()
                .expect("retry operation envelope")
                .authority
                .nonce,
            mutation
                .envelope
                .operation()
                .expect("original operation envelope")
                .authority
                .nonce,
            "a retry must use a fresh attempt nonce"
        );

        let replay = commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &retry,
                authoritative_state_msgpack: &state,
                result_msgpack: Some(&[0x81, 0xa2, b'o', b'k']),
                committed_at_ms: 101,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();
        assert!(replay.replayed);
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        assert!(
            read_one_node(&db, "graph-a", "replacement", DurableCrypto::none())
                .unwrap()
                .is_some()
        );
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn staged_row_delta_updates_only_affected_durable_rows() {
        use sha2::{Digest, Sha256};

        let path = temp_path("authoritative-row-delta");
        let db = open(&path);
        let initial = batch("batch-row-delta-base", "idem-row-delta-base");
        commit_at(&db, &initial, None).unwrap();

        let before = crate::graph::GraphCore::new();
        before.add_node(
            "a".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 1})).unwrap(),
        );
        before.add_node(
            "b".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 2})).unwrap(),
        );
        before.clear_ledger();
        let before_snapshot = before.snapshot();
        let after = crate::graph::GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.add_node(
            "a".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"value": 9})).unwrap(),
        );
        after
            .add_edge(
                "a".to_string(),
                "b".to_string(),
                rmp_serde::to_vec_named(&serde_json::json!({"kind": "new"})).unwrap(),
            )
            .unwrap();
        after
            .semantic_store
            .write()
            .add_embedding("a".to_string(), vec![0.25, 0.75])
            .unwrap();
        after.set_integrity_policy(crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        });
        let delta = crate::graph_delta::GraphRowDelta::between(&before_snapshot, &after.snapshot())
            .unwrap();
        let state = delta.to_msgpack().unwrap();

        let mut mutation = batch("batch-row-delta", "idem-row-delta");
        mutation.version_expectation = VersionExpectation::Graph(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::ApplyMutation {
                event_type: "authoritative_state_operation".to_string(),
                query: "sha256-row-delta-v2:opaque".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 4,
            target_graph_version: 5,
        });
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("row delta fixture reseals its final body");
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_state(
            &db,
            StateCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                authoritative_state_msgpack: &state,
                result_msgpack: None,
                committed_at_ms: 102,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .unwrap();

        let a: serde_json::Value = decode_durable(
            &read_one_node(&db, "graph-a", "a", DurableCrypto::none())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(a["value"], 9);
        let b: serde_json::Value = decode_durable(
            &read_one_node(&db, "graph-a", "b", DurableCrypto::none())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(b["value"], 2, "the untouched row must survive");
        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert_eq!(dump.edges.len(), 1);
        assert_eq!(dump.ledger, after.snapshot().ledger);
        let semantic: crate::compute::semantic::SemanticStore =
            decode_durable(&dump.semantic).unwrap();
        assert_eq!(
            semantic.embeddings_snapshot(),
            vec![("a".to_string(), vec![0.25, 0.75])]
        );
        assert_eq!(dump.source_snapshot_version, 5);
        assert_eq!(dump.integrity_policy, after.integrity_policy());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_row_delta_commit_does_not_publish_integrity_policy() {
        use sha2::{Digest, Sha256};

        let path = temp_path("integrity-policy-rollback");
        let db = open(&path);
        commit_at(&db, &batch("batch-policy-base", "idem-policy-base"), None).unwrap();

        let before = crate::graph::GraphCore::new();
        let before_snapshot = before.snapshot();
        let after = crate::graph::GraphCore::from_snapshot(before_snapshot.clone(), 0).unwrap();
        after.set_integrity_policy(crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        });
        let delta = crate::graph_delta::GraphRowDelta::between(&before_snapshot, &after.snapshot())
            .unwrap();
        let state = delta.to_msgpack().unwrap();
        let mut mutation = batch("batch-policy-fail", "idem-policy-fail");
        mutation.version_expectation = VersionExpectation::Graph(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphSnapshot,
            method: Method::IcvConfigure {
                graph: Some("graph-a".to_string()),
                mode: "enforce".to_string(),
                shapes: "sha256:policy-receipt".to_string(),
            },
        }];
        mutation.authoritative_state = Some(crate::mutation_batch::MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(&state)),
            source_graph_version: 4,
            target_graph_version: 5,
        });
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        assert!(commit_mutation_batch_inner(
            &db,
            BatchCommitInput {
                graph_fname: "graph-a",
                batch: &mutation,
                change: None,
                authoritative_state_msgpack: Some(&state),
                crossmodal: None,
                result_msgpack: None,
                committed_at_ms: 103,
                audited: true,
                crashpoint: Some(MutationBatchCrashpoint::BeforeCommit),
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
        .is_err());

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert!(dump.integrity_policy.is_none());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn idempotency_key_reuse_for_different_work_fails_closed() {
        let path = temp_path("conflict");
        let db = open(&path);
        let first = batch("batch-one", "same-key");
        commit_at(&db, &first, None).unwrap();
        let mut conflicting = batch("batch-two", "same-key");
        conflicting.operations[0].method = node("different", 99);
        conflicting
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let err = commit_at(&db, &conflicting, None).unwrap_err();
        assert!(err.contains("IDEMPOTENCY_CONFLICT"));
        assert!(
            read_one_node(&db, "graph-a", "different", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn batch_id_reuse_with_a_fresh_key_fails_closed() {
        let path = temp_path("batch-id-conflict");
        let db = open(&path);
        let first = batch("same-batch", "first-key");
        commit_at(&db, &first, None).unwrap();
        let mut conflicting = batch("same-batch", "fresh-key");
        conflicting.operations[0].method = node("different", 99);
        conflicting
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let err = commit_at(&db, &conflicting, None).unwrap_err();
        assert!(err.contains("IDEMPOTENCY_CONFLICT"));
        assert!(
            read_one_node(&db, "graph-a", "different", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lifecycle_adapter_commits_meta_and_delete_before_registry_publication() {
        let path = temp_path("lifecycle");
        let db = open(&path);
        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("lifecycle create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();
        let meta = read_all_graph_meta(&db).unwrap();
        assert!(meta
            .iter()
            .any(|(fname, name, graph_type, incarnation_id)| {
                fname == "graph-a"
                    && name == "graph-a"
                    && *graph_type == GraphType::Agent
                    && incarnation_id == "create-graph-a"
            }));

        let mut delete = batch("delete-graph-a", "delete-key");
        delete.version_expectation = VersionExpectation::Graph(4);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        delete
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("lifecycle delete fixture reseals its final body");
        commit_at(&db, &delete, None).unwrap();
        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert_eq!(
            read_mutation_batch_for_graph(&db, "graph-a", "delete-graph-a")
                .unwrap()
                .unwrap()
                .status,
            MutationBatchStatus::Committed
        );
        drop(db);
        let db = reopen(&path);
        // Reopening and binding the same name yields a fresh scope after the
        // delete retired its prior identity; the stale batch below must fail
        // against that new authoritative version before metadata can return.
        // The old incarnation's scope identity cannot be re-admitted after the
        // delete, so this retry must fail closed before metadata is recreated.
        let stale = commit_at(&db, &create, None).unwrap_err();
        assert!(stale.contains("STALE_VERSION"), "got: {stale}");
        assert!(
            read_all_graph_meta(&db).unwrap().is_empty(),
            "retrying the old Create must not resurrect graph metadata after Delete"
        );
        let _ = std::fs::remove_file(path);
    }

    /// D-P0-U04 regression: `Method::DeleteGraph` must atomically remove the
    /// PRIOR incarnation's mutation-authority rows (idempotency replay keys,
    /// `MUTATION_BATCHES`/`MUTATION_OUTBOX` records) -- not only graph/change/
    /// resource/lane rows -- so a same-name recreate never collides with or
    /// attempts to decrypt an old-incarnation mutation record. Fails before
    /// the `clear_mutation_authority_rows` call was wired into `DeleteGraph`'s
    /// commit path (the idempotency/outbox rows below survived the delete);
    /// passes after.
    #[test]
    fn delete_graph_purges_prior_incarnation_mutation_authority() {
        let path = temp_path("mutation-authority-purge");
        let db = open(&path);

        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();

        // An ORDINARY (non-lifecycle) content mutation against the live graph --
        // this is what stamps MUTATION_IDEMPOTENCY/MUTATION_BATCHES/MUTATION_OUTBOX
        // for the incarnation being deleted below.
        let mut content = batch("content-batch-1", "content-key-1");
        content.version_expectation = VersionExpectation::Graph(4);
        content
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge content fixture reseals its final body");
        commit_at(&db, &content, None).unwrap();

        // Prove the prior incarnation's kernel ledger and outbox state is
        // actually present before delete, so the purge assertion is not
        // vacuous.
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_some()
        );
        assert!(!read_mutation_outbox(&db, "graph-a", "content-batch-1")
            .unwrap()
            .is_empty());

        let mut delete = batch("delete-graph-a", "delete-key");
        delete.version_expectation = VersionExpectation::Graph(5);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        delete
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority purge delete fixture reseals its final body");
        commit_at(&db, &delete, None).unwrap();

        // The PRIOR incarnation's kernel ledger and outbox rows must be gone.
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_none(),
            "DeleteGraph must retire the prior incarnation's receipt"
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_empty(),
            "DeleteGraph must retire the prior incarnation's outbox"
        );

        // A recreate under the SAME name reusing the SAME idempotency key must
        // be treated as fresh work, not resolved as a replay of the deleted
        // incarnation's stale batch_id.
        // A retired scope has no surviving version authority.  Recreate binds a
        // new incarnation at version zero, and its first content commit advances
        // that new scope to one.
        let mut recreate = batch("create-graph-a-v2", "create-key-v2");
        recreate.version_expectation = VersionExpectation::Graph(0);
        recreate.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        recreate
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority recreate fixture reseals its final body");
        commit_at(&db, &recreate, None).unwrap();
        let mut content_v2 = batch("content-batch-1-v2", "content-key-1");
        content_v2.version_expectation = VersionExpectation::Graph(1);
        content_v2
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("authority recreate content fixture reseals its final body");
        commit_at(&db, &content_v2, None).unwrap();
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1-v2")
                .unwrap()
                .is_some(),
            "the recreated scope must accept fresh work after prior retirement"
        );

        let _ = std::fs::remove_file(path);
    }

    /// The embedded/legacy whole-graph purge seam must remove the COMPLETE
    /// lifecycle-owned authority surface, not only graph rows and graph_meta.
    /// This is deliberately separate from
    /// `delete_graph_purges_prior_incarnation_mutation_authority`: the
    /// canonical MutationBatch DeleteGraph path already exercises its own
    /// in-transaction cleanup, while `purge_graph_rows` is the path used by
    /// `EmbeddedEngine::delete_graph` and the persistence `PurgeGraph` command.
    #[test]
    fn purge_graph_rows_removes_all_lifecycle_owned_mutation_state() {
        let path = temp_path("whole-graph-authority-purge");
        let db = open(&path);

        let mut create = batch("create-graph-a", "create-key");
        create.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: DurabilityDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        create
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("whole purge create fixture reseals its final body");
        commit_at(&db, &create, None).unwrap();

        // The ordinary mutation seeds an independent idempotency/batch/outbox
        // row for the incarnation being purged. The lifecycle batch above also
        // seeds the graph version, fence, and lifecycle-head rows.
        let mut content = batch("content-batch-1", "content-key-1");
        content.version_expectation = VersionExpectation::Graph(4);
        content
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("whole purge content fixture reseals its final body");
        commit_at(&db, &content, None).unwrap();

        // The helper is the shared durable whole-graph purge used by both the
        // embedded engine and the persistence writer's PurgeGraph command.
        purge_graph_rows(&db, "graph-a").unwrap();

        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "content-batch-1")
                .unwrap()
                .is_none()
        );
        assert!(read_mutation_outbox(&db, "graph-a", "content-batch-1")
            .unwrap()
            .is_empty());
        for retired in [
            "mutation_batches",
            "mutation_idempotency",
            "mutation_outbox",
            "mutation_lifecycle_head",
            "mutation_graph_version",
            "mutation_fence",
            "mutation_outbox_delivery",
            "mutation_projection_cursor",
        ] {
            assert!(
                !eg_storage::owner_table_names(eg_storage::OwnerLayout::GraphShard)
                    .contains(&retired),
                "retired private table remains declared: {retired}"
            );
        }

        // The deletion is durable, not merely visible in the write
        // transaction that performed it.
        drop(db);
        // Reopening must not resurrect the retired receipt or catalog row.
        let reopened = reopen(&path);
        assert!(read_all_graph_meta(&reopened).unwrap().is_empty());
        assert!(
            read_mutation_batch_for_graph(&reopened, "graph-a", "content-batch-1")
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&reopened, "graph-a", "content-batch-1")
                .unwrap()
                .is_empty()
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    fn governed_envelope_for_tenant(
        tenant: &str,
        batch_id: &str,
        key: &str,
        sequence: u64,
        expected_graph_version: u64,
        envelope_id: &str,
    ) -> ChangeEnvelope {
        let mut mutation = batch(batch_id, key);
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new(tenant).unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:redb-store").unwrap(),
        );
        let actor = format!("principal:sha256:{}", "a".repeat(64));
        mutation.identity = identity.clone();
        mutation.envelope = super::fixture_operation_envelope(&identity, &actor, 42, key);
        mutation.version_expectation = VersionExpectation::Graph(expected_graph_version);
        mutation.operations.truncate(1);
        mutation.outbox[0].payload = rmp_serde::to_vec_named(&serde_json::json!({
            "event": "projection.test"
        }))
        .unwrap();
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let digest = if sequence == 1 { "a" } else { "b" }.repeat(64);
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: envelope_id.to_string(),
            mutation,
            content_version: ContentVersion {
                object_id: "object-1".to_string(),
                digest_algorithm: "sha256".to_string(),
                digest,
                previous_digest: (sequence > 1).then(|| "a".repeat(64)),
                source_version: ContentVersionPosition::Sequence(sequence),
            },
            cursor: Some(ChangeCursor {
                source: "fixture-source".to_string(),
                partition: "partition-1".to_string(),
                position: CursorPosition::Sequence(sequence),
                expected_previous: (sequence > 1).then_some(CursorPosition::Sequence(sequence - 1)),
            }),
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            policies: vec![PolicyRecord {
                policy_id: "policy-object-1".to_string(),
                operation: MaterialOperation::Upsert,
                object_id: "object-1".to_string(),
                tenant: tenant.to_string(),
                classification: "internal".to_string(),
                policy_version: "policy-v1".to_string(),
                subject_set_digest: "c".repeat(64),
                retention_policy: "standard".to_string(),
                legal_hold: false,
            }],
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".to_string(),
                sanitizer_version: "sanitizer-v1".to_string(),
                sanitized_payload_digest: "d".repeat(64),
            },
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }

    fn governed_envelope(batch_id: &str, key: &str, sequence: u64) -> ChangeEnvelope {
        governed_envelope_for_tenant(
            "tenant-a",
            batch_id,
            key,
            sequence,
            2 + sequence,
            &format!("envelope-{sequence}"),
        )
    }

    fn commit_envelope_at(
        shard: &Shard,
        envelope: &ChangeEnvelope,
    ) -> Result<ChangeEnvelopeCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelope(
            shard,
            "graph-a",
            envelope,
            123,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    #[test]
    fn change_envelope_commits_rows_governance_version_cursor_and_outbox_once() {
        let path = temp_path("change-envelope");
        let db = open(&path);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            3,
            "open seeds graph-a through three committed maintenance admissions"
        );
        let first = governed_envelope("change-batch-1", "change-key-1", 1);
        let committed = commit_envelope_at(&db, &first).unwrap();
        assert!(!committed.replayed);
        assert_eq!(committed.outbox_count, 3);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            4,
            "the first envelope advances the three-maintenance seed from 3 to 4"
        );
        let baseline_outbox =
            assert_single_outbox_effect(&db, "change-batch-1", "a", "projection.test");
        assert!(read_one_node(&db, "graph-a", "a", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert_eq!(
            read_change_envelope(&db, "graph-a", "envelope-1", DurableCrypto::none())
                .unwrap()
                .unwrap()
                .envelope
                .mutation
                .batch_id,
            "change-batch-1"
        );
        assert_eq!(
            read_content_version(
                &db,
                "tenant-a",
                "graph-a",
                "object-1",
                DurableCrypto::none(),
            )
            .unwrap()
            .unwrap()
            .source_version,
            ContentVersionPosition::Sequence(1)
        );
        let mut first_retry = governed_envelope("change-batch-1", "change-key-1", 1);
        // Admission rebinds the caller's OCC expectation before replay
        // resolution; a fresh retry therefore carries the version now visible
        // at the graph while retaining the same stable operation identity.
        first_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        assert!(commit_envelope_at(&db, &first_retry).unwrap().replayed);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            4,
            "the replay returns the first receipt without advancing the version"
        );
        assert_eq!(
            assert_single_outbox_effect(&db, "change-batch-1", "a", "projection.test"),
            baseline_outbox,
            "replay must not duplicate or rewrite an outbox row"
        );

        let second = governed_envelope("change-batch-2", "change-key-2", 2);
        let second_commit = commit_envelope_at(&db, &second).unwrap();
        assert_eq!(second_commit.outbox_count, 3);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            5,
            "three maintenance admissions plus two fresh envelopes account for version 5"
        );
        assert_single_outbox_effect(&db, "change-batch-2", "a", "projection.test");
        assert_eq!(
            read_change_cursor(
                &db,
                "tenant-a",
                "graph-a",
                "fixture-source",
                "partition-1",
                DurableCrypto::none(),
            )
            .unwrap()
            .unwrap()
            .position,
            CursorPosition::Sequence(2)
        );
        let mut stale = governed_envelope("change-batch-3", "change-key-3", 2);
        stale.envelope_id = "envelope-stale".to_string();
        // The stale content-version assertion must run after the OCC check: the
        // three maintenance seed admissions plus the two fresh envelopes leave
        // the authoritative graph at version 5, while sequence 2 is already
        // present and must be rejected as stale content.
        stale.mutation.version_expectation = VersionExpectation::Graph(5);
        let error = commit_envelope_at(&db, &stale).unwrap_err();
        assert!(
            error.contains("STALE_CONTENT_VERSION"),
            "unexpected stale-envelope rejection: {error}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelope_rows_remain_scoped_to_each_caller_tenant() {
        let path = temp_path("change-envelope-caller-tenants");
        let db = open(&path);
        let tenant_a = governed_envelope_for_tenant(
            "tenant-a",
            "caller-tenant-a-batch",
            "caller-tenant-a-key",
            1,
            3,
            "caller-tenant-a-envelope",
        );
        let tenant_b = governed_envelope_for_tenant(
            "tenant-b",
            "caller-tenant-b-batch",
            "caller-tenant-b-key",
            1,
            4,
            "caller-tenant-b-envelope",
        );

        commit_envelope_at(&db, &tenant_a).unwrap();
        commit_envelope_at(&db, &tenant_b).unwrap();

        for (tenant, envelope_id) in [
            ("tenant-a", "caller-tenant-a-envelope"),
            ("tenant-b", "caller-tenant-b-envelope"),
        ] {
            let retained = read_change_envelope(&db, "graph-a", envelope_id, DurableCrypto::none())
                .unwrap()
                .expect("the caller envelope remains durably readable")
                .envelope;
            assert_eq!(retained.mutation.identity.tenant().as_str(), tenant);
            assert_eq!(retained.policies[0].tenant, tenant);

            assert_eq!(
                read_content_version(&db, tenant, "graph-a", "object-1", DurableCrypto::none(),)
                    .unwrap()
                    .unwrap()
                    .source_version,
                ContentVersionPosition::Sequence(1)
            );
            assert_eq!(
                read_change_cursor(
                    &db,
                    tenant,
                    "graph-a",
                    "fixture-source",
                    "partition-1",
                    DurableCrypto::none(),
                )
                .unwrap()
                .unwrap()
                .position,
                CursorPosition::Sequence(1)
            );
        }

        let _ = std::fs::remove_file(path);
    }

    // ── Batched ChangeEnvelope commit (W1.4) ──────────────────────────────────

    /// One first-write envelope on a DISTINCT object, chained onto the graph's seeded
    /// version 3: envelope `index` expects graph version `3 + index` and advances the
    /// shared source cursor to `index + 1`. A whole page of these commits in ONE
    /// transaction (read-your-writes chains version + cursor across the envelopes).
    fn governed_envelope_seq(index: u64) -> ChangeEnvelope {
        let object = format!("object-{index}");
        let mut mutation = batch(&format!("batch-{index}"), &format!("key-{index}"));
        mutation.version_expectation = VersionExpectation::Graph(3 + index);
        mutation.operations.truncate(1);
        mutation.operations[0].method = node(&format!("n{index}"), index as i64);
        mutation.outbox[0].payload =
            rmp_serde::to_vec_named(&serde_json::json!({ "event": "batch" })).unwrap();
        mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: format!("env-{index}"),
            mutation,
            content_version: ContentVersion {
                object_id: object.clone(),
                digest_algorithm: "sha256".to_string(),
                digest: format!("{:064x}", index + 1),
                previous_digest: None,
                source_version: ContentVersionPosition::Sequence(1),
            },
            cursor: Some(ChangeCursor {
                source: "batch-source".to_string(),
                partition: "p1".to_string(),
                position: CursorPosition::Sequence(index + 1),
                expected_previous: (index > 0).then_some(CursorPosition::Sequence(index)),
            }),
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            policies: vec![PolicyRecord {
                policy_id: format!("policy-{index}"),
                operation: MaterialOperation::Upsert,
                object_id: object,
                tenant: "tenant-a".to_string(),
                classification: "internal".to_string(),
                policy_version: "policy-v1".to_string(),
                subject_set_digest: "c".repeat(64),
                retention_policy: "standard".to_string(),
                legal_hold: false,
            }],
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".to_string(),
                sanitizer_version: "sanitizer-v1".to_string(),
                sanitized_payload_digest: "d".repeat(64),
            },
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }

    fn commit_envelopes_at(
        shard: &Shard,
        envelopes: &[ChangeEnvelope],
    ) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelopes(
            shard,
            "graph-a",
            envelopes,
            123,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
    }

    fn assert_single_outbox_effect(
        shard: &Shard,
        batch_id: &str,
        node_id: &str,
        event: &str,
    ) -> Vec<eg_types::mutation_batch::MutationOutboxRecord> {
        let receipt = read_mutation_batch_for_graph(shard, "graph-a", batch_id)
            .unwrap()
            .expect("the committed envelope must leave one durable kernel receipt");
        assert_eq!(receipt.batch.operations.len(), 1);
        match &receipt.batch.operations[0].method {
            Method::AddNode {
                node_id: recorded_node,
                ..
            } => assert_eq!(recorded_node, node_id),
            other => panic!("unexpected page operation in receipt: {other:?}"),
        }
        assert_eq!(receipt.batch.outbox.len(), 1);
        assert_eq!(receipt.batch.outbox[0].topic, "projection.test");
        assert_eq!(receipt.batch.outbox[0].key, batch_id);
        let expected_payload =
            rmp_serde::to_vec_named(&serde_json::json!({ "event": event })).unwrap();
        assert_eq!(receipt.batch.outbox[0].payload, expected_payload);

        let outbox = read_mutation_outbox(shard, "graph-a", batch_id).unwrap();
        assert_eq!(
            outbox.len(),
            1,
            "one batch outbox intent must write one row"
        );
        assert_eq!(outbox[0].ordinal, 0);
        assert_eq!(outbox[0].batch_id, batch_id);
        assert_eq!(outbox[0].intent.topic, receipt.batch.outbox[0].topic);
        assert_eq!(outbox[0].intent.key, receipt.batch.outbox[0].key);
        assert_eq!(outbox[0].intent.payload, expected_payload);
        outbox
    }

    #[test]
    fn change_envelopes_commit_whole_page_in_one_transaction() {
        let path = temp_path("change-envelopes-page");
        let db = open(&path);
        let page: Vec<ChangeEnvelope> = (0..3).map(governed_envelope_seq).collect();

        let commits = commit_envelopes_at(&db, &page).unwrap();

        // Per-envelope result vocabulary: every envelope is `applied` (not replayed).
        assert_eq!(commits.len(), 3);
        assert!(commits.iter().all(|commit| !commit.replayed));
        assert_eq!(commits[0].envelope_id, "env-0");
        // Every object landed and the graph version advanced by exactly N (3 -> 6),
        // proving all three applied inside the one shared transaction.
        for index in 0..3 {
            assert!(
                read_one_node(&db, "graph-a", &format!("n{index}"), DurableCrypto::none())
                    .unwrap()
                    .is_some()
            );
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_some(),
                "envelope {index} must leave one durable kernel receipt"
            );
            assert_single_outbox_effect(
                &db,
                &format!("batch-{index}"),
                &format!("n{index}"),
                "batch",
            );
        }
        assert!(commits.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            6,
            "the shared page must advance the graph version once per envelope"
        );
        // The chained cursor advanced to the last envelope's position — proof that the
        // final envelope (and therefore every earlier one) committed atomically.
        assert_eq!(
            read_change_cursor(
                &db,
                "tenant-a",
                "graph-a",
                "batch-source",
                "p1",
                DurableCrypto::none()
            )
            .unwrap()
            .unwrap()
            .position,
            CursorPosition::Sequence(3)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_idempotent_replay_skips_without_duplicating_outbox() {
        let path = temp_path("change-envelopes-replay");
        let db = open(&path);
        let page: Vec<ChangeEnvelope> = (0..2).map(governed_envelope_seq).collect();

        commit_envelopes_at(&db, &page).unwrap();
        let baseline_batch_0 = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let baseline_batch_1 = assert_single_outbox_effect(&db, "batch-1", "n1", "batch");
        // A fresh attempt over the same stable operations: every envelope
        // idempotent-skips without reusing a consumed nonce.
        let mut retry_page: Vec<ChangeEnvelope> = (0..2).map(governed_envelope_seq).collect();
        for retry in &mut retry_page {
            retry.mutation.version_expectation = VersionExpectation::Graph(5);
        }
        let replay = commit_envelopes_at(&db, &retry_page).unwrap();
        assert!(replay.iter().all(|commit| commit.replayed));
        assert!(replay.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            baseline_batch_0,
            "idempotent replay must not duplicate or rewrite batch-0's outbox"
        );
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-1", "n1", "batch"),
            baseline_batch_1,
            "idempotent replay must not duplicate or rewrite batch-1's outbox"
        );
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            5,
            "an all-replay page must not advance the graph version"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_mixed_fresh_replay_and_fresh_share_one_transaction() {
        let path = temp_path("change-envelopes-mixed-success");
        let db = open(&path);
        let replay = governed_envelope_seq(0);
        commit_envelopes_at(&db, std::slice::from_ref(&replay)).unwrap();
        let replay_outbox = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let mut replay_retry = governed_envelope_seq(0);
        replay_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        let fresh_first = governed_envelope_seq(1);
        let fresh_last = governed_envelope_seq(2);
        // A trailing replay must leave the last fresh batch as the group's
        // terminal reference; replacing it with this replay would make the
        // shared commit reject an otherwise valid page.
        let mut trailing_replay = governed_envelope_seq(0);
        trailing_replay.mutation.version_expectation = VersionExpectation::Graph(6);

        let commits = commit_envelopes_at(
            &db,
            &[fresh_first, replay_retry, fresh_last, trailing_replay],
        )
        .unwrap();

        assert_eq!(commits.len(), 4);
        assert!(!commits[0].replayed);
        assert!(commits[1].replayed);
        assert!(!commits[2].replayed);
        assert!(commits[3].replayed);
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n1", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n2", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert_single_outbox_effect(&db, "batch-1", "n1", "batch");
        assert_single_outbox_effect(&db, "batch-2", "n2", "batch");
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            replay_outbox,
            "the replay must not duplicate or rewrite its prior outbox"
        );
        assert!(commits.iter().all(|commit| commit.outbox_count == 3));
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            6,
            "only the two fresh members advance the graph version"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_mixed_replay_and_late_failure_roll_back_fresh_suffix() {
        let path = temp_path("change-envelopes-mixed-abort");
        let db = open(&path);
        let replay = governed_envelope_seq(0);
        commit_envelopes_at(&db, std::slice::from_ref(&replay)).unwrap();
        let replay_outbox = assert_single_outbox_effect(&db, "batch-0", "n0", "batch");
        let mut replay_retry = governed_envelope_seq(0);
        replay_retry.mutation.version_expectation = VersionExpectation::Graph(4);
        let mut bad = governed_envelope_seq(2);
        bad.content_version.previous_digest = Some("a".repeat(64));

        let error =
            commit_envelopes_at(&db, &[replay_retry, governed_envelope_seq(1), bad]).unwrap_err();

        assert_eq!(error.index, 2);
        assert!(error.error.contains("STALE_CONTENT_VERSION"));
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_some());
        assert!(read_one_node(&db, "graph-a", "n1", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", "batch-1")
                .unwrap()
                .is_none(),
            "fresh work after a replay must roll back with the late failure"
        );
        assert!(read_mutation_outbox(&db, "graph-a", "batch-1")
            .unwrap()
            .is_empty());
        assert_eq!(
            assert_single_outbox_effect(&db, "batch-0", "n0", "batch"),
            replay_outbox,
            "the already durable replay remains unchanged"
        );
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 4);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_consumed_nonce_rejects_page_without_fresh_effects() {
        let path = temp_path("change-envelopes-nonce");
        let db = open(&path);
        let first = governed_envelope_seq(0);
        let mut duplicate_nonce = governed_envelope_seq(1);
        let nonce = first
            .mutation
            .envelope
            .operation()
            .expect("fixture operation envelope")
            .authority
            .nonce
            .clone();
        let eg_types::mutation_batch::MutationEnvelope::Operation(operation) =
            &mut duplicate_nonce.mutation.envelope
        else {
            panic!("fixture operation envelope");
        };
        operation.authority.nonce = nonce;
        operation.authority.idempotency_key =
            Some(eg_types::contract::IdempotencyKey::new("different-page-key").unwrap());
        operation.authority.context_digest =
            operation.authority.recompute_context_digest().unwrap();
        duplicate_nonce
            .mutation
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();

        let error = commit_envelopes_at(&db, &[first, duplicate_nonce]).unwrap_err();

        assert_eq!(error.index, 1);
        assert!(error.error.contains("REPLAY_NONCE_CONSUMED"), "{error:?}");
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(read_mutation_batch_for_graph(&db, "graph-a", "batch-0")
            .unwrap()
            .is_none());
        assert!(read_mutation_outbox(&db, "graph-a", "batch-0")
            .unwrap()
            .is_empty());
        assert_eq!(read_mutation_graph_version(&db, "graph-a").unwrap(), 3);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_abort_rolls_back_the_whole_graph_batch() {
        let path = temp_path("change-envelopes-abort");
        let db = open(&path);
        // The third envelope fails its content-version check (previous digest on a
        // fresh object) — both earlier envelopes must roll back with the page.
        let mut bad = governed_envelope_seq(2);
        bad.content_version.previous_digest = Some("a".repeat(64));
        let page = vec![governed_envelope_seq(0), governed_envelope_seq(1), bad];

        let error = commit_envelopes_at(&db, &page).unwrap_err();
        assert_eq!(error.index, 2);
        assert!(
            error.error.contains("STALE_CONTENT_VERSION"),
            "{}",
            error.error
        );
        // NOTHING committed: the first (valid) envelope rolled back with the batch.
        for index in 0..2 {
            assert!(
                read_one_node(&db, "graph-a", &format!("n{index}"), DurableCrypto::none())
                    .unwrap()
                    .is_none()
            );
            assert!(read_change_envelope(
                &db,
                "graph-a",
                &format!("env-{index}"),
                DurableCrypto::none()
            )
            .unwrap()
            .is_none());
            assert!(
                read_mutation_batch_for_graph(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_none(),
                "the kernel receipt for earlier envelope {index} must roll back"
            );
            assert!(
                read_mutation_outbox(&db, "graph-a", &format!("batch-{index}"))
                    .unwrap()
                    .is_empty(),
                "earlier envelope {index} must not leave an outbox row"
            );
        }
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            3,
            "the graph version must roll back with the envelope rows"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_oversized_batch_is_a_typed_error() {
        let path = temp_path("change-envelopes-oversized");
        let db = open(&path);
        let too_many: Vec<ChangeEnvelope> =
            (0..(crate::change_envelope::MAX_ENVELOPES_PER_BATCH as u64 + 1))
                .map(governed_envelope_seq)
                .collect();

        let error = commit_envelopes_at(&db, &too_many).unwrap_err();
        assert!(
            error.error.contains("CHANGE_BATCH_TOO_LARGE"),
            "{}",
            error.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelope_batch_of_one_matches_the_single_commit_receipt() {
        let batch_path = temp_path("change-envelopes-parity-batch");
        let single_path = temp_path("change-envelopes-parity-single");
        let batch_db = open(&batch_path);
        let single_db = open(&single_path);
        let envelope = governed_envelope_seq(0);

        let batched = commit_envelopes_at(&batch_db, std::slice::from_ref(&envelope)).unwrap();
        let single = commit_envelope_at(&single_db, &envelope).unwrap();

        // A one-envelope batch yields the exact same receipt the single method does.
        assert_eq!(batched.len(), 1);
        assert_eq!(batched[0], single);
        let _ = std::fs::remove_file(batch_path);
        let _ = std::fs::remove_file(single_path);
    }

    // CXA-EG-02 characterization: `DeferWorkItem` and `CancelWorkItem` had NO
    // coverage anywhere in this module (or in `mutation_batch_tests` more broadly)
    // before this lane -- every other `apply_work_item_rows` arm (ClaimWorkItem,
    // RenewWorkItemLease, CasWorkItemMetadata, CommitWorkItemResult) is exercised
    // above, but these two were not. Added as part of decomposing
    // `apply_work_item_rows` (CCN 117 -> 2) so the extraction of these two arms has
    // a real black-box regression net, not just a clean `cargo check`. Ran GREEN
    // against the UNMODIFIED function before the refactor landed (see the lane
    // report for the exact `cargo test` transcript); unchanged by the refactor
    // commit.
    #[test]
    fn defer_work_item_returns_leased_item_to_ready_with_bumped_epoch() {
        let path = temp_path("work-item-defer");
        let db = open(&path);

        let mut seed = batch("work-item-defer-seed", "work-item-defer-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-defer", 3),
        }];
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "work-item-defer-claim",
            "work-item-defer-claim-key",
            4,
            Some("work-defer"),
            "worker-a",
            0,
            60_000,
            64,
        );
        assert!(claimed.claimed);
        assert_eq!(claimed.lease_epoch, Some(1));
        assert_eq!(claimed.fencing_token, Some(1));

        let mut defer = batch("work-item-defer-op", "work-item-defer-op-key");
        // `DeferWorkItem` is a `native_terminal_work_item_cas` batch (its own
        // lease/fencing token is the real CAS guard), but `version_expectation`
        // is still checked like any other graph-scoped batch by
        // `check_occ_version_and_fence` -- 5 is the actual current graph
        // version here (seed 3->4, claim 4->5) and must match exactly.
        defer.version_expectation = VersionExpectation::Graph(5);
        defer.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::DeferWorkItem {
                tenant: "tenant-a".into(),
                work_item_id: "work-defer".into(),
                worker_id: "worker-a".into(),
                lease_epoch: 1,
                fencing_token: 1,
                idempotency_key: "defer-key".into(),
                next_retry_at_ms: 5_000,
                reason_ref: Some("reason:sha256:one".into()),
                now_ms: 1_000,
            },
        }];
        defer
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("defer fixture reseals its final body");
        let committed = commit_at(&db, &defer, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("defer result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("DeferWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "deferred");
        assert_eq!(value["lease_epoch"], 2);
        assert_eq!(value["fencing_token"], 2);
        assert_eq!(value["next_retry_at_ms"], 5_000);
        assert_eq!(value["attempt"], 0);
        assert_eq!(value["defer_count"], 1);

        let row = read_one_node(&db, "graph-a", "work-defer", DurableCrypto::none())
            .unwrap()
            .expect("deferred item remains inspectable");
        let props: serde_json::Value = decode_durable(&row).unwrap();
        assert_eq!(props["status"], "ready");
        assert_eq!(props["lease_owner"], serde_json::Value::Null);
        assert_eq!(props["defer_reason_ref"], "reason:sha256:one");
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cancel_work_item_marks_ready_item_cancelled_and_replay_is_a_noop() {
        let path = temp_path("work-item-cancel");
        let db = open(&path);

        let mut seed = batch("work-item-cancel-seed", "work-item-cancel-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: ready_work_item_method("work-cancel", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel seed fixture reseals its final body");
        commit_at(&db, &seed, None).unwrap();

        let mut cancel = batch("work-item-cancel-op", "work-item-cancel-op-key");
        // `CancelWorkItem` is a `native_terminal_work_item_cas` batch, but
        // v1 removed the "supply no expectation to skip the OCC check"
        // escape hatch structurally (a graph-scoped batch must always carry
        // `VersionExpectation::Graph(_)`; see `check_occ_version_and_fence`'s
        // doc). `check_occ_version_and_fence` DOES check this value now, for
        // every graph-scoped batch uniformly -- the same real-OCC upgrade
        // `resource_reservation_tests.rs` deliberately opted into. 4 is the
        // actual current graph version here (only `seed` has committed:
        // 3->4).
        cancel.version_expectation = VersionExpectation::Graph(4);
        cancel.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method: Method::CancelWorkItem {
                tenant: "tenant-a".into(),
                work_item_id: "work-cancel".into(),
                idempotency_key: "cancel-key".into(),
                reason_ref: Some("reason:sha256:two".into()),
                now_ms: 1_000,
            },
        }];
        cancel
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel fixture reseals its final body");
        let committed = commit_at(&db, &cancel, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("cancel result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("CancelWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "cancelled");

        let row = read_one_node(&db, "graph-a", "work-cancel", DurableCrypto::none())
            .unwrap()
            .expect("cancelled item remains inspectable");
        let props: serde_json::Value = decode_durable(&row).unwrap();
        assert_eq!(props["status"], "cancelled");
        assert_eq!(props["cancel_reason_ref"], "reason:sha256:two");

        // A fresh (distinct idempotency key) CancelWorkItem operation against an
        // ALREADY-cancelled item exercises the handler's own internal
        // `matches!(status, "succeeded"|"failed"|"cancelled"|"dead_letter") ->
        // noop` guard -- distinct from MutationBatch-level idempotency replay,
        // which a different idempotency_key deliberately bypasses. Because it
        // is a genuinely NEW batch (not a replay), it is subject to
        // `check_occ_version_and_fence` like any other graph-scoped commit --
        // `cancel`'s own commit above already advanced `graph-a` 4->5, so this
        // second, independent commit must expect the CURRENT version (5), not
        // a stale clone of `cancel`'s now-superseded `Graph(4)`.
        let mut cancel_again = cancel.clone();
        cancel_again.batch_id = "work-item-cancel-op-2".into();
        cancel_again.envelope = fixture_operation_envelope(
            &cancel_again.identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            42,
            "work-item-cancel-op-2-key",
        );
        cancel_again.outbox[0].key = cancel_again.batch_id.clone();
        cancel_again.version_expectation = VersionExpectation::Graph(5);
        cancel_again
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("cancel replay fixture reseals its final body");
        let replay = commit_at(&db, &cancel_again, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            replay
                .record
                .result_msgpack
                .as_deref()
                .expect("cancel replay result"),
        )
        .unwrap();
        let value = match payload {
            crate::protocol::ResultPayload::Json(value) => value,
            other => panic!("CancelWorkItem must return a JSON result, got {other:?}"),
        };
        assert_eq!(value["status"], "noop");

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_commits_rows_outbox_and_replays_after_reopen() {
        let path = temp_path("terminal-extension-success-reopen");
        let db = open(&path);
        let mut seed = batch("terminal-extension-seed", "terminal-extension-seed-key");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: DurabilityDomain::GraphRows,
            method: delegated_work_item_method("work-extension-success", 3),
        }];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "terminal-extension-claim",
            "terminal-extension-claim-key",
            4,
            Some("work-extension-success"),
            "worker-a",
            0,
            60_000,
            64,
        );
        let terminal = terminal_extension_batch(
            "terminal-extension-success",
            "terminal-extension-success-key",
            5,
            "work-extension-success",
            "worker-a",
            claimed.lease_epoch.unwrap(),
            claimed.fencing_token.unwrap(),
            "succeeded",
            false,
        );
        let committed = commit_at(&db, &terminal, None).unwrap();
        assert!(!committed.replayed);
        let work_item = read_one_node(
            &db,
            "graph-a",
            "work-extension-success",
            DurableCrypto::none(),
        )
        .unwrap()
        .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "succeeded");
        for node_id in ["trace:terminal", "toolcall:terminal:0", "outcome:terminal"] {
            let receipt = read_one_node(&db, "graph-a", node_id, DurableCrypto::none())
                .unwrap()
                .unwrap();
            let receipt: serde_json::Value = decode_durable(&receipt).unwrap();
            assert_eq!(receipt["work_item_id"], "work-extension-success");
            assert_eq!(receipt["delegator_id"], "agent:delegator-a");
            assert_eq!(receipt["selected_agent_id"], "agent:selected-b");
            assert_eq!(receipt["executor_lease_actor"], "worker-a");
            assert_eq!(receipt["outcome"], "succeeded");
            assert_eq!(receipt["completeness"], "complete");
            assert_eq!(receipt["missing_refs"], serde_json::json!([]));
            assert_eq!(receipt["model_digest"], digest_for('e'));
            assert_eq!(receipt["policy_digest"], digest_for('d'));
        }
        let outbox = read_mutation_outbox(&db, "graph-a", terminal.batch_id.as_str()).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox
                .iter()
                .filter(|row| row.intent.topic == RUN_EVENT_OUTBOX_TOPIC)
                .count(),
            1
        );
        drop(db);

        let reopened = reopen(&path);
        let durable_receipt = read_one_node(
            &reopened,
            "graph-a",
            "trace:terminal",
            DurableCrypto::none(),
        )
        .unwrap();
        assert!(durable_receipt.is_some());
        let mut replay = terminal.clone();
        let eg_types::mutation_batch::MutationEnvelope::Operation(operation) = &mut replay.envelope
        else {
            panic!("terminal replay fixture needs an operation envelope");
        };
        operation.authority.nonce = eg_types::contract::Nonce::from_bytes([0x43; 32]);
        operation.authority.context_digest =
            operation.authority.recompute_context_digest().unwrap();
        replay.created_at_ms = 200;
        replay
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let replayed = commit_at(&reopened, &replay, None).unwrap();
        assert!(replayed.replayed);
        let replay_outbox =
            read_mutation_outbox(&reopened, "graph-a", terminal.batch_id.as_str()).unwrap();
        assert_eq!(replay_outbox.len(), 1);
        assert_eq!(
            replay_outbox
                .iter()
                .filter(|row| row.intent.topic == RUN_EVENT_OUTBOX_TOPIC)
                .count(),
            1
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_rejects_resealed_foreign_scope_before_durable_advance() {
        let path = temp_path("terminal-extension-foreign-scope");
        let db = open(&path);
        let work_item_id = "work-terminal-extension-foreign-scope";
        let claimed = seed_and_claim_terminal_work_item(
            &db,
            "terminal-extension-foreign-scope",
            work_item_id,
        );
        let batch_id = "terminal-extension-foreign-scope-batch";
        let mut terminal = terminal_extension_batch(
            batch_id,
            "terminal-extension-foreign-scope-key",
            5,
            work_item_id,
            "worker-a",
            claimed.lease_epoch.unwrap(),
            claimed.fencing_token.unwrap(),
            "succeeded",
            false,
        );
        let version_before = read_mutation_graph_version(&db, "graph-a").unwrap();
        let caller_scope = terminal
            .outbox
            .iter()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .and_then(|intent| intent.headers.get("scope_sha256"))
            .cloned()
            .expect("terminal fixture carries the caller scope digest");

        let mut foreign_scope_digest = Sha256::new();
        foreign_scope_digest.update(b"tenant-b");
        foreign_scope_digest.update([0]);
        foreign_scope_digest.update(b"graph-b");
        let foreign_scope = hex::encode(foreign_scope_digest.finalize());
        terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("terminal fixture carries a run event")
            .headers
            .insert("scope_sha256".into(), foreign_scope);
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();

        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(
            error.contains("scope_sha256") && error.contains("caller mutation scope"),
            "got: {error}"
        );
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            version_before,
            "caller-scope rejection must not advance the graph version"
        );
        assert!(
            read_mutation_batch_for_graph(&db, "graph-a", batch_id)
                .unwrap()
                .is_none(),
            "caller-scope rejection must not persist a batch receipt"
        );
        assert!(
            read_mutation_outbox(&db, "graph-a", batch_id)
                .unwrap()
                .is_empty(),
            "caller-scope rejection must not persist an outbox row"
        );

        // Restore the authenticated caller header and retry with the same
        // attempt nonce. A successful commit proves the rejected attempt did
        // not consume the durable replay nonce before scope authentication.
        terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("terminal fixture carries a run event")
            .headers
            .insert("scope_sha256".into(), caller_scope);
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        let committed = commit_at(&db, &terminal, None).unwrap();
        assert!(!committed.replayed);
        assert!(read_mutation_batch_for_graph(&db, "graph-a", batch_id)
            .unwrap()
            .is_some());
        assert_eq!(
            read_mutation_outbox(&db, "graph-a", batch_id)
                .unwrap()
                .len(),
            1
        );
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_persists_terminal_currency_across_reopen() {
        for (outcome, tag) in [
            ("failed", "terminal-extension-failed-reopen"),
            ("cancelled", "terminal-extension-cancelled-reopen"),
        ] {
            let path = temp_path(tag);
            let db = open(&path);
            let work_item_id = format!("work-{tag}");
            let claimed = seed_and_claim_terminal_work_item(&db, tag, &work_item_id);
            let batch_id = format!("{tag}-batch");
            let terminal = terminal_extension_batch(
                &batch_id,
                &format!("{tag}-key"),
                5,
                &work_item_id,
                "worker-a",
                claimed.lease_epoch.unwrap(),
                claimed.fencing_token.unwrap(),
                outcome,
                false,
            );
            commit_at(&db, &terminal, None).unwrap();
            assert_persisted_terminal_currency(
                &db,
                &batch_id,
                outcome,
                OutcomeCompleteness::Complete,
                &[],
            );
            drop(db);

            let reopened = reopen(&path);
            assert_persisted_terminal_currency(
                &reopened,
                &batch_id,
                outcome,
                OutcomeCompleteness::Complete,
                &[],
            );
            drop(reopened);
            let _ = std::fs::remove_file(path);
        }

        let path = temp_path("terminal-extension-degraded-reopen");
        let db = open(&path);
        let work_item_id = "work-terminal-extension-degraded";
        let claimed =
            seed_and_claim_terminal_work_item(&db, "terminal-extension-degraded", work_item_id);
        let batch_id = "terminal-extension-degraded-batch";
        let mut terminal = terminal_extension_batch(
            batch_id,
            "terminal-extension-degraded-key",
            5,
            work_item_id,
            "worker-a",
            claimed.lease_epoch.unwrap(),
            claimed.fencing_token.unwrap(),
            "succeeded",
            false,
        );
        let missing_refs = ["toolcall:terminal:0"];
        let degraded_event = {
            let Method::CommitWorkItemResult {
                outcome_extension: Some(extension),
                ..
            } = &mut terminal.operations[0].method
            else {
                panic!("degraded fixture must carry a terminal extension");
            };
            extension.outcome_bundle.completeness = OutcomeCompleteness::Degraded;
            extension.outcome_bundle.missing_refs = missing_refs
                .iter()
                .map(|reference| (*reference).to_string())
                .collect();
            extension.run_event.completeness = OutcomeCompleteness::Degraded;
            extension.run_event.missing_refs = extension.outcome_bundle.missing_refs.clone();
            extension.run_event.kind = "degraded".into();
            let bundle = extension.outcome_bundle.clone();
            extension.receipt_nodes = vec![
                terminal_receipt_node(&bundle, ReceiptNodeKind::RunTrace, &bundle.trace_ref),
                terminal_receipt_node(
                    &bundle,
                    ReceiptNodeKind::OutcomeEvaluation,
                    &bundle.outcome_ref,
                ),
            ];
            extension.run_event.clone()
        };
        let event_intent = terminal
            .outbox
            .iter_mut()
            .find(|intent| intent.topic == RUN_EVENT_OUTBOX_TOPIC)
            .expect("degraded fixture must carry a run event intent");
        event_intent.payload = rmp_serde::to_vec_named(&degraded_event).unwrap();
        event_intent
            .headers
            .insert("completeness".into(), "degraded".into());
        event_intent.headers.insert(
            "missing_refs".into(),
            serde_json::to_string(&missing_refs).unwrap(),
        );
        terminal
            .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &terminal, None).unwrap();
        assert_persisted_terminal_currency(
            &db,
            batch_id,
            "succeeded",
            OutcomeCompleteness::Degraded,
            &missing_refs,
        );
        drop(db);

        let reopened = reopen(&path);
        assert_persisted_terminal_currency(
            &reopened,
            batch_id,
            "succeeded",
            OutcomeCompleteness::Degraded,
            &missing_refs,
        );
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn terminal_extension_negative_results_have_no_receipt_rows_or_run_event() {
        let cases = [
            ("missing", "terminal-extension-missing"),
            ("fenced", "terminal-extension-fenced"),
            ("noop", "terminal-extension-noop"),
            ("retry_scheduled", "terminal-extension-retry"),
        ];
        for (outcome_case, tag) in cases {
            let path = temp_path(tag);
            let db = open(&path);
            let (expected_version, lease_epoch, fencing_token, worker, retryable) =
                match outcome_case {
                    "missing" => (3, 1, 1, "worker-a", false),
                    "fenced" => {
                        let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                        seed.operations = vec![MutationOperation {
                            ordinal: 0,
                            surface: MutationSurface::Transaction,
                            domain: DurabilityDomain::GraphRows,
                            method: delegated_work_item_method("work-extension-fenced", 3),
                        }];
                        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                            .unwrap();
                        commit_at(&db, &seed, None).unwrap();
                        commit_native_claim(
                            &db,
                            &format!("{tag}-claim"),
                            &format!("{tag}-claim-key"),
                            4,
                            Some("work-extension-fenced"),
                            "worker-a",
                            0,
                            60_000,
                            64,
                        );
                        (5, 1, 999, "worker-b", false)
                    }
                    "noop" => {
                        let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                        seed.operations = vec![MutationOperation {
                            ordinal: 0,
                            surface: MutationSurface::Transaction,
                            domain: DurabilityDomain::GraphRows,
                            method: delegated_work_item_method_with_status(
                                "work-extension-noop",
                                3,
                                "succeeded",
                            ),
                        }];
                        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                            .unwrap();
                        commit_at(&db, &seed, None).unwrap();
                        (4, 1, 1, "worker-a", false)
                    }
                    "retry_scheduled" => {
                        let mut seed = batch(&format!("{tag}-seed"), &format!("{tag}-seed-key"));
                        seed.operations = vec![MutationOperation {
                            ordinal: 0,
                            surface: MutationSurface::Transaction,
                            domain: DurabilityDomain::GraphRows,
                            method: delegated_work_item_method("work-extension-retry", 3),
                        }];
                        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                            .unwrap();
                        commit_at(&db, &seed, None).unwrap();
                        let claimed = commit_native_claim(
                            &db,
                            &format!("{tag}-claim"),
                            &format!("{tag}-claim-key"),
                            4,
                            Some("work-extension-retry"),
                            "worker-a",
                            0,
                            60_000,
                            64,
                        );
                        (
                            5,
                            claimed.lease_epoch.unwrap(),
                            claimed.fencing_token.unwrap(),
                            "worker-a",
                            true,
                        )
                    }
                    _ => unreachable!(),
                };
            let work_item_id = match outcome_case {
                "missing" => "work-extension-missing",
                "fenced" => "work-extension-fenced",
                "noop" => "work-extension-noop",
                _ => "work-extension-retry",
            };
            let terminal = terminal_extension_batch(
                &format!("{tag}-batch"),
                &format!("{tag}-key"),
                expected_version,
                work_item_id,
                worker,
                lease_epoch,
                fencing_token,
                if outcome_case == "retry_scheduled" {
                    "failed"
                } else {
                    "succeeded"
                },
                retryable,
            );
            let committed = commit_at(&db, &terminal, None).unwrap();
            let result: crate::protocol::ResultPayload =
                decode_durable(committed.record.result_msgpack.as_deref().unwrap()).unwrap();
            let result = match result {
                crate::protocol::ResultPayload::Json(value) => value,
                other => panic!("terminal result must be JSON, got {other:?}"),
            };
            assert_eq!(result["status"], outcome_case);
            assert!(
                read_one_node(&db, "graph-a", "trace:terminal", DurableCrypto::none())
                    .unwrap()
                    .is_none()
            );
            assert!(
                read_mutation_outbox(&db, "graph-a", terminal.batch_id.as_str())
                    .unwrap()
                    .is_empty()
            );
            if outcome_case == "retry_scheduled" {
                let work_item = read_one_node(&db, "graph-a", work_item_id, DurableCrypto::none())
                    .unwrap()
                    .unwrap();
                let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
                assert_eq!(work_item["status"], "ready");
            }
            drop(db);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn terminal_extension_rejects_preexisting_receipt_ids_atomically() {
        let path = temp_path("terminal-extension-preexisting-id");
        let db = open(&path);
        let preexisting_extension = terminal_extension(
            "terminal-extension-preexisting-batch",
            "work-extension-preexisting",
            1,
            "succeeded",
            "worker-a",
        );
        let mut seed = batch(
            "terminal-extension-preexisting-seed",
            "terminal-extension-preexisting-seed-key",
        );
        seed.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: delegated_work_item_method("work-extension-preexisting", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::AddNode {
                    node_id: "trace:terminal".into(),
                    properties_msgpack: preexisting_extension.receipt_nodes[0]
                        .properties_msgpack
                        .clone(),
                },
            },
        ];
        seed.reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
            .unwrap();
        commit_at(&db, &seed, None).unwrap();
        let claimed = commit_native_claim(
            &db,
            "terminal-extension-preexisting-claim",
            "terminal-extension-preexisting-claim-key",
            4,
            Some("work-extension-preexisting"),
            "worker-a",
            0,
            60_000,
            64,
        );
        let terminal = terminal_extension_batch(
            "terminal-extension-preexisting-batch",
            "terminal-extension-preexisting-key",
            5,
            "work-extension-preexisting",
            "worker-a",
            claimed.lease_epoch.unwrap(),
            claimed.fencing_token.unwrap(),
            "succeeded",
            false,
        );
        let error = commit_at(&db, &terminal, None).unwrap_err();
        assert!(error.contains("already exists"), "{error}");
        let work_item = read_one_node(
            &db,
            "graph-a",
            "work-extension-preexisting",
            DurableCrypto::none(),
        )
        .unwrap()
        .unwrap();
        let work_item: serde_json::Value = decode_durable(&work_item).unwrap();
        assert_eq!(work_item["status"], "leased");
        assert!(read_mutation_batch_for_graph(
            &db,
            "graph-a",
            "terminal-extension-preexisting-batch",
        )
        .unwrap()
        .is_none());
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}
