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

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
use crate::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationBatchStatus, MutationDomain,
    MutationOperation, MutationOutboxIntent, MutationOutboxLease, MutationOutboxRecord,
    MutationProjectionCursor, MutationSurface, MutationVersionScope, MUTATION_BATCH_VERSION,
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

#[cfg(test)]
mod resource_reservation_tests;

#[cfg(feature = "redb")]
pub(crate) mod capacity_lease;
#[cfg(feature = "redb")]
pub(crate) mod development_lane;
pub(crate) mod work_item_capability;

fn decode_durable<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(bytes, durable_msgpack_limits())
        .map_err(|_| "durable value is invalid or exceeds resource limits".to_string())
}

fn decode_mutation_batch_record(bytes: &[u8]) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_durable(bytes)?;
    record.batch.validate()?;
    Ok(record)
}

fn decode_mutation_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_durable(bytes)?;
    record.validate()?;
    if record.version_scope != MutationVersionScope::Graph || record.source_graph_version == 0 {
        return Err("graph mutation store contains a non-graph outbox record".to_string());
    }
    Ok(record)
}

fn decode_mutation_projection_cursor(bytes: &[u8]) -> Result<MutationProjectionCursor, String> {
    let cursor: MutationProjectionCursor = decode_durable(bytes)?;
    cursor.validate()?;
    if cursor.version_scope != MutationVersionScope::Graph || cursor.source_graph_version == 0 {
        return Err("graph mutation store contains a non-graph projection cursor".to_string());
    }
    Ok(cursor)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DurableMutationFence {
    placement_epoch: u64,
    fencing_token: u64,
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
    policies: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    host_ref: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, DurableResourceDiskPolicy)>, String> {
    let prefix = format!("{host_ref}\0");
    let mut rows = Vec::new();
    for row in policies
        .range((graph, prefix.as_str())..)
        .map_err(|error| error.to_string())?
    {
        if rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
            return Err("resource disk-policy scan exceeds native bound".to_string());
        }
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, policy_key) = key.value();
        if row_graph != graph || !policy_key.starts_with(&prefix) {
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
#[cfg(feature = "security")]
pub(crate) const PROVENANCE_ANCHOR_MEMBERS: TableDefinition<(&str, u64), &[u8]> =
    TableDefinition::new("provenance_anchor_members");
pub(crate) const GRAPH_META: TableDefinition<&str, &[u8]> = TableDefinition::new("graph_meta");
/// Monotonic native WorkItem command sequence, scoped by graph.  The sequence
/// is allocated in the same transaction as the submitted WorkItem row and its
/// mutation/outbox record, so replay never allocates a second command number.
pub(crate) const WORK_ITEM_COMMAND_SEQUENCE: TableDefinition<&str, u64> =
    TableDefinition::new("work_item_command_sequence");
/// Authoritative mutation-batch status/result rows, keyed by stable `batch_id`.
/// The complete batch is retained so a retry can prove that the idempotency key
/// names byte-identical work rather than silently accepting key reuse.
pub(crate) const MUTATION_BATCHES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("mutation_batches");
/// Durable idempotency index: `(tenant, graph, key) -> batch_id`.
pub(crate) const MUTATION_IDEMPOTENCY: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("mutation_idempotency");
/// Transactional projection/CDC/audit/lineage outbox.  Rows are immutable and
/// retry-addressable by `(batch_id, ordinal)`.
pub(crate) const MUTATION_OUTBOX: TableDefinition<(&str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox");
/// Latest committed lifecycle batch for a graph.  This is the generation fence
/// that prevents retrying an old Create after a later Delete (or vice versa).
pub(crate) const MUTATION_LIFECYCLE_HEAD: TableDefinition<&str, &str> =
    TableDefinition::new("mutation_lifecycle_head");
/// Monotonic authoritative graph version used for optimistic validation even
/// when the in-memory projection is absent/restarting.
pub(crate) const MUTATION_GRAPH_VERSION: TableDefinition<&str, u64> =
    TableDefinition::new("mutation_graph_version");
/// Highest accepted `(placement_epoch, fencing_token)` for a graph. A stale
/// route or superseded lease can never commit after this row advances.
pub(crate) const MUTATION_FENCE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("mutation_fence");
/// Durable delivery lease/ack state for transactional outbox rows.
pub(crate) const MUTATION_OUTBOX_DELIVERY: TableDefinition<(&str, u32, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_delivery");
/// Per-projection reconciliation watermark, scoped by tenant and graph.
pub(crate) const MUTATION_PROJECTION_CURSOR: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("mutation_projection_cursor");
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
#[cfg(feature = "compute-dist")]
pub(crate) const MATVIEWS: TableDefinition<&str, &[u8]> = TableDefinition::new("matviews");

// Named PLAN-BACKED materialized views (CONCEPT:EG-KG.storage.plan-backed-matview). One
// row per matview keyed by `name`, holding the MessagePack-serialized plan-backed
// DEFINITION (name + target graph + `wire::Plan` bytes + reorder hint) — NOT the result
// rows, which ride the version-keyed result cache. DISJOINT table from the algo-only
// `matviews` table above (and from Lane D's secondary-index tables): a distinct redb
// table name, so the two matview families and the index rows never collide. Reloaded into
// the in-RAM plan-matview manager on boot.
#[cfg(feature = "matview")]
pub(crate) const PLAN_MATVIEWS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("plan_matviews");

// DBSP INCREMENTAL operator state (CONCEPT:EG-KG.storage.incremental-matview): the durable
// snapshot of an incrementally-maintained plan-backed matview's circuit state (its
// membership map / per-bucket accumulators + CDC watermark), keyed by view name. The
// direct analogue of turso's `dbsp_state` btree, scoped down to redb. DISJOINT from
// `plan_matviews` (that table holds the DEFINITION; this holds the maintained STATE).
// Written when an incremental view is defined and dropped with it.
#[cfg(feature = "matview")]
pub(crate) const MATVIEW_OPERATOR_STATE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("matview_operator_state");

/// Materialize every table owned by the canonical authoritative graph store.
///
/// A read transaction cannot open a table that has never been created. Keep the
/// schema bootstrap beside the table definitions so the served, embedded, and
/// test paths cannot drift as new authoritative projections are added. Callers
/// may open transport-specific tables in the same transaction before committing.
fn init_canonical_core_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(NODES).map_err(|error| error.to_string())?;
    wtx.open_table(EDGES).map_err(|error| error.to_string())?;
    wtx.open_table(LEDGER).map_err(|error| error.to_string())?;
    wtx.open_table(SEMANTIC)
        .map_err(|error| error.to_string())?;
    wtx.open_table(GRAPH_META)
        .map_err(|error| error.to_string())?;
    wtx.open_table(WORK_ITEM_COMMAND_SEQUENCE)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn init_canonical_mutation_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(MUTATION_BATCHES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_OUTBOX)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_OUTBOX_DELIVERY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_PROJECTION_CURSOR)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_GRAPH_VERSION)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_FENCE)
        .map_err(|error| error.to_string())?;
    wtx.open_table(MUTATION_LIFECYCLE_HEAD)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn init_canonical_resource_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(RESOURCE_RESERVATIONS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_HOSTS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_EXCLUSIVITY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_FAIRNESS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_CONCURRENCY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_ANTI_AFFINITY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(RESOURCE_DISK_POLICIES)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn init_canonical_capability_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    development_lane::initialize_tables(wtx)?;
    capacity_lease::initialize_tables(wtx)?;
    work_item_capability::initialize_tables(wtx)?;
    Ok(())
}

fn init_canonical_change_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(CHANGE_ENVELOPES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CONTENT_VERSIONS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_CURSORS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_BLOBS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_FEATURES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_EVIDENCE)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_POLICIES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(CHANGE_LINEAGE)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn init_canonical_misc_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(RAFT_LOG)
        .map_err(|error| error.to_string())?;
    wtx.open_table(XSHARD_PREPARE)
        .map_err(|error| error.to_string())?;
    wtx.open_table(XSHARD_DECISION)
        .map_err(|error| error.to_string())?;
    #[cfg(feature = "compute-dist")]
    wtx.open_table(MATVIEWS)
        .map_err(|error| error.to_string())?;
    #[cfg(feature = "matview")]
    wtx.open_table(PLAN_MATVIEWS)
        .map_err(|error| error.to_string())?;
    #[cfg(feature = "security")]
    wtx.open_table(AUDIT).map_err(|error| error.to_string())?;
    #[cfg(feature = "security")]
    wtx.open_table(PROVENANCE_ANCHOR_MEMBERS)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn initialize_canonical_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    init_canonical_core_tables(wtx)?;
    init_canonical_mutation_tables(wtx)?;
    init_canonical_resource_tables(wtx)?;
    init_canonical_capability_tables(wtx)?;
    init_canonical_change_tables(wtx)?;
    init_canonical_misc_tables(wtx)?;
    Ok(())
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

/// An owned, off-lock dump of one graph used by the checkpoint + load paths.
pub struct GraphDump {
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

/// Commit all buffered mutations (and any Raft log appends) in ONE write
/// transaction at the given durability (CONCEPT:EG-KG.storage.one-fsync-covers-raft). A graph mutation and a
/// Raft log entry in the same batch therefore share ONE `WriteTransaction` and
/// ONE fsync. The embedded path passes an empty `raft_log_ops`.
pub(crate) fn commit_ops(
    db: &Database,
    ops: &mut Vec<(String, Method)>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    durability: Durability,
    crypto: DurableCrypto<'_>,
    // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), owned by the caller across batches.
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    if ops.is_empty() && raft_log_ops.is_empty() {
        return Ok(());
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(durability).map_err(|e| e.to_string())?;
    // Graphs touched by this batch — used to backfill a graph_meta row for any
    // graph that received writes but was never explicitly registered (e.g. the
    // pre-created `__commons__`), so authoritative `load_all` recovers it even with
    // no checkpoint.
    let mut touched: std::collections::HashSet<String> = std::collections::HashSet::new();
    {
        // `commit_ops` is the embedded/raft low-level graph-row path.  Even
        // though it predates the canonical MutationBatch kernel, every method
        // is still a possible WorkItem image replacement, so validate the
        // post-image before this transaction can commit.
        let mut lane_validation_graphs = std::collections::BTreeSet::new();
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut native_work_items = wtx
            .open_table(work_item_capability::NATIVE_WORK_ITEMS)
            .map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        let mut resource_reservations = wtx
            .open_table(RESOURCE_RESERVATIONS)
            .map_err(|e| e.to_string())?;
        let mut resource_tenant_index = wtx
            .open_table(RESOURCE_RESERVATION_TENANT_INDEX)
            .map_err(|e| e.to_string())?;
        let mut resource_attempts = wtx
            .open_table(RESOURCE_RESERVATION_ATTEMPTS)
            .map_err(|e| e.to_string())?;
        let mut resource_hosts = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
        let mut resource_exclusivity = wtx
            .open_table(RESOURCE_EXCLUSIVITY)
            .map_err(|e| e.to_string())?;
        let mut resource_fairness = wtx
            .open_table(RESOURCE_FAIRNESS)
            .map_err(|e| e.to_string())?;
        let mut resource_concurrency = wtx
            .open_table(RESOURCE_CONCURRENCY)
            .map_err(|e| e.to_string())?;
        let mut resource_anti_affinity = wtx
            .open_table(RESOURCE_ANTI_AFFINITY)
            .map_err(|e| e.to_string())?;
        let mut resource_disk_policies = wtx
            .open_table(RESOURCE_DISK_POLICIES)
            .map_err(|e| e.to_string())?;
        let mut command_sequences = wtx
            .open_table(WORK_ITEM_COMMAND_SEQUENCE)
            .map_err(|e| e.to_string())?;
        #[cfg(feature = "security")]
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for (graph, method) in ops.drain(..) {
            touched.insert(graph.clone());
            lane_validation_graphs.insert(graph.clone());
            if matches!(&method, Method::ClearGraph | Method::DeleteGraph { .. }) {
                command_sequences
                    .remove(graph.as_str())
                    .map_err(|e| e.to_string())?;
                clear_resource_rows(
                    &graph,
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
                development_lane::clear_native_graph_rows_in_wtx(&wtx, &graph, crypto)?;
                capacity_lease::clear_graph_rows(&wtx, &graph)?;
                work_item_capability::clear_graph_rows_in_wtx_with_native(
                    &wtx,
                    &graph,
                    &mut native_work_items,
                )?;
            }
            apply_method_rows(
                &graph,
                &method,
                &mut nodes,
                &mut edges,
                &mut ledger,
                &mut semantic,
                &native_work_items,
                crypto,
            )?;
            #[cfg(feature = "security")]
            append_audit_entry(&mut audit, audit_tail, &graph, &method)?;
        }
        drop(nodes);
        drop(edges);
        drop(ledger);
        drop(semantic);
        drop(command_sequences);
        for graph in &lane_validation_graphs {
            development_lane::validate_current_lane_links_in_wtx(&wtx, graph, crypto)?;
        }
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        for g in &touched {
            if meta.get(g.as_str()).map_err(|e| e.to_string())?.is_none() {
                let incarnation_id = new_incarnation_id(g);
                let encoded = encode_meta_with_incarnation(g, GraphType::Global, &incarnation_id)?;
                meta.insert(g.as_str(), encoded.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        if !raft_log_ops.is_empty() {
            let mut log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
            for (gid, idx, blob) in raft_log_ops.drain(..) {
                // Consensus entries carry Method payloads.  When the deployment
                // data key is active, seal them just like authoritative value rows
                // so source properties are not exposed by the local Raft log.
                let sealed = crypto.seal(&blob);
                log.insert((gid, idx), sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
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
    db: &Database,
    graph_fname: &str,
    batch: &MutationBatch,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        db,
        graph_fname,
        batch,
        None,
        None,
        None,
        result_msgpack,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
        // Compact-row batches never carry `authoritative_state`, so `audited` is
        // never consulted for them (see `commit_mutation_batch_inner`): method
        // identity is preserved (not opaque-wrapped) and `append_audit_entry`'s
        // per-method `audit_line` match already gates correctly.
        true,
        None,
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
    db: &Database,
    input: StateCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        db,
        input.graph_fname,
        input.batch,
        None,
        Some(input.authoritative_state_msgpack),
        None,
        input.result_msgpack,
        input.committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
        input.audited,
        None,
    )
}

/// Engine-native ChangeEnvelope commit. Graph rows, every material/governance
/// projection, version/cursor fences, terminal batch/envelope records, and the
/// CDC outbox are written by one redb transaction and one durability barrier.
pub(crate) fn commit_change_envelope(
    db: &Database,
    graph_fname: &str,
    envelope: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<ChangeEnvelopeCommit, String> {
    envelope.validate()?;
    let mutation = commit_mutation_batch_inner(
        db,
        graph_fname,
        &envelope.mutation,
        Some(envelope),
        None,
        None,
        None,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
        // No `authoritative_state`, so `audited` is inert here -- see
        // `commit_mutation_batch_inner`'s doc comment.
        true,
        None,
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
/// naming the offending index. A byte-identical idempotency replay is NOT a failure —
/// it is reported per envelope via `ChangeEnvelopeCommit::replayed` and the
/// transaction still commits its non-replayed siblings. When every envelope is a
/// replay, nothing was written and the transaction is dropped without an fsync,
/// exactly like the single-envelope path.
pub(crate) fn commit_change_envelopes(
    db: &Database,
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
    // Stage the audit tail once for the whole shared transaction; the caller-owned
    // cache is advanced only after the single commit succeeds.
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    let mut wtx = db.begin_write().map_err(|e| at(0, e.to_string()))?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| at(0, e.to_string()))?;

    let mut commits = Vec::with_capacity(envelopes.len());
    let mut any_applied = false;
    for (index, envelope) in envelopes.iter().enumerate() {
        envelope.validate().map_err(|e| at(index, e))?;
        let mutation = apply_mutation_batch_in_wtx(
            &wtx,
            graph_fname,
            &envelope.mutation,
            Some(envelope),
            None,
            None,
            None,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            &mut staged_audit_tail,
            // No `authoritative_state`, so `audited` is inert here -- see
            // `apply_mutation_batch_in_wtx`'s doc comment.
            true,
            None,
        )
        .map_err(|e| at(index, e))?;
        if !mutation.replayed {
            any_applied = true;
        }
        let outbox_count = envelope
            .mutation
            .operations
            .len()
            .checked_add(envelope.mutation.outbox.len())
            .and_then(|count| count.checked_add(1))
            .and_then(|count| u32::try_from(count).ok())
            .ok_or_else(|| at(index, "change envelope outbox count overflow".to_string()))?;
        commits.push(ChangeEnvelopeCommit {
            envelope_id: envelope.envelope_id.clone(),
            batch_id: envelope.mutation.batch_id.clone(),
            content_version: envelope.content_version.clone(),
            cursor: envelope.cursor.clone(),
            outbox_count,
            replayed: mutation.replayed,
        });
    }

    // Commit the whole group as ONE fsync only when something was actually written.
    // An all-replay batch did reads only, so drop the transaction (no fsync) exactly
    // as the single-envelope path does; the caller-owned audit tail is untouched.
    if any_applied {
        wtx.commit().map_err(|e| at(0, e.to_string()))?;
        #[cfg(feature = "security")]
        {
            *audit_tail = staged_audit_tail;
        }
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
    db: &Database,
    input: CrossModalCommitInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<MutationBatchCommit, String> {
    commit_mutation_batch_inner(
        db,
        input.graph_fname,
        input.batch,
        None,
        None,
        Some(input.rows),
        input.result_msgpack,
        input.committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
        // No `authoritative_state`, so `audited` is inert here -- see
        // `commit_mutation_batch_inner`'s doc comment.
        true,
        None,
    )
}

/// `audited`: whether THIS commit should append tamper-evident audit-chain
/// entries for its operations. Only consulted when `authoritative_state_msgpack`
/// is `Some` (the Snapshot/RowDelta branches below) -- irrelevant otherwise
/// (compact-row/crossmodal/envelope callers pass a placeholder `true`; see each
/// call site).
///
/// Why this can't be re-derived from `batch.operations` alone: a state-backed
/// commit's `MutationOperation::method` is NOT the original causal `Method`
/// (e.g. `TouchNodes`) -- `mutation_batch::compile_methods`'s `opaque_state_operation`
/// unconditionally rewrites EVERY state-backed operation into the SAME opaque
/// digest receipt shape (`Method::ApplyMutation{event_type:
/// "authoritative_state_operation", ..}`), by design, so sensitive row payloads
/// never enter the durable batch/audit/outbox record. `audit::audit_line` always
/// recognizes that receipt shape as auditable, so by the time this function sees
/// `batch.operations`, the original method's OWN `eg_capabilities::policy(..).audited`
/// answer is unrecoverable from the operation alone. Callers must therefore
/// capture it themselves (typically `MutationPlan::audited`, already resolved from
/// the untranslated method at the top of `commit_mutation`/
/// `commit_conditional_mutation_async`) and pass it through here.
/// Commit ONE canonical MutationBatch/ChangeEnvelope: open the shard write
/// transaction, apply the batch through [`apply_mutation_batch_in_wtx`], and commit
/// it as one indivisible fsync point. The applier owns everything BETWEEN
/// `begin_write` and `commit`; keeping the transaction boundary here lets the batch
/// envelope path ([`commit_change_envelopes`]) reuse the identical apply logic across
/// MANY envelopes inside ONE transaction without duplicating the kernel.
#[allow(clippy::too_many_arguments)]
fn commit_mutation_batch_inner(
    db: &Database,
    graph_fname: &str,
    batch: &MutationBatch,
    change: Option<&ChangeEnvelope>,
    authoritative_state_msgpack: Option<&[u8]>,
    crossmodal: Option<CrossModalBatchRows<'_>>,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
    audited: bool,
    crashpoint: Option<MutationBatchCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    // Audit-tail updates are staged alongside the redb transaction. Advancing the
    // process cache before `wtx.commit()` would create a false tail when an
    // injected/real failure drops this transaction.
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let committed = apply_mutation_batch_in_wtx(
        &wtx,
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        result_msgpack,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        &mut staged_audit_tail,
        audited,
        crashpoint,
    )?;
    // A byte-identical replay short-circuited on reads only; nothing was written, so
    // drop the transaction (no fsync) exactly as the pre-refactor path did.
    if committed.replayed {
        return Ok(committed);
    }

    if crashpoint == Some(MutationBatchCrashpoint::BeforeCommit) {
        return Err("injected crash before mutation commit".to_string());
    }
    crate::mutation_batch::apply_certification_fault(
        batch,
        crate::mutation_batch::MutationCommitPhase::BeforeCommit,
    )?;

    wtx.commit().map_err(|e| e.to_string())?;
    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }

    if crashpoint == Some(MutationBatchCrashpoint::AfterCommitBeforeAck) {
        return Err("injected crash after mutation commit before acknowledgement".to_string());
    }
    crate::mutation_batch::apply_certification_fault(
        batch,
        crate::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck,
    )?;

    Ok(committed)
}

/// Apply one canonical MutationBatch/ChangeEnvelope's rows, governance material,
/// version/cursor/fence checks, terminal batch record, and outbox INTO an
/// already-open `wtx` — WITHOUT opening or committing the transaction. The caller
/// owns `begin_write`/`commit` and the post-commit audit-tail writeback, so multiple
/// batches (the ChangeEnvelope batch path) can share ONE transaction/fsync. Returns
/// `replayed: true` (having done reads only) on an exact request-identity hit. A
/// cross-modal retry may carry the current re-derived OCC version; all other
/// request identity fields remain exact.
///
/// `audited`: whether THIS commit should append tamper-evident audit-chain
/// entries for its operations. Only consulted when `authoritative_state_msgpack`
/// is `Some` (the Snapshot/RowDelta branches below) -- irrelevant otherwise
/// (compact-row/crossmodal/envelope callers pass a placeholder `true`; see each
/// call site).
///
/// Why this can't be re-derived from `batch.operations` alone: a state-backed
/// commit's `MutationOperation::method` is NOT the original causal `Method`
/// (e.g. `TouchNodes`) -- `mutation_batch::compile_methods`'s `opaque_state_operation`
/// unconditionally rewrites EVERY state-backed operation into the SAME opaque
/// digest receipt shape (`Method::ApplyMutation{event_type:
/// "authoritative_state_operation", ..}`), by design, so sensitive row payloads
/// never enter the durable batch/audit/outbox record. `audit::audit_line` always
/// recognizes that receipt shape as auditable, so by the time this function sees
/// `batch.operations`, the original method's OWN `eg_capabilities::policy(..).audited`
/// answer is unrecoverable from the operation alone. Callers must therefore
/// capture it themselves (typically `MutationPlan::audited`, already resolved from
/// the untranslated method at the top of `commit_mutation`/
/// `commit_conditional_mutation_async`) and pass it through here.
#[allow(clippy::too_many_arguments)]
fn apply_mutation_batch_in_wtx(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: Option<&ChangeEnvelope>,
    authoritative_state_msgpack: Option<&[u8]>,
    crossmodal: Option<CrossModalBatchRows<'_>>,
    result_msgpack: Option<&[u8]>,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
    crashpoint: Option<MutationBatchCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    // CX-EG-05 (CCN 353 -> decomposed): this function now only orchestrates the
    // phases below, each moved into its own named function (see immediately
    // after this function in the file). Every phase function is a literal,
    // behaviour-preserving relocation of the original inline code -- no
    // validation order, error message, or table-open sequence changed.
    let prepared = match prepare_and_validate_mutation_batch(
        wtx,
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal.as_ref(),
        crossmodal.is_some(),
        crypto,
    )? {
        MutationBatchPrepareOutcome::Replayed(commit) => return Ok(*commit),
        MutationBatchPrepareOutcome::Fresh(plan) => plan,
    };
    let MutationBatchPlan {
        native_terminal_work_item_cas: _native_terminal_work_item_cas,
        staged_state,
        integrity_policy_update,
        lifecycle,
        current_graph_version,
        proposed_fence,
    } = prepared;

    run_mutation_batch_crashpoint(batch, crashpoint, MutationBatchCrashpoint::BeforeRows)?;

    // Audit-tail updates are staged into the caller-owned `staged_audit_tail`
    // alongside the redb transaction. The caller advances the process cache only
    // AFTER `wtx.commit()`, so an injected/real failure that drops this transaction
    // (or a sibling envelope aborting the shared batch transaction) never leaves a
    // false tail.
    let generated_result = apply_state_dispatch_rows(
        wtx,
        graph_fname,
        staged_state.as_ref(),
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        audited,
        crossmodal.is_some(),
    )?;

    apply_crossmodal_rows_phase(wtx, graph_fname, crossmodal.as_ref(), crypto)?;

    apply_post_row_cleanup(
        wtx,
        graph_fname,
        batch,
        lifecycle.as_ref(),
        authoritative_state_msgpack.is_none(),
    )?;

    run_mutation_batch_crashpoint(
        batch,
        crashpoint,
        MutationBatchCrashpoint::AfterRowsBeforeMetadata,
    )?;

    let record = write_mutation_batch_commit_rows(
        &MutationRowCtx {
            wtx,
            graph_fname,
            batch,
            crypto,
        },
        MutationCommitReceipt {
            generated_result,
            result_msgpack,
            committed_at_ms,
        },
        MutationCommitVersioning {
            current_graph_version,
            proposed_fence,
            lifecycle,
            integrity_policy_update,
        },
        change,
    )?;

    // The transaction boundary (BeforeCommit crashpoint / certification fault /
    // `wtx.commit()` / audit-tail writeback / AfterCommitBeforeAck) is owned by the
    // caller so the shared-transaction batch path can commit MANY applied envelopes
    // with ONE fsync. This function only stages rows into `wtx`.
    Ok(MutationBatchCommit {
        record,
        replayed: false,
    })
}

fn compute_native_terminal_work_item_cas(batch: &MutationBatch) -> bool {
    batch.authoritative_state.is_none()
        && match batch.operations.first() {
            Some(first)
                if first.domain == MutationDomain::ControlPlane
                    && first.surface == MutationSurface::Job
                    && matches!(&first.method, Method::CommitWorkItemResult { .. }) =>
            {
                batch.operations[1..]
                    .iter()
                    .all(|op| matches!(&op.method, Method::AddNode { .. }))
            }
            Some(first) => {
                batch.operations.len() == 1
                    && first.domain == MutationDomain::ControlPlane
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

fn validate_mutation_batch_route_and_lowering(
    batch: &MutationBatch,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    staged_state_is_none: bool,
) -> Result<(), String> {
    if graph_fname != sanitize(&batch.graph) {
        return Err(format!(
            "mutation batch graph route mismatch: batch '{}' resolved to '{}' not '{}'",
            batch.graph,
            sanitize(&batch.graph),
            graph_fname
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
        if batch.operations.len() != 1 || graph_name != &batch.graph {
            return Err(
                "lifecycle MutationBatch must contain exactly one operation for its target graph"
                    .to_string(),
            );
        }
    }
    Ok(lifecycle)
}

fn read_current_mutation_graph_version(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
) -> Result<u64, String> {
    let versions = wtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    let found = versions.get(graph_fname).map_err(|e| e.to_string())?;
    Ok(found
        .map(|value| value.value())
        .unwrap_or(INITIAL_GRAPH_VERSION))
}

#[allow(clippy::too_many_arguments)]
/// The caller-owned identity keys a replayed batch must reproduce exactly.
fn mutation_batch_replay_identity_keys(stored: &MutationBatch, proposed: &MutationBatch) -> bool {
    stored.batch_id == proposed.batch_id
        && stored.context == proposed.context
        && stored.tenant == proposed.tenant
        && stored.graph == proposed.graph
        && stored.idempotency_key == proposed.idempotency_key
}

/// `expected_graph_version` is an OCC observation, not a caller-owned payload.
/// The original observation is retained in the durable record, while a replay
/// rebuilt after an ack-loss may carry the current authoritative observation.
/// Permit that one derived value only for cross-modal requests; every other
/// identity component stays byte/exact and a same-key different request still
/// conflicts.
fn mutation_batch_replay_expected_version_matches(
    stored: &MutationBatch,
    proposed: &MutationBatch,
    crossmodal_present: bool,
    current_graph_version: u64,
) -> bool {
    stored.expected_graph_version == proposed.expected_graph_version
        || (crossmodal_present && proposed.expected_graph_version == Some(current_graph_version))
}

/// Whether `proposed` is a faithful replay of the durably committed `stored`
/// batch.
///
/// `created_at_ms` is deliberately excluded: a network retry may rebuild the
/// identical batch later. Native resource retries use the dedicated typed
/// comparator (`native_resource_placement_replay_match`), which normalizes only
/// authority-owned `now_ms`.  Their placement epoch/fencing token may advance
/// only monotonically after leader failover; every caller-controlled resource
/// field, WorkItem fence, host revision, and outbox byte still must match.
/// Other operations retain exact placement bytes.
fn mutation_batch_replay_matches(
    stored: &MutationBatch,
    proposed: &MutationBatch,
    crossmodal_present: bool,
    current_graph_version: u64,
) -> Result<bool, String> {
    let operations_match =
        mutation_operations_retry_match(&stored.operations, &proposed.operations)?;
    let placement_matches = stored.placement_epoch == proposed.placement_epoch
        && stored.fencing_token == proposed.fencing_token;
    let placement_replay =
        native_resource_placement_replay_match(stored, proposed, operations_match);
    let outbox_match = native_retry_outbox_match(
        &stored.operations,
        &proposed.operations,
        &stored.outbox,
        &proposed.outbox,
        operations_match,
    )?;
    Ok(mutation_batch_replay_identity_keys(stored, proposed)
        && mutation_batch_replay_expected_version_matches(
            stored,
            proposed,
            crossmodal_present,
            current_graph_version,
        )
        && stored.authoritative_state == proposed.authoritative_state
        && outbox_match
        && (placement_matches || placement_replay)
        && operations_match)
}

/// A lifecycle replay may only be acknowledged while the stored batch is still
/// the graph's lifecycle head.
fn check_replay_lifecycle_head(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    stored_batch_id: &str,
    graph_name: &str,
) -> Result<(), String> {
    let heads = wtx
        .open_table(MUTATION_LIFECYCLE_HEAD)
        .map_err(|e| e.to_string())?;
    let current = heads
        .get(graph_fname)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_string());
    if current.as_deref() != Some(stored_batch_id) {
        return Err(format!(
            "STALE_FENCE: lifecycle batch '{}' is no longer current for graph '{}'",
            stored_batch_id, graph_name
        ));
    }
    Ok(())
}

/// An envelope replay must reproduce the committed envelope byte-for-byte.
fn check_replay_envelope_matches(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    change: &ChangeEnvelope,
    graph_name: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let envelopes = wtx
        .open_table(CHANGE_ENVELOPES)
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

#[allow(clippy::too_many_arguments)]
fn check_idempotency_replay(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: Option<&ChangeEnvelope>,
    crossmodal_present: bool,
    current_graph_version: u64,
    lifecycle: Option<&(bool, String, Option<GraphType>)>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<MutationBatchCommit>, String> {
    let idem = wtx
        .open_table(MUTATION_IDEMPOTENCY)
        .map_err(|e| e.to_string())?;
    let existing_id = idem
        .get((
            batch.tenant.as_str(),
            graph_fname,
            batch.idempotency_key.as_str(),
        ))
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_string());
    let Some(existing_id) = existing_id else {
        return Ok(None);
    };
    let records = wtx
        .open_table(MUTATION_BATCHES)
        .map_err(|e| e.to_string())?;
    let stored = records
        .get(existing_id.as_str())
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!(
                "corrupt mutation idempotency index: '{}' has no batch record",
                existing_id
            )
        })?;
    let bytes = crypto.unseal(stored.value())?;
    let record = decode_mutation_batch_record(&bytes)?;
    if !mutation_batch_replay_matches(
        &record.batch,
        batch,
        crossmodal_present,
        current_graph_version,
    )? {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: key '{}' is already committed as batch '{}'",
            batch.idempotency_key, record.batch.batch_id
        ));
    }
    if lifecycle.is_some() {
        check_replay_lifecycle_head(
            wtx,
            graph_fname,
            record.batch.batch_id.as_str(),
            batch.graph.as_str(),
        )?;
    }
    if let Some(change) = change {
        check_replay_envelope_matches(wtx, graph_fname, change, batch.graph.as_str(), crypto)?;
    }
    Ok(Some(MutationBatchCommit {
        record,
        replayed: true,
    }))
}

fn check_batch_id_uniqueness(wtx: &redb::WriteTransaction, batch_id: &str) -> Result<(), String> {
    let records = wtx
        .open_table(MUTATION_BATCHES)
        .map_err(|e| e.to_string())?;
    if records.get(batch_id).map_err(|e| e.to_string())?.is_some() {
        return Err(format!(
                "IDEMPOTENCY_CONFLICT: batch_id '{}' is already committed under a different idempotency scope or key",
                batch_id
            ));
    }
    Ok(())
}

/// Precondition 1: the envelope id must not already be committed for this graph.
fn check_change_envelope_not_committed(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    change: &ChangeEnvelope,
) -> Result<(), String> {
    let envelopes = wtx
        .open_table(CHANGE_ENVELOPES)
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let versions = wtx
        .open_table(CONTENT_VERSIONS)
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    tenant: &str,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let cursors = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    tenant: &str,
    change: Option<&ChangeEnvelope>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(change) = change else {
        return Ok(());
    };
    check_change_envelope_not_committed(wtx, graph_fname, change)?;
    check_change_content_version_precondition(wtx, graph_fname, tenant, change, crypto)?;
    if let Some(cursor) = &change.cursor {
        check_change_cursor_precondition(wtx, graph_fname, tenant, cursor, crypto)?;
    }
    Ok(())
}

fn check_occ_version_and_fence(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    current_graph_version: u64,
    lifecycle_is_none: bool,
    native_terminal_work_item_cas: bool,
    crypto: DurableCrypto<'_>,
) -> Result<DurableMutationFence, String> {
    if let Some(expected) = batch.expected_graph_version {
        if expected != current_graph_version {
            return Err(format!(
                "STALE_VERSION: graph '{}' expected version {} but authoritative version is {}",
                batch.graph, expected, current_graph_version
            ));
        }
    } else if lifecycle_is_none && !native_terminal_work_item_cas {
        return Err(
            "authoritative non-lifecycle MutationBatch requires expected_graph_version".to_string(),
        );
    }

    let current_fence = {
        let fences = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
        let value = fences
            .get(graph_fname)
            .map_err(|e| e.to_string())?
            .map(|value| {
                let bytes = crypto.unseal(value.value())?;
                decode_durable::<DurableMutationFence>(&bytes)
            })
            .transpose()?
            .unwrap_or(DurableMutationFence {
                placement_epoch: 0,
                fencing_token: 0,
            });
        value
    };
    let proposed_fence = DurableMutationFence {
        placement_epoch: batch.placement_epoch,
        fencing_token: batch.fencing_token.unwrap_or(0),
    };
    if proposed_fence.placement_epoch < current_fence.placement_epoch
        || (proposed_fence.placement_epoch == current_fence.placement_epoch
            && proposed_fence.fencing_token < current_fence.fencing_token)
    {
        return Err(format!(
            "STALE_FENCE: graph '{}' route ({},{}) is older than ({},{})",
            batch.graph,
            proposed_fence.placement_epoch,
            proposed_fence.fencing_token,
            current_fence.placement_epoch,
            current_fence.fencing_token,
        ));
    }
    Ok(proposed_fence)
}

#[allow(clippy::too_many_arguments)]
fn apply_snapshot_state(
    wtx: &redb::WriteTransaction,
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
    work_item_capability::clear_graph_rows_in_wtx(wtx, graph_fname)?;
    development_lane::validate_lane_links_in_wtx(wtx, graph_fname, &incoming_nodes, crypto)?;
    let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
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
    let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    semantic
        .insert(graph_fname, sealed_semantic.as_ref())
        .map_err(|e| e.to_string())?;

    #[cfg(feature = "security")]
    if audited {
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    delta: &crate::graph_delta::GraphRowDelta,
    batch: &MutationBatch,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    let _ = audited;
    let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let native_work_items = wtx
        .open_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    for method in delta.operations() {
        apply_method_rows(
            graph_fname,
            method,
            &mut nodes,
            &mut edges,
            &mut ledger,
            &mut semantic,
            &native_work_items,
            crypto,
        )?;
    }
    if let Some((_, retain, append)) = delta.ledger_patch() {
        let suffix_keys: Vec<u64> = ledger
            .range((graph_fname, retain)..)
            .map_err(|error| error.to_string())?
            .map_while(|row| match row {
                Ok((key, _)) if key.value().0 == graph_fname => Some(Ok(key.value().1)),
                Ok(_) => None,
                Err(error) => Some(Err(error.to_string())),
            })
            .collect::<Result<_, _>>()?;
        for sequence in suffix_keys {
            ledger
                .remove((graph_fname, sequence))
                .map_err(|error| error.to_string())?;
        }
        for (offset, line) in append.iter().enumerate() {
            let sequence = retain
                .checked_add(offset as u64)
                .ok_or_else(|| "graph row delta ledger sequence overflow".to_string())?;
            ledger
                .insert((graph_fname, sequence), line.as_str())
                .map_err(|error| error.to_string())?;
        }
    }
    drop(nodes);
    drop(edges);
    drop(ledger);
    drop(semantic);
    drop(native_work_items);
    development_lane::validate_current_lane_links_in_wtx(wtx, graph_fname, crypto)?;

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
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
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
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
        redb::Table<'txn, (&'static str, u64), &'static str>,
        redb::Table<'txn, &'static str, &'static [u8]>,
        redb::Table<'txn, &'static str, u64>,
    ),
    String,
> {
    let nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let native_work_items = wtx
        .open_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let command_sequences = wtx
        .open_table(WORK_ITEM_COMMAND_SEQUENCE)
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
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str, &'static str), &'static str>,
        redb::Table<'txn, (&'static str, &'static str, u64), &'static str>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str), &'static str>,
    ),
    String,
> {
    let resource_reservations = wtx
        .open_table(RESOURCE_RESERVATIONS)
        .map_err(|e| e.to_string())?;
    let resource_tenant_index = wtx
        .open_table(RESOURCE_RESERVATION_TENANT_INDEX)
        .map_err(|e| e.to_string())?;
    let resource_attempts = wtx
        .open_table(RESOURCE_RESERVATION_ATTEMPTS)
        .map_err(|e| e.to_string())?;
    let resource_hosts = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
    let resource_exclusivity = wtx
        .open_table(RESOURCE_EXCLUSIVITY)
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
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str), u64>,
        redb::Table<'txn, (&'static str, &'static str, &'static str), u64>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let resource_fairness = wtx
        .open_table(RESOURCE_FAIRNESS)
        .map_err(|e| e.to_string())?;
    let resource_concurrency = wtx
        .open_table(RESOURCE_CONCURRENCY)
        .map_err(|e| e.to_string())?;
    let resource_anti_affinity = wtx
        .open_table(RESOURCE_ANTI_AFFINITY)
        .map_err(|e| e.to_string())?;
    let resource_disk_policies = wtx
        .open_table(RESOURCE_DISK_POLICIES)
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
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str, u64), &'static str>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<
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
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let lane_holds = wtx
        .open_table(development_lane::HOLDS)
        .map_err(|e| e.to_string())?;
    let lane_work_item_index = wtx
        .open_table(development_lane::WORK_ITEM_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_counters = wtx
        .open_table(development_lane::COUNTERS)
        .map_err(|e| e.to_string())?;
    let lane_pressure_index = wtx
        .open_table(development_lane::PRESSURE_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_policies = wtx
        .open_table(development_lane::POLICIES)
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
    if work_item_result_ops == 1
        && batch.operations.iter().any(|op| {
            !matches!(
                &op.method,
                Method::CommitWorkItemResult { .. } | Method::AddNode { .. }
            )
        })
    {
        return Err(
            "a CommitWorkItemResult MutationBatch may only carry additional AddNode \
             (provenance) operations"
                .to_string(),
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_native_clear_or_delete_graph_rows(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    command_sequences: &mut redb::Table<&str, u64>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    lane_holds: &mut redb::Table<(&str, &str), &[u8]>,
    lane_work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    lane_counters: &mut redb::Table<(&str, &str), &[u8]>,
    lane_pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
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
        wtx,
        graph_fname,
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
        crypto,
    )?;
    capacity_lease::clear_graph_rows(wtx, graph_fname)?;
    work_item_capability::clear_graph_rows_in_wtx_with_native(wtx, graph_fname, native_work_items)?;
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

fn apply_native_submit_work_item_operation(
    graph_fname: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
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
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "SubmitWorkItem MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    let payload = crate::protocol::ResultPayload::raw(&result);
    *generated_result = Some(rmp_serde::to_vec_named(&payload).map_err(|e| e.to_string())?);
    Ok(())
}

fn apply_native_submit_work_items_operation(
    graph_fname: &str,
    request: &eg_types::native_control::SubmitWorkItemsRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
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
    if generated_result.is_some() || batch.operations.len() != 1 {
        return Err(
            "SubmitWorkItems MutationBatch must contain exactly one result-producing operation"
                .to_string(),
        );
    }
    let payload = crate::protocol::ResultPayload::raw(&result);
    *generated_result = Some(rmp_serde::to_vec_named(&payload).map_err(|e| e.to_string())?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_native_work_item_family_operation(
    graph_fname: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    lane_holds: &mut redb::Table<(&str, &str), &[u8]>,
    lane_work_item_index: &redb::Table<(&str, &str, u64), &str>,
    lane_counters: &mut redb::Table<(&str, &str), &[u8]>,
    lane_pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    batch: &MutationBatch,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(
        graph_fname,
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
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    lane_holds: &mut redb::Table<(&str, &str), &[u8]>,
    lane_work_item_index: &redb::Table<(&str, &str, u64), &str>,
    lane_counters: &mut redb::Table<(&str, &str), &[u8]>,
    lane_pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    generated_result: &mut Option<Vec<u8>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = apply_work_item_rows(
        graph_fname,
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
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    operation: &MutationOperation,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    generated_result: &mut Option<Vec<u8>>,
    crossmodal_present: bool,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    lane_holds: &mut redb::Table<(&str, &str), &[u8]>,
    lane_work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    lane_counters: &mut redb::Table<(&str, &str), &[u8]>,
    lane_pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut redb::Table<(&str, &str), &[u8]>,
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
            wtx,
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
        method => apply_method_rows(
            graph_fname,
            method,
            nodes,
            edges,
            ledger,
            semantic,
            native_work_items,
            crypto,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_native_operation_rows_loop(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    generated_result: &mut Option<Vec<u8>>,
    crossmodal_present: bool,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    lane_holds: &mut redb::Table<(&str, &str), &[u8]>,
    lane_work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    lane_counters: &mut redb::Table<(&str, &str), &[u8]>,
    lane_pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    lane_policies: &mut redb::Table<(&str, &str), &[u8]>,
    #[cfg(feature = "security")] audit: &mut redb::Table<(&str, u64), &[u8]>,
) -> Result<(), String> {
    for operation in &batch.operations {
        apply_one_native_operation_row(
            wtx,
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
        wtx,
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
    ) = open_native_operation_graph_tables(wtx)?;
    let (
        mut resource_reservations,
        mut resource_tenant_index,
        mut resource_attempts,
        mut resource_hosts,
        mut resource_exclusivity,
    ) = open_native_operation_resource_tables_a(wtx)?;
    let (
        mut resource_fairness,
        mut resource_concurrency,
        mut resource_anti_affinity,
        mut resource_disk_policies,
    ) = open_native_operation_resource_tables_b(wtx)?;
    let (
        mut lane_holds,
        mut lane_work_item_index,
        mut lane_counters,
        mut lane_pressure_index,
        mut lane_policies,
    ) = open_native_operation_lane_tables(wtx)?;
    #[cfg(feature = "security")]
    let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;

    validate_native_operations_commit_work_item_result_shape(batch)?;

    apply_native_operation_rows_loop(
        wtx,
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
    development_lane::validate_current_lane_links_in_wtx(wtx, graph_fname, crypto)?;
    Ok(())
}

/// The compact graph/control path can replace or remove a linked WorkItem
/// just as a snapshot/row-delta can (`clears_semantic`); a lifecycle DeleteGraph
/// additionally purges the PRIOR incarnation's mutation-authority rows before
/// this delete writes its own fresh tombstone record.
fn apply_post_row_cleanup(
    wtx: &redb::WriteTransaction,
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
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        semantic.remove(graph_fname).map_err(|e| e.to_string())?;
    }
    if matches!(lifecycle, Some((false, _, _))) {
        clear_change_material_rows(wtx, graph_fname)?;
        // D-P0-U04: drop the PRIOR incarnation's mutation-authority rows
        // (idempotency keys, outbox/delivery, projection cursors, fence,
        // lifecycle head, graph version) before this delete writes its own
        // fresh tombstone record below -- otherwise a same-name recreate can
        // collide with or attempt to decrypt an old-incarnation mutation
        // record after key loss/rotation.
        clear_mutation_authority_rows(wtx, graph_fname)?;
    }
    Ok(())
}

/// Bundled result of `prepare_and_validate_mutation_batch`'s non-replay path
/// (CX-EG-05 decomposition of the former single 353-CCN `apply_mutation_batch_in_wtx`).
struct MutationBatchPlan {
    native_terminal_work_item_cas: bool,
    staged_state: Option<AuthoritativeGraphState>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
    current_graph_version: u64,
    proposed_fence: DurableMutationFence,
}

enum MutationBatchPrepareOutcome {
    /// An exact request-identity hit: `record` was read, not written, and the
    /// caller must return it directly without touching any row.
    Replayed(Box<MutationBatchCommit>),
    Fresh(MutationBatchPlan),
}

/// Every validation/idempotency/OCC/fence check that must pass BEFORE a single
/// row is touched, bundled into one call so the top-level orchestrator pays
/// only one `?` for the whole prelude instead of one per sub-check.
#[allow(clippy::too_many_arguments)]
fn prepare_and_validate_mutation_batch(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: Option<&ChangeEnvelope>,
    authoritative_state_msgpack: Option<&[u8]>,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    crossmodal_present: bool,
    crypto: DurableCrypto<'_>,
) -> Result<MutationBatchPrepareOutcome, String> {
    batch.validate()?;
    let native_terminal_work_item_cas = compute_native_terminal_work_item_cas(batch);
    let staged_state = resolve_mutation_authoritative_state(batch, authoritative_state_msgpack)?;
    let integrity_policy_update = resolve_integrity_policy_update(staged_state.as_ref());
    validate_mutation_batch_route_and_lowering(
        batch,
        graph_fname,
        crossmodal,
        staged_state.is_none(),
    )?;
    let lifecycle = detect_and_validate_lifecycle(batch)?;
    let current_graph_version = read_current_mutation_graph_version(wtx, graph_fname)?;
    if let Some(commit) = check_idempotency_replay(
        wtx,
        graph_fname,
        batch,
        change,
        crossmodal_present,
        current_graph_version,
        lifecycle.as_ref(),
        crypto,
    )? {
        return Ok(MutationBatchPrepareOutcome::Replayed(Box::new(commit)));
    }
    check_batch_id_uniqueness(wtx, batch.batch_id.as_str())?;
    validate_change_envelope_preconditions(
        wtx,
        graph_fname,
        batch.tenant.as_str(),
        change,
        crypto,
    )?;
    let proposed_fence = check_occ_version_and_fence(
        wtx,
        graph_fname,
        batch,
        current_graph_version,
        lifecycle.is_none(),
        native_terminal_work_item_cas,
        crypto,
    )?;
    Ok(MutationBatchPrepareOutcome::Fresh(MutationBatchPlan {
        native_terminal_work_item_cas,
        staged_state,
        integrity_policy_update,
        lifecycle,
        current_graph_version,
        proposed_fence,
    }))
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
    wtx: &redb::WriteTransaction,
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
                wtx,
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
                wtx,
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
                    wtx,
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if let Some(rows) = crossmodal {
        if !rows.methods.is_empty() {
            let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
            let mut native_work_items = wtx
                .open_table(work_item_capability::NATIVE_WORK_ITEMS)
                .map_err(|e| e.to_string())?;
            let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
            let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            let mut resource_reservations = wtx
                .open_table(RESOURCE_RESERVATIONS)
                .map_err(|e| e.to_string())?;
            let mut resource_tenant_index = wtx
                .open_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .map_err(|e| e.to_string())?;
            let mut resource_attempts = wtx
                .open_table(RESOURCE_RESERVATION_ATTEMPTS)
                .map_err(|e| e.to_string())?;
            let mut resource_hosts = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
            let mut resource_exclusivity = wtx
                .open_table(RESOURCE_EXCLUSIVITY)
                .map_err(|e| e.to_string())?;
            let mut resource_fairness = wtx
                .open_table(RESOURCE_FAIRNESS)
                .map_err(|e| e.to_string())?;
            let mut resource_concurrency = wtx
                .open_table(RESOURCE_CONCURRENCY)
                .map_err(|e| e.to_string())?;
            let mut resource_anti_affinity = wtx
                .open_table(RESOURCE_ANTI_AFFINITY)
                .map_err(|e| e.to_string())?;
            let mut resource_disk_policies = wtx
                .open_table(RESOURCE_DISK_POLICIES)
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
                development_lane::clear_native_graph_rows_in_wtx(wtx, graph_fname, crypto)?;
                capacity_lease::clear_graph_rows(wtx, graph_fname)?;
                work_item_capability::clear_graph_rows_in_wtx_with_native(
                    wtx,
                    graph_fname,
                    &mut native_work_items,
                )?;
            }
            for method in rows.methods {
                apply_method_rows(
                    graph_fname,
                    method,
                    &mut nodes,
                    &mut edges,
                    &mut ledger,
                    &mut semantic,
                    &native_work_items,
                    crypto,
                )?;
            }
            drop(nodes);
            drop(edges);
            drop(ledger);
            drop(semantic);
            drop(native_work_items);
            development_lane::validate_current_lane_links_in_wtx(wtx, graph_fname, crypto)?;
        }
        if rows
            .methods
            .iter()
            .any(|method| matches!(method, Method::ClearGraph))
        {
            let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            semantic.remove(graph_fname).map_err(|e| e.to_string())?;
        }
        apply_crossmodal_projection_rows(
            wtx,
            graph_fname,
            rows.vectors,
            rows.blob_refs,
            rows.measurements,
            crypto,
        )?;
        // Blob/vector projection is also an in-transaction node/semantic
        // replacement surface.  Re-run the lane policy after it so the final
        // image, not only the pre-projection graph rows, is what can commit.
        development_lane::validate_current_lane_links_in_wtx(wtx, graph_fname, crypto)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn mutation_batch_next_graph_version(
    batch: &MutationBatch,
    current_graph_version: u64,
) -> Result<u64, String> {
    match batch.authoritative_state.as_ref() {
        Some(state) => Ok(state.target_graph_version),
        None => current_graph_version
            .checked_add(1)
            .ok_or_else(|| "mutation graph version overflow".to_string()),
    }
}

fn write_mutation_batch_identity_rows(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    sealed_record: &[u8],
) -> Result<(), String> {
    let mut records = wtx
        .open_table(MUTATION_BATCHES)
        .map_err(|e| e.to_string())?;
    records
        .insert(batch.batch_id.as_str(), sealed_record)
        .map_err(|e| e.to_string())?;

    let mut idem = wtx
        .open_table(MUTATION_IDEMPOTENCY)
        .map_err(|e| e.to_string())?;
    idem.insert(
        (
            batch.tenant.as_str(),
            graph_fname,
            batch.idempotency_key.as_str(),
        ),
        batch.batch_id.as_str(),
    )
    .map_err(|e| e.to_string())?;

    Ok(())
}

/// The four values EVERY durable row writer on the MutationBatch commit path
/// needs: the open write transaction, the graph file the rows belong to, the
/// batch being committed, and the (Copy) sealing handle. Bundled so the row
/// writers stay inside clippy's parameter cap; each field is exactly the borrow
/// the callers used to pass positionally, so no value any writer sees changes.
#[derive(Clone, Copy)]
struct MutationRowCtx<'a> {
    wtx: &'a redb::WriteTransaction,
    graph_fname: &'a str,
    batch: &'a MutationBatch,
    crypto: DurableCrypto<'a>,
}

/// The committed record's own payload/timestamp inputs, bundled out of
/// [`write_mutation_batch_commit_rows`]'s parameter list.
struct MutationCommitReceipt<'a> {
    generated_result: Option<Vec<u8>>,
    result_msgpack: Option<&'a [u8]>,
    committed_at_ms: u64,
}

/// The already-validated `MutationBatchPlan` values the commit-row writers
/// consume, bundled out of [`write_mutation_batch_commit_rows`]'s parameter list.
struct MutationCommitVersioning {
    current_graph_version: u64,
    proposed_fence: DurableMutationFence,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
}

fn write_mutation_batch_version_and_fence_rows(
    ctx: &MutationRowCtx<'_>,
    next_graph_version: u64,
    proposed_fence: &DurableMutationFence,
    lifecycle_is_some: bool,
) -> Result<(), String> {
    let MutationRowCtx {
        wtx,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    let mut versions = wtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    versions
        .insert(graph_fname, next_graph_version)
        .map_err(|e| e.to_string())?;

    let fence_bytes = rmp_serde::to_vec_named(proposed_fence).map_err(|e| e.to_string())?;
    let sealed_fence = crypto.seal(&fence_bytes);
    let mut fences = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
    fences
        .insert(graph_fname, sealed_fence.as_ref())
        .map_err(|e| e.to_string())?;

    if lifecycle_is_some {
        let mut heads = wtx
            .open_table(MUTATION_LIFECYCLE_HEAD)
            .map_err(|e| e.to_string())?;
        heads
            .insert(graph_fname, batch.batch_id.as_str())
            .map_err(|e| e.to_string())?;
    }

    Ok(())
}

fn write_mutation_batch_core_rows(
    ctx: &MutationRowCtx<'_>,
    sealed_record: &[u8],
    next_graph_version: u64,
    proposed_fence: &DurableMutationFence,
    lifecycle_is_some: bool,
) -> Result<(), String> {
    write_mutation_batch_identity_rows(ctx.wtx, ctx.graph_fname, ctx.batch, sealed_record)?;
    write_mutation_batch_version_and_fence_rows(
        ctx,
        next_graph_version,
        proposed_fence,
        lifecycle_is_some,
    )?;
    Ok(())
}

// Every operation receives a canonical committed event even if a surface
// supplied no bespoke projection intent.  This makes rebuild/replay a
// property of the commit kernel, not handler discipline.
fn write_mutation_outbox_operation_rows(
    wtx: &redb::WriteTransaction,
    batch: &MutationBatch,
    next_graph_version: u64,
    next_ordinal: &mut u32,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    for operation in &batch.operations {
        let payload = rmp_serde::to_vec_named(operation).map_err(|e| e.to_string())?;
        let out = MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch.batch_id.clone(),
            ordinal: *next_ordinal,
            tenant: batch.tenant.clone(),
            graph: batch.graph.clone(),
            version_scope: MutationVersionScope::Graph,
            source_graph_version: next_graph_version,
            intent: MutationOutboxIntent {
                topic: "engine.mutation.committed".to_string(),
                key: batch.batch_id.clone(),
                payload,
                headers: Default::default(),
            },
            created_at_ms: batch.created_at_ms,
        };
        out.validate()?;
        let bytes = rmp_serde::to_vec_named(&out).map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&bytes);
        outbox
            .insert((batch.batch_id.as_str(), *next_ordinal), sealed.as_ref())
            .map_err(|e| e.to_string())?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| "mutation outbox ordinal overflow".to_string())?;
    }
    Ok(())
}

fn write_mutation_outbox_intent_rows(
    wtx: &redb::WriteTransaction,
    batch: &MutationBatch,
    next_graph_version: u64,
    next_ordinal: &mut u32,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    for intent in &batch.outbox {
        let out = MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch.batch_id.clone(),
            ordinal: *next_ordinal,
            tenant: batch.tenant.clone(),
            graph: batch.graph.clone(),
            version_scope: MutationVersionScope::Graph,
            source_graph_version: next_graph_version,
            intent: intent.clone(),
            created_at_ms: batch.created_at_ms,
        };
        out.validate()?;
        let bytes = rmp_serde::to_vec_named(&out).map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&bytes);
        outbox
            .insert((batch.batch_id.as_str(), *next_ordinal), sealed.as_ref())
            .map_err(|e| e.to_string())?;
        *next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| "mutation outbox ordinal overflow".to_string())?;
    }
    Ok(())
}

// The envelope event is metadata-only: material payloads remain in
// their encrypted authoritative rows and are retrieved under policy.
fn write_change_committed_outbox_row(
    wtx: &redb::WriteTransaction,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    next_graph_version: u64,
    next_ordinal: &mut u32,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let event = serde_json::json!({
        "schema": "epistemic.change.committed.v1",
        "envelope_id": change.envelope_id.as_str(),
        "batch_id": batch.batch_id.as_str(),
        "tenant": batch.tenant.as_str(),
        "graph": batch.graph.as_str(),
        "object_id": change.content_version.object_id.as_str(),
        "content_digest": change.content_version.digest.as_str(),
    });
    let event_payload = rmp_serde::to_vec_named(&event).map_err(|e| e.to_string())?;
    let out = MutationOutboxRecord {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch.batch_id.clone(),
        ordinal: *next_ordinal,
        tenant: batch.tenant.clone(),
        graph: batch.graph.clone(),
        version_scope: MutationVersionScope::Graph,
        source_graph_version: next_graph_version,
        intent: MutationOutboxIntent {
            topic: "engine.change.committed".to_string(),
            key: change.envelope_id.clone(),
            payload: event_payload,
            headers: Default::default(),
        },
        created_at_ms: batch.created_at_ms,
    };
    out.validate()?;
    let bytes = rmp_serde::to_vec_named(&out).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    outbox
        .insert((batch.batch_id.as_str(), *next_ordinal), sealed.as_ref())
        .map_err(|e| e.to_string())?;
    *next_ordinal = next_ordinal
        .checked_add(1)
        .ok_or_else(|| "change envelope outbox ordinal overflow".to_string())?;
    Ok(())
}

fn write_change_envelope_and_content_version_rows(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
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
    let mut envelopes = wtx
        .open_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    envelopes
        .insert((graph_fname, change.envelope_id.as_str()), sealed.as_ref())
        .map_err(|e| e.to_string())?;

    let bytes = rmp_serde::to_vec_named(&change.content_version).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut versions = wtx
        .open_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    versions
        .insert(
            (
                graph_fname,
                batch.tenant.as_str(),
                change.content_version.object_id.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn write_change_committed_outbox_and_envelope_rows(
    ctx: &MutationRowCtx<'_>,
    change: &ChangeEnvelope,
    next_graph_version: u64,
    committed_at_ms: u64,
    next_ordinal: &mut u32,
) -> Result<(), String> {
    let MutationRowCtx {
        wtx,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    write_change_committed_outbox_row(
        wtx,
        batch,
        change,
        next_graph_version,
        next_ordinal,
        crypto,
    )?;
    write_change_envelope_and_content_version_rows(
        wtx,
        graph_fname,
        batch,
        change,
        committed_at_ms,
        crypto,
    )?;
    Ok(())
}

fn write_change_cursor_row(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(cursor).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut cursors = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
    cursors
        .insert(
            (
                graph_fname,
                batch.tenant.as_str(),
                cursor.source.as_str(),
                cursor.partition.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn write_change_material_blobs(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut blobs = wtx.open_table(CHANGE_BLOBS).map_err(|e| e.to_string())?;
    for blob in &change.blobs {
        let key = (graph_fname, batch.tenant.as_str(), blob.blob_id.as_str());
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut features = wtx.open_table(CHANGE_FEATURES).map_err(|e| e.to_string())?;
    for feature in &change.features {
        let key = (
            graph_fname,
            batch.tenant.as_str(),
            feature.feature_id.as_str(),
        );
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut evidence = wtx.open_table(CHANGE_EVIDENCE).map_err(|e| e.to_string())?;
    for item in &change.evidence {
        let key = (
            graph_fname,
            batch.tenant.as_str(),
            item.evidence_id.as_str(),
        );
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut policies = wtx.open_table(CHANGE_POLICIES).map_err(|e| e.to_string())?;
    for policy in &change.policies {
        let key = (
            graph_fname,
            batch.tenant.as_str(),
            policy.policy_id.as_str(),
        );
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
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut lineage = wtx.open_table(CHANGE_LINEAGE).map_err(|e| e.to_string())?;
    for item in &change.lineage {
        let key = (graph_fname, batch.tenant.as_str(), item.lineage_id.as_str());
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

fn apply_change_envelope_commit_rows(
    ctx: &MutationRowCtx<'_>,
    change: &ChangeEnvelope,
    next_graph_version: u64,
    committed_at_ms: u64,
    next_ordinal: &mut u32,
) -> Result<(), String> {
    let MutationRowCtx {
        wtx,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    write_change_committed_outbox_and_envelope_rows(
        ctx,
        change,
        next_graph_version,
        committed_at_ms,
        next_ordinal,
    )?;

    if let Some(cursor) = &change.cursor {
        write_change_cursor_row(wtx, graph_fname, batch, cursor, crypto)?;
    }

    write_change_material_blobs(wtx, graph_fname, batch, change, crypto)?;
    write_change_material_features(wtx, graph_fname, batch, change, crypto)?;
    write_change_material_evidence(wtx, graph_fname, batch, change, crypto)?;
    write_change_material_policies(wtx, graph_fname, batch, change, crypto)?;
    write_change_material_lineage(wtx, graph_fname, batch, change, crypto)?;

    debug_assert_eq!(
        *next_ordinal as usize,
        batch.operations.len() + batch.outbox.len() + 1
    );
    Ok(())
}

fn resolve_default_graph_meta_update(
    meta: &redb::Table<&str, &[u8]>,
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
            &batch.graph,
            GraphType::Global,
            &batch.batch_id,
            policy.and_then(Option::as_ref),
        )?),
    };
    Ok(encoded)
}

fn write_mutation_batch_graph_meta_row(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    batch: &MutationBatch,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
) -> Result<(), String> {
    let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
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

fn write_mutation_batch_commit_rows(
    ctx: &MutationRowCtx<'_>,
    receipt: MutationCommitReceipt<'_>,
    versioning: MutationCommitVersioning,
    change: Option<&ChangeEnvelope>,
) -> Result<MutationBatchRecord, String> {
    let MutationRowCtx {
        wtx,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    let MutationCommitReceipt {
        generated_result,
        result_msgpack,
        committed_at_ms,
    } = receipt;
    let MutationCommitVersioning {
        current_graph_version,
        proposed_fence,
        lifecycle,
        integrity_policy_update,
    } = versioning;
    let record = MutationBatchRecord {
        batch: batch.clone(),
        status: MutationBatchStatus::Committed,
        result_msgpack: generated_result.or_else(|| result_msgpack.map(ToOwned::to_owned)),
        committed_at_ms,
    };
    let record_bytes = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
    let sealed_record = crypto.seal(&record_bytes);

    let next_graph_version = mutation_batch_next_graph_version(batch, current_graph_version)?;

    write_mutation_batch_core_rows(
        ctx,
        sealed_record.as_ref(),
        next_graph_version,
        &proposed_fence,
        lifecycle.is_some(),
    )?;

    let mut next_ordinal = 0u32;
    write_mutation_outbox_operation_rows(
        wtx,
        batch,
        next_graph_version,
        &mut next_ordinal,
        crypto,
    )?;
    write_mutation_outbox_intent_rows(wtx, batch, next_graph_version, &mut next_ordinal, crypto)?;

    if let Some(change) = change {
        apply_change_envelope_commit_rows(
            ctx,
            change,
            next_graph_version,
            committed_at_ms,
            &mut next_ordinal,
        )?;
    }

    write_mutation_batch_graph_meta_row(
        wtx,
        graph_fname,
        batch,
        lifecycle,
        integrity_policy_update,
    )?;

    Ok(record)
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
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
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

// RMDD-27 native reservation bounds.  These are deliberately independent of
// the much larger durable MessagePack budget: reservation strings and status
// scans are public control-plane inputs and must remain cheap to validate.
const MAX_RESOURCE_TEXT: usize = 256;
const MAX_RESOURCE_LABELS: usize = 128;
const MAX_RESOURCE_STATUS_LIMIT: usize = 1_000;
const MAX_RESOURCE_STATUS_SCAN: usize = 100_000;
// ResourceHostUpdate/Status schemas expose at most 128 versioned disk-policy
// rows. Admission uses the same bound before creating a new host+policy key;
// otherwise a native peer could persist a snapshot that generated clients
// cannot decode or force an unbounded policy scan during reconciliation.
const MAX_RESOURCE_HOST_DISK_POLICIES: usize = 128;
// Graph clear/delete is an administrative operation, but its drain check must
// remain bounded in allocation even if a hostile or corrupted graph accumulated
// a large terminal history.  Deletion proceeds in bounded key chunks from an
// in-transaction cursor; the cap is not a lifetime limit on tombstone history.
const MAX_RESOURCE_CLEAR_SCAN: usize = 100_000;
const MAX_RESOURCE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const RESOURCE_HEARTBEAT_GRACE_MS: u64 = 120_000;
const MAX_RESOURCE_DIMENSION: u64 = 1_000_000_000_000;

fn resource_text(value: &str, name: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_RESOURCE_TEXT {
        return Err(format!(
            "{name} is empty or exceeds {MAX_RESOURCE_TEXT} bytes"
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{name} contains a control character"));
    }
    Ok(())
}

fn resource_labels(values: &[String], name: &str) -> Result<(), String> {
    if values.len() > MAX_RESOURCE_LABELS {
        return Err(format!("{name} exceeds {MAX_RESOURCE_LABELS} entries"));
    }
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    for value in values {
        resource_text(value, name)?;
        if !seen.insert(value) {
            return Err(format!("{name} contains a duplicate value"));
        }
    }
    Ok(())
}

fn resource_disk_policy_blocked(
    previously_blocked: bool,
    predicted_used_mib: u64,
    low_watermark_mib: Option<u64>,
    high_watermark_mib: Option<u64>,
) -> bool {
    if previously_blocked {
        // RMDD-08 watermarks are USED MiB.  A blocked policy reopens only at
        // or below low; with low==high this branch must not immediately fall
        // through to the open-state high-watermark check.
        low_watermark_mib.is_none_or(|low| predicted_used_mib > low)
    } else {
        high_watermark_mib.is_some_and(|high| predicted_used_mib >= high)
    }
}

fn resource_fingerprint(value: &str, name: &str) -> Result<(), String> {
    resource_text(value, name)?;
    let bytes = value.as_bytes();
    if bytes.len() != 67 || &bytes[..3] != b"v1:" || !bytes[3..].iter().all(u8::is_ascii_hexdigit) {
        return Err(format!("{name} must be v1:<64 lowercase hex characters>"));
    }
    if bytes[3..].iter().any(u8::is_ascii_uppercase) {
        return Err(format!("{name} must use lowercase hex"));
    }
    Ok(())
}

/// Phase 1 of `resource_validate_request`: the identity text fields and the
/// canonical-integer `profile_version`.
fn resource_validate_request_identity(request: &ResourceReservationRequest) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource tenant_ref")?;
    resource_text(&request.work_item_id, "resource work_item_id")?;
    resource_text(&request.owner_id, "resource owner_id")?;
    resource_text(&request.fence, "resource fence")?;
    resource_text(&request.reservation_id, "resource reservation_id")?;
    resource_text(&request.profile_name, "resource profile_name")?;
    resource_text(&request.profile_version, "resource profile_version")?;
    let parsed_profile_version = request
        .profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if parsed_profile_version.to_string() != request.profile_version {
        return Err("resource profile_version must be a canonical integer".to_string());
    }
    Ok(())
}

/// Phase 2 of `resource_validate_request`: host/target selector, the remaining
/// bounded text fields, the input fingerprint and the label sets.
fn resource_validate_request_selectors(request: &ResourceReservationRequest) -> Result<(), String> {
    resource_text(&request.host_ref, "resource host_ref")?;
    let target_kind = resource_request_target_kind(request.target_kind);
    resource_text(target_kind, "resource target_kind")?;
    if (target_kind == "local") != request.target_alias.is_none() {
        return Err("resource target_alias does not match target_kind".to_string());
    }
    if let Some(alias) = request.target_alias.as_deref() {
        resource_text(alias, "resource target_alias")?;
    }
    resource_text(&request.repository_id, "resource repository_id")?;
    resource_text(&request.branch, "resource branch")?;
    resource_text(&request.concurrency_key, "resource concurrency_key")?;
    resource_text(&request.fairness_group, "resource fairness_group")?;
    resource_text(&request.disk_policy_key, "resource disk_policy_key")?;
    resource_text(&request.idempotency_key, "resource idempotency_key")?;
    resource_fingerprint(&request.input_fingerprint, "resource input_fingerprint")?;
    resource_labels(&request.required_labels, "resource required_labels")?;
    resource_labels(&request.anti_affinity, "resource anti_affinity")?;
    Ok(())
}

/// The requirement vector is rejected when any dimension is zero or above
/// `MAX_RESOURCE_DIMENSION`.  Verbatim lift of the original disjunction.
fn resource_requirement_dimensions_invalid(requirement: &ResourceRequirement) -> bool {
    requirement.cpu_weight == 0
        || requirement.memory_mib == 0
        || requirement.disk_mib == 0
        || requirement.process_slots == 0
        || requirement.cpu_weight > MAX_RESOURCE_DIMENSION
        || requirement.memory_mib > MAX_RESOURCE_DIMENSION
        || requirement.disk_mib > MAX_RESOURCE_DIMENSION
        || requirement.process_slots > MAX_RESOURCE_DIMENSION
}

/// Phase 3 of `resource_validate_request`: attempt, requirement dimensions,
/// concurrency limit and fairness cost.
fn resource_validate_request_requirement(
    request: &ResourceReservationRequest,
) -> Result<(), String> {
    if request.attempt == 0 {
        return Err("resource attempt must be positive".to_string());
    }
    if resource_requirement_dimensions_invalid(&request.requirement) {
        return Err("resource requirement dimensions must be positive".to_string());
    }
    if request.concurrency_limit.is_some_and(|limit| limit == 0) {
        return Err("resource concurrency_limit must be positive".to_string());
    }
    if request.fairness_cost.checked_add(0).is_none() || request.fairness_cost == 0 {
        return Err("resource fairness_cost must be positive".to_string());
    }
    if request.fairness_cost > MAX_RESOURCE_DIMENSION {
        return Err("resource fairness_cost exceeds the native bound".to_string());
    }
    Ok(())
}

/// Phase 4 of `resource_validate_request`: disk watermark ordering and the
/// reservation window/TTL bound.
fn resource_validate_request_window(request: &ResourceReservationRequest) -> Result<(), String> {
    if request
        .disk_low_watermark_mib
        .zip(request.disk_high_watermark_mib)
        .is_some_and(|(low, high)| low > high)
    {
        return Err("resource disk low watermark exceeds high watermark".to_string());
    }
    if request.expires_at_ms <= request.reserved_at_ms {
        return Err("resource expiry must be after reservation time".to_string());
    }
    if request.expires_at_ms.saturating_sub(request.reserved_at_ms) > MAX_RESOURCE_TTL_MS {
        return Err("resource TTL exceeds the native bound".to_string());
    }
    Ok(())
}

/// The four phases run in the original statement order, so a doubly-invalid
/// request still reports the first field that was wrong before the split.
fn resource_validate_request(request: &ResourceReservationRequest) -> Result<(), String> {
    resource_validate_request_identity(request)?;
    resource_validate_request_selectors(request)?;
    resource_validate_request_requirement(request)?;
    resource_validate_request_window(request)?;
    Ok(())
}

fn resource_capacity_sum(host: &DurableResourceHost, requirement: &ResourceRequirement) -> bool {
    host.observed
        .cpu_weight
        .checked_add(host.held_cpu_weight)
        .and_then(|value| value.checked_add(requirement.cpu_weight))
        .is_some_and(|value| value <= host.capacity.cpu_weight)
        && host
            .observed
            .memory_mib
            .checked_add(host.held_memory_mib)
            .and_then(|value| value.checked_add(requirement.memory_mib))
            .is_some_and(|value| value <= host.capacity.memory_mib)
        && host
            .observed
            .disk_mib
            .checked_add(host.held_disk_mib)
            .and_then(|value| value.checked_add(requirement.disk_mib))
            .is_some_and(|value| value <= host.capacity.disk_mib)
        && host
            .observed
            .process_slots
            .checked_add(host.held_process_slots)
            .and_then(|value| value.checked_add(requirement.process_slots))
            .is_some_and(|value| value <= host.capacity.process_slots)
}

fn resource_result_state(state: ResourceReservationRecordState) -> ResourceReservationResultState {
    match state {
        ResourceReservationRecordState::Reserved => ResourceReservationResultState::Reserved,
        ResourceReservationRecordState::Released => ResourceReservationResultState::Released,
        ResourceReservationRecordState::Reclaimed => ResourceReservationResultState::Reclaimed,
        ResourceReservationRecordState::Expired => ResourceReservationResultState::Expired,
        ResourceReservationRecordState::Superseded => ResourceReservationResultState::Superseded,
        ResourceReservationRecordState::Absent => ResourceReservationResultState::Absent,
    }
}

fn resource_summary_state(
    state: ResourceReservationRecordState,
) -> ResourceReservationSummaryState {
    match state {
        ResourceReservationRecordState::Reserved => ResourceReservationSummaryState::Reserved,
        ResourceReservationRecordState::Released => ResourceReservationSummaryState::Released,
        ResourceReservationRecordState::Reclaimed => ResourceReservationSummaryState::Reclaimed,
        ResourceReservationRecordState::Expired => ResourceReservationSummaryState::Expired,
        ResourceReservationRecordState::Superseded => ResourceReservationSummaryState::Superseded,
        ResourceReservationRecordState::Absent => ResourceReservationSummaryState::Absent,
    }
}

fn resource_result_payload(
    decision: ResourceReservationResultDecision,
    request: &ResourceReservationRequest,
    record: Option<ResourceReservationRecord>,
    host: Option<&DurableResourceHost>,
    fairness_debt: u64,
    changed: Vec<String>,
) -> crate::protocol::ResultPayload {
    let (state, lifecycle_revision, tombstone, held) = match record.as_ref() {
        Some(record) => {
            let held = if record.state == ResourceReservationRecordState::Reserved {
                (
                    record.requirement.cpu_weight,
                    record.requirement.memory_mib,
                    record.requirement.disk_mib,
                    record.requirement.process_slots,
                )
            } else {
                (0, 0, 0, 0)
            };
            (
                resource_result_state(record.state),
                record.lifecycle_revision,
                record.tombstone,
                held,
            )
        }
        None => (
            ResourceReservationResultState::Absent,
            0,
            false,
            (0, 0, 0, 0),
        ),
    };
    let host_ref = record
        .as_ref()
        .map(|record| record.host_ref.clone())
        .or_else(|| Some(request.host_ref.clone()));
    let host_revision = host.map_or(0, |host| host.revision);
    crate::protocol::ResultPayload::raw(&ResourceReservationResult {
        schema_version: ResourceReservationResultSchemaVersion::V1,
        decision,
        reservation_id: Some(record.as_ref().map_or_else(
            || request.reservation_id.clone(),
            |record| record.reservation_id.clone(),
        )),
        work_item_id: request.work_item_id.clone(),
        attempt: record
            .as_ref()
            .map_or(request.attempt, |record| record.attempt),
        lease_epoch: record
            .as_ref()
            .map_or(request.lease_epoch, |record| record.lease_epoch),
        fencing_token: record
            .as_ref()
            .map_or(request.fencing_token, |record| record.fencing_token),
        lifecycle_revision,
        host_ref,
        host_revision,
        record,
        state,
        held_cpu_weight: held.0,
        held_memory_mib: held.1,
        held_disk_mib: held.2,
        held_process_slots: held.3,
        fairness_debt,
        tombstone,
        changed_work_item_ids: changed,
    })
}

fn resource_host_result(
    request: &ResourceHostUpdateRequest,
    host: Option<&DurableResourceHost>,
    policies: &[(String, DurableResourceDiskPolicy)],
    accepted: bool,
    reason: ResourceHostUpdateResultReason,
) -> Result<crate::protocol::ResultPayload, String> {
    let host_snapshot = host
        .map(|host| resource_host_update_snapshot(host, policies))
        .transpose()?;
    Ok(crate::protocol::ResultPayload::raw(
        &ResourceHostUpdateResult {
            schema_version: ResourceHostUpdateResultSchemaVersion::V1,
            accepted,
            reason,
            host_ref: request.host_ref.clone(),
            host_snapshot,
            revision: host.map_or(request.revision, |host| host.revision),
            held_cpu_weight: host.map_or(0, |host| host.held_cpu_weight),
            held_memory_mib: host.map_or(0, |host| host.held_memory_mib),
            held_disk_mib: host.map_or(0, |host| host.held_disk_mib),
            held_process_slots: host.map_or(0, |host| host.held_process_slots),
            draining: host.is_some_and(|host| host.draining),
            quarantined: host.is_some_and(|host| host.quarantined),
        },
    ))
}

fn resource_b64_urlsafe(value: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let bytes = value.as_bytes();
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        if chunk.len() == 1 {
            encoded.push(ALPHABET[((first & 0x03) << 4) as usize] as char);
            encoded.push('=');
            encoded.push('=');
            continue;
        }
        let second = chunk[1];
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() == 2 {
            encoded.push(ALPHABET[((second & 0x0f) << 2) as usize] as char);
            encoded.push('=');
            continue;
        }
        let third = chunk[2];
        encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
    }
    let chunks: Vec<String> = encoded
        .as_bytes()
        .chunks(3)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect();
    format!("opaque:v1:{}", chunks.join("."))
}

fn resource_b64_value(value: &str, name: &str) -> Result<String, String> {
    let original = value.to_string();
    let encoded = value
        .strip_prefix("opaque:v1:")
        .ok_or_else(|| format!("{name} is not an opaque:v1 value"))?
        .replace('.', "");
    if encoded.is_empty() || encoded.len() % 4 != 0 || encoded.len() > 512 {
        return Err(format!("{name} has invalid opaque:v1 length"));
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks_exact(4) {
        let a = digit(chunk[0]).ok_or_else(|| format!("{name} has invalid base64"))?;
        let b = digit(chunk[1]).ok_or_else(|| format!("{name} has invalid base64"))?;
        decoded.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            let c = digit(chunk[2]).ok_or_else(|| format!("{name} has invalid base64"))?;
            decoded.push((b << 4) | (c >> 2));
            if chunk[3] != b'=' {
                let d = digit(chunk[3]).ok_or_else(|| format!("{name} has invalid base64"))?;
                decoded.push((c << 6) | d);
            }
        } else if chunk[3] != b'=' {
            return Err(format!("{name} has invalid base64 padding"));
        }
    }
    let value = String::from_utf8(decoded).map_err(|_| format!("{name} is not UTF-8"))?;
    resource_text(&value, name)?;
    if resource_b64_urlsafe(&value) != original {
        return Err(format!("{name} is not canonical opaque:v1 encoding"));
    }
    Ok(value)
}

fn resource_opaque_string(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .ok_or_else(|| format!("{name} must be an opaque string or null"))?;
    Ok(Some(resource_b64_value(value, name)?))
}

fn resource_opaque_sequence(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<Vec<String>, String> {
    let values = value
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{name} must be an opaque string array"))?;
    if values.len() > MAX_RESOURCE_LABELS {
        return Err(format!("{name} exceeds {MAX_RESOURCE_LABELS} entries"));
    }
    let mut decoded = Vec::with_capacity(values.len());
    for value in values {
        decoded.push(
            resource_opaque_string(Some(value), name)?
                .ok_or_else(|| format!("{name} contains a null value"))?,
        );
    }
    resource_labels(&decoded, name)?;
    decoded.sort();
    Ok(decoded)
}

type ResourceMetadataMaps<'a> = (
    &'a serde_json::Map<String, serde_json::Value>,
    &'a serde_json::Map<String, serde_json::Value>,
);

fn resource_metadata_maps(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<ResourceMetadataMaps<'_>, String> {
    let metadata = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource admission metadata is missing".to_string())?;
    let repository = metadata
        .get("repository_work_item")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource admission extension is missing".to_string())?;
    let resource = repository
        .get("resource_reservation")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem resource reservation extension is missing".to_string())?;
    Ok((repository, resource))
}

fn resource_metadata_string(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    name: &str,
) -> Result<String, String> {
    resource_opaque_string(map.get(key), name)?.ok_or_else(|| format!("{name} is missing"))
}

fn resource_metadata_u64(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    name: &str,
) -> Result<u64, String> {
    map.get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("{name} is missing or invalid"))
}

#[derive(Debug, Clone)]
struct ResourceWorkItemFence {
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    superseded: bool,
}

fn resource_expected_fence(fencing_token: u64) -> String {
    // The Repository Manager bridge exposes the engine fencing token as the
    // stable opaque fence string.  Do not accept a caller-invented composite
    // spelling merely because the numeric epoch/token pair happens to match.
    fencing_token.to_string()
}

fn resource_request_target_kind(kind: ResourceReservationRequestTargetKind) -> &'static str {
    match kind {
        ResourceReservationRequestTargetKind::Local => "local",
        ResourceReservationRequestTargetKind::InventoryAlias => "inventory_alias",
    }
}

fn resource_record_target_kind(kind: ResourceReservationRecordTargetKind) -> &'static str {
    match kind {
        ResourceReservationRecordTargetKind::Local => "local",
        ResourceReservationRecordTargetKind::InventoryAlias => "inventory_alias",
    }
}

fn resource_host_target_kind(kind: ResourceHostUpdateRequestTargetKind) -> &'static str {
    match kind {
        ResourceHostUpdateRequestTargetKind::Local => "local",
        ResourceHostUpdateRequestTargetKind::InventoryAlias => "inventory_alias",
    }
}

fn resource_snapshot_kind(kind: &str) -> Result<ResourceTargetSnapshotKind, String> {
    match kind {
        "local" => Ok(ResourceTargetSnapshotKind::Local),
        "inventory_alias" => Ok(ResourceTargetSnapshotKind::InventoryAlias),
        _ => Err("resource target kind is invalid".to_string()),
    }
}

fn resource_reservation_snapshot_kind(
    kind: &str,
) -> Result<ResourceReservationHostSnapshotTargetKind, String> {
    match kind {
        "local" => Ok(ResourceReservationHostSnapshotTargetKind::Local),
        "inventory_alias" => Ok(ResourceReservationHostSnapshotTargetKind::InventoryAlias),
        _ => Err("resource host target kind is invalid".to_string()),
    }
}

fn resource_host_update_snapshot_kind(
    kind: &str,
) -> Result<ResourceHostUpdateSnapshotTargetKind, String> {
    match kind {
        "local" => Ok(ResourceHostUpdateSnapshotTargetKind::Local),
        "inventory_alias" => Ok(ResourceHostUpdateSnapshotTargetKind::InventoryAlias),
        _ => Err("resource host target kind is invalid".to_string()),
    }
}

fn resource_opaque_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<&str>,
    name: &str,
) -> Result<bool, String> {
    let actual = resource_opaque_string(map.get(key), name)?;
    Ok(actual.as_deref() == expected)
}

fn resource_u64_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: u64,
    name: &str,
) -> Result<bool, String> {
    Ok(resource_metadata_u64(map, key, name)? == expected)
}

fn resource_optional_u64_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<u64>,
    name: &str,
) -> Result<bool, String> {
    let actual = match map.get(key) {
        Some(value) if !value.is_null() => {
            Some(value.as_u64().ok_or_else(|| format!("{name} is invalid"))?)
        }
        _ => None,
    };
    Ok(actual == expected)
}

fn resource_bool_matches(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: bool,
    name: &str,
) -> Result<bool, String> {
    Ok(map
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| format!("{name} is missing or invalid"))?
        == expected)
}

fn validate_resource_extension_authority(
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    if extension
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        != Some("1")
        || extension
            .get("resolved_profile_authority")
            .and_then(serde_json::Value::as_str)
            != Some("repository_manager:resource_profile_registry:v1")
    {
        return Err(
            "WorkItem resource extension is legacy or lacks resolved-profile authority".into(),
        );
    }
    Ok(())
}

fn resolve_resource_extension_branch(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    let extension_branch = resource_metadata_string(extension, "branch", "resource branch")?;
    if request.branch_exclusive
        && extension
            .get("branch_explicit")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err("branch-exclusive WorkItem has no explicit branch".into());
    }
    Ok(extension_branch)
}

fn resource_extension_validate_and_extract_branch(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    validate_resource_extension_authority(extension)?;
    resolve_resource_extension_branch(extension, request)
}

/// The four sorted label sets `resource_extension_resolve_labels` returns, in
/// order: the host's advertised labels, the request's required labels, the host's
/// anti-affinity keys, and the request's anti-affinity keys.
type ResourceLabelSets = (Vec<String>, Vec<String>, Vec<String>, Vec<String>);

fn resource_extension_resolve_labels(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<ResourceLabelSets, String> {
    let mut labels =
        resource_opaque_sequence(extension.get("host_labels"), "resource host_labels")?;
    labels.sort();
    let mut request_labels = request.required_labels.clone();
    request_labels.sort();
    let mut anti_affinity =
        resource_opaque_sequence(extension.get("anti_affinity"), "resource anti_affinity")?;
    anti_affinity.sort();
    let mut request_anti_affinity = request.anti_affinity.clone();
    request_anti_affinity.sort();
    Ok((labels, request_labels, anti_affinity, request_anti_affinity))
}

// This is the immutable outer WorkItem digest, not an opaque user field.
// Keep its frozen `v1:<lowercase-hex>` spelling separate from the nested
// opaque:v1 values so a valid resolved WorkItem is not rejected at the
// trust boundary.
fn verify_resource_extension_work_item_digest(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    let work_item_digest = extension
        .get("work_item_input_fingerprint")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "work_item_input_fingerprint is missing".to_string())?
        .to_string();
    resource_fingerprint(&work_item_digest, "work_item_input_fingerprint")?;
    let stored_work_item_digest = repository
        .get("immutable_input_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository immutable_input_digest is missing".to_string())?;
    if stored_work_item_digest.len() != 64
        || !stored_work_item_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || work_item_digest != format!("v1:{stored_work_item_digest}")
    {
        // The nested WorkItem admission digest is distinct from the later
        // fenced reservation fingerprint, but it must still be bound to the
        // immutable outer WorkItem digest.  A validly-shaped forged digest
        // cannot otherwise be detected by field-by-field policy comparison.
        return Ok(false);
    }
    Ok(true)
}

fn resource_extension_resolve_alias_if_digest_matches(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<Option<String>>, String> {
    let alias = resource_opaque_string(extension.get("target_alias"), "resource target_alias")?;
    if !verify_resource_extension_work_item_digest(repository, extension)? {
        return Ok(None);
    }
    Ok(Some(alias))
}

#[allow(clippy::type_complexity)]
fn resolve_resource_extension_profile_fields(
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, String, String, String, String, String), String> {
    let profile_version =
        resource_metadata_string(extension, "profile_version", "resource profile_version")?;
    let profile_version_number = profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if profile_version_number.to_string() != profile_version {
        return Err("resource profile_version must use canonical integer spelling".to_string());
    }
    let profile_name =
        resource_metadata_string(extension, "profile_name", "resource profile_name")?;
    let repository_id =
        resource_metadata_string(extension, "repository_id", "resource repository_id")?;
    let concurrency_key =
        resource_metadata_string(extension, "concurrency_key", "resource concurrency_key")?;
    let fairness_group =
        resource_metadata_string(extension, "fairness_group", "resource fairness_group")?;
    let disk_policy_key =
        resource_metadata_string(extension, "disk_policy_key", "resource disk_policy_key")?;
    Ok((
        profile_version,
        profile_name,
        repository_id,
        concurrency_key,
        fairness_group,
        disk_policy_key,
    ))
}

#[allow(clippy::type_complexity)]
fn resolve_resource_extension_repository_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, String, String, Option<String>, String), String> {
    let repository_id_outer =
        resource_metadata_string(repository, "repository_id", "repository repository_id")?;
    let owner_id_outer = resource_metadata_string(repository, "owner_id", "repository owner_id")?;
    let outer_target_kind = repository
        .get("target_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository target_kind is missing".to_string())?
        .to_string();
    let outer_target_alias =
        resource_opaque_string(repository.get("target_alias"), "repository target_alias")?;
    let tenant_id = repository
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "repository tenant_id is missing".to_string())?;
    let tenant_id = resource_b64_value(tenant_id, "repository tenant_id")?;
    Ok((
        repository_id_outer,
        owner_id_outer,
        outer_target_kind,
        outer_target_alias,
        tenant_id,
    ))
}

#[allow(clippy::type_complexity)]
fn resource_extension_resolve_extracted_fields(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<
    (
        (String, String, String, String, String, String),
        (String, String, String, Option<String>, String),
    ),
    String,
> {
    let profile_fields = resolve_resource_extension_profile_fields(extension)?;
    let repository_fields = resolve_resource_extension_repository_fields(repository)?;
    Ok((profile_fields, repository_fields))
}

fn resolve_resource_extension_target_kind(
    extension: &serde_json::Map<String, serde_json::Value>,
    alias: &Option<String>,
) -> Result<String, String> {
    let extension_target_kind = extension
        .get("target_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "resource target_kind is missing".to_string())?;
    if extension_target_kind != "local" && extension_target_kind != "inventory_alias" {
        return Err("resource target_kind is invalid".to_string());
    }
    if (extension_target_kind == "local") != alias.is_none() {
        return Err("resource target_alias does not match target_kind".to_string());
    }
    Ok(extension_target_kind.to_string())
}

#[allow(clippy::too_many_arguments)]
fn resource_extension_identity_matches(
    request: &ResourceReservationRequest,
    profile_name: &str,
    profile_version: &str,
    repository_id: &str,
    repository_id_outer: &str,
    owner_id_outer: &str,
    tenant_id: &str,
    extension_branch: &str,
    extension_target_kind: &str,
    outer_target_kind: &str,
    alias: &Option<String>,
    outer_target_alias: &Option<String>,
    concurrency_key: &str,
) -> bool {
    profile_name == request.profile_name
        && profile_version == request.profile_version
        && repository_id == request.repository_id
        && repository_id_outer == request.repository_id
        && owner_id_outer == request.owner_id
        && tenant_id == request.tenant_ref
        && extension_branch == request.branch
        // The nested extension and the outer WorkItem projection must agree on
        // the original execution-target declaration.  The reservation request
        // carries the scheduler's *selected* host target, which may be remote
        // even when this top-level declaration is local with a remote
        // preferred/required policy; that selected pair is checked separately
        // against the host row below.
        && extension_target_kind == outer_target_kind
        && alias.as_deref() == outer_target_alias.as_deref()
        && concurrency_key == request.concurrency_key
}

fn resource_extension_requirements_match(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<bool, String> {
    Ok(resource_u64_matches(
        extension,
        "cpu_weight",
        request.requirement.cpu_weight,
        "resource cpu_weight",
    )? && resource_u64_matches(
        extension,
        "memory_mib",
        request.requirement.memory_mib,
        "resource memory_mib",
    )? && resource_u64_matches(
        extension,
        "disk_mib",
        request.requirement.disk_mib,
        "resource disk_mib",
    )? && resource_u64_matches(
        extension,
        "process_slots",
        request.requirement.process_slots,
        "resource process_slots",
    )?)
}

#[allow(clippy::too_many_arguments)]
fn resource_extension_affinity_and_exclusivity_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    fairness_group: &str,
) -> Result<bool, String> {
    Ok(labels == request_labels
        && anti_affinity == request_anti_affinity
        && fairness_group == request.fairness_group
        && resource_optional_u64_matches(
            extension,
            "concurrency_limit",
            request.concurrency_limit,
            "resource concurrency_limit",
        )?
        && resource_bool_matches(
            extension,
            "repository_exclusive",
            request.repository_exclusive,
            "resource repository_exclusive",
        )?
        && resource_bool_matches(
            extension,
            "branch_exclusive",
            request.branch_exclusive,
            "resource branch_exclusive",
        )?)
}

fn resource_extension_disk_policy_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    disk_policy_key: &str,
) -> Result<bool, String> {
    Ok(resource_optional_u64_matches(
        extension,
        "disk_low_watermark_mib",
        request.disk_low_watermark_mib,
        "resource disk_low_watermark_mib",
    )? && resource_optional_u64_matches(
        extension,
        "disk_high_watermark_mib",
        request.disk_high_watermark_mib,
        "resource disk_high_watermark_mib",
    )? && disk_policy_key == request.disk_policy_key
        && resource_optional_u64_matches(
            extension,
            "fairness_cost",
            Some(request.fairness_cost),
            "resource fairness_cost",
        )?)
}

#[allow(clippy::too_many_arguments)]
fn resource_extension_policy_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    fairness_group: &str,
    disk_policy_key: &str,
) -> Result<bool, String> {
    Ok(resource_extension_affinity_and_exclusivity_matches(
        extension,
        request,
        labels,
        request_labels,
        anti_affinity,
        request_anti_affinity,
        fairness_group,
    )? && resource_extension_disk_policy_matches(extension, request, disk_policy_key)?)
}

#[allow(clippy::too_many_arguments)]
fn resource_extension_final_match(
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    extension_branch: &str,
    labels: &[String],
    request_labels: &[String],
    anti_affinity: &[String],
    request_anti_affinity: &[String],
    alias: &Option<String>,
    profile_fields: &(String, String, String, String, String, String),
    repository_fields: &(String, String, String, Option<String>, String),
    extension_target_kind: &str,
) -> Result<bool, String> {
    let (
        profile_version,
        profile_name,
        repository_id,
        concurrency_key,
        fairness_group,
        disk_policy_key,
    ) = profile_fields;
    let (repository_id_outer, owner_id_outer, outer_target_kind, outer_target_alias, tenant_id) =
        repository_fields;
    let identity_ok = resource_extension_identity_matches(
        request,
        profile_name,
        profile_version,
        repository_id,
        repository_id_outer,
        owner_id_outer,
        tenant_id,
        extension_branch,
        extension_target_kind,
        outer_target_kind,
        alias,
        outer_target_alias,
        concurrency_key,
    );
    let requirements_ok = resource_extension_requirements_match(extension, request)?;
    let policy_ok = resource_extension_policy_matches(
        extension,
        request,
        labels,
        request_labels,
        anti_affinity,
        request_anti_affinity,
        fairness_group,
        disk_policy_key,
    )?;
    Ok(identity_ok && requirements_ok && policy_ok)
}

fn resource_extension_matches(
    repository: &serde_json::Map<String, serde_json::Value>,
    extension: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<bool, String> {
    let extension_branch = resource_extension_validate_and_extract_branch(extension, request)?;
    let (labels, request_labels, anti_affinity, request_anti_affinity) =
        resource_extension_resolve_labels(extension, request)?;
    let alias = match resource_extension_resolve_alias_if_digest_matches(repository, extension)? {
        Some(alias) => alias,
        None => return Ok(false),
    };
    let (profile_fields, repository_fields) =
        resource_extension_resolve_extracted_fields(repository, extension)?;
    let extension_target_kind = resolve_resource_extension_target_kind(extension, &alias)?;
    resource_extension_final_match(
        extension,
        request,
        &extension_branch,
        &labels,
        &request_labels,
        &anti_affinity,
        &request_anti_affinity,
        &alias,
        &profile_fields,
        &repository_fields,
        &extension_target_kind,
    )
}

/// Tail of `resource_validate_work_item`, run after the fence checks and in the
/// same order: the lease-owner match (skipped for a superseded row, exactly as
/// before) followed by the repository/extension projection comparison.
fn resource_validate_work_item_owner_and_extension(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    superseded: bool,
) -> Result<(), ResourceReservationResultDecision> {
    let status = property_string(props, "status");
    let owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    if !superseded && owner != request.owner_id {
        return Err(ResourceReservationResultDecision::Stale);
    }
    let (repository, extension) =
        resource_metadata_maps(props).map_err(|_| ResourceReservationResultDecision::Policy)?;
    let matches = resource_extension_matches(repository, extension, request)
        .map_err(|_| ResourceReservationResultDecision::Policy)?;
    if !matches {
        return Err(ResourceReservationResultDecision::InputConflict);
    }
    Ok(())
}

fn resource_validate_work_item(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
    allow_superseded: bool,
) -> Result<ResourceWorkItemFence, ResourceReservationResultDecision> {
    if property_string(props, "node_type") != "WorkItem"
        || property_string(props, "tenant") != request.tenant_ref
    {
        return Err(ResourceReservationResultDecision::NotFound);
    }
    let current_attempt = property_u64(props, "attempt");
    let lease_epoch = property_u64(props, "lease_epoch");
    let fencing_token = property_u64(props, "fencing_token");
    let superseded = current_attempt > request.attempt
        || lease_epoch != request.lease_epoch
        || fencing_token != request.fencing_token;
    if superseded && !(allow_superseded && current_attempt > request.attempt) {
        return Err(ResourceReservationResultDecision::Stale);
    }
    if request.fence != resource_expected_fence(request.fencing_token) {
        return Err(ResourceReservationResultDecision::Stale);
    }
    resource_validate_work_item_owner_and_extension(props, request, superseded)?;
    Ok(ResourceWorkItemFence {
        attempt: current_attempt,
        lease_epoch,
        fencing_token,
        superseded,
    })
}

fn resource_target_policy_value(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<serde_json::Value, String> {
    let map = value
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{name} is missing"))?;
    let kind = map
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{name}.kind is missing"))?;
    if kind != "local" && kind != "inventory_alias" {
        return Err(format!("{name}.kind is invalid"));
    }
    let alias = resource_opaque_string(map.get("alias"), &format!("{name}.alias"))?;
    if (kind == "local") != alias.is_none() {
        return Err(format!("{name}.alias does not match kind"));
    }
    let labels = resource_opaque_sequence(
        map.get("capability_labels"),
        &format!("{name}.capability_labels"),
    )?;
    let mut value = serde_json::Map::new();
    value.insert(
        "alias".into(),
        alias.map_or(serde_json::Value::Null, serde_json::Value::String),
    );
    value.insert("capability_labels".into(), serde_json::json!(labels));
    // ResourceProfileRegistry's TargetPolicy.model_dump(mode="json") includes
    // its contract marker.  This is part of RMDD-08's canonical fingerprint,
    // not merely a wire-validation detail.
    value.insert("contract_version".into(), serde_json::json!("1"));
    value.insert("kind".into(), serde_json::Value::String(kind.to_string()));
    Ok(serde_json::Value::Object(value))
}

/// Validate the selected host against the immutable WorkItem target policy.
/// A preferred target is a placement hint and is therefore intentionally not
/// required to equal the selected host; a required target is an admission
/// constraint and must match exactly.
fn resource_target_selection_matches(
    extension: &serde_json::Map<String, serde_json::Value>,
    host: &DurableResourceHost,
) -> Result<bool, String> {
    if let Some(required) = extension.get("required_target") {
        if !required.is_null() {
            let required =
                resource_target_policy_value(Some(required), "resource required_target")?;
            let required_kind = required
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "resource required_target.kind is missing".to_string())?;
            let required_alias = required.get("alias").and_then(serde_json::Value::as_str);
            return Ok(
                required_kind == host.target_kind && required_alias == host.target_alias.as_deref()
            );
        }
    }
    let preferred = resource_target_policy_value(
        extension.get("preferred_target"),
        "resource preferred_target",
    )?;
    let preferred_kind = preferred
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "resource preferred_target.kind is missing".to_string())?;
    // A remote/inventory host is eligible only when the immutable policy
    // explicitly names an inventory preference.  The preferred alias orders
    // eligible remote hosts; it is not an equality constraint here.
    Ok(host.target_kind == "local" || preferred_kind == "inventory_alias")
}

fn resource_selected_target_matches_request(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
) -> bool {
    resource_request_target_kind(request.target_kind) == host.target_kind
        && request.target_alias.as_deref() == host.target_alias.as_deref()
}

fn resource_recomputed_fingerprint(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationRequest,
) -> Result<String, String> {
    use std::collections::BTreeMap;
    let (repository, extension) = resource_metadata_maps(props)?;
    let job_id = resource_metadata_string(repository, "job_id", "repository job_id")?;
    let host_labels =
        resource_opaque_sequence(extension.get("host_labels"), "resource host_labels")?;
    let anti_affinity =
        resource_opaque_sequence(extension.get("anti_affinity"), "resource anti_affinity")?;
    let required_target = match extension.get("required_target") {
        Some(value) if !value.is_null() => Some(resource_target_policy_value(
            Some(value),
            "resource required_target",
        )?),
        _ => None,
    };
    let preferred_target = resource_target_policy_value(
        extension.get("preferred_target"),
        "resource preferred_target",
    )?;
    let profile_version =
        resource_metadata_string(extension, "profile_version", "resource profile_version")?;
    let profile_version_number = profile_version
        .parse::<u64>()
        .map_err(|_| "resource profile_version must be a canonical integer".to_string())?;
    if profile_version_number.to_string() != profile_version {
        return Err("resource profile_version must use canonical integer spelling".to_string());
    }
    let priority = resource_metadata_u64(repository, "priority", "repository priority")?;
    let queue_deadline = repository
        .get("queue_deadline")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let queue_deadline = match queue_deadline {
        serde_json::Value::String(value) if value.ends_with("+00:00") => {
            serde_json::Value::String(format!("{}Z", &value[..value.len() - 6]))
        }
        value => value,
    };
    let ttl_ms = request
        .expires_at_ms
        .checked_sub(request.reserved_at_ms)
        .ok_or_else(|| "resource TTL underflow".to_string())?;
    if ttl_ms == 0 || ttl_ms % 1_000 != 0 {
        return Err("resource TTL must be an integral number of seconds".to_string());
    }
    let mut resources = BTreeMap::new();
    resources.insert("anti_affinity", serde_json::json!(anti_affinity));
    resources.insert(
        "concurrency_key",
        serde_json::json!(request.concurrency_key),
    );
    resources.insert("contract_version", serde_json::json!("1"));
    resources.insert(
        "cpu_weight",
        serde_json::json!(request.requirement.cpu_weight),
    );
    resources.insert(
        "disk_high_watermark_mib",
        request
            .disk_high_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert(
        "disk_low_watermark_mib",
        request
            .disk_low_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert("disk_mib", serde_json::json!(request.requirement.disk_mib));
    resources.insert("fairness_group", serde_json::json!(request.fairness_group));
    resources.insert("host_labels", serde_json::json!(host_labels));
    resources.insert(
        "memory_mib",
        serde_json::json!(request.requirement.memory_mib),
    );
    resources.insert("preferred_target", preferred_target);
    resources.insert("priority", serde_json::json!(priority));
    resources.insert(
        "process_slots",
        serde_json::json!(request.requirement.process_slots),
    );
    resources.insert("queue_deadline", queue_deadline);
    resources.insert(
        "required_target",
        required_target.map_or(serde_json::Value::Null, |value| value),
    );
    resources.insert("resource_class", serde_json::json!(request.profile_name));
    let mut payload = BTreeMap::new();
    payload.insert("attempt", serde_json::json!(request.attempt));
    payload.insert("branch", serde_json::json!(request.branch));
    payload.insert("fence", serde_json::json!(request.fence));
    payload.insert("job_id", serde_json::json!(job_id));
    payload.insert("owner_id", serde_json::json!(request.owner_id));
    payload.insert("profile", serde_json::json!(request.profile_name));
    // RMDD-08 hashes the resolved registry profile version as an integer.  The
    // wire/record projection retains its bounded string spelling, but the
    // canonical digest must use the frozen numeric JSON form.
    payload.insert("profile_version", serde_json::json!(profile_version_number));
    payload.insert("repository_id", serde_json::json!(request.repository_id));
    payload.insert("reservation_id", serde_json::json!(request.reservation_id));
    payload.insert(
        "resources",
        serde_json::to_value(resources).map_err(|e| e.to_string())?,
    );
    payload.insert("tenant_id", serde_json::json!(request.tenant_ref));
    payload.insert("ttl_seconds", serde_json::json!(ttl_ms / 1_000));
    payload.insert("version", serde_json::json!("v1"));
    payload.insert("work_item_id", serde_json::json!(request.work_item_id));
    let bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    use sha2::{Digest, Sha256};
    Ok(format!("v1:{}", hex::encode(Sha256::digest(bytes))))
}

/// Identity/fencing half of the replay comparison: the keys that name the
/// reservation and the attempt that produced it.
fn resource_request_matches_identity(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.reservation_id == record.reservation_id
        && request.tenant_ref == record.tenant_ref
        && request.owner_id == record.owner_id
        && request.work_item_id == record.work_item_id
        && request.fence == record.fence
        && request.attempt == record.attempt
        && request.lease_epoch == record.lease_epoch
        && request.fencing_token == record.fencing_token
}

/// Placement half: the fingerprint, host binding, resolved profile, requirement
/// vector and target selector.
fn resource_request_matches_placement(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.input_fingerprint == record.input_fingerprint
        && request.host_ref == record.host_ref
        && request.profile_name == record.profile_name
        && request.profile_version == record.profile_version
        && request.requirement == record.requirement
        && resource_request_target_kind(request.target_kind)
            == resource_record_target_kind(record.target_kind)
        && request.target_alias == record.target_alias
        && request.repository_id == record.repository_id
}

/// Admission half: branch, concurrency key/limit, the exclusivity flags and the
/// label/anti-affinity selectors.
fn resource_request_matches_admission(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.branch == record.branch
        && request.concurrency_key == record.concurrency_key
        && request.concurrency_limit == record.concurrency_limit
        && request.repository_exclusive == record.repository_exclusive
        && request.branch_exclusive == record.branch_exclusive
        && request.required_labels == record.required_labels
        && request.anti_affinity == record.anti_affinity
        && request.fairness_group == record.fairness_group
}

/// Accounting half: fairness cost, the disk watermarks/policy key and the
/// reservation window.
fn resource_request_matches_accounting(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.fairness_cost == record.fairness_cost
        && request.disk_low_watermark_mib == record.disk_low_watermark_mib
        && request.disk_high_watermark_mib == record.disk_high_watermark_mib
        && request.disk_policy_key == record.disk_policy_key
        && request.reserved_at_ms == record.reserved_at_ms
        && request.expires_at_ms == record.expires_at_ms
        && request.expected_host_revision == record.expected_host_revision
}

/// A stored reservation replays a request only when every projected field is
/// identical.  The four halves are evaluated in the original field order, so a
/// mismatch short-circuits at exactly the same field it did before the split.
fn resource_request_matches_record(
    request: &ResourceReservationRequest,
    record: &ResourceReservationRecord,
) -> bool {
    resource_request_matches_identity(request, record)
        && resource_request_matches_placement(request, record)
        && resource_request_matches_admission(request, record)
        && resource_request_matches_accounting(request, record)
}

fn resource_encode<T: serde::Serialize>(
    value: &T,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|e| e.to_string())?;
    Ok(crypto.seal(&bytes).into_owned())
}

fn resource_decode<T: serde::de::DeserializeOwned>(
    value: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<T, String> {
    let bytes = crypto.unseal(value)?;
    decode_durable(&bytes)
}

fn resource_put_host(
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    host: &DurableResourceHost,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(host, crypto)?;
    hosts
        .insert((graph, host.host_ref.as_str()), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn resource_put_reservation(
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    reservation: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(reservation, crypto)?;
    reservations
        .insert(
            (graph, reservation.record.reservation_id.as_str()),
            bytes.as_slice(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn resource_load_fairness(
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    crypto: DurableCrypto<'_>,
) -> Result<DurableResourceFairness, String> {
    let key = resource_fairness_scope_key(tenant, group);
    fairness
        .get((graph, key.as_str()))
        .map_err(|e| e.to_string())?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
        .map(|value| value.unwrap_or_default())
}

fn resource_put_fairness(
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    value: &DurableResourceFairness,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let key = resource_fairness_scope_key(tenant, group);
    let bytes = resource_encode(value, crypto)?;
    fairness
        .insert((graph, key.as_str()), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn resource_load_host(
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    host_ref: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    hosts
        .get((graph, host_ref))
        .map_err(|error| error.to_string())?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

fn resource_load_reservation(
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    reservation_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceReservation>, String> {
    reservations
        .get((graph, reservation_id))
        .map_err(|error| error.to_string())?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

fn resource_adjust_concurrency(
    concurrency: &mut redb::Table<(&str, &str), u64>,
    graph: &str,
    key: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = concurrency
        .get((graph, key))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource concurrency counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource concurrency counter underflow".to_string())?
    };
    if next == 0 {
        concurrency
            .remove((graph, key))
            .map_err(|error| error.to_string())?;
    } else {
        concurrency
            .insert((graph, key), next)
            .map_err(|error| error.to_string())?;
    }
    Ok(next)
}

fn resource_adjust_anti_affinity(
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    graph: &str,
    host_ref: &str,
    tag: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = anti_affinity
        .get((graph, host_ref, tag))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource anti-affinity counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource anti-affinity counter underflow".to_string())?
    };
    if next == 0 {
        anti_affinity
            .remove((graph, host_ref, tag))
            .map_err(|error| error.to_string())?;
    } else {
        anti_affinity
            .insert((graph, host_ref, tag), next)
            .map_err(|error| error.to_string())?;
    }
    Ok(next)
}

fn resource_exclusivity_keys(request: &ResourceReservationRequest) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    if request.repository_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "repository",
            &request.repository_id,
        ]));
    }
    if request.branch_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "branch",
            &request.repository_id,
            &request.branch,
        ]));
    }
    keys
}

/// Composite native index keys use a reserved NUL separator. Every component
/// has already passed `resource_text`, which rejects NUL/control characters,
/// making tenant/scope boundaries unambiguous instead of relying on a caller's
/// arbitrary spelling.
fn resource_scope_key(parts: &[&str]) -> String {
    parts.join("\0")
}

/// Concurrency keys are explicit global scheduler scope.  The prefix prevents
/// an untrusted caller from colliding with future tenant-scoped namespaces while
/// preserving one exact counter across tenants for a shared host profile.
fn resource_concurrency_scope_key(key: &str) -> String {
    resource_scope_key(&["global", key])
}

fn resource_fairness_scope_key(tenant: &str, group: &str) -> String {
    resource_scope_key(&[tenant, group])
}

fn resource_record_target_kind_from_request(
    kind: ResourceReservationRequestTargetKind,
) -> ResourceReservationRecordTargetKind {
    match kind {
        ResourceReservationRequestTargetKind::Local => ResourceReservationRecordTargetKind::Local,
        ResourceReservationRequestTargetKind::InventoryAlias => {
            ResourceReservationRecordTargetKind::InventoryAlias
        }
    }
}

fn resource_build_record(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    revision: u64,
    lifecycle_revision: u64,
) -> Result<ResourceReservationRecord, String> {
    let mut labels = host.labels.clone();
    labels.sort();
    Ok(ResourceReservationRecord {
        reservation_id: request.reservation_id.clone(),
        tenant_ref: request.tenant_ref.clone(),
        owner_id: request.owner_id.clone(),
        work_item_id: request.work_item_id.clone(),
        fence: request.fence.clone(),
        attempt: request.attempt,
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        input_fingerprint: request.input_fingerprint.clone(),
        host_ref: request.host_ref.clone(),
        profile_name: request.profile_name.clone(),
        profile_version: request.profile_version.clone(),
        requirement: request.requirement.clone(),
        capacity_snapshot: ResourceCapacitySnapshot {
            cpu_weight: host.capacity.cpu_weight,
            memory_mib: host.capacity.memory_mib,
            disk_mib: host.capacity.disk_mib,
            process_slots: host.capacity.process_slots,
            host_revision: host.revision,
        },
        selected_target: ResourceTargetSnapshot {
            kind: resource_snapshot_kind(&host.target_kind)?,
            alias: host.target_alias.clone(),
            capability_labels: labels,
        },
        target_kind: resource_record_target_kind_from_request(request.target_kind),
        target_alias: request.target_alias.clone(),
        repository_id: request.repository_id.clone(),
        branch: request.branch.clone(),
        concurrency_key: request.concurrency_key.clone(),
        concurrency_limit: request.concurrency_limit,
        repository_exclusive: request.repository_exclusive,
        branch_exclusive: request.branch_exclusive,
        required_labels: request.required_labels.clone(),
        anti_affinity: request.anti_affinity.clone(),
        fairness_group: request.fairness_group.clone(),
        fairness_cost: request.fairness_cost,
        disk_low_watermark_mib: request.disk_low_watermark_mib,
        disk_high_watermark_mib: request.disk_high_watermark_mib,
        disk_policy_key: request.disk_policy_key.clone(),
        reserved_at_ms: request.reserved_at_ms,
        expires_at_ms: request.expires_at_ms,
        expected_host_revision: request.expected_host_revision,
        // Lifecycle CAS is an operation input, not immutable admission
        // identity. Reserve creation accepts only absent/zero; release and
        // reclaim record the successful precondition on their tombstone below
        // so an exact lifecycle retry can be distinguished from a changed one.
        expected_lifecycle_revision: None,
        state: ResourceReservationRecordState::Reserved,
        revision,
        lifecycle_revision,
        tombstone: false,
    })
}

fn resource_validate_host_freshness(
    host: &DurableResourceHost,
    now_ms: u64,
) -> ResourceReservationResultDecision {
    if host.heartbeat_at_ms > now_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if now_ms.saturating_sub(host.heartbeat_at_ms) > host.heartbeat_ttl_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if host.draining {
        return ResourceReservationResultDecision::Drained;
    }
    if host.quarantined {
        return ResourceReservationResultDecision::Quarantined;
    }
    ResourceReservationResultDecision::Accepted
}

/// Apply reserve/release/reclaim/host-update in the caller's already-open
/// MutationBatch WriteTransaction. Every read and index update below is part of
/// that one transaction; no scheduler mirror or second CAS participates.
#[allow(clippy::too_many_arguments)]
fn apply_resource_reservation_rows(
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    // CX-EG-05 (CCN 186 -> decomposed): the two match arms below are each a
    // literal, behaviour-preserving relocation of the original arm body into
    // its own named function (see immediately after this function).
    match method {
        Method::UpdateResourceHost { request } => {
            apply_update_resource_host_rows(request, graph, hosts, disk_policies, crypto)
        }
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => {
            apply_resource_reservation_lifecycle_rows(
                graph,
                method,
                request,
                nodes,
                reservations,
                tenant_index,
                attempts,
                hosts,
                exclusivity,
                fairness,
                concurrency,
                anti_affinity,
                disk_policies,
                crypto,
            )
        }
        _ => Ok(None),
    }
}

fn validate_resource_host_update_identity(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource host tenant_ref")?;
    resource_text(&request.host_ref, "resource host_ref")?;
    resource_labels(&request.labels, "resource host labels")?;
    resource_text(
        request.target_alias.as_deref().unwrap_or("local"),
        "resource host target_alias",
    )?;
    Ok(())
}

fn resolve_resource_host_update_target_kind(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    let target_kind = resource_host_target_kind(request.target_kind);
    if (target_kind == "local") != request.target_alias.is_none() {
        return Err("resource host target_alias does not match target_kind".into());
    }
    Ok(target_kind)
}

fn resource_host_heartbeat_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.heartbeat_ttl_ms < 1_000
        || request.heartbeat_ttl_ms > 86_400_000
        || request.heartbeat_at_ms > request.now_ms
        || request.now_ms.saturating_sub(request.heartbeat_at_ms) > request.heartbeat_ttl_ms
}

fn resource_host_capacity_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.capacity.cpu_weight == 0
        || request.capacity.memory_mib == 0
        || request.capacity.disk_mib == 0
        || request.capacity.process_slots == 0
        || request.capacity.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.capacity.memory_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.disk_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.process_slots > MAX_RESOURCE_DIMENSION
}

fn resource_host_disk_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.disk_used_mib > request.disk_capacity_mib
        || request.disk_capacity_mib == 0
        || request.disk_capacity_mib > MAX_RESOURCE_DIMENSION
        || request.disk_used_mib > MAX_RESOURCE_DIMENSION
}

fn resource_host_observed_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.observed.cpu_weight > request.capacity.cpu_weight
        || request.observed.memory_mib > request.capacity.memory_mib
        || request.observed.disk_mib > request.capacity.disk_mib
        || request.observed.process_slots > request.capacity.process_slots
        || request.observed.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.observed.memory_mib > MAX_RESOURCE_DIMENSION
        || request.observed.disk_mib > MAX_RESOURCE_DIMENSION
        || request.observed.process_slots > MAX_RESOURCE_DIMENSION
}

fn validate_resource_host_update_telemetry_bounds(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    if resource_host_heartbeat_bounds_violated(request)
        || resource_host_capacity_bounds_violated(request)
        || resource_host_disk_bounds_violated(request)
        || resource_host_observed_bounds_violated(request)
        || request.revision == 0
    {
        return Err("resource host update violates telemetry bounds".into());
    }
    Ok(())
}

fn validate_resource_host_update_request(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    validate_resource_host_update_identity(request)?;
    let target_kind = resolve_resource_host_update_target_kind(request)?;
    validate_resource_host_update_telemetry_bounds(request)?;
    Ok(target_kind)
}

fn resource_host_update_exceeds_capacity(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
) -> bool {
    request
        .observed
        .cpu_weight
        .checked_add(host.held_cpu_weight)
        .is_none_or(|value| value > request.capacity.cpu_weight)
        || request
            .observed
            .memory_mib
            .checked_add(host.held_memory_mib)
            .is_none_or(|value| value > request.capacity.memory_mib)
        || request
            .observed
            .disk_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.capacity.disk_mib)
        || request
            .disk_used_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.disk_capacity_mib)
        || request
            .observed
            .process_slots
            .checked_add(host.held_process_slots)
            .is_none_or(|value| value > request.capacity.process_slots)
}

fn check_resource_host_update_conflicts(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
    target_kind: &str,
    policy_rows: &[(String, DurableResourceDiskPolicy)],
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    if host.target_kind != target_kind || host.target_alias != request.target_alias {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    if request.revision <= host.revision {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::StaleHost,
        )?));
    }
    if resource_host_update_exceeds_capacity(request, host) {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    Ok(None)
}

fn build_resource_host_from_update(
    request: &ResourceHostUpdateRequest,
    current: Option<&DurableResourceHost>,
    target_kind: &str,
) -> DurableResourceHost {
    DurableResourceHost {
        // Physical host accounting is graph-scoped and shared across
        // tenants.  Preserve the first controller's provenance label;
        // authz on UpdateResourceHost, not this label, controls who may
        // publish telemetry.
        tenant_ref: current.map_or_else(
            || request.tenant_ref.clone(),
            |host| host.tenant_ref.clone(),
        ),
        host_ref: request.host_ref.clone(),
        revision: request.revision,
        capacity: request.capacity.clone(),
        observed: request.observed.clone(),
        heartbeat_at_ms: request.heartbeat_at_ms,
        heartbeat_ttl_ms: request.heartbeat_ttl_ms,
        now_ms: request.now_ms,
        draining: request.draining,
        quarantined: request.quarantined,
        labels: request.labels.clone(),
        target_kind: target_kind.to_string(),
        target_alias: request.target_alias.clone(),
        disk_used_mib: request.disk_used_mib,
        disk_capacity_mib: request.disk_capacity_mib,
        held_cpu_weight: current.map_or(0, |h| h.held_cpu_weight),
        held_memory_mib: current.map_or(0, |h| h.held_memory_mib),
        held_disk_mib: current.map_or(0, |h| h.held_disk_mib),
        held_process_slots: current.map_or(0, |h| h.held_process_slots),
    }
}

fn apply_update_resource_host_rows(
    request: &ResourceHostUpdateRequest,
    graph: &str,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let target_kind = validate_resource_host_update_request(request)?;
    let current = resource_load_host(hosts, graph, &request.host_ref, crypto)?;
    let policy_rows =
        resource_collect_disk_policy_rows(disk_policies, graph, &request.host_ref, crypto)?;
    if let Some(host) = current.as_ref() {
        if let Some(payload) =
            check_resource_host_update_conflicts(request, host, target_kind, &policy_rows)?
        {
            return Ok(Some(payload));
        }
    }
    let host = build_resource_host_from_update(request, current.as_ref(), target_kind);
    resource_put_host(hosts, graph, &host, crypto)?;
    Ok(Some(resource_host_result(
        request,
        Some(&host),
        &policy_rows,
        true,
        ResourceHostUpdateResultReason::Accepted,
    )?))
}

/// Early-return signal used while decomposing `apply_resource_reservation_lifecycle_rows`
/// into phase functions: `Continue(v)` carries the phase's output forward to the next
/// phase; `Return(payload)` means the phase already produced the function's final result
/// and every caller must stop and return it unchanged.
enum ReservationLifecycleStep<T> {
    Continue(T),
    Return(crate::protocol::ResultPayload),
}

#[allow(clippy::too_many_arguments)]
fn apply_resource_reservation_lifecycle_rows(
    graph: &str,
    method: &Method,
    request: &ResourceReservationRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let (is_reserve, is_reclaim, existing, props, work_item_fence) =
        match resource_lifecycle_precheck_and_load_work_item(
            method,
            request,
            reservations,
            hosts,
            nodes,
            graph,
            crypto,
        )? {
            ReservationLifecycleStep::Return(payload) => return Ok(Some(payload)),
            ReservationLifecycleStep::Continue(value) => value,
        };
    let extension = match resource_validate_work_item_status_and_extension(
        request,
        is_reserve,
        is_reclaim,
        &work_item_fence,
        &props,
    )? {
        ReservationLifecycleStep::Return(payload) => return Ok(Some(payload)),
        ReservationLifecycleStep::Continue(value) => value,
    };
    if let Some(payload) = resource_commit_release_or_reclaim_or_reserve_gate(
        graph,
        request,
        existing.as_ref(),
        is_reserve,
        is_reclaim,
        &work_item_fence,
        &props,
        hosts,
        reservations,
        fairness,
        concurrency,
        anti_affinity,
        exclusivity,
        disk_policies,
        crypto,
    )? {
        return Ok(Some(payload));
    }
    let host = match resource_admit_reserve_host_with_winner_check(
        attempts,
        reservations,
        hosts,
        disk_policies,
        anti_affinity,
        concurrency,
        exclusivity,
        graph,
        request,
        extension,
        crypto,
    )? {
        ReservationLifecycleStep::Return(payload) => return Ok(Some(payload)),
        ReservationLifecycleStep::Continue(value) => value,
    };
    resource_commit_reserve_admission(
        graph,
        request,
        host,
        hosts,
        reservations,
        tenant_index,
        attempts,
        exclusivity,
        concurrency,
        anti_affinity,
        fairness,
        crypto,
    )
}

/// Thin sequencing wrapper: runs Phase 1 (`resource_lifecycle_precheck`) then Phase 2
/// (`resource_load_and_validate_work_item`) back to back, so the orchestrator has a
/// single call/match site for "validate the request and load both the existing
/// reservation (if any) and the WorkItem row." No behaviour is added; this is pure
/// call-site consolidation (see CX-EG-05's finding that a `?` after a call counts as
/// a branch under this repo's complexity gate the same as an `if`, so flattening N
/// sequential fallible calls into fewer named steps is what brings the caller's own
/// CCN down, not simplifying any individual step).
/// What Phases 1+2 hand the reservation orchestrator, in order: is-reserve,
/// is-reclaim, the existing reservation (if any), the WorkItem's properties, and
/// its lease fence.
type ResourceLifecyclePrelude = (
    bool,
    bool,
    Option<DurableResourceReservation>,
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

#[allow(clippy::too_many_arguments)]
fn resource_lifecycle_precheck_and_load_work_item(
    method: &Method,
    request: &ResourceReservationRequest,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<ResourceLifecyclePrelude>, String> {
    let (is_reserve, is_reclaim, existing) =
        match resource_lifecycle_precheck(method, request, reservations, hosts, graph, crypto)? {
            ReservationLifecycleStep::Return(payload) => {
                return Ok(ReservationLifecycleStep::Return(payload));
            }
            ReservationLifecycleStep::Continue(value) => value,
        };
    let (props, work_item_fence) =
        match resource_load_and_validate_work_item(nodes, graph, request, is_reclaim, crypto)? {
            ReservationLifecycleStep::Return(payload) => {
                return Ok(ReservationLifecycleStep::Return(payload));
            }
            ReservationLifecycleStep::Continue(value) => value,
        };
    Ok(ReservationLifecycleStep::Continue((
        is_reserve,
        is_reclaim,
        existing,
        props,
        work_item_fence,
    )))
}

/// Thin sequencing wrapper: runs Phase 4 (`resource_commit_release_or_reclaim`) and,
/// only when it declined to decide (no existing reservation row), applies the
/// `!is_reserve -> NotFound` fallback that immediately followed it in the original
/// function. Pure call-site consolidation, no behaviour change.
#[allow(clippy::too_many_arguments)]
fn resource_commit_release_or_reclaim_or_reserve_gate(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    if let Some(payload) = resource_commit_release_or_reclaim(
        graph,
        request,
        existing,
        is_reserve,
        is_reclaim,
        work_item_fence,
        props,
        hosts,
        reservations,
        fairness,
        concurrency,
        anti_affinity,
        exclusivity,
        disk_policies,
        crypto,
    )? {
        return Ok(Some(payload));
    }
    if !is_reserve {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::NotFound,
            request,
            None,
            None,
            0,
            vec![],
        )));
    }
    Ok(None)
}

/// Thin sequencing wrapper: runs Phase 5 (`resource_check_attempt_winner_conflict`)
/// then, only if it did not already decide the request, Phase 6
/// (`resource_admit_reserve_host`). Pure call-site consolidation, no behaviour change.
#[allow(clippy::too_many_arguments)]
fn resource_admit_reserve_host_with_winner_check(
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    if let Some(payload) =
        resource_check_attempt_winner_conflict(attempts, reservations, graph, request, crypto)?
    {
        return Ok(ReservationLifecycleStep::Return(payload));
    }
    resource_admit_reserve_host(
        hosts,
        disk_policies,
        anti_affinity,
        concurrency,
        exclusivity,
        graph,
        request,
        extension,
        crypto,
    )
}

/// Phase 1: TTL/precondition/window validation, plus the FIRST idempotency-precondition
/// pass over any existing reservation row (tenant match, request match, the
/// release/reclaim `expected_lifecycle_revision` precondition, and the terminal-replay
/// short-circuit). Literal relocation of the original function's first ~110 lines;
/// no branch was added, removed, or reordered.
#[allow(clippy::too_many_arguments)]
/// The two reserve-only window preconditions.
///
/// A creation has no prior lifecycle revision to satisfy: accept only the
/// explicit zero/absent form, since a positive caller precondition must not be
/// silently persisted or bypassed by a reserve replay.  The reservation window
/// must also contain `now`.
fn resource_reserve_window_precheck(
    request: &ResourceReservationRequest,
) -> Option<crate::protocol::ResultPayload> {
    if request
        .expected_lifecycle_revision
        .is_some_and(|revision| revision != 0)
    {
        return Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            0,
            vec![],
        ));
    }
    if request.now_ms < request.reserved_at_ms || request.now_ms >= request.expires_at_ms {
        return Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            None,
            0,
            vec![],
        ));
    }
    None
}

/// The `expected_lifecycle_revision` precondition of a release/reclaim.
///
/// A live row is matched against its current revision; a terminal replay carries
/// the precondition captured by the successful lifecycle mutation, which keeps an
/// exact retry idempotent while refusing a changed precondition after the row is
/// tombstoned.  The refusal decision differs accordingly (`Stale` vs
/// `InputConflict`).
fn resource_lifecycle_revision_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
) -> Option<crate::protocol::ResultPayload> {
    let reserved = stored.record.state == ResourceReservationRecordState::Reserved;
    let lifecycle_matches = if reserved {
        request.expected_lifecycle_revision == Some(stored.record.lifecycle_revision)
    } else {
        request.expected_lifecycle_revision == stored.record.expected_lifecycle_revision
    };
    if lifecycle_matches {
        return None;
    }
    Some(resource_result_payload(
        if reserved {
            ResourceReservationResultDecision::Stale
        } else {
            ResourceReservationResultDecision::InputConflict
        },
        request,
        Some(stored.record.clone()),
        None,
        stored.fairness_debt,
        vec![],
    ))
}

/// The idempotency-precondition pass over an existing reservation row: tenant
/// match, full immutable record match, the release/reclaim lifecycle-revision
/// precondition, and the terminal-replay short-circuit.  `Ok(Some(..))` decides
/// the request; `Ok(None)` lets the lifecycle continue.
fn resource_existing_reservation_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    is_reserve: bool,
    graph: &str,
    hosts: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    if stored.record.tenant_ref != request.tenant_ref {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Conflict,
            request,
            None,
            None,
            0,
            vec![],
        )));
    }
    if !resource_request_matches_record(request, &stored.record) {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            stored.fairness_debt,
            vec![],
        )));
    }
    if !is_reserve {
        if let Some(payload) = resource_lifecycle_revision_precheck(request, stored) {
            return Ok(Some(payload));
        }
    }
    if stored.record.state != ResourceReservationRecordState::Reserved {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )));
    }
    Ok(None)
}

fn resource_lifecycle_precheck(
    method: &Method,
    request: &ResourceReservationRequest,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<(bool, bool, Option<DurableResourceReservation>)>, String> {
    resource_validate_request(request)?;
    if request.expires_at_ms <= request.reserved_at_ms
        || request.expires_at_ms.saturating_sub(request.reserved_at_ms) > MAX_RESOURCE_TTL_MS
    {
        return Err("resource TTL violates the native bound".into());
    }
    let is_reserve = matches!(method, Method::ReserveWorkItemResources { .. });
    let is_reclaim = matches!(method, Method::ReclaimWorkItemResources { .. });
    if is_reserve {
        if let Some(payload) = resource_reserve_window_precheck(request) {
            return Ok(ReservationLifecycleStep::Return(payload));
        }
    }
    // A terminal row is the durable idempotency tombstone.  Replay of
    // the exact release/reclaim (or an accepted reserve) must remain
    // answerable after the WorkItem has rotated to a newer attempt or
    // even been removed from the graph; requiring the old live lease
    // first would turn a safe replay into a misleading stale refusal.
    // The full immutable record comparison prevents a caller from
    // using a tombstone's reservation id as a substitute for current
    // WorkItem/fence authorization.
    let existing = resource_load_reservation(reservations, graph, &request.reservation_id, crypto)?;
    if let Some(stored) = existing.as_ref() {
        if let Some(payload) = resource_existing_reservation_precheck(
            request, stored, is_reserve, graph, hosts, crypto,
        )? {
            return Ok(ReservationLifecycleStep::Return(payload));
        }
    }
    Ok(ReservationLifecycleStep::Continue((
        is_reserve, is_reclaim, existing,
    )))
}

/// Phase 2: load the WorkItem row and validate its fence (attempt/lease_epoch/
/// fencing_token vs. the request), exactly as the original function's next block.
/// The validated WorkItem row: its properties map plus the lease fence read from
/// the same MVCC snapshot.
type ResourceWorkItemAdmission = (
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

fn resource_load_and_validate_work_item(
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationRequest,
    is_reclaim: bool,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<ResourceWorkItemAdmission>, String> {
    let item_bytes = nodes
        .get((graph, request.work_item_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(item_bytes) = item_bytes else {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::NotFound,
            request,
            None,
            None,
            0,
            vec![],
        )));
    };
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&item_bytes)?;
    let work_item_fence = match resource_validate_work_item(&props, request, is_reclaim) {
        Ok(fence) => fence,
        Err(decision) => {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                decision,
                request,
                None,
                None,
                0,
                vec![],
            )));
        }
    };
    Ok(ReservationLifecycleStep::Continue((props, work_item_fence)))
}

/// Phase 3: validate the WorkItem's live status is consistent with the requested
/// lifecycle transition, then parse (and return) its resource admission extension,
/// checking the reserve-only input fingerprint. Literal relocation of the original
/// function's third block.
fn resource_validate_work_item_status_and_extension<'p>(
    request: &ResourceReservationRequest,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &'p serde_json::Map<String, serde_json::Value>,
) -> Result<ReservationLifecycleStep<&'p serde_json::Map<String, serde_json::Value>>, String> {
    if is_reserve {
        let status = property_string(props, "status");
        let lease_expires_at_ms =
            (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
        if !matches!(status, "leased" | "running") || lease_expires_at_ms <= request.now_ms {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::Stale,
                request,
                None,
                None,
                0,
                vec![],
            )));
        }
    } else if (!is_reclaim || !work_item_fence.superseded)
        && !matches!(
            property_string(props, "status"),
            "leased" | "running" | "succeeded" | "failed" | "cancelled" | "dead_letter"
        )
    {
        // Release: any non-terminal, non-live status is stale.
        // Reclaim: same, but a reclaim of an already-superseded
        // reservation is legitimate (that is precisely what reclaim
        // is for), so it is exempted -- `!is_reclaim ||
        // !superseded` is `true` for release and `!superseded` for
        // reclaim, matching the two branches this replaces.
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Stale,
            request,
            None,
            None,
            0,
            vec![],
        )));
    }
    let (_repository, extension) = resource_metadata_maps(props)
        .map_err(|_| "WorkItem resource admission extension is invalid".to_string())?;
    if is_reserve {
        let expected = resource_recomputed_fingerprint(props, request)?;
        if expected != request.input_fingerprint {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::InputConflict,
                request,
                None,
                None,
                0,
                vec![],
            )));
        }
    }
    Ok(ReservationLifecycleStep::Continue(extension))
}

/// Phase 4: the SECOND existing-reservation branch -- when a reservation row is
/// already on file, this is guaranteed to fully decide the request (idempotent
/// replay, reserve short-circuit, reclaim-not-yet-expired refusal, or the actual
/// release/reclaim commit that decrements the host and tombstones the record).
/// Returns `Ok(None)` only when `existing` is `None`, meaning: no decision made,
/// continue into the reserve-admission path. Literal relocation of the original
/// function's fourth block (`if let Some(stored) = existing.as_ref() { .. }`).
#[allow(clippy::too_many_arguments)]
fn resource_commit_release_or_reclaim(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    is_reserve: bool,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let Some(stored) = existing else {
        return Ok(None);
    };
    if stored.record.tenant_ref != request.tenant_ref {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Conflict,
            request,
            None,
            None,
            0,
            vec![],
        )));
    }
    if !resource_request_matches_record(request, &stored.record) {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            stored.fairness_debt,
            vec![],
        )));
    }
    if stored.record.state != ResourceReservationRecordState::Reserved {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )));
    }
    if is_reserve {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )));
    }
    if is_reclaim {
        if request.now_ms < stored.record.expires_at_ms {
            return Ok(Some(resource_result_payload(
                ResourceReservationResultDecision::Policy,
                request,
                Some(stored.record.clone()),
                None,
                stored.fairness_debt,
                vec![],
            )));
        }
        let status = property_string(props, "status");
        let lease_expires_at_ms =
            (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
        if matches!(status, "leased" | "running") && lease_expires_at_ms > request.now_ms {
            return Ok(Some(resource_result_payload(
                ResourceReservationResultDecision::Policy,
                request,
                Some(stored.record.clone()),
                None,
                stored.fairness_debt,
                vec![],
            )));
        }
    }
    let Some(mut host) = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)? else {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            Some(stored.record.clone()),
            None,
            stored.fairness_debt,
            vec![],
        )));
    };
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_sub(stored.held_cpu_weight)
        .ok_or_else(|| "resource host cpu accounting underflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_sub(stored.held_memory_mib)
        .ok_or_else(|| "resource host memory accounting underflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_sub(stored.held_disk_mib)
        .ok_or_else(|| "resource host disk accounting underflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_sub(stored.held_process_slots)
        .ok_or_else(|| "resource host process accounting underflow".to_string())?;
    resource_put_host(hosts, graph, &host, crypto)?;
    let mut next = stored.clone();
    next.record.state = if is_reclaim {
        if work_item_fence.superseded {
            ResourceReservationRecordState::Superseded
        } else {
            ResourceReservationRecordState::Reclaimed
        }
    } else {
        ResourceReservationRecordState::Released
    };
    next.record.revision = next.record.revision.saturating_add(1);
    next.record.lifecycle_revision = next.record.lifecycle_revision.saturating_add(1);
    next.record.expected_lifecycle_revision = request.expected_lifecycle_revision;
    next.record.tombstone = true;
    next.held_cpu_weight = 0;
    next.held_memory_mib = 0;
    next.held_disk_mib = 0;
    next.held_process_slots = 0;
    let debt_row = resource_load_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        crypto,
    )?;
    // Fairness debt is historical service debt, not held capacity;
    // releasing a reservation must not erase the cost already
    // charged to this tenant/group.
    let debt = debt_row.debt;
    next.fairness_debt = debt;
    resource_put_reservation(reservations, graph, &next, crypto)?;
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(concurrency, graph, &concurrency_key, -1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(anti_affinity, graph, &request.host_ref, tag, -1)?;
    }
    for key in resource_exclusivity_keys(request) {
        let owner = exclusivity
            .get((graph, key.as_str()))
            .map_err(|error| error.to_string())?
            .map(|value| value.value().to_string());
        if owner.as_deref() == Some(request.reservation_id.as_str()) {
            exclusivity
                .remove((graph, key.as_str()))
                .map_err(|error| error.to_string())?;
        }
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = disk_policies
        .get((graph, disk_key.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    if let Some(policy_bytes) = existing_policy {
        let mut policy: DurableResourceDiskPolicy = resource_decode(&policy_bytes, crypto)?;
        if policy.low_watermark_mib == request.disk_low_watermark_mib
            && policy.high_watermark_mib == request.disk_high_watermark_mib
        {
            let used = host.disk_used_mib.saturating_add(host.held_disk_mib);
            if policy
                .low_watermark_mib
                .is_some_and(|watermark| used <= watermark)
            {
                policy.blocked = false;
                policy.revision = policy.revision.saturating_add(1);
                let bytes = resource_encode(&policy, crypto)?;
                disk_policies
                    .insert((graph, disk_key.as_str()), bytes.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(next.record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )))
}

/// Phase 5 (reserve-only path): the attempt-index winner check. Literal relocation.
fn resource_check_attempt_winner_conflict(
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let winner = attempts
        .get((graph, request.work_item_id.as_str(), request.attempt))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_string());
    if let Some(winner) = winner {
        // The attempt index is a derived invariant, not an alternate
        // source of reservation truth.  Recharging an existing host
        // when the index points at a missing authoritative row would
        // turn partial/corrupt state into a second accepted hold.
        if resource_load_reservation(reservations, graph, &winner, crypto)?.is_none() {
            return Err("resource reservation attempt index references missing reservation".into());
        }
        if winner != request.reservation_id {
            return Ok(Some(resource_result_payload(
                ResourceReservationResultDecision::Conflict,
                request,
                None,
                None,
                0,
                vec![],
            )));
        }
    }
    Ok(None)
}

/// Phase 6 (reserve-only path): every host-admission gate (freshness, labels, target
/// selection, anti-affinity, concurrency, exclusivity, capacity, disk-policy bound and
/// hysteresis), including the disk-policy table normalization writes that were part of
/// the same guard chain in the original function. Literal relocation; the ONLY
/// difference from the original is that the final "insert a fresh default policy when
/// none existed" step (previously the last few lines before the fairness/commit phase)
/// is included here rather than split across the phase boundary, because nothing after
/// it in the original function ever read `existing_policy`, `policy_rows`, or
/// `disk_key` again.
#[allow(clippy::too_many_arguments)]
fn resource_admit_reserve_host(
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    let host = resource_load_host(hosts, graph, &request.host_ref, crypto)?;
    let Some(host) = host else {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::NotFound,
            request,
            None,
            None,
            0,
            vec![],
        )));
    };
    // Admission and host snapshots share the schema's 128-policy
    // bound.  Enumerating this exact host prefix is part of the same
    // transaction, so a new policy key cannot race a concurrent
    // reservation into an undecodable/unbounded host projection.
    let policy_rows =
        resource_collect_disk_policy_rows(disk_policies, graph, &request.host_ref, crypto)?;
    if let Some(expected) = request.expected_host_revision {
        if expected != host.revision {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::StaleHost,
                request,
                None,
                Some(&host),
                0,
                vec![],
            )));
        }
    }
    let host_state = resource_validate_host_freshness(&host, request.now_ms);
    if host_state != ResourceReservationResultDecision::Accepted {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            host_state,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    if !request
        .required_labels
        .iter()
        .all(|label| host.labels.iter().any(|value| value == label))
    {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Labels,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    if !resource_target_selection_matches(extension, &host)? {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    // The request target is the scheduler's selected placement, while
    // preferred/required targets in the WorkItem extension describe
    // eligibility and ordering.  Once selected, the host's immutable
    // target identity must still equal the asserted local/alias pair;
    // otherwise a local record could carry an inventory host snapshot
    // (or vice versa) and RM could reconstruct a contradictory target.
    if !resource_selected_target_matches_request(request, &host) {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    for tag in &request.anti_affinity {
        let count = anti_affinity
            .get((graph, request.host_ref.as_str(), tag.as_str()))
            .map_err(|error| error.to_string())?
            .map(|value| value.value())
            .unwrap_or(0);
        if count != 0 {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::AntiAffinity,
                request,
                None,
                Some(&host),
                0,
                vec![],
            )));
        }
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    let concurrency_count = concurrency
        .get((graph, concurrency_key.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    if request
        .concurrency_limit
        .is_some_and(|limit| concurrency_count >= limit)
    {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Concurrency,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    for key in resource_exclusivity_keys(request) {
        if exclusivity
            .get((graph, key.as_str()))
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::Exclusivity,
                request,
                None,
                Some(&host),
                0,
                vec![],
            )));
        }
    }
    if !resource_capacity_sum(&host, &request.requirement) {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Capacity,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = disk_policies
        .get((graph, disk_key.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| resource_decode::<DurableResourceDiskPolicy>(value.value(), crypto))
        .transpose()?;
    if existing_policy.is_none() && policy_rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    if let Some(policy) = existing_policy.as_ref() {
        if policy.low_watermark_mib != request.disk_low_watermark_mib
            || policy.high_watermark_mib != request.disk_high_watermark_mib
        {
            return Ok(ReservationLifecycleStep::Return(resource_result_payload(
                ResourceReservationResultDecision::Policy,
                request,
                None,
                Some(&host),
                0,
                vec![],
            )));
        }
    }
    let available_disk = host
        .disk_capacity_mib
        .checked_sub(host.disk_used_mib)
        .and_then(|value| value.checked_sub(host.held_disk_mib))
        .unwrap_or(0);
    if request.requirement.disk_mib > available_disk {
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Disk,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    let predicted_used = host
        .disk_used_mib
        .checked_add(host.held_disk_mib)
        .and_then(|value| value.checked_add(request.requirement.disk_mib))
        .ok_or_else(|| "resource disk accounting overflow".to_string())?;
    let blocked = resource_disk_policy_blocked(
        existing_policy
            .as_ref()
            .is_some_and(|policy| policy.blocked),
        predicted_used,
        request.disk_low_watermark_mib,
        request.disk_high_watermark_mib,
    );
    if blocked {
        let policy = DurableResourceDiskPolicy {
            blocked,
            low_watermark_mib: request.disk_low_watermark_mib,
            high_watermark_mib: request.disk_high_watermark_mib,
            revision: existing_policy
                .as_ref()
                .map_or(1, |value| value.revision.saturating_add(1)),
        };
        let bytes = resource_encode(&policy, crypto)?;
        disk_policies
            .insert((graph, disk_key.as_str()), bytes.as_slice())
            .map_err(|error| error.to_string())?;
        return Ok(ReservationLifecycleStep::Return(resource_result_payload(
            ResourceReservationResultDecision::Disk,
            request,
            None,
            Some(&host),
            0,
            vec![],
        )));
    }
    if existing_policy
        .as_ref()
        .is_some_and(|policy| policy.blocked != blocked)
    {
        let policy = DurableResourceDiskPolicy {
            blocked,
            low_watermark_mib: request.disk_low_watermark_mib,
            high_watermark_mib: request.disk_high_watermark_mib,
            revision: existing_policy
                .as_ref()
                .map_or(1, |value| value.revision.saturating_add(1)),
        };
        let bytes = resource_encode(&policy, crypto)?;
        disk_policies
            .insert((graph, disk_key.as_str()), bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    if existing_policy.is_none() {
        let policy = DurableResourceDiskPolicy {
            blocked: false,
            low_watermark_mib: request.disk_low_watermark_mib,
            high_watermark_mib: request.disk_high_watermark_mib,
            revision: 1,
        };
        let bytes = resource_encode(&policy, crypto)?;
        disk_policies
            .insert((graph, disk_key.as_str()), bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    Ok(ReservationLifecycleStep::Continue(host))
}

/// Phase 7 (reserve-only path): fairness-debt update, host held-capacity increments,
/// and the final durable persist (reservation, tenant index, attempt index,
/// exclusivity, concurrency, anti-affinity) that produces the Accepted result.
/// Literal relocation of the original function's final block.
#[allow(clippy::too_many_arguments)]
fn resource_commit_reserve_admission(
    graph: &str,
    request: &ResourceReservationRequest,
    mut host: DurableResourceHost,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let mut debt = resource_load_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        crypto,
    )?
    .debt;
    debt = debt
        .checked_add(request.fairness_cost)
        .ok_or_else(|| "resource fairness debt overflow".to_string())?;
    let fairness_row = DurableResourceFairness { debt };
    resource_put_fairness(
        fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        &fairness_row,
        crypto,
    )?;
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_add(request.requirement.cpu_weight)
        .ok_or_else(|| "resource host cpu accounting overflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_add(request.requirement.memory_mib)
        .ok_or_else(|| "resource host memory accounting overflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_add(request.requirement.disk_mib)
        .ok_or_else(|| "resource host disk accounting overflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_add(request.requirement.process_slots)
        .ok_or_else(|| "resource host process accounting overflow".to_string())?;
    let record = resource_build_record(request, &host, 1, 1)?;
    let stored = DurableResourceReservation {
        record: record.clone(),
        held_cpu_weight: request.requirement.cpu_weight,
        held_memory_mib: request.requirement.memory_mib,
        held_disk_mib: request.requirement.disk_mib,
        held_process_slots: request.requirement.process_slots,
        fairness_debt: debt,
    };
    resource_put_host(hosts, graph, &host, crypto)?;
    resource_put_reservation(reservations, graph, &stored, crypto)?;
    tenant_index
        .insert(
            (
                graph,
                request.tenant_ref.as_str(),
                request.reservation_id.as_str(),
            ),
            request.reservation_id.as_str(),
        )
        .map_err(|error| error.to_string())?;
    attempts
        .insert(
            (graph, request.work_item_id.as_str(), request.attempt),
            request.reservation_id.as_str(),
        )
        .map_err(|error| error.to_string())?;
    for key in resource_exclusivity_keys(request) {
        exclusivity
            .insert((graph, key.as_str()), request.reservation_id.as_str())
            .map_err(|error| error.to_string())?;
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(concurrency, graph, &concurrency_key, 1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(anti_affinity, graph, &request.host_ref, tag, 1)?;
    }
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )))
}

fn resource_request_from_record(
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> ResourceReservationRequest {
    ResourceReservationRequest {
        schema_version: crate::epistemic_operations::ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: record.tenant_ref.clone(),
        work_item_id: record.work_item_id.clone(),
        owner_id: record.owner_id.clone(),
        fence: record.fence.clone(),
        lease_epoch: record.lease_epoch,
        fencing_token: record.fencing_token,
        attempt: record.attempt,
        reservation_id: record.reservation_id.clone(),
        input_fingerprint: record.input_fingerprint.clone(),
        profile_name: record.profile_name.clone(),
        profile_version: record.profile_version.clone(),
        host_ref: record.host_ref.clone(),
        requirement: record.requirement.clone(),
        target_kind: match record.target_kind {
            ResourceReservationRecordTargetKind::Local => {
                ResourceReservationRequestTargetKind::Local
            }
            ResourceReservationRecordTargetKind::InventoryAlias => {
                ResourceReservationRequestTargetKind::InventoryAlias
            }
        },
        target_alias: record.target_alias.clone(),
        repository_id: record.repository_id.clone(),
        branch: record.branch.clone(),
        concurrency_key: record.concurrency_key.clone(),
        concurrency_limit: record.concurrency_limit,
        repository_exclusive: record.repository_exclusive,
        branch_exclusive: record.branch_exclusive,
        required_labels: record.required_labels.clone(),
        anti_affinity: record.anti_affinity.clone(),
        fairness_group: record.fairness_group.clone(),
        fairness_cost: record.fairness_cost,
        disk_low_watermark_mib: record.disk_low_watermark_mib,
        disk_high_watermark_mib: record.disk_high_watermark_mib,
        disk_policy_key: record.disk_policy_key.clone(),
        reserved_at_ms: record.reserved_at_ms,
        expires_at_ms: record.expires_at_ms,
        idempotency_key: format!("query:{}", record.reservation_id),
        now_ms,
        expected_host_revision: record.expected_host_revision,
        expected_lifecycle_revision: record.expected_lifecycle_revision,
    }
}

fn resource_no_reservation_query_result(
    request: &ResourceReservationStatusRequest,
    decision: ResourceReservationResultDecision,
) -> crate::protocol::ResultPayload {
    let work_item_id = request.work_item_id.clone().unwrap_or_default();
    crate::protocol::ResultPayload::raw(&ResourceReservationResult {
        schema_version: ResourceReservationResultSchemaVersion::V1,
        decision,
        reservation_id: None,
        work_item_id,
        attempt: request.attempt.unwrap_or(1),
        lease_epoch: request.lease_epoch.unwrap_or(0),
        fencing_token: request.fencing_token.unwrap_or(0),
        lifecycle_revision: 0,
        host_ref: None,
        host_revision: 0,
        record: None,
        state: ResourceReservationResultState::Absent,
        held_cpu_weight: 0,
        held_memory_mib: 0,
        held_disk_mib: 0,
        held_process_slots: 0,
        fairness_debt: 0,
        tombstone: false,
        changed_work_item_ids: Vec::new(),
    })
}

/// Bounded-text validation of a status query's optional selectors, in the
/// original field order so the first offending field is still the one reported.
fn resource_validate_query_selectors(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.work_item_id.as_deref() {
        resource_text(value, "resource query work_item_id")?;
    }
    if let Some(value) = request.reservation_id.as_deref() {
        resource_text(value, "resource query reservation_id")?;
    }
    if let Some(value) = request.host_ref.as_deref() {
        resource_text(value, "resource query host_ref")?;
    }
    if let Some(value) = request.owner_id.as_deref() {
        resource_text(value, "resource query owner_id")?;
    }
    Ok(())
}

/// Continuation of `resource_validate_query_selectors`: fence, fingerprint,
/// fairness group and cursor.
fn resource_validate_query_correlations(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.fence.as_deref() {
        resource_text(value, "resource query fence")?;
    }
    if let Some(value) = request.input_fingerprint.as_deref() {
        resource_fingerprint(value, "resource query input_fingerprint")?;
    }
    if let Some(value) = request.fairness_group.as_deref() {
        resource_text(value, "resource query fairness_group")?;
    }
    if let Some(value) = request.cursor.as_deref() {
        resource_text(value, "resource query cursor")?;
    }
    Ok(())
}

fn resource_validate_query_request(
    request: &ResourceReservationStatusRequest,
    require_limit: bool,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource query tenant_ref")?;
    resource_validate_query_selectors(request)?;
    resource_validate_query_correlations(request)?;
    if request.attempt.is_some_and(|attempt| attempt == 0) {
        return Err("resource query attempt must be positive".into());
    }
    if require_limit {
        if request.limit == 0 || request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
            return Err("resource status request violates bounds".into());
        }
    } else if request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
        return Err("resource query violates bounds".into());
    }
    Ok(())
}

fn resource_record_work_item_live(
    props: &serde_json::Map<String, serde_json::Value>,
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> bool {
    let status = property_string(props, "status");
    let owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let lease_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == record.tenant_ref
        && property_u64(props, "attempt") == record.attempt
        && property_u64(props, "lease_epoch") == record.lease_epoch
        && property_u64(props, "fencing_token") == record.fencing_token
        && owner == record.owner_id
        && matches!(status, "leased" | "running")
        && lease_until > now_ms
        && resource_expected_fence(record.fencing_token) == record.fence
}

fn resource_decode_result_payload(
    payload: crate::protocol::ResultPayload,
) -> Result<ResourceReservationResult, String> {
    let bytes = match payload {
        crate::protocol::ResultPayload::Raw(bytes)
        | crate::protocol::ResultPayload::PropertiesMsgpack(bytes) => bytes,
        _ => return Err("resource query result encoding failed".into()),
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .map_err(|_| "resource query result encoding failed".into())
}

/// Exact query reads the native reservation row and all caller correlations
/// from one MVCC snapshot.  A null reservation id is the intentionally narrow
/// current-WorkItem precheck used for scheduler ranking; it never returns a
/// reservation ledger and Reserve revalidates the same fence transactionally.
#[allow(clippy::type_complexity)]
fn resolve_current_work_item_query_identity_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str), String> {
    let work_item_id = request
        .work_item_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires work_item_id".to_string())?;
    let owner = request
        .owner_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires owner_id".to_string())?;
    let fence = request
        .fence
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires fence".to_string())?;
    Ok((work_item_id, owner, fence))
}

fn resolve_current_work_item_query_fence_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(u64, u64, u64), String> {
    let attempt = request
        .attempt
        .ok_or_else(|| "current WorkItem query requires attempt".to_string())?;
    let lease_epoch = request
        .lease_epoch
        .ok_or_else(|| "current WorkItem query requires lease_epoch".to_string())?;
    let fencing_token = request
        .fencing_token
        .ok_or_else(|| "current WorkItem query requires fencing_token".to_string())?;
    Ok((attempt, lease_epoch, fencing_token))
}

#[allow(clippy::type_complexity)]
fn resolve_current_work_item_query_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str, u64, u64, u64), String> {
    let (work_item_id, owner, fence) = resolve_current_work_item_query_identity_fields(request)?;
    let (attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fence_fields(request)?;
    Ok((
        work_item_id,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    ))
}

#[allow(clippy::too_many_arguments)]
fn current_work_item_query_matches(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationStatusRequest,
    owner: &str,
    fence: &str,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
) -> bool {
    let current_attempt = property_u64(props, "attempt");
    let current_epoch = property_u64(props, "lease_epoch");
    let current_token = property_u64(props, "fencing_token");
    let status = property_string(props, "status");
    let current_owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let live_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == request.tenant_ref
        && current_attempt == attempt
        && current_epoch == lease_epoch
        && current_token == fencing_token
        && fence == resource_expected_fence(fencing_token)
        && current_owner == owner
        && matches!(status, "leased" | "running")
        && live_until > request.now_ms
}

fn read_resource_reservation_current_work_item_query(
    nodes: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let (work_item_id, owner, fence, attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fields(request)?;
    let bytes = nodes
        .get((graph, work_item_id))
        .map_err(|error| error.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = bytes else {
        return resource_decode_result_payload(resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        ));
    };
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let current = current_work_item_query_matches(
        &props,
        request,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    );
    let decision = if current {
        ResourceReservationResultDecision::Accepted
    } else {
        ResourceReservationResultDecision::Stale
    };
    resource_decode_result_payload(resource_no_reservation_query_result(request, decision))
}

// RM's mirrorless retry query intentionally omits the fingerprint: the
// native record is the source of truth and the adapter compares it
// after decoding.  If a mirror supplies one, it remains an exact
// correlation and a mismatch fails closed.
fn resource_reservation_query_correlates(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.work_item_id.as_deref() == Some(record.work_item_id.as_str())
        && request
            .host_ref
            .as_deref()
            .is_none_or(|host_ref| host_ref == record.host_ref)
        && request.owner_id.as_deref() == Some(record.owner_id.as_str())
        && request.fence.as_deref() == Some(record.fence.as_str())
        && request.attempt == Some(record.attempt)
        && request.lease_epoch == Some(record.lease_epoch)
        && request.fencing_token == Some(record.fencing_token)
        && request
            .input_fingerprint
            .as_deref()
            .is_none_or(|fingerprint| fingerprint == record.input_fingerprint.as_str())
}

fn resource_reservation_query_host(
    rtx: &redb::ReadTransaction,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let hosts = rtx
        .open_table(RESOURCE_HOSTS)
        .map_err(|error| error.to_string())?;
    hosts
        .get((graph, record.host_ref.as_str()))
        .map_err(|error| error.to_string())?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

fn resource_reservation_query_current_work_item(
    nodes: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
    let current_item = nodes
        .get((graph, record.work_item_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    current_item
        .as_deref()
        .map(decode_durable::<serde_json::Map<String, serde_json::Value>>)
        .transpose()
}

fn build_resource_reservation_query_result(
    rtx: &redb::ReadTransaction,
    nodes: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    stored: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let record = &stored.record;
    let host = resource_reservation_query_host(rtx, graph, record, crypto)?;
    let request_for_payload = resource_request_from_record(record, request.now_ms);
    let current = resource_reservation_query_current_work_item(nodes, graph, record, crypto)?;
    let current_valid = current
        .as_ref()
        .is_some_and(|props| resource_record_work_item_live(props, record, request.now_ms));
    let tombstone_replay = record.tombstone;
    let decision = if current_valid || tombstone_replay {
        ResourceReservationResultDecision::Idempotent
    } else {
        ResourceReservationResultDecision::Stale
    };
    let bytes = resource_result_payload(
        decision,
        &request_for_payload,
        (current_valid || tombstone_replay).then(|| record.clone()),
        host.as_ref(),
        stored.fairness_debt,
        Vec::new(),
    );
    resource_decode_result_payload(bytes)
}

fn read_resource_reservation_by_id(
    rtx: &redb::ReadTransaction,
    nodes: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let reservation_id = request.reservation_id.as_deref().unwrap_or_default();
    resource_text(reservation_id, "resource reservation_id")?;
    let reservations = rtx
        .open_table(RESOURCE_RESERVATIONS)
        .map_err(|error| error.to_string())?;
    // A mirrorless RM admission query is an expected pre-reserve read.  A
    // missing native row is a typed absence, not a transport failure; the
    // scheduler then submits Reserve and lets that transaction revalidate
    // the WorkItem/fence atomically.
    let Some(row) = reservations
        .get((graph, reservation_id))
        .map_err(|error| error.to_string())?
    else {
        return resource_decode_result_payload(resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        ));
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    if stored.record.tenant_ref != request.tenant_ref {
        // Preserve tenant isolation while keeping the public query vocabulary
        // typed and bounded.  Do not reveal whether another tenant owns this
        // reservation id through a transport error.
        return resource_decode_result_payload(resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        ));
    }
    if !resource_reservation_query_correlates(request, &stored.record) {
        return Err("resource reservation correlation does not match".into());
    }
    build_resource_reservation_query_result(rtx, nodes, graph, request, &stored, crypto)
}

pub(crate) fn read_resource_reservation(
    db: &Database,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    resource_validate_query_request(request, false)?;
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|error| error.to_string())?;
    if request.reservation_id.is_none() {
        return read_resource_reservation_current_work_item_query(&nodes, graph, request, crypto);
    }
    read_resource_reservation_by_id(&rtx, &nodes, graph, request, crypto)
}

#[allow(clippy::type_complexity)]
fn open_resource_reservation_status_tables(
    rtx: &redb::ReadTransaction,
) -> Result<
    (
        redb::ReadOnlyTable<(&'static str, &'static str), &'static [u8]>,
        redb::ReadOnlyTable<(&'static str, &'static str, &'static str), &'static str>,
        redb::ReadOnlyTable<(&'static str, &'static str), &'static [u8]>,
        redb::ReadOnlyTable<(&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let reservations = rtx
        .open_table(RESOURCE_RESERVATIONS)
        .map_err(|error| error.to_string())?;
    let tenant_index = rtx
        .open_table(RESOURCE_RESERVATION_TENANT_INDEX)
        .map_err(|error| error.to_string())?;
    let hosts = rtx
        .open_table(RESOURCE_HOSTS)
        .map_err(|error| error.to_string())?;
    let disk_policies = rtx
        .open_table(RESOURCE_DISK_POLICIES)
        .map_err(|error| error.to_string())?;
    Ok((reservations, tenant_index, hosts, disk_policies))
}

enum ResourceReservationStatusRowOutcome {
    StopScan,
    SkipCursor,
    Orphan,
    Processed {
        superseded: bool,
        summary: Option<ResourceReservationSummary>,
    },
}

fn resource_reservation_status_row_is_filtered_out(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request
        .host_ref
        .as_deref()
        .is_some_and(|host| host != record.host_ref)
        || request
            .work_item_id
            .as_deref()
            .is_some_and(|id| id != record.work_item_id)
        || request
            .fairness_group
            .as_deref()
            .is_some_and(|group| group != record.fairness_group)
        || request
            .owner_id
            .as_deref()
            .is_some_and(|owner| owner != record.owner_id)
        || request
            .fence
            .as_deref()
            .is_some_and(|fence| fence != record.fence)
        || request
            .input_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| fingerprint != record.input_fingerprint)
}

fn build_resource_reservation_summary(
    record: &ResourceReservationRecord,
    stored: &DurableResourceReservation,
) -> ResourceReservationSummary {
    let is_reserved = record.state == ResourceReservationRecordState::Reserved;
    ResourceReservationSummary {
        reservation_id: record.reservation_id.clone(),
        work_item_id: record.work_item_id.clone(),
        attempt: record.attempt,
        host_ref: record.host_ref.clone(),
        profile_name: record.profile_name.clone(),
        fairness_group: record.fairness_group.clone(),
        state: resource_summary_state(record.state),
        revision: record.revision,
        expires_at_ms: record.expires_at_ms,
        held_cpu_weight: if is_reserved {
            stored.held_cpu_weight
        } else {
            0
        },
        held_memory_mib: if is_reserved {
            stored.held_memory_mib
        } else {
            0
        },
        held_disk_mib: if is_reserved { stored.held_disk_mib } else { 0 },
        held_process_slots: if is_reserved {
            stored.held_process_slots
        } else {
            0
        },
        tombstone: record.tombstone,
    }
}

fn resolve_resource_reservation_status_row(
    reservation_id: &str,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    reservations: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    let Some(row) = reservations
        .get((graph, reservation_id))
        .map_err(|error| error.to_string())?
    else {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    let record = &stored.record;
    if record.tenant_ref != request.tenant_ref {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    let superseded = record.state == ResourceReservationRecordState::Superseded;
    if resource_reservation_status_row_is_filtered_out(request, record) {
        return Ok(ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary: None,
        });
    }
    let summary = build_resource_reservation_summary(record, &stored);
    Ok(ResourceReservationStatusRowOutcome::Processed {
        superseded,
        summary: Some(summary),
    })
}

#[allow(clippy::too_many_arguments)]
fn resource_reservation_status_row_outcome(
    row_graph: &str,
    tenant: &str,
    reservation_id: &str,
    index_value: &str,
    graph: &str,
    cursor: &str,
    request: &ResourceReservationStatusRequest,
    reservations: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    if row_graph != graph || tenant != request.tenant_ref {
        return Ok(ResourceReservationStatusRowOutcome::StopScan);
    }
    if reservation_id == cursor {
        return Ok(ResourceReservationStatusRowOutcome::SkipCursor);
    }
    if index_value != reservation_id {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    resolve_resource_reservation_status_row(reservation_id, graph, request, reservations, crypto)
}

struct ResourceReservationStatusScan {
    values: Vec<ResourceReservationSummary>,
    has_more: bool,
    last_returned_cursor: Option<String>,
    orphan_count: u64,
    superseded_count: u64,
}

fn apply_resource_reservation_status_row_outcome(
    outcome: ResourceReservationStatusRowOutcome,
    scan: &mut ResourceReservationStatusScan,
    limit: usize,
) -> std::ops::ControlFlow<()> {
    match outcome {
        ResourceReservationStatusRowOutcome::StopScan => std::ops::ControlFlow::Break(()),
        ResourceReservationStatusRowOutcome::SkipCursor => std::ops::ControlFlow::Continue(()),
        ResourceReservationStatusRowOutcome::Orphan => {
            scan.orphan_count = scan.orphan_count.saturating_add(1);
            std::ops::ControlFlow::Continue(())
        }
        ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary,
        } => {
            if superseded {
                scan.superseded_count = scan.superseded_count.saturating_add(1);
            }
            let Some(summary) = summary else {
                return std::ops::ControlFlow::Continue(());
            };
            let reservation_cursor = summary.reservation_id.clone();
            scan.values.push(summary);
            if scan.values.len() > limit {
                scan.values.pop();
                scan.has_more = true;
                return std::ops::ControlFlow::Break(());
            }
            scan.last_returned_cursor = Some(reservation_cursor);
            std::ops::ControlFlow::Continue(())
        }
    }
}

fn scan_resource_reservation_status_rows(
    tenant_index: &redb::ReadOnlyTable<(&str, &str, &str), &str>,
    reservations: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    cursor: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusScan, String> {
    let mut scan = ResourceReservationStatusScan {
        values: Vec::new(),
        has_more: false,
        last_returned_cursor: None,
        orphan_count: 0,
        superseded_count: 0,
    };
    let mut scanned = 0usize;
    for row in tenant_index
        .range((graph, request.tenant_ref.as_str(), cursor)..)
        .map_err(|error| error.to_string())?
    {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_STATUS_SCAN {
            return Err("resource status scan exceeds native bound".into());
        }
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        let outcome = resource_reservation_status_row_outcome(
            row_graph,
            tenant,
            reservation_id,
            value.value(),
            graph,
            cursor,
            request,
            reservations,
            crypto,
        )?;
        if apply_resource_reservation_status_row_outcome(outcome, &mut scan, request.limit as usize)
            .is_break()
        {
            break;
        }
    }
    Ok(scan)
}

fn resource_reservation_status_host(
    hosts: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let Some(host_ref) = request.host_ref.as_deref() else {
        return Ok(None);
    };
    hosts
        .get((graph, host_ref))
        .map_err(|error| error.to_string())?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

fn decode_resource_disk_policy_row(
    policy_key: &str,
    prefix: &str,
    value_bytes: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(String, DurableResourceDiskPolicy), String> {
    let policy_key = policy_key
        .strip_prefix(prefix)
        .ok_or_else(|| "resource disk-policy key escaped host scope".to_string())?;
    resource_text(policy_key, "resource disk_policy_key")?;
    Ok((
        policy_key.to_string(),
        resource_decode::<DurableResourceDiskPolicy>(value_bytes, crypto)?,
    ))
}

fn resource_reservation_status_host_policies(
    disk_policies: &redb::ReadOnlyTable<(&str, &str), &[u8]>,
    graph: &str,
    host: Option<&DurableResourceHost>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, DurableResourceDiskPolicy)>, String> {
    let Some(host) = host else {
        return Ok(Vec::new());
    };
    let prefix = format!("{}\0", host.host_ref);
    let mut rows = Vec::new();
    for row in disk_policies
        .range((graph, prefix.as_str())..)
        .map_err(|error| error.to_string())?
    {
        if rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
            return Err("resource disk-policy scan exceeds native bound".to_string());
        }
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, policy_key) = key.value();
        if row_graph != graph || !policy_key.starts_with(&prefix) {
            break;
        }
        rows.push(decode_resource_disk_policy_row(
            policy_key,
            &prefix,
            value.value(),
            crypto,
        )?);
    }
    Ok(rows)
}

fn resource_reservation_status_fairness_debt(
    rtx: &redb::ReadTransaction,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    let Some(group) = request.fairness_group.as_deref() else {
        return Ok(0);
    };
    let fairness = rtx
        .open_table(RESOURCE_FAIRNESS)
        .map_err(|error| error.to_string())?;
    let row = fairness
        .get((
            graph,
            resource_fairness_scope_key(&request.tenant_ref, group).as_str(),
        ))
        .map_err(|error| error.to_string())?
        .map(|row| resource_decode::<DurableResourceFairness>(row.value(), crypto))
        .transpose()?;
    Ok(row.map_or(0, |row| row.debt))
}

fn build_resource_reservation_status_result(
    scan: ResourceReservationStatusScan,
    host: Option<DurableResourceHost>,
    host_snapshot: Option<ResourceReservationHostSnapshot>,
    fairness_debt: u64,
) -> ResourceReservationStatusResult {
    let next_cursor = scan.has_more.then_some(scan.last_returned_cursor).flatten();
    ResourceReservationStatusResult {
        schema_version: ResourceReservationStatusResultSchemaVersion::V1,
        complete: !scan.has_more,
        next_cursor,
        host_snapshot,
        host_ref: host.as_ref().map(|value| value.host_ref.clone()),
        host_revision: host.as_ref().map_or(0, |value| value.revision),
        held_cpu_weight: host.as_ref().map_or(0, |value| value.held_cpu_weight),
        held_memory_mib: host.as_ref().map_or(0, |value| value.held_memory_mib),
        held_disk_mib: host.as_ref().map_or(0, |value| value.held_disk_mib),
        held_process_slots: host.as_ref().map_or(0, |value| value.held_process_slots),
        fairness_debt,
        reservations: scan.values,
        orphan_count: scan.orphan_count,
        superseded_count: scan.superseded_count,
    }
}

pub(crate) fn read_resource_reservation_status(
    db: &Database,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusResult, String> {
    resource_validate_query_request(request, true)?;
    let cursor = request.cursor.as_deref().unwrap_or("");
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let (reservations, tenant_index, hosts, disk_policies) =
        open_resource_reservation_status_tables(&rtx)?;

    let scan = scan_resource_reservation_status_rows(
        &tenant_index,
        &reservations,
        graph,
        cursor,
        request,
        crypto,
    )?;

    let host = resource_reservation_status_host(&hosts, graph, request, crypto)?;
    let host_policies =
        resource_reservation_status_host_policies(&disk_policies, graph, host.as_ref(), crypto)?;
    let host_snapshot = host
        .as_ref()
        .map(|value| resource_reservation_host_snapshot(value, &host_policies))
        .transpose()?;
    let fairness_debt = resource_reservation_status_fairness_debt(&rtx, graph, request, crypto)?;

    Ok(build_resource_reservation_status_result(
        scan,
        host,
        host_snapshot,
        fairness_debt,
    ))
}

/// Apply one native WorkItem transition while the MutationBatch write
/// transaction is held. The returned payload is persisted as the batch result in
/// that same transaction, so a retry observes the exact original claim/commit
/// outcome rather than running selection twice.
/// The commit-scoped inputs both WorkItem row appliers need alongside their redb
/// tables: the durable crypto handle, the authoritative commit timestamp, and the
/// outbox id. Grouped so each applier keeps a readable arity
/// (clippy::too_many_arguments) without disturbing the borrowed table params,
/// whose redb lifetimes are load-bearing.
struct WorkItemCommitScope<'a> {
    crypto: DurableCrypto<'a>,
    authoritative_now_ms: u64,
    outbox_id: &'a str,
}

fn validate_submit_work_item_identity(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{NativeControlSchemaVersion, MAX_SUBMIT_REF_BYTES};
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("SubmitWorkItem schema_version must be 1".to_string());
    }
    let context_tenant = request.context.tenant_id.as_str();
    let context_graph = request.context.graph.as_str();
    if context_tenant.trim().is_empty() || sanitize(context_graph) != graph {
        return Err("SubmitWorkItem context graph/tenant is invalid".to_string());
    }
    for (field, value) in [
        ("idempotency_key", request.idempotency_key.as_str()),
        ("command_digest", request.command_digest.as_str()),
        ("kind", request.kind.as_str()),
        ("policy_digest", request.policy_digest.as_str()),
        ("catalog_digest", request.catalog_digest.as_str()),
        ("model_digest", request.model_digest.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > MAX_SUBMIT_REF_BYTES {
            return Err(format!("SubmitWorkItem {field} is outside native bounds"));
        }
    }
    Ok(())
}

fn validate_submit_work_item_refs(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{MAX_SUBMIT_DEPENDENCIES, MAX_SUBMIT_REF_BYTES};
    if request.command_digest.len() != 64
        || !request
            .command_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("SubmitWorkItem command_digest must be a SHA-256 hex value".to_string());
    }
    if request.input_ref.len() > MAX_SUBMIT_REF_BYTES {
        return Err("SubmitWorkItem input_ref exceeds native payload bound".to_string());
    }
    if request.depends_on.len() > MAX_SUBMIT_DEPENDENCIES {
        return Err("SubmitWorkItem dependency count exceeds native bound".to_string());
    }
    Ok(())
}

fn validate_submit_work_item_identity_and_refs(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    validate_submit_work_item_identity(graph, request)?;
    validate_submit_work_item_refs(request)?;
    Ok(())
}

fn resolve_submit_work_item_max_inflight(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<u64, String> {
    let max_inflight = if request.max_tenant_in_flight == 0 {
        4096
    } else {
        request.max_tenant_in_flight
    };
    if !(1..=4096).contains(&max_inflight)
        || request.max_attempts == 0
        || request.max_attempts > 4096
        || !(-1024..=1024).contains(&request.priority)
    {
        return Err(
            "SubmitWorkItem admission/max_attempts/priority is outside native bounds".to_string(),
        );
    }
    if request
        .deadline_unix
        .is_some_and(|deadline| !deadline.is_finite() || deadline < 0.0)
    {
        return Err("SubmitWorkItem deadline_unix is invalid".to_string());
    }
    Ok(max_inflight)
}

fn validate_submit_work_item_provenance_and_metadata(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<(), String> {
    use eg_types::native_control::{
        MAX_SUBMIT_METADATA_BYTES, MAX_SUBMIT_PROVENANCE_REFS, MAX_SUBMIT_REF_BYTES,
    };
    if request.provenance_refs.len() > MAX_SUBMIT_PROVENANCE_REFS
        || request
            .provenance_refs
            .iter()
            .any(|reference| reference.trim().is_empty() || reference.len() > MAX_SUBMIT_REF_BYTES)
    {
        return Err("SubmitWorkItem provenance_refs exceed native bounds".to_string());
    }
    let metadata_bytes = rmp_serde::to_vec_named(&request.metadata).map_err(|e| e.to_string())?;
    if metadata_bytes.len() > MAX_SUBMIT_METADATA_BYTES {
        return Err("SubmitWorkItem metadata exceeds native bound".to_string());
    }
    Ok(())
}

fn resolve_submit_work_item_admission_limits(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<u64, String> {
    let max_inflight = resolve_submit_work_item_max_inflight(request)?;
    validate_submit_work_item_provenance_and_metadata(request)?;
    Ok(max_inflight)
}

fn check_submit_work_item_dependencies_unique(
    request: &eg_types::native_control::SubmitWorkItemRequest,
) -> Result<Vec<String>, String> {
    let mut dependencies = request.depends_on.clone();
    dependencies.sort();
    dependencies.dedup();
    if dependencies.len() != request.depends_on.len() {
        return Err("SubmitWorkItem dependencies must be unique".to_string());
    }
    Ok(dependencies)
}

// Resolve the dependency state in this same write snapshot.  A successful
// parent is already satisfied; every other existing WorkItem remains a
// counted dependency and receives a downstream index entry below.  The
// index is what the existing terminal WorkItem transition uses for atomic
// push-release, so native submit must populate it rather than relying on a
// later graph scan.
fn resolve_submit_work_item_dependency_row(
    graph: &str,
    dependency: &str,
    context_tenant: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    if dependency.trim().is_empty() || dependency.len() > 512 {
        return Err("SubmitWorkItem dependency id is outside native bounds".to_string());
    }
    let value = nodes
        .get((graph, dependency))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("SubmitWorkItem dependency '{dependency}' was not found"))?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(value.value())?)?;
    if property_string(&props, "node_type") != "WorkItem" {
        return Err(format!(
            "SubmitWorkItem dependency '{dependency}' is not a WorkItem"
        ));
    }
    if property_string(&props, "tenant") != context_tenant {
        return Err("ACCESS_DENIED: WorkItem dependency tenant mismatch".to_string());
    }
    Ok(property_string(&props, "status") != "succeeded")
}

fn resolve_submit_work_item_dependency_rows(
    graph: &str,
    dependencies: &[String],
    context_tenant: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, bool)>, String> {
    let mut dependency_rows = Vec::with_capacity(dependencies.len());
    for dependency in dependencies {
        let pending = resolve_submit_work_item_dependency_row(
            graph,
            dependency,
            context_tenant,
            nodes,
            crypto,
        )?;
        dependency_rows.push((dependency.clone(), pending));
    }
    Ok(dependency_rows)
}

/// A submit request's resolved admission inputs, in order: the max-inflight
/// bound, the declared dependency ids, and each dependency's `(id, satisfied)` row.
type SubmitWorkItemDependencies = (u64, Vec<String>, Vec<(String, bool)>);

fn validate_and_resolve_submit_work_item_dependencies(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<SubmitWorkItemDependencies, String> {
    validate_submit_work_item_identity_and_refs(graph, request)?;
    let max_inflight = resolve_submit_work_item_admission_limits(request)?;
    let dependencies = check_submit_work_item_dependencies_unique(request)?;
    let dependency_rows = resolve_submit_work_item_dependency_rows(
        graph,
        &dependencies,
        request.context.tenant_id.as_str(),
        nodes,
        crypto,
    )?;
    Ok((max_inflight, dependencies, dependency_rows))
}

fn is_submit_work_item_inflight_row(
    props: &serde_json::Map<String, serde_json::Value>,
    context_tenant: &str,
) -> bool {
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == context_tenant
        && !matches!(
            property_string(props, "status"),
            "succeeded" | "failed" | "cancelled" | "dead_letter" | "completed"
        )
}

fn count_submit_work_item_tenant_inflight(
    graph: &str,
    context_tenant: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    let mut inflight = 0u64;
    let mut scanned = 0usize;
    for row in nodes.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        if key.value().0 != graph {
            break;
        }
        scanned += 1;
        if scanned > 50_000 {
            return Err(
                "ADMISSION_BACKPRESSURE: WorkItem quota scan exceeds native bound".to_string(),
            );
        }
        let props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&crypto.unseal(value.value())?)?;
        if is_submit_work_item_inflight_row(&props, context_tenant) {
            inflight = inflight.saturating_add(1);
        }
    }
    Ok(inflight)
}

fn resolve_submit_work_item_id(
    graph: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let work_item_id = request.work_item_id.clone().unwrap_or_else(|| {
        let mut digest = Sha256::new();
        digest.update(graph.as_bytes());
        digest.update([0]);
        digest.update(context_tenant.as_bytes());
        digest.update([0]);
        digest.update(request.idempotency_key.as_bytes());
        format!("work-item:{}", hex::encode(digest.finalize()))
    });
    if work_item_id.trim().is_empty() || work_item_id.len() > 512 {
        return Err("SubmitWorkItem work_item_id is outside native bounds".to_string());
    }
    if nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("IDEMPOTENCY_CONFLICT: work_item_id is already present".to_string());
    }
    Ok(work_item_id)
}

fn resolve_submit_work_item_command_sequence(
    graph: &str,
    command_sequences: &mut redb::Table<&str, u64>,
) -> Result<u64, String> {
    let found_command_sequence = command_sequences.get(graph).map_err(|e| e.to_string())?;
    let command_sequence = found_command_sequence
        .map(|v| v.value())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "WorkItem command sequence exhausted".to_string())?;
    command_sequences
        .insert(graph, command_sequence)
        .map_err(|e| e.to_string())?;
    Ok(command_sequence)
}

#[allow(clippy::too_many_arguments)]
fn admit_and_identify_submit_work_item(
    graph: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
    max_inflight: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(u64, String, u64), String> {
    let inflight = count_submit_work_item_tenant_inflight(graph, context_tenant, nodes, crypto)?;
    if inflight >= max_inflight {
        return Err(format!(
            "TENANT_QUOTA: tenant has {inflight} in-flight WorkItems (limit {max_inflight})"
        ));
    }
    let work_item_id = resolve_submit_work_item_id(graph, context_tenant, request, nodes)?;
    let command_sequence = resolve_submit_work_item_command_sequence(graph, command_sequences)?;
    Ok((inflight, work_item_id, command_sequence))
}

fn submit_work_item_status(pending_dependencies: usize) -> &'static str {
    if pending_dependencies == 0 {
        "ready"
    } else {
        "submitted"
    }
}

#[allow(clippy::too_many_arguments)]
fn build_submit_work_item_props(
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    pending_dependencies: usize,
    command_sequence: u64,
    now_s: f64,
    status: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut props = serde_json::Map::new();
    props.insert(
        "node_type".into(),
        serde_json::Value::String("WorkItem".into()),
    );
    props.insert(
        "name".into(),
        serde_json::Value::String(format!("WorkItem: {}", request.kind)),
    );
    props.insert(
        "tenant".into(),
        serde_json::Value::String(context_tenant.to_string()),
    );
    props.insert(
        "kind".into(),
        serde_json::Value::String(request.kind.clone()),
    );
    props.insert(
        "queue".into(),
        serde_json::Value::String(request.kind.clone()),
    );
    props.insert("status".into(), serde_json::Value::String(status.into()));
    props.insert("state".into(), serde_json::Value::String(status.into()));
    props.insert("priority".into(), serde_json::Value::from(request.priority));
    props.insert(
        "prio_bucket".into(),
        serde_json::Value::from(request.priority),
    );
    props.insert(
        "depends_on".into(),
        serde_json::Value::Array(
            dependencies
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    props.insert(
        "dep_count".into(),
        serde_json::Value::from(pending_dependencies as u64),
    );
    props.insert(
        "downstream_ids".into(),
        serde_json::Value::Array(Vec::new()),
    );
    props.insert("next_retry_at".into(), serde_json::Value::from(0.0));
    props.insert("backoff_base_s".into(), serde_json::Value::from(1.0));
    props.insert(
        "resource_class".into(),
        serde_json::Value::String(String::new()),
    );
    props.insert(
        "fairness_group".into(),
        serde_json::Value::String(String::new()),
    );
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("last_lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(0u64));
    props.insert("fencing_token".into(), serde_json::Value::from(0u64));
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert(
        "work_item_fence".into(),
        serde_json::Value::String(String::new()),
    );
    props.insert("defer_count".into(), serde_json::Value::from(0u64));
    props.insert(
        "input_artifact_refs".into(),
        serde_json::json!([request.input_ref]),
    );
    props.insert(
        "output_artifact_refs".into(),
        serde_json::Value::Array(Vec::new()),
    );
    props.insert(
        "payload_ref".into(),
        serde_json::Value::String(request.input_ref.clone()),
    );
    props.insert("attempt".into(), serde_json::Value::from(0u64));
    props.insert(
        "max_attempts".into(),
        serde_json::Value::from(request.max_attempts),
    );
    props.insert("created_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "idempotency_key".into(),
        serde_json::Value::String(request.idempotency_key.clone()),
    );
    props.insert(
        "command_digest".into(),
        serde_json::Value::String(request.command_digest.to_ascii_lowercase()),
    );
    props.insert(
        "policy_digest".into(),
        serde_json::Value::String(request.policy_digest.clone()),
    );
    props.insert(
        "catalog_digest".into(),
        serde_json::Value::String(request.catalog_digest.clone()),
    );
    props.insert(
        "model_digest".into(),
        serde_json::Value::String(request.model_digest.clone()),
    );
    props.insert(
        "metadata".into(),
        serde_json::Value::Object(request.metadata.clone().into_iter().collect()),
    );
    props.insert(
        "provenance_refs".into(),
        serde_json::Value::Array(
            request
                .provenance_refs
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    props.insert(
        "context".into(),
        serde_json::to_value(&request.context).map_err(|e| e.to_string())?,
    );
    props.insert(
        "command_sequence".into(),
        serde_json::Value::from(command_sequence),
    );
    if let Some(deadline) = request.deadline_unix {
        props.insert("deadline_unix".into(), serde_json::Value::from(deadline));
    }
    Ok(props)
}

fn write_submit_work_item_dependency_edges(
    graph: &str,
    work_item_id: &str,
    context_tenant: &str,
    dependencies: &[String],
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for dependency in dependencies {
        let edge_props = rmp_serde::to_vec_named(&serde_json::json!({
            "relationship": "DEPENDS_ON",
            "tenant": context_tenant,
        }))
        .map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&edge_props);
        let ordinal = next_edge_ordinal(edges, graph, work_item_id, dependency)?;
        edges
            .insert(
                (graph, work_item_id, String::as_str(dependency), ordinal),
                sealed.as_ref(),
            )
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn insert_submit_work_item_downstream_id(
    parent: &mut serde_json::Map<String, serde_json::Value>,
    dependency: &str,
    work_item_id: &str,
) -> Result<bool, String> {
    let downstream = parent
        .entry("downstream_ids".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let ids = downstream.as_array_mut().ok_or_else(|| {
        format!("SubmitWorkItem dependency '{dependency}' has invalid downstream index")
    })?;
    if ids.iter().any(|id| id.as_str() == Some(work_item_id)) {
        Ok(false)
    } else {
        ids.push(serde_json::Value::String(work_item_id.to_string()));
        Ok(true)
    }
}

fn update_submit_work_item_downstream_row(
    graph: &str,
    work_item_id: &str,
    dependency: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut parent: serde_json::Map<String, serde_json::Value> = {
        let value = nodes
            .get((graph, dependency))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("SubmitWorkItem dependency '{dependency}' disappeared"))?;
        decode_durable(&crypto.unseal(value.value())?)?
    };
    let insert_downstream =
        insert_submit_work_item_downstream_id(&mut parent, dependency, work_item_id)?;
    if insert_downstream {
        write_work_item_props(nodes, graph, dependency, &parent, crypto)?;
    }
    Ok(())
}

fn update_submit_work_item_downstream_index(
    graph: &str,
    work_item_id: &str,
    dependency_rows: &[(String, bool)],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (dependency, pending) in dependency_rows {
        if !pending {
            continue;
        }
        update_submit_work_item_downstream_row(graph, work_item_id, dependency, nodes, crypto)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_submit_work_item_node_and_edges(
    graph: &str,
    work_item_id: &str,
    context_tenant: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    dependency_rows: &[(String, bool)],
    command_sequence: u64,
    now_s: f64,
    status: &str,
    pending_dependencies: usize,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let props = build_submit_work_item_props(
        context_tenant,
        request,
        dependencies,
        pending_dependencies,
        command_sequence,
        now_s,
        status,
    )?;
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    write_submit_work_item_dependency_edges(
        graph,
        work_item_id,
        context_tenant,
        dependencies,
        edges,
        crypto,
    )?;
    update_submit_work_item_downstream_index(graph, work_item_id, dependency_rows, nodes, crypto)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_submit_work_item_result(
    work_item_id: String,
    status: &str,
    command_sequence: u64,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    dependencies: &[String],
    dependency_rows: &[(String, bool)],
    inflight: u64,
    max_inflight: u64,
    outbox_id: &str,
) -> eg_types::native_control::SubmitWorkItemResult {
    let mut changed_work_item_ids = Vec::with_capacity(1 + dependency_rows.len());
    changed_work_item_ids.push(work_item_id.clone());
    changed_work_item_ids.extend(
        dependency_rows
            .iter()
            .filter(|(_, pending)| *pending)
            .map(|(dependency, _)| dependency.clone()),
    );
    eg_types::native_control::SubmitWorkItemResult {
        schema_version: eg_types::native_control::NativeControlSchemaVersion::V1,
        work_item_id,
        status: status.to_string(),
        created: true,
        replayed: false,
        command_sequence,
        idempotency_key: request.idempotency_key.clone(),
        dependency_count: dependencies.len() as u32,
        admitted_count: inflight.saturating_add(1),
        max_tenant_in_flight: max_inflight,
        outbox_id: outbox_id.to_string(),
        command_digest: request.command_digest.to_ascii_lowercase(),
        provenance_refs: request.provenance_refs.clone(),
        changed_work_item_ids,
    }
}

fn apply_submit_work_item_rows(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
    scope: WorkItemCommitScope<'_>,
) -> Result<eg_types::native_control::SubmitWorkItemResult, String> {
    let WorkItemCommitScope {
        crypto,
        authoritative_now_ms,
        outbox_id,
    } = scope;
    let context_tenant = request.context.tenant_id.as_str();

    let (max_inflight, dependencies, dependency_rows) =
        validate_and_resolve_submit_work_item_dependencies(graph, request, nodes, crypto)?;

    let (inflight, work_item_id, command_sequence) = admit_and_identify_submit_work_item(
        graph,
        context_tenant,
        request,
        nodes,
        command_sequences,
        max_inflight,
        crypto,
    )?;

    let now_s = authoritative_now_ms as f64 / 1000.0;
    let pending_dependencies = dependency_rows
        .iter()
        .filter(|(_, pending)| *pending)
        .count();
    let status = submit_work_item_status(pending_dependencies);

    write_submit_work_item_node_and_edges(
        graph,
        &work_item_id,
        context_tenant,
        request,
        &dependencies,
        &dependency_rows,
        command_sequence,
        now_s,
        status,
        pending_dependencies,
        nodes,
        edges,
        crypto,
    )?;

    Ok(build_submit_work_item_result(
        work_item_id,
        status,
        command_sequence,
        request,
        &dependencies,
        &dependency_rows,
        inflight,
        max_inflight,
        outbox_id,
    ))
}

fn apply_submit_work_items_rows(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemsRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    command_sequences: &mut redb::Table<&str, u64>,
    scope: WorkItemCommitScope<'_>,
) -> Result<eg_types::native_control::SubmitWorkItemsResult, String> {
    let WorkItemCommitScope {
        crypto,
        authoritative_now_ms,
        outbox_id,
    } = scope;
    use eg_types::native_control::{
        NativeControlSchemaVersion, MAX_SUBMIT_BATCH, MAX_SUBMIT_BATCH_CHANGED_IDS,
    };
    if request.schema_version != NativeControlSchemaVersion::V1
        || request.requests.is_empty()
        || request.requests.len() > MAX_SUBMIT_BATCH
    {
        return Err("SubmitWorkItems batch is outside native bounds".to_string());
    }
    if request.idempotency_key.trim().is_empty() || sanitize(&request.context.graph) != graph {
        return Err("SubmitWorkItems context/idempotency is invalid".to_string());
    }
    let changed_bound = request
        .requests
        .iter()
        .try_fold(0usize, |total, child| {
            total
                .checked_add(child.depends_on.len().saturating_add(1))
                .ok_or(())
        })
        .map_err(|_| "SubmitWorkItems result cardinality overflow".to_string())?;
    if changed_bound > MAX_SUBMIT_BATCH_CHANGED_IDS {
        return Err("SubmitWorkItems changed-row result exceeds native bound".to_string());
    }
    let mut results = Vec::with_capacity(request.requests.len());
    let mut changed = Vec::with_capacity(request.requests.len());
    for child in &request.requests {
        if child.context.tenant_id != request.context.tenant_id
            || sanitize(&child.context.graph) != graph
        {
            return Err("SubmitWorkItems child context scope mismatch".to_string());
        }
        let result = apply_submit_work_item_rows(
            graph,
            child,
            nodes,
            edges,
            command_sequences,
            WorkItemCommitScope {
                crypto,
                authoritative_now_ms,
                outbox_id,
            },
        )?;
        changed.extend(result.changed_work_item_ids.clone());
        results.push(result);
    }
    Ok(eg_types::native_control::SubmitWorkItemsResult {
        schema_version: NativeControlSchemaVersion::V1,
        results,
        replayed: false,
        outbox_id: outbox_id.to_string(),
        changed_work_item_ids: changed,
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_work_item_rows(
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    work_item_index: &redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    match method {
        Method::ClaimWorkItem { request } => {
            apply_claim_work_item_row(graph, request, nodes, native_work_items, crypto)
        }
        Method::RenewWorkItemLease {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            now_ms,
            lease_ms,
        } => apply_renew_work_item_lease_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            *now_ms,
            *lease_ms,
            nodes,
            crypto,
        ),
        Method::CasWorkItemMetadata { request } => {
            apply_cas_work_item_metadata_row(graph, request, nodes, crypto)
        }
        Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            outcome,
            result_ref,
            error_ref,
            retryable,
            now_ms,
            ..
        } => apply_commit_work_item_result_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            outcome,
            result_ref,
            error_ref,
            *retryable,
            *now_ms,
            nodes,
            holds,
            work_item_index,
            counters,
            pressure_index,
            policies,
            crypto,
        ),
        Method::CancelWorkItem {
            tenant,
            work_item_id,
            reason_ref,
            now_ms,
            ..
        } => apply_cancel_work_item_row(
            graph,
            tenant,
            work_item_id,
            reason_ref,
            *now_ms,
            nodes,
            holds,
            work_item_index,
            counters,
            pressure_index,
            policies,
            crypto,
        ),
        Method::DeferWorkItem {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            next_retry_at_ms,
            reason_ref,
            now_ms,
            ..
        } => apply_defer_work_item_row(
            graph,
            tenant,
            work_item_id,
            worker_id,
            *lease_epoch,
            *fencing_token,
            *next_retry_at_ms,
            reason_ref,
            *now_ms,
            nodes,
            crypto,
        ),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
/// One selectable row of a claim scan: `(prio_bucket, deadline, created_at_ms,
/// node_id, props)`.  The first four components are the sort key.
type ClaimCandidateRow = (
    u64,
    u64,
    u64,
    String,
    serde_json::Map<String, serde_json::Value>,
);

/// What the claim scan decided about one scanned node.
enum ClaimRowOutcome {
    /// Not a claimable row for this request; the scan moves on.
    Skip,
    /// A live lease held by someone else — counts against the tenant quota.
    InFlight,
    /// An expired lease past its attempt ceiling, retired to `dead_letter`.
    Exhausted(serde_json::Map<String, serde_json::Value>),
    /// A selectable candidate.
    Candidate(ClaimCandidateRow),
}

/// Everything one pass over the graph's nodes produced for a claim.
struct ClaimWorkItemScan {
    inflight: u32,
    candidates: Vec<ClaimCandidateRow>,
    // The redb range cursor immutably borrows the table, so expired
    // exhausted rows are collected here and written only after the
    // scan. They still commit in this same MutationBatch transaction.
    exhausted: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    changed_work_item_ids: Vec<String>,
}

/// Fence out an expired lease owner before the item participates in selection.
/// Returns `true` when the attempt ceiling was reached and the item was retired
/// to `dead_letter`; `false` when it was reclaimed back to `ready`.  Either way
/// the update is still private to the held transaction.
// `node_id`/`status` feed the statechart mirror only; a slim build without that
// feature still needs them in the signature.
#[cfg_attr(not(feature = "statechart"), allow(unused_variables))]
fn claim_reclaim_expired_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    node_id: &str,
    status: &str,
    now_s: f64,
) -> bool {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    let next_epoch = property_u64(props, "lease_epoch").saturating_add(1);
    if attempts >= max_attempts {
        props.insert(
            "status".into(),
            serde_json::Value::String("dead_letter".into()),
        );
        props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
        props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
        props.insert("lease_owner".into(), serde_json::Value::Null);
        props.insert("lease_expires_at".into(), serde_json::Value::Null);
        props.insert("completed_at".into(), serde_json::Value::from(now_s));
        props.insert("updated_at".into(), serde_json::Value::from(now_s));
        props.insert(
            "error_ref".into(),
            serde_json::Value::String("lease_exhausted".into()),
        );
        #[cfg(feature = "statechart")]
        apply_work_item_mirror(
            props,
            node_id,
            status,
            crate::work_item_statechart::EV_LEASE_EXHAUSTED,
            serde_json::json!({}),
            Some("dead_letter"),
        );
        return true;
    }
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        props,
        node_id,
        status,
        crate::work_item_statechart::EV_LEASE_RECLAIM,
        serde_json::json!({}),
        Some("ready"),
    );
    false
}

/// The selection filter, in its original order: the item must be `ready`, match
/// every supplied queue/class/fairness selector, be past its retry backoff, and
/// not have blown its deadline.
fn claim_candidate_is_excluded(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
) -> bool {
    property_string(props, "status") != "ready"
        || request
            .queue_ref
            .as_deref()
            .is_some_and(|queue| property_string(props, "queue") != queue)
        || request
            .resource_class
            .as_deref()
            .is_some_and(|resource_class| {
                property_string(props, "resource_class") != resource_class
            })
        || request
            .fairness_group
            .as_deref()
            .is_some_and(|fairness_group| {
                property_string(props, "fairness_group") != fairness_group
            })
        || property_f64(props, "next_retry_at") > now_s
        || props
            .get("deadline_unix")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|deadline| deadline < now_s)
}

/// The sort-key deadline of a selectable candidate: an absent (or already
/// filtered-out) deadline sorts last.
fn claim_candidate_deadline(props: &serde_json::Map<String, serde_json::Value>, now_s: f64) -> u64 {
    props
        .get("deadline_unix")
        .and_then(serde_json::Value::as_f64)
        .filter(|deadline| *deadline >= now_s)
        .map(|deadline| (deadline * 1000.0) as u64)
        .unwrap_or(u64::MAX)
}

/// Classify one scanned node for a claim request.  The checks run in the
/// original order, which matters: admission is tenant-wide even for an exact-id
/// delivery, so live leases contribute to the quota before the exact-id filter,
/// and that filter runs before an expired unrelated row could be reclaimed.
fn classify_claim_row(
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    node_id: &str,
    mut props: serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> ClaimRowOutcome {
    if property_string(&props, "node_type") != "WorkItem" {
        return ClaimRowOutcome::Skip;
    }
    if property_string(&props, "tenant") != request.tenant_ref.as_str() {
        return ClaimRowOutcome::Skip;
    }
    let status = property_string(&props, "status").to_string();
    if matches!(status.as_str(), "leased" | "running")
        && property_f64(&props, "lease_expires_at") > now_s
    {
        return ClaimRowOutcome::InFlight;
    }
    if request
        .work_item_id
        .as_deref()
        .is_some_and(|selected| selected != node_id)
    {
        return ClaimRowOutcome::Skip;
    }
    if matches!(status.as_str(), "leased" | "running")
        && claim_reclaim_expired_lease(&mut props, node_id, &status, now_s)
    {
        return ClaimRowOutcome::Exhausted(props);
    }
    if claim_candidate_is_excluded(&props, request, now_s) {
        return ClaimRowOutcome::Skip;
    }
    let deadline = claim_candidate_deadline(&props, now_s);
    ClaimRowOutcome::Candidate((
        property_u64(&props, "prio_bucket"),
        deadline,
        (property_f64(&props, "created_at") * 1000.0) as u64,
        node_id.to_string(),
        props,
    ))
}

/// One bounded pass over this graph's node rows, tallying the tenant's in-flight
/// leases, the expired rows to retire, and the claimable candidates.
fn scan_claim_work_item_candidates(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ClaimWorkItemScan, String> {
    let mut scan = ClaimWorkItemScan {
        inflight: 0,
        candidates: Vec::new(),
        exhausted: Vec::new(),
        changed_work_item_ids: Vec::new(),
    };
    for row in nodes.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, node_id) = key.value();
        if row_graph != graph {
            break;
        }
        let bytes = crypto.unseal(value.value())?;
        let Ok(props) = decode_durable::<serde_json::Map<String, serde_json::Value>>(&bytes) else {
            continue;
        };
        match classify_claim_row(request, node_id, props, now_s) {
            ClaimRowOutcome::Skip => {}
            ClaimRowOutcome::InFlight => scan.inflight = scan.inflight.saturating_add(1),
            ClaimRowOutcome::Exhausted(props) => {
                let node_id = node_id.to_string();
                scan.changed_work_item_ids.push(node_id.clone());
                scan.exhausted.push((node_id, props));
            }
            ClaimRowOutcome::Candidate(candidate) => scan.candidates.push(candidate),
        }
    }
    Ok(scan)
}

/// The `claimed: false` result shape, shared by the tenant-quota and empty-queue
/// refusals.
fn claim_not_claimed_payload(
    reason: ClaimWorkItemResultReason,
    inflight: u32,
    changed_work_item_ids: Vec<String>,
) -> crate::protocol::ResultPayload {
    crate::protocol::ResultPayload::raw(&ClaimWorkItemResult {
        schema_version: ClaimWorkItemResultSchemaVersion::V1,
        claimed: false,
        reason,
        work_item_id: None,
        kind: None,
        payload_ref: None,
        lease_holder_ref: None,
        lease_epoch: None,
        fencing_token: None,
        lease_expires_at_ms: None,
        attempt: None,
        max_attempts: None,
        tenant_in_flight: Some(u64::from(inflight)),
        changed_work_item_ids,
    })
}

/// Stamp the granted lease onto the selected candidate and report its
/// `(lease_epoch, attempt)`.
///
/// The native claim authority owns the per-attempt WorkItem fence.  Ready
/// submissions cannot supply one through generic graph writes; deriving it from
/// the authoritative lease epoch keeps the Raft transition deterministic while
/// ensuring every capability-bound live lease has a non-empty fence that changes
/// on reclaim.
fn claim_grant_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    now_s: f64,
    lease_until_s: f64,
) -> (u64, u64) {
    let epoch = property_u64(props, "lease_epoch").saturating_add(1);
    let attempt = property_u64(props, "attempt").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("leased".into()));
    props.insert(
        "lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_epoch".into(), serde_json::Value::from(epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(epoch));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(lease_until_s),
    );
    props.insert(
        "work_item_fence".into(),
        serde_json::Value::String(format!("lease-fence-v1:{epoch}")),
    );
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("attempt".into(), serde_json::Value::from(attempt));
    (epoch, attempt)
}

fn apply_claim_work_item_row(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let tenant = &request.tenant_ref;
    let worker_id = &request.worker_ref;
    let now_ms = request.now_ms;
    let lease_ms = request.lease_ms;
    let max_tenant_in_flight = request.max_tenant_in_flight;
    if tenant.trim().is_empty()
        || worker_id.trim().is_empty()
        || lease_ms == 0
        || !(1..=4096).contains(&max_tenant_in_flight)
    {
        return Err("ClaimWorkItem request violates the current protocol contract".into());
    }
    let now_s = now_ms as f64 / 1000.0;
    let lease_until_s = now_s + (lease_ms as f64 / 1000.0);
    let tenant_in_flight_limit = max_tenant_in_flight as u32;
    let ClaimWorkItemScan {
        inflight,
        mut candidates,
        exhausted,
        mut changed_work_item_ids,
    } = scan_claim_work_item_candidates(graph, request, now_s, nodes, crypto)?;
    for (node_id, props) in exhausted {
        write_work_item_props(nodes, graph, &node_id, &props, crypto)?;
    }
    if inflight >= tenant_in_flight_limit {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::TenantQuota,
            inflight,
            changed_work_item_ids,
        )));
    }
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, &left.3).cmp(&(&right.0, &right.1, &right.2, &right.3))
    });
    let Some((_, _, _, node_id, mut props)) = candidates.into_iter().next() else {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::Empty,
            inflight,
            changed_work_item_ids,
        )));
    };
    let (epoch, attempt) = claim_grant_lease(&mut props, worker_id, now_s, lease_until_s);
    let kind = property_string(&props, "kind").to_string();
    let payload_ref = property_string(&props, "payload_ref").to_string();
    let max_attempts = property_u64(&props, "max_attempts").max(1);
    // Phase-1 statechart mirror: the picked candidate was `ready` (it passed the
    // `status != "ready"` filter above); selection already happened outside the
    // chart, so its `ready --claim--> leased` edge is unconditional.
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        &node_id,
        "ready",
        crate::work_item_statechart::EV_CLAIM,
        serde_json::json!({}),
        Some("leased"),
    );
    write_work_item_props(nodes, graph, &node_id, &props, crypto)?;
    work_item_capability::record_native_claim_in_wtx(
        native_work_items,
        graph,
        &node_id,
        &props,
        now_ms,
        crypto,
    )?;
    Ok(Some(crate::protocol::ResultPayload::raw(
        &ClaimWorkItemResult {
            schema_version: ClaimWorkItemResultSchemaVersion::V1,
            claimed: true,
            reason: ClaimWorkItemResultReason::Claimed,
            work_item_id: Some(node_id.clone()),
            kind: (!kind.is_empty()).then_some(kind),
            payload_ref: (!payload_ref.is_empty()).then_some(payload_ref),
            lease_holder_ref: Some(worker_id.clone()),
            lease_epoch: Some(epoch),
            fencing_token: Some(epoch),
            lease_expires_at_ms: Some(now_ms.saturating_add(lease_ms)),
            attempt: Some(attempt),
            max_attempts: Some(max_attempts),
            tenant_in_flight: Some(u64::from(inflight.saturating_add(1))),
            changed_work_item_ids: {
                changed_work_item_ids.push(node_id);
                changed_work_item_ids
            },
        },
    )))
}

#[allow(clippy::too_many_arguments)]
fn apply_renew_work_item_lease_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
    lease_ms: u64,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    if worker_id.trim().is_empty() || lease_ms == 0 {
        return Err("RenewWorkItemLease requires worker_id and non-zero lease_ms".into());
    }
    let current = nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    // Every WorkItem result — including one that changed no row — MUST carry
    // `changed_work_item_ids`. The commit has already advanced the authoritative
    // graph version by the time `commit_work_item` reads this field, so a shape
    // missing it strands the serving projection one version behind and makes the
    // graph permanently read-only (INCIDENT-kg-readonly-2026-07-31).
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "renewed": false,
                "reason": "missing",
                "changed_work_item_ids": [],
            }),
        )));
    };
    let mut props = decode(&bytes)?;
    let valid = property_string(&props, "tenant") == tenant
        && property_string(&props, "lease_owner") == worker_id
        && matches!(property_string(&props, "status"), "leased" | "running")
        && property_u64(&props, "lease_epoch") == lease_epoch
        && property_u64(&props, "fencing_token") == fencing_token
        && property_f64(&props, "lease_expires_at") >= now_ms as f64 / 1000.0;
    if !valid {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "renewed": false,
                "reason": "fenced",
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: the lease was validated (fence_valid), so leased|running →
    // running. Capture the pre-status before the authority overwrites it.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let now_s = now_ms as f64 / 1000.0;
    props.insert("status".into(), serde_json::Value::String("running".into()));
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(now_s + lease_ms as f64 / 1000.0),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_RENEW,
        serde_json::json!({ "fence_valid": true }),
        Some("running"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "renewed": true,
            "work_item_id": work_item_id,
            "lease_epoch": lease_epoch,
            "fencing_token": fencing_token,
            "lease_expires_at_ms": (now_ms).saturating_add(lease_ms),
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}

#[allow(clippy::too_many_arguments)]
/// Shape validation for a `CasWorkItemMetadata` request, in the original order:
/// exactly one settable field, a non-empty expected status set, and non-blank
/// tenant/work-item identifiers.
fn validate_cas_work_item_metadata_request(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
) -> Result<(), String> {
    let field_pairs_set = [
        request.set_checkpoint_id.is_some(),
        request.set_metadata_msgpack.is_some(),
        request.set_prio_bucket.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if field_pairs_set != 1 {
        return Err(
            "CasWorkItemMetadata requires exactly one of set_checkpoint_id / \
             set_metadata_msgpack / set_prio_bucket"
                .to_string(),
        );
    }
    if request.expected_status.is_empty() {
        return Err("CasWorkItemMetadata requires a non-empty expected_status".into());
    }
    if request.tenant_ref.trim().is_empty() || request.work_item_id.trim().is_empty() {
        return Err("CasWorkItemMetadata requires tenant_ref and work_item_id".into());
    }
    Ok(())
}

/// The status / tenant / lease-fence preconditions of a metadata CAS.  All three
/// are evaluated (as before) and the conjunction decides; a `false` here is a
/// `Conflict` outcome, not an error.
fn cas_work_item_metadata_preconditions_ok(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let tenant = &request.tenant_ref;
    let status_ok = request
        .expected_status
        .iter()
        .any(|status| status == property_string(props, "status"));
    let tenant_ok = property_string(props, "tenant") == tenant;
    let lease_ok = match &request.expected_lease {
        Some(fence) => {
            property_string(props, "lease_owner") == fence.worker_ref
                && property_u64(props, "lease_epoch") == fence.lease_epoch
                && property_u64(props, "fencing_token") == fence.fencing_token
        }
        None => true,
    };
    status_ok && tenant_ok && lease_ok
}

/// Apply the one settable field of a metadata CAS to `props`, after checking its
/// own expected pre-image.  Returns `Ok(false)` when that pre-image does not
/// match -- the caller turns that into a `Conflict` outcome, exactly as the
/// inline branches did.  `props` is only mutated on the matching path.
fn apply_cas_work_item_metadata_field(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    if let Some(set_checkpoint_id) = &request.set_checkpoint_id {
        let current_checkpoint_id = props
            .get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if current_checkpoint_id != request.expected_checkpoint_id {
            return Ok(false);
        }
        props.insert(
            "checkpoint_id".into(),
            serde_json::Value::String(set_checkpoint_id.clone()),
        );
    } else if let Some(set_metadata_bytes) = &request.set_metadata_msgpack {
        let current_metadata = props
            .get("metadata")
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let expected_metadata = match &request.expected_metadata_msgpack {
            Some(bytes) => decode_durable::<serde_json::Value>(bytes)
                .map_err(|_| "invalid expected_metadata_msgpack".to_string())?,
            None => serde_json::Value::Object(Default::default()),
        };
        if current_metadata != expected_metadata {
            return Ok(false);
        }
        let set_metadata = decode_durable::<serde_json::Value>(set_metadata_bytes)
            .map_err(|_| "invalid set_metadata_msgpack".to_string())?;
        props.insert("metadata".into(), set_metadata);
    } else if let Some(set_prio_bucket) = request.set_prio_bucket {
        let expected_prio_bucket = request.expected_prio_bucket.unwrap_or(0);
        let current_prio_bucket = props
            .get("prio_bucket")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if current_prio_bucket != expected_prio_bucket {
            return Ok(false);
        }
        props.insert(
            "prio_bucket".into(),
            serde_json::Value::from(set_prio_bucket),
        );
    }
    Ok(true)
}

fn apply_cas_work_item_metadata_row(
    graph: &str,
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use crate::epistemic_operations_ext::{
        CasWorkItemMetadataOutcome, CasWorkItemMetadataResult,
        CasWorkItemMetadataResultSchemaVersion,
    };

    let work_item_id = &request.work_item_id;
    let now_ms = request.now_ms;

    validate_cas_work_item_metadata_request(request)?;

    let respond = |outcome: CasWorkItemMetadataOutcome, changed: Vec<String>| {
        Ok(Some(crate::protocol::ResultPayload::raw(
            &CasWorkItemMetadataResult {
                schema_version: CasWorkItemMetadataResultSchemaVersion::V1,
                outcome,
                work_item_id: work_item_id.clone(),
                changed_work_item_ids: changed,
            },
        )))
    };

    let current = nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return respond(CasWorkItemMetadataOutcome::NotFound, vec![]);
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;

    if !cas_work_item_metadata_preconditions_ok(request, &props) {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    if !apply_cas_work_item_metadata_field(request, &mut props)? {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    let now_s = now_ms as f64 / 1000.0;
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    respond(
        CasWorkItemMetadataOutcome::Applied,
        vec![work_item_id.clone()],
    )
}

/// The lease fence a `CommitWorkItemResult` must satisfy: the caller owns the
/// lease, the item is live, and the lease has not expired.
fn commit_work_item_lease_is_valid(
    props: &serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> bool {
    property_string(props, "lease_owner") == worker_id
        && matches!(property_string(props, "status"), "leased" | "running")
        && property_u64(props, "lease_epoch") == lease_epoch
        && property_u64(props, "fencing_token") == fencing_token
        && property_f64(props, "lease_expires_at") >= now_ms as f64 / 1000.0
}

/// The three short-circuit responses of a commit, in their original order:
/// a tenant mismatch reads as `missing`, an already-terminal item as `noop`,
/// and a failed lease fence as `fenced`.  `Ok(None)` means the commit proceeds.
fn commit_work_item_result_precheck(
    props: &serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    tenant: &str,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> Option<crate::protocol::ResultPayload> {
    if property_string(props, "tenant") != tenant {
        return Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        ));
    }
    if matches!(
        property_string(props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Some(crate::protocol::ResultPayload::Json(serde_json::json!({
            "status": "noop",
            "work_item_id": work_item_id,
            "changed_work_item_ids": [],
        })));
    }
    if !commit_work_item_lease_is_valid(props, worker_id, lease_epoch, fencing_token, now_ms) {
        return Some(crate::protocol::ResultPayload::Json(serde_json::json!({
            "status": "fenced",
            "work_item_id": work_item_id,
            "changed_work_item_ids": [],
        })));
    }
    None
}

/// Write the committed status into `props` and report it.  A retryable failure
/// below the attempt ceiling reschedules (`ready` + backoff + bumped fence) and
/// reports `retry_scheduled`; otherwise the item goes terminal (`dead_letter`
/// for an exhausted retryable failure, else the outcome verb itself).
fn commit_work_item_apply_status<'o>(
    props: &mut serde_json::Map<String, serde_json::Value>,
    outcome: &'o str,
    retryable: bool,
    lease_epoch: u64,
    fencing_token: u64,
    now_s: f64,
) -> &'o str {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    if outcome == "failed" && retryable && attempts < max_attempts {
        let backoff = property_f64(props, "backoff_base_s").max(1.0)
            * 2f64.powi(attempts.saturating_sub(1).min(31) as i32);
        props.insert("status".into(), serde_json::Value::String("ready".into()));
        props.insert(
            "next_retry_at".into(),
            serde_json::Value::from(now_s + backoff),
        );
        props.insert(
            "lease_epoch".into(),
            serde_json::Value::from((lease_epoch).saturating_add(1)),
        );
        props.insert(
            "fencing_token".into(),
            serde_json::Value::from((fencing_token).saturating_add(1)),
        );
        return "retry_scheduled";
    }
    let terminal = if outcome == "failed" && retryable {
        "dead_letter"
    } else {
        outcome
    };
    props.insert("status".into(), serde_json::Value::String(terminal.into()));
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    terminal
}

/// Record the commit's lease/result bookkeeping on the item.
fn commit_work_item_record_result_refs(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    result_ref: &Option<String>,
    error_ref: &Option<String>,
    now_s: f64,
) {
    props.insert(
        "result_ref".into(),
        result_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert(
        "error_ref".into(),
        error_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
}

/// Decrement each downstream child's dependency count after a successful
/// commit, releasing a child to `ready` once its last dependency clears.
/// Children that no longer exist are skipped, as before.
fn commit_work_item_release_downstream(
    graph: &str,
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
    changed: &mut Vec<String>,
) -> Result<(), String> {
    let downstream = props
        .get("downstream_ids")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for child in downstream.iter().filter_map(serde_json::Value::as_str) {
        let child_bytes = nodes
            .get((graph, child))
            .map_err(|e| e.to_string())?
            .map(|value| crypto.unseal(value.value()))
            .transpose()?;
        let Some(child_bytes) = child_bytes else {
            continue;
        };
        let mut child_props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&child_bytes)?;
        let count = property_u64(&child_props, "dep_count").saturating_sub(1);
        child_props.insert("dep_count".into(), serde_json::Value::from(count));
        if count == 0 && property_string(&child_props, "status") == "submitted" {
            child_props.insert("status".into(), serde_json::Value::String("ready".into()));
        }
        child_props.insert("updated_at".into(), serde_json::Value::from(now_s));
        write_work_item_props(nodes, graph, child, &child_props, crypto)?;
        changed.push(child.to_string());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_commit_work_item_result_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    outcome: &String,
    result_ref: &Option<String>,
    error_ref: &Option<String>,
    retryable: bool,
    now_ms: u64,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    work_item_index: &redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let current = nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let pre_props = props.clone();
    if let Some(payload) = commit_work_item_result_precheck(
        &props,
        work_item_id.as_str(),
        tenant.as_str(),
        worker_id.as_str(),
        lease_epoch,
        fencing_token,
        now_ms,
    ) {
        return Ok(Some(payload));
    }
    if !matches!(outcome.as_str(), "succeeded" | "failed" | "cancelled") {
        return Err("CommitWorkItemResult outcome must be succeeded, failed, or cancelled".into());
    }
    let now_s = now_ms as f64 / 1000.0;
    // Phase-1 mirror inputs: pre-status (leased|running, validated above) + the
    // DLQ-threshold POLICY boolean (`retryable && attempt < max_attempts`) the
    // chart reads as a pre-computed guard input — see `work_item_statechart`.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    #[cfg(feature = "statechart")]
    let commit_retry_eligible =
        property_u64(&props, "attempt") < property_u64(&props, "max_attempts").max(1);
    let committed_status = commit_work_item_apply_status(
        &mut props,
        outcome.as_str(),
        retryable,
        lease_epoch,
        fencing_token,
        now_s,
    );
    commit_work_item_record_result_refs(
        &mut props,
        worker_id.as_str(),
        result_ref,
        error_ref,
        now_s,
    );
    development_lane::transition_work_item_terminal_hold(
        graph,
        &pre_props,
        work_item_id,
        committed_status,
        false,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    )?;
    // Phase-1 mirror: the commit outcome maps to the chart's commit_* event; the
    // authoritative next state is whatever the handler persisted (ready on a
    // scheduled retry, else the terminal). The chart must independently agree.
    #[cfg(feature = "statechart")]
    {
        let event = match outcome.as_str() {
            "succeeded" => crate::work_item_statechart::EV_COMMIT_SUCCEEDED,
            "cancelled" => crate::work_item_statechart::EV_COMMIT_CANCELLED,
            _ => crate::work_item_statechart::EV_COMMIT_FAILED,
        };
        let mirror_payload = serde_json::json!({
            "fence_valid": true,
            "retryable": retryable,
            "retry_eligible": commit_retry_eligible,
        });
        let authoritative_next = property_string(&props, "status").to_string();
        apply_work_item_mirror(
            &mut props,
            work_item_id,
            &pre_status,
            event,
            mirror_payload,
            Some(&authoritative_next),
        );
    }
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;

    let mut changed = vec![work_item_id.clone()];
    if committed_status == "succeeded" {
        commit_work_item_release_downstream(graph, &props, now_s, nodes, crypto, &mut changed)?;
    }
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": committed_status,
            "work_item_id": work_item_id,
            "lease_epoch": lease_epoch,
            "fencing_token": fencing_token,
            "changed_work_item_ids": changed,
        }),
    )))
}

#[allow(clippy::too_many_arguments)]
fn apply_cancel_work_item_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    reason_ref: &Option<String>,
    now_ms: u64,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    work_item_index: &redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    let current = nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props = decode(&bytes)?;
    let pre_props = props.clone();
    if property_string(&props, "tenant") != tenant {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    }
    if matches!(
        property_string(&props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "noop",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    let now_s = now_ms as f64 / 1000.0;
    if matches!(property_string(&props, "status"), "leased" | "running")
        && property_f64(&props, "lease_expires_at") >= now_s
    {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "in_flight",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    if !matches!(
        property_string(&props, "status"),
        "submitted" | "ready" | "leased" | "running"
    ) {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "not_cancellable",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: capture the pre-status (a cancellable non-terminal state)
    // before the authority marks it cancelled.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let lease_owner = property_string(&props, "lease_owner");
    let last_lease_owner = if lease_owner.is_empty() {
        property_string(&props, "last_lease_owner")
    } else {
        lease_owner
    }
    .to_string();
    let next_epoch = property_u64(&props, "lease_epoch")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem lease epoch overflow".to_string())?;
    let next_fencing_token = property_u64(&props, "fencing_token")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem fencing token overflow".to_string())?;
    props.insert(
        "status".into(),
        serde_json::Value::String("cancelled".into()),
    );
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(last_lease_owner),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert(
        "fencing_token".into(),
        serde_json::Value::from(next_fencing_token),
    );
    props.insert(
        "cancel_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    development_lane::transition_work_item_terminal_hold(
        graph,
        &pre_props,
        work_item_id,
        "cancelled",
        true,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    )?;
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_CANCEL,
        serde_json::json!({ "cancellable": true }),
        Some("cancelled"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": "cancelled",
            "work_item_id": work_item_id,
            "lease_epoch": next_epoch,
            "fencing_token": next_fencing_token,
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}

#[allow(clippy::too_many_arguments)]
fn apply_defer_work_item_row(
    graph: &str,
    tenant: &String,
    work_item_id: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    next_retry_at_ms: u64,
    reason_ref: &Option<String>,
    now_ms: u64,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    if next_retry_at_ms < now_ms {
        return Err("DeferWorkItem next_retry_at_ms must not precede now_ms".into());
    }
    let current = nodes
        .get((graph, work_item_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({"status": "missing", "changed_work_item_ids": []}),
        )));
    };
    let mut props = decode(&bytes)?;
    let now_s = now_ms as f64 / 1000.0;
    let valid = property_string(&props, "tenant") == tenant
        && property_string(&props, "lease_owner") == worker_id
        && matches!(property_string(&props, "status"), "leased" | "running")
        && property_u64(&props, "lease_epoch") == lease_epoch
        && property_u64(&props, "fencing_token") == fencing_token
        && property_f64(&props, "lease_expires_at") >= now_s;
    if !valid {
        return Ok(Some(crate::protocol::ResultPayload::Json(
            serde_json::json!({
                "status": "fenced",
                "work_item_id": work_item_id,
                "changed_work_item_ids": [],
            }),
        )));
    }
    // Phase-1 mirror: capture the leased|running pre-status before the fenced
    // lease is released back to `ready`.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let next_epoch = (lease_epoch).saturating_add(1);
    let attempts = property_u64(&props, "attempt").saturating_sub(1);
    let defer_count = property_u64(&props, "defer_count").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert(
        "next_retry_at".into(),
        serde_json::Value::from(next_retry_at_ms as f64 / 1000.0),
    );
    props.insert("attempt".into(), serde_json::Value::from(attempts));
    props.insert("defer_count".into(), serde_json::Value::from(defer_count));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "defer_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_DEFER,
        serde_json::json!({ "fence_valid": true }),
        Some("ready"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    Ok(Some(crate::protocol::ResultPayload::Json(
        serde_json::json!({
            "status": "deferred",
            "work_item_id": work_item_id,
            "lease_epoch": next_epoch,
            "fencing_token": next_epoch,
            "next_retry_at_ms": next_retry_at_ms,
            "attempt": attempts,
            "defer_count": defer_count,
            "changed_work_item_ids": [work_item_id],
        }),
    )))
}

/// Read one durable batch record from a snapshot.  Used by retry/recovery and by
/// tests that close/reopen the database to model process death.
pub(crate) fn read_mutation_batch(
    db: &Database,
    batch_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<MutationBatchRecord>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(MUTATION_BATCHES) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let record = table
        .get(batch_id)
        .map_err(|e| e.to_string())?
        .map(|v| {
            let bytes = crypto.unseal(v.value())?;
            decode_mutation_batch_record(&bytes)
        })
        .transpose()?;
    Ok(record)
}

pub(crate) fn read_change_envelope(
    db: &Database,
    graph_fname: &str,
    envelope_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeEnvelopeRecord>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(CHANGE_ENVELOPES) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let record = table
        .get((graph_fname, envelope_id))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable(&bytes)
        })
        .transpose()?;
    Ok(record)
}

pub(crate) fn read_content_version(
    db: &Database,
    tenant: &str,
    graph_fname: &str,
    object_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ContentVersion>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(CONTENT_VERSIONS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let version = table
        .get((graph_fname, tenant, object_id))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable(&bytes)
        })
        .transpose()?;
    Ok(version)
}

pub(crate) fn read_change_cursor(
    db: &Database,
    tenant: &str,
    graph_fname: &str,
    source: &str,
    partition: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeCursor>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(CHANGE_CURSORS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let cursor = table
        .get((graph_fname, tenant, source, partition))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable(&bytes)
        })
        .transpose()?;
    Ok(cursor)
}

/// Read all immutable outbox rows for a batch in ordinal order.
pub(crate) fn read_mutation_outbox(
    db: &Database,
    batch_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(MUTATION_OUTBOX) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e.to_string()),
    };
    let mut rows = Vec::new();
    for row in table
        .range((batch_id, 0u32)..=(batch_id, u32::MAX))
        .map_err(|e| e.to_string())?
    {
        let (_, value) = row.map_err(|e| e.to_string())?;
        let bytes = crypto.unseal(value.value())?;
        rows.push(decode_mutation_outbox_record(&bytes)?);
    }
    Ok(rows)
}

/// Claim pending transactional-outbox rows for one durable consumer identity.
///
/// Lease state is keyed by `(batch, ordinal, consumer)`, so independent
/// projections each observe every event while concurrent workers for the same
/// consumer are fenced by a monotonically increasing lease epoch. Selection and
/// lease installation share one immediate redb transaction; queue pressure can
/// therefore delay a claim but can never lose one.
pub(crate) fn claim_mutation_outbox(
    db: &Database,
    graph_fname: &str,
    consumer: &str,
    now_ms: u64,
    lease_ms: u64,
    limit: usize,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<MutationOutboxLease>, String> {
    if consumer.trim().is_empty() || lease_ms == 0 || limit == 0 {
        return Err(
            "outbox claim requires consumer, non-zero lease_ms, and non-zero limit".to_string(),
        );
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;

    let mut candidates = {
        let outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
        let mut rows = Vec::new();
        for row in outbox.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let bytes = crypto.unseal(value.value())?;
            let record = decode_mutation_outbox_record(&bytes)?;
            if sanitize(&record.graph) == graph_fname {
                rows.push(record);
            }
        }
        rows
    };
    candidates.sort_by(|left, right| {
        (
            left.source_graph_version,
            left.created_at_ms,
            left.batch_id.as_str(),
            left.ordinal,
        )
            .cmp(&(
                right.source_graph_version,
                right.created_at_ms,
                right.batch_id.as_str(),
                right.ordinal,
            ))
    });

    let mut claimed = Vec::new();
    {
        let mut deliveries = wtx
            .open_table(MUTATION_OUTBOX_DELIVERY)
            .map_err(|e| e.to_string())?;
        for record in candidates {
            if claimed.len() >= limit {
                break;
            }
            let key = (record.batch_id.as_str(), record.ordinal, consumer);
            let current = deliveries
                .get(key)
                .map_err(|e| e.to_string())?
                .map(|value| {
                    let bytes = crypto.unseal(value.value())?;
                    decode_durable::<DurableOutboxDelivery>(&bytes)
                })
                .transpose()?
                .unwrap_or_default();
            if current.delivered_at_ms.is_some() || current.lease_until_ms > now_ms {
                continue;
            }
            let delivery = DurableOutboxDelivery {
                consumer: consumer.to_string(),
                lease_epoch: current.lease_epoch.saturating_add(1),
                lease_until_ms: now_ms.saturating_add(lease_ms),
                attempt: current.attempt.saturating_add(1),
                delivered_at_ms: None,
            };
            let bytes = rmp_serde::to_vec_named(&delivery).map_err(|e| e.to_string())?;
            let sealed = crypto.seal(&bytes);
            deliveries
                .insert(key, sealed.as_ref())
                .map_err(|e| e.to_string())?;
            claimed.push(MutationOutboxLease {
                record,
                consumer: delivery.consumer,
                lease_epoch: delivery.lease_epoch,
                lease_until_ms: delivery.lease_until_ms,
                attempt: delivery.attempt,
            });
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(claimed)
}

/// Acknowledge the exact durable outbox lease and advance a projection watermark
/// in the same transaction. A superseded or expired worker fails closed; a retry
/// of an already-delivered lease is idempotent only when its cursor is already at
/// that exact event.
fn validate_ack_outbox_request(
    graph_fname: &str,
    lease: &MutationOutboxLease,
    projection: &str,
) -> Result<(), String> {
    if projection.trim().is_empty() || lease.consumer.trim().is_empty() {
        return Err("outbox ack requires projection and consumer".to_string());
    }
    lease.record.validate()?;
    if sanitize(&lease.record.graph) != graph_fname {
        return Err("outbox ack graph route does not match the leased record".to_string());
    }
    Ok(())
}

// Bind the acknowledgement to the immutable outbox row, not merely to
// caller-supplied batch/ordinal strings.
fn verify_outbox_lease_matches_durable_event(
    wtx: &redb::WriteTransaction,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    let row = outbox
        .get((lease.record.batch_id.as_str(), lease.record.ordinal))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "outbox lease references a missing event".to_string())?;
    let bytes = crypto.unseal(row.value())?;
    let stored = decode_mutation_outbox_record(&bytes)?;
    if stored != lease.record {
        return Err("outbox lease record does not match durable event".to_string());
    }
    Ok(())
}

fn ack_outbox_derived_graph_version(batch: &MutationBatch) -> Result<Option<u64>, String> {
    if let Some(state) = batch.authoritative_state.as_ref() {
        Ok(Some(state.target_graph_version))
    } else if let Some(version) = batch.expected_graph_version {
        Ok(Some(version.checked_add(1).ok_or_else(|| {
            "mutation graph version overflow".to_string()
        })?))
    } else {
        Ok(None)
    }
}

fn ack_source_batch_is_bound(batch: &MutationBatchRecord, lease: &MutationOutboxLease) -> bool {
    batch.status == MutationBatchStatus::Committed
        && batch.batch.batch_id == lease.record.batch_id
        && batch.batch.tenant == lease.record.tenant
        && batch.batch.graph == lease.record.graph
}

fn load_ack_source_batch(
    wtx: &redb::WriteTransaction,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<MutationBatchRecord, String> {
    let batches = wtx
        .open_table(MUTATION_BATCHES)
        .map_err(|e| e.to_string())?;
    let row = batches
        .get(lease.record.batch_id.as_str())
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "outbox event has no committed mutation batch".to_string())?;
    let bytes = crypto.unseal(row.value())?;
    let batch = decode_mutation_batch_record(&bytes)?;
    if !ack_source_batch_is_bound(&batch, lease) {
        return Err("outbox event is not bound to its committed mutation batch".to_string());
    }
    Ok(batch)
}

fn derive_ack_source_graph_version(
    batch: &MutationBatchRecord,
    lease: &MutationOutboxLease,
) -> Result<u64, String> {
    if lease.record.version_scope != MutationVersionScope::Graph {
        return Err("graph projection cannot acknowledge a non-graph outbox event".to_string());
    }
    let derived = ack_outbox_derived_graph_version(&batch.batch)?;
    if let Some(derived) = derived {
        if lease.record.source_graph_version != derived {
            return Err("outbox event graph version does not match its batch".to_string());
        }
    } else if !batch.batch.operations.iter().all(|operation| {
        matches!(
            &operation.method,
            Method::CreateGraph { .. } | Method::DeleteGraph { .. }
        )
    }) {
        return Err("committed graph outbox event has no authoritative version source".to_string());
    }
    Ok(lease.record.source_graph_version)
}

fn resolve_ack_source_graph_version(
    wtx: &redb::WriteTransaction,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    let batch = load_ack_source_batch(wtx, lease, crypto)?;
    derive_ack_source_graph_version(&batch, lease)
}

fn read_ack_current_cursor(
    wtx: &redb::WriteTransaction,
    projection: &str,
    lease: &MutationOutboxLease,
    graph_fname: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<MutationProjectionCursor>, String> {
    let cursors = wtx
        .open_table(MUTATION_PROJECTION_CURSOR)
        .map_err(|e| e.to_string())?;
    let value = cursors
        .get((projection, lease.record.tenant.as_str(), graph_fname))
        .map_err(|e| e.to_string())?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode_mutation_projection_cursor(&bytes)
        })
        .transpose()?;
    Ok(value)
}

fn read_ack_outbox_delivery(
    wtx: &redb::WriteTransaction,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<DurableOutboxDelivery, String> {
    let deliveries = wtx
        .open_table(MUTATION_OUTBOX_DELIVERY)
        .map_err(|e| e.to_string())?;
    let row = deliveries
        .get((
            lease.record.batch_id.as_str(),
            lease.record.ordinal,
            lease.consumer.as_str(),
        ))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "outbox lease is not durably claimed".to_string())?;
    let bytes = crypto.unseal(row.value())?;
    decode_durable::<DurableOutboxDelivery>(&bytes)
}

fn find_earlier_outbox_keys(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, u32)>, String> {
    let proposed_order = (
        lease.record.source_graph_version,
        lease.record.created_at_ms,
        lease.record.batch_id.as_str(),
        lease.record.ordinal,
    );
    let outbox = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
    let mut keys = Vec::new();
    for row in outbox.iter().map_err(|e| e.to_string())? {
        let (_, value) = row.map_err(|e| e.to_string())?;
        let bytes = crypto.unseal(value.value())?;
        let record = decode_mutation_outbox_record(&bytes)?;
        let order = (
            record.source_graph_version,
            record.created_at_ms,
            record.batch_id.as_str(),
            record.ordinal,
        );
        if sanitize(&record.graph) == graph_fname && order < proposed_order {
            keys.push((record.batch_id, record.ordinal));
        }
    }
    Ok(keys)
}

fn ensure_earlier_outbox_events_delivered(
    wtx: &redb::WriteTransaction,
    earlier: &[(String, u32)],
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let deliveries = wtx
        .open_table(MUTATION_OUTBOX_DELIVERY)
        .map_err(|e| e.to_string())?;
    for (batch_id, ordinal) in earlier {
        let prior = deliveries
            .get((batch_id.as_str(), *ordinal, lease.consumer.as_str()))
            .map_err(|e| e.to_string())?
            .map(|value| {
                let bytes = crypto.unseal(value.value())?;
                decode_durable::<DurableOutboxDelivery>(&bytes)
            })
            .transpose()?;
        if prior.and_then(|state| state.delivered_at_ms).is_none() {
            return Err(format!(
                "OUTBOX_ORDER_GAP: event '{}:{}' is not yet delivered",
                batch_id, ordinal
            ));
        }
    }
    Ok(())
}

fn already_delivered_cursor(
    lease: &MutationOutboxLease,
    current_cursor: &Option<MutationProjectionCursor>,
    source_graph_version: u64,
) -> Option<MutationProjectionCursor> {
    let cursor = current_cursor.as_ref()?;
    if cursor.batch_id == lease.record.batch_id
        && cursor.outbox_ordinal == lease.record.ordinal
        && cursor.version_scope == lease.record.version_scope
        && cursor.source_graph_version == source_graph_version
    {
        Some(cursor.clone())
    } else {
        None
    }
}

fn check_ack_delivery_freshness(
    delivery: &DurableOutboxDelivery,
    lease: &MutationOutboxLease,
    now_ms: u64,
    current_cursor: &Option<MutationProjectionCursor>,
    source_graph_version: u64,
) -> Result<Option<MutationProjectionCursor>, String> {
    if delivery.consumer != lease.consumer || delivery.lease_epoch != lease.lease_epoch {
        return Err("STALE_OUTBOX_LEASE: consumer or epoch was superseded".to_string());
    }
    if delivery.delivered_at_ms.is_some() {
        return match already_delivered_cursor(lease, current_cursor, source_graph_version) {
            Some(cursor) => Ok(Some(cursor)),
            None => Err("STALE_OUTBOX_LEASE: event was already delivered".to_string()),
        };
    }
    if delivery.lease_until_ms < now_ms || lease.lease_until_ms != delivery.lease_until_ms {
        return Err("STALE_OUTBOX_LEASE: lease expired or was replaced".to_string());
    }
    Ok(None)
}

fn check_ack_cursor_watermark(
    current_cursor: &Option<MutationProjectionCursor>,
    lease: &MutationOutboxLease,
    source_graph_version: u64,
) -> Result<(), String> {
    if let Some(current) = current_cursor {
        if current.version_scope != lease.record.version_scope
            || source_graph_version < current.source_graph_version
            || (source_graph_version == current.source_graph_version
                && (current.batch_id != lease.record.batch_id
                    || current.outbox_ordinal >= lease.record.ordinal))
        {
            return Err("STALE_PROJECTION_CURSOR: event does not advance watermark".to_string());
        }
    }
    Ok(())
}

fn check_ack_lease_and_cursor_state(
    delivery: &DurableOutboxDelivery,
    lease: &MutationOutboxLease,
    now_ms: u64,
    current_cursor: &Option<MutationProjectionCursor>,
    source_graph_version: u64,
) -> Result<Option<MutationProjectionCursor>, String> {
    if let Some(cursor) = check_ack_delivery_freshness(
        delivery,
        lease,
        now_ms,
        current_cursor,
        source_graph_version,
    )? {
        return Ok(Some(cursor));
    }
    check_ack_cursor_watermark(current_cursor, lease, source_graph_version)?;
    Ok(None)
}

fn build_mutation_projection_cursor(
    projection: &str,
    lease: &MutationOutboxLease,
    source_graph_version: u64,
    now_ms: u64,
) -> Result<MutationProjectionCursor, String> {
    let cursor = MutationProjectionCursor {
        schema_version: MUTATION_BATCH_VERSION,
        projection: projection.to_string(),
        tenant: lease.record.tenant.clone(),
        graph: lease.record.graph.clone(),
        batch_id: lease.record.batch_id.clone(),
        outbox_ordinal: lease.record.ordinal,
        version_scope: lease.record.version_scope,
        source_graph_version,
        advanced_at_ms: now_ms,
    };
    cursor.validate()?;
    Ok(cursor)
}

fn commit_ack_outbox_rows(
    wtx: &redb::WriteTransaction,
    delivery_key: (&str, u32, &str),
    delivery: &DurableOutboxDelivery,
    cursor_key: (&str, &str, &str),
    cursor: &MutationProjectionCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let delivery_bytes = rmp_serde::to_vec_named(delivery).map_err(|e| e.to_string())?;
    let sealed_delivery = crypto.seal(&delivery_bytes);
    let mut deliveries = wtx
        .open_table(MUTATION_OUTBOX_DELIVERY)
        .map_err(|e| e.to_string())?;
    deliveries
        .insert(delivery_key, sealed_delivery.as_ref())
        .map_err(|e| e.to_string())?;

    let cursor_bytes = rmp_serde::to_vec_named(cursor).map_err(|e| e.to_string())?;
    let sealed_cursor = crypto.seal(&cursor_bytes);
    let mut cursors = wtx
        .open_table(MUTATION_PROJECTION_CURSOR)
        .map_err(|e| e.to_string())?;
    cursors
        .insert(cursor_key, sealed_cursor.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn begin_verified_ack_outbox_wtx(
    db: &Database,
    graph_fname: &str,
    lease: &MutationOutboxLease,
    projection: &str,
    crypto: DurableCrypto<'_>,
) -> Result<redb::WriteTransaction, String> {
    validate_ack_outbox_request(graph_fname, lease, projection)?;
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    verify_outbox_lease_matches_durable_event(&wtx, lease, crypto)?;
    Ok(wtx)
}

#[allow(clippy::type_complexity)]
fn resolve_ack_outbox_state(
    wtx: &redb::WriteTransaction,
    graph_fname: &str,
    projection: &str,
    lease: &MutationOutboxLease,
    crypto: DurableCrypto<'_>,
) -> Result<(u64, Option<MutationProjectionCursor>, DurableOutboxDelivery), String> {
    let source_graph_version = resolve_ack_source_graph_version(wtx, lease, crypto)?;
    let current_cursor = read_ack_current_cursor(wtx, projection, lease, graph_fname, crypto)?;
    let delivery = read_ack_outbox_delivery(wtx, lease, crypto)?;
    let earlier = find_earlier_outbox_keys(wtx, graph_fname, lease, crypto)?;
    ensure_earlier_outbox_events_delivered(wtx, &earlier, lease, crypto)?;
    Ok((source_graph_version, current_cursor, delivery))
}

pub(crate) fn ack_mutation_outbox(
    db: &Database,
    graph_fname: &str,
    lease: &MutationOutboxLease,
    projection: &str,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<MutationProjectionCursor, String> {
    let wtx = begin_verified_ack_outbox_wtx(db, graph_fname, lease, projection, crypto)?;

    let (source_graph_version, current_cursor, mut delivery) =
        resolve_ack_outbox_state(&wtx, graph_fname, projection, lease, crypto)?;

    if let Some(cursor) = check_ack_lease_and_cursor_state(
        &delivery,
        lease,
        now_ms,
        &current_cursor,
        source_graph_version,
    )? {
        return Ok(cursor);
    }

    let cursor = build_mutation_projection_cursor(projection, lease, source_graph_version, now_ms)?;
    delivery.delivered_at_ms = Some(now_ms);
    let delivery_key = (
        lease.record.batch_id.as_str(),
        lease.record.ordinal,
        lease.consumer.as_str(),
    );
    let cursor_key = (projection, lease.record.tenant.as_str(), graph_fname);
    commit_ack_outbox_rows(&wtx, delivery_key, &delivery, cursor_key, &cursor, crypto)?;
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(cursor)
}

pub(crate) fn read_mutation_projection_cursor(
    db: &Database,
    graph_fname: &str,
    projection: &str,
    tenant: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<MutationProjectionCursor>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(MUTATION_PROJECTION_CURSOR) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let cursor = table
        .get((projection, tenant, graph_fname))
        .map_err(|e| e.to_string())?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode_mutation_projection_cursor(&bytes)
        })
        .transpose()?;
    Ok(cursor)
}

pub(crate) fn read_mutation_graph_version(
    db: &Database,
    graph_fname: &str,
) -> Result<Option<u64>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(MUTATION_GRAPH_VERSION) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let version = table
        .get(graph_fname)
        .map_err(|e| e.to_string())?
        .map(|value| value.value());
    Ok(version)
}

/// Current lifecycle generation for retry fencing.
pub(crate) fn read_mutation_lifecycle_head(
    db: &Database,
    graph_fname: &str,
) -> Result<Option<String>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = match rtx.open_table(MUTATION_LIFECYCLE_HEAD) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let head = table
        .get(graph_fname)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_string());
    Ok(head)
}

/// One node's vector upsert for a cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node).
pub type VectorUpsert = (String, Vec<f32>);

/// A blob-reference for a cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node): a `(node_id, digest)`
/// pair recorded as a durable graph-side link to an already-stored blob. The blob
/// BYTES live in the content-addressed `blob.redb` (pre-uploaded); THIS is the durable
/// graph pointer that must land atomically with the node/vector/property.
pub type BlobRefRow = (String, String);

/// Apply only the non-topology projections of a cross-modal batch inside an
/// already-open redb write transaction. The universal MutationBatch kernel calls
/// this after graph rows and before status/outbox; the low-level cross-modal
/// primitive uses the same row shapes. No commit occurs here.
/// Blob-ref half of the cross-modal projection: a `__blob__` reserved property
/// carrying the digest, merged into the node row.  A node under native WorkItem
/// authority (or one whose properties cannot be decoded) is refused.
fn apply_crossmodal_blob_ref_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    blob_refs: &[BlobRefRow],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let native_work_items = wtx
        .open_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    for (node_id, digest) in blob_refs {
        let current = nodes
            .get((graph, node_id.as_str()))
            .map_err(|e| e.to_string())?
            .map(|value| crypto.unseal(value.value()))
            .transpose()?;
        if native_work_items
            .get((graph, node_id.as_str()))
            .map_err(|e| e.to_string())?
            .is_some()
            || current.as_ref().is_some_and(|bytes| {
                decode_durable::<serde_json::Map<String, serde_json::Value>>(bytes)
                    .map(|props| property_string(&props, "node_type") == "WorkItem")
                    .unwrap_or(true)
            })
        {
            return Err("native WorkItem authority required for generic blob update".to_string());
        }
        let mut props: serde_json::Map<String, serde_json::Value> = match current {
            Some(bytes) => decode_durable(&bytes)?,
            None => serde_json::Map::new(),
        };
        props.insert(
            "__blob__".to_string(),
            serde_json::Value::String(digest.clone()),
        );
        let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&bytes);
        nodes
            .insert((graph, node_id.as_str()), sealed.as_ref())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Vector half of the cross-modal projection: the graph's `SEMANTIC` blob is
/// read-modify-written inside the caller's transaction.
fn apply_crossmodal_vector_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    vectors: &[VectorUpsert],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let current = semantic
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let mut store = match current {
        Some(bytes) => decode_durable::<crate::compute::semantic::SemanticStore>(&bytes)?,
        None => crate::compute::semantic::SemanticStore::default(),
    };
    for (node_id, embedding) in vectors {
        // CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007): a rejected write bails via `?`
        // BEFORE `store` is reserialized/inserted below, so a mid-batch mismatch
        // never reaches durable storage — `store` here is a scratch decode, not
        // the live in-RAM store, discarded on this early return.
        store
            .add_embedding(node_id.clone(), embedding.clone())
            .map_err(|error| error.to_string())?;
    }
    let bytes = rmp_serde::to_vec_named(&store).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    semantic
        .insert(graph, sealed.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Time-series half of the cross-modal projection: each batch is appended into
/// SERIES_CHUNKS/SERIES_META on the caller's transaction.
#[cfg(feature = "tsdb")]
fn apply_crossmodal_measurement_rows(
    wtx: &redb::WriteTransaction,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    for (series, n_fields, bucket_ns, field_names, points) in measurements {
        if eg_tsdb::store::SeriesKey::decode(series).is_none() {
            return Err("time-series key is not canonically scoped".to_string());
        }
        let points = points
            .iter()
            .map(|(ts, values)| eg_tsdb::point::Point {
                ts: *ts,
                values: values.clone(),
            })
            .collect::<Vec<_>>();
        eg_tsdb::store::append_batch_in_wtx(
            wtx,
            series,
            *n_fields,
            *bucket_ns,
            field_names,
            &points,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// A slim redb-only build has no time-series store, so a non-empty measurement
/// batch is refused rather than silently dropped.
#[cfg(not(feature = "tsdb"))]
fn apply_crossmodal_measurement_rows(
    _wtx: &redb::WriteTransaction,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    if !measurements.is_empty() {
        return Err("time-series cross-modal commit requires the `tsdb` feature".to_string());
    }
    Ok(())
}

fn apply_crossmodal_projection_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
    measurements: &[crate::MeasurementBatch],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if !blob_refs.is_empty() {
        apply_crossmodal_blob_ref_rows(wtx, graph, blob_refs, crypto)?;
    }
    if !vectors.is_empty() {
        apply_crossmodal_vector_rows(wtx, graph, vectors, crypto)?;
    }
    apply_crossmodal_measurement_rows(wtx, measurements)?;
    Ok(())
}

/// **Cross-modal ACID commit (CONCEPT:EG-KG.txn.reader-never-sees-node)** — land a graph + vector + blob-ref +
/// property write-set for ONE graph in ONE redb [`WriteTransaction`], all-or-nothing.
///
/// This is the durable barrier the single-graph cross-modal txn commits through. Every
/// modality writes into the SAME authoritative-shard transaction so the commit is atomic:
///   * **graph** ops (`AddNode`/`AddEdge`/`CompareAndSetNodeFields`/…) → NODES/EDGES,
///     via the shared [`apply_method_rows`] (the SAME rows the single-modal path writes);
///   * **vectors** → the graph's `SEMANTIC` blob is read-modify-written inside the txn
///     (deserialize → `add_embedding` each upsert → reserialize), so a node and its
///     embedding are durable together — never a node without its vector or vice-versa;
///   * **blob refs** → a `__blob__` reserved property on the node carrying the digest,
///     written into NODES, so the graph-side link to the (separately content-addressed)
///     blob lands in the SAME transaction as everything else.
///   * **measurements** (CONCEPT:EG-KG.backend.cross-modal-atomic-commit) → each time-series batch is appended into
///     SERIES_CHUNKS/SERIES_META on THIS transaction via the shared eg-tsdb chunk
///     encoding ([`eg_tsdb::store::append_batch_in_wtx`]), so the points land in the
///     SAME authoritative-shard commit as the node/vector/blob writes (not a separate
///     `series.redb`). `tsdb`-gated; a slim redb-only build errors on a non-empty batch.
///     This shard copy is the atomic/authoritative one; the caller
///     (`handlers::txn::commit_cross_modal_txn`) additionally replays the same batch
///     into the SERVED `series.redb` right after this call returns `Ok`, so it's
///     actually reachable through the public `Ts*`/`Op::TsScan` read path
///     (CONCEPT:EG-KG.backend.ts-served-materialize, EG-P0-4) — see that function's doc comment for the exact
///     guarantee and the one remaining non-atomic boundary.
///
/// If ANY step errors, the `WriteTransaction` is DROPPED without `commit()` — redb
/// discards every staged write, so NONE of the modalities land (a true rollback, no
/// partial). On success the txn commits at `Durability::Immediate` (commit-before-ack:
/// the cross-modal write is on disk before the client is told it succeeded).
// The modality set (db + graph + methods + vectors + blob-refs + measurements + crypto
// [+ audit tail]) is intrinsic to a one-`WriteTransaction` cross-modal commit; grouping
// them into a struct would only relocate the same fields, so the arg count stays flat.
#[allow(clippy::too_many_arguments)]
fn begin_crossmodal_wtx(db: &Database) -> Result<redb::WriteTransaction, String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    Ok(wtx)
}

fn crossmodal_has_clear_or_delete(methods: &[Method]) -> bool {
    methods
        .iter()
        .any(|method| matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }))
}

fn crossmodal_has_node_topology_methods(methods: &[Method]) -> bool {
    methods.iter().any(|method| {
        matches!(
            method,
            Method::AddNode { .. }
                | Method::RemoveNode { .. }
                | Method::CompareAndSetNodeFields { .. }
                | Method::BatchUpdate { .. }
                | Method::ClearGraph
                | Method::DeleteGraph { .. }
        )
    })
}

fn apply_crossmodal_clear_native_graph_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    development_lane::clear_native_graph_rows_in_wtx(wtx, graph, crypto)?;
    capacity_lease::clear_graph_rows(wtx, graph)?;
    Ok(())
}

#[allow(clippy::type_complexity)]
fn open_crossmodal_graph_tables<'txn>(
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
        redb::Table<'txn, (&'static str, u64), &'static str>,
        redb::Table<'txn, &'static str, &'static [u8]>,
    ),
    String,
> {
    let nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let native_work_items = wtx
        .open_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    Ok((nodes, native_work_items, edges, ledger, semantic))
}

// 1. Graph mutations (nodes/edges/properties) — the SAME row apply.
#[allow(clippy::too_many_arguments)]
fn apply_crossmodal_methods(
    graph: &str,
    methods: &[Method],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit: &mut redb::Table<(&str, u64), &[u8]>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    for method in methods {
        apply_method_rows(
            graph,
            method,
            nodes,
            edges,
            ledger,
            semantic,
            native_work_items,
            crypto,
        )?;
        #[cfg(feature = "security")]
        append_audit_entry(audit, audit_tail, graph, method)?;
    }
    Ok(())
}

// 2. Blob refs — a reserved `__blob__` node property pointing at the digest.
// Read-modify-write the node's property blob so the ref rides the node row.
// Unseal the current blob before merging, re-seal the merged result.
//
// Reopened-tables shape: used after the topology-method branch has already
// dropped and reopened `nodes`, so the WorkItem-authority guard reads
// `current` once and combines both checks with `||`.
fn apply_crossmodal_blob_ref_reopened(
    graph: &str,
    node_id: &str,
    digest: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let current = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?;
    if native_work_items
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .is_some()
        || current.as_ref().is_some_and(|bytes| {
            decode_durable::<serde_json::Map<String, serde_json::Value>>(bytes)
                .map(|props| property_string(&props, "node_type") == "WorkItem")
                .unwrap_or(true)
        })
    {
        return Err("native WorkItem authority required for generic blob update".to_string());
    }
    let mut props: serde_json::Map<String, serde_json::Value> = match current {
        Some(bytes) => decode_durable(&bytes)?,
        None => serde_json::Map::new(),
    };
    props.insert(
        "__blob__".to_string(),
        serde_json::Value::String(digest.to_string()),
    );
    let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
    let blob = crypto.seal(&bytes);
    nodes
        .insert((graph, node_id), blob.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn apply_crossmodal_blob_refs_reopened(
    graph: &str,
    blob_refs: &[BlobRefRow],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (node_id, digest) in blob_refs {
        apply_crossmodal_blob_ref_reopened(
            graph,
            node_id,
            digest,
            nodes,
            native_work_items,
            crypto,
        )?;
    }
    Ok(())
}

// Original-tables shape: used when `nodes` is still the FIRST handle opened
// in this transaction (no topology method forced a drop/reopen), so the
// WorkItem-authority guard checks `native_work_items` before ever reading
// `current`, as two separate sequential checks.
fn apply_crossmodal_blob_ref_original(
    graph: &str,
    node_id: &str,
    digest: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if native_work_items
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("native WorkItem authority required for generic blob update".to_string());
    }
    let current = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?;
    if current.as_ref().is_some_and(|bytes| {
        decode_durable::<serde_json::Map<String, serde_json::Value>>(bytes)
            .map(|props| property_string(&props, "node_type") == "WorkItem")
            .unwrap_or(true)
    }) {
        return Err("native WorkItem authority required for generic blob update".to_string());
    }
    let mut props: serde_json::Map<String, serde_json::Value> = match current {
        Some(bytes) => decode_durable(&bytes)?,
        None => serde_json::Map::new(),
    };
    props.insert(
        "__blob__".to_string(),
        serde_json::Value::String(digest.to_string()),
    );
    let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
    let blob = crypto.seal(&bytes);
    nodes
        .insert((graph, node_id), blob.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn apply_crossmodal_blob_refs_original(
    graph: &str,
    blob_refs: &[BlobRefRow],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (node_id, digest) in blob_refs {
        apply_crossmodal_blob_ref_original(
            graph,
            node_id,
            digest,
            nodes,
            native_work_items,
            crypto,
        )?;
    }
    Ok(())
}

// 3. Vectors — read-modify-write the graph's SEMANTIC store blob in-txn.
// Byte-identical between the topology and non-topology paths in the
// pre-decomposition code, so both call this one copy.
fn apply_crossmodal_vectors(
    graph: &str,
    vectors: &[VectorUpsert],
    semantic: &mut redb::Table<&str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if vectors.is_empty() {
        return Ok(());
    }
    let current = semantic
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?;
    let mut store = match current {
        Some(bytes) => decode_durable::<crate::compute::semantic::SemanticStore>(&bytes)?,
        None => crate::compute::semantic::SemanticStore::default(),
    };
    for (node_id, embedding) in vectors {
        // CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007): see the identical comment in
        // `apply_crossmodal_projection_rows` above — `store` is a scratch
        // decode discarded on this early return, and per this function's own
        // doc comment ANY error here drops the whole `WriteTransaction`
        // without committing, so a rejected write here never partially lands.
        store
            .add_embedding(node_id.clone(), embedding.clone())
            .map_err(|error| error.to_string())?;
    }
    let bytes = rmp_serde::to_vec_named(&store).map_err(|e| e.to_string())?;
    let blob = crypto.seal(&bytes);
    semantic
        .insert(graph, blob.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Blob references are read-modify-write node projections and must be
/// covered by the same final lane lifecycle check as topology methods.
///
/// The `if` branch drops/rebinds `nodes`/`edges`/`ledger`/`semantic`/`audit`
/// before its own `validate_current_lane_links_in_wtx` call, so its
/// (shadowed, block-local) handles go out of scope naturally. The `else`
/// branch never rebinds them — it reuses the tables opened by the caller for
/// its own blob-refs/vectors work — so they must be dropped explicitly here,
/// BEFORE the shared `validate_current_lane_links_in_wtx` call in
/// `finalize_crossmodal_commit` reopens `NODES`: leaving them open made that
/// reopen fail with redb's "Table 'nodes' already opened" error whenever
/// `methods` carried no AddNode/RemoveNode/CompareAndSetNodeFields/
/// BatchUpdate/ClearGraph/DeleteGraph (e.g. a measurement-only or
/// blob-refs-only cross-modal commit).
#[allow(clippy::too_many_arguments)]
fn apply_crossmodal_blob_and_vector_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
    methods: &[Method],
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
    nodes: redb::Table<'_, (&str, &str), &[u8]>,
    edges: redb::Table<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: redb::Table<'_, (&str, u64), &str>,
    semantic: redb::Table<'_, &str, &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    #[cfg(feature = "security")] audit: redb::Table<'_, (&str, u64), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if crossmodal_has_node_topology_methods(methods) {
        drop(nodes);
        drop(edges);
        drop(ledger);
        drop(semantic);
        #[cfg(feature = "security")]
        drop(audit);
        development_lane::validate_current_lane_links_in_wtx(wtx, graph, crypto)?;
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;

        apply_crossmodal_blob_refs_reopened(
            graph,
            blob_refs,
            &mut nodes,
            native_work_items,
            crypto,
        )?;
        apply_crossmodal_vectors(graph, vectors, &mut semantic, crypto)?;
    } else {
        let mut nodes = nodes;
        let mut semantic = semantic;
        apply_crossmodal_blob_refs_original(
            graph,
            blob_refs,
            &mut nodes,
            native_work_items,
            crypto,
        )?;
        apply_crossmodal_vectors(graph, vectors, &mut semantic, crypto)?;

        drop(nodes);
        drop(edges);
        drop(ledger);
        drop(semantic);
        #[cfg(feature = "security")]
        drop(audit);
    }
    Ok(())
}

// 4. Measurements (CONCEPT:EG-KG.backend.cross-modal-atomic-commit) — append each time-series batch into
// SERIES_CHUNKS/SERIES_META ON THIS transaction (the shared eg-tsdb chunk
// encoding, via `append_batch_in_wtx`), so the points land in the SAME
// authoritative-shard commit as the node/vector/blob writes. redb's exclusive
// per-process file lock means this is the ONLY way a measurement can be atomic
// WITH the graph modalities: through the transaction the writer already owns.
#[cfg(feature = "tsdb")]
fn apply_crossmodal_measurements(
    wtx: &redb::WriteTransaction,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    for (series, n_fields, bucket_ns, field_names, points) in measurements {
        // Persistence accepts only the authority-scoped key produced at the
        // verified carrier boundary; it never derives or guesses a tenant.
        if eg_tsdb::store::SeriesKey::decode(series).is_none() {
            return Err("time-series key is not canonically scoped".to_string());
        }
        let pts: Vec<eg_tsdb::point::Point> = points
            .iter()
            .map(|(ts, values)| eg_tsdb::point::Point {
                ts: *ts,
                values: values.clone(),
            })
            .collect();
        eg_tsdb::store::append_batch_in_wtx(wtx, series, *n_fields, *bucket_ns, field_names, &pts)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// A build without the `tsdb` feature has no SERIES tables + no eg-tsdb dep, so a
// measurement here has no durable home — error rather than silently drop it (the
// staging handler is `tsdb`-gated, so in practice this is never non-empty).
#[cfg(not(feature = "tsdb"))]
fn apply_crossmodal_measurements(
    _wtx: &redb::WriteTransaction,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    if !measurements.is_empty() {
        return Err("time-series cross-modal commit requires the `tsdb` feature".to_string());
    }
    Ok(())
}

/// Backfill a graph_meta identity row so authoritative load_all recovers it.
fn backfill_crossmodal_graph_meta(wtx: &redb::WriteTransaction, graph: &str) -> Result<(), String> {
    let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    if meta.get(graph).map_err(|e| e.to_string())?.is_none() {
        let incarnation_id = new_incarnation_id(graph);
        let encoded = encode_meta_with_incarnation(graph, GraphType::Global, &incarnation_id)?;
        meta.insert(graph, encoded.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn finalize_crossmodal_commit(
    wtx: &redb::WriteTransaction,
    graph: &str,
    measurements: &[crate::MeasurementBatch],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    development_lane::validate_current_lane_links_in_wtx(wtx, graph, crypto)?;
    apply_crossmodal_measurements(wtx, measurements)?;
    backfill_crossmodal_graph_meta(wtx, graph)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_crossmodal_body(
    wtx: &redb::WriteTransaction,
    graph: &str,
    methods: &[Method],
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
    measurements: &[crate::MeasurementBatch],
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let (mut nodes, mut native_work_items, mut edges, mut ledger, mut semantic) =
        open_crossmodal_graph_tables(wtx)?;

    if crossmodal_has_clear_or_delete(methods) {
        work_item_capability::clear_graph_rows_in_wtx_with_native(
            wtx,
            graph,
            &mut native_work_items,
        )?;
    }

    #[cfg(feature = "security")]
    let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;

    apply_crossmodal_methods(
        graph,
        methods,
        &mut nodes,
        &mut edges,
        &mut ledger,
        &mut semantic,
        &native_work_items,
        crypto,
        #[cfg(feature = "security")]
        &mut audit,
        #[cfg(feature = "security")]
        audit_tail,
    )?;

    apply_crossmodal_blob_and_vector_rows(
        wtx,
        graph,
        methods,
        vectors,
        blob_refs,
        nodes,
        edges,
        ledger,
        semantic,
        &native_work_items,
        #[cfg(feature = "security")]
        audit,
        crypto,
    )?;

    finalize_crossmodal_commit(wtx, graph, measurements, crypto)?;
    Ok(())
}

/// Everything one cross-modal commit stages, bundled so [`commit_crossmodal`]
/// stays inside clippy's parameter cap. Each slice is exactly the borrow callers
/// used to pass positionally, in the same order.
#[derive(Clone, Copy, Default)]
pub(crate) struct CrossModalStaged<'a> {
    pub methods: &'a [Method],
    pub vectors: &'a [VectorUpsert],
    pub blob_refs: &'a [BlobRefRow],
    /// Staged time-series measurement batches (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Each lands in the SAME
    /// `WriteTransaction` as the graph/vector/blob writes, into SERIES_CHUNKS/SERIES_META
    /// in THIS shard (not a separate `series.redb`), so a measurement and the node
    /// it annotates are durable together — never one without the other.
    pub measurements: &'a [crate::MeasurementBatch],
}

pub(crate) fn commit_crossmodal(
    db: &Database,
    graph: &str,
    staged: CrossModalStaged<'_>,
    crypto: DurableCrypto<'_>,
    // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), shared with the group-commit path.
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let CrossModalStaged {
        methods,
        vectors,
        blob_refs,
        measurements,
    } = staged;
    let wtx = begin_crossmodal_wtx(db)?;
    if crossmodal_has_clear_or_delete(methods) {
        apply_crossmodal_clear_native_graph_rows(&wtx, graph, crypto)?;
    }
    apply_crossmodal_body(
        &wtx,
        graph,
        methods,
        vectors,
        blob_refs,
        measurements,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )?;
    // The atomic commit point: every modality lands here, or (on any `?` above) the
    // dropped wtx discards them all.
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably write/overwrite a graph_meta identity row in its OWN transaction.
pub(crate) fn write_graph_meta(
    db: &Database,
    graph: &str,
    name: &str,
    graph_type: GraphType,
) -> Result<(), String> {
    {
        let rtx = db.begin_read().map_err(|e| e.to_string())?;
        let meta = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
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
    write_graph_meta_with_incarnation(db, graph, name, graph_type, &incarnation_id)
}

/// Durably register an exact lifecycle incarnation. Repeating the same identity
/// is idempotent; attempting to overwrite a live same-name incarnation fails
/// closed so stale work cannot silently retarget itself.
pub(crate) fn write_graph_meta_with_incarnation(
    db: &Database,
    graph: &str,
    name: &str,
    graph_type: GraphType,
    incarnation_id: &str,
) -> Result<(), String> {
    if incarnation_id.trim().is_empty() {
        return Err("graph incarnation id must not be empty".to_string());
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
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
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Point-read a single node's stored properties (read-through path).
pub(crate) fn read_one_node(
    db: &Database,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
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
    db: &Database,
    graph: &str,
    node_ids: &[String],
) -> Result<Vec<bool>, String> {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|error| error.to_string())?;
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
    semantic: &redb::Table<&str, &[u8]>,
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
    semantic: &mut redb::Table<&str, &[u8]>,
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
    semantic: &mut redb::Table<&str, &[u8]>,
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
    semantic: &mut redb::Table<&str, &[u8]>,
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
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    let ordinals: Vec<u32> = edges
        .range((graph, source, target, 0u32)..)
        .map_err(|error| error.to_string())?
        .filter_map(Result::ok)
        .take_while(|(key, _)| {
            let (candidate_graph, candidate_source, candidate_target, _) = key.value();
            candidate_graph == graph && candidate_source == source && candidate_target == target
        })
        .map(|(key, _)| key.value().3)
        .collect();
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
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    semantic: &mut redb::Table<&str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    remove_durable_node_rows(graph, node_id, nodes, edges)?;
    remove_durable_embedding(semantic, graph, node_id, crypto)
}

fn remove_durable_node_rows(
    graph: &str,
    node_id: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    nodes
        .remove((graph, node_id))
        .map_err(|error| error.to_string())?;
    // The edge key is `(graph, source, target, ordinal)`: outgoing edges form a
    // prefix, while incoming edges require one bounded scan of this graph.
    let incident: Vec<(String, String, u32)> = edges
        .range((graph, "", "", 0u32)..)
        .map_err(|error| error.to_string())?
        .filter_map(Result::ok)
        .take_while(|(key, _)| key.value().0 == graph)
        .filter_map(|(key, _)| {
            let (_, source, target, ordinal) = key.value();
            (source == node_id || target == node_id)
                .then(|| (source.to_string(), target.to_string(), ordinal))
        })
        .collect();
    for (source, target, ordinal) in incident {
        edges
            .remove((graph, source.as_str(), target.as_str(), ordinal))
            .map_err(|error| error.to_string())?;
        invalidate_edge_ord(graph, &source, &target);
    }
    invalidate_node_edge_ords(graph, node_id);
    Ok(())
}

/// Translate ONE applied method into redb row writes inside an open transaction.
/// Mirrors `crate::mutation_apply::apply`'s method set: the durable DATA mutations only.
// crate-internal only (no external callers): each parameter is a distinct
// borrowed redb table handle the write touches, plus the method/graph being
// applied -- bundling the table handles into a struct would just move the
// same borrows behind one more layer of indirection.
#[allow(clippy::too_many_arguments)]
/// `Method::CompareAndSetNodeFields` row effect.  Evaluates and merges the CAS
/// against the durable pre-image inside the held transaction; persisting
/// `updates_msgpack` by itself discarded every untouched property and could
/// diverge from GraphCore.  A missing node is a silent no-op, as before.
fn apply_cas_node_fields_row(
    graph: &str,
    node_id: &str,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
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
    nodes: &redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
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

pub(crate) fn apply_method_rows(
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
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

/// Per-graph audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store): `graph -> (last_seq, last_hash)`.
///
/// **Why this exists (profiling rationale).** After EG-024 (group-commit micro-linger)
/// freed the disk, the single `eg-redb-writer` thread became ~99.9% CPU-bound in
/// userspace. The hot spot was [`append_audit_entry`]: it range-scanned this graph's
/// audit tail **per op** to find `(last_seq, last_hash)` — O(ops) B-tree walks inside
/// the held `WriteTransaction`, and the cost GREW as EG-024 made batches bigger.
///
/// The redb file has a single exclusive writer, so within the server process the
/// writer thread is the **only** mutator of the `AUDIT` table. That makes an in-memory
/// tail authoritative: nothing else can advance a graph's chain behind our back, so we
/// can keep `(seq, hash)` hot in RAM across the thread's lifetime and chain off it with
/// **no scan**. The cache is seeded ONCE per graph from a single range-scan on first
/// touch (which also re-seeds correctly after a restart), then updated in place on every
/// append. `apply_checkpoint`/`purge_graph_rows`/`ClearGraph` never delete AUDIT rows,
/// so the cached tail is never invalidated by those paths.
#[cfg(feature = "security")]
pub(crate) type AuditTailCache = std::collections::HashMap<String, (u64, crate::audit::Hash)>;

/// Append ONE tamper-evident audit-chain entry for a durable mutation, inside the
/// caller's open WriteTransaction (CONCEPT:EG-KG.sharding.row-level-security; O(1) via CONCEPT:EG-KG.storage.embedded-store). Uses the
/// cached per-graph chain tail (`last seq` + its hash) to get `prev_hash` + next `seq`,
/// links the new entry, inserts it, and updates the cache to the just-appended entry —
/// so the NEXT op chains off RAM with NO per-op range scan. On a cache miss (first touch
/// of the graph since the writer opened — incl. after a restart) the tail is seeded from
/// exactly ONE range-scan, then stays hot. A method with no canonical audit line (e.g. a
/// pure-compute op that slipped through) is skipped. The audit row rides the SAME
/// transaction as the data mutation, so they are durable together. Only compiled/called
/// under `security`.
///
/// **Correctness:** the linked hash is computed identically to before
/// (`link_hash(prev, graph, seq, line)`, prev = previous entry's hash, seq = prev+1 or
/// genesis 0). The cache only replaces the *lookup* of `(prev_seq, prev_hash)`; the seed
/// scan returns the exact same tail the old per-op scan did, and every subsequent value
/// is the hash we just stored. So the persisted chain is byte-for-byte what the scanning
/// version produced — tamper-evidence and `verify_audit` are unchanged.
#[cfg(feature = "security")]
pub(crate) fn append_audit_entry(
    audit: &mut redb::Table<(&str, u64), &[u8]>,
    cache: &mut AuditTailCache,
    graph: &str,
    method: &Method,
) -> Result<(), String> {
    let line = match crate::audit::audit_line(method) {
        Some(l) => l,
        None => return Ok(()),
    };
    append_audit_entry_with_line(audit, cache, graph, line.as_bytes()).map(|_| ())
}

/// [`append_audit_entry`]'s underlying primitive: append ONE chain entry for an
/// explicit `line` (rather than deriving it from a `Method`) and return the
/// assigned `(seq, hash)`. Shared by the per-mutation audit trail above AND the
/// provenance-anchor job ([`provenance_anchor_commit`]), which appends a
/// `PROVENANCE_ANCHOR|...` line that has no corresponding `Method` at all — it is
/// synthesized by a periodic sweep, not a client request. Behavior (and the
/// persisted bytes) for the `Method`-driven call sites are byte-for-byte
/// unchanged: `append_audit_entry` now does nothing but derive `line` and forward
/// here.
#[cfg(feature = "security")]
pub(crate) fn append_audit_entry_with_line(
    audit: &mut redb::Table<(&str, u64), &[u8]>,
    cache: &mut AuditTailCache,
    graph: &str,
    line: &[u8],
) -> Result<(u64, crate::audit::Hash), String> {
    // O(1): chain off the cached tail; seed it from ONE scan only on first touch.
    let (prev, next_seq) = match cache.get(graph) {
        Some(&(seq, hash)) => (hash, seq + 1),
        None => {
            // First touch since open (or after restart): seek the highest existing
            // seq directly via a BOUNDED reverse range — the audit-tail sibling of
            // `scan_next_edge_ordinal`'s `next_back()` pattern below. The upper bound
            // `(graph, u64::MAX)` already excludes every later graph's rows, so this
            // is one B-tree seek to the tail (O(log chain length)), not the old
            // forward walk-to-the-end (`.range((graph, 0u64)..)` + `.last()`) whose
            // cost grew with the chain. Extract OWNED values so the read
            // access-guards drop before the mutable `insert` below.
            let tail: Option<(u64, crate::audit::Hash)> = {
                let mut iter = audit
                    .range((graph, 0u64)..=(graph, u64::MAX))
                    .map_err(|e| e.to_string())?;
                // Pull explicitly (rather than `.next_back()` inline) so every audit
                // row this cold seed touches passes through ONE counted point. A
                // bounded reverse seek pulls exactly 1 regardless of chain length; a
                // regression back to a forward walk (`.last()`, or a `while let
                // Some(..) = iter.next()` loop) pulls N through this same site and the
                // counter records it. See
                // `audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan`, which
                // asserts the count is CONSTANT across a 5-entry and a 200,000-entry
                // chain — a deterministic, machine-independent statement of the
                // O(1)-vs-O(n) property that a wall-clock budget could only ever
                // approximate (and which was unreproducible on a shared build host).
                let last = iter.next_back().transpose().map_err(|e| e.to_string())?;
                #[cfg(test)]
                if last.is_some() {
                    cold_seed_rows_touched_inc(1);
                }
                match last {
                    Some((k, v)) => {
                        let seq = k.value().1;
                        let (_, hash, _) = crate::audit::decode_entry(v.value())
                            .ok_or_else(|| "corrupt audit tail entry".to_string())?;
                        Some((seq, hash))
                    }
                    None => None,
                }
            };
            match tail {
                Some((seq, hash)) => (hash, seq + 1),
                None => (crate::audit::GENESIS, 0u64),
            }
        }
    };
    let hash = crate::audit::link_hash(&prev, graph, next_seq, line);
    let blob = crate::audit::encode_entry(&prev, &hash, line);
    audit
        .insert((graph, next_seq), blob.as_slice())
        .map_err(|e| e.to_string())?;
    // Keep the tail hot: the next op (this batch or a later one) chains off RAM.
    cache.insert(graph.to_string(), (next_seq, hash));
    Ok((next_seq, hash))
}

// Audit rows pulled by audit-tail COLD SEEDS on THIS THREAD (test builds only).
//
// The one counted point for the O(1)-vs-O(n) property that
// `audit_tail_cold_seed_is_a_bounded_seek_not_a_forward_scan` asserts. Kept out of
// release builds entirely so the hot append path pays nothing.
//
// `thread_local!`, NOT a process-global `AtomicU64` (its original shape): `cargo
// test`'s default harness runs every `#[test]` function on its own dedicated OS
// thread from a pool, all sharing ONE process — a process-global counter is
// incremented by EVERY concurrently-running test that happens to durably commit
// through `commit_ops` (i.e. most of this crate's redb-backed tests), not just the
// one test measuring it. That produced exactly the failure this shape fixes:
// `cold_seed_rows_touched_take()` observing 4 rows touched instead of the 1 this
// test itself caused, because 3 more were attributed from unrelated sibling tests
// racing on the SAME global counter during this test's measurement window. Since
// `commit_ops` runs synchronously on the caller's thread (no internal thread
// spawn) and this test never spawns another thread either, a thread-local counter
// isolates this test's own count perfectly, with no cross-test synchronization
// needed at all — strictly more correct than the shared-`Mutex`/lock alternative,
// not merely faster.
#[cfg(test)]
thread_local! {
    static COLD_SEED_ROWS_TOUCHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn cold_seed_rows_touched_inc(n: u64) {
    COLD_SEED_ROWS_TOUCHED.with(|c| c.set(c.get() + n));
}

/// Read-and-reset the cold-seed row counter. Tests call this immediately before and
/// after the call under measurement.
#[cfg(test)]
fn cold_seed_rows_touched_take() -> u64 {
    COLD_SEED_ROWS_TOUCHED.with(|c| c.replace(0))
}

/// Verify a graph's hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security). Range-scans
/// `(graph, 0..)` in seq order and walks the chain via `crate::audit::verify_chain`.
#[cfg(feature = "security")]
pub(crate) fn verify_audit(
    db: &Database,
    graph: &str,
) -> Result<crate::protocol::AuditReport, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let audit = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
    for r in audit.range((graph, 0u64)..).map_err(|e| e.to_string())? {
        let (k, v) = r.map_err(|e| e.to_string())?;
        if k.value().0 != graph {
            break;
        }
        rows.push((k.value().1, v.value().to_vec()));
    }
    Ok(crate::audit::verify_chain(
        graph,
        rows.iter().map(|(s, b)| (*s, b.as_slice())),
    ))
}

// ── Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security) ───────────────────────────────
//
// A periodic engine job (`server::persistence::provenance_anchor`) Merkle-anchors a
// graph's `:ToolCall`/`:RunTrace` provenance-node window into the SAME hash-chained
// AUDIT table above, so a byte-level tamper of an anchored node's durable content —
// invisible to `verify_audit` alone, which only proves the SEQUENCE of audit lines
// is unbroken, not that a node's current bytes match what was written — becomes
// detectable via a Merkle inclusion proof against that anchored, chain-protected
// root. See `crate::audit`'s module doc for the full design rationale.
//
// The three functions below split the work by WHERE it is safe to run:
//   * [`provenance_leaf_hashes`] is a lock-free MVCC snapshot read (like
//     `read_one_node`) — it does NOT touch the writer thread, so hashing a large
//     window never competes with the ordinary write path.
//   * [`provenance_anchor_commit`] is the only piece that writes; its own cost is
//     O(1) in window size (the window was already hashed off-thread) and it skips
//     entirely (no transaction at all) when the graph's last anchored root is
//     unchanged — the overhead-budget guarantee this whole feature must meet.
//   * [`prove_inclusion`] is a read-only reconstruction of one node's inclusion
//     proof against a chosen (or the latest) anchor.

/// Per-graph provenance-anchor tail cache: `graph -> (last anchor seq, last
/// anchored root)`. Mirrors [`AuditTailCache`]'s O(1) seed-once-then-hot-in-RAM
/// design so the periodic anchor sweep's "did anything change since the last
/// anchor" check never range-scans on the common (unchanged) tick.
#[cfg(feature = "security")]
pub(crate) type ProvenanceAnchorCache = HashMap<String, (u64, crate::audit::Hash)>;

/// Read the CURRENT durable content of each of `node_ids` and hash it into a
/// provenance leaf hash. A lock-free MVCC snapshot read (mirrors `read_one_node`/
/// `durable_node_presence`) — does NOT go through the writer thread, so this can
/// process a large window without competing with the ordinary write path. An id
/// with no durable row (removed since it was selected as a candidate) is
/// silently excluded: the window is "whatever is durably present right now", not
/// a promise that every candidate survives to be anchored.
#[cfg(feature = "security")]
pub(crate) fn provenance_leaf_hashes(
    db: &Database,
    graph: &str,
    node_ids: &[String],
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, crate::audit::Hash)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(node_ids.len());
    for id in node_ids {
        if let Some(v) = nodes.get((graph, id.as_str())).map_err(|e| e.to_string())? {
            let content = crypto.unseal(v.value())?;
            out.push((id.clone(), crate::audit::merkle_leaf_hash(id, &content)));
        }
    }
    Ok(out)
}

/// Seek `graph`'s latest provenance-anchor `(seq, root)` directly off durable
/// storage via a bounded reverse scan (the `append_audit_entry` tail-seek
/// pattern, never a forward walk) — used to seed [`ProvenanceAnchorCache`] on
/// first touch and to resolve `Method::AuditProveInclusion`'s `anchor_seq: None`.
/// The root is always decoded from the tamper-evident AUDIT entry at that seq,
/// never trusted from the `PROVENANCE_ANCHOR_MEMBERS` side table.
#[cfg(feature = "security")]
fn read_latest_provenance_anchor_root(
    db: &Database,
    graph: &str,
) -> Result<Option<(u64, crate::audit::Hash)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let anchor_members = rtx
        .open_table(PROVENANCE_ANCHOR_MEMBERS)
        .map_err(|e| e.to_string())?;
    let last = anchor_members
        .range((graph, 0u64)..=(graph, u64::MAX))
        .map_err(|e| e.to_string())?
        .next_back()
        .transpose()
        .map_err(|e| e.to_string())?;
    let Some((k, _)) = last else {
        return Ok(None);
    };
    let seq = k.value().1;
    let audit = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let audit_row = audit
        .get((graph, seq))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "provenance anchor row has no matching audit entry".to_string())?;
    let (_, _, line) = crate::audit::decode_entry(audit_row.value())
        .ok_or_else(|| "corrupt audit entry at anchor seq".to_string())?;
    let (_, root) = crate::audit::parse_provenance_anchor_line(line)
        .ok_or_else(|| "anchor seq is not a PROVENANCE_ANCHOR line".to_string())?;
    Ok(Some((seq, root)))
}

/// Durably anchor a provenance-node window's Merkle root into `graph`'s
/// tamper-evident audit chain. `members` is the CALLER's already-hashed
/// `(node_id, leaf_hash)` window (see [`provenance_leaf_hashes`], computed OFF
/// any transaction so this function's own cost is independent of window size —
/// the write-throughput overhead budget this satisfies). Returns `Ok(None)` with
/// NO transaction opened at all when `root` already equals the graph's last
/// anchored root per the in-RAM `cache` (an idle graph's provenance window is
/// unchanged tick to tick — the common case). On a genuine change: opens one
/// `WriteTransaction`, appends a `PROVENANCE_ANCHOR|count=N|sha256:ROOT` line to
/// the SAME audit chain [`append_audit_entry`] uses, stores `members` at the
/// assigned seq so a later inclusion proof can reconstruct the sibling path (see
/// [`prove_inclusion`]), and returns `Ok(Some(seq))`.
#[cfg(feature = "security")]
pub(crate) fn provenance_anchor_commit(
    db: &Database,
    cache: &mut ProvenanceAnchorCache,
    audit_tail: &mut AuditTailCache,
    graph: &str,
    root: crate::audit::Hash,
    members: &[(String, crate::audit::Hash)],
) -> Result<Option<u64>, String> {
    if members.is_empty() {
        return Ok(None);
    }
    // Fast path: the cache already says nothing changed -- zero redb transactions.
    if cache.get(graph).map(|&(_, last)| last) == Some(root) {
        return Ok(None);
    }
    // First touch since open (or after restart): seed from durable state via a
    // plain read transaction (no write lock held) before deciding to write.
    if !cache.contains_key(graph) {
        if let Some((seq, seeded_root)) = read_latest_provenance_anchor_root(db, graph)? {
            cache.insert(graph.to_string(), (seq, seeded_root));
            if seeded_root == root {
                return Ok(None);
            }
        }
    }

    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let seq = {
        let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        let mut anchor_members = wtx
            .open_table(PROVENANCE_ANCHOR_MEMBERS)
            .map_err(|e| e.to_string())?;
        let line = crate::audit::provenance_anchor_line(members.len(), &root);
        let (seq, _hash) =
            append_audit_entry_with_line(&mut audit, audit_tail, graph, line.as_bytes())?;
        let on_disk: Vec<(String, Vec<u8>)> = members
            .iter()
            .map(|(id, h)| (id.clone(), h.to_vec()))
            .collect();
        let encoded = rmp_serde::to_vec_named(&on_disk).map_err(|e| e.to_string())?;
        anchor_members
            .insert((graph, seq), encoded.as_slice())
            .map_err(|e| e.to_string())?;
        seq
    };
    wtx.commit().map_err(|e| e.to_string())?;
    cache.insert(graph.to_string(), (seq, root));
    Ok(Some(seq))
}

/// Produce + verify a Merkle inclusion proof for `node_id` against a provenance
/// anchor (`Method::AuditProveInclusion`). `anchor_seq = None` resolves to the
/// graph's most recent anchor. The ANCHORED ROOT is always read from the
/// tamper-evident audit-chain entry at that seq (never from the members side
/// table); `node_id`'s CURRENT durable content is re-hashed and walked up the
/// anchor-time sibling path (from the members table) to compare against that
/// root — a mismatch is the tamper signal (`verified = false`), independent of
/// whatever happened to any OTHER node in the window (each leaf's proof only
/// needs its own O(log n) sibling hashes, not its neighbors' current content).
#[cfg(feature = "security")]
pub(crate) fn prove_inclusion(
    db: &Database,
    graph: &str,
    node_id: &str,
    anchor_seq: Option<u64>,
    crypto: DurableCrypto<'_>,
) -> Result<crate::protocol::MerkleInclusionReport, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;

    let seq = match anchor_seq {
        Some(seq) => seq,
        None => {
            let anchor_members = rtx
                .open_table(PROVENANCE_ANCHOR_MEMBERS)
                .map_err(|e| e.to_string())?;
            let last = anchor_members
                .range((graph, 0u64)..=(graph, u64::MAX))
                .map_err(|e| e.to_string())?
                .next_back()
                .transpose()
                .map_err(|e| e.to_string())?;
            match last {
                Some((k, _)) => k.value().1,
                None => return Err(format!("graph '{graph}' has no provenance anchor yet")),
            }
        }
    };

    let audit = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
    let audit_row = audit
        .get((graph, seq))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no audit entry at seq {seq}"))?;
    let (_, _, line) = crate::audit::decode_entry(audit_row.value())
        .ok_or_else(|| "corrupt audit entry".to_string())?;
    let (count, anchored_root) = crate::audit::parse_provenance_anchor_line(line)
        .ok_or_else(|| format!("audit entry at seq {seq} is not a PROVENANCE_ANCHOR line"))?;

    let anchor_members = rtx
        .open_table(PROVENANCE_ANCHOR_MEMBERS)
        .map_err(|e| e.to_string())?;
    let members_row = anchor_members
        .get((graph, seq))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no provenance-anchor member row at seq {seq}"))?;
    let stored: Vec<(String, Vec<u8>)> = decode_durable(members_row.value())?;
    if stored.len() != count {
        return Err("provenance-anchor member row does not match its audit line count".to_string());
    }
    let members: Vec<(String, crate::audit::Hash)> = stored
        .into_iter()
        .map(|(id, h)| {
            let hash: crate::audit::Hash = h
                .as_slice()
                .try_into()
                .map_err(|_| "corrupt provenance-anchor member hash".to_string())?;
            Ok((id, hash))
        })
        .collect::<Result<_, String>>()?;

    let window_size = members.len();
    let anchored_root_sha256 = hex::encode(anchored_root);

    let Some(index) = members.iter().position(|(id, _)| id == node_id) else {
        return Ok(crate::protocol::MerkleInclusionReport {
            graph: graph.to_string(),
            node_id: node_id.to_string(),
            anchor_seq: seq,
            window_size,
            included: false,
            verified: false,
            anchored_root_sha256: anchored_root_sha256.clone(),
            computed_root_sha256: anchored_root_sha256,
            proof: Vec::new(),
            detail: "node was not part of this anchor's provenance window".to_string(),
        });
    };

    let leaf_hashes: Vec<crate::audit::Hash> = members.iter().map(|(_, h)| *h).collect();
    let path = crate::audit::audit_path_from_hashes(&leaf_hashes, index);

    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let current = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?;

    let (current_leaf_hash, detail_if_missing) = match &current {
        Some(content) => (crate::audit::merkle_leaf_hash(node_id, content), None),
        // No durable row anymore (removed since anchoring). There is nothing left
        // to re-hash; fold in a fixed domain-tagged sentinel so the proof walk
        // stays well-defined. It CANNOT reproduce the real anchor-time leaf hash,
        // so verification fails closed exactly like real content tampering would.
        None => (
            crate::audit::merkle_leaf_hash(node_id, crate::audit::MISSING_NODE_SENTINEL),
            Some("node has no durable row anymore (removed since anchoring)".to_string()),
        ),
    };

    let computed_root = crate::audit::recompute_root(&current_leaf_hash, &path);
    let verified = computed_root == anchored_root;

    let proof = path
        .into_iter()
        .map(|step| crate::protocol::MerkleProofStep {
            sibling_sha256: hex::encode(step.sibling),
            side: step.side,
        })
        .collect();

    let detail = if verified {
        "verified: current durable content matches the anchored leaf".to_string()
    } else if let Some(missing) = detail_if_missing {
        missing
    } else {
        "TAMPER DETECTED: current durable content does not match the anchored leaf".to_string()
    };

    Ok(crate::protocol::MerkleInclusionReport {
        graph: graph.to_string(),
        node_id: node_id.to_string(),
        anchor_seq: seq,
        window_size,
        included: true,
        verified,
        anchored_root_sha256,
        computed_root_sha256: hex::encode(computed_root),
        proof,
        detail,
    })
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
    edges: &redb::Table<(&str, &str, &str, u32), &[u8]>,
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
    edges: &redb::Table<(&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    let max = edges
        .range((graph, src, tgt, 0u32)..=(graph, src, tgt, u32::MAX))
        .map_err(|e| e.to_string())?
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
            let database = Database::create(path).map_err(|error| error.to_string())?;
            let transaction = database.begin_write().map_err(|error| error.to_string())?;
            let mut edges = transaction
                .open_table(EDGES)
                .map_err(|error| error.to_string())?;
            let value = [0u8];
            for ordinal in 0..parallel_rows as u32 {
                edges
                    .insert(("g37", "source", "target", ordinal), value.as_slice())
                    .map_err(|error| error.to_string())?;
            }
            let cold = next_edge_ordinal(&edges, "g37", "source", "target")?;
            let hot = next_edge_ordinal(&edges, "g37", "source", "target")?;
            invalidate_edge_ord("g37", "source", "target");
            let reseeded = next_edge_ordinal(&edges, "g37", "source", "target")?;
            drop(edges);
            transaction.abort().map_err(|error| error.to_string())?;
            Ok((cold, hot, reseeded))
        })
        .map_err(|error| error.to_string())?
        .join()
        .map_err(|_| "edge-ordinal probe worker panicked".to_string())?
}

/// Apply a decoded `BatchUpdate` op-list as row writes.
/// `BatchOperation::AddNode`.  An upsert merges over the durable pre-image
/// first; a plain add replaces the row outright.
fn apply_batch_add_node_row(
    graph: &str,
    index: usize,
    id: &str,
    mut properties_msgpack: Vec<u8>,
    upsert: bool,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
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
    nodes: &redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
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
    nodes: &redb::Table<(&str, &str), &[u8]>,
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

fn apply_batch_rows(
    graph: &str,
    operations_msgpack: &[u8],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    semantic: &mut redb::Table<&str, &[u8]>,
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
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    let node_keys: Vec<String> = nodes
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1.to_string())
        .collect();
    for id in node_keys {
        let _ = nodes.remove((graph, id.as_str()));
    }
    let edge_keys: Vec<(String, String, u32)> = edges
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| {
            let (_, s, t, o) = k.value();
            (s.to_string(), t.to_string(), o)
        })
        .collect();
    for (s, t, o) in edge_keys {
        let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
    }
    let seqs: Vec<u64> = ledger
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1)
        .collect();
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
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    let seqs: Vec<u64> = ledger
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1)
        .collect();
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
    reservations: &redb::Table<(&str, &str), &[u8]>,
    tenant_index: &redb::Table<(&str, &str, &str), &str>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let mut has_active_rows = false;
    let rows = reservations
        .range((graph, "")..)
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, row_reservation_id) = key.value();
        if row_graph != graph {
            break;
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
    tenant_index: &redb::Table<(&str, &str, &str), &str>,
    reservations: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let rows = tenant_index
        .range((graph, "", "")..)
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            break;
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

/// Clear every `(graph, key)` row of a two-part-key table for `graph`, in
/// bounded `MAX_RESOURCE_CLEAR_SCAN`-sized passes so the caller's open
/// `WriteTransaction` never has to allocate an unbounded key list.
/// One bounded scan pass of `clear_resource_two_part_table`: collects at most
/// `MAX_RESOURCE_CLEAR_SCAN` second-key parts for `graph`, starting after
/// `cursor`.  Mirrors `collect_resource_attempts_clear_keys`.
fn collect_resource_two_part_clear_keys<V: redb::Value + 'static>(
    table: &redb::Table<(&str, &str), V>,
    graph: &str,
    cursor: &Option<String>,
) -> Result<Vec<String>, String> {
    let start = cursor.as_deref().unwrap_or("");
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table
        .range((graph, start)..)
        .map_err(|error| error.to_string())?
    {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, key_part) = key.value();
        if row_graph != graph {
            break;
        }
        if cursor.as_deref() == Some(key_part) {
            continue;
        }
        keys.push(key_part.to_string());
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

fn clear_resource_two_part_table<V: redb::Value + 'static>(
    table: &mut redb::Table<(&str, &str), V>,
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
    table: &redb::Table<(&str, &str, u64), &str>,
    graph: &str,
    cursor: &Option<(String, u64)>,
) -> Result<Vec<(String, u64)>, String> {
    let (start_work_item, start_attempt) = cursor
        .as_ref()
        .map(|(work_item, attempt)| (work_item.as_str(), *attempt))
        .unwrap_or(("", 0));
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table
        .range((graph, start_work_item, start_attempt)..)
        .map_err(|error| error.to_string())?
    {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, work_item, attempt) = key.value();
        if row_graph != graph {
            break;
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_work_item, cursor_attempt)| {
                cursor_work_item == work_item && *cursor_attempt == attempt
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
    table: &mut redb::Table<(&str, &str, u64), &str>,
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
    table: &redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let (start_tenant, start_reservation) = cursor
        .as_ref()
        .map(|(tenant, reservation)| (tenant.as_str(), reservation.as_str()))
        .unwrap_or(("", ""));
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table
        .range((graph, start_tenant, start_reservation)..)
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            break;
        }
        if value.value() != reservation_id {
            return Err("resource tenant index key/value escaped clear scope".into());
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_tenant, cursor_reservation)| {
                cursor_tenant == tenant && cursor_reservation == reservation_id
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

fn clear_resource_tenant_index_table(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<(String, String)> = None;
    loop {
        let keys = collect_resource_tenant_index_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (tenant, reservation_id) in &keys {
            table
                .remove((graph, tenant.as_str(), reservation_id.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

fn collect_resource_anti_affinity_clear_keys(
    table: &redb::Table<(&str, &str, &str), u64>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let (start_host, start_tag) = cursor
        .as_ref()
        .map(|(host, tag)| (host.as_str(), tag.as_str()))
        .unwrap_or(("", ""));
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table
        .range((graph, start_host, start_tag)..)
        .map_err(|error| error.to_string())?
    {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, host, tag) = key.value();
        if row_graph != graph {
            break;
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_host, cursor_tag)| cursor_host == host && cursor_tag == tag)
        {
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
    table: &mut redb::Table<(&str, &str, &str), u64>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<(String, String)> = None;
    loop {
        let keys = collect_resource_anti_affinity_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (host, tag) in &keys {
            table
                .remove((graph, host.as_str(), tag.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

fn clear_resource_reservation_side_tables(
    graph: &str,
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
) -> Result<(), String> {
    clear_resource_two_part_table(reservations, graph)?;
    clear_resource_tenant_index_table(tenant_index, graph)?;
    clear_resource_attempts_table(attempts, graph)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn clear_resource_host_side_tables(
    graph: &str,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
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
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    attempts: &mut redb::Table<(&str, &str, u64), &str>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    exclusivity: &mut redb::Table<(&str, &str), &str>,
    fairness: &mut redb::Table<(&str, &str), &[u8]>,
    concurrency: &mut redb::Table<(&str, &str), u64>,
    anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
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

/// Remove every current ChangeEnvelope projection for a graph inside the caller's
/// open transaction. The immutable MutationBatch/outbox audit ledger is retained;
/// current object/material/governance state cannot leak into a same-name graph.
pub(crate) fn clear_change_material_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let mut envelopes = wtx
        .open_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let envelope_keys: Vec<String> = envelopes
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| key.value().1.to_string())
        .collect();
    for id in envelope_keys {
        envelopes
            .remove((graph, id.as_str()))
            .map_err(|e| e.to_string())?;
    }

    macro_rules! purge_graph_three_part_table {
        ($definition:expr) => {{
            let mut table = wtx.open_table($definition).map_err(|e| e.to_string())?;
            let keys: Vec<(String, String)> = table
                .range((graph, "", "")..)
                .map_err(|e| e.to_string())?
                .filter_map(|row| row.ok())
                .take_while(|(key, _)| key.value().0 == graph)
                .map(|(key, _)| {
                    let (_, tenant, id) = key.value();
                    (tenant.to_string(), id.to_string())
                })
                .collect();
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

    let mut cursors = wtx.open_table(CHANGE_CURSORS).map_err(|e| e.to_string())?;
    let cursor_keys: Vec<(String, String, String)> = cursors
        .range((graph, "", "", "")..)
        .map_err(|e| e.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (_, tenant, source, partition) = key.value();
            (
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
            )
        })
        .collect();
    for (tenant, source, partition) in cursor_keys {
        cursors
            .remove((graph, tenant.as_str(), source.as_str(), partition.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Remove every mutation-authority row `graph`'s PRIOR incarnation owns —
/// its `MUTATION_IDEMPOTENCY` replay keys, `MUTATION_BATCHES`/`MUTATION_OUTBOX`/
/// `MUTATION_OUTBOX_DELIVERY` records, `MUTATION_PROJECTION_CURSOR` watermarks,
/// and the single-row `MUTATION_GRAPH_VERSION`/`MUTATION_FENCE`/
/// `MUTATION_LIFECYCLE_HEAD` entries (D-P0-U04, CONCEPT:EG-KG.storage.kg-kg).
///
/// `Method::DeleteGraph` previously cleared graph/change-material/resource/lane
/// rows (`clear_graph_rows`/`clear_change_material_rows`/`clear_resource_rows`)
/// but INTENTIONALLY left this mutation-authority material behind — the same
/// history a genuinely distinct online-reshard move already purges via
/// `server::persistence::online_reshard::purge_moved_mutation_rows`, which now
/// calls this exact function instead of carrying its own private copy. Left in
/// place, a same-name recreate after key loss/rotation could collide with or
/// attempt to decrypt a PRIOR incarnation's idempotency key, outbox delivery
/// row, projection cursor, or fence — corrupting exactly-once mutation replay
/// for the NEW incarnation. The caller (the `Method::DeleteGraph` commit path in
/// `commit_ops`) invokes this BEFORE it writes the delete operation's own fresh
/// `MUTATION_BATCHES`/`MUTATION_IDEMPOTENCY`/... tombstone record, so the old
/// incarnation's authority is atomically gone in the SAME transaction that
/// records the deletion — never a separate, interruptible pass.
///
/// `MUTATION_IDEMPOTENCY` is keyed `(tenant, graph, idempotency_key) -> batch_id`
/// — `graph` is not the leading key component, so every prior incarnation's
/// batch_id is discovered by a full-table scan filtered on the graph component
/// (there is no cheaper index; DeleteGraph is a rare, deliberate operation).
/// `MUTATION_OUTBOX`/`MUTATION_OUTBOX_DELIVERY` ARE keyed by `batch_id` first,
/// so once the batch_ids are known their rows are removed by a plain
/// `(batch_id, ..)` range scan. Iterator/storage errors are propagated rather
/// than silently skipped: a partially observed authority set must abort the
/// enclosing transaction instead of allowing a recreate to inherit unknown
/// state.
/// Every `MUTATION_IDEMPOTENCY` batch_id recorded for `graph`.  The table is
/// keyed `(tenant, graph, idempotency_key)`, so `graph` is not the leading
/// component and the scan is necessarily full-table.
fn collect_mutation_idempotency_batch_ids(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<Vec<String>, String> {
    let table = wtx
        .open_table(MUTATION_IDEMPOTENCY)
        .map_err(|e| e.to_string())?;
    let mut batch_ids = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (_, row_graph, _) = key.value();
        if row_graph == graph {
            batch_ids.push(value.value().to_string());
        }
    }
    Ok(batch_ids)
}

/// Remove every `MUTATION_IDEMPOTENCY` replay key belonging to `graph`.
fn clear_mutation_idempotency_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let mut table = wtx
        .open_table(MUTATION_IDEMPOTENCY)
        .map_err(|e| e.to_string())?;
    let mut keys = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, _) = row.map_err(|e| e.to_string())?;
        let (tenant, row_graph, idempotency) = key.value();
        if row_graph == graph {
            keys.push((tenant.to_string(), idempotency.to_string()));
        }
    }
    for (tenant, idempotency) in keys {
        table
            .remove((tenant.as_str(), graph, idempotency.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Remove one batch's `MUTATION_BATCHES` record plus its `MUTATION_OUTBOX` and
/// `MUTATION_OUTBOX_DELIVERY` rows.  Each table is opened in its own scope, in
/// the original order, so no two are open at once inside the transaction.
fn clear_mutation_batch_authority_rows(
    wtx: &redb::WriteTransaction,
    batch_id: &str,
) -> Result<(), String> {
    {
        let mut table = wtx
            .open_table(MUTATION_BATCHES)
            .map_err(|e| e.to_string())?;
        table.remove(batch_id).map_err(|e| e.to_string())?;
    }
    {
        let mut table = wtx.open_table(MUTATION_OUTBOX).map_err(|e| e.to_string())?;
        let keys: Vec<u32> = table
            .range((batch_id, 0u32)..)
            .map_err(|e| e.to_string())?
            .filter_map(|row| row.ok())
            .take_while(|(key, _)| key.value().0 == batch_id)
            .map(|(key, _)| key.value().1)
            .collect();
        for ordinal in keys {
            table
                .remove((batch_id, ordinal))
                .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut table = wtx
            .open_table(MUTATION_OUTBOX_DELIVERY)
            .map_err(|e| e.to_string())?;
        let keys: Vec<(u32, String)> = table
            .range((batch_id, 0u32, "")..)
            .map_err(|e| e.to_string())?
            .filter_map(|row| row.ok())
            .take_while(|(key, _)| key.value().0 == batch_id)
            .map(|(key, _)| {
                let (_, ordinal, consumer) = key.value();
                (ordinal, consumer.to_string())
            })
            .collect();
        for (ordinal, consumer) in keys {
            table
                .remove((batch_id, ordinal, consumer.as_str()))
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Remove every `MUTATION_PROJECTION_CURSOR` row belonging to `graph`.
fn clear_mutation_projection_cursor_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let mut table = wtx
        .open_table(MUTATION_PROJECTION_CURSOR)
        .map_err(|e| e.to_string())?;
    let mut keys = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, _) = row.map_err(|e| e.to_string())?;
        let (tenant, row_graph, projection) = key.value();
        if row_graph == graph {
            keys.push((tenant.to_string(), projection.to_string()));
        }
    }
    for (tenant, projection) in keys {
        table
            .remove((tenant.as_str(), graph, projection.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Remove the graph-keyed lifecycle scalars: version, fence and lifecycle head.
fn clear_mutation_graph_scalar_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    {
        let mut table = wtx
            .open_table(MUTATION_GRAPH_VERSION)
            .map_err(|e| e.to_string())?;
        table.remove(graph).map_err(|e| e.to_string())?;
    }
    {
        let mut table = wtx.open_table(MUTATION_FENCE).map_err(|e| e.to_string())?;
        table.remove(graph).map_err(|e| e.to_string())?;
    }
    {
        let mut table = wtx
            .open_table(MUTATION_LIFECYCLE_HEAD)
            .map_err(|e| e.to_string())?;
        table.remove(graph).map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(crate) fn clear_mutation_authority_rows(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let batch_ids = collect_mutation_idempotency_batch_ids(wtx, graph)?;
    clear_mutation_idempotency_rows(wtx, graph)?;
    for batch_id in &batch_ids {
        clear_mutation_batch_authority_rows(wtx, batch_id)?;
    }
    clear_mutation_projection_cursor_rows(wtx, graph)?;
    clear_mutation_graph_scalar_rows(wtx, graph)?;
    Ok(())
}

/// Drop EVERY durable row for `graph` in ONE durable transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same,
/// the tenant-DELETE path). Unlike `clear_graph_rows` (which empties a LIVE graph's
/// data but keeps its `graph_meta` identity), this ALSO removes the `semantic_store`
/// blob and the `graph_meta` row, so the graph ceases to exist durably — a recreate
/// of the same name then starts from a clean slate instead of inheriting the deleted
/// incarnation's rows on a read-through / `load_all`. Lives in the SHARED redb_store
/// so the embedded engine's delete path purges correctly too (CONCEPT:EG-KG.backend.engine-modes).
/// The purge also removes the graph's mutation-authority rows (replay keys,
/// batch/outbox/delivery records, projection cursors, and lifecycle fences), so
/// this whole-graph seam cannot leave state that a same-name recreate inherits.
pub(crate) fn purge_graph_rows(
    db: &Database,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        // nodes/edges/ledger — reuse the same range-scan-and-remove as ClearGraph.
        clear_graph_rows(graph, &mut nodes, &mut edges, &mut ledger)?;
        // semantic store blob (keyed by graph) + the identity row.
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        let _ = semantic.remove(graph).map_err(|e| e.to_string())?;
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        let _ = meta.remove(graph).map_err(|e| e.to_string())?;
        let mut command_sequences = wtx
            .open_table(WORK_ITEM_COMMAND_SEQUENCE)
            .map_err(|e| e.to_string())?;
        command_sequences.remove(graph).map_err(|e| e.to_string())?;

        clear_change_material_rows(&wtx, graph)?;
        // Mutation authority is lifecycle-owned state too.  Keep this shared
        // whole-graph purge aligned with the canonical DeleteGraph commit
        // path: a caller using the embedded/legacy purge seam must not leave
        // replay, outbox, projection, or lifecycle-fence rows behind for a
        // same-name recreate.
        clear_mutation_authority_rows(&wtx, graph)?;
        let mut reservations = wtx
            .open_table(RESOURCE_RESERVATIONS)
            .map_err(|e| e.to_string())?;
        let mut tenant_index = wtx
            .open_table(RESOURCE_RESERVATION_TENANT_INDEX)
            .map_err(|e| e.to_string())?;
        let mut attempts = wtx
            .open_table(RESOURCE_RESERVATION_ATTEMPTS)
            .map_err(|e| e.to_string())?;
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).map_err(|e| e.to_string())?;
        let mut exclusivity = wtx
            .open_table(RESOURCE_EXCLUSIVITY)
            .map_err(|e| e.to_string())?;
        let mut fairness = wtx
            .open_table(RESOURCE_FAIRNESS)
            .map_err(|e| e.to_string())?;
        let mut concurrency = wtx
            .open_table(RESOURCE_CONCURRENCY)
            .map_err(|e| e.to_string())?;
        let mut anti_affinity = wtx
            .open_table(RESOURCE_ANTI_AFFINITY)
            .map_err(|e| e.to_string())?;
        let mut disk_policies = wtx
            .open_table(RESOURCE_DISK_POLICIES)
            .map_err(|e| e.to_string())?;
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
        )?;
        development_lane::clear_native_graph_rows_in_wtx(&wtx, graph, crypto)?;
        capacity_lease::clear_graph_rows(&wtx, graph)?;
        work_item_capability::clear_graph_rows_in_wtx(&wtx, graph)?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

// ── Cross-shard 2PC durable rows (CONCEPT:EG-KG.storage.lane-n-increment) — pure, server-INDEPENDENT ──
// Shared store helpers (mirroring NODES/EDGES/purge_graph_rows): the `Cmd` arms in
// `redb_backend`'s off-reactor writer thread call straight into these.

/// Durably persist one participant group's prepared slice (its own transaction).
pub(crate) fn put_xshard_prepare(
    db: &Database,
    txn_id: &str,
    gid: u64,
    slice: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        // Production MutationBatch parents require the environment data key before
        // entering this path, so their prepare bodies are ciphertext.  Retain the
        // generic no-cipher behavior for low-level in-process Raft harnesses.
        let sealed = crypto.seal(slice);
        let mut t = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
        t.insert((txn_id, gid), sealed.as_ref())
            .map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Read one participant's prepared slice by its exact composite key.
pub(crate) fn get_xshard_prepare(
    db: &Database,
    txn_id: &str,
    gid: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = rtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let sealed = table
        .get((txn_id, gid))
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_vec());
    sealed.map(|value| crypto.unseal(&value)).transpose()
}

/// Durably write the coordinator's decision row (the atomic commit point).
pub(crate) fn put_xshard_decision(
    db: &Database,
    txn_id: &str,
    commit: bool,
    retain_for_parent: bool,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
        let encoded = match (commit, retain_for_parent) {
            (false, false) => 0u8,
            (true, false) => 1u8,
            (false, true) => 2u8,
            (true, true) => 3u8,
        };
        t.insert(txn_id, encoded).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Mark a parent-recoverable 2PC attempt as started but not yet decided.  Value 4
/// is deliberately not COMMIT/ABORT; recovery resolves it by presumed abort while
/// retaining that outcome until the MutationBatch parent is terminal.
pub(crate) fn put_xshard_recoverable_pending(db: &Database, txn_id: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut table = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
        table.insert(txn_id, 4u8).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())
}

/// Clear one participant's prepare record after resolution.
pub(crate) fn clear_xshard_prepare(db: &Database, txn_id: &str, gid: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
        t.remove((txn_id, gid)).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Clear a resolved txn's decision record.
pub(crate) fn clear_xshard_decision(db: &Database, txn_id: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
        t.remove(txn_id).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every in-doubt prepare record `(txn_id, group_id, slice)` for recovery.
pub(crate) fn scan_xshard_prepares(db: &Database, crypto: DurableCrypto<'_>) -> XshardPrepareScan {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        let (txn_id, gid) = k.value();
        out.push((txn_id.to_string(), gid, crypto.unseal(v.value())?));
    }
    Ok(out)
}

/// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
pub(crate) fn get_xshard_decision(db: &Database, txn_id: &str) -> Result<Option<bool>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    let encoded = t
        .get(txn_id)
        .map_err(|e| e.to_string())?
        .map(|value| value.value());
    match encoded {
        None | Some(4) => Ok(None),
        Some(0 | 2) => Ok(Some(false)),
        Some(1 | 3) => Ok(Some(true)),
        Some(_) => Err("corrupt cross-shard decision value".to_string()),
    }
}

/// Whether the decision/pending marker must survive participant recovery until a
/// separate MutationBatch parent receipt is durable.
pub(crate) fn get_xshard_decision_retain(db: &Database, txn_id: &str) -> Result<bool, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let table = rtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    let retain = table
        .get(txn_id)
        .map_err(|e| e.to_string())?
        .map(|value| matches!(value.value(), 2..=4))
        .unwrap_or(false);
    Ok(retain)
}

/// Scan digest-only decision keys for parent-aware startup GC.  No prepared slice
/// or source payload is returned.
pub(crate) fn scan_xshard_decisions(db: &Database) -> XshardDecisionScan {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let table = rtx
        .open_table(XSHARD_DECISION)
        .map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let encoded = value.value();
        let outcome = match encoded {
            0 | 2 => Some(false),
            1 | 3 => Some(true),
            4 => None,
            _ => return Err("corrupt cross-shard decision value".to_string()),
        };
        rows.push((key.value().to_string(), outcome, matches!(encoded, 2..=4)));
    }
    Ok(rows)
}

/// Durably upsert a named materialized view's serialized blob (CONCEPT:EG-KG.storage.feature).
#[cfg(feature = "compute-dist")]
pub(crate) fn put_matview(db: &Database, name: &str, blob: &[u8]) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;
        t.insert(name, blob).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every persisted materialized view `(name, blob)` for reload on boot.
#[cfg(feature = "compute-dist")]
pub(crate) fn scan_matviews(db: &Database) -> Result<Vec<(String, Vec<u8>)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    // A fresh DB may not have the table yet — treat "table missing" as "no views".
    let t = match rtx.open_table(MATVIEWS) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        out.push((k.value().to_string(), v.value().to_vec()));
    }
    Ok(out)
}

/// Durably upsert a PLAN-BACKED matview's serialized definition
/// (CONCEPT:EG-KG.storage.plan-backed-matview). Disjoint table from `put_matview`.
#[cfg(feature = "matview")]
pub(crate) fn put_plan_matview(db: &Database, name: &str, blob: &[u8]) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(PLAN_MATVIEWS).map_err(|e| e.to_string())?;
        t.insert(name, blob).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably delete a plan-backed matview definition. A missing row is a clean no-op.
#[cfg(feature = "matview")]
pub(crate) fn delete_plan_matview(db: &Database, name: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(PLAN_MATVIEWS).map_err(|e| e.to_string())?;
        t.remove(name).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every persisted plan-backed matview `(name, definition-blob)` for reload on boot.
#[cfg(feature = "matview")]
pub(crate) fn scan_plan_matviews(db: &Database) -> Result<Vec<(String, Vec<u8>)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = match rtx.open_table(PLAN_MATVIEWS) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        out.push((k.value().to_string(), v.value().to_vec()));
    }
    Ok(out)
}

/// Durably upsert an incremental matview's operator-state snapshot
/// (CONCEPT:EG-KG.storage.incremental-matview).
#[cfg(feature = "matview")]
pub(crate) fn put_matview_operator_state(
    db: &Database,
    name: &str,
    blob: &[u8],
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx
            .open_table(MATVIEW_OPERATOR_STATE)
            .map_err(|e| e.to_string())?;
        t.insert(name, blob).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably delete an incremental matview's operator-state snapshot (missing = no-op).
#[cfg(feature = "matview")]
pub(crate) fn delete_matview_operator_state(db: &Database, name: &str) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx
            .open_table(MATVIEW_OPERATOR_STATE)
            .map_err(|e| e.to_string())?;
        t.remove(name).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Scan every persisted incremental-matview operator-state snapshot `(name, blob)`.
#[cfg(feature = "matview")]
pub(crate) fn scan_matview_operator_state(db: &Database) -> Result<Vec<(String, Vec<u8>)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = match rtx.open_table(MATVIEW_OPERATOR_STATE) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for kv in t.iter().map_err(|e| e.to_string())? {
        let (k, v) = kv.map_err(|e| e.to_string())?;
        out.push((k.value().to_string(), v.value().to_vec()));
    }
    Ok(out)
}

/// Validate the WorkItem side of every active native reservation before replacing
/// a graph image from a checkpoint.  Resource rows are deliberately preserved by
/// ordinary GraphDump restore, so accepting a dump which omits a linked WorkItem
/// would leave a held claim with no authoritative lifecycle/fence row to release.
/// Keep this check inside the caller's write transaction: any missing, malformed,
/// stale, or policy-mismatched WorkItem aborts the whole checkpoint before graph
/// rows are cleared.  The checkpoint has no caller-supplied clock; the retained
/// reservation timestamp is the lower-bound liveness instant, while subsequent
/// linearizable resource reads/reconciliation re-check current lease expiry.
/// Therefore an expiry after `reserved_at_ms` is intentionally accepted here,
/// even if it is already past by wall-clock time; later expiry/reclaim belongs
/// only to an explicit authoritative transaction carrying `now_ms`.
#[allow(clippy::too_many_arguments)]
const CHECKPOINT_RESOURCE_REFUSAL: &str = "checkpoint resource domain validation failed";

/// Every checkpoint resource-link failure reports the same opaque refusal, so
/// that a dump cannot probe the durable resource domain through error text.
fn checkpoint_resource_refusal() -> String {
    CHECKPOINT_RESOURCE_REFUSAL.to_string()
}

/// Scan this graph's reservation rows, bounded by `MAX_RESOURCE_CLEAR_SCAN`, and
/// return the ones still holding capacity.  The active test is the shared
/// `resource_reservation_row_is_active` predicate -- the same disjunction this
/// scan carried inline.
fn collect_checkpoint_active_reservations(
    graph: &str,
    reservations: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<DurableResourceReservation>, String> {
    let mut scanned = 0usize;
    let mut active_rows = Vec::new();
    for row in reservations
        .range((graph, "")..)
        .map_err(|_| checkpoint_resource_refusal())?
    {
        let (key, value) = row.map_err(|_| checkpoint_resource_refusal())?;
        let (row_graph, reservation_id) = key.value();
        if row_graph != graph {
            break;
        }
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_CLEAR_SCAN {
            return Err(checkpoint_resource_refusal());
        }
        let stored: DurableResourceReservation =
            resource_decode(value.value(), crypto).map_err(|_| checkpoint_resource_refusal())?;
        if stored.record.reservation_id != reservation_id {
            return Err(checkpoint_resource_refusal());
        }
        if !resource_reservation_row_is_active(&stored) {
            continue;
        }
        active_rows.push(stored);
    }
    Ok(active_rows)
}

/// Index only the bounded set of WorkItems linked by active holds.  This is one
/// O(nodes + active-holds) pass over the incoming image instead of an
/// O(nodes * reservations) search, while keeping index allocation tied to the
/// native reservation scan bound rather than graph size.  A duplicate id in the
/// incoming image is a refusal.
fn index_checkpoint_incoming_active<'n>(
    incoming_nodes: &'n [(String, Vec<u8>)],
    active_rows: &[DurableResourceReservation],
) -> Result<std::collections::HashMap<&'n str, &'n [u8]>, String> {
    let active_ids: std::collections::HashSet<String> = active_rows
        .iter()
        .map(|stored| stored.record.work_item_id.clone())
        .collect();
    let mut incoming_active: std::collections::HashMap<&str, &[u8]> =
        std::collections::HashMap::with_capacity(active_ids.len());
    for (id, bytes) in incoming_nodes {
        if active_ids.contains(id)
            && incoming_active
                .insert(id.as_str(), bytes.as_slice())
                .is_some()
        {
            return Err(checkpoint_resource_refusal());
        }
    }
    Ok(incoming_active)
}

/// Validate ONE active hold against the incoming replacement image, not the rows
/// currently in redb.  `clear_graph_rows` runs immediately after this validation,
/// so checking the old table would accidentally approve a dump which then deletes
/// the only linked WorkItem for an active hold.
fn validate_checkpoint_active_reservation(
    graph: &str,
    stored: &DurableResourceReservation,
    incoming_active: &std::collections::HashMap<&str, &[u8]>,
    hosts: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(item_bytes) = incoming_active
        .get(stored.record.work_item_id.as_str())
        .copied()
    else {
        return Err(checkpoint_resource_refusal());
    };
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(item_bytes).map_err(|_| checkpoint_resource_refusal())?;
    let request = resource_request_from_record(&stored.record, stored.record.reserved_at_ms);
    resource_validate_work_item(&props, &request, false)
        .map_err(|_| checkpoint_resource_refusal())?;
    if !resource_record_work_item_live(&props, &stored.record, stored.record.reserved_at_ms) {
        return Err(checkpoint_resource_refusal());
    }

    let (_, extension) =
        resource_metadata_maps(&props).map_err(|_| checkpoint_resource_refusal())?;
    let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)
        .map_err(|_| checkpoint_resource_refusal())?
        .ok_or_else(checkpoint_resource_refusal)?;
    if host.host_ref != stored.record.host_ref
        || host.target_kind != resource_record_target_kind(stored.record.target_kind)
        || host.target_alias != stored.record.target_alias
    {
        return Err(checkpoint_resource_refusal());
    }
    if !resource_target_selection_matches(extension, &host)
        .map_err(|_| checkpoint_resource_refusal())?
    {
        return Err(checkpoint_resource_refusal());
    }
    Ok(())
}

fn validate_checkpoint_resource_links(
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    reservations: &mut redb::Table<(&str, &str), &[u8]>,
    hosts: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let active_rows = collect_checkpoint_active_reservations(graph, reservations, crypto)?;
    let incoming_active = index_checkpoint_incoming_active(incoming_nodes, &active_rows)?;
    for stored in &active_rows {
        validate_checkpoint_active_reservation(graph, stored, &incoming_active, hosts, crypto)?;
    }
    Ok(())
}

/// Snapshot the full registry dump into redb, overwriting each graph's rows, and
/// commit durably. Folds any buffered mutations into the SAME transaction first.
#[allow(clippy::type_complexity)]
fn open_checkpoint_graph_tables<'txn>(
    wtx: &'txn redb::WriteTransaction,
) -> Result<
    (
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str), &'static [u8]>,
        redb::Table<'txn, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
        redb::Table<'txn, (&'static str, u64), &'static str>,
        redb::Table<'txn, &'static str, &'static [u8]>,
        redb::Table<'txn, &'static str, &'static [u8]>,
        redb::Table<'txn, &'static str, u64>,
    ),
    String,
> {
    let nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let native_work_items = wtx
        .open_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let versions = wtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    Ok((
        nodes,
        native_work_items,
        edges,
        ledger,
        semantic,
        meta,
        versions,
    ))
}

#[allow(clippy::too_many_arguments)]
fn apply_checkpoint_pending_operation(
    wtx: &redb::WriteTransaction,
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }) {
        clear_resource_rows(
            graph,
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
        development_lane::clear_native_graph_rows_in_wtx(wtx, graph, crypto)?;
        capacity_lease::clear_graph_rows(wtx, graph)?;
        work_item_capability::clear_graph_rows_in_wtx_with_native(wtx, graph, native_work_items)?;
    }
    apply_method_rows(
        graph,
        method,
        nodes,
        edges,
        ledger,
        semantic,
        native_work_items,
        crypto,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_checkpoint_pending_operations(
    wtx: &redb::WriteTransaction,
    pending: &[(String, Method)],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    resource_attempts: &mut redb::Table<(&str, &str, u64), &str>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    resource_exclusivity: &mut redb::Table<(&str, &str), &str>,
    resource_fairness: &mut redb::Table<(&str, &str), &[u8]>,
    resource_concurrency: &mut redb::Table<(&str, &str), u64>,
    resource_anti_affinity: &mut redb::Table<(&str, &str, &str), u64>,
    resource_disk_policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (graph, method) in pending.iter() {
        apply_checkpoint_pending_operation(
            wtx,
            graph,
            method,
            nodes,
            edges,
            ledger,
            semantic,
            native_work_items,
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
    }
    Ok(())
}

fn checkpoint_dump_current_snapshot_version(
    versions: &redb::Table<&str, u64>,
    graph: &str,
) -> Result<u64, String> {
    Ok(versions
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|value| value.value())
        .unwrap_or(0))
}

// The dump's node/edge/semantic blobs are plaintext (from the live
// GraphCore snapshot) — SEAL them on the way to disk (no-op when
// encryption is off). The ledger lines stay plaintext (operational mirror
// / audit-chain input).
fn validate_and_clear_checkpoint_dump(
    wtx: &redb::WriteTransaction,
    dump: &GraphDump,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let incoming_nodes = dump
        .nodes
        .iter()
        .map(|(node_id, properties)| (node_id.clone(), properties.clone()))
        .collect::<Vec<_>>();
    work_item_capability::validate_snapshot_nodes(&incoming_nodes)?;
    work_item_capability::clear_graph_rows_in_wtx(wtx, &dump.graph)?;
    validate_checkpoint_resource_links(
        &dump.graph,
        &dump.nodes,
        resource_reservations,
        resource_hosts,
        crypto,
    )?;
    let lane_holds = wtx
        .open_table(development_lane::HOLDS)
        .map_err(|e| e.to_string())?;
    development_lane::validate_checkpoint_lane_links(
        &dump.graph,
        &dump.nodes,
        &lane_holds,
        crypto,
    )?;
    Ok(())
}

fn write_checkpoint_dump_nodes(
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    dump: &GraphDump,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (id, props) in &dump.nodes {
        let blob = crypto.seal(props);
        nodes
            .insert((dump.graph.as_str(), id.as_str()), blob.as_ref())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_checkpoint_dump_edges(
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    dump: &GraphDump,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (src, tgt, props) in &dump.edges {
        let ord = next_edge_ordinal(edges, &dump.graph, src, tgt)?;
        let blob = crypto.seal(props);
        edges
            .insert(
                (dump.graph.as_str(), src.as_str(), tgt.as_str(), ord),
                blob.as_ref(),
            )
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_checkpoint_dump_ledger(
    ledger: &mut redb::Table<(&str, u64), &str>,
    dump: &GraphDump,
) -> Result<(), String> {
    for (seq, line) in dump.ledger.iter().enumerate() {
        ledger
            .insert((dump.graph.as_str(), seq as u64), line.as_str())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn write_checkpoint_dump_semantic_and_meta(
    semantic: &mut redb::Table<&str, &[u8]>,
    meta: &mut redb::Table<&str, &[u8]>,
    versions: &mut redb::Table<&str, u64>,
    dump: &GraphDump,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let sem = crypto.seal(&dump.semantic);
    semantic
        .insert(dump.graph.as_str(), sem.as_ref())
        .map_err(|e| e.to_string())?;
    let encoded = encode_meta_record(
        &dump.name,
        dump.graph_type,
        &dump.incarnation_id,
        dump.integrity_policy.as_ref(),
    )?;
    meta.insert(dump.graph.as_str(), encoded.as_slice())
        .map_err(|e| e.to_string())?;
    versions
        .insert(dump.graph.as_str(), dump.source_snapshot_version)
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_checkpoint_dump(
    wtx: &redb::WriteTransaction,
    dump: GraphDump,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    meta: &mut redb::Table<&str, &[u8]>,
    versions: &mut redb::Table<&str, u64>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let current_snapshot_version =
        checkpoint_dump_current_snapshot_version(versions, dump.graph.as_str())?;
    if dump.source_snapshot_version < current_snapshot_version {
        // A checkpoint image must not move the graph authority backwards
        // while native holds remain preserved outside the ordinary dump.
        // Keep the refusal generic so graph identifiers cannot escape via
        // storage errors.
        return Err("checkpoint graph image is stale".to_string());
    }
    validate_and_clear_checkpoint_dump(wtx, &dump, resource_reservations, resource_hosts, crypto)?;
    clear_graph_rows(&dump.graph, nodes, edges, ledger)?;
    // Resource rows are a separate native authority and are not part of
    // an ordinary GraphDump.  Preserve them across checkpoint image
    // replacement; only an explicit, committed ClearGraph/DeleteGraph
    // above may clear them after the drain guard succeeds.
    write_checkpoint_dump_nodes(nodes, &dump, crypto)?;
    write_checkpoint_dump_edges(edges, &dump, crypto)?;
    write_checkpoint_dump_ledger(ledger, &dump)?;
    write_checkpoint_dump_semantic_and_meta(semantic, meta, versions, &dump, crypto)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_checkpoint_dumps(
    wtx: &redb::WriteTransaction,
    graphs: Vec<GraphDump>,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
    semantic: &mut redb::Table<&str, &[u8]>,
    meta: &mut redb::Table<&str, &[u8]>,
    versions: &mut redb::Table<&str, u64>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    resource_hosts: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<usize, String> {
    let mut count = 0usize;
    for dump in graphs {
        apply_checkpoint_dump(
            wtx,
            dump,
            nodes,
            edges,
            ledger,
            semantic,
            meta,
            versions,
            resource_reservations,
            resource_hosts,
            crypto,
        )?;
        count += 1;
    }
    Ok(count)
}

pub(crate) fn apply_checkpoint(
    db: &Database,
    pending: &mut Vec<(String, Method)>,
    graphs: Vec<GraphDump>,
    crypto: DurableCrypto<'_>,
) -> Result<usize, String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let count = {
        let (
            mut nodes,
            mut native_work_items,
            mut edges,
            mut ledger,
            mut semantic,
            mut meta,
            mut versions,
        ) = open_checkpoint_graph_tables(&wtx)?;
        let (
            mut resource_reservations,
            mut resource_tenant_index,
            mut resource_attempts,
            mut resource_hosts,
            mut resource_exclusivity,
        ) = open_native_operation_resource_tables_a(&wtx)?;
        let (
            mut resource_fairness,
            mut resource_concurrency,
            mut resource_anti_affinity,
            mut resource_disk_policies,
        ) = open_native_operation_resource_tables_b(&wtx)?;

        apply_checkpoint_pending_operations(
            &wtx,
            pending,
            &mut nodes,
            &mut edges,
            &mut ledger,
            &mut semantic,
            &mut native_work_items,
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

        drop(native_work_items);

        apply_checkpoint_dumps(
            &wtx,
            graphs,
            &mut nodes,
            &mut edges,
            &mut ledger,
            &mut semantic,
            &mut meta,
            &mut versions,
            &mut resource_reservations,
            &mut resource_hosts,
            crypto,
        )?
    };
    wtx.commit().map_err(|e| e.to_string())?;
    pending.clear();
    Ok(count)
}

/// Read ONE graph's durable rows back into an owned [`GraphDump`] (CONCEPT:EG-KG.storage.100m-tenant —
/// tenant rehydration). Range-scans each table by the `graph` key prefix, so a cold
/// tenant rehydrates from redb without reading the whole store. `None` when the graph
/// has no durable identity (`graph_meta`) row — a genuine absence, not a hibernation.
pub(crate) fn read_graph_dump(
    db: &Database,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<GraphDump>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let meta_record = match meta_table.get(graph).map_err(|e| e.to_string())? {
        Some(v) => decode_meta_record(graph, v.value())?,
        None => return Ok(None),
    };
    let version_table = rtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    let source_snapshot_version = version_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;

    let mut nodes = Vec::new();
    for row in nodes_table
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if g != graph {
            break;
        }
        nodes.push((id.to_string(), crypto.unseal(v.value())?));
    }
    let mut edges = Vec::new();
    for row in edges_table
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        if g != graph {
            break;
        }
        edges.push((s.to_string(), t.to_string(), crypto.unseal(v.value())?));
    }
    let mut ledger = Vec::new();
    for row in ledger_table
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        if k.value().0 != graph {
            break;
        }
        ledger.push(v.value().to_string());
    }
    let semantic = semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
        .unwrap_or_default();

    Ok(Some(GraphDump {
        graph: graph.to_string(),
        name: meta_record.name,
        graph_type: meta_record.graph_type,
        incarnation_id: meta_record.incarnation_id,
        source_snapshot_version,
        integrity_policy: meta_record.integrity_policy,
        nodes,
        edges,
        ledger,
        semantic,
    }))
}

/// One bounded, SOURCE-level page of ONE graph's durable rows (CONCEPT:EG-KG.memory.graph-guided-paging,
/// CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged adjacency"). The paged sibling of
/// [`read_graph_dump`]: instead of collecting the WHOLE graph's node/edge rows into one
/// `Vec` before returning (the thing that makes a lazy first-open of a 10M+-node/token
/// graph spike RAM), this walks the SAME per-graph range scan but stops after `page_size`
/// combined rows and reports whether more remain — so the caller (`RedbBackend`'s
/// `GraphMaterializer::materialize_page` override) never holds more than one page's worth
/// of rows in memory at a time, at the SOURCE, not just when replaying into `GraphCore`.
pub(crate) struct GraphDumpPage {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    /// Only populated on the first page (no keyset cursor) — mirrors
    /// [`eg_core::registry::GraphMaterializer::materialize_page`]'s single-blob
    /// convention so a paged replay attaches the semantic store exactly once.
    pub semantic: Vec<u8>,
    /// Authoritative graph-control state, populated on the first page only.
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub nodes_exhausted: bool,
    pub edges_exhausted: bool,
    /// Effective durable keyset positions after this page. They preserve the
    /// prior value when a page advances only the other row family.
    pub node_after: Option<String>,
    pub edge_after: Option<(String, String, u32)>,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
}

/// A bounded page cursor for [`read_graph_dump_page`] — every argument except
/// the routing `db`/`graph`/`crypto`, borrowed so the caller's owned
/// [`crate::server::persistence::redb_backend::PageQuery`] (or a test literal)
/// need not be cloned just to make this call.
pub(crate) struct PageCursorRef<'a> {
    pub node_offset: usize,
    pub edge_offset: usize,
    pub node_after: Option<&'a str>,
    pub edge_after: Option<(&'a str, &'a str, u32)>,
    pub page_size: usize,
}

fn load_graph_dump_page_meta(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<Option<GraphMetaRecord>, String> {
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let Some(meta_value) = meta_table.get(graph).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    Ok(Some(decode_meta_record(graph, meta_value.value())?))
}

fn read_graph_dump_page_source_version(
    rtx: &redb::ReadTransaction,
    graph: &str,
) -> Result<u64, String> {
    let version_table = rtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;
    Ok(version_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|value| value.value())
        .unwrap_or(0))
}

// Nodes: seek to the last returned composite key, skip that one inclusive row,
// then take at most `page_size` more. This makes a complete paged recovery
// O(N log N/pages + N), rather than restarting at the prefix and skipping an
// ever-growing offset (O(N^2/page_size)). One extra `.next()` after filling the
// page (NOT collected) tells us whether more nodes remain.
#[allow(clippy::type_complexity)]
enum GraphDumpPageNodeRowStep {
    EndOfGraph,
    SkipEqual,
    Pushed(String, Vec<u8>),
}

/// One `(graph, id) -> value` NODES row as the table iterator yields it.
type GraphDumpNodeRow<'a> = redb::Result<(
    redb::AccessGuard<'a, (&'a str, &'a str)>,
    redb::AccessGuard<'a, &'a [u8]>,
)>;

fn graph_dump_page_node_row_step(
    row: GraphDumpNodeRow<'_>,
    graph: &str,
    skip_equal: Option<&str>,
    crypto: DurableCrypto<'_>,
) -> Result<GraphDumpPageNodeRowStep, String> {
    let (k, v) = row.map_err(|e| e.to_string())?;
    let (g, id) = k.value();
    if g != graph {
        // ran off the end of this graph's key range
        return Ok(GraphDumpPageNodeRowStep::EndOfGraph);
    }
    if skip_equal.is_some_and(|cursor| cursor == id) {
        return Ok(GraphDumpPageNodeRowStep::SkipEqual);
    }
    Ok(GraphDumpPageNodeRowStep::Pushed(
        id.to_string(),
        crypto.unseal(v.value())?,
    ))
}

fn graph_dump_page_nodes_peek_exhausted(
    iter: &mut redb::Range<'_, (&str, &str), &[u8]>,
    graph: &str,
) -> Result<bool, String> {
    match iter.next() {
        Some(row) => {
            let (k, _v) = row.map_err(|e| e.to_string())?;
            Ok(k.value().0 != graph)
        }
        None => Ok(true),
    }
}

// Nodes: seek to the last returned composite key, skip that one inclusive row,
// then take at most `page_size` more. This makes a complete paged recovery
// O(N log N/pages + N), rather than restarting at the prefix and skipping an
// ever-growing offset (O(N^2/page_size)). One extra `.next()` after filling the
// page (NOT collected) tells us whether more nodes remain.
#[allow(clippy::type_complexity)]
fn read_graph_dump_page_nodes(
    rtx: &redb::ReadTransaction,
    graph: &str,
    node_after: Option<&str>,
    page_size: usize,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<(String, Vec<u8>)>, bool, Option<String>), String> {
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut nodes = Vec::with_capacity(page_size.min(1024));
    let mut nodes_exhausted = true;
    let mut next_node_after = node_after.map(str::to_string);
    let lower = node_after.unwrap_or("");
    let mut iter = nodes_table
        .range((graph, lower)..)
        .map_err(|e| e.to_string())?;
    let mut skip_equal = node_after;
    while nodes.len() < page_size {
        let Some(row) = iter.next() else { break };
        match graph_dump_page_node_row_step(row, graph, skip_equal, crypto)? {
            GraphDumpPageNodeRowStep::EndOfGraph => break,
            GraphDumpPageNodeRowStep::SkipEqual => {
                skip_equal = None;
                continue;
            }
            GraphDumpPageNodeRowStep::Pushed(id, bytes) => {
                skip_equal = None;
                next_node_after = Some(id.clone());
                nodes.push((id, bytes));
                nodes_exhausted = false; // provisional; corrected by the peek below
            }
        }
    }
    if !nodes_exhausted {
        nodes_exhausted = graph_dump_page_nodes_peek_exhausted(&mut iter, graph)?;
    }
    Ok((nodes, nodes_exhausted, next_node_after))
}

enum GraphDumpPageEdgeRowStep {
    EndOfGraph,
    SkipEqual,
    Pushed(String, String, u32, Vec<u8>),
}

/// One `(graph, source, target, ordinal) -> value` EDGES row as the table
/// iterator yields it.
type GraphDumpEdgeRow<'a> = redb::Result<(
    redb::AccessGuard<'a, (&'a str, &'a str, &'a str, u32)>,
    redb::AccessGuard<'a, &'a [u8]>,
)>;

fn graph_dump_page_edge_row_step(
    row: GraphDumpEdgeRow<'_>,
    graph: &str,
    skip_equal: Option<(&str, &str, u32)>,
    crypto: DurableCrypto<'_>,
) -> Result<GraphDumpPageEdgeRowStep, String> {
    let (k, v) = row.map_err(|e| e.to_string())?;
    let (g, s, t, ordinal) = k.value();
    if g != graph {
        return Ok(GraphDumpPageEdgeRowStep::EndOfGraph);
    }
    if skip_equal.is_some_and(|(cursor_source, cursor_target, cursor_ordinal)| {
        cursor_source == s && cursor_target == t && cursor_ordinal == ordinal
    }) {
        return Ok(GraphDumpPageEdgeRowStep::SkipEqual);
    }
    Ok(GraphDumpPageEdgeRowStep::Pushed(
        s.to_string(),
        t.to_string(),
        ordinal,
        crypto.unseal(v.value())?,
    ))
}

fn graph_dump_page_edges_peek_exhausted(
    iter: &mut redb::Range<'_, (&str, &str, &str, u32), &[u8]>,
    graph: &str,
) -> Result<bool, String> {
    match iter.next() {
        Some(row) => {
            let (k, _v) = row.map_err(|e| e.to_string())?;
            Ok(k.value().0 != graph)
        }
        None => Ok(true),
    }
}

// Edges: only once every node has been paged in (mirrors `apply_material_page`'s
// nodes-before-edges ordering, so a partially-opened graph never has an edge
// dangling on a not-yet-added node), spend the page's remaining budget on edges.
// `should_scan` is exactly "no more node rows remain for this graph AND the page
// still has budget left" (the same nodes-first gate `apply_material_page` uses).
// The returned `bool` tracks whether the EDGE range itself is drained (only
// meaningful once nodes are exhausted); the caller always ANDs it with
// `nodes_exhausted`, so a page that is still working through nodes never
// falsely reports edges done.
#[allow(clippy::type_complexity)]
fn read_graph_dump_page_edges(
    rtx: &redb::ReadTransaction,
    graph: &str,
    edge_after: Option<(&str, &str, u32)>,
    edge_budget: usize,
    should_scan: bool,
    crypto: DurableCrypto<'_>,
) -> Result<
    (
        Vec<(String, String, Vec<u8>)>,
        bool,
        Option<(String, String, u32)>,
    ),
    String,
> {
    let mut edges = Vec::new();
    let mut edges_done_this_call = false;
    let mut next_edge_after = edge_after
        .map(|(source, target, ordinal)| (source.to_string(), target.to_string(), ordinal));
    if !should_scan {
        return Ok((edges, edges_done_this_call, next_edge_after));
    }
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let (lower_source, lower_target, lower_ordinal) = edge_after.unwrap_or(("", "", 0));
    let mut iter = edges_table
        .range((graph, lower_source, lower_target, lower_ordinal)..)
        .map_err(|e| e.to_string())?;
    let mut skip_equal = edge_after;
    edges_done_this_call = true;
    while edges.len() < edge_budget {
        let Some(row) = iter.next() else { break };
        match graph_dump_page_edge_row_step(row, graph, skip_equal, crypto)? {
            GraphDumpPageEdgeRowStep::EndOfGraph => break,
            GraphDumpPageEdgeRowStep::SkipEqual => {
                skip_equal = None;
                continue;
            }
            GraphDumpPageEdgeRowStep::Pushed(s, t, ordinal, bytes) => {
                skip_equal = None;
                next_edge_after = Some((s.clone(), t.clone(), ordinal));
                edges.push((s, t, bytes));
                edges_done_this_call = false;
            }
        }
    }
    if !edges_done_this_call {
        edges_done_this_call = graph_dump_page_edges_peek_exhausted(&mut iter, graph)?;
    }
    Ok((edges, edges_done_this_call, next_edge_after))
}

fn read_graph_dump_page_semantic(
    rtx: &redb::ReadTransaction,
    graph: &str,
    node_after: Option<&str>,
    edge_after: Option<(&str, &str, u32)>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    if node_after.is_some() || edge_after.is_some() {
        return Ok(Vec::new());
    }
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    Ok(semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
        .unwrap_or_default())
}

pub(crate) fn read_graph_dump_page(
    db: &Database,
    graph: &str,
    crypto: DurableCrypto<'_>,
    cursor: PageCursorRef<'_>,
) -> Result<Option<GraphDumpPage>, String> {
    let PageCursorRef {
        node_offset: _node_offset,
        edge_offset: _edge_offset,
        node_after,
        edge_after,
        page_size,
    } = cursor;
    let page_size = page_size.max(1);
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let Some(meta_record) = load_graph_dump_page_meta(&rtx, graph)? else {
        return Ok(None);
    };
    let source_snapshot_version = read_graph_dump_page_source_version(&rtx, graph)?;

    let (nodes, nodes_exhausted, next_node_after) =
        read_graph_dump_page_nodes(&rtx, graph, node_after, page_size, crypto)?;

    let edge_budget = page_size.saturating_sub(nodes.len());
    let should_scan_edges = nodes_exhausted && edge_budget > 0;
    let (edges, edges_done_this_call, next_edge_after) = read_graph_dump_page_edges(
        &rtx,
        graph,
        edge_after,
        edge_budget,
        should_scan_edges,
        crypto,
    )?;
    let edges_exhausted = nodes_exhausted && edges_done_this_call;

    let semantic = read_graph_dump_page_semantic(&rtx, graph, node_after, edge_after, crypto)?;

    let first_page = node_after.is_none() && edge_after.is_none();
    Ok(Some(GraphDumpPage {
        nodes,
        edges,
        semantic,
        integrity_policy: first_page.then_some(meta_record.integrity_policy).flatten(),
        nodes_exhausted,
        edges_exhausted,
        node_after: next_node_after,
        edge_after: next_edge_after,
        incarnation_id: meta_record.incarnation_id,
        source_snapshot_version,
    }))
}

#[cfg(test)]
mod keyset_page_tests {
    use super::*;

    #[test]
    fn durable_decode_rejects_declared_allocation_bomb() {
        let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_durable::<Vec<serde_json::Value>>(&allocation_bomb).is_err());
    }

    #[test]
    fn graph_metadata_requires_the_current_version_and_complete_identity() {
        let policy = crate::graph::IntegrityPolicy {
            shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
        };
        let encoded = encode_meta_record(
            "graph",
            GraphType::Global,
            "incarnation:test:current",
            Some(&policy),
        )
        .unwrap();
        let decoded = decode_meta_record("graph", &encoded).unwrap();
        assert_eq!(decoded.name, "graph");
        assert_eq!(decoded.incarnation_id, "incarnation:test:current");
        assert_eq!(decoded.integrity_policy, Some(policy));

        let unversioned = rmp_serde::to_vec_named(&serde_json::json!({
            "name": "graph",
            "graph_type": GraphType::Global,
            "incarnation_id": "incarnation:test:retired"
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &unversioned).is_err());

        let missing_incarnation = rmp_serde::to_vec_named(&serde_json::json!({
            "schema_version": GRAPH_META_SCHEMA_VERSION,
            "name": "graph",
            "graph_type": GraphType::Global,
            "integrity_policy": null
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &missing_incarnation).is_err());

        let missing_policy = rmp_serde::to_vec_named(&serde_json::json!({
            "schema_version": GRAPH_META_SCHEMA_VERSION,
            "name": "graph",
            "graph_type": GraphType::Global,
            "incarnation_id": "incarnation:test:missing-policy"
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &missing_policy).is_err());
    }

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "eg-keyset-page-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn keyset_pages_recover_every_node_and_parallel_edge_without_prefix_skips() {
        let path = temp_path();
        let db = Database::create(&path).unwrap();
        let wtx = db.begin_write().unwrap();
        {
            wtx.open_table(NODES).unwrap();
            wtx.open_table(EDGES).unwrap();
            wtx.open_table(LEDGER).unwrap();
            wtx.open_table(SEMANTIC).unwrap();
            wtx.open_table(GRAPH_META).unwrap();
            wtx.open_table(MUTATION_GRAPH_VERSION).unwrap();
        }
        wtx.commit().unwrap();

        let nodes: Vec<_> = ["a", "b", "c", "n00", "n01", "n02", "n03"]
            .into_iter()
            .map(|id| (id.to_string(), id.as_bytes().to_vec()))
            .collect();
        let mut edges = Vec::new();
        for ordinal in 0..5u8 {
            edges.push(("a".to_string(), "b".to_string(), vec![ordinal]));
        }
        edges.push(("b".to_string(), "c".to_string(), vec![5]));
        edges.push(("b".to_string(), "c".to_string(), vec![6]));
        apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![GraphDump {
                graph: "graph".to_string(),
                name: "graph".to_string(),
                graph_type: GraphType::Global,
                incarnation_id: "incarnation:test:keyset".to_string(),
                source_snapshot_version: 7,
                integrity_policy: None,
                nodes: nodes.clone(),
                edges: edges.clone(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            }],
            DurableCrypto::none(),
        )
        .unwrap();

        let mut node_after: Option<String> = None;
        let mut edge_after: Option<(String, String, u32)> = None;
        let mut got_nodes = Vec::new();
        let mut got_edges = Vec::new();
        let mut first = true;
        loop {
            let edge_cursor = edge_after
                .as_ref()
                .map(|(source, target, ordinal)| (source.as_str(), target.as_str(), *ordinal));
            // Offsets are deliberately nonsense after page one. Correct recovery
            // must be driven solely by the durable keyset positions.
            let offset = if first { 0 } else { 1_000_000 };
            let page = read_graph_dump_page(
                &db,
                "graph",
                DurableCrypto::none(),
                PageCursorRef {
                    node_offset: offset,
                    edge_offset: offset,
                    node_after: node_after.as_deref(),
                    edge_after: edge_cursor,
                    page_size: 3,
                },
            )
            .unwrap()
            .unwrap();
            assert!(page.nodes.len() + page.edges.len() <= 3);
            got_nodes.extend(page.nodes.iter().map(|(id, _)| id.clone()));
            got_edges.extend(page.edges.iter().cloned());
            node_after = page.node_after;
            edge_after = page.edge_after;
            first = false;
            if page.nodes_exhausted && page.edges_exhausted {
                break;
            }
        }

        assert_eq!(
            got_nodes,
            nodes.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>()
        );
        assert_eq!(got_edges, edges);
        let _ = std::fs::remove_file(path);
    }
}

/// Read the entire store into owned per-graph dumps. Each graph's rows are
/// collected by iterating the whole table once and bucketing by the graph prefix.
pub(crate) fn read_all_dumps(
    db: &Database,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<GraphDump>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let version_table = rtx
        .open_table(MUTATION_GRAPH_VERSION)
        .map_err(|e| e.to_string())?;

    let mut dumps: HashMap<String, GraphDump> = HashMap::new();
    for row in meta_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let graph = k.value().to_string();
        let record = decode_meta_record(&graph, v.value())?;
        let source_snapshot_version = version_table
            .get(graph.as_str())
            .map_err(|e| e.to_string())?
            .map(|value| value.value())
            .unwrap_or(0);
        dumps.insert(
            graph.clone(),
            GraphDump {
                graph,
                name: record.name,
                graph_type: record.graph_type,
                incarnation_id: record.incarnation_id,
                source_snapshot_version,
                integrity_policy: record.integrity_policy,
                nodes: Vec::new(),
                edges: Vec::new(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            },
        );
    }

    for row in nodes_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(g) {
            d.nodes.push((id.to_string(), plain));
        }
    }
    for row in edges_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(g) {
            d.edges.push((s.to_string(), t.to_string(), plain));
        }
    }
    for row in ledger_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, _) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.ledger.push(v.value().to_string());
        }
    }
    for row in semantic_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let plain = crypto.unseal(v.value())?;
        if let Some(d) = dumps.get_mut(k.value()) {
            d.semantic = plain;
        }
    }
    Ok(dumps.into_values().collect())
}

/// Cheap CATALOG-ONLY scan: every graph's identity row `(fname, name, graph_type)`
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — NO node/edge/ledger/semantic table is
/// touched. Booting with millions of persisted graphs costs one sequential scan of
/// small `{name, graph_type}` rows, not `read_all_dumps`'s full per-graph
/// rehydrate. Each returned graph materializes its `GraphCore` lazily on first
/// access via the registry's `GraphMaterializer` seam (which reuses
/// [`read_graph_dump`] to fetch the SAME durable rows this scan skipped).
pub(crate) fn read_all_graph_meta(
    db: &Database,
) -> Result<Vec<(String, String, GraphType, String)>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in meta_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let fname = k.value().to_string();
        let record = decode_meta_record(&fname, v.value())?;
        out.push((fname, record.name, record.graph_type, record.incarnation_id));
    }
    Ok(out)
}

pub(crate) fn encode_meta_with_incarnation(
    name: &str,
    gtype: GraphType,
    incarnation_id: &str,
) -> Result<Vec<u8>, String> {
    encode_meta_record(name, gtype, incarnation_id, None)
}

fn encode_meta_record(
    name: &str,
    graph_type: GraphType,
    incarnation_id: &str,
    integrity_policy: Option<&crate::graph::IntegrityPolicy>,
) -> Result<Vec<u8>, String> {
    if name.trim().is_empty() || incarnation_id.trim().is_empty() {
        return Err("graph metadata identity fields must not be empty".to_string());
    }
    rmp_serde::to_vec_named(&GraphMetaRecord {
        schema_version: GRAPH_META_SCHEMA_VERSION,
        name: name.to_string(),
        graph_type,
        incarnation_id: incarnation_id.to_string(),
        integrity_policy: integrity_policy.cloned(),
    })
    .map_err(|error| format!("encode graph metadata: {error}"))
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphMetaRecord {
    schema_version: u16,
    name: String,
    graph_type: GraphType,
    incarnation_id: String,
    /// Explicit `None` is the current fail-closed unconfigured state. Because
    /// this field has no serde default, an older/incomplete record is rejected.
    #[serde(deserialize_with = "deserialize_required_option")]
    integrity_policy: Option<crate::graph::IntegrityPolicy>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

const GRAPH_META_SCHEMA_VERSION: u16 = 2;

/// The durable `graph_meta` schema version this build writes.
pub(crate) fn graph_meta_schema_version() -> u16 {
    GRAPH_META_SCHEMA_VERSION
}

/// The pre-schema_version `graph_meta` value: a bare msgpack map of exactly
/// `{"name", "graph_type"}` written by every engine build before the versioned
/// record landed.
///
/// Retaining this reader is the ON-DISK MIGRATION exception to No-Legacy — the
/// one case the architecture doc carves out, because a durable store cannot be
/// updated by editing code. Without it a store written by any prior build is
/// permanently unopenable: `GraphMetaRecord` is `deny_unknown_fields` and its
/// `integrity_policy` has no serde default, so a legacy row cannot decode, the
/// catalog load fails, and the engine refuses to start with
/// "durable recovery failed; refusing availability". That is exactly what a 9.9G
/// production store did.
///
/// This is read-old → write-new, not a permanent dual-format reader: every
/// decoded legacy row is rewritten in the current format by
/// [`upgrade_legacy_graph_meta`], so a converted store never takes this path
/// again and this shape can be deleted once no legacy store remains.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyGraphMetaRecord {
    name: String,
    graph_type: GraphType,
}

/// Derive a STABLE incarnation id for a graph recovered from a legacy record.
///
/// Deliberately not [`new_incarnation_id`]: that mixes in the wall clock, so a
/// legacy store would mint a different incarnation on every open. Incarnation
/// identity is what fencing and raft replica agreement are keyed on, so a
/// per-restart value would make a migrated graph look like a new incarnation on
/// each boot and would disagree across replicas converting the same store.
/// Hashing only a fixed domain tag and the graph name makes the upgrade
/// deterministic, idempotent, and identical on every node.
fn legacy_incarnation_id(graph: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-incarnation-legacy-v1\0");
    digest.update(graph.as_bytes());
    format!("{:x}", digest.finalize())[..32].to_string()
}

fn new_incarnation_id(graph: &str) -> String {
    static NEXT_INCARNATION: AtomicU64 = AtomicU64::new(1);
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-incarnation-v1\0");
    digest.update(graph.as_bytes());
    digest.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    digest.update(
        NEXT_INCARNATION
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes(),
    );
    format!("incarnation:durable:{}", hex::encode(digest.finalize()))
}

fn decode_meta_record(graph: &str, blob: &[u8]) -> Result<GraphMetaRecord, String> {
    let record: GraphMetaRecord = match decode_durable::<GraphMetaRecord>(blob) {
        Ok(record) => record,
        // A legacy row cannot decode into the versioned record at all (it has no
        // schema_version/incarnation_id and the struct denies unknown fields), so
        // the fallback is keyed on the decode failing, not on a version compare.
        Err(current_error) => decode_legacy_meta_record(graph, blob)
            .ok_or_else(|| format!("decode graph metadata for {graph}: {current_error}"))?,
    };
    if record.schema_version != GRAPH_META_SCHEMA_VERSION {
        return Err(format!(
            "graph metadata for {graph} has unsupported schema version {}",
            record.schema_version
        ));
    }
    if record.name.trim().is_empty() || record.incarnation_id.trim().is_empty() {
        return Err(format!(
            "graph metadata for {graph} has incomplete identity"
        ));
    }
    Ok(record)
}

/// Lift a legacy `{"name", "graph_type"}` row into the current record shape.
///
/// Returns `None` when the blob is not a legacy record either, so the caller can
/// report the CURRENT format's decode error rather than masking genuine
/// corruption as "not legacy".
///
/// `integrity_policy` becomes `None` — its documented fail-closed unconfigured
/// state, and a faithful reading of a store written before the field existed:
/// no policy was ever configured, so none is asserted.
fn decode_legacy_meta_record(graph: &str, blob: &[u8]) -> Option<GraphMetaRecord> {
    let legacy: LegacyGraphMetaRecord = decode_durable(blob).ok()?;
    if legacy.name.trim().is_empty() {
        return None;
    }
    Some(GraphMetaRecord {
        schema_version: GRAPH_META_SCHEMA_VERSION,
        incarnation_id: legacy_incarnation_id(&legacy.name),
        name: legacy.name,
        graph_type: legacy.graph_type,
        integrity_policy: None,
    })
    .inspect(|_| {
        tracing::info!(
            "graph metadata for {graph} upgraded from the pre-versioned format \
             (integrity policy unconfigured); it is rewritten in the current format"
        )
    })
}

/// Rewrite every legacy `graph_meta` row in `db` in the current format.
///
/// The write-new half of the one-time migration. Runs inside a single redb write
/// transaction so a crash mid-upgrade leaves the store wholly on the old format
/// (still readable by the fallback above) rather than half-converted. Idempotent:
/// rows already in the current format decode on the first attempt and are left
/// untouched, so a second run is a no-op and rewrites nothing.
///
/// Returns the number of rows upgraded.
pub(crate) fn upgrade_legacy_graph_meta(db: &Database) -> Result<usize, String> {
    let stale: Vec<(String, Vec<u8>)> = {
        let rtx = db.begin_read().map_err(|e| e.to_string())?;
        // A store that has never held a graph has no `graph_meta` table yet, and
        // opening a missing table in a READ transaction is an error (a write
        // transaction would create it). A fresh install has nothing to migrate,
        // so treat that as "no legacy rows" rather than failing startup — the
        // migration must never be the reason a new deployment cannot boot.
        let Ok(table) = rtx.open_table(GRAPH_META) else {
            return Ok(0);
        };
        let mut stale = Vec::new();
        for row in table.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let key = k.value().to_string();
            if decode_durable::<GraphMetaRecord>(v.value()).is_ok() {
                continue; // already current
            }
            let Some(record) = decode_legacy_meta_record(&key, v.value()) else {
                // Neither format: leave it. The catalog load reports it properly.
                continue;
            };
            let encoded = encode_meta_record(
                &record.name,
                record.graph_type,
                &record.incarnation_id,
                record.integrity_policy.as_ref(),
            )?;
            stale.push((key, encoded));
        }
        stale
    };
    if stale.is_empty() {
        return Ok(0);
    }
    let count = stale.len();
    let wtx = db.begin_write().map_err(|e| e.to_string())?;
    {
        let mut table = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        for (key, encoded) in stale {
            table
                .insert(key.as_str(), encoded.as_slice())
                .map_err(|e| e.to_string())?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(count)
}

/// Decode the logical identity carried by a raw `graph_meta` row.
///
/// Raft snapshots and online resharding copy this row verbatim.  Consumers must
/// derive the logical name/type from that sole durable authority rather than
/// trusting a second, independently serialized copy that could disagree with it.
pub(crate) fn decode_graph_meta_identity(
    graph: &str,
    blob: &[u8],
) -> Result<(String, GraphType, String), String> {
    let record = decode_meta_record(graph, blob)?;
    Ok((record.name, record.graph_type, record.incarnation_id))
}

#[cfg(test)]
mod graph_meta_migration_tests {
    //! The pre-versioned `graph_meta` format must stay openable, because a
    //! durable store cannot be migrated by shipping new code alone. A production
    //! store written by an earlier build was permanently unopenable without this
    //! path: the engine died with "durable recovery failed; refusing availability".
    use super::*;

    /// The exact bytes a pre-versioned build wrote: `{"name", "graph_type"}`.
    fn legacy_blob(name: &str, gtype: GraphType) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"name": name, "graph_type": gtype})).unwrap()
    }

    fn open(dir: &std::path::Path) -> Database {
        Database::create(dir.join("graph-0.redb")).unwrap()
    }

    fn put(db: &Database, key: &str, blob: &[u8]) {
        let wtx = db.begin_write().unwrap();
        {
            let mut t = wtx.open_table(GRAPH_META).unwrap();
            t.insert(key, blob).unwrap();
        }
        wtx.commit().unwrap();
    }

    #[test]
    fn a_legacy_row_decodes_instead_of_failing_recovery() {
        let record = decode_meta_record("g", &legacy_blob("mygraph", GraphType::Global)).unwrap();
        assert_eq!(record.name, "mygraph");
        assert_eq!(record.schema_version, GRAPH_META_SCHEMA_VERSION);
        // Faithful to a store written before the field existed: nothing was configured.
        assert!(record.integrity_policy.is_none());
        assert!(!record.incarnation_id.trim().is_empty());
    }

    #[test]
    fn a_legacy_incarnation_is_stable_across_calls() {
        // Fencing and raft replica agreement key on incarnation identity, so a
        // per-open value would make a migrated graph look new on every boot and
        // would disagree between replicas converting the same store.
        assert_eq!(legacy_incarnation_id("g"), legacy_incarnation_id("g"));
        assert_ne!(legacy_incarnation_id("g"), legacy_incarnation_id("h"));
        assert_eq!(
            decode_meta_record("g", &legacy_blob("g", GraphType::Global))
                .unwrap()
                .incarnation_id,
            decode_meta_record("g", &legacy_blob("g", GraphType::Global))
                .unwrap()
                .incarnation_id
        );
    }

    #[test]
    fn a_current_row_still_round_trips_unchanged() {
        let encoded = encode_meta_with_incarnation("g", GraphType::Global, "inc-1").unwrap();
        let record = decode_meta_record("g", &encoded).unwrap();
        assert_eq!(record.incarnation_id, "inc-1");
    }

    #[test]
    fn genuine_corruption_still_reports_the_current_format_error() {
        // The fallback must not mask real corruption as "not legacy".
        let error = match decode_meta_record("g", b"\xc1\xc1not-msgpack") {
            Ok(_) => panic!("corrupt graph metadata must not decode"),
            Err(error) => error,
        };
        assert!(error.contains("decode graph metadata for g"), "{error}");
    }

    #[test]
    fn upgrade_rewrites_legacy_rows_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        put(&db, "graph-a", &legacy_blob("graph-a", GraphType::Global));
        put(
            &db,
            "graph-b",
            &encode_meta_with_incarnation("graph-b", GraphType::Global, "inc-b").unwrap(),
        );

        assert_eq!(
            upgrade_legacy_graph_meta(&db).unwrap(),
            1,
            "only the legacy row"
        );
        // Second run rewrites nothing — the migration is genuinely one-time.
        assert_eq!(upgrade_legacy_graph_meta(&db).unwrap(), 0);

        let rows = read_all_graph_meta(&db).unwrap();
        assert_eq!(rows.len(), 2);
        // The converted row now decodes as current WITHOUT the legacy fallback.
        let rtx = db.begin_read().unwrap();
        let table = rtx.open_table(GRAPH_META).unwrap();
        let raw = table.get("graph-a").unwrap().unwrap();
        assert!(decode_durable::<GraphMetaRecord>(raw.value()).is_ok());
    }

    #[test]
    fn upgrade_is_a_no_op_on_a_store_with_no_graph_meta_table() {
        // A fresh install has never written a graph, so the table does not exist.
        // Opening a missing table in a read txn is an error; the migration must
        // not turn that into a startup failure for a brand-new deployment.
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        assert_eq!(upgrade_legacy_graph_meta(&db).unwrap(), 0);
    }

    #[test]
    fn a_whole_legacy_store_recovers_its_catalog() {
        // The end-to-end shape of the production failure: several legacy graphs,
        // none of which the current record can decode.
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        for name in ["alpha", "beta", "gamma"] {
            put(&db, name, &legacy_blob(name, GraphType::Global));
        }
        let rows = read_all_graph_meta(&db).unwrap();
        assert_eq!(rows.len(), 3);
        let mut names: Vec<_> = rows.into_iter().map(|(_, n, _, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }
}

#[cfg(all(test, feature = "security"))]
mod security_tests {
    //! Encryption-at-rest + tamper-evident audit proofs over the durable store
    //! (CONCEPT:EG-KG.sharding.row-level-security), exercised through the SAME `commit_ops`/read/`verify_audit`
    //! the server + embedded engine use.
    use super::*;
    use crate::crypto::ValueCipher;

    fn open_db(dir: &std::path::Path) -> Database {
        let path = dir.join("graph-0.redb");
        let db = Database::create(&path).unwrap();
        let wtx = db.begin_write().unwrap();
        wtx.open_table(NODES).unwrap();
        wtx.open_table(EDGES).unwrap();
        wtx.open_table(LEDGER).unwrap();
        wtx.open_table(SEMANTIC).unwrap();
        wtx.open_table(SEMANTIC).unwrap();
        wtx.open_table(GRAPH_META).unwrap();
        wtx.open_table(AUDIT).unwrap();
        // `read_all_dumps` (used by `encryption_no_plaintext_on_disk_round_trips_and_wrong_key_fails`)
        // opens MUTATION_GRAPH_VERSION on a READ transaction to populate
        // `GraphDump::source_snapshot_version`; unlike a write transaction, a read
        // transaction's `open_table` does not auto-create a missing table, so this
        // minimal test store must create it up front like `initialize_canonical_tables` does.
        wtx.open_table(MUTATION_GRAPH_VERSION).unwrap();
        wtx.commit().unwrap();
        db
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
    fn edge_ords(db: &Database, graph: &str, src: &str, tgt: &str) -> Vec<u32> {
        let rtx = db.begin_read().unwrap();
        let edges = rtx.open_table(EDGES).unwrap();
        edges
            .range((graph, src, tgt, 0u32)..)
            .unwrap()
            .filter_map(|r| r.ok())
            .take_while(|(k, _)| {
                let (g, s, t, _) = k.value();
                g == graph && s == src && t == tgt
            })
            .map(|(k, _)| k.value().3)
            .collect()
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
                let wtx = db.begin_write().unwrap();
                let edges = wtx.open_table(EDGES).unwrap();
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
                        Durability::Immediate,
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
                        Durability::Immediate,
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
            Durability::Immediate,
            crypto,
            &mut tail,
        )
        .unwrap();

        let rtx = db.begin_read().unwrap();
        let nodes = rtx.open_table(NODES).unwrap();
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
                Durability::Immediate,
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
                Durability::Immediate,
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
        {
            let wtx = db.begin_write().unwrap();
            {
                let mut audit = wtx.open_table(AUDIT).unwrap();
                let original = audit.get(("g", 1u64)).unwrap().unwrap().value().to_vec();
                let mut mutated = original.clone();
                let last = mutated.len() - 1;
                mutated[last] ^= 0xFF;
                audit.insert(("g", 1u64), mutated.as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }

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
        let commit_batch = |db: &Database, cache: &mut AuditTailCache, batch: &[(&str, &str)]| {
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
            commit_ops(db, &mut ops, &mut log, Durability::Immediate, crypto, cache).unwrap();
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
        {
            let wtx = db.begin_write().unwrap();
            {
                let mut audit = wtx.open_table(AUDIT).unwrap();
                let orig = audit.get(("g1", 3u64)).unwrap().unwrap().value().to_vec();
                let mut mutated = orig;
                let last = mutated.len() - 1;
                mutated[last] ^= 0xFF;
                audit.insert(("g1", 3u64), mutated.as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }
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
                Durability::Immediate,
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
            let _ = cold_seed_rows_touched_take(); // discard anything the build left
            commit_ops(
                &db,
                &mut restart_ops,
                &mut restart_log,
                Durability::Immediate,
                crypto,
                &mut cold_cache,
            )
            .unwrap();
            let rows_touched = cold_seed_rows_touched_take();

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
        let base = std::env::temp_dir().join(format!(
            "eg-sec-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }
}

#[cfg(test)]
mod mutation_batch_tests {
    use super::*;
    use crate::change_envelope::{
        ChangeCursor, ChangeEnvelope, ContentVersion, ContentVersionPosition, CursorPosition,
        PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
    };
    use crate::mutation_batch::{
        MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
        MutationSurface, MUTATION_BATCH_VERSION,
    };

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "eg-mutation-batch-{tag}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn open(path: &std::path::Path) -> Database {
        let db = Database::create(path).unwrap();
        let wtx = db.begin_write().unwrap();
        initialize_canonical_tables(&wtx).unwrap();
        {
            let mut versions = wtx.open_table(MUTATION_GRAPH_VERSION).unwrap();
            let initialize = versions.get("graph-a").unwrap().is_none();
            if initialize {
                versions.insert("graph-a", 3).unwrap();
            }
        }
        wtx.commit().unwrap();
        db
    }

    /// Reopen an EXISTING fixture database without seeding anything.
    ///
    /// [`open`] deliberately seeds `MUTATION_GRAPH_VERSION["graph-a"] = 3` when
    /// that row is absent, which 47 fixtures depend on. That makes it the wrong
    /// tool for asserting a row was durably DELETED: it re-inserts the very row
    /// under test between the deletion and the assertion, so such a test can only
    /// ever fail — it reports as coverage of durability while being incapable of
    /// observing it.
    fn reopen(path: &std::path::Path) -> Database {
        let db = Database::create(path).unwrap();
        let wtx = db.begin_write().unwrap();
        initialize_canonical_tables(&wtx).unwrap();
        wtx.commit().unwrap();
        db
    }

    fn node(id: &str, value: i64) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": value}))
                .unwrap(),
        }
    }

    fn batch(batch_id: &str, key: &str) -> MutationBatch {
        MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            context: MutationRequestContext {
                request_id: 42,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: Some("crash-test".to_string()),
                policy_fingerprint: Some("policy-v1".to_string()),
                trace_id: Some("trace-1".to_string()),
            },
            tenant: "tenant-a".to_string(),
            graph: "graph-a".to_string(),
            placement_epoch: 7,
            idempotency_key: key.to_string(),
            expected_graph_version: Some(3),
            fencing_token: Some(9),
            authoritative_state: None,
            operations: vec![
                MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Transaction,
                    domain: MutationDomain::GraphRows,
                    method: node("a", 1),
                },
                MutationOperation {
                    ordinal: 1,
                    surface: MutationSurface::Transaction,
                    domain: MutationDomain::GraphRows,
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
        }
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
        claim.expected_graph_version = Some(expected_graph_version);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: MutationDomain::GraphRows,
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
    }

    // Test-only fixture builder; mirrors `native_claim_batch`'s justification
    // above plus the `db` handle it commits the built batch against.
    #[allow(clippy::too_many_arguments)]
    fn commit_native_claim(
        db: &Database,
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
            db,
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
            crate::protocol::ResultPayload::Raw(inner)
            | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
            other => panic!("ClaimWorkItem must return a bin-encoded typed result, got {other:?}"),
        };
        decode_durable(&bytes).unwrap()
    }

    fn public_batch_method(operations: serde_json::Value) -> Method {
        Method::BatchUpdate {
            operations_msgpack: rmp_serde::to_vec_named(&operations).unwrap(),
        }
    }

    fn commit_at(
        db: &Database,
        batch: &MutationBatch,
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            db,
            "graph-a",
            batch,
            None,
            None,
            None,
            Some(&[0x81, 0xa2, b'o', b'k']),
            101,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
            true,
            point,
        )
    }

    fn commit_crossmodal_at(
        db: &Database,
        batch: &MutationBatch,
        methods: &[Method],
        vectors: &[VectorUpsert],
        point: Option<MutationBatchCrashpoint>,
    ) -> Result<MutationBatchCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_mutation_batch_inner(
            db,
            "graph-a",
            batch,
            None,
            None,
            Some(CrossModalBatchRows {
                methods,
                vectors,
                blob_refs: &[],
                measurements: &[],
            }),
            Some(&[0x81, 0xa2, b'o', b'k']),
            101,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
            true,
            point,
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
            read_mutation_batch(&reopened, batch_id, DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&reopened, batch_id, DurableCrypto::none())
                .unwrap()
                .is_empty()
        );
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
    fn missing_graph_version_is_initial_zero_not_caller_seeded() {
        let path = temp_path("missing-version");
        let db = open(&path);
        {
            let wtx = db.begin_write().unwrap();
            wtx.open_table(MUTATION_GRAPH_VERSION)
                .unwrap()
                .remove("graph-a")
                .unwrap();
            wtx.commit().unwrap();
        }
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
            let record = read_mutation_batch(&db, "batch-post", DurableCrypto::none())
                .unwrap()
                .unwrap();
            assert_eq!(record.status, MutationBatchStatus::Committed);
            let outbox = read_mutation_outbox(&db, "batch-post", DurableCrypto::none()).unwrap();
            assert_eq!(
                outbox.len(),
                3,
                "two canonical events + one explicit intent"
            );

            let replay = commit_at(&db, &b, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                read_mutation_outbox(&db, "batch-post", DurableCrypto::none())
                    .unwrap()
                    .len(),
                3
            );
        }
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
            domain: MutationDomain::GraphRows,
            method: public_batch_method(operations),
        }];
        {
            let db = open(&path);
            let committed = commit_at(&db, &initial, None).unwrap();
            assert!(!committed.replayed);
        }
        {
            let db = open(&path);
            let replay = commit_at(&db, &initial, None).unwrap();
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
        removal.expected_graph_version = Some(4);
        removal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: MutationDomain::GraphRows,
            method: public_batch_method(serde_json::json!([
                {"op": "remove_node", "id": "a"}
            ])),
        }];
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

    #[test]
    fn public_batch_upsert_node_merges_durable_fields_across_reopen() {
        let path = temp_path("public-batch-node-upsert");
        let mut seed = batch("batch-upsert-seed", "idem-upsert-seed");
        seed.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: MutationDomain::GraphRows,
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
        let mut upsert = batch("batch-upsert-merge", "idem-upsert-merge");
        upsert.expected_graph_version = Some(4);
        upsert.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: MutationDomain::GraphRows,
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
                domain: MutationDomain::GraphRows,
                method,
            }];
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
                read_mutation_batch(&db, "batch-invalid", DurableCrypto::none())
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("work-1", 3),
        }];
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
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch(
            "work:terminal-stable-batch",
            "work-idem:terminal-stable-key",
        );
        terminal.context.request_id = 777;
        terminal.context.purpose = None;
        terminal.context.policy_fingerprint = None;
        terminal.context.trace_id = None;
        terminal.expected_graph_version = None;
        terminal.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: MutationDomain::ControlPlane,
            method: terminal_method,
        }];
        terminal.outbox[0].key = terminal.batch_id.clone();

        let first = commit_at(&db, &terminal, None).unwrap();
        assert!(!first.replayed);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            Some(6)
        );

        // A fresh transport request is normalized to the same durable request id
        // before this kernel sees it; only the non-identity creation timestamp differs.
        let mut retry = terminal.clone();
        retry.created_at_ms = 200;
        let replay = commit_at(&db, &retry, None).unwrap();
        assert!(replay.replayed);
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            Some(6)
        );

        let mut conflicting_payload = retry.clone();
        let Method::CommitWorkItemResult { result_ref, .. } =
            &mut conflicting_payload.operations[0].method
        else {
            unreachable!();
        };
        *result_ref = Some("result:sha256:different".into());
        let error = commit_at(&db, &conflicting_payload, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));

        let mut conflicting_authority = retry;
        conflicting_authority.context.principal = format!("principal:sha256:{}", "b".repeat(64));
        let error = commit_at(&db, &conflicting_authority, None).unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            Some(6)
        );

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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("work-bundle-1", 3),
        }];
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
            error_ref: None,
            retryable: false,
            now_ms: 1_000,
        };
        let mut terminal = batch("work:bundle-batch", "work-idem:bundle-key");
        terminal.expected_graph_version = None;
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: terminal_method,
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: MutationDomain::GraphSnapshot,
                method: node("trace:bundle-1", 11),
            },
            MutationOperation {
                ordinal: 2,
                surface: MutationSurface::Job,
                domain: MutationDomain::GraphSnapshot,
                method: node("outcome:bundle-1", 22),
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();

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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("work-disallowed-1", 3),
        }];
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
        terminal.expected_graph_version = Some(5);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: terminal_method,
            },
            // Disallowed: only AddNode may ride alongside CommitWorkItemResult.
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: MutationDomain::GraphSnapshot,
                method: Method::RemoveNode {
                    node_id: "work-disallowed-1".into(),
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();

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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("work-double-1", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("work-double-2", 3),
            },
        ];
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
        terminal.expected_graph_version = Some(6);
        terminal.operations = vec![
            MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-1".into(),
                    worker_id: "worker-a".into(),
                    lease_epoch: claimed_1.lease_epoch.unwrap(),
                    fencing_token: claimed_1.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-1".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-one".into()),
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".into(),
                    work_item_id: "work-double-2".into(),
                    worker_id: "worker-b".into(),
                    lease_epoch: claimed_2.lease_epoch.unwrap(),
                    fencing_token: claimed_2.fencing_token.unwrap(),
                    idempotency_key: "double-terminal-key-2".into(),
                    outcome: "succeeded".into(),
                    result_ref: Some("result:sha256:double-two".into()),
                    error_ref: None,
                    retryable: false,
                    now_ms: 1_000,
                },
            },
        ];
        terminal.outbox[0].key = terminal.batch_id.clone();

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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("leased", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
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
        claim.expected_graph_version = Some(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: MutationDomain::GraphRows,
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
        let committed = commit_at(&db, &claim, None).unwrap();
        let payload: crate::protocol::ResultPayload = decode_durable(
            committed
                .record
                .result_msgpack
                .as_deref()
                .expect("claim result"),
        )
        .unwrap();
        // `ResultPayload` is `#[serde(untagged)]` with `PropertiesMsgpack` declared BEFORE
        // `Raw` (both are `serde_bytes` bins), so a round-tripped bin decodes as the FIRST
        // matching bin variant (`PropertiesMsgpack`) — the enum's own doc notes this is by
        // design (the client re-`unpackb`s any top-level bin regardless of variant name).
        // The claim result is therefore the inner bytes under whichever bin variant serde
        // picked; accept either. (Pre-W2.2 rot: this assertion named only `Raw`, which the
        // untagged decoder can never yield for a bin.)
        let bytes = match payload {
            crate::protocol::ResultPayload::Raw(inner)
            | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("live", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("ready", 3),
            },
        ];
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
        claim.expected_graph_version = Some(5);
        claim.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: MutationDomain::GraphRows,
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
            crate::protocol::ResultPayload::Raw(inner)
            | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("exhausted", 3),
        }];
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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("exhausted", 3),
            },
            MutationOperation {
                ordinal: 1,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("runnable", 3),
            },
        ];
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
            domain: MutationDomain::GraphRows,
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("leased", 3),
        }];
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
        renew.expected_graph_version = Some(5);
        renew.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Transaction,
            domain: MutationDomain::GraphRows,
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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("boundary", 3),
            }];
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("cas-a", 3),
        }];
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
            op.expected_graph_version = Some(expected_graph_version);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
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
                crate::protocol::ResultPayload::Raw(inner)
                | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("cas-race", 3),
        }];
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
            op.expected_graph_version = Some(5);
            op.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
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
                crate::protocol::ResultPayload::Raw(inner)
                | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
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
            domain: MutationDomain::GraphRows,
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
            crate::protocol::ResultPayload::Raw(inner)
            | crate::protocol::ResultPayload::PropertiesMsgpack(inner) => inner,
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
                domain: MutationDomain::GraphRows,
                method: ready_work_item_method("cas-restart", 3),
            }];
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
            apply.expected_graph_version = Some(5);
            apply.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
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

        let seed_ready = |db: &Database| {
            let mut seed = batch("wi-seed", "wi-seed-key");
            seed.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
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
            commit_at(db, &seed, None).unwrap();
        };

        let claim_batch = || {
            let mut claim = batch("wi-claim", "wi-claim-key");
            claim.expected_graph_version = Some(4);
            claim.operations = vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::GraphRows,
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
        };

        let read_pair = |db: &Database| -> (Option<String>, Option<String>) {
            match read_one_node(db, "graph-a", "wi", DurableCrypto::none()).unwrap() {
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
            domain: MutationDomain::CrossModal,
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
                read_mutation_batch(&db, "batch-crossmodal", DurableCrypto::none(),)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                read_mutation_outbox(&db, "batch-crossmodal", DurableCrypto::none())
                    .unwrap()
                    .len(),
                2,
            );
            let replay = commit_crossmodal_at(&db, &mutation, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.expected_graph_version,
                Some(3),
                "replay must retain the original OCC observation in the durable identity"
            );

            // A retry reconstructed after the acknowledgement-lost crash may
            // carry the now-current graph version.  It is still the same
            // cross-modal request and must replay without applying rows again.
            let mut rederived = mutation.clone();
            rederived.expected_graph_version = Some(4);
            let replay = commit_crossmodal_at(&db, &rederived, &methods, &vectors, None).unwrap();
            assert!(replay.replayed);
            assert_eq!(
                replay.record.batch.expected_graph_version,
                Some(3),
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

        let leases = claim_mutation_outbox(
            &db,
            "graph-a",
            "projection-worker",
            1_000,
            100,
            10,
            DurableCrypto::none(),
        )
        .unwrap();
        assert_eq!(leases.len(), 3);
        let gap = ack_mutation_outbox(
            &db,
            "graph-a",
            &leases[1],
            "search-index",
            1_001,
            DurableCrypto::none(),
        )
        .unwrap_err();
        assert!(gap.contains("OUTBOX_ORDER_GAP"));

        for lease in &leases {
            ack_mutation_outbox(
                &db,
                "graph-a",
                lease,
                "search-index",
                1_001,
                DurableCrypto::none(),
            )
            .unwrap();
        }
        assert!(claim_mutation_outbox(
            &db,
            "graph-a",
            "projection-worker",
            2_000,
            100,
            10,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_empty());
        let cursor = read_mutation_projection_cursor(
            &db,
            "graph-a",
            "search-index",
            "tenant-a",
            DurableCrypto::none(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(cursor.batch_id, "batch-outbox");
        assert_eq!(cursor.outbox_ordinal, 2);
        assert_eq!(cursor.schema_version, MUTATION_BATCH_VERSION);
        assert_eq!(cursor.version_scope, MutationVersionScope::Graph);
        assert_eq!(cursor.source_graph_version, 4);

        let mut next = batch("batch-outbox-next", "idem-outbox-next");
        next.expected_graph_version = Some(4);
        commit_at(&db, &next, None).unwrap();
        let next_lease = claim_mutation_outbox(
            &db,
            "graph-a",
            "projection-worker",
            2_100,
            100,
            10,
            DurableCrypto::none(),
        )
        .unwrap()
        .remove(0);
        let advanced = ack_mutation_outbox(
            &db,
            "graph-a",
            &next_lease,
            "search-index",
            2_101,
            DurableCrypto::none(),
        )
        .unwrap();
        assert_eq!(advanced.source_graph_version, 5);
        assert!(ack_mutation_outbox(
            &db,
            "graph-a",
            &leases[2],
            "search-index",
            2_102,
            DurableCrypto::none(),
        )
        .unwrap_err()
        .contains("STALE_OUTBOX_LEASE"));

        let first = claim_mutation_outbox(
            &db,
            "graph-a",
            "lease-fence-worker",
            3_000,
            10,
            1,
            DurableCrypto::none(),
        )
        .unwrap()
        .remove(0);
        let replacement = claim_mutation_outbox(
            &db,
            "graph-a",
            "lease-fence-worker",
            3_011,
            10,
            1,
            DurableCrypto::none(),
        )
        .unwrap()
        .remove(0);
        assert!(replacement.lease_epoch > first.lease_epoch);
        assert!(ack_mutation_outbox(
            &db,
            "graph-a",
            &first,
            "secondary-index",
            3_012,
            DurableCrypto::none(),
        )
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
            domain: MutationDomain::GraphSnapshot,
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
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            Some(4)
        );

        let replay = commit_mutation_batch_state(
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
        assert!(replay.replayed);
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
        mutation.expected_graph_version = Some(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: MutationDomain::GraphSnapshot,
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
        mutation.expected_graph_version = Some(4);
        mutation.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: MutationDomain::GraphSnapshot,
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
            "graph-a",
            &mutation,
            None,
            Some(&state),
            None,
            None,
            103,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
            true,
            Some(MutationBatchCrashpoint::BeforeCommit),
        )
        .is_err());

        let dump = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .unwrap();
        assert!(dump.integrity_policy.is_none());
        assert_eq!(
            read_mutation_graph_version(&db, "graph-a").unwrap(),
            Some(4)
        );
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
            domain: MutationDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        commit_at(&db, &create, None).unwrap();
        assert_eq!(
            read_mutation_lifecycle_head(&db, "graph-a")
                .unwrap()
                .as_deref(),
            Some("create-graph-a")
        );
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
        delete.expected_graph_version = Some(4);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: MutationDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        commit_at(&db, &delete, None).unwrap();
        assert_eq!(
            read_mutation_lifecycle_head(&db, "graph-a")
                .unwrap()
                .as_deref(),
            Some("delete-graph-a")
        );
        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert_eq!(
            read_mutation_batch(&db, "delete-graph-a", DurableCrypto::none())
                .unwrap()
                .unwrap()
                .status,
            MutationBatchStatus::Committed
        );
        drop(db);
        let db = open(&path);
        assert_eq!(
            read_mutation_lifecycle_head(&db, "graph-a")
                .unwrap()
                .as_deref(),
            Some("delete-graph-a"),
            "the lifecycle fence must survive restart"
        );
        // D-P0-U04: DeleteGraph now purges the prior incarnation's mutation-
        // authority rows (`clear_mutation_authority_rows`), INCLUDING the old
        // Create's own `MUTATION_IDEMPOTENCY` entry -- so this retry no longer
        // finds a matching idempotency record and never reaches the
        // idempotency-replay STALE_FENCE check (that check only runs when an
        // existing record IS found). The retry is instead rejected by the
        // separate, unconditional optimistic-concurrency guard: `create`'s
        // `expected_graph_version` (3, captured before Delete) can never match
        // the authoritative version once ANY later batch — including the
        // Delete itself — has committed, since the version counter is
        // monotonic and NOT reset by DeleteGraph (only mutation-authority
        // ROWS are cleared, not the version fence, which guards a different,
        // graph-name-scoped invariant). So the resurrection is still
        // deterministically rejected, just by STALE_VERSION rather than
        // STALE_FENCE -- the invariant this test protects (a stale Create can
        // never resurrect graph metadata after Delete) is unchanged.
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
            domain: MutationDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        commit_at(&db, &create, None).unwrap();

        // An ORDINARY (non-lifecycle) content mutation against the live graph --
        // this is what stamps MUTATION_IDEMPOTENCY/MUTATION_BATCHES/MUTATION_OUTBOX
        // for the incarnation being deleted below.
        let mut content = batch("content-batch-1", "content-key-1");
        content.expected_graph_version = Some(4);
        commit_at(&db, &content, None).unwrap();

        // Prove the prior incarnation's mutation authority is actually there
        // before delete (so the assertions below are a real before/after, not
        // a vacuous pass against rows that were never written).
        {
            let rtx = db.begin_read().unwrap();
            let idem = rtx.open_table(MUTATION_IDEMPOTENCY).unwrap();
            assert_eq!(
                idem.get(("tenant-a", "graph-a", "content-key-1"))
                    .unwrap()
                    .map(|v| v.value().to_string()),
                Some("content-batch-1".to_string()),
                "fixture sanity: idempotency row must exist before delete"
            );
            let batches = rtx.open_table(MUTATION_BATCHES).unwrap();
            assert!(batches.get("content-batch-1").unwrap().is_some());
            let outbox = rtx.open_table(MUTATION_OUTBOX).unwrap();
            assert!(outbox.get(("content-batch-1", 0u32)).unwrap().is_some());
        }

        let mut delete = batch("delete-graph-a", "delete-key");
        delete.expected_graph_version = Some(5);
        delete.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: MutationDomain::Lifecycle,
            method: Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            },
        }];
        commit_at(&db, &delete, None).unwrap();

        // The PRIOR incarnation's mutation-authority rows must be gone --
        // this is the D-P0-U04 assertion.
        {
            let rtx = db.begin_read().unwrap();
            let idem = rtx.open_table(MUTATION_IDEMPOTENCY).unwrap();
            assert!(
                idem.get(("tenant-a", "graph-a", "content-key-1"))
                    .unwrap()
                    .is_none(),
                "DeleteGraph must remove the prior incarnation's idempotency row"
            );
            let batches = rtx.open_table(MUTATION_BATCHES).unwrap();
            assert!(
                batches.get("content-batch-1").unwrap().is_none(),
                "DeleteGraph must remove the prior incarnation's MutationBatch record"
            );
            let outbox = rtx.open_table(MUTATION_OUTBOX).unwrap();
            assert!(
                outbox.get(("content-batch-1", 0u32)).unwrap().is_none(),
                "DeleteGraph must remove the prior incarnation's outbox rows"
            );
        }

        // A recreate under the SAME name reusing the SAME idempotency key must
        // be treated as fresh work, not resolved as a replay of the deleted
        // incarnation's stale batch_id.
        // The graph-version counter is a monotonic per-graph-name authority,
        // NOT reset by delete (the delete's own commit above already bumped it
        // to 6: 3 (fixture initial) -> 4 (create) -> 5 (content) -> 6 (delete)).
        // Only the mutation-AUTHORITY rows this fix targets (idempotency/
        // outbox/batches/fence/lifecycle-head) are cleared, so a caller must
        // still supply the next real version to pass optimistic concurrency --
        // clearing mutation authority intentionally does not also erase the
        // version fence, which guards a different invariant (monotonic replay
        // ordering for whichever incarnation currently owns the name).
        let mut recreate = batch("create-graph-a-v2", "create-key-v2");
        recreate.expected_graph_version = Some(6);
        recreate.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Lifecycle,
            domain: MutationDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        commit_at(&db, &recreate, None).unwrap();
        let mut content_v2 = batch("content-batch-1-v2", "content-key-1");
        content_v2.expected_graph_version = Some(7);
        commit_at(&db, &content_v2, None).unwrap();
        {
            let rtx = db.begin_read().unwrap();
            let idem = rtx.open_table(MUTATION_IDEMPOTENCY).unwrap();
            assert_eq!(
                idem.get(("tenant-a", "graph-a", "content-key-1"))
                    .unwrap()
                    .map(|v| v.value().to_string()),
                Some("content-batch-1-v2".to_string()),
                "the reused idempotency key must resolve to the NEW incarnation's \
                 batch, never the deleted incarnation's stale batch_id"
            );
        }

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
            domain: MutationDomain::Lifecycle,
            method: Method::CreateGraph {
                graph_name: "graph-a".to_string(),
                graph_type: GraphType::Agent,
            },
        }];
        commit_at(&db, &create, None).unwrap();

        // The ordinary mutation seeds an independent idempotency/batch/outbox
        // row for the incarnation being purged. The lifecycle batch above also
        // seeds the graph version, fence, and lifecycle-head rows.
        let mut content = batch("content-batch-1", "content-key-1");
        content.expected_graph_version = Some(4);
        commit_at(&db, &content, None).unwrap();

        // Delivery and projection cursor rows are not written by commit_at;
        // seed them explicitly so this fixture covers every table named by
        // clear_mutation_authority_rows, not just the basic replay/outbox set.
        {
            let wtx = db.begin_write().unwrap();
            wtx.open_table(MUTATION_OUTBOX_DELIVERY)
                .unwrap()
                .insert(
                    ("content-batch-1", 0u32, "projection-worker"),
                    &[1u8, 2, 3][..],
                )
                .unwrap();
            wtx.open_table(MUTATION_PROJECTION_CURSOR)
                .unwrap()
                .insert(
                    ("tenant-a", "graph-a", "projection-worker"),
                    &[4u8, 5, 6][..],
                )
                .unwrap();
            wtx.commit().unwrap();
        }

        // The helper is the shared durable whole-graph purge used by both the
        // embedded engine and the persistence writer's PurgeGraph command.
        purge_graph_rows(&db, "graph-a", DurableCrypto::none()).unwrap();

        let rtx = db.begin_read().unwrap();
        assert!(read_all_graph_meta(&db).unwrap().is_empty());
        assert!(rtx
            .open_table(MUTATION_BATCHES)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_IDEMPOTENCY)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_OUTBOX)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_OUTBOX_DELIVERY)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_PROJECTION_CURSOR)
            .unwrap()
            .iter()
            .unwrap()
            .next()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_GRAPH_VERSION)
            .unwrap()
            .get("graph-a")
            .unwrap()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_FENCE)
            .unwrap()
            .get("graph-a")
            .unwrap()
            .is_none());
        assert!(rtx
            .open_table(MUTATION_LIFECYCLE_HEAD)
            .unwrap()
            .get("graph-a")
            .unwrap()
            .is_none());
        drop(rtx);

        // The deletion is durable, not merely visible in the write
        // transaction that performed it.
        drop(db);
        // `reopen`, NOT `open`: `open` re-seeds MUTATION_GRAPH_VERSION["graph-a"]
        // whenever it is missing, which is exactly the row the next assertion
        // requires to be gone.
        let reopened = reopen(&path);
        assert!(read_all_graph_meta(&reopened).unwrap().is_empty());
        assert!(read_mutation_graph_version(&reopened, "graph-a")
            .unwrap()
            .is_none());
        assert!(read_mutation_lifecycle_head(&reopened, "graph-a")
            .unwrap()
            .is_none());
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    fn governed_envelope(batch_id: &str, key: &str, sequence: u64) -> ChangeEnvelope {
        let mut mutation = batch(batch_id, key);
        mutation.expected_graph_version = Some(2 + sequence);
        mutation.operations.truncate(1);
        mutation.outbox[0].payload = rmp_serde::to_vec_named(&serde_json::json!({
            "event": "projection.test"
        }))
        .unwrap();
        let digest = if sequence == 1 { "a" } else { "b" }.repeat(64);
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: format!("envelope-{sequence}"),
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

    fn commit_envelope_at(
        db: &Database,
        envelope: &ChangeEnvelope,
    ) -> Result<ChangeEnvelopeCommit, String> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelope(
            db,
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
        let first = governed_envelope("change-batch-1", "change-key-1", 1);
        let committed = commit_envelope_at(&db, &first).unwrap();
        assert!(!committed.replayed);
        assert_eq!(committed.outbox_count, 3);
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
        assert!(commit_envelope_at(&db, &first).unwrap().replayed);
        assert_eq!(
            read_mutation_outbox(&db, "change-batch-1", DurableCrypto::none())
                .unwrap()
                .len(),
            3,
            "replay must not duplicate an outbox row"
        );

        let second = governed_envelope("change-batch-2", "change-key-2", 2);
        commit_envelope_at(&db, &second).unwrap();
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
        let error = commit_envelope_at(&db, &stale).unwrap_err();
        assert!(
            error.contains("STALE_CONTENT_VERSION"),
            "unexpected stale-envelope rejection: {error}"
        );
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
        mutation.expected_graph_version = Some(3 + index);
        mutation.operations.truncate(1);
        mutation.operations[0].method = node(&format!("n{index}"), index as i64);
        mutation.outbox[0].payload =
            rmp_serde::to_vec_named(&serde_json::json!({ "event": "batch" })).unwrap();
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
        db: &Database,
        envelopes: &[ChangeEnvelope],
    ) -> Result<Vec<ChangeEnvelopeCommit>, ChangeEnvelopesError> {
        #[cfg(feature = "security")]
        let mut audit = AuditTailCache::new();
        commit_change_envelopes(
            db,
            "graph-a",
            envelopes,
            123,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit,
        )
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
        }
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
        // A byte-identical replay of the whole page: every envelope idempotent-skips.
        let replay = commit_envelopes_at(&db, &page).unwrap();
        assert!(replay.iter().all(|commit| commit.replayed));
        assert_eq!(
            read_mutation_outbox(&db, "batch-0", DurableCrypto::none())
                .unwrap()
                .len(),
            3,
            "idempotent replay must not duplicate an outbox row"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn change_envelopes_abort_rolls_back_the_whole_graph_batch() {
        let path = temp_path("change-envelopes-abort");
        let db = open(&path);
        // The second envelope fails its content-version check (previous digest on a
        // fresh object) — the whole atomic graph-batch must roll back.
        let mut bad = governed_envelope_seq(1);
        bad.content_version.previous_digest = Some("a".repeat(64));
        let page = vec![governed_envelope_seq(0), bad];

        let error = commit_envelopes_at(&db, &page).unwrap_err();
        assert_eq!(error.index, 1);
        assert!(
            error.error.contains("STALE_CONTENT_VERSION"),
            "{}",
            error.error
        );
        // NOTHING committed: the first (valid) envelope rolled back with the batch.
        assert!(read_one_node(&db, "graph-a", "n0", DurableCrypto::none())
            .unwrap()
            .is_none());
        assert!(
            read_change_envelope(&db, "graph-a", "env-0", DurableCrypto::none())
                .unwrap()
                .is_none()
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
            domain: MutationDomain::GraphRows,
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
        defer.expected_graph_version = None;
        defer.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: MutationDomain::ControlPlane,
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
            domain: MutationDomain::GraphRows,
            method: ready_work_item_method("work-cancel", 3),
        }];
        commit_at(&db, &seed, None).unwrap();

        let mut cancel = batch("work-item-cancel-op", "work-item-cancel-op-key");
        cancel.expected_graph_version = None;
        cancel.operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: MutationDomain::ControlPlane,
            method: Method::CancelWorkItem {
                tenant: "tenant-a".into(),
                work_item_id: "work-cancel".into(),
                idempotency_key: "cancel-key".into(),
                reason_ref: Some("reason:sha256:two".into()),
                now_ms: 1_000,
            },
        }];
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
        // which a different idempotency_key deliberately bypasses.
        let mut cancel_again = cancel.clone();
        cancel_again.batch_id = "work-item-cancel-op-2".into();
        cancel_again.idempotency_key = "work-item-cancel-op-2-key".into();
        cancel_again.outbox[0].key = cancel_again.batch_id.clone();
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
}
