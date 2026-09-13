use super::store_prelude::*;
use super::*;

pub(crate) fn decode_durable<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(bytes, durable_msgpack_limits())
        .map_err(|_| "durable value is invalid or exceeds resource limits".to_string())
}

pub(crate) fn decode_mutation_batch_record(
    bytes: &[u8],
    expected_graph_fname: &str,
    expected_batch_id: &str,
) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_durable(bytes)?;
    record.validate()?;
    validate_graph_mutation_record_binding(&record, expected_graph_fname, expected_batch_id)?;
    Ok(record)
}

pub(crate) fn validate_graph_mutation_record_binding(
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

pub(crate) fn decode_mutation_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_durable(bytes)?;
    record.validate()?;
    if !matches!(record.committed_version, CommittedVersion::Graph { source, .. } if source != 0) {
        return Err("graph mutation store contains a non-graph outbox record".to_string());
    }
    Ok(record)
}

pub(crate) fn decode_mutation_projection_cursor(
    bytes: &[u8],
) -> Result<MutationProjectionCursor, String> {
    let cursor: MutationProjectionCursor = decode_durable(bytes)?;
    cursor.validate()?;
    if !matches!(cursor.committed_version, CommittedVersion::Graph { source, .. } if source != 0) {
        return Err("graph mutation store contains a non-graph projection cursor".to_string());
    }
    Ok(cursor)
}

pub(crate) fn validate_graph_mutation_record_sizes(
    plaintext: &[u8],
    stored: &[u8],
) -> Result<(), String> {
    if plaintext.len() > MAX_DURABLE_MSGPACK_BYTES {
        return Err("graph mutation record exceeds durable resource limits".to_string());
    }
    if stored.len() > MAX_DURABLE_STORED_BYTES {
        return Err("sealed graph mutation record exceeds durable resource limits".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct DurableOutboxDelivery {
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
pub(crate) struct DurableResourceReservation {
    pub(crate) record: ResourceReservationRecord,
    pub(crate) held_cpu_weight: u64,
    pub(crate) held_memory_mib: u64,
    pub(crate) held_disk_mib: u64,
    pub(crate) held_process_slots: u64,
    pub(crate) fairness_debt: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DurableResourceHost {
    pub(crate) tenant_ref: String,
    pub(crate) host_ref: String,
    pub(crate) revision: u64,
    pub(crate) capacity: ResourceCapacity,
    pub(crate) observed: ResourceCapacity,
    pub(crate) heartbeat_at_ms: u64,
    pub(crate) heartbeat_ttl_ms: u64,
    pub(crate) now_ms: u64,
    pub(crate) draining: bool,
    pub(crate) quarantined: bool,
    pub(crate) labels: Vec<String>,
    pub(crate) target_kind: String,
    pub(crate) target_alias: Option<String>,
    pub(crate) disk_used_mib: u64,
    pub(crate) disk_capacity_mib: u64,
    pub(crate) held_cpu_weight: u64,
    pub(crate) held_memory_mib: u64,
    pub(crate) held_disk_mib: u64,
    pub(crate) held_process_slots: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct DurableResourceFairness {
    pub(crate) debt: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DurableResourceDiskPolicy {
    pub(crate) blocked: bool,
    pub(crate) low_watermark_mib: Option<u64>,
    pub(crate) high_watermark_mib: Option<u64>,
    pub(crate) revision: u64,
}

pub(crate) fn resource_reservation_host_capacity_snapshot(
    value: &ResourceCapacity,
) -> ResourceReservationHostCapacitySnapshot {
    ResourceReservationHostCapacitySnapshot {
        cpu_weight: value.cpu_weight,
        memory_mib: value.memory_mib,
        disk_mib: value.disk_mib,
        process_slots: value.process_slots,
    }
}

pub(crate) fn resource_host_update_capacity_snapshot(
    value: &ResourceCapacity,
) -> ResourceHostUpdateCapacitySnapshot {
    ResourceHostUpdateCapacitySnapshot {
        cpu_weight: value.cpu_weight,
        memory_mib: value.memory_mib,
        disk_mib: value.disk_mib,
        process_slots: value.process_slots,
    }
}

pub(crate) fn resource_reservation_disk_policy_snapshot(
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

pub(crate) fn resource_host_update_disk_policy_snapshot(
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

pub(crate) fn resource_reservation_host_snapshot(
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

pub(crate) fn resource_host_update_snapshot(
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

pub(crate) fn resource_collect_disk_policy_rows(
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
pub(crate) fn retire_scoped_table<K, V>(
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
    pub(crate) fn seal<'b>(&self, plaintext: &'b [u8]) -> Cow<'b, [u8]> {
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
