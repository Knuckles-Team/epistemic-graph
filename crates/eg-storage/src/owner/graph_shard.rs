//! The graph shard's closed physical table census.
//!
//! `graph-N.redb` is the engine's authoritative graph store. One shard file
//! hosts many graphs, so its serving scope is a [`MutationScope::Graph`] rather
//! than a native domain scope: a graph scope binds to the shard file of its
//! graph, and `OwnerLayout::accepts` admits it because the layout's declared
//! domain (`DurabilityDomain::GraphRows`) is graph-authoritative and may own no
//! native scope of its own.
//!
//! The census here is exact and is the manifest contract. Two deliberate
//! omissions, each with its reason:
//!
//! * The eight `mutation_*` tables the shard declared for its own private
//!   admit/idempotency/OCC/fence/outbox/projection ledger are **not** here.
//!   RF-RULING-004 makes `eg-transaction::MutationKernel` the sole mutation
//!   owner, so declaring them as owner tables would put a second mutation
//!   ledger in a kernel-owned file. They retire onto the kernel ledger and are
//!   quarantined by name, not re-declared.
//! * Every table is declared unconditionally, with no `cfg` gate, even though
//!   `audit_chain`, `provenance_anchor_members`, `matviews`, `plan_matviews`
//!   and `matview_operator_state` are written only under `security`,
//!   `compute-dist` or `matview`. The physical registry is the file's format
//!   identity: a shard written by an all-features build must stay openable by a
//!   narrower one, and `validate_table_census` is exact equality, so a
//!   feature-dependent census would make the same file valid or invalid
//!   depending on the reader's build.
//!
//! The three `series_*` tables are declared here as well as on
//! `OwnerLayout::TimeSeries`: a cross-modal atomic commit writes measurements
//! into the SAME shard transaction as the graph, vector and blob modalities
//! (CONCEPT:EG-KG.backend.cross-modal-atomic-commit), so the shard layout must
//! declare them for `eg-tsdb`'s `GraphShardSeriesOwner` seam to reach them.

use crate::physical::manifest::TableScope;
use redb::TableDefinition;

// -- graph rows -----------------------------------------------------------
pub(crate) const NODES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("nodes");
pub(crate) const EDGES: TableDefinition<'static, (&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("edges");
pub(crate) const LEDGER: TableDefinition<'static, (&str, u64), &str> =
    TableDefinition::new("ledger");
pub(crate) const SEMANTIC_STORE: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("semantic_store");
pub(crate) const AUDIT_CHAIN: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("audit_chain");
pub(crate) const PROVENANCE_ANCHOR_MEMBERS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("provenance_anchor_members");
pub(crate) const GRAPH_META: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("graph_meta");
pub(crate) const WORK_ITEM_COMMAND_SEQUENCE: TableDefinition<'static, &str, u64> =
    TableDefinition::new("work_item_command_sequence");

// -- resource reservation -------------------------------------------------
pub(crate) const RESOURCE_RESERVATIONS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("resource_reservations");
pub(crate) const RESOURCE_RESERVATION_TENANT_INDEX:
    TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("resource_reservation_tenant_index");
pub(crate) const RESOURCE_RESERVATION_ATTEMPTS:
    TableDefinition<'static, (&str, &str, u64), &str> =
    TableDefinition::new("resource_reservation_attempts");
pub(crate) const RESOURCE_HOSTS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("resource_hosts");
pub(crate) const RESOURCE_EXCLUSIVITY: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("resource_exclusivity");
pub(crate) const RESOURCE_FAIRNESS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("resource_fairness");
pub(crate) const RESOURCE_CONCURRENCY: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("resource_concurrency");
pub(crate) const RESOURCE_ANTI_AFFINITY: TableDefinition<'static, (&str, &str, &str), u64> =
    TableDefinition::new("resource_anti_affinity");
pub(crate) const RESOURCE_DISK_POLICIES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("resource_disk_policies");

// -- change feed ----------------------------------------------------------
pub(crate) const CHANGE_ENVELOPES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("change_envelopes");
pub(crate) const CONTENT_VERSIONS: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("content_versions");
pub(crate) const CHANGE_CURSORS: TableDefinition<'static, (&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("change_cursors");
pub(crate) const CHANGE_BLOBS: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("change_blobs");
pub(crate) const CHANGE_FEATURES: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("change_features");
pub(crate) const CHANGE_EVIDENCE: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("change_evidence");
pub(crate) const CHANGE_POLICIES: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("change_policies");
pub(crate) const CHANGE_LINEAGE: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("change_lineage");

// -- consensus and replication (store-private: keyed by group or txn) ------
pub(crate) const RAFT_LOG: TableDefinition<'static, (u64, u64), &[u8]> =
    TableDefinition::new("raft_log");
pub(crate) const RAFT_META: TableDefinition<'static, (u64, &str), &[u8]> =
    TableDefinition::new("raft_meta");
pub(crate) const XSHARD_PREPARE: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("xshard_prepare");
pub(crate) const XSHARD_DECISION: TableDefinition<'static, &str, u8> =
    TableDefinition::new("xshard_decision");

// -- materialized views (store-private: keyed by view name) ----------------
pub(crate) const MATVIEWS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("matviews");
pub(crate) const PLAN_MATVIEWS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("plan_matviews");
pub(crate) const MATVIEW_OPERATOR_STATE: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("matview_operator_state");

// -- capacity leases ------------------------------------------------------
pub(crate) const CAPACITY_CELLS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("capacity_cells");
pub(crate) const CAPACITY_LEASES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("capacity_leases");
pub(crate) const CAPACITY_USAGE: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("capacity_usage");
pub(crate) const CAPACITY_IDEMPOTENCY: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("capacity_idempotency");

// -- work-item claim capability -------------------------------------------
pub(crate) const WORK_ITEM_CLAIM_CAPABILITIES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capabilities");
pub(crate) const WORK_ITEM_CLAIM_CAPABILITY_INVOCATIONS:
    TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capability_invocations");
pub(crate) const NATIVE_WORK_ITEM_AUTHORITY: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("native_work_item_authority");

// -- development lane -----------------------------------------------------
pub(crate) const DEVELOPMENT_LANE_HOLDS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("development_lane_holds");
pub(crate) const DEVELOPMENT_LANE_TENANT_INDEX:
    TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("development_lane_tenant_index");
pub(crate) const DEVELOPMENT_LANE_LANE_INDEX:
    TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("development_lane_lane_index");
pub(crate) const DEVELOPMENT_LANE_REPOSITORY_BRANCH_INDEX:
    TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("development_lane_repository_branch_index");
pub(crate) const DEVELOPMENT_LANE_WORKTREE_INDEX: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("development_lane_worktree_index");
pub(crate) const DEVELOPMENT_LANE_WORK_ITEM_INDEX:
    TableDefinition<'static, (&str, &str, u64), &str> =
    TableDefinition::new("development_lane_work_item_index");
pub(crate) const DEVELOPMENT_LANE_COUNTERS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("development_lane_counters");
pub(crate) const DEVELOPMENT_LANE_PRESSURE_INDEX:
    TableDefinition<'static, (&str, &str, &str, &str, u64, &str), u8> =
    TableDefinition::new("development_lane_pressure_index");
pub(crate) const DEVELOPMENT_LANE_POLICIES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("development_lane_policies");
pub(crate) const DEVELOPMENT_LANE_INVOCATIONS:
    TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("development_lane_invocations");

// -- encryption-at-rest key binding ---------------------------------------
pub(crate) const ENCRYPTION_CANARY: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("encryption_canary");

// -- cross-modal series rows (also declared by `OwnerLayout::TimeSeries`) --
pub(crate) const SERIES_CHUNKS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("series_chunks");
pub(crate) const SERIES_META: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("series_meta");
pub(crate) const SERIES_PROJECTION_STATE: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("series_projection_state");

/// Visit every declared graph-shard table exactly once, in census order.
macro_rules! visit_graph_shard_tables {
    ($visit:ident) => {{
        use $crate::owner::graph_shard as shard;
        $visit!(shard::NODES);
        $visit!(shard::EDGES);
        $visit!(shard::LEDGER);
        $visit!(shard::SEMANTIC_STORE);
        $visit!(shard::AUDIT_CHAIN);
        $visit!(shard::PROVENANCE_ANCHOR_MEMBERS);
        $visit!(shard::GRAPH_META);
        $visit!(shard::WORK_ITEM_COMMAND_SEQUENCE);
        $visit!(shard::RESOURCE_RESERVATIONS);
        $visit!(shard::RESOURCE_RESERVATION_TENANT_INDEX);
        $visit!(shard::RESOURCE_RESERVATION_ATTEMPTS);
        $visit!(shard::RESOURCE_HOSTS);
        $visit!(shard::RESOURCE_EXCLUSIVITY);
        $visit!(shard::RESOURCE_FAIRNESS);
        $visit!(shard::RESOURCE_CONCURRENCY);
        $visit!(shard::RESOURCE_ANTI_AFFINITY);
        $visit!(shard::RESOURCE_DISK_POLICIES);
        $visit!(shard::CHANGE_ENVELOPES);
        $visit!(shard::CONTENT_VERSIONS);
        $visit!(shard::CHANGE_CURSORS);
        $visit!(shard::CHANGE_BLOBS);
        $visit!(shard::CHANGE_FEATURES);
        $visit!(shard::CHANGE_EVIDENCE);
        $visit!(shard::CHANGE_POLICIES);
        $visit!(shard::CHANGE_LINEAGE);
        $visit!(shard::RAFT_LOG);
        $visit!(shard::RAFT_META);
        $visit!(shard::XSHARD_PREPARE);
        $visit!(shard::XSHARD_DECISION);
        $visit!(shard::MATVIEWS);
        $visit!(shard::PLAN_MATVIEWS);
        $visit!(shard::MATVIEW_OPERATOR_STATE);
        $visit!(shard::CAPACITY_CELLS);
        $visit!(shard::CAPACITY_LEASES);
        $visit!(shard::CAPACITY_USAGE);
        $visit!(shard::CAPACITY_IDEMPOTENCY);
        $visit!(shard::WORK_ITEM_CLAIM_CAPABILITIES);
        $visit!(shard::WORK_ITEM_CLAIM_CAPABILITY_INVOCATIONS);
        $visit!(shard::NATIVE_WORK_ITEM_AUTHORITY);
        $visit!(shard::DEVELOPMENT_LANE_HOLDS);
        $visit!(shard::DEVELOPMENT_LANE_TENANT_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_LANE_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_REPOSITORY_BRANCH_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_WORKTREE_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_WORK_ITEM_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_COUNTERS);
        $visit!(shard::DEVELOPMENT_LANE_PRESSURE_INDEX);
        $visit!(shard::DEVELOPMENT_LANE_POLICIES);
        $visit!(shard::DEVELOPMENT_LANE_INVOCATIONS);
        $visit!(shard::ENCRYPTION_CANARY);
        $visit!(shard::SERIES_CHUNKS);
        $visit!(shard::SERIES_META);
        $visit!(shard::SERIES_PROJECTION_STATE);
    }};
}
pub(crate) use visit_graph_shard_tables;

/// The census, in the same order the visit macro walks it.
pub(crate) const GRAPH_SHARD_TABLES: &[&str] = &[
    "nodes",
    "edges",
    "ledger",
    "semantic_store",
    "audit_chain",
    "provenance_anchor_members",
    "graph_meta",
    "work_item_command_sequence",
    "resource_reservations",
    "resource_reservation_tenant_index",
    "resource_reservation_attempts",
    "resource_hosts",
    "resource_exclusivity",
    "resource_fairness",
    "resource_concurrency",
    "resource_anti_affinity",
    "resource_disk_policies",
    "change_envelopes",
    "content_versions",
    "change_cursors",
    "change_blobs",
    "change_features",
    "change_evidence",
    "change_policies",
    "change_lineage",
    "raft_log",
    "raft_meta",
    "xshard_prepare",
    "xshard_decision",
    "matviews",
    "plan_matviews",
    "matview_operator_state",
    "capacity_cells",
    "capacity_leases",
    "capacity_usage",
    "capacity_idempotency",
    "work_item_claim_capabilities",
    "work_item_claim_capability_invocations",
    "native_work_item_authority",
    "development_lane_holds",
    "development_lane_tenant_index",
    "development_lane_lane_index",
    "development_lane_repository_branch_index",
    "development_lane_worktree_index",
    "development_lane_work_item_index",
    "development_lane_counters",
    "development_lane_pressure_index",
    "development_lane_policies",
    "development_lane_invocations",
    "encryption_canary",
    "series_chunks",
    "series_meta",
    "series_projection_state",
];

/// The eight tables of the shard's retired private mutation ledger.
///
/// Test-only for now: it is the census the registry test asserts is absent from
/// every layout. It becomes a production quarantine entry when the shard's
/// ledger code is deleted and these names join `RETIRED_PROTOTYPE_TABLES`.
///
/// RF-RULING-004 gives `eg-transaction::MutationKernel` sole ownership of
/// admission, idempotency, ordering, fencing, outbox and projection cursors, so
/// these are not owner tables of any layout. They are named here so the
/// quarantine list and its test have one source.
#[cfg(test)]
pub(crate) const RETIRED_SHARD_LEDGER_TABLES: &[&str] = &[
    "mutation_batches",
    "mutation_idempotency",
    "mutation_outbox",
    "mutation_lifecycle_head",
    "mutation_graph_version",
    "mutation_fence",
    "mutation_outbox_delivery",
    "mutation_projection_cursor",
];

/// Redb key type for one graph-shard table, or `None` when the name is not one.
pub(crate) fn key_type(name: &str) -> Option<&'static str> {
    match name {
        "semantic_store" | "graph_meta" | "work_item_command_sequence" | "xshard_decision"
        | "matviews" | "plan_matviews" | "matview_operator_state" | "encryption_canary" => {
            Some("&str")
        }
        "edges" => Some("(&str,&str,&str,u32)"),
        "ledger" | "audit_chain" | "provenance_anchor_members" | "xshard_prepare" => {
            Some("(&str,u64)")
        }
        "raft_log" => Some("(u64,u64)"),
        "raft_meta" => Some("(u64,&str)"),
        "change_cursors" => Some("(&str,&str,&str,&str)"),
        "development_lane_pressure_index" => Some("(&str,&str,&str,&str,u64,&str)"),
        "resource_reservation_attempts" | "development_lane_work_item_index" => {
            Some("(&str,&str,u64)")
        }
        "resource_reservation_tenant_index"
        | "resource_anti_affinity"
        | "content_versions"
        | "change_blobs"
        | "change_features"
        | "change_evidence"
        | "change_policies"
        | "change_lineage"
        | "capacity_idempotency"
        | "development_lane_tenant_index"
        | "development_lane_lane_index"
        | "development_lane_repository_branch_index"
        | "development_lane_invocations" => Some("(&str,&str,&str)"),
        "nodes"
        | "resource_reservations"
        | "resource_hosts"
        | "resource_exclusivity"
        | "resource_fairness"
        | "resource_concurrency"
        | "resource_disk_policies"
        | "change_envelopes"
        | "capacity_cells"
        | "capacity_leases"
        | "capacity_usage"
        | "work_item_claim_capabilities"
        | "work_item_claim_capability_invocations"
        | "native_work_item_authority"
        | "development_lane_holds"
        | "development_lane_counters"
        | "development_lane_policies"
        | "development_lane_worktree_index" => Some("(&str,&str)"),
        _ => None,
    }
}

/// Redb value type for one graph-shard table, or `None` when the name is not one.
pub(crate) fn value_type(name: &str) -> Option<&'static str> {
    match name {
        "work_item_command_sequence" | "resource_concurrency" | "resource_anti_affinity" => {
            Some("u64")
        }
        "xshard_decision" | "development_lane_pressure_index" => Some("u8"),
        "ledger"
        | "resource_reservation_tenant_index"
        | "resource_reservation_attempts"
        | "resource_exclusivity"
        | "development_lane_tenant_index"
        | "development_lane_lane_index"
        | "development_lane_repository_branch_index"
        | "development_lane_worktree_index"
        | "development_lane_work_item_index" => Some("&str"),
        name if GRAPH_SHARD_TABLES.contains(&name) => Some("&[u8]"),
        _ => None,
    }
}

/// Logical value codec for one graph-shard table.
pub(crate) fn logical_codec(name: &str) -> Option<&'static str> {
    match name {
        "work_item_command_sequence"
        | "resource_concurrency"
        | "resource_anti_affinity"
        | "xshard_decision"
        | "development_lane_pressure_index"
        | "ledger"
        | "resource_reservation_tenant_index"
        | "resource_reservation_attempts"
        | "resource_exclusivity"
        | "development_lane_tenant_index"
        | "development_lane_lane_index"
        | "development_lane_repository_branch_index"
        | "development_lane_worktree_index"
        | "development_lane_work_item_index" => Some("redb-scalar-v1"),
        // Opaque byte payloads the shard never interprets as a document: the
        // audit chain's `prev|entry|line` framing, replicated Raft bytes, and
        // the encryption-key canary.
        "audit_chain" | "raft_log" | "raft_meta" | "encryption_canary" => Some("raw-bytes-v1"),
        // Shared verbatim with `OwnerLayout::TimeSeries`: one table name has
        // one codec regardless of which layout's file it sits in.
        "series_chunks" => Some("packed-timeseries-chunk-v1"),
        name if GRAPH_SHARD_TABLES.contains(&name) => Some("msgpack-v1"),
        _ => None,
    }
}

/// Scope class for one graph-shard table.
///
/// `Serving` means the key's leading component is the graph name -- the shard's
/// serving scope -- so a row is addressable within one graph. The rest are
/// keyed by Raft group, cross-shard transaction id, view name, series id or a
/// fixed file-wide key, and belong to the file rather than to any one graph.
pub(crate) fn scope(name: &str) -> Option<TableScope> {
    match name {
        "raft_log" | "raft_meta" | "xshard_prepare" | "xshard_decision" | "matviews"
        | "plan_matviews" | "matview_operator_state" | "encryption_canary" | "series_chunks"
        | "series_meta" | "series_projection_state" => Some(TableScope::StorePrivate),
        name if GRAPH_SHARD_TABLES.contains(&name) => Some(TableScope::Serving),
        _ => None,
    }
}

/// True when this graph-shard table is a derived secondary index.
pub(crate) fn is_derived_index(name: &str) -> bool {
    matches!(
        name,
        "resource_reservation_tenant_index"
            | "development_lane_tenant_index"
            | "development_lane_lane_index"
            | "development_lane_repository_branch_index"
            | "development_lane_worktree_index"
            | "development_lane_work_item_index"
            | "development_lane_pressure_index"
    )
}

/// Capability mask for one graph-shard table.
///
/// Every shard table is inserted, overwritten in place and removed wholesale by
/// a graph purge, so the mask is uniform. `raft_log` is the one exception: a
/// replicated entry is never rewritten at its index -- a conflicting suffix is
/// truncated and re-appended -- so it carries no update bit.
pub(crate) fn capabilities(name: &str) -> Option<u16> {
    use crate::owner::contract::{CAP_DELETE, CAP_INSERT, CAP_READ, CAP_UPDATE};
    match name {
        "raft_log" => Some(CAP_READ | CAP_INSERT | CAP_DELETE),
        name if GRAPH_SHARD_TABLES.contains(&name) => {
            Some(CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE)
        }
        _ => None,
    }
}
