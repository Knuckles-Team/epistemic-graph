//! `OwnerLayout::GraphShard` — the authoritative graph shard's census, its
//! table scope classes, and the graph-scope binding rule that makes one shard
//! file serve many graphs.

use crate::kernel::{authenticate_scope_in, bind_serving_scope_in, create_physical, open_physical};
use crate::owner::domain::GraphShardOwner;
use crate::owner::grant::ScopeGrantVerifier;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::{OwnerLayout, OWNER_LAYOUT_DOMAINS};
use crate::owner::registry::{owner_layouts, owner_table_names};
use crate::owner::table_api::{owner_table_access, OwnerTableAccess};
use crate::physical::manifest::{OwnerManifest, TableScope};
use crate::scoped::ScopeRow;
use eg_types::mutation_batch::{DurabilityDomain, IncarnationId, LogicalName, ScopeTenantId};
use eg_types::MutationScopeIdentity;

/// The shard census is exact, and the eight private-ledger tables are absent.
///
/// `owner_table_names(GraphShard)` is the manifest contract for `graph-N.redb`,
/// so this pins the whole list by name and type rather than only its
/// cardinality: a table silently dropped from, or added to, the shard is a
/// different physical format under the same layout name.
#[test]
fn the_graph_shard_census_is_exact_and_carries_no_private_mutation_ledger() {
    let manifest = OwnerManifest::new(
        PhysicalStoreIdentity::new("physical:test:graph-shard-census").unwrap(),
        OwnerLayout::GraphShard,
    )
    .unwrap();
    let declared = owner_table_names(OwnerLayout::GraphShard);

    for (name, key, value) in [
        ("nodes", "(&str,&str)", "&[u8]"),
        ("edges", "(&str,&str,&str,u32)", "&[u8]"),
        ("ledger", "(&str,u64)", "&str"),
        ("semantic_store", "&str", "&[u8]"),
        ("audit_chain", "(&str,u64)", "&[u8]"),
        ("provenance_anchor_members", "(&str,u64)", "&[u8]"),
        ("graph_meta", "&str", "&[u8]"),
        ("work_item_command_sequence", "&str", "u64"),
        ("resource_reservations", "(&str,&str)", "&[u8]"),
        (
            "resource_reservation_tenant_index",
            "(&str,&str,&str)",
            "&str",
        ),
        ("resource_reservation_attempts", "(&str,&str,u64)", "&str"),
        ("resource_hosts", "(&str,&str)", "&[u8]"),
        ("resource_exclusivity", "(&str,&str)", "&str"),
        ("resource_fairness", "(&str,&str)", "&[u8]"),
        ("resource_concurrency", "(&str,&str)", "u64"),
        ("resource_anti_affinity", "(&str,&str,&str)", "u64"),
        ("resource_disk_policies", "(&str,&str)", "&[u8]"),
        ("change_envelopes", "(&str,&str)", "&[u8]"),
        ("content_versions", "(&str,&str,&str)", "&[u8]"),
        ("change_cursors", "(&str,&str,&str,&str)", "&[u8]"),
        ("change_blobs", "(&str,&str,&str)", "&[u8]"),
        ("change_features", "(&str,&str,&str)", "&[u8]"),
        ("change_evidence", "(&str,&str,&str)", "&[u8]"),
        ("change_policies", "(&str,&str,&str)", "&[u8]"),
        ("change_lineage", "(&str,&str,&str)", "&[u8]"),
        ("raft_log", "(u64,u64)", "&[u8]"),
        ("raft_meta", "(u64,&str)", "&[u8]"),
        ("xshard_prepare", "(&str,u64)", "&[u8]"),
        ("xshard_decision", "&str", "u8"),
        ("matviews", "&str", "&[u8]"),
        ("plan_matviews", "&str", "&[u8]"),
        ("matview_operator_state", "&str", "&[u8]"),
        ("capacity_cells", "(&str,&str)", "&[u8]"),
        ("capacity_leases", "(&str,&str)", "&[u8]"),
        ("capacity_usage", "(&str,&str)", "&[u8]"),
        ("capacity_idempotency", "(&str,&str,&str)", "&[u8]"),
        ("work_item_claim_capabilities", "(&str,&str)", "&[u8]"),
        (
            "work_item_claim_capability_invocations",
            "(&str,&str)",
            "&[u8]",
        ),
        ("native_work_item_authority", "(&str,&str)", "&[u8]"),
        ("development_lane_holds", "(&str,&str)", "&[u8]"),
        ("development_lane_tenant_index", "(&str,&str,&str)", "&str"),
        ("development_lane_lane_index", "(&str,&str,&str)", "&str"),
        (
            "development_lane_repository_branch_index",
            "(&str,&str,&str)",
            "&str",
        ),
        ("development_lane_worktree_index", "(&str,&str)", "&str"),
        (
            "development_lane_work_item_index",
            "(&str,&str,u64)",
            "&str",
        ),
        ("development_lane_counters", "(&str,&str)", "&[u8]"),
        (
            "development_lane_pressure_index",
            "(&str,&str,&str,&str,u64,&str)",
            "u8",
        ),
        ("development_lane_policies", "(&str,&str)", "&[u8]"),
        ("development_lane_invocations", "(&str,&str,&str)", "&[u8]"),
        ("encryption_canary", "&str", "&[u8]"),
        ("series_chunks", "(&str,u64)", "&[u8]"),
        ("series_meta", "&str", "&[u8]"),
        ("series_projection_state", "&str", "&[u8]"),
    ] {
        assert!(declared.contains(&name), "undeclared shard table: {name}");
        let table = manifest
            .tables
            .iter()
            .find(|table| table.table_id == name)
            .unwrap_or_else(|| panic!("no contract for {name}"));
        assert_eq!(
            (table.key_type_id.as_str(), table.value_type_id.as_str()),
            (key, value),
            "{name}"
        );
        assert_eq!(table.domain, Some(DurabilityDomain::GraphRows), "{name}");
        assert_eq!(
            owner_table_access(name),
            OwnerTableAccess::DomainService,
            "{name}"
        );
    }

    // RF-RULING-004: the shard's own admit/idempotency/OCC/fence/outbox/
    // projection ledger belongs to `MutationKernel`, so none of its eight
    // tables is an owner table of any layout.
    for retired in crate::owner::graph_shard::RETIRED_SHARD_LEDGER_TABLES {
        assert!(
            owner_layouts()
                .into_iter()
                .all(|layout| !owner_table_names(layout).contains(retired)),
            "retired shard ledger table is still declared: {retired}"
        );
    }
}

/// The Raft, cross-shard, matview, canary, series and **catalog** rows belong
/// to the file, not to any one graph, and say so in the manifest.
///
/// The split is 41 `Serving` / 12 `StorePrivate`, not 42/11: `graph_meta` moved
/// to the file side because it is the catalog the boot scan reads to learn the
/// graph names, which is strictly before any graph scope can be bound. See
/// `graph_shard::scope`.
#[test]
fn the_shard_separates_graph_scoped_rows_from_file_wide_rows() {
    let manifest = OwnerManifest::new(
        PhysicalStoreIdentity::new("physical:test:graph-shard-scope").unwrap(),
        OwnerLayout::GraphShard,
    )
    .unwrap();
    let scope_of = |name: &str| {
        manifest
            .tables
            .iter()
            .find(|table| table.table_id == name)
            .unwrap()
            .scope
    };
    for name in [
        "graph_meta",
        "raft_log",
        "raft_meta",
        "xshard_prepare",
        "xshard_decision",
        "matviews",
        "plan_matviews",
        "matview_operator_state",
        "encryption_canary",
        "series_chunks",
        "series_meta",
        "series_projection_state",
    ] {
        assert_eq!(scope_of(name), TableScope::StorePrivate, "{name}");
    }
    for name in ["nodes", "edges", "semantic_store", "development_lane_holds"] {
        assert_eq!(scope_of(name), TableScope::Serving, "{name}");
    }
    let private = owner_table_names(OwnerLayout::GraphShard)
        .iter()
        .filter(|name| scope_of(name) == TableScope::StorePrivate)
        .count();
    assert_eq!((private, 53 - private), (12, 41));
}

/// The catalog is readable by the scope that exists before any graph is known,
/// and the graph-keyed tables beside it still are not.
///
/// This is the property the reclassification exists for, asserted against the
/// real row-class ACL rather than against the declaration alone: the boot scan
/// runs on the control scope, so `graph_meta` must be openable there, while
/// `semantic_store` -- the other `&str`-keyed shard table, whose key is also
/// exactly the graph name -- must stay unreachable from it. Without the second
/// half the test would pass for a layout that had simply made everything
/// file-wide.
#[test]
fn only_the_catalog_is_readable_before_any_graph_scope_is_known() {
    use crate::owner::row_key::{owner_row_key, RowKey};

    assert_eq!(
        owner_row_key("graph_meta", OwnerLayout::GraphShard),
        RowKey::FileWide,
        "the boot scan cannot bind a graph scope to discover graph names"
    );
    for still_scoped in ["semantic_store", "work_item_command_sequence", "nodes"] {
        assert_eq!(
            owner_row_key(still_scoped, OwnerLayout::GraphShard),
            RowKey::ScopePrefixed,
            "{still_scoped} is one graph's rows and stays confined to that graph"
        );
    }
}

/// A graph scope binds to the shard layout and to nothing else, and every
/// native scope still binds exactly where it did before `accepts` was
/// generalised.
#[test]
fn only_the_shard_layout_accepts_a_graph_scope() {
    let graph = MutationScopeIdentity::fixed_graph("tenant-a", "graph-a", "inc-1").unwrap();
    for layout in owner_layouts() {
        let expected = matches!(layout, OwnerLayout::LedgerOnly | OwnerLayout::GraphShard);
        assert_eq!(layout.accepts(&graph), expected, "{layout:?}");
    }

    // The pairing `accepts` used to spell out by hand, asserted as a whole: a
    // native scope binds to exactly the layouts whose declared domain it names.
    for domain in [
        DurabilityDomain::ControlPlane,
        DurabilityDomain::AnalyticsJob,
        DurabilityDomain::Lifecycle,
        DurabilityDomain::TimeSeries,
        DurabilityDomain::KvStore,
        DurabilityDomain::BlobStore,
        DurabilityDomain::SemanticIndex,
        DurabilityDomain::SqlCatalog,
    ] {
        let identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            domain,
            LogicalName::new("resource-a").unwrap(),
            IncarnationId::new("inc-1").unwrap(),
        )
        .unwrap();
        for layout in owner_layouts() {
            let expected = layout == OwnerLayout::LedgerOnly
                || OWNER_LAYOUT_DOMAINS[layout as usize] == domain;
            assert_eq!(layout.accepts(&identity), expected, "{layout:?} {domain:?}");
        }
    }
}

/// The test shard authority: admits only graph scopes presented by the test
/// principal with the fixed proof.
struct ShardVerifier;

impl ScopeGrantVerifier for ShardVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        (layout == OwnerLayout::GraphShard
            && identity.scope().graph_name().is_some()
            && principal == "principal:test:shard"
            && proof == b"verified")
            .then_some(())
            .ok_or_else(|| "test shard authority rejected".to_string())
    }
}

/// `(graph, node)` of every row a scoped scan yields, in order.
fn scanned_pairs<'t>(
    rows: impl Iterator<Item = ScopeRow<'t, (&'static str, &'static str), &'static [u8]>>,
) -> Vec<(String, String)> {
    rows.map(|row| {
        let (key, _) = row.unwrap();
        let (g, n) = key.value();
        (g.to_string(), n.to_string())
    })
    .collect()
}

/// One shard file serves many graphs: each graph scope authenticates and binds
/// independently against the same physical store.
#[test]
fn many_graph_scopes_bind_to_one_shard_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let store = create_physical(
        &path,
        PhysicalStoreIdentity::new("physical:test:graph-0").unwrap(),
        None,
        OwnerLayout::GraphShard,
    )
    .unwrap();

    for graph in ["graph-a", "graph-b"] {
        let identity = MutationScopeIdentity::fixed_graph("tenant-a", graph, "inc-1").unwrap();
        let grant = authenticate_scope_in::<GraphShardOwner>(
            &store,
            &ShardVerifier,
            identity.clone(),
            "principal:test:shard".to_string(),
            b"verified",
        )
        .unwrap();
        let owner = bind_serving_scope_in(&store, grant, 0).unwrap();
        assert_eq!(owner.identity(), &identity);
    }

    // A native scope has no shard file to bind to, whatever the verifier says.
    let native = MutationScopeIdentity::native(
        ScopeTenantId::new("tenant-a").unwrap(),
        DurabilityDomain::KvStore,
        LogicalName::new("kv-catalog").unwrap(),
        IncarnationId::new("inc-1").unwrap(),
    )
    .unwrap();
    assert!(authenticate_scope_in::<GraphShardOwner>(
        &store,
        &ShardVerifier,
        native,
        "principal:test:shard".to_string(),
        b"verified",
    )
    .is_err());

    drop(store);
    open_physical(
        &path,
        PhysicalStoreIdentity::new("physical:test:graph-0").unwrap(),
        None,
        OwnerLayout::GraphShard,
    )
    .unwrap();
}

/// A scope-bounded scan yields exactly its own scope's rows, and stops at the
/// first key that leaves the scope even when the neighbouring scope's name is a
/// prefix-extension of it.
///
/// `graph-a` < `graph-ab` < `graph-b` are adjacent in redb's key order and
/// `graph-a` is a proper prefix of `graph-ab`, which is the case a naive
/// "starts with the scope name" bound gets wrong. The scan is bounded by the
/// whole leading key COMPONENT, so it stops correctly.
#[test]
fn scope_rows_yields_one_scopes_rows_and_never_a_neighbours() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph-0.redb");
    let store = create_physical(
        &path,
        PhysicalStoreIdentity::new("physical:test:graph-scan").unwrap(),
        None,
        OwnerLayout::GraphShard,
    )
    .unwrap();

    let bind = |graph: &str| {
        let identity = MutationScopeIdentity::fixed_graph("tenant-a", graph, "inc-1").unwrap();
        let grant = authenticate_scope_in::<GraphShardOwner>(
            &store,
            &ShardVerifier,
            identity,
            "principal:test:shard".to_string(),
            b"verified",
        )
        .unwrap();
        bind_serving_scope_in::<GraphShardOwner>(&store, grant, 0).unwrap()
    };

    // `graph-empty` is bound but never written: an empty scope must yield
    // nothing rather than the next scope's rows.
    let owners: Vec<_> = ["graph-a", "graph-ab", "graph-b", "graph-empty"]
        .iter()
        .map(|graph| (*graph, bind(graph)))
        .collect();

    for (graph, owner) in &owners {
        if *graph == "graph-empty" {
            continue;
        }
        let write = crate::capability::PhysicalWriteCapability::open(&store, owner).unwrap();
        {
            let mut nodes = write
                .scoped_owner_table_mut(crate::owner::graph_shard::NODES)
                .unwrap();
            for node in ["n1", "n2"] {
                nodes.insert((*graph, node), b"row".as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
    }

    for (graph, owner) in &owners {
        let read = crate::capability::ScopedRead::open(&store, owner).unwrap();
        let table = read
            .scoped_owner_table(crate::owner::graph_shard::NODES)
            .unwrap();
        let seen = scanned_pairs(table.scope_rows().unwrap());
        let expected: Vec<(String, String)> = if *graph == "graph-empty" {
            Vec::new()
        } else {
            ["n1", "n2"]
                .iter()
                .map(|n| ((*graph).to_string(), (*n).to_string()))
                .collect()
        };
        assert_eq!(seen, expected, "{graph}");
        // The confinement negative, stated as a property rather than inferred
        // from the equality above: no yielded row belongs to another scope.
        assert!(
            seen.iter().all(|(g, _)| g == graph),
            "{graph} saw another scope's rows"
        );
        assert_seek_is_confined(&table, graph, &expected);
    }
}

/// The seek form starts AT its key and keeps `scope_rows`' confinement: from
/// `n2`, `graph-a` yields only its own tail -- never `graph-ab`'s rows, which
/// sort immediately after -- and a start key naming another scope is refused
/// rather than read.
fn assert_seek_is_confined(
    table: &crate::scoped::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    graph: &str,
    expected: &[(String, String)],
) {
    let resumed = scanned_pairs(table.scope_rows_from((graph, "n2")).unwrap());
    let tail: Vec<(String, String)> = expected
        .iter()
        .filter(|(_, node)| node.as_str() >= "n2")
        .cloned()
        .collect();
    assert_eq!(resumed, tail, "{graph} resumed");
    assert!(
        table.scope_rows_from(("graph-other", "n1")).is_err(),
        "{graph} seeked into another scope"
    );
}
