//! The shard file's fixed durable shape: its reserved control scope, and a
//! canonical table bootstrap that does not depend on which features the binary
//! that opened the file was built with.

use super::{
    initialize_canonical_tables, purge_graph_rows, reject_reserved_graph, sanitize,
    write_graph_meta_with_incarnation, DurableCrypto, SHARD_CONTROL_GRAPH,
};
use crate::protocol::{GraphType, Method};
use crate::redb_store::AuditTailCache;
use redb::{Database, ReadableDatabase, TableHandle};

fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-shard-control-{tag}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Materialize a shard file exactly as `Shard::open` does.
fn bootstrap(path: &std::path::Path) -> Database {
    let db = Database::create(path).unwrap();
    let wtx = db.begin_write().unwrap();
    initialize_canonical_tables(&wtx).unwrap();
    wtx.open_table(crate::server::persistence::redb_backend::RAFT_META)
        .unwrap();
    wtx.open_table(crate::server::persistence::redb_backend::ENCRYPTION_CANARY)
        .unwrap();
    wtx.commit().unwrap();
    db
}

fn table_names(db: &Database) -> std::collections::BTreeSet<String> {
    let rtx = db.begin_read().unwrap();
    rtx.list_tables()
        .unwrap()
        .map(|table| table.name().to_string())
        .collect()
}

/// The kernel and the durable tier name the SAME reserved scope.
///
/// `eg_storage::GRAPH_SHARD_CONTROL_GRAPH` is the single owner of the literal --
/// `authenticate_scope` refuses it on any layout that does not reserve it, and
/// `admit_group` reads a member's control class off its identity -- so this
/// asserts the kernel really does reserve it for the layout the shard uses,
/// rather than only that two constants happen to be spelled alike.
#[test]
fn the_kernel_reserves_the_same_control_scope_the_durable_tier_refuses() {
    assert_eq!(
        eg_storage::reserved_control_graph(eg_storage::OwnerLayout::GraphShard),
        Some(SHARD_CONTROL_GRAPH)
    );
    assert!(reject_reserved_graph(eg_storage::GRAPH_SHARD_CONTROL_GRAPH).is_err());
}

#[test]
fn the_reserved_control_scope_is_not_a_usable_graph_name() {
    // Both the raw logical name and the durable key it sanitizes to.
    assert!(reject_reserved_graph(SHARD_CONTROL_GRAPH).is_err());
    assert_eq!(sanitize(SHARD_CONTROL_GRAPH), SHARD_CONTROL_GRAPH);
    assert!(reject_reserved_graph(&sanitize(SHARD_CONTROL_GRAPH)).is_err());
    // A bracketed name that is a REAL user-visible graph stays usable, so the
    // rule is this one name and not a `__…__` prefix ban.
    for allowed in ["__commons__", "graph-a", "tenant/graph", ""] {
        assert!(reject_reserved_graph(allowed).is_ok(), "{allowed}");
    }
}

/// Each durable chokepoint refuses, so every entrypoint above it does too.
#[test]
fn every_durable_chokepoint_refuses_the_reserved_control_scope() {
    let path = temp_path("chokepoints");
    let db = bootstrap(&path);

    // 1. the durable identity writer (server `Cmd::RegisterGraph`, embedded
    //    `register_graph`, and the checkpoint path all reach it).
    assert!(write_graph_meta_with_incarnation(
        &db,
        SHARD_CONTROL_GRAPH,
        SHARD_CONTROL_GRAPH,
        GraphType::Global,
        "inc-1",
    )
    .is_err());

    // 2. the coalesced write path -- one reserved op poisons the whole batch,
    //    before any row is written.
    let node = |id: &str| Method::AddNode {
        node_id: id.to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&std::collections::BTreeMap::<
            String,
            String,
        >::new())
        .unwrap(),
    };
    let mut ops = vec![
        ("graph-a".to_string(), node("n1")),
        (SHARD_CONTROL_GRAPH.to_string(), node("n2")),
    ];
    let mut log = Vec::new();
    assert!(super::commit_ops(
        &db,
        &mut ops,
        &mut log,
        redb::Durability::Immediate,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut AuditTailCache::new(),
    )
    .is_err());
    // Nothing landed: the refusal happens before the write transaction opens.
    let rtx = db.begin_read().unwrap();
    let nodes = rtx.open_table(super::NODES).unwrap();
    assert!(nodes.get(("graph-a", "n1")).unwrap().is_none());
    drop(nodes);
    drop(rtx);

    // 3. whole-graph teardown never deletes the shard's own control rows.
    assert!(purge_graph_rows(&db, SHARD_CONTROL_GRAPH, DurableCrypto::none()).is_err());

    drop(db);
    let _ = std::fs::remove_file(&path);
}

/// The bootstrap materializes the same table set in every feature configuration,
/// and that set is the one `OwnerLayout::GraphShard` declares -- exactly, and
/// with the two differences this cutover still has to close named.
#[test]
fn the_canonical_bootstrap_matches_the_declared_shard_census() {
    let path = temp_path("census");
    let db = bootstrap(&path);
    let actual = table_names(&db);

    let declared = eg_storage::owner_table_names(eg_storage::OwnerLayout::GraphShard)
        .iter()
        .map(|name| name.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(declared.len(), 53);

    // The shard's own private mutation ledger is still bootstrapped but is
    // declared by no layout: RF-RULING-004 gives admission, idempotency, OCC,
    // fencing, outbox and projection cursors to `MutationKernelV1` alone, and
    // retiring these eight is the remaining step of this cutover.
    let retired: std::collections::BTreeSet<String> = [
        "mutation_batches",
        "mutation_idempotency",
        "mutation_outbox",
        "mutation_lifecycle_head",
        "mutation_graph_version",
        "mutation_fence",
        "mutation_outbox_delivery",
        "mutation_projection_cursor",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    // The three cross-modal series tables are declared by the layout and are
    // created by `eg-tsdb`'s writer on first measurement rather than by this
    // bootstrap; they arrive when the shard opens through `create_owner`.
    let series: std::collections::BTreeSet<String> = [
        "series_chunks",
        "series_meta",
        "series_projection_state",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    assert_eq!(
        actual,
        declared
            .difference(&series)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .union(&retired)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        "the canonical bootstrap and the declared GraphShard census have drifted"
    );

    // Named individually so a feature-gated regression is reported as itself
    // rather than as a set difference: these five were `cfg`-gated (three of
    // them behind features that are off in a slim build) and one was never
    // pre-warmed at all.
    for name in [
        "audit_chain",
        "provenance_anchor_members",
        "matviews",
        "plan_matviews",
        "matview_operator_state",
        "encryption_canary",
    ] {
        assert!(actual.contains(name), "missing unconditional table: {name}");
        assert!(declared.contains(name), "undeclared table: {name}");
    }

    drop(db);
    let _ = std::fs::remove_file(&path);
}
