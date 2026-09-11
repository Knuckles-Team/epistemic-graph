//! The shard file's fixed durable shape: its reserved control scope and the
//! kernel-owned GraphShard table census.

use super::control::{clear_xshard_decision, put_xshard_decision};
#[cfg(feature = "compute-dist")]
use super::control::{put_matview, scan_matviews};
use super::{
    commit_ops, purge_graph_rows, reject_reserved_graph, sanitize,
    write_graph_meta_with_incarnation, DurableCrypto, SHARD_CONTROL_GRAPH,
};
use crate::protocol::{GraphType, Method};
use crate::redb_store::shard::{Shard, ShardWrite};

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

/// Materialize a shard file exactly as the production composition root does.
fn bootstrap(path: &std::path::Path) -> Shard {
    Shard::open(path).unwrap()
}

/// The kernel and the durable tier name the SAME reserved scope.
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
    assert!(reject_reserved_graph(SHARD_CONTROL_GRAPH).is_err());
    assert_eq!(sanitize(SHARD_CONTROL_GRAPH), SHARD_CONTROL_GRAPH);
    assert!(reject_reserved_graph(&sanitize(SHARD_CONTROL_GRAPH)).is_err());
    for allowed in ["__commons__", "graph-a", "tenant/graph", ""] {
        assert!(reject_reserved_graph(allowed).is_ok(), "{allowed}");
    }
}

/// Each durable chokepoint refuses, so every entrypoint above it does too.
#[test]
fn every_durable_chokepoint_refuses_the_reserved_control_scope() {
    let path = temp_path("chokepoints");
    let shard = bootstrap(&path);

    assert!(write_graph_meta_with_incarnation(
        &shard,
        SHARD_CONTROL_GRAPH,
        SHARD_CONTROL_GRAPH,
        GraphType::Global,
        "inc-1",
    )
    .is_err());

    let node = |id: &str| Method::AddNode {
        node_id: id.to_string(),
        properties_msgpack: rmp_serde::to_vec_named(
            &std::collections::BTreeMap::<String, String>::new(),
        )
        .unwrap(),
    };
    let mut ops = vec![
        ("graph-a".to_string(), node("n1")),
        (SHARD_CONTROL_GRAPH.to_string(), node("n2")),
    ];
    let mut log = Vec::new();
    assert!(commit_ops(
        &shard,
        &mut ops,
        &mut log,
        "control-chokepoint",
        0,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut super::AuditTailCache::new(),
    )
    .is_err());
    let handle = shard.graph("graph-a").unwrap();
    let read = shard.read(&handle).unwrap();
    assert!(read
        .scoped_owner_table(super::NODES)
        .unwrap()
        .get(("graph-a", "n1"))
        .unwrap()
        .is_none());

    assert!(purge_graph_rows(&shard, SHARD_CONTROL_GRAPH).is_err());
    drop(shard);
    let _ = std::fs::remove_file(&path);
}

/// The owner layout is the canonical physical census. The retired private
/// mutation tables must not appear in it; the kernel materializes this exact
/// declaration when `Shard::open` creates the file.
#[test]
fn the_canonical_bootstrap_matches_the_declared_shard_census() {
    let path = temp_path("census");
    let shard = bootstrap(&path);
    let declared = eg_storage::owner_table_names(eg_storage::OwnerLayout::GraphShard);
    assert_eq!(declared.len(), 53);

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
            !declared.contains(&retired),
            "retired table remains declared: {retired}"
        );
    }

    // Exercise both owner classes through their capabilities. A graph member
    // cannot open file-wide rows, and the control member cannot open graph rows;
    // this is the scope boundary the census makes enforceable.
    let names = vec!["graph-a".to_string()];
    let members = shard.graph_members(&names).unwrap();
    let (group, batches) = shard.admit_maintenance(&members, "census-probe").unwrap();
    let write = ShardWrite::open(&shard, &group, &members, &batches).unwrap();
    write.control().open_table(super::GRAPH_META).unwrap();
    write
        .graph("graph-a")
        .unwrap()
        .open_scoped_table(super::NODES)
        .unwrap();
    write.finish().unwrap();
    shard.commit_drain(group, &batches, 0).unwrap();

    drop(shard);
    let _ = std::fs::remove_file(&path);
}

/// A control-row write is REPEATABLE, not first-wins.
///
/// `control_write!` admits the control member as a ledgered maintenance write
/// keyed by its operation id, and the kernel's maintenance claim is first-wins
/// by design: the SECOND write presenting the same claim key resolves as a
/// replay, and a replayed member refuses owner rows
/// ("owner write requires an admitted mutation batch"). Every control operation
/// id used to be derived from its SUBJECT alone -- `matview/put/{name}`,
/// `xshard/decision-clear/{txn_id}` -- so each of these tables could be written
/// exactly ONCE per subject for the life of the store, and every later write
/// failed closed:
///
/// * a materialized view could never be refreshed after its first
///   materialization, and
/// * cross-shard decision cleanup could never run twice for one transaction,
///   which is precisely what crash recovery does when it re-reconciles an
///   in-doubt transaction.
///
/// These writes carry no replay requirement -- their durability is the caller's
/// own 2PC record or refresh protocol, not a first-wins claim -- so their claim
/// key is per ATTEMPT, the same rule `shard::drain_batch` states for the drain
/// id. Note what the assertion below is: the second `put_matview` must WIN, not
/// merely not-error. A replay treated as a silent no-op would look like a fix
/// and would durably drop every refresh after the first.
#[test]
fn control_rows_are_repeatable_writes_not_first_wins_claims() {
    let path = temp_path("control-repeat");
    let shard = bootstrap(&path);

    #[cfg(feature = "compute-dist")]
    {
        put_matview(&shard, "view-1", b"first").unwrap();
        put_matview(&shard, "view-1", b"second").unwrap();
        let stored = scan_matviews(&shard).unwrap();
        assert_eq!(
            stored
                .iter()
                .find(|(name, _)| name == "view-1")
                .map(|(_, blob)| blob.as_slice()),
            Some(&b"second"[..]),
            "a matview refresh must replace the materialization, not be swallowed"
        );
    }

    put_xshard_decision(&shard, "txn-1", true, false).unwrap();
    clear_xshard_decision(&shard, "txn-1").unwrap();
    // Recovery re-reconciling the same in-doubt transaction repeats this step.
    clear_xshard_decision(&shard, "txn-1").unwrap();

    drop(shard);
    let _ = std::fs::remove_file(&path);
}
