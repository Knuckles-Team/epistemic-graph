//! Shared real-owner fixtures for migration layout, recovery and payload proofs.

use super::*;
use crate::protocol::{GraphType, Method};
use crate::redb_store::{CHANGE_CURSORS, CHANGE_ENVELOPES, CONTENT_VERSIONS};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use eg_storage::{OwnerRowScope, OwnerRowScopeStart};

pub(crate) fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A fresh temporary root for one test, with its `src` directory created: the
/// root, the source directory and the (not yet created) destination directory.
fn fresh_migration_dirs(
    tag: &str,
    src: &str,
    dst: &str,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let root = temp_root(tag);
    let _ = std::fs::remove_dir_all(&root);
    let (src, dst) = (root.join(src), root.join(dst));
    std::fs::create_dir_all(&src).unwrap();
    (root, src, dst)
}

/// A fresh temporary directory for one in-place test, and its path as text.
fn fresh_in_place_dir(tag: &str) -> (std::path::PathBuf, String) {
    let dir = temp_root(tag);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();
    (dir, dir_s)
}

/// The node `a` every seeded graph carries is tagged with its own graph name,
/// which proves a read-back graph was not mixed with another.
pub(crate) fn assert_node_a_carries_graph_tag(dump: &crate::redb_store::GraphDump, graph: &str) {
    let a = dump
        .nodes
        .iter()
        .find(|(id, _)| id == "a")
        .map(|(_, blob)| blob.clone())
        .expect("node a present");
    let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
    assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(graph));
}

/// Require a completed migration of `graphs` (two nodes and one edge each) into
/// `dst` at `k` shards, then reopen `dst` and require every graph to route and
/// read back intact.
fn assert_migrated_graphs_read_back(
    report: &MigrationReport,
    dst: &std::path::Path,
    k: usize,
    graphs: &[&str],
) {
    assert_eq!(report.dest_shards, k);
    assert_eq!(report.graphs, graphs.len());
    assert_eq!(report.nodes, (graphs.len() * 2) as u64);
    assert_eq!(report.edges, graphs.len() as u64);
    for i in 0..k {
        assert!(
            dst.join(format!("graph-{i}.redb")).exists(),
            "graph-{i}.redb"
        );
    }
    let backend = RedbBackend::open(dst.to_string_lossy().to_string(), 256)
        .unwrap_or_else(|error| panic!("reopen K={k}: {error}"));
    assert_eq!(backend.shard_count(), k, "on-disk layout honored as K={k}");
    for g in graphs {
        let dump = backend
            .read_graph_dump_blocking(g)
            .expect("read")
            .unwrap_or_else(|| panic!("graph {g} missing after migration"));
        assert_eq!(dump.name, *g);
        assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
        assert_eq!(dump.edges.len(), 1, "graph {g} edges");
        assert_node_a_carries_graph_tag(&dump, g);
    }
    backend.shutdown();
}

fn temp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-migrate-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

/// Create one empty kernel-owned shard file at `path`.
///
/// The layout tests below need a FILE to exist under a given name; what they assert
/// is filename discovery, which never opens the file. `Shard::open` is how a shard
/// file comes into existence after the cut, so that is how these fixtures make one.
fn empty_shard(path: &std::path::Path) {
    drop(Shard::open(path).expect("create shard fixture"));
}

/// Rows of one scope-prefixed owner table for ONE graph, read back through that
/// graph's own bound scope. Used to inspect a migration's ON-DISK output directly.
fn graph_row_count<K, V>(
    path: &std::path::Path,
    graph: &str,
    def: TableDefinition<'static, K, V>,
) -> usize
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope + OwnerRowScopeStart<'k>,
    V: redb::Value + 'static,
{
    let shard = Shard::open(path).expect("open shard for inspection");
    let handle = shard.graph(graph).expect("bind graph for inspection");
    let read = shard.read(&handle).expect("scoped read");
    let table = read
        .scoped_owner_table(def)
        .expect("open scoped owner table");
    table.scope_rows().expect("scan the scope's rows").count()
}

/// Rows of one FILE-WIDE owner table, read back through the file's control scope.
#[cfg(feature = "matview")]
fn control_row_count<K, V>(path: &std::path::Path, def: TableDefinition<'static, K, V>) -> usize
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let shard = Shard::open(path).expect("open shard for inspection");
    let read = shard.control_read().expect("control read");
    read.open_owner_table(def)
        .expect("open file-wide owner table")
        .iter()
        .expect("iterate table")
        .count()
}

/// The authoritative version and one receipt of `graph` on the shard file at `path`.
fn graph_ledger(path: &std::path::Path, graph: &str, batch_id: &str) -> (u64, bool) {
    let shard = Shard::open(path).expect("open shard for inspection");
    let handle = shard.graph(graph).expect("bind graph for inspection");
    let read = shard.read(&handle).expect("scoped read");
    let version = eg_transaction::version(&read).expect("scope version");
    let receipt = eg_transaction::read_ledger(&read, batch_id)
        .expect("read receipt")
        .is_some();
    (version, receipt)
}

/// Seed one graph on a shard file: its `graph_meta` catalog row, the governed
/// ChangeEnvelope rows a migration must carry with it, and ONE committed
/// maintenance batch, so the graph has a real kernel ledger, receipt and version.
///
/// Returns that receipt's batch id and the version the source ends at.
fn seed_graph(path: &std::path::Path, graph: &str, tag: &str) -> (String, u64) {
    let shard = Shard::open(path).expect("open source shard");
    let members = shard.graph_members(&[graph]).expect("bind graph");
    let op_id = format!("seed-{tag}");
    let (group, batches) = shard.admit_maintenance(&members, &op_id).expect("admit");
    let write = ShardWrite::open(&shard, &group, &members, &batches).expect("open write");
    write
        .control()
        .open_table(GRAPH_META)
        .expect("graph_meta")
        .insert(graph, format!("meta-{tag}").as_bytes())
        .expect("catalog row");
    {
        let rows = write.graph(graph).expect("graph member");
        rows.open_scoped_table(CHANGE_ENVELOPES)
            .expect("change_envelopes")
            .insert((graph, "envelope-1"), format!("envelope-{tag}").as_bytes())
            .expect("envelope row");
        rows.open_scoped_table(CONTENT_VERSIONS)
            .expect("content_versions")
            .insert((graph, "tenant-a", "object-1"), &b"version"[..])
            .expect("content version row");
        rows.open_scoped_table(CHANGE_CURSORS)
            .expect("change_cursors")
            .insert(
                (graph, "tenant-a", "source-a", "partition-a"),
                &b"cursor"[..],
            )
            .expect("cursor row");
    }
    write.finish().expect("finish");
    shard.commit_drain(group, &batches, 1).expect("commit");

    let handle = shard.graph(graph).expect("graph handle");
    let read = shard.read(&handle).expect("scoped read");
    let version = eg_transaction::version(&read).expect("scope version");
    (format!("shard_drain/{graph}:{op_id}"), version)
}

/// Insert one owner row directly into `table` for `graph`, bypassing every request/
/// validation path — the same "seed the durable table, not the API" technique
/// `graph_row_count` already uses for reads, through the one admitted write path a
/// domain has. `second_key` doubles as the write's operation id, so seeding several
/// tables on one shard never re-presents an identity the ledger already holds.
fn seed_owner_row(
    shard_path: &std::path::Path,
    table: TableDefinition<'static, (&str, &str), &[u8]>,
    graph: &str,
    second_key: &str,
    value: &[u8],
) {
    let shard = Shard::open(shard_path).expect("open shard for seeding");
    let members = shard.graph_members(&[graph]).expect("bind graph");
    let op_id = format!("seed-owner-row/{second_key}");
    let (group, batches) = shard.admit_maintenance(&members, &op_id).expect("admit");
    let write = ShardWrite::open(&shard, &group, &members, &batches).expect("open write");
    write
        .graph(graph)
        .expect("graph member")
        .open_scoped_table(table)
        .expect("open table for seeding")
        .insert((graph, second_key), value)
        .expect("insert seed row");
    write.finish().expect("finish");
    shard.commit_drain(group, &batches, 1).expect("commit");
}

/// A K=1 backend over `src` with the one graph `g` registered.
async fn k1_backend_with_graph_g(src: &std::path::Path) -> RedbBackend {
    let backend =
        RedbBackend::open(src.to_string_lossy().to_string(), 256).expect("open K=1 backend");
    backend
        .register_graph("g", "g", GraphType::Global)
        .await
        .expect("register");
    backend
}

/// Write G graphs (each with nodes + an edge) through a K=1 backend, durably.
async fn seed_k1(dir: &str, graphs: &[&str]) {
    seed_at_k(dir, 1, graphs).await;
}

/// Seed `graphs.len()` graphs (2 nodes + 1 edge each) through a backend opened at
/// an EXPLICIT shard count `k`.
pub(crate) async fn seed_at_k(dir: &str, k: usize, graphs: &[&str]) {
    let backend = RedbBackend::open_with_shards(dir.to_string(), 256, k)
        .expect("open backend at requested K");
    for g in graphs {
        backend
            .register_graph(g, g, GraphType::Global)
            .await
            .expect("register");
        backend
            .record_durable(
                g,
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task", "g": g})),
                },
            )
            .await
            .expect("node a");
        backend
            .record_durable(
                g,
                &Method::AddNode {
                    node_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"type": "Task"})),
                },
            )
            .await
            .expect("node b");
        backend
            .record_durable(
                g,
                &Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: props(serde_json::json!({"w": 1})),
                },
            )
            .await
            .expect("edge");
    }
    backend.shutdown();
}

mod layout;
mod payload;
mod recovery;
