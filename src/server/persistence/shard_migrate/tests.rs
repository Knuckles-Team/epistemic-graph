//! Shared real-owner fixtures for migration layout, recovery and payload proofs.

use super::*;
use crate::protocol::{GraphType, Method};
use crate::redb_store::{CHANGE_CURSORS, CHANGE_ENVELOPES, CONTENT_VERSIONS};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use eg_storage::{OwnerRowScope, OwnerRowScopeStart};

fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
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

/// Write G graphs (each with nodes + an edge) through a K=1 backend, durably.
async fn seed_k1(dir: &str, graphs: &[&str]) {
    seed_at_k(dir, 1, graphs).await;
}

/// Seed `graphs.len()` graphs (2 nodes + 1 edge each) through a backend opened at
/// an EXPLICIT shard count `k`.
async fn seed_at_k(dir: &str, k: usize, graphs: &[&str]) {
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
