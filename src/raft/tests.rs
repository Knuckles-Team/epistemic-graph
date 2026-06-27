//! 3-node Raft cluster test (CONCEPT:KG-2.188).
//!
//! Spins three in-process Raft nodes on three local TCP ports, each with its own
//! redb-AUTHORITATIVE persistence dir. Verifies the full HA contract:
//!
//! 1. a leader is elected,
//! 2. a batch of writes routed through the leader's `client_write` replicates,
//! 3. the writes are READABLE from a FOLLOWER (read replica),
//! 4. KILLING the leader (shutdown Raft + stop its RPC listener) triggers a NEW
//!    leader election among the survivors,
//! 5. EVERY committed write survives on the new leader (no data loss, no
//!    split-brain — the survivors form a quorum of 2/3).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::config::RaftClusterConfig;
use super::node::{self, StartedNode};
use super::{NodeId, RaftRequest};
use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method};
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;
use crate::wal_service::FsyncPolicy;

/// Build a ServerState with a redb-AUTHORITATIVE backend rooted at `dir`.
async fn make_state(dir: &str) -> Arc<RwLock<ServerState>> {
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.to_string(), FsyncPolicy::Each, 4096).expect("open redb"));
    make_state_with_backend(dir, backend).await
}

/// Build a ServerState over an ALREADY-OPEN backend (redb is single-handle-per-
/// process, so a test that needs the concrete backend handle must SHARE it with the
/// state, never open a second one over the same dir).
async fn make_state_with_backend(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "raft-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        redb_authoritative: true,
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "rdf-redb")]
        rdf_quads: None,
        #[cfg(feature = "streaming")]
        cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: std::sync::Arc::new(parking_lot::Mutex::new(
            crate::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
    }))
}

fn peer_map(ports: &[u16]) -> BTreeMap<NodeId, BasicNode> {
    ports
        .iter()
        .enumerate()
        .map(|(i, p)| ((i + 1) as NodeId, BasicNode::new(format!("127.0.0.1:{p}"))))
        .collect()
}

fn cluster_cfg(node_id: NodeId, ports: &[u16]) -> RaftClusterConfig {
    let peers = peer_map(ports);
    let bind_addr = peers.get(&node_id).unwrap().addr.clone();
    RaftClusterConfig {
        node_id,
        peers: peers.clone(),
        bind_addr,
        is_bootstrap: peers.keys().next() == Some(&node_id),
    }
}

/// Pick three currently-free localhost ports by binding then dropping.
fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..n {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        ports.push(l.local_addr().unwrap().port());
        listeners.push(l);
    }
    // Listeners drop here, freeing the ports for the Raft listeners to claim.
    ports
}

/// A read-the-graph helper: total node count in the named graph on a node's state.
async fn node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_replicates_and_survives_leader_failover() {
    let tmp = std::env::temp_dir().join(format!("eg-raft-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let dirs: Vec<String> = (1..=3)
        .map(|i| {
            let d = tmp.join(format!("node{i}"));
            std::fs::create_dir_all(&d).unwrap();
            d.to_string_lossy().to_string()
        })
        .collect();
    let ports = free_ports(3);

    // ── Start three nodes ───────────────────────────────────────────────
    let mut states = Vec::new();
    let mut nodes: BTreeMap<NodeId, StartedNode> = BTreeMap::new();
    for i in 1..=3u64 {
        let state = make_state(&dirs[(i - 1) as usize]).await;
        let started = node::start(cluster_cfg(i, &ports), state.clone())
            .await
            .expect("start raft node");
        state.write().await.raft = Some(started.handle.clone());
        states.push(state);
        nodes.insert(i, started);
    }

    // ── 1. A leader is elected ──────────────────────────────────────────
    // Bootstrap node (id 1) initializes; wait until SOME node reports a leader.
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(15))
        .await
        .expect("a leader must be elected");
    assert!((1..=3).contains(&leader_id));

    // ── 2. Replicate a batch of writes through the leader ───────────────
    let graph = "__commons__";
    let n_writes = 25u64;
    let leader = nodes.get(&leader_id).unwrap().handle.clone();
    for k in 0..n_writes {
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(graph),
            graph_name: graph.to_string(),
            graph_type: GraphType::Commons,
            method: Method::AddNode {
                node_id: format!("n{k}"),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"k": k})).unwrap(),
            },
        };
        leader
            .client_write(req)
            .await
            .unwrap_or_else(|e| panic!("write {k} via leader failed: {e}"));
    }

    // ── 3. Read the writes back from a FOLLOWER (read replica) ──────────
    let follower_id = (1..=3u64).find(|&i| i != leader_id).unwrap();
    let follower_state = states[(follower_id - 1) as usize].clone();
    wait_until(Duration::from_secs(10), || {
        let st = follower_state.clone();
        async move { node_count(&st, graph).await as u64 == n_writes }
    })
    .await
    .expect("all writes must replicate to the follower");
    assert_eq!(node_count(&follower_state, graph).await as u64, n_writes);

    // ── 4. KILL the leader: shut down its Raft + stop its RPC listener ──
    let killed = nodes.remove(&leader_id).unwrap();
    killed.multi.stop_listener();
    let _ = killed.handle.raft.shutdown().await;

    // ── 5. A NEW leader is elected among the survivors, with NO data loss ─
    let new_leader = wait_for_leader_excluding(&nodes, leader_id, Duration::from_secs(20))
        .await
        .expect("a new leader must be elected after the old one is killed");
    assert_ne!(new_leader, leader_id, "must not be the dead leader");

    // All committed writes survive on the new leader's state machine.
    let new_leader_state = states[(new_leader - 1) as usize].clone();
    assert_eq!(
        node_count(&new_leader_state, graph).await as u64,
        n_writes,
        "every committed write must survive the failover (no data loss)"
    );

    // And a write through the NEW leader still commits (cluster is live, not split).
    let post = RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Commons,
        method: Method::AddNode {
            node_id: "after-failover".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"post": true}))
                .unwrap(),
        },
    };
    nodes
        .get(&new_leader)
        .unwrap()
        .handle
        .client_write(post)
        .await
        .expect("the new leader must accept writes (cluster is live)");
    assert_eq!(
        node_count(&new_leader_state, graph).await as u64,
        n_writes + 1
    );

    // ── Cleanup ─────────────────────────────────────────────────────────
    for (_, n) in nodes {
        n.multi.stop_listener();
        let _ = n.handle.raft.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Wait until any node reports a current leader; returns its id.
async fn wait_for_leader(
    nodes: &BTreeMap<NodeId, StartedNode>,
    timeout: Duration,
) -> Option<NodeId> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes.values() {
            if let Some(l) = n.handle.raft.current_leader().await {
                return Some(l);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Wait until a leader OTHER than `excluded` is reported by a surviving node.
async fn wait_for_leader_excluding(
    nodes: &BTreeMap<NodeId, StartedNode>,
    excluded: NodeId,
    timeout: Duration,
) -> Option<NodeId> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes.values() {
            if let Some(l) = n.handle.raft.current_leader().await {
                if l != excluded {
                    return Some(l);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Poll an async predicate until it is true or the timeout elapses.
async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if pred().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Err(())
}

// ─────────────────────────────────────────────────────────────────────────
// KG-2.204: durable redb Raft log — replay after restart + fault injection
// ─────────────────────────────────────────────────────────────────────────

use super::store::EgStore;
use super::AppCtx;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftStorage;

/// A fresh temp dir for one test, removed first.
fn fresh_dir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!("eg-raft-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.to_string_lossy().to_string()
}

/// Encode a Normal log `Entry` for group `gid` at `index`/`term` carrying an AddNode.
fn make_log_entry(index: u64, term: u64, node_id: &str) -> openraft::Entry<super::TypeConfig> {
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
        payload: EntryPayload::Normal(RaftRequest {
            graph_fname: crate::persist::sanitize("__commons__"),
            graph_name: "__commons__".to_string(),
            graph_type: GraphType::Commons,
            method: Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"id": node_id}))
                    .unwrap(),
            },
        }),
    }
}

/// KG-2.204: append log entries → DROP the store → RE-OPEN over the SAME redb →
/// the log replays from disk with NO leader/network (the in-memory log could not).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_log_replays_from_redb_after_restart() {
    let dir = fresh_dir("logreplay");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx { state };

    // ── Append 12 entries through the real RaftStorage::append_to_log path ──
    {
        let mut store = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
        let entries: Vec<_> = (1..=12)
            .map(|i| make_log_entry(i, 1, &format!("n{i}")))
            .collect();
        store.append_to_log(entries).await.expect("append_to_log");
        // Drop the store (simulate process exit). The redb backend stays open here,
        // but the EgStore's in-RAM log state (if any) is gone — the next open must
        // recover the tail PURELY from redb.
        drop(store);
    }

    // ── Re-open the store over the SAME redb; no leader, no network ─────────
    let mut store2 = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
    let log_state = store2.get_log_state().await.expect("get_log_state");
    assert_eq!(
        log_state.last_log_id.map(|l| l.index),
        Some(12),
        "the restarted store must recover its log tail from redb (index 12)"
    );
    let replayed = store2
        .try_get_log_entries(1..=12)
        .await
        .expect("read log range");
    assert_eq!(replayed.len(), 12, "all 12 entries replay from redb");
    for (k, e) in replayed.iter().enumerate() {
        assert_eq!(e.log_id.index, (k + 1) as u64);
    }

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// KG-2.204 fault injection: append committed log entries, then KILL the writer
/// MID-LIFE (drop the backend → its writer thread joins, flushing) and RE-OPEN a
/// brand-new backend over the same files. No committed (fsynced) entry is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fault_injection_no_committed_log_entry_lost_on_restart() {
    let dir = fresh_dir("faultlog");
    // FsyncPolicy::Each = a committed (awaited) append is fsynced before the await
    // returns, so anything we observe as Ok IS on disk.
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
    };

    {
        let mut store = EgStore::open(super::DEFAULT_GROUP, backend.clone(), ctx.clone()).unwrap();
        // Append 8 entries; each append_to_log awaits the durable group-commit fsync
        // (commit-before-ack), so all 8 are provably on disk when this returns Ok.
        for i in 1..=8u64 {
            store
                .append_to_log(vec![make_log_entry(i, 1, &format!("k{i}"))])
                .await
                .expect("durable append");
        }
        drop(store);
    }
    // "Kill": drop ALL handles to the backend so its writer thread shuts down + the
    // single-process redb file lock releases (the closest in-process analog to losing
    // the process — the on-disk redb is exactly what a kill -9 would leave, since
    // every append was fsynced). The state holds a backend Arc too: release it first.
    state.write().await.persistence = None;
    backend.shutdown();
    drop(backend);

    // Restart: brand-new backend + store over the SAME files. Every fsynced entry
    // must still be present — no lost committed entry.
    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("reopen redb"));
    let mut store2 = EgStore::open(super::DEFAULT_GROUP, backend2.clone(), ctx).unwrap();
    let entries = store2
        .try_get_log_entries(1..=8)
        .await
        .expect("read after restart");
    assert_eq!(
        entries.len(),
        8,
        "every fsynced (committed) log entry must survive the kill+restart"
    );

    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// KG-2.205: multi-group isolation — composite-key log rows for different groups
// do not collide, and groups commit independently.
// ─────────────────────────────────────────────────────────────────────────

/// Two stores for DIFFERENT group ids share ONE redb DB. Their logs are keyed by
/// `(group_id, index)`, so writing group A does not touch group B's log even at the
/// same index — proving the shared-DB composite-key layout isolates groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_group_logs_isolate_on_shared_redb() {
    let dir = fresh_dir("multigroup");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx { state };

    let mut g7 = EgStore::open(7, backend.clone(), ctx.clone()).unwrap();
    let mut g9 = EgStore::open(9, backend.clone(), ctx.clone()).unwrap();

    // Group 7 gets indices 1..=5; group 9 gets indices 1..=3 — same index range,
    // DIFFERENT groups, ONE redb DB.
    g7.append_to_log(
        (1..=5)
            .map(|i| make_log_entry(i, 1, &format!("g7-{i}")))
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    g9.append_to_log(
        (1..=3)
            .map(|i| make_log_entry(i, 1, &format!("g9-{i}")))
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();

    // Each group sees ONLY its own entries.
    assert_eq!(
        g7.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .map(|l| l.index),
        Some(5)
    );
    assert_eq!(
        g9.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .map(|l| l.index),
        Some(3)
    );
    let e7 = g7.try_get_log_entries(1..=10).await.unwrap();
    let e9 = g9.try_get_log_entries(1..=10).await.unwrap();
    assert_eq!(e7.len(), 5, "group 7 has exactly its 5 entries");
    assert_eq!(e9.len(), 3, "group 9 has exactly its 3 entries");

    // Truncating group 7 does NOT affect group 9 (isolation under mutation).
    g7.delete_conflict_logs_since(make_log_entry(3, 1, "x").log_id)
        .await
        .unwrap();
    assert_eq!(g7.try_get_log_entries(1..=10).await.unwrap().len(), 2);
    assert_eq!(
        g9.try_get_log_entries(1..=10).await.unwrap().len(),
        3,
        "group 9's log is untouched by group 7's truncation"
    );

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// KG-2.205: a live two-group cluster (one node, two groups on the SHARED listener)
/// — each group elects its own leader and commits independently, demuxed by group id
/// over ONE TCP listener, all over ONE shared redb DB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_groups_one_node_commit_independently() {
    use super::multi::MultiRaft;

    let dir = fresh_dir("twogroups");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let state = make_state_with_backend(&dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
    };
    let port = free_ports(1)[0];
    let node_id: NodeId = 1;
    let peers: BTreeMap<NodeId, BasicNode> =
        [(node_id, BasicNode::new(format!("127.0.0.1:{port}")))].into();

    let multi = MultiRaft::start(node_id, format!("127.0.0.1:{port}"), backend.clone(), ctx)
        .await
        .expect("start multi");
    // Two groups on the SAME node + SAME shared listener + SAME redb DB.
    multi.create_group(100, peers.clone(), true).await.unwrap();
    multi.create_group(200, peers.clone(), true).await.unwrap();

    // Each group elects its own (single-node) leader.
    for gid in [100u64, 200u64] {
        let g = multi.group(gid).await.expect("group exists");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(node_id) }
        })
        .await
        .unwrap_or_else(|_| panic!("group {gid} must elect a leader"));
    }

    // Route a write to each group; both commit independently.
    multi.router().assign("graphA", 100);
    multi.router().assign("graphB", 200);
    for (graph, gid) in [("graphA", 100u64), ("graphB", 200u64)] {
        let g = multi.group_for_graph(graph).await.expect("group for graph");
        assert_eq!(multi.router().group_of(graph), gid);
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(graph),
            graph_name: graph.to_string(),
            graph_type: GraphType::Global,
            method: Method::AddNode {
                node_id: format!("{graph}-n1"),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"g": graph}))
                    .unwrap(),
            },
        };
        g.client_write(req)
            .await
            .unwrap_or_else(|e| panic!("write to group {gid} failed: {e}"));
    }

    // Both graphs materialized — independent commits on two groups, one node.
    let s = state.read().await;
    assert_eq!(
        s.registry.get("graphA").map(|e| e.core.node_count()),
        Some(1)
    );
    assert_eq!(
        s.registry.get("graphB").map(|e| e.core.node_count()),
        Some(1)
    );
    drop(s);

    // Group lifecycle: close one group; the other is unaffected.
    multi.close_group(100).await.unwrap();
    assert!(multi.group(100).await.is_none(), "group 100 closed");
    assert!(multi.group(200).await.is_some(), "group 200 still running");

    multi.close_group(200).await.unwrap();
    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Distributed graph compute (CONCEPT:KG-2.227) ─────────────────────────────
//
// These prove the cross-shard Pregel engine produces the SAME result as the
// single-graph algorithm run over the UNION graph: a graph split across two shard
// graphs, computed distributed, equals the whole graph computed in one core. Plus the
// incremental connected-components variant equals a from-scratch run on a delta. They
// build a minimal ServerState (no Raft groups needed — the engine reads each shard
// graph's snapshot from the registry) over an in-memory-only backend.
#[cfg(feature = "compute-dist")]
mod dist_compute {
    use super::*;
    use crate::protocol::{DistAlgo, GraphType};
    use crate::raft::pregel::{self, DistResult};

    fn props(n: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "N", "id": n})).unwrap()
    }

    /// Build a state with two shard graphs `shA`/`shB`, the union's nodes partitioned
    /// across them (a node lives in the shard named in `placement`), and EVERY edge
    /// added to the shard that owns its SOURCE. Returns the state + a single-graph
    /// `union` core holding the WHOLE graph for the reference comparison.
    async fn two_shard_state(
        nodes: &[&str],
        edges: &[(&str, &str)],
        placement: &dyn Fn(&str) -> &'static str,
    ) -> (
        Arc<RwLock<ServerState>>,
        std::sync::Arc<crate::graph::GraphCore>,
    ) {
        let dir = std::env::temp_dir().join(format!(
            "eg-pregel-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir.to_string_lossy().to_string(), FsyncPolicy::Each, 256)
                .expect("open redb"),
        );
        let state = make_state_with_backend(&dir.to_string_lossy(), backend).await;
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("shA", GraphType::Global, None)
                .unwrap();
            s.registry
                .create_graph("shB", GraphType::Global, None)
                .unwrap();
            for n in nodes {
                let g = placement(n);
                s.registry
                    .get(g)
                    .unwrap()
                    .core
                    .add_node(n.to_string(), props(n));
            }
            for (u, v) in edges {
                // The edge's endpoints must both exist in the owning shard for petgraph
                // to add it, so we ensure the target node exists in the source's shard
                // too (a cross-shard edge's far endpoint is mirrored as a bare node).
                let g = placement(u);
                let core = &s.registry.get(g).unwrap().core;
                if !core.has_node(v) {
                    core.add_node(v.to_string(), props(v));
                }
                core.add_edge(u.to_string(), v.to_string(), props(u))
                    .unwrap();
            }
        }

        // The single-graph reference: the WHOLE union in ONE core.
        let union = std::sync::Arc::new(crate::graph::GraphCore::new());
        for n in nodes {
            union.add_node(n.to_string(), props(n));
        }
        for (u, v) in edges {
            union
                .add_edge(u.to_string(), v.to_string(), props(u))
                .unwrap();
        }
        (state, union)
    }

    /// Round a score map for tolerant float comparison.
    fn score_map(rows: &[(String, f64)]) -> std::collections::BTreeMap<String, i64> {
        rows.iter()
            .map(|(k, v)| (k.clone(), (v * 1e6).round() as i64))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_pagerank_matches_single_graph() {
        // A union graph split across two shards by a parity rule. Distributed PageRank
        // over the two shards must match the single-graph power iteration on the union.
        let nodes = ["a", "b", "c", "d", "e", "f"];
        let edges = [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("c", "d"), // cross-shard edge (c in shA, d in shB)
            ("d", "e"),
            ("e", "f"),
            ("f", "d"),
        ];
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "b" | "c") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, union) = two_shard_state(&nodes, &edges, &place).await;

        let dist = pregel::run_distributed(
            &state,
            &["shA".into(), "shB".into()],
            &DistAlgo::PageRank {
                damping: 0.85,
                iterations: 50,
            },
        )
        .await
        .unwrap();
        let DistResult::Scores(dist_scores) = dist else {
            panic!("expected scores")
        };

        let single = crate::algorithms::pagerank(&union.topology_snapshot(), 0.85, 50);
        assert_eq!(
            score_map(&dist_scores),
            score_map(&single),
            "cross-shard PageRank must equal single-graph PageRank on the union"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_connected_components_matches_single_graph() {
        // Two components: {a,b,c} (in shA) and {d,e} (split shA/shB) — a cross-shard
        // component. Distributed CC must produce the SAME partition as single-graph CC.
        let nodes = ["a", "b", "c", "d", "e"];
        let edges = [("a", "b"), ("b", "c"), ("d", "e")];
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "b" | "d") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, union) = two_shard_state(&nodes, &edges, &place).await;

        let dist = pregel::run_distributed(
            &state,
            &["shA".into(), "shB".into()],
            &DistAlgo::ConnectedComponents,
        )
        .await
        .unwrap();
        let DistResult::Labels(dist_labels) = dist else {
            panic!("expected labels")
        };

        // Reference: single-graph CC partition. Compare as "same-component" equivalence
        // (the label VALUE differs by indexing, so compare which nodes share a label).
        let single = crate::algorithms::connected_components(&union.topology_snapshot());
        let mut single_comp: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (ci, comp) in single.iter().enumerate() {
            for n in comp {
                single_comp.insert(n.clone(), ci);
            }
        }
        let dist_map: std::collections::HashMap<String, i64> = dist_labels.into_iter().collect();

        // Two nodes share a distributed label IFF they share a single-graph component.
        for i in &nodes {
            for j in &nodes {
                let same_dist = dist_map[*i] == dist_map[*j];
                let same_single = single_comp[*i] == single_comp[*j];
                assert_eq!(
                    same_dist, same_single,
                    "cross-shard CC partition must match single-graph for ({i},{j})"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn incremental_cc_equals_from_scratch() {
        // Start with two separate components, compute CC, then add a cross-shard edge
        // that MERGES them. The incremental recompute (seeded from the prior labeling,
        // re-propagating only the affected region) must equal a from-scratch CC.
        let nodes = ["a", "b", "c", "d"];
        let edges = [("a", "b"), ("c", "d")]; // two components {a,b}, {c,d}
        let place = |n: &str| -> &'static str {
            if matches!(n, "a" | "c") {
                "shA"
            } else {
                "shB"
            }
        };
        let (state, _union) = two_shard_state(&nodes, &edges, &place).await;

        let graphs = ["shA".to_string(), "shB".to_string()];
        let prior = match pregel::run_distributed(&state, &graphs, &DistAlgo::ConnectedComponents)
            .await
            .unwrap()
        {
            DistResult::Labels(l) => l,
            _ => panic!("labels"),
        };

        // DELTA: add edge b→c, merging the two components. b lives in shB; add there.
        {
            let s = state.read().await;
            let core = &s.registry.get("shB").unwrap().core;
            if !core.has_node("c") {
                core.add_node("c".to_string(), props("c"));
            }
            core.add_edge("b".to_string(), "c".to_string(), props("b"))
                .unwrap();
        }

        // Incremental: only b and c are affected by the new edge.
        let affected: std::collections::HashSet<String> =
            ["b".to_string(), "c".to_string()].into_iter().collect();
        let incr = pregel::incremental_connected_components(&state, &graphs, &prior, &affected)
            .await
            .unwrap();

        // From scratch over the same (post-delta) graphs.
        let scratch = match pregel::run_distributed(&state, &graphs, &DistAlgo::ConnectedComponents)
            .await
            .unwrap()
        {
            DistResult::Labels(l) => l,
            _ => panic!("labels"),
        };

        // Compare partitions (same-component equivalence — label values are stable here
        // because both seed from the same vertex indexing, but compare structurally to
        // be robust).
        let im: std::collections::HashMap<String, i64> = incr.into_iter().collect();
        let sm: std::collections::HashMap<String, i64> = scratch.into_iter().collect();
        for i in &nodes {
            for j in &nodes {
                assert_eq!(
                    im[*i] == im[*j],
                    sm[*i] == sm[*j],
                    "incremental CC partition must equal from-scratch for ({i},{j})"
                );
            }
        }
        // All four now in one component (the merge took effect).
        assert!(
            nodes.iter().all(|n| sm[*n] == sm["a"]),
            "the cross-shard merge must unite all four nodes"
        );
    }
}

// ── Materialized view durability (CONCEPT:KG-2.227) ──────────────────────────
//
// A materialized view round-trips through the redb durable tier: the handler computes
// + persists it, and a fresh scan (the boot reload path) recovers the SAME view. Tests
// the redb matview_put/matview_scan + the MatView serde, the durability half of the
// matview lifecycle.
#[cfg(feature = "compute-dist")]
mod matview {
    use super::*;
    use crate::protocol::DistAlgo;
    use crate::raft::pregel::{DistResult, MatView};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matview_persists_and_reloads_from_redb() {
        let dir = std::env::temp_dir().join(format!(
            "eg-matview-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let backend =
            RedbBackend::open(dir.to_string_lossy().to_string(), FsyncPolicy::Each, 256).unwrap();

        let view = MatView {
            name: "ranks".into(),
            graphs: vec!["shA".into(), "shB".into()],
            algo: DistAlgo::PageRank {
                damping: 0.85,
                iterations: 20,
            },
            result: DistResult::Scores(vec![("a".into(), 0.5), ("b".into(), 0.3)]),
        };
        let blob = rmp_serde::to_vec_named(&view).unwrap();
        backend.matview_put("ranks", blob).await.unwrap();

        // The boot reload path: scan recovers the SAME view, byte-for-byte.
        let scanned = backend.matview_scan().unwrap();
        assert_eq!(scanned.len(), 1, "exactly one matview persisted");
        let (name, recovered_blob) = &scanned[0];
        assert_eq!(name, "ranks");
        let recovered: MatView = rmp_serde::from_slice(recovered_blob).unwrap();
        assert_eq!(
            recovered, view,
            "matview must round-trip through redb unchanged"
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
