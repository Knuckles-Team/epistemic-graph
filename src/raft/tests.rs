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
        let started = node::start(
            cluster_cfg(i, &ports),
            &dirs[(i - 1) as usize],
            state.clone(),
        )
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
    killed.listener.abort();
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
        n.listener.abort();
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
