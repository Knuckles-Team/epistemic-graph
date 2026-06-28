//! Managed in-process Raft cluster for the harness (CONCEPT:KG-2.212).
//!
//! Wraps the same 3-node in-process machinery `raft::tests` uses (each node a
//! `ServerState` over its OWN redb-AUTHORITATIVE persist dir, one shared listener
//! per node demuxing the DEFAULT group) but exposes the controls the nemesis needs:
//! start the cluster, find the current leader/term, KILL a node (`kill -9` analog:
//! drop its MultiRaft + backend so its on-disk redb is exactly what a crash leaves),
//! RESTART a killed node over the same files, and read a node's applied state.
//!
//! This is the cluster-under-test. The `Nemesis` schedules faults against it and the
//! `LoadGen` drives writes through its leader.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;
use crate::wal_service::FsyncPolicy;

use super::super::config::RaftClusterConfig;
use super::super::node::{self, StartedNode};
use super::super::{NodeId, RaftRequest};
use crate::protocol::{GraphType, Method};

/// The graph every harness write targets.
pub const GRAPH: &str = "__commons__";

/// One member of the managed cluster — running OR killed (slot kept so it can be
/// restarted over the same files).
struct Member {
    id: NodeId,
    dir: String,
    /// `Some` while running; `None` after a kill (until restart).
    started: Option<StartedNode>,
    /// Held so a restart re-opens the SAME persist dir + ports.
    state: Option<Arc<RwLock<ServerState>>>,
}

/// A managed N-node in-process Raft cluster the harness drives.
pub struct Cluster {
    members: BTreeMap<NodeId, Member>,
    ports: Vec<u16>,
    n: usize,
    /// Root temp dir (removed on `teardown`).
    root: std::path::PathBuf,
}

/// Build a redb-AUTHORITATIVE `ServerState` rooted at `dir` (mirrors `tests::make_state`)
/// and REHYDRATE its registry from the durable redb store — exactly the M2
/// `load_all` step the real boot path (`main.rs`) runs BEFORE Raft starts
/// (`store::EgStore::open` docs: "the graph DATA is recovered separately by the M2
/// `load_all` path before Raft starts"). Without this a RESTARTED node would come up
/// with an empty graph and only re-acquire data via leader catch-up — which would
/// make the harness mis-report a durable-but-not-yet-replayed write as "lost".
async fn make_state(dir: &str) -> Result<Arc<RwLock<ServerState>>, String> {
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.to_string(), FsyncPolicy::Each, 4096)
            .map_err(|e| format!("open redb {dir}: {e}"))?,
    );
    let state = Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "harness".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend.clone()),
        redb_authoritative: true,
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(256)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(256)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 64,
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
    }));
    // M2 rehydration: load the durable graph data from redb into the registry before
    // Raft starts (the real boot path's `load_all`). A fresh dir loads 0; a restarted
    // node loads back every committed-before-kill entry.
    backend
        .load_all(&state)
        .await
        .map_err(|e| format!("load_all {dir}: {e}"))?;
    Ok(state)
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

/// Pick `n` currently-free localhost ports by binding then dropping.
fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..n {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        ports.push(l.local_addr().unwrap().port());
        listeners.push(l);
    }
    ports
}

impl Cluster {
    /// Start an `n`-node cluster (n should be odd: 3/5). `tag` namespaces the temp
    /// dir. Returns once every node is up; the caller waits for a leader.
    pub async fn start(n: usize, tag: &str) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "eg-harness-{tag}-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let ports = free_ports(n);
        let mut members = BTreeMap::new();
        for i in 1..=n as u64 {
            let dir = root.join(format!("node{i}"));
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
            let dir = dir.to_string_lossy().to_string();
            let state = make_state(&dir).await?;
            let started = node::start(cluster_cfg(i, &ports), state.clone())
                .await
                .map_err(|e| format!("start node {i}: {e}"))?;
            state.write().await.raft = Some(started.handle.clone());
            members.insert(
                i,
                Member {
                    id: i,
                    dir,
                    started: Some(started),
                    state: Some(state),
                },
            );
        }
        Ok(Self {
            members,
            ports,
            n,
            root,
        })
    }

    /// Node ids of CURRENTLY-RUNNING members.
    pub fn live_ids(&self) -> Vec<NodeId> {
        self.members
            .values()
            .filter(|m| m.started.is_some())
            .map(|m| m.id)
            .collect()
    }

    pub fn all_ids(&self) -> Vec<NodeId> {
        self.members.keys().copied().collect()
    }

    /// Wait until a running node reports a current leader; returns its id.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Option<NodeId> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(l) = self.current_leader().await {
                // Confirm the reported leader is itself alive AND actually thinks it
                // is the leader (avoids a stale follower hint).
                if self.is_running(l) {
                    return Some(l);
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        None
    }

    /// Wait for a leader OTHER than `excluded`.
    pub async fn wait_for_leader_excluding(
        &self,
        excluded: NodeId,
        timeout: Duration,
    ) -> Option<NodeId> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(l) = self.current_leader().await {
                if l != excluded && self.is_running(l) {
                    return Some(l);
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        None
    }

    /// The leader any running node currently reports (may be `None` during election).
    pub async fn current_leader(&self) -> Option<NodeId> {
        for m in self.members.values() {
            if let Some(s) = &m.started {
                if let Some(l) = s.handle.raft.current_leader().await {
                    return Some(l);
                }
            }
        }
        None
    }

    /// `(node_id, term, is_leader)` for every running node — the raw material the
    /// single-leader-per-term invariant is checked over.
    pub async fn leadership_view(&self) -> Vec<(NodeId, u64, bool)> {
        let mut out = Vec::new();
        for m in self.members.values() {
            if let Some(s) = &m.started {
                let metrics = s.handle.raft.metrics();
                let m = metrics.borrow_watched();
                out.push((
                    m.id,
                    m.current_term,
                    matches!(m.state, openraft::ServerState::Leader),
                ));
            }
        }
        out
    }

    pub fn is_running(&self, id: NodeId) -> bool {
        self.members
            .get(&id)
            .map(|m| m.started.is_some())
            .unwrap_or(false)
    }

    /// The `RaftRequest` for an AddNode write of `n{seq}`.
    pub fn add_node_req(seq: u64) -> RaftRequest {
        RaftRequest {
            graph_fname: crate::persist::sanitize(GRAPH),
            graph_name: GRAPH.to_string(),
            graph_type: GraphType::Commons,
            method: Method::AddNode {
                node_id: format!("n{seq}"),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"seq": seq}))
                    .unwrap(),
            },
        }
    }

    /// Issue a write through `leader`'s `client_write`. `Ok` ⇒ committed+applied.
    pub async fn write_via(&self, leader: NodeId, seq: u64) -> Result<(), String> {
        let m = self
            .members
            .get(&leader)
            .and_then(|m| m.started.as_ref())
            .ok_or_else(|| format!("node {leader} not running"))?;
        m.handle
            .client_write(Self::add_node_req(seq))
            .await
            .map(|_| ())
    }

    /// Applied node-count in `GRAPH` on node `id` (0 if killed/absent).
    pub async fn node_count(&self, id: NodeId) -> usize {
        let Some(m) = self.members.get(&id) else {
            return 0;
        };
        let Some(state) = &m.state else { return 0 };
        let s = state.read().await;
        s.registry
            .get(GRAPH)
            .map(|e| e.core.node_count())
            .unwrap_or(0)
    }

    /// Does node `id`'s applied state contain `node_id` (e.g. `n{seq}`)?
    pub async fn has_node(&self, id: NodeId, node_id: &str) -> bool {
        let Some(m) = self.members.get(&id) else {
            return false;
        };
        let Some(state) = &m.state else { return false };
        let s = state.read().await;
        s.registry
            .get(GRAPH)
            .map(|e| e.core.has_node(node_id))
            .unwrap_or(false)
    }

    /// KILL a node (the `kill -9` analog): stop its listener, shut down its Raft
    /// group, and DROP its persistence backend so the on-disk redb is exactly what a
    /// crashed process leaves (every acked write was fsynced before the ack). The
    /// slot + dir + port are kept so `restart` can re-open over the same files.
    pub async fn kill(&mut self, id: NodeId) -> Result<(), String> {
        let m = self
            .members
            .get_mut(&id)
            .ok_or_else(|| format!("no node {id}"))?;
        let started = m
            .started
            .take()
            .ok_or_else(|| format!("node {id} already killed"))?;
        // Release the state's backend Arc FIRST so the drop below actually closes the
        // redb file lock (mirrors `tests::fault_injection_*`).
        if let Some(state) = &m.state {
            state.write().await.persistence = None;
            state.write().await.raft = None;
        }
        started.multi.stop_listener();
        let _ = started.handle.raft.shutdown().await;
        // Drop the MultiRaft (its groups + the shared backend handle it holds).
        drop(started);
        // Drop the old ServerState so its backend Arc is the last one and the redb
        // file lock releases — a true process-exit analog for restart.
        m.state = None;
        Ok(())
    }

    /// RESTART a previously-killed node over the SAME persist dir + port. Its durable
    /// redb log replays locally (CONCEPT:KG-2.204) and it rejoins, catching up via the
    /// leader's append/snapshot.
    pub async fn restart(&mut self, id: NodeId) -> Result<(), String> {
        let dir = {
            let m = self
                .members
                .get(&id)
                .ok_or_else(|| format!("no node {id}"))?;
            if m.started.is_some() {
                return Err(format!("node {id} is running, not killed"));
            }
            m.dir.clone()
        };
        let state = make_state(&dir).await?;
        let started = node::start(cluster_cfg(id, &self.ports), state.clone())
            .await
            .map_err(|e| format!("restart node {id}: {e}"))?;
        state.write().await.raft = Some(started.handle.clone());
        let m = self.members.get_mut(&id).unwrap();
        m.started = Some(started);
        m.state = Some(state);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.n
    }

    /// Shut everything down and remove the temp dir.
    pub async fn teardown(mut self) {
        let ids: Vec<NodeId> = self.live_ids();
        for id in ids {
            let _ = self.kill(id).await;
        }
        super::super::network::partition::heal();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
