//! Managed in-process Raft cluster for the harness (CONCEPT:AU-KG.ontology.emits-database-ontology-entities).
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::channels::ChannelManager;
use crate::durability::DurabilityPolicy;
use crate::isolation::IsolationLayer;
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

use super::super::config::RaftClusterConfig;
use super::super::node::{self, StartedNode};
use super::super::{NodeId, RaftRequest};
use crate::protocol::{GraphType, Method};

/// The graph every harness write targets.
pub const GRAPH: &str = "__commons__";

static HARNESS_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// One member of the managed cluster — running OR killed (slot kept so it can be
/// restarted over the same files).
struct Member {
    id: NodeId,
    dir: String,
    /// `Some` while running; `None` after a kill (until restart).
    started: Option<StartedNode>,
    /// Held so a restart re-opens the SAME persist dir + ports.
    state: Option<Arc<RwLock<ServerState>>>,
    /// A cleanup failure makes the cluster unsafe to remove. Keep it attached to
    /// the slot so `teardown` cannot accidentally miss a failed `kill` after the
    /// running handle has already been taken.
    cleanup_error: Option<String>,
}

/// A managed N-node in-process Raft cluster the harness drives.
pub struct Cluster {
    members: BTreeMap<NodeId, Member>,
    ports: Vec<u16>,
    n: usize,
    /// Root temp dir (removed on `teardown`).
    root: std::path::PathBuf,
}

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_POLL: Duration = Duration::from_millis(25);
const PORT_ALLOCATION_ATTEMPTS: usize = 32;

/// Build a redb-AUTHORITATIVE `ServerState` rooted at `dir` (mirrors `tests::make_state`)
/// and REHYDRATE its registry from the durable redb store — exactly the M2
/// `load_all` step the real boot path (`main.rs`) runs BEFORE Raft starts
/// (`store::EgStore::open` docs: "the graph DATA is recovered separately by the M2
/// `load_all` path before Raft starts"). Without this a RESTARTED node would come up
/// with an empty graph and only re-acquire data via leader catch-up — which would
/// make the harness mis-report a durable-but-not-yet-replayed write as "lost".
async fn make_state(dir: &str) -> Result<Arc<RwLock<ServerState>>, String> {
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.to_string(), DurabilityPolicy::Each, 4096)
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
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(256)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(256)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 64,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
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
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
    }));
    // M2 rehydration: load the durable graph data from redb into the registry before
    // Raft starts (the real boot path's `load_all`). A fresh dir loads 0; a restarted
    // node loads back every committed-before-kill entry.
    if let Err(error) = backend.load_all(&state).await {
        // `make_state` is also used by the partial-start path. If recovery fails,
        // stop the writer before returning so the failed attempt does not retain a
        // redb file lock while the caller removes its temporary root.
        backend.shutdown();
        return Err(format!("load_all {dir}: {error}"));
    }
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
        // ADR-1 / W1.1: harness nodes advertise a distinct client endpoint per
        // node id so `ClusterMembers`/`PlacementRoute.endpoints` are exercisable
        // against a real (loopback) multi-node topology.
        advertised_client_addr: format!("tcp://127.0.0.1:{}", 20_000 + node_id),
        advertised_tls_server_name: None,
        is_bootstrap: peers.keys().next() == Some(&node_id),
        groups: 1,
        transport_secret: Some(
            super::super::config::RaftTransportSecret::from_material(&[0x5a; 32]).unwrap(),
        ),
    }
}

fn cluster_start_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Pick `n` currently-free localhost ports. `Cluster::start` holds the process-wide
/// startup lock until every selected port is occupied by its real Raft listener,
/// eliminating the parallel in-process allocator race without presenting probe
/// listeners that peers could mistake for Raft endpoints.
fn free_ports(n: usize) -> Result<Vec<u16>, String> {
    for _attempt in 0..PORT_ALLOCATION_ATTEMPTS {
        let mut listeners = Vec::with_capacity(n);
        let mut ports = Vec::with_capacity(n);
        let mut failed = false;
        for _ in 0..n {
            match std::net::TcpListener::bind("127.0.0.1:0") {
                Ok(listener) => match listener.local_addr() {
                    Ok(address) => {
                        ports.push(address.port());
                        listeners.push(Some(listener));
                    }
                    Err(_) => {
                        failed = true;
                        break;
                    }
                },
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed && ports.len() == n {
            return Ok(ports);
        }
        // The listeners are dropped here before trying the next allocation set.
    }
    Err(format!("unable to reserve {n} localhost Raft port(s)"))
}

fn allocate_root(tag: &str) -> Result<std::path::PathBuf, String> {
    for _attempt in 0..PORT_ALLOCATION_ATTEMPTS {
        let sequence = HARNESS_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "eg-harness-{tag}-{}-{sequence}",
            std::process::id(),
        ));
        match std::fs::create_dir(&root) {
            Ok(()) => return Ok(root),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(format!("create harness root {}: {error}", root.display())),
        }
    }
    Err(format!(
        "unable to allocate a unique harness root for tag {tag:?}"
    ))
}

fn port_is_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

async fn wait_for_backend_drop(
    backend: std::sync::Weak<dyn PersistenceBackend>,
    label: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let remaining = backend.strong_count();
        if remaining == 0 {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{label} persistence still has {remaining} live handle(s) after cleanup"
            ));
        }
        tokio::time::sleep(CLEANUP_POLL).await;
    }
}

async fn shutdown_backend(backend: Arc<dyn PersistenceBackend>, label: &str) -> Result<(), String> {
    let weak = Arc::downgrade(&backend);
    let join = tokio::task::spawn_blocking(move || backend.shutdown());
    match tokio::time::timeout(CLEANUP_TIMEOUT, join).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(format!("{label} persistence shutdown task failed: {error}"));
        }
        Err(_) => {
            return Err(format!(
                "{label} persistence shutdown exceeded {}s",
                CLEANUP_TIMEOUT.as_secs()
            ));
        }
    }
    wait_for_backend_drop(weak, label).await
}

async fn clear_state_backend(state: Arc<RwLock<ServerState>>, label: &str) -> Result<(), String> {
    let mut guard = tokio::time::timeout(CLEANUP_TIMEOUT, state.write())
        .await
        .map_err(|_| format!("{label} state write lock did not drain before cleanup"))?;
    let backend = guard.persistence.take();
    guard.raft = None;
    #[cfg(feature = "raft")]
    {
        guard.multi_raft = None;
    }
    drop(guard);
    if let Some(backend) = backend {
        shutdown_backend(backend, label).await?;
    }
    Ok(())
}

async fn cleanup_unstarted_state(
    state: Arc<RwLock<ServerState>>,
    label: &str,
    port: u16,
) -> Result<(), String> {
    let result = clear_state_backend(state, label).await;
    if !port_is_free(port) {
        let port_error = format!("{label} listener port {port} remains bound after failed start");
        return match result {
            Ok(()) => Err(port_error),
            Err(error) => Err(format!("{error}; {port_error}")),
        };
    }
    result
}

impl Cluster {
    /// Start an `n`-node cluster (n should be odd: 3/5). `tag` namespaces the temp
    /// dir. Returns once every node is up; the caller waits for a leader.
    pub async fn start(n: usize, tag: &str) -> Result<Self, String> {
        if n == 0 {
            return Err("cluster must contain at least one node".to_string());
        }
        let _startup_guard = cluster_start_lock().lock().await;
        let ports = free_ports(n)?;
        let root = allocate_root(tag)?;
        let mut cluster = Self {
            members: BTreeMap::new(),
            ports,
            n,
            root,
        };
        for i in 1..=n as u64 {
            let dir = cluster.root.join(format!("node{i}"));
            if let Err(error) = std::fs::create_dir_all(&dir) {
                return Err(cluster
                    .fail_start(format!("mkdir node {i}: {error}"), false)
                    .await);
            }
            let dir = dir.to_string_lossy().to_string();
            let state = match make_state(&dir).await {
                Ok(state) => state,
                Err(error) => {
                    return Err(cluster.fail_start(error, false).await);
                }
            };
            let started = match node::start(cluster_cfg(i, &cluster.ports), state.clone()).await {
                Ok(started) => started,
                Err(error) => {
                    let startup_cleanup = cleanup_unstarted_state(
                        state,
                        &format!("node {i}"),
                        cluster.ports[i as usize - 1],
                    )
                    .await;
                    let startup_cleanup_failed = startup_cleanup.is_err();
                    let reason = match startup_cleanup {
                        Ok(()) => format!("start node {i}: {error}"),
                        Err(cleanup_error) => {
                            format!("start node {i}: {error}; cleanup: {cleanup_error}")
                        }
                    };
                    return Err(cluster.fail_start(reason, startup_cleanup_failed).await);
                }
            };
            state.write().await.raft = Some(started.handle.clone());
            cluster.members.insert(
                i,
                Member {
                    id: i,
                    dir,
                    started: Some(started),
                    state: Some(state),
                    cleanup_error: None,
                },
            );
        }
        Ok(cluster)
    }

    async fn fail_start(&mut self, reason: String, retain_root: bool) -> String {
        let mut errors = vec![reason];
        errors.extend(self.stop_live_members().await);
        if retain_root
            || self.members.values().any(|member| {
                member.started.is_some() || member.state.is_some() || member.cleanup_error.is_some()
            })
        {
            errors.push(format!(
                "harness root retained at {} because live member handles remain",
                self.root.display()
            ));
        } else if let Err(error) = std::fs::remove_dir_all(&self.root) {
            errors.push(format!(
                "remove failed-start harness root {}: {error}",
                self.root.display()
            ));
        }
        errors.join("; ")
    }

    async fn stop_live_members(&mut self) -> Vec<String> {
        let ids: Vec<NodeId> = self.live_ids();
        let mut errors = Vec::new();
        for id in ids {
            if let Err(error) = self.kill(id).await {
                errors.push(format!("node {id} cleanup failed: {error}"));
            }
        }
        errors
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
                if self.is_local_leader(l) {
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
                if l != excluded && self.is_local_leader(l) {
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

    /// Require the candidate's own Raft metrics to say Leader.  `current_leader()`
    /// is a last-known hint and may remain stale on a follower after an election;
    /// returning that hint as an authority would make harness writes race a redirect.
    fn is_local_leader(&self, id: NodeId) -> bool {
        let Some(started) = self
            .members
            .get(&id)
            .and_then(|member| member.started.as_ref())
        else {
            return false;
        };
        let metrics = started.handle.raft.metrics();
        let is_leader = matches!(
            metrics.borrow_watched().state,
            openraft::ServerState::Leader
        );
        is_leader
    }

    /// The `RaftRequest` for an AddNode write of `n{seq}`.
    pub fn add_node_req(seq: u64) -> RaftRequest {
        RaftRequest {
            graph_fname: crate::persist::sanitize(GRAPH),
            graph_name: GRAPH.to_string(),
            graph_type: GraphType::Commons,
            committed_at_ms: 0,
            mutation: super::super::RaftMutationContext::internal(
                "raft-cluster-harness",
                GRAPH,
                &format!("write-{seq}"),
                seq,
                0,
            ),
            command: super::super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: format!("n{seq}"),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"seq": seq}))
                        .unwrap(),
                },
                "harness",
            )
            .unwrap(),
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

    /// The `RaftRequest` for a `CreateGraph` of an ARBITRARY named graph — the
    /// counterpart to [`Self::add_node_req`] needed to exercise the
    /// CreateGraph-immediately-followed-by-a-write catch-up/replay sequence
    /// (`impl/raft-catchup-apply`). Replicates as `NativeMutationCommand::GraphLifecycle`,
    /// the SAME encoding the real client `Method::CreateGraph` path produces.
    pub fn create_graph_req(graph_name: &str, graph_type: GraphType, seq: u64) -> RaftRequest {
        let secret = "harness";
        let command = super::super::NativeMutationCommand::from_public_method(
            Method::CreateGraph {
                graph_name: graph_name.to_string(),
                graph_type,
            },
            secret,
        )
        .map_err(|_| "CreateGraph must be an inventoried native domain")
        .unwrap();
        RaftRequest {
            graph_fname: crate::persist::sanitize(graph_name),
            graph_name: graph_name.to_string(),
            graph_type,
            committed_at_ms: 0,
            mutation: super::super::RaftMutationContext::internal(
                "raft-cluster-harness",
                graph_name,
                &format!("create-{seq}"),
                seq,
                0,
            ),
            command: super::super::ReplicatedMutation::Native { command },
        }
    }

    /// The `RaftRequest` for an AddNode write of `n{seq}` into an ARBITRARY named
    /// graph (generalizes [`Self::add_node_req`] off the fixed [`GRAPH`] constant).
    pub fn add_node_req_for(graph_name: &str, graph_type: GraphType, seq: u64) -> RaftRequest {
        RaftRequest {
            graph_fname: crate::persist::sanitize(graph_name),
            graph_name: graph_name.to_string(),
            graph_type,
            committed_at_ms: 0,
            mutation: super::super::RaftMutationContext::internal(
                "raft-cluster-harness",
                graph_name,
                &format!("write-{seq}"),
                seq,
                0,
            ),
            command: super::super::ReplicatedMutation::graph(
                Method::AddNode {
                    node_id: format!("n{seq}"),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"seq": seq}))
                        .unwrap(),
                },
                "harness",
            )
            .unwrap(),
        }
    }

    /// Issue an arbitrary pre-built `RaftRequest` through `leader`'s `client_write`.
    /// `Ok` ⇒ committed+applied on a quorum (the leader's own apply — NOT proof any
    /// particular follower has caught up).
    pub async fn write_req(&self, leader: NodeId, req: RaftRequest) -> Result<(), String> {
        let m = self
            .members
            .get(&leader)
            .and_then(|m| m.started.as_ref())
            .ok_or_else(|| format!("node {leader} not running"))?;
        m.handle.client_write(req).await.map(|_| ())
    }

    /// Applied node-count for an ARBITRARY graph on node `id` (0 if killed/absent/
    /// the graph is not yet resident there).
    pub async fn node_count_in(&self, id: NodeId, graph_name: &str) -> usize {
        let Some(m) = self.members.get(&id) else {
            return 0;
        };
        let Some(state) = &m.state else { return 0 };
        let s = state.read().await;
        s.registry
            .get(graph_name)
            .map(|e| e.core.node_count())
            .unwrap_or(0)
    }

    /// Does node `id`'s applied state contain `node_id` in `graph_name`?
    pub async fn has_node_in(&self, id: NodeId, graph_name: &str, node_id: &str) -> bool {
        let Some(m) = self.members.get(&id) else {
            return false;
        };
        let Some(state) = &m.state else {
            return false;
        };
        let s = state.read().await;
        s.registry
            .get(graph_name)
            .map(|e| e.core.has_node(node_id))
            .unwrap_or(false)
    }

    /// Is `graph_name` REGISTERED (resident or catalog-only) on node `id`?
    pub async fn graph_exists(&self, id: NodeId, graph_name: &str) -> bool {
        let Some(m) = self.members.get(&id) else {
            return false;
        };
        let Some(state) = &m.state else {
            return false;
        };
        state.read().await.registry.exists(graph_name)
    }

    /// The applied log index node `id` has reached (`None` if killed/absent or no
    /// entry has ever applied).
    pub async fn applied_index(&self, id: NodeId) -> Option<u64> {
        let m = self.members.get(&id)?;
        let s = m.started.as_ref()?;
        let metrics = s.handle.raft.metrics();
        let last_applied = metrics.borrow_watched().last_applied;
        last_applied.map(|l| l.index)
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
        let mut started = started;
        // Close public admission first. A request that begins after this write
        // cannot obtain a stale Raft route or a persistence handle while shutdown
        // is draining the node. In-flight requests that already cloned a handle
        // are crash semantics and are cancelled by the Raft/backend shutdown.
        let state = m.state.clone();
        let backend = if let Some(state) = &state {
            let mut guard = match tokio::time::timeout(CLEANUP_TIMEOUT, state.write()).await {
                Ok(guard) => guard,
                Err(_) => {
                    let error = format!("node {id} state write lock did not drain before cleanup");
                    m.cleanup_error = Some(error.clone());
                    m.started = Some(started);
                    return Err(error);
                }
            };
            let backend = guard.persistence.take();
            guard.raft = None;
            #[cfg(feature = "raft")]
            {
                // `attach_multi_raft` installs a clone in ServerState. Clear it
                // before dropping StartedNode so it cannot retain the group store.
                guard.multi_raft = None;
            }
            backend
        } else {
            None
        };

        // Stop and join the listener plus all accepted/deferred connection tasks
        // only after the public state is fail-closed. The node-owned report task
        // owns a MultiRaft clone outside that task registry and is joined first.
        started.stop_background_tasks().await;
        if let Err(error) = tokio::time::timeout(CLEANUP_TIMEOUT, started.multi.shutdown()).await {
            let error = format!("node {id} MultiRaft shutdown exceeded cleanup timeout: {error}");
            m.cleanup_error = Some(error.clone());
            m.started = Some(started);
            // `backend` is intentionally dropped here. MultiRaft retains the
            // authority handle for the next idempotent cleanup attempt.
            return Err(error);
        }

        // Raft and every connection task have now been stopped and joined. Drop
        // their backend handles before invoking the synchronous writer shutdown.
        drop(started);
        m.state = None;
        if let Some(backend) = backend {
            if let Err(error) = shutdown_backend(backend, &format!("node {id}")).await {
                m.cleanup_error = Some(error.clone());
                return Err(error);
            }
        }
        m.cleanup_error = None;
        Ok(())
    }

    /// RESTART a previously-killed node over the SAME persist dir + port. Its durable
    /// redb log replays locally (CONCEPT:EG-KG.storage.one-fsync-covers-raft) and it rejoins, catching up via the
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
        let started = match node::start(cluster_cfg(id, &self.ports), state.clone()).await {
            Ok(started) => started,
            Err(error) => {
                let cleanup = cleanup_unstarted_state(
                    state,
                    &format!("restart node {id}"),
                    self.ports[id as usize - 1],
                )
                .await;
                return match cleanup {
                    Ok(()) => Err(format!("restart node {id}: {error}")),
                    Err(cleanup_error) => Err(format!(
                        "restart node {id}: {error}; cleanup: {cleanup_error}"
                    )),
                };
            }
        };
        state.write().await.raft = Some(started.handle.clone());
        let m = self.members.get_mut(&id).unwrap();
        m.started = Some(started);
        m.state = Some(state);
        m.cleanup_error = None;
        Ok(())
    }

    /// Best-effort synchronous abort used only by test cleanup when async
    /// unwinding cannot await [`Self::teardown`].  The normal path must use
    /// `teardown`, which awaits each Raft shutdown before dropping persistence.
    #[cfg(test)]
    pub fn abort_sync(&mut self) {
        let runtime = tokio::runtime::Handle::try_current().ok();
        let mut started_nodes = Vec::new();
        let mut states = Vec::new();
        for member in self.members.values_mut() {
            if let Some(started) = member.started.take() {
                started.multi.stop_listener();
                started_nodes.push(started);
            }
            if let Some(state) = member.state.take() {
                states.push(state);
            }
        }
        super::super::network::partition::heal();
        let root = self.root.clone();
        let ports = self.ports.clone();
        let Some(runtime) = runtime else {
            // A synchronous Drop path cannot prove that Raft and its writer
            // threads have drained. Leave the root in place rather than deleting
            // data while detached handles may still own it.
            tracing::error!(
                root = %root.display(),
                "cluster abort has no Tokio runtime; temporary root retained until an explicit teardown"
            );
            return;
        };
        runtime.spawn(async move {
            let mut errors = Vec::new();
            for mut started in started_nodes {
                started.stop_background_tasks().await;
                if let Err(error) =
                    tokio::time::timeout(CLEANUP_TIMEOUT, started.multi.shutdown()).await
                {
                    errors.push(format!("MultiRaft shutdown timed out: {error}"));
                }
                drop(started);
            }
            for state in states {
                if let Err(error) = clear_state_backend(state, "aborted cluster").await {
                    errors.push(error);
                }
            }
            if errors.is_empty() && ports.iter().copied().all(port_is_free) {
                if let Err(error) = std::fs::remove_dir_all(&root) {
                    errors.push(format!(
                        "remove aborted harness root {}: {error}",
                        root.display()
                    ));
                }
            } else if errors.is_empty() {
                errors.push("aborted cluster listener port remains bound".to_string());
            }
            if !errors.is_empty() {
                tracing::error!(
                    root = %root.display(),
                    errors = ?errors,
                    "cluster abort cleanup failed; temporary root retained"
                );
            }
        });
    }

    pub fn size(&self) -> usize {
        self.n
    }

    /// Shut everything down in place and remove the temp dir. This form lets an
    /// `Arc<Mutex<Cluster>>` owner clean up even when an unexpected extra Arc
    /// prevents ownership recovery.
    pub async fn shutdown_in_place(&mut self) -> Result<(), String> {
        let mut errors = self.stop_live_members().await;
        super::super::network::partition::heal();
        let unfinished: Vec<String> = self
            .members
            .values()
            .filter_map(|member| {
                if member.started.is_some() || member.state.is_some() {
                    Some(format!(
                        "node {} still owns live harness handles",
                        member.id
                    ))
                } else {
                    member.cleanup_error.clone()
                }
            })
            .collect();
        errors.extend(unfinished);
        if errors.is_empty() && self.ports.iter().copied().all(port_is_free) {
            if let Err(error) = std::fs::remove_dir_all(&self.root) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    errors.push(format!(
                        "remove harness root {} failed: {error}",
                        self.root.display()
                    ));
                }
            }
        } else if errors.is_empty() {
            errors.push("cluster listener port remains bound after shutdown".to_string());
        }
        if !errors.is_empty() {
            return Err(format!(
                "cluster teardown failed; temporary root retained at {}: {}",
                self.root.display(),
                errors.join("; ")
            ));
        }
        Ok(())
    }

    /// Shut everything down and remove the temp dir.
    pub async fn teardown(mut self) {
        if let Err(error) = self.shutdown_in_place().await {
            panic!("{error}");
        }
    }
}

#[cfg(test)]
#[path = "resource_acceptance_test.rs"]
mod resource_acceptance_test;
