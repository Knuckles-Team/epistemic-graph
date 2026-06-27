//! Multi-Raft manager + routing (CONCEPT:KG-2.205).
//!
//! A [`MultiRaft`] holds N openraft groups in one process, keyed by [`GroupId`],
//! each its OWN [`store::EgStore`] state machine applying to its own graph data —
//! but ALL groups share:
//!
//! * **ONE redb Database** (the M2 `graph.redb`): every group's durable log + meta
//!   is keyed by `(group_id, …)` (CONCEPT:KG-2.204). This is the spike's FD-ceiling
//!   fix — NOT a redb file per group.
//! * **ONE TCP listener per node**: the [`network`] frame is tagged with the group
//!   id, and the shared listener demuxes each RPC to the right group's [`EgRaft`]
//!   (the spike's shared-channel design, productionized).
//!
//! ### Routing — one graph = one group (this increment)
//!
//! [`GroupRouter`] maps a graph name → [`GroupId`]. In this increment it maps EVERY
//! graph to [`DEFAULT_GROUP`], so a single-group cluster behaves exactly like the
//! pre-multi single-group path (now with a durable log). The machinery to run N
//! groups is here and exercised by tests (`multi_group_isolation`), so splitting a
//! keyspace into its own group later is a routing change, not a storage change.
//!
//! ### Group = transaction boundary (explicit follow-up: cross-group txns)
//!
//! A graph belongs to exactly one group, and a transaction stays inside one group.
//! Atomically touching two graphs in two DIFFERENT groups (a cross-group / 2-phase
//! commit) is a SEPARATE, larger project and is deliberately NOT in this increment
//! (documented follow-up CONCEPT:KG-2.207). The router enforces the boundary by
//! construction (each graph resolves to one group).

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use openraft::storage::Adaptor;
use openraft::BasicNode;
use openraft::Config;
use tokio::sync::RwLock;

use super::network::{self, GroupRpc};
use super::store::EgStore;
use super::{
    AppCtx, EgRaft, GroupId, NodeId, RaftHandle, RaftRequest, RaftResponse, DEFAULT_GROUP,
};

/// Routes a graph name to the Raft group that owns it (CONCEPT:KG-2.205 +
/// KG-2.266). One graph belongs to exactly one group. Resolution order:
///
/// 1. an explicit per-graph **override** ([`assign`], used by reshard + tests) —
///    highest priority, a graph pinned to a specific group;
/// 2. otherwise the **tenant-range ring** ([`set_group_ring`], CONCEPT:KG-2.266):
///    the graph name hashes (stable FNV-1a) onto a sorted set of group ids, so
///    un-pinned tenants SPREAD across groups instead of all landing on one;
/// 3. otherwise [`DEFAULT_GROUP`].
///
/// With NO ring configured (the default) every un-pinned graph maps to
/// [`DEFAULT_GROUP`] — byte-for-byte the single-group scaffold behavior.
///
/// [`assign`]: GroupRouter::assign
/// [`set_group_ring`]: GroupRouter::set_group_ring
#[derive(Default)]
pub struct GroupRouter {
    overrides: DashMap<String, GroupId>,
    /// The tenant-range ring (CONCEPT:KG-2.266): a sorted, de-duplicated set of group
    /// ids that un-pinned graphs hash-distribute across. EMPTY ⇒ single-group default.
    ring: parking_lot::RwLock<Vec<GroupId>>,
}

/// Stable, deterministic FNV-1a hash of a graph name → ring slot. Stable forever and
/// identical on every node (NOT the randomized `RandomState`), so all nodes route a
/// given tenant to the SAME group. Routing is recomputed live, never persisted.
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

impl GroupRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// The group that owns `graph_name`. Override → tenant-range ring → [`DEFAULT_GROUP`].
    pub fn group_of(&self, graph_name: &str) -> GroupId {
        if let Some(g) = self.overrides.get(graph_name) {
            return *g;
        }
        let ring = self.ring.read();
        if ring.is_empty() {
            return DEFAULT_GROUP;
        }
        ring[(fnv1a(graph_name) % ring.len() as u64) as usize]
    }

    /// Configure the tenant-range ring (CONCEPT:KG-2.266): un-pinned graphs
    /// hash-distribute across these group ids. The set is sorted + de-duplicated so
    /// the mapping is stable regardless of input order. Pass an EMPTY slice to
    /// collapse back to the single-group default. Replaces any prior ring.
    pub fn set_group_ring(&self, groups: &[GroupId]) {
        let mut ring: Vec<GroupId> = groups.to_vec();
        ring.sort_unstable();
        ring.dedup();
        *self.ring.write() = ring;
    }

    /// The current tenant-range ring (sorted group ids); empty ⇒ single-group default.
    pub fn group_ring(&self) -> Vec<GroupId> {
        self.ring.read().clone()
    }

    /// Pin a graph to a specific group (used by multi-group setups/tests). One graph
    /// → one group: re-assigning is allowed but a graph is never in two groups.
    pub fn assign(&self, graph_name: &str, group_id: GroupId) {
        self.overrides.insert(graph_name.to_string(), group_id);
    }

    /// The distinct groups a set of graph names spans (CONCEPT:KG-2.222). A txn whose
    /// write-set touches graphs that resolve to >1 group is a CROSS-SHARD txn and must
    /// route through the 2PC coordinator; a set that resolves to exactly one group is
    /// the single-group FAST PATH (unchanged). This is the span-detection the commit
    /// path uses to decide between the two.
    pub fn span<I, S>(&self, graph_names: I) -> std::collections::BTreeSet<GroupId>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        graph_names
            .into_iter()
            .map(|g| self.group_of(g.as_ref()))
            .collect()
    }

    /// True when `graph_names` span ≥2 groups — i.e. a transaction over them is
    /// cross-shard and needs 2PC. Exactly the gate `Commit` checks before choosing
    /// the coordinator over the single-group fast path.
    pub fn is_cross_shard<I, S>(&self, graph_names: I) -> bool
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.span(graph_names).len() >= 2
    }
}

/// A single running group: its `EgRaft` handle + the node id it runs as. Cloneable.
#[derive(Clone)]
pub struct Group {
    pub raft: EgRaft,
    pub node_id: NodeId,
}

impl Group {
    /// Route a durable mutation through THIS group's consensus (leader `client_write`).
    pub async fn client_write(&self, req: RaftRequest) -> Result<RaftResponse, String> {
        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => Err(format!("raft client_write: {e}")),
        }
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }
}

/// The per-node multi-group manager (CONCEPT:KG-2.205). Holds the live group map +
/// the single shared RPC listener that demuxes by group id. The group map is shared
/// (`Arc<RwLock<…>>`) with the listener so a `create_group` is visible to incoming
/// RPCs immediately.
pub struct MultiRaft {
    node_id: NodeId,
    groups: Arc<RwLock<BTreeMap<GroupId, EgRaft>>>,
    router: Arc<GroupRouter>,
    /// The shared M2 backend handle every group's store is opened over.
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    ctx: AppCtx,
    /// Shared per-peer outbound connection pool (CONCEPT:KG-2.265) — one per node,
    /// reused by every group's network clients.
    pool: Arc<network::PeerPool>,
    listener_handle: tokio::task::JoinHandle<()>,
    /// Per-tenant migration locks (CONCEPT:KG-2.224). A reshard or hibernate of a
    /// graph takes its lock so the two cannot race / interleave for one tenant; ops
    /// on DIFFERENT graphs proceed concurrently. Lazily created per graph name.
    tenant_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl MultiRaft {
    /// Start the per-node shared listener. Groups are added via [`create_group`].
    pub async fn start(
        node_id: NodeId,
        bind_addr: String,
        backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
        ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        let groups: Arc<RwLock<BTreeMap<GroupId, EgRaft>>> = Arc::new(RwLock::new(BTreeMap::new()));
        let groups_for_listener = groups.clone();
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| format!("raft multi-listener bind {bind_addr}: {e}"))?;
        tracing::info!("Raft multi-group RPC listening on {bind_addr}");
        let listener_handle = tokio::spawn(async move {
            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let groups = groups_for_listener.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_conn(stream, groups).await {
                        tracing::debug!("raft multi rpc conn ended: {e}");
                    }
                });
            }
        });
        Ok(Arc::new(Self {
            node_id,
            groups,
            router: Arc::new(GroupRouter::new()),
            backend,
            ctx,
            pool: network::PeerPool::new(),
            listener_handle,
            tenant_locks: Arc::new(DashMap::new()),
        }))
    }

    /// Acquire the per-tenant migration lock for `graph_name` (CONCEPT:KG-2.224), so a
    /// reshard and a hibernate of the SAME graph serialize. Lazily creates the lock.
    /// Returns an owned guard the caller holds for the duration of the migration.
    pub async fn tenant_lock(&self, graph_name: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .tenant_locks
            .entry(graph_name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    /// Ensure group `gid` is running on this node, creating it (single-member,
    /// bootstrap) if absent — the resharding target-group seam (CONCEPT:KG-2.224).
    /// Idempotent: a no-op if the group already runs. The new group shares the SAME
    /// listener + `graph.redb`; its durable log/meta are keyed by `gid`.
    pub async fn ensure_group(&self, gid: GroupId) -> Result<(), String> {
        if self.groups.read().await.contains_key(&gid) {
            return Ok(());
        }
        let peers: BTreeMap<NodeId, BasicNode> = [(
            self.node_id,
            BasicNode::new(format!("self-{}", self.node_id)),
        )]
        .into();
        self.create_group(gid, peers, true).await
    }

    pub fn router(&self) -> Arc<GroupRouter> {
        self.router.clone()
    }

    /// The node's shared per-peer connection pool (CONCEPT:KG-2.265) — exposed for
    /// metrics/tests (e.g. asserting RPCs reused a warm connection).
    pub fn pool(&self) -> Arc<network::PeerPool> {
        self.pool.clone()
    }

    /// Configure a tenant-range ring of `n_groups` groups (CONCEPT:KG-2.266) and bring
    /// each up on THIS node, distributing un-pinned graphs across them. Group ids are
    /// `0..n_groups` (so [`DEFAULT_GROUP`] = 0 is always in the ring). Each group is a
    /// single-member bootstrap on this node — the multi-NODE membership join per group
    /// is a separate follow-up (see the M2 status doc). `n_groups <= 1` leaves the ring
    /// empty (the single-group default), so this is a safe superset.
    pub async fn configure_group_ring(self: &Arc<Self>, n_groups: u64) -> Result<(), String> {
        if n_groups <= 1 {
            return Ok(());
        }
        let groups: Vec<GroupId> = (0..n_groups).collect();
        for &gid in &groups {
            self.ensure_group(gid).await?;
        }
        self.router.set_group_ring(&groups);
        Ok(())
    }

    /// The shared `ServerState` (registry + persistence) every group applies into —
    /// reached via the manager's [`AppCtx`]. Used by the cross-shard 2PC coordinator
    /// (CONCEPT:KG-2.222) to validate slices against live group state.
    pub fn app_state(&self) -> Arc<RwLock<crate::server::ServerState>> {
        self.ctx.state.clone()
    }

    /// The shared M2 backend (for the cross-shard coordinator's durable 2PC records).
    pub fn backend(&self) -> Arc<dyn crate::server::persistence::PersistenceBackend> {
        self.backend.clone()
    }

    /// Create + start group `gid` on this node with the given peer set. The store is
    /// opened over the SHARED M2 backend keyed by `gid` (CONCEPT:KG-2.204), so all
    /// groups share ONE `graph.redb`. The lowest-id member bootstraps.
    pub async fn create_group(
        &self,
        gid: GroupId,
        peers: BTreeMap<NodeId, BasicNode>,
        is_bootstrap: bool,
    ) -> Result<(), String> {
        if self.groups.read().await.contains_key(&gid) {
            return Err(format!("group {gid} already open on node {}", self.node_id));
        }
        // The store's ctx carries the router so its snapshot dump is SCOPED to this
        // group's tenant-range graphs (CONCEPT:KG-2.267), not the whole registry.
        let store_ctx = AppCtx {
            state: self.ctx.state.clone(),
            router: Some(self.router.clone()),
        };
        let store = EgStore::open(gid, self.backend.clone(), store_ctx)?;
        let raft_config = Arc::new(
            Config {
                cluster_name: format!("epistemic-graph-g{gid}"),
                heartbeat_interval: 250,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()
            .map_err(|e| format!("invalid raft config: {e}"))?,
        );
        let (log_store, state_machine) = Adaptor::new(store);
        let network = network::GroupNetworkFactory::new(gid, self.node_id, self.pool.clone());
        let raft: EgRaft =
            openraft::Raft::new(self.node_id, raft_config, network, log_store, state_machine)
                .await
                .map_err(|e| format!("raft new g{gid}: {e}"))?;
        self.groups.write().await.insert(gid, raft.clone());

        if is_bootstrap {
            let raft_for_init = raft.clone();
            tokio::spawn(async move {
                // Give peers a moment to bind their listener / open their group.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                match raft_for_init.initialize(peers).await {
                    Ok(()) => tracing::info!("raft: group {gid} initialized by bootstrap node"),
                    Err(e) => {
                        tracing::info!(
                            "raft: group {gid} initialize skipped (already formed?): {e}"
                        )
                    }
                }
            });
        }
        tracing::info!("Raft group {gid} created on node {}", self.node_id);
        Ok(())
    }

    /// A cloneable handle to one group's `EgRaft` (None if this node doesn't run it).
    pub async fn group(&self, gid: GroupId) -> Option<Group> {
        self.groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .map(|raft| Group {
                raft,
                node_id: self.node_id,
            })
    }

    /// The group that owns `graph_name`, ready to route a write through.
    pub async fn group_for_graph(&self, graph_name: &str) -> Option<Group> {
        self.group(self.router.group_of(graph_name)).await
    }

    /// Close (shut down + drop) a group on this node — group lifecycle, the elastic
    /// resharding/destroy seam. The group's durable log/meta rows are NOT deleted
    /// here (a destroy-with-GC is a documented follow-up); this stops replication
    /// and frees the in-RAM group. Idempotent.
    pub async fn close_group(&self, gid: GroupId) -> Result<(), String> {
        let raft = self.groups.write().await.remove(&gid);
        if let Some(raft) = raft {
            let _ = raft.shutdown().await;
            tracing::info!("Raft group {gid} closed on node {}", self.node_id);
        }
        Ok(())
    }

    /// A [`RaftHandle`] that routes a graph's writes through ITS group (the dispatch
    /// seam). Returns `None` if the graph's group isn't running on this node.
    pub async fn handle_for_graph(self: &Arc<Self>, graph_name: &str) -> Option<RaftHandle> {
        let gid = self.router.group_of(graph_name);
        let raft = self.groups.read().await.get(&gid).cloned()?;
        Some(RaftHandle {
            raft,
            node_id: self.node_id,
        })
    }

    /// Shut down the listener (and, by drop, stop accepting). Groups keep running
    /// until dropped/closed; used by graceful shutdown + tests.
    pub fn stop_listener(&self) {
        self.listener_handle.abort();
    }
}

/// Serve one connection on the shared listener, demuxing each framed RPC to the
/// group it is tagged for (CONCEPT:KG-2.205). An RPC for a group this node doesn't
/// run gets a per-variant error reply (openraft treats it as a transient failure).
async fn serve_conn(
    mut stream: tokio::net::TcpStream,
    groups: Arc<RwLock<BTreeMap<GroupId, EgRaft>>>,
) -> std::io::Result<()> {
    loop {
        let body = match network::read_frame(&mut stream).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let rpc: GroupRpc = rmp_serde::from_slice(&body)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let gid = rpc.group_id();
        let raft = groups.read().await.get(&gid).cloned();
        let reply = network::dispatch_group(raft, gid, rpc).await;
        let out = rmp_serde::to_vec_named(&reply)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        network::write_frame(&mut stream, &out).await?;
    }
}
