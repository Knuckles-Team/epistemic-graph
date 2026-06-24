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

/// Routes a graph name to the Raft group that owns it (CONCEPT:KG-2.205). One graph
/// belongs to exactly one group. This increment maps everything to [`DEFAULT_GROUP`]
/// (single-group behavior); an explicit override map lets a test / a later increment
/// place specific graphs in other groups without a wire/storage change.
#[derive(Default)]
pub struct GroupRouter {
    overrides: DashMap<String, GroupId>,
}

impl GroupRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// The group that owns `graph_name`. Defaults to [`DEFAULT_GROUP`].
    pub fn group_of(&self, graph_name: &str) -> GroupId {
        self.overrides
            .get(graph_name)
            .map(|g| *g)
            .unwrap_or(DEFAULT_GROUP)
    }

    /// Pin a graph to a specific group (used by multi-group setups/tests). One graph
    /// → one group: re-assigning is allowed but a graph is never in two groups.
    pub fn assign(&self, graph_name: &str, group_id: GroupId) {
        self.overrides.insert(graph_name.to_string(), group_id);
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
    listener_handle: tokio::task::JoinHandle<()>,
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
            listener_handle,
        }))
    }

    pub fn router(&self) -> Arc<GroupRouter> {
        self.router.clone()
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
        let store = EgStore::open(gid, self.backend.clone(), self.ctx.clone())?;
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
        let network = network::GroupNetworkFactory::new(gid, self.node_id);
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
