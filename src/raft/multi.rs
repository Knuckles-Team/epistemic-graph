//! Multi-Raft manager + routing (CONCEPT:EG-KG.sharding.raft-resharding).
//!
//! A [`MultiRaft`] holds N openraft groups in one process, keyed by [`GroupId`],
//! each its OWN [`store::EgStore`] state machine applying to its own graph data —
//! but ALL groups share:
//!
//! * **ONE redb Database** (the M2 authoritative shard): every group's durable log + meta
//!   is keyed by `(group_id, …)` (CONCEPT:EG-KG.storage.one-fsync-covers-raft). This is the spike's FD-ceiling
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
//! ### Group-owned participants and cross-group transactions
//!
//! A graph belongs to exactly one group. A transaction spanning multiple groups is
//! coordinated outside state-machine apply through typed prepare, durable decision,
//! participant commit/abort, and finalization commands; every participant command is
//! replicated by the group that owns that graph.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;
use openraft::Config;
use tokio::sync::RwLock;

use super::network::{self, GroupRpcReply, RaftFrame, RaftFrameReply};
use super::membership_shrink::{
    MembershipShrinkEvidence, MembershipShrinkJournal, MembershipShrinkPhase,
};
use super::placement::{self, PlacementCatalog, PlacementRoute};
use super::store::EgStore;
use super::{
    AppCtx, EgRaft, GroupId, NodeId, RaftHandle, RaftRequest, RaftResponse, DEFAULT_GROUP,
};
use crate::protocol::{Method, ResultPayload};

/// Routes a graph name to the Raft group that owns it (CONCEPT:EG-KG.sharding.raft-resharding +
/// KG-2.266). One graph belongs to exactly one group. Resolution order:
///
/// 1. an explicit per-graph **override** ([`assign`], used by reshard + tests) —
///    highest priority, a graph pinned to a specific group;
/// 2. otherwise the **tenant-range ring** ([`set_group_ring`], CONCEPT:AU-KG.ingest.mirror-inbound):
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
    /// The tenant-range ring (CONCEPT:AU-KG.ingest.mirror-inbound): a sorted, de-duplicated set of group
    /// ids that un-pinned graphs hash-distribute across. EMPTY ⇒ single-group default.
    ring: parking_lot::RwLock<Vec<GroupId>>,
}

/// Stable, deterministic FNV-1a hash of a graph name → ring slot. Stable forever and
/// identical on every node (NOT the randomized `RandomState`), so all nodes route a
/// given tenant to the SAME group. Routing is recomputed live, never persisted.
pub(crate) fn fnv1a(s: &str) -> u64 {
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
    ///
    /// The ring path hashes the **sanitized** durable key ([`crate::persist::sanitize`]),
    /// NOT the raw name — deliberately, so it agrees with the durable-storage router
    /// (`redb_backend::shard_index(graph_fname, K)`, which also hashes the sanitized key
    /// with the SAME FNV-1a). That agreement is the ADR-2 / W1.2 invariant "raft group `g`
    /// owns redb shard `g`": with the production ring `0..K` and K == N (see
    /// `MultiRaft::configure_group_ring` + `resolve_shard_count`), `group_of(name)` reduces
    /// to `fnv1a(sanitize(name)) % N` == the shard the graph's data lands in, so a group's
    /// consensus and its graphs' data share one shard. (Overrides — reshard/placement pins,
    /// keyed on the raw name callers pass — still win first.)
    pub fn group_of(&self, graph_name: &str) -> GroupId {
        if let Some(g) = self.overrides.get(graph_name) {
            return *g;
        }
        let ring = self.ring.read();
        if ring.is_empty() {
            return DEFAULT_GROUP;
        }
        ring[(fnv1a(&crate::persist::sanitize(graph_name)) % ring.len() as u64) as usize]
    }

    /// Configure the tenant-range ring (CONCEPT:AU-KG.ingest.mirror-inbound): un-pinned graphs
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

    /// The distinct groups a set of graph names spans (CONCEPT:EG-KG.storage.lane-n-increment). A txn whose
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

/// The deterministic round-robin target leader for group `gid` over its SORTED voter
/// set (CONCEPT:EG-KG.sharding.multi-raft). Identical on every node (the voter set is the replicated
/// membership, sorted), so all nodes agree which node should lead each group WITHOUT
/// coordination — the property that makes the cooperative balancer converge. `None`
/// for an empty voter set.
pub(crate) fn desired_leader(gid: GroupId, sorted_voters: &[NodeId]) -> Option<NodeId> {
    if sorted_voters.is_empty() {
        return None;
    }
    Some(sorted_voters[(gid % sorted_voters.len() as u64) as usize])
}

/// Automatic transfers are safe only when both nodes have an observed domain and
/// those domains differ.  Equality/absence is deliberately a hard no-op: a
/// balancer must never infer that two addresses represent independent failure
/// domains merely because their node ids differ.
pub(crate) fn failure_domain_safe(
    domains: &BTreeMap<NodeId, String>,
    current: NodeId,
    target: NodeId,
) -> bool {
    current != target
        && domains
            .get(&current)
            .zip(domains.get(&target))
            .is_some_and(|(current_domain, target_domain)| current_domain != target_domain)
}

/// What one [`rebalance_leaders`](MultiRaft::rebalance_leaders) pass decided, for
/// observability + tests (CONCEPT:EG-KG.sharding.multi-raft → KG-2.273).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RebalanceReport {
    /// Per local group: the round-robin target leader node id.
    pub targets: BTreeMap<GroupId, NodeId>,
    /// Groups this node (as their CURRENT leader) gracefully HANDED OFF this pass via
    /// the native openraft-0.10 `trigger().transfer_leader(target)` (CONCEPT:AU-KG.backend.authority-has-already-acked),
    /// because the round-robin target is another node. Empty on an already-balanced
    /// cluster (or on a node that leads nothing it shouldn't).
    pub transferred: Vec<GroupId>,
    /// Per-group transfer-trigger errors (rare — e.g. the group was shutting down).
    pub errors: Vec<(GroupId, String)>,
    /// Groups deliberately left untouched by the safety/balance gate.  Keeping the
    /// reason visible makes a no-op distinguishable from a missing observation.
    pub skipped: Vec<(GroupId, String)>,
}

/// A single running group: its `EgRaft` handle + the node id it runs as. Cloneable.
#[derive(Clone)]
pub struct Group {
    pub raft: EgRaft,
    pub node_id: NodeId,
}

/// Placement-aware handle returned at the ordinary dispatch boundary. Keeping the
/// group and epoch beside the Raft handle prevents callers from resolving placement
/// and then accidentally discarding the fencing information before the write.
#[derive(Clone)]
pub struct RoutedRaftHandle {
    pub handle: RaftHandle,
    pub group_id: GroupId,
    pub epoch: u64,
    pub placed: bool,
}

impl RoutedRaftHandle {
    /// Stable, non-secret fencing token suitable for responses and retry metadata.
    pub fn fencing_token(&self) -> GroupId {
        self.group_id
    }
}

impl Group {
    /// Route a durable mutation through THIS group's consensus (leader `client_write`).
    pub async fn client_write(&self, req: RaftRequest) -> Result<RaftResponse, String> {
        req.validate()?;
        match self.raft.client_write(req).await {
            Ok(resp) => {
                resp.data.validate()?;
                Ok(resp.data)
            }
            Err(e) => Err(format!("raft client_write: {e}")),
        }
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }
}

/// The per-node multi-group manager (CONCEPT:EG-KG.sharding.raft-resharding). Holds the live group map +
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
    /// Shared per-peer outbound connection pool (CONCEPT:AU-KG.ontology.manage-arbitrary) — one per node,
    /// reused by every group's network clients.
    pool: Arc<network::PeerPool>,
    listener_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Accepted connection and deferred group-initialization tasks must be
    /// cancelled with the listener. Otherwise a quiet keep-alive connection or
    /// bootstrap task retains the read service/group store and its redb backend
    /// after shutdown, preventing an in-process restart from reopening the files.
    connection_tasks: Arc<ConnectionTaskSet>,
    /// Per-tenant migration locks (CONCEPT:EG-KG.storage.100m-tenant). A reshard or hibernate of a
    /// graph takes its lock so the two cannot race / interleave for one tenant; ops
    /// on DIFFERENT graphs proceed concurrently. Lazily created per graph name.
    tenant_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-group cooldown timestamps for the leader balancer (CONCEPT:AU-KG.backend.authority-has-already-acked). A
    /// triggered transfer hands leadership away, so [`rebalance_leaders`] refuses to
    /// re-issue a transfer for a group within [`TRANSFER_COOLDOWN`] — this stops it
    /// spamming transfer commands while the handoff settles when polled on a tick.
    ///
    /// [`rebalance_leaders`]: MultiRaft::rebalance_leaders
    last_transfer: Arc<DashMap<GroupId, Instant>>,
    /// Live failure-domain metadata used to prevent an automatic transfer inside
    /// one host/AZ/rack.  Missing or equal domains fail closed.
    failure_domains: Arc<parking_lot::RwLock<BTreeMap<NodeId, String>>>,
    /// Cross-group heartbeat coalescer and its bounded flush worker.  The worker
    /// is owned by the manager so all live OpenRaft network clients share it.
    heartbeat_coalescer: Arc<network::HeartbeatCoalescer>,
    heartbeat_flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Periodic, policy-free observation loop.  Each actual transfer still passes
    /// through the benefit, failure-domain, and cooldown gates below.
    leader_balance_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The ONE placement authority (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1): a durable,
    /// Raft-replicated virtual-partition → group map that [`route_graph`] consults
    /// before returning [`router`]'s engine-owned unplaced policy. Always present (even with an
    /// empty catalog it changes nothing — see [`route_graph`]).
    ///
    /// [`route_graph`]: MultiRaft::route_graph
    /// [`router`]: MultiRaft::router
    placement: Arc<PlacementCatalog>,
    /// Shared implementation used identically by local leaders and authenticated
    /// remote `ReadPage` RPCs.
    read_service: Arc<super::xread::ReadPageService>,
    /// Serializes the idempotent full shutdown sequence.
    shutdown_lock: tokio::sync::Mutex<()>,
}

impl Drop for MultiRaft {
    fn drop(&mut self) {
        // Callers normally use the async `shutdown` path, but harnesses and startup
        // error paths can drop the last Arc directly.  Do not leave a detached
        // coalescer or balance ticker retaining runtime resources in that case.
        self.heartbeat_coalescer.stop();
        if let Ok(mut task) = self.heartbeat_flush_task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
        if let Ok(mut task) = self.leader_balance_task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

#[derive(Default)]
struct ConnectionTaskSet {
    stopping: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ConnectionTaskSet {
    async fn register(&self, task: tokio::task::JoinHandle<()>) {
        let late_task = {
            let mut tasks = self
                .tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            tasks.retain(|task| !task.is_finished());
            if self.stopping.load(Ordering::Acquire) {
                Some(task)
            } else {
                tasks.push(task);
                None
            }
        };
        if let Some(task) = late_task {
            task.abort();
            let _ = task.await;
        }
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Retain the handles so a later full shutdown can await cancellation and
        // prove that every backend-retaining future has actually dropped.
        for task in tasks.iter() {
            task.abort();
        }
    }

    async fn stop_and_wait(&self) {
        self.stopping.store(true, Ordering::Release);
        let tasks = {
            let mut tasks = self
                .tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            tasks.drain(..).collect::<Vec<_>>()
        };
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// Minimum interval between two balancer-triggered leader transfers for the SAME group
/// (CONCEPT:AU-KG.backend.authority-has-already-acked). Comfortably above `election_timeout_max` (3s) so a handoff has
/// settled before the balancer would consider another — no flapping.
const TRANSFER_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(10);
/// A transfer must remove at least one leader slot from the current node.  This is
/// the explicit hysteresis margin: equal loads never churn, even when the target
/// function is deterministic but membership/metrics are settling.
const LEADER_TRANSFER_MIN_MARGIN: usize = 1;
/// At most one automatic transfer is issued by a node per observation pass.  The
/// next pass observes the new terms/loads before making another change.
const MAX_AUTOMATIC_TRANSFERS_PER_PASS: usize = 1;
/// The automatic scheduler is intentionally much slower than the 250 ms Raft
/// heartbeat; a transfer gets several election/replication observations to settle.
const LEADER_BALANCE_INTERVAL: Duration = Duration::from_secs(15);
const MAX_RAFT_INBOUND_CONNECTIONS: usize = 64;
const MAX_PLACEMENT_EPOCH_RETRIES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlacementCommitOutcome {
    Committed,
    EpochConflict,
}

fn loopback_endpoint(endpoint: &str) -> bool {
    if let Ok(addr) = endpoint.parse::<std::net::SocketAddr>() {
        return addr.ip().is_loopback();
    }
    endpoint
        .rsplit_once(':')
        .map(|(host, _)| {
            host.trim_matches(['[', ']'])
                .eq_ignore_ascii_case("localhost")
        })
        .unwrap_or(false)
}

impl MultiRaft {
    /// Start an explicitly plaintext loopback listener for the fault-injection and
    /// in-process harnesses. Production startup must use [`start_configured`].
    #[cfg(any(test, feature = "harness"))]
    pub async fn start(
        node_id: NodeId,
        bind_addr: String,
        backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
        ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        let state = ctx.state.clone();
        // Harness nodes share a process/host, but each node id is an explicit
        // fixture failure domain so the local leader-balance acceptance path can
        // exercise a safe transfer without claiming cross-host proof.
        let failure_domains = [(node_id, format!("harness-node-{node_id}"))]
            .into_iter()
            .collect();
        let multi = Self::start_inner(
            node_id,
            bind_addr,
            backend,
            ctx,
            None,
            failure_domains,
        )
        .await?;
        // Harness construction is process-owned: once the real MultiRaft
        // listener/catalog exists, publish it to the shared ServerState rather
        // than making every fixture remember a second wiring assignment.
        state
            .write()
            .await
            .install_multi_raft_placement_authority(None, multi.clone());
        Ok(multi)
    }

    /// Production callers cannot select the harness-only plaintext constructor.
    /// Keeping the symbol as a fail-closed shim avoids an accidental insecure API
    /// fallback and gives direct library callers an actionable runtime error.
    #[cfg(not(any(test, feature = "harness")))]
    pub async fn start(
        _node_id: NodeId,
        _bind_addr: String,
        _backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
        _ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        Err(
            "Raft production startup requires an explicit peer set and transport policy"
                .to_string(),
        )
    }

    /// Start the production listener with fail-closed transport policy. A runtime
    /// secret creates an authenticated, encrypted peer channel. Plain transport is
    /// accepted only for one member whose bind and advertised addresses are both
    /// loopback.
    pub async fn start_configured(
        node_id: NodeId,
        bind_addr: String,
        peers: &super::PeerMap,
        secret: Option<&super::config::RaftTransportSecret>,
        backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
        ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        let failure_domains = super::config::resolve_failure_domains(peers)?;
        let auth = match secret {
            Some(secret) => Some(
                network::RaftTransportAuth::new(
                    node_id,
                    secret.expose(),
                    peers.iter().map(|(id, node)| (*id, node.addr.clone())),
                )
                .map_err(|_| "invalid Raft transport peer configuration".to_string())?,
            ),
            None if peers.len() == 1
                && peers.contains_key(&node_id)
                && loopback_endpoint(&bind_addr)
                && peers.values().all(|node| loopback_endpoint(&node.addr)) =>
            {
                None
            }
            None => {
                return Err(
                    "refusing unauthenticated Raft transport outside one-member loopback mode"
                        .to_string(),
                )
            }
        };
        Self::start_inner(node_id, bind_addr, backend, ctx, auth, failure_domains).await
    }

    async fn start_inner(
        node_id: NodeId,
        bind_addr: String,
        backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
        ctx: AppCtx,
        auth: Option<Arc<network::RaftTransportAuth>>,
        failure_domains: BTreeMap<NodeId, String>,
    ) -> Result<Arc<Self>, String> {
        let groups: Arc<RwLock<BTreeMap<GroupId, EgRaft>>> = Arc::new(RwLock::new(BTreeMap::new()));
        let groups_for_listener = groups.clone();
        let auth_for_listener = auth.clone();
        let router = Arc::new(GroupRouter::new());
        let placement = Arc::new(PlacementCatalog::new(ctx.state.clone()));
        let read_service = Arc::new(super::xread::ReadPageService::new(
            backend.clone(),
            placement.clone(),
            router.clone(),
        ));
        let read_service_for_listener = read_service.clone();
        let connection_permits =
            Arc::new(tokio::sync::Semaphore::new(MAX_RAFT_INBOUND_CONNECTIONS));
        let frame_budget = Arc::new(tokio::sync::Semaphore::new(
            network::RAFT_FRAME_BUDGET_UNITS,
        ));
        let connection_tasks = Arc::new(ConnectionTaskSet::default());
        let connection_tasks_for_listener = connection_tasks.clone();
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|_| "unable to bind Raft multi-group listener".to_string())?;
        tracing::info!("Raft multi-group RPC listener started");
        let listener_handle = tokio::spawn(async move {
            loop {
                let (stream, _peer) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let Ok(permit) = connection_permits.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let groups = groups_for_listener.clone();
                let auth = auth_for_listener.clone();
                let frame_budget = frame_budget.clone();
                let read_service = read_service_for_listener.clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) =
                        serve_conn(stream, groups, auth, frame_budget, read_service).await
                    {
                        tracing::debug!("raft multi rpc conn ended: {e}");
                    }
                });
                connection_tasks_for_listener.register(task).await;
            }
        });
        let pool = match auth {
            Some(auth) => network::PeerPool::with_auth(auth),
            None => network::PeerPool::new(),
        };
        let heartbeat_coalescer = Arc::new(network::HeartbeatCoalescer::new());
        let multi = Arc::new(Self {
            node_id,
            groups,
            router,
            backend,
            placement,
            read_service,
            ctx,
            pool,
            listener_handle: Mutex::new(Some(listener_handle)),
            connection_tasks,
            tenant_locks: Arc::new(DashMap::new()),
            last_transfer: Arc::new(DashMap::new()),
            failure_domains: Arc::new(parking_lot::RwLock::new(failure_domains)),
            heartbeat_coalescer: heartbeat_coalescer.clone(),
            heartbeat_flush_task: Mutex::new(None),
            leader_balance_task: Mutex::new(None),
            shutdown_lock: tokio::sync::Mutex::new(()),
        });
        let heartbeat_task = tokio::spawn(heartbeat_coalescer.run(multi.pool.clone()));
        *multi
            .heartbeat_flush_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(heartbeat_task);

        let weak_multi = Arc::downgrade(&multi);
        let leader_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(LEADER_BALANCE_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Consume the immediate interval tick; the first automatic pass occurs
            // only after the bounded observation window has elapsed.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(multi) = weak_multi.upgrade() else {
                    break;
                };
                let report = multi.rebalance_leaders().await;
                if !report.transferred.is_empty() || !report.errors.is_empty() {
                    tracing::info!(
                        node_id = multi.node_id,
                        transferred = ?report.transferred,
                        errors = ?report.errors,
                        "automatic Raft leader-balance pass completed"
                    );
                }
            }
        });
        *multi
            .leader_balance_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(leader_task);
        Ok(multi)
    }

    /// Acquire the per-tenant migration lock for `graph_name` (CONCEPT:EG-KG.storage.100m-tenant), so a
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
    /// bootstrap) if absent — the resharding target-group seam (CONCEPT:EG-KG.storage.100m-tenant).
    /// Idempotent: a no-op if the group already runs. The new group shares the SAME
    /// listener + authoritative shard; its durable log/meta are keyed by `gid`.
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

    /// The node's shared per-peer connection pool (CONCEPT:AU-KG.ontology.manage-arbitrary) — exposed for
    /// metrics/tests (e.g. asserting RPCs reused a warm connection).
    pub fn pool(&self) -> Arc<network::PeerPool> {
        self.pool.clone()
    }

    /// Configure a tenant-range ring of `n_groups` groups (CONCEPT:AU-KG.ingest.mirror-inbound) and bring
    /// each up on THIS node, distributing un-pinned graphs across them. Group ids are
    /// `0..n_groups` (so [`DEFAULT_GROUP`] = 0 is always in the ring). Every group is
    /// created with the complete configured peer set, giving non-default groups the
    /// same replication/failover contract as the default group. `n_groups <= 1`
    /// leaves the ring empty (the single-group default).
    pub async fn configure_group_ring(
        self: &Arc<Self>,
        n_groups: u64,
        peers: &BTreeMap<NodeId, BasicNode>,
        is_bootstrap: bool,
    ) -> Result<(), String> {
        if n_groups <= 1 {
            return Ok(());
        }
        let groups: Vec<GroupId> = (0..n_groups).collect();
        for &gid in groups.iter().skip(1) {
            if self.group(gid).await.is_none() {
                self.create_group(gid, peers.clone(), is_bootstrap).await?;
            }
        }
        self.router.set_group_ring(&groups);
        Ok(())
    }

    /// The shared `ServerState` (registry + persistence) every group applies into —
    /// reached via the manager's [`AppCtx`]. Used by the cross-shard 2PC coordinator
    /// (CONCEPT:EG-KG.storage.lane-n-increment) to validate slices against live group state.
    pub fn app_state(&self) -> Arc<RwLock<crate::server::ServerState>> {
        self.ctx.state.clone()
    }

    /// The shared M2 backend (for the cross-shard coordinator's durable 2PC records).
    pub fn backend(&self) -> Arc<dyn crate::server::persistence::PersistenceBackend> {
        self.backend.clone()
    }

    /// Submit an engine-internal command to a specific placement group, forwarding
    /// it over the authenticated Raft peer channel when this node is not that
    /// group's leader. This is the non-nested coordinator seam used by distributed
    /// transaction prepare/decision/commit; state-machine apply never calls it.
    pub(crate) async fn client_write_group(
        &self,
        group_id: GroupId,
        request: RaftRequest,
    ) -> Result<RaftResponse, String> {
        request.validate()?;
        let raft = self
            .groups
            .read()
            .await
            .get(&group_id)
            .cloned()
            .ok_or_else(|| format!("placement group {group_id} is not running on this node"))?;
        let leader = raft
            .current_leader()
            .await
            .ok_or_else(|| format!("placement group {group_id} has no current leader"))?;
        if leader == self.node_id {
            return super::RaftHandle {
                raft,
                node_id: self.node_id,
            }
            .client_write(request)
            .await;
        }
        let addr = {
            let metrics = raft.metrics();
            let current = metrics.borrow_watched();
            current
                .membership_config
                .get_node(&leader)
                .map(|node| node.addr.clone())
                .ok_or_else(|| {
                    format!(
                        "placement group {group_id} has no committed address for leader {leader}"
                    )
                })?
        };
        network::forward_client_write(&self.pool, &addr, group_id, request).await
    }

    /// Resolve a group's current leader from committed membership.  Every read RPC
    /// goes to that leader; a follower is never used as an implicit stale fallback.
    async fn read_leader(
        &self,
        group_id: GroupId,
    ) -> Result<(EgRaft, Option<String>), super::xread::ReadPageError> {
        let raft = self
            .groups
            .read()
            .await
            .get(&group_id)
            .cloned()
            .ok_or_else(|| {
                super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::GroupUnavailable)
            })?;
        let leader = raft.current_leader().await.ok_or_else(|| {
            super::xread::ReadPageError::new(super::xread::ReadPageErrorCode::NoLeader)
        })?;
        if leader == self.node_id {
            return Ok((raft, None));
        }
        let address = {
            let metrics = raft.metrics();
            let current = metrics.borrow_watched();
            current
                .membership_config
                .get_node(&leader)
                .map(|node| node.addr.clone())
                .ok_or_else(|| {
                    super::xread::ReadPageError::new(
                        super::xread::ReadPageErrorCode::GroupUnavailable,
                    )
                })?
        };
        Ok((raft, Some(address)))
    }

    /// Require this process to be the current coordinator for a group-owned
    /// workflow. Individual Raft writes can be forwarded, but a multi-stage move
    /// must have exactly one driver; callers retry against the elected placement
    /// leader after a handoff instead of running competing workflows on followers.
    pub(crate) async fn require_local_group_leader(&self, group_id: GroupId) -> Result<(), String> {
        let raft = self
            .groups
            .read()
            .await
            .get(&group_id)
            .cloned()
            .ok_or_else(|| "move coordinator group is unavailable".to_string())?;
        match raft.current_leader().await {
            Some(leader) if leader == self.node_id => Ok(()),
            Some(_) => Err("partition moves must run on the placement leader".to_string()),
            None => Err("placement group has no elected move coordinator".to_string()),
        }
    }

    /// Per-group ReadIndex routed to the current leader over the authenticated peer
    /// pool when the leader is remote.
    pub(crate) async fn read_barrier_group(
        &self,
        group_id: GroupId,
    ) -> Result<u64, super::xread::ReadPageError> {
        let (raft, remote) = self.read_leader(group_id).await?;
        match remote {
            None => super::xread::linearizable_barrier(&raft).await,
            Some(address) => {
                let barrier = network::forward_read_barrier(&self.pool, &address, group_id).await?;
                // The route catalog is read from this process's state machine after
                // the remote leader answers.  Wait until the local replica has
                // applied that exact barrier so route resolution cannot observe an
                // older epoch.
                raft.wait(Some(std::time::Duration::from_secs(5)))
                    .applied_index_at_least(Some(barrier), "cross-graph read barrier")
                    .await
                    .map_err(|_| {
                        super::xread::ReadPageError::new(
                            super::xread::ReadPageErrorCode::BarrierFailed,
                        )
                    })?;
                Ok(barrier)
            }
        }
    }

    /// Serve one placement-fenced graph page on the owning group's current leader.
    pub(crate) async fn read_page_group(
        &self,
        group_id: GroupId,
        request: super::xread::ReadPageRequest,
    ) -> Result<super::xread::ReadPageReply, super::xread::ReadPageError> {
        let (raft, remote) = self.read_leader(group_id).await?;
        match remote {
            None => self.read_service.read_page(raft, group_id, request).await,
            Some(address) => {
                network::forward_read_page(&self.pool, &address, group_id, request).await
            }
        }
    }

    /// Create + start group `gid` on this node with the given peer set. The store is
    /// opened over the SHARED M2 backend keyed by `gid` (CONCEPT:EG-KG.storage.one-fsync-covers-raft), so all
    /// groups share ONE authoritative shard. The lowest-id member bootstraps.
    pub async fn create_group(
        &self,
        gid: GroupId,
        peers: BTreeMap<NodeId, BasicNode>,
        is_bootstrap: bool,
    ) -> Result<(), String> {
        if self.groups.read().await.contains_key(&gid) {
            return Err(format!("group {gid} already open on node {}", self.node_id));
        }
        for (peer_id, peer) in &peers {
            self.pool
                .register_peer(*peer_id, &peer.addr)
                .map_err(|_| "invalid or conflicting Raft peer registration".to_string())?;
            self.failure_domains
                .write()
                .entry(*peer_id)
                .or_insert_with(|| {
                    super::config::failure_domain_for_peer(*peer_id, &peer.addr)
                });
        }
        // The store's ctx carries the router so its snapshot dump is SCOPED to this
        // group's tenant-range graphs (CONCEPT:AU-KG.ingest.staged), not the whole registry.
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
        // openraft 0.10 (CONCEPT:AU-KG.backend.authority-has-already-acked): the v1 `RaftStorage`/`Adaptor` split is
        // gone — `EgStore` implements `RaftLogStorage` AND `RaftStateMachine` on
        // `Arc<EgStore>`, so we hand the SAME store in as both (a cheap clone). They
        // share the one underlying redb-backed log + state machine.
        let log_store = store.clone();
        let state_machine = store;
        let network = network::GroupNetworkFactory::with_coalescer(
            gid,
            self.node_id,
            self.pool.clone(),
            self.heartbeat_coalescer.clone(),
        );
        let raft: EgRaft =
            openraft::Raft::new(self.node_id, raft_config, network, log_store, state_machine)
                .await
                .map_err(|e| format!("raft new g{gid}: {e}"))?;
        self.groups.write().await.insert(gid, raft.clone());

        if is_bootstrap {
            let raft_for_init = raft.clone();
            let task = tokio::spawn(async move {
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
            self.connection_tasks.register(task).await;
        }
        tracing::info!("Raft group {gid} created on node {}", self.node_id);
        Ok(())
    }

    // ── R3: multi-node membership join (CONCEPT:EG-KG.storage.kg-kg-2) ──────────────────
    //
    // `create_group(.., is_bootstrap=true)` brings a group up as a SINGLE-member
    // cluster on one node. To make a group span multiple NODES you (a) stand the
    // group up EMPTY on each joining node (`join_group`, is_bootstrap=false, so it
    // never `initialize`s — it just opens its store + serves its slot on the shared
    // listener) and (b) on the group's LEADER add each joiner as a learner
    // (`add_group_learner`) then promote it to a voter (`change_group_voters`), or do
    // both at once with `add_group_member`. This is the openraft add-learner →
    // change-membership lifecycle, per group, over the shared listener — reachable at
    // runtime via `Method::RaftAddLearner`/`Method::RaftChangeMembership`
    // (src/server/handlers/raft_admin.rs), not just this in-process API.

    /// Stand group `gid` up on THIS node as an EMPTY, non-bootstrapping member ready to
    /// receive replication (CONCEPT:EG-KG.storage.kg-kg-2). Unlike [`ensure_group`] (which
    /// single-member bootstraps), this NEVER calls `initialize`: the group joins an
    /// existing cluster only when its leader calls [`add_group_member`] for this node.
    /// Idempotent. `peers` may be empty — a follower learns peer addresses from the
    /// membership the leader replicates.
    ///
    /// [`add_group_member`]: MultiRaft::add_group_member
    pub async fn join_group(
        &self,
        gid: GroupId,
        peers: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), String> {
        if self.groups.read().await.contains_key(&gid) {
            return Ok(());
        }
        self.create_group(gid, peers, false).await
    }

    /// The current VOTER set of group `gid` as this node sees it (sorted), or `None` if
    /// the group isn't running here. Read from openraft's replicated membership config,
    /// so on a caught-up node it is the committed cluster membership.
    /// Every group id currently running on THIS node, sorted (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 /
    /// W1.1 — `Method::ClusterMembers`' group enumeration). `DEFAULT_GROUP` is
    /// always included once any group has started (`ensure_group`/`create_group`/
    /// `join_group` populate it).
    pub async fn known_groups(&self) -> Vec<GroupId> {
        self.groups.read().await.keys().copied().collect()
    }

    pub async fn group_membership(&self, gid: GroupId) -> Option<Vec<NodeId>> {
        let raft = self.groups.read().await.get(&gid).cloned()?;
        let metrics = raft.metrics();
        let mut voters: Vec<NodeId> = metrics
            .borrow_watched()
            .membership_config
            .voter_ids()
            .collect();
        voters.sort_unstable();
        Some(voters)
    }

    /// The current NON-VOTING LEARNER set of group `gid` as this node sees it (sorted;
    /// voters excluded), or `None` if the group isn't running here (CONCEPT:EG-KG.storage.kg-kg-2). The
    /// [`add_group_learner`](MultiRaft::add_group_learner) counterpart to
    /// [`group_membership`](MultiRaft::group_membership)'s voter set, so a caller can
    /// observe a learner attached but not yet promoted.
    pub async fn group_learners(&self, gid: GroupId) -> Option<Vec<NodeId>> {
        let raft = self.groups.read().await.get(&gid).cloned()?;
        let metrics = raft.metrics();
        let mut learners: Vec<NodeId> = metrics
            .borrow_watched()
            .membership_config
            .membership()
            .learner_ids()
            .collect();
        learners.sort_unstable();
        Some(learners)
    }

    /// The highest committed membership-log index observed across the groups
    /// running on this node.  This is deliberately derived from openraft's
    /// replicated membership authority rather than from a response-local
    /// member count, so a caller can reject a snapshot that moved backwards
    /// during a membership change.
    pub async fn membership_epoch(&self) -> u64 {
        let groups = self.groups.read().await;
        let mut epoch = 0;
        for raft in groups.values() {
            let metrics = raft.metrics();
            let watched = metrics.borrow_watched();
            if let Some(log_id) = watched.membership_config.log_id().as_ref() {
                epoch = epoch.max(log_id.index);
            }
        }
        epoch
    }

    /// Attach `new_node` (reachable at `addr`) to group `gid` as a NON-VOTING LEARNER
    /// (CONCEPT:EG-KG.storage.kg-kg-2) — the primitive `cluster_deployment.md` §5 item 2 flagged as having
    /// no external caller. MUST be called on the group's current LEADER (membership
    /// changes are leader-only in Raft). Registers `addr` in the node's [`BasicNode`]
    /// so every member's network layer can reach it, then runs ONLY the openraft
    /// `add_learner(new_node, blocking)` step: it starts replicating to the node and
    /// BLOCKS until its log is caught up, but does NOT touch the voter set, so quorum
    /// size and fault tolerance are unaffected. This is the safe, always-available
    /// first step; promote the learner afterward with [`change_group_voters`] (or use
    /// [`add_group_member`] for the old bundled add+promote behavior). Idempotent:
    /// re-adding an existing learner/voter re-confirms the same membership.
    ///
    /// [`change_group_voters`]: MultiRaft::change_group_voters
    /// [`add_group_member`]: MultiRaft::add_group_member
    pub async fn add_group_learner(
        &self,
        gid: GroupId,
        new_node: NodeId,
        addr: String,
    ) -> Result<(), String> {
        self.pool
            .register_peer(new_node, &addr)
            .map_err(|_| "invalid or conflicting Raft peer registration".to_string())?;
        self.failure_domains.write().entry(new_node).or_insert_with(|| {
            super::config::failure_domain_for_peer(new_node, &addr)
        });
        let raft = self
            .groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))?;
        raft.add_learner(new_node, BasicNode::new(addr), true)
            .await
            .map_err(|e| format!("add_learner {new_node} to group {gid}: {e}"))?;
        Ok(())
    }

    /// Set group `gid`'s VOTER set to exactly `voters` (CONCEPT:EG-KG.storage.kg-kg-2) — openraft
    /// `change_membership`. This is the promotion/rebalance half of the two-step join,
    /// split out of [`add_group_member`] so a learner added via [`add_group_learner`]
    /// can be promoted (or the voter set otherwise changed) as its OWN admin step: pass
    /// the full desired voter set (existing voters plus whichever learner(s) are being
    /// promoted). MUST be called on the group's current LEADER. Refuses to produce an
    /// EMPTY voter set (that would make the group leaderless / unrecoverable).
    /// Idempotent: setting the same voter set re-commits the same config.
    ///
    /// [`add_group_member`]: MultiRaft::add_group_member
    /// [`add_group_learner`]: MultiRaft::add_group_learner
    pub async fn change_group_voters(
        &self,
        gid: GroupId,
        voters: BTreeSet<NodeId>,
    ) -> Result<(), String> {
        if voters.is_empty() {
            return Err(format!(
                "refusing to set an empty voter set for group {gid}"
            ));
        }
        let raft = self
            .groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))?;
        raft.change_membership(voters, false)
            .await
            .map_err(|e| format!("change_membership group {gid}: {e}"))?;
        Ok(())
    }

    /// Add `new_node` (reachable at `addr`) to group `gid` as a VOTER (CONCEPT:EG-KG.storage.kg-kg-2).
    /// MUST be called on the group's current LEADER (membership changes are leader-only
    /// in Raft). The original bundled two-step join, now composed from
    /// [`add_group_learner`] (add + block-until-caught-up) followed by
    /// [`change_group_voters`] (commit the new uniform config including `new_node`) —
    /// byte-identical behavior to before the split, kept for callers that always want
    /// the immediate promotion. Idempotent-ish: re-adding an existing voter re-commits
    /// the same set.
    ///
    /// [`add_group_learner`]: MultiRaft::add_group_learner
    /// [`change_group_voters`]: MultiRaft::change_group_voters
    pub async fn add_group_member(
        &self,
        gid: GroupId,
        new_node: NodeId,
        addr: String,
    ) -> Result<(), String> {
        self.add_group_learner(gid, new_node, addr).await?;
        let raft = self
            .groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))?;
        let mut voters: BTreeSet<NodeId> = {
            let metrics = raft.metrics();
            let watched = metrics.borrow_watched();
            watched.membership_config.voter_ids().collect()
        };
        voters.insert(new_node);
        self.change_group_voters(gid, voters).await
    }

    /// Legacy removal entry point. A bare `change_membership` cannot prove
    /// drain, learner catch-up, leadership transfer, or topology safety, so it
    /// is deliberately fail-closed. Use
    /// [`remove_group_member_with_evidence`](Self::remove_group_member_with_evidence)
    /// with the durable shrink contract instead.
    pub async fn remove_group_member(&self, gid: GroupId, node: NodeId) -> Result<(), String> {
        Err(format!(
            "refusing unsafe membership shrink of node {node} from group {gid}; "
                "submit a drain/learner/quorum evidence journal first"
        ))
    }

    /// Remove one voter only through the durable drain/safety state machine.
    /// Every phase is retained in the placement graph before the next side
    /// effect. A crash after `change_membership` therefore leaves enough state
    /// for restart reconciliation to complete or abort deterministically.
    pub async fn remove_group_member_with_evidence(
        &self,
        gid: GroupId,
        node: NodeId,
        evidence: MembershipShrinkEvidence,
    ) -> Result<(), String> {
        let raft = self
            .groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))?;
        let (mut voters, current_term, current_leader, learners, learner_caught_up_live) = {
            let metrics = raft.metrics();
            let watched = metrics.borrow_watched();
            let v: BTreeSet<NodeId> = watched
                .membership_config
                .voter_ids()
                .collect();
            let learners: BTreeSet<NodeId> = watched
                .membership_config
                .membership()
                .learner_ids()
                .collect();
            let learner_caught_up_live = evidence
                .observed_learner
                .and_then(|learner| {
                    watched.replication.as_ref().and_then(|progress| {
                        progress
                            .get(&learner)
                            .and_then(|log_id| log_id.as_ref().map(|log_id| log_id.index))
                    })
                })
                .zip(watched.last_log_index)
                .is_some_and(|(learner_index, leader_index)| learner_index >= leader_index);
            (
                v,
                watched.current_term,
                watched.current_leader,
                learners,
                learner_caught_up_live,
            )
        };
        if !voters.remove(&node) {
            return Ok(());
        }
        if voters.is_empty() {
            return Err(format!(
                "refusing to remove the last voter {node} from group {gid}"
            ));
        }
        if voters.len() < 2 {
            return Err(format!(
                "refusing to shrink group {gid} below two retained voters"
            ));
        }
        let learner = evidence
            .observed_learner
            .ok_or_else(|| "membership shrink requires a caught-up learner".to_string())?;
        if !learners.contains(&learner) {
            return Err("membership shrink learner is not in the committed learner set".to_string());
        }
        if current_leader != Some(self.node_id) {
            return Err("membership shrink must be proposed by the current group leader".to_string());
        }
        if current_leader == Some(node) {
            return Err("membership shrink requires leadership transfer before removal".to_string());
        }
        if !learner_caught_up_live {
            return Err("membership shrink learner has not caught up to the leader log".to_string());
        }
        let expected_voters: Vec<NodeId> = {
            let mut current: Vec<NodeId> = voters.iter().copied().collect();
            current.push(node);
            current.sort_unstable();
            current
        };
        if evidence.observed_term != current_term
            || evidence.observed_voters != expected_voters
            || evidence.observed_leader != current_leader
        {
            return Err("membership shrink evidence does not match live term or voter set".to_string());
        }
        let mut journal = MembershipShrinkJournal::new(
            gid,
            node,
            learner,
            current_term,
            expected_voters,
        )?;
        if let Some(existing) = self
            .placement
            .membership_shrink_journal(&journal.operation_id)
            .await?
        {
            if existing.phase.terminal() {
                return if existing.phase == MembershipShrinkPhase::Completed {
                    Ok(())
                } else {
                    Err("membership shrink has a retained terminal abort".to_string())
                };
            }
            return Err("membership shrink already has an active recovery journal".to_string());
        }
        self.persist_membership_shrink_journal(&journal).await?;

        for phase in [
            MembershipShrinkPhase::DrainRequested,
            MembershipShrinkPhase::Drained,
            MembershipShrinkPhase::LearnerCaughtUp,
            MembershipShrinkPhase::LeadershipTransferred,
            MembershipShrinkPhase::SafetyChecked,
        ] {
            journal = journal.advance(phase, evidence.clone())?;
            self.persist_membership_shrink_journal(&journal).await?;
        }
        if !journal.ready_for_removal() {
            return Err("membership shrink did not reach its durable safety gate".to_string());
        }

        raft.change_membership(voters.clone(), false)
            .await
            .map_err(|e| format!("change_membership group {gid} remove {node}: {e}"))?;

        let (observed_voters, observed_leader) = {
            let metrics = raft.metrics();
            let watched = metrics.borrow_watched();
            let mut observed: Vec<NodeId> = watched
                .membership_config
                .voter_ids()
                .collect();
            observed.sort_unstable();
            (observed, watched.current_leader)
        };
        if observed_voters != voters.iter().copied().collect::<Vec<_>>() || observed_leader == Some(node)
        {
            return Err("membership shrink commit did not produce the expected voter fence".to_string());
        }
        let mut committed_evidence = evidence;
        committed_evidence.observed_voters = observed_voters;
        committed_evidence.observed_leader = observed_leader;
        committed_evidence.membership_change_committed = true;
        committed_evidence.target_absent = true;
        journal = journal.advance(
            MembershipShrinkPhase::RemovalCommitted,
            committed_evidence.clone(),
        )?;
        self.persist_membership_shrink_journal(&journal).await?;
        journal = journal.advance(MembershipShrinkPhase::Completed, committed_evidence)?;
        self.persist_membership_shrink_journal(&journal).await?;
        Ok(())
    }

    // ── R1: leader balancing across groups (CONCEPT:EG-KG.sharding.multi-raft → KG-2.273) ────
    //
    // With N groups over M nodes, leaders cluster on the bootstrap node (it
    // single-member-initializes every group). [`rebalance_leaders`] spreads leadership by
    // a deterministic round-robin: each group has a target leader computed identically on
    // every node ([`desired_leader`]). EVERY node runs this pass (like a real cluster);
    // each only acts on the groups it currently LEADS:
    //
    //   * **Transfer** — if THIS node IS the leader of a group whose round-robin target
    //     is ELSEWHERE, it issues the native openraft-0.10
    //     `trigger().transfer_leader(target)`. openraft hands a fresh term + the leader
    //     vote to the target and notifies it (over `NetTransferLeader`) to campaign at
    //     once — a GRACEFUL, near-instant handoff. No cooperative heartbeat-yield is
    //     needed any more (that was the 0.9 workaround for the missing transfer RPC).
    //
    // A follower never acts (only the current leader can transfer). Converges to the
    // round-robin spread within roughly one heartbeat, not a couple of election timeouts.

    /// Run one leader-balancing pass over the groups running on THIS node
    /// (CONCEPT:AU-KG.backend.authority-has-already-acked). For each group THIS node leads whose round-robin target is a
    /// different node, it issues the native `trigger().transfer_leader(target)` for an
    /// instant graceful handoff (rate-limited per group by [`TRANSFER_COOLDOWN`] so it
    /// never spams transfers while one settles). A no-op for single-voter groups and for
    /// groups this node already leads correctly (or does not lead), so repeated passes on
    /// a balanced cluster do nothing. Returns a [`RebalanceReport`].
    pub async fn rebalance_leaders(&self) -> RebalanceReport {
        let mut report = RebalanceReport::default();
        let mut observations = Vec::new();
        {
            let groups = self.groups.read().await;
            for (&gid, raft) in groups.iter() {
                let (mut voters, current_leader, local_is_leader, discovered_domains) = {
                    let metrics = raft.metrics();
                    let m = metrics.borrow_watched();
                    let voters: Vec<NodeId> = m.membership_config.voter_ids().collect();
                    let discovered_domains = m
                        .membership_config
                        .nodes()
                        .map(|(node_id, node)| {
                            (
                                *node_id,
                                super::config::failure_domain_for_peer(*node_id, &node.addr),
                            )
                        })
                        .collect::<Vec<_>>();
                    (
                        voters,
                        m.current_leader,
                        matches!(m.state, openraft::ServerState::Leader),
                        discovered_domains,
                    )
                };
                voters.sort_unstable();
                let Some(target) = desired_leader(gid, &voters) else {
                    continue;
                };
                report.targets.insert(gid, target);
                observations.push((
                    gid,
                    raft.clone(),
                    voters,
                    current_leader,
                    local_is_leader,
                    target,
                    discovered_domains,
                ));
            }
        }

        // Build a cluster-consistent, best-effort load view from committed Raft
        // metrics.  Every node sees the same leader assignment once caught up,
        // while an unsettled/unknown leader simply contributes no load and cannot
        // trigger a speculative transfer.
        let mut leader_loads: BTreeMap<NodeId, usize> = BTreeMap::new();
        for (_, _, voters, current_leader, _, _, _) in &observations {
            if let Some(leader) = current_leader.filter(|leader| voters.contains(leader)) {
                *leader_loads.entry(leader).or_default() += 1;
            }
        }
        let leader_view_complete = observations.iter().all(|(_, _, voters, leader, _, _, _)| {
            leader.is_some_and(|leader| voters.contains(&leader))
        });
        {
            let mut known_domains = self.failure_domains.write();
            for (_, _, _, _, _, _, discovered_domains) in &observations {
                for (node_id, domain) in discovered_domains {
                    known_domains.entry(*node_id).or_insert_with(|| domain.clone());
                }
            }
        }
        let failure_domains = self.failure_domains.read().clone();

        for (
            gid,
            raft,
            voters,
            current_leader,
            local_is_leader,
            target,
            _,
        ) in observations
        {
            // Nothing to balance for a single-voter group.
            if voters.len() <= 1 {
                continue;
            }
            let Some(current) = current_leader else {
                report
                    .skipped
                    .push((gid, "current leader is not yet observed".to_string()));
                continue;
            };
            // Only the current local leader can hand off, and only when the target
            // is elsewhere.  This also prevents every follower from competing to
            // transfer the same group.
            if !local_is_leader || current != self.node_id || target == self.node_id {
                continue;
            }
            if !leader_view_complete {
                report.skipped.push((
                    gid,
                    "leader-load view is incomplete while membership/terms settle".to_string(),
                ));
                continue;
            }
            if !failure_domain_safe(&failure_domains, current, target) {
                report.skipped.push((
                    gid,
                    "target shares the current leader failure domain or has no domain"
                        .to_string(),
                ));
                continue;
            }
            let current_load = leader_loads.get(&current).copied().unwrap_or_default();
            let target_load = leader_loads.get(&target).copied().unwrap_or_default();
            if current_load.saturating_sub(target_load) < LEADER_TRANSFER_MIN_MARGIN {
                report.skipped.push((
                    gid,
                    format!(
                        "leader-load margin below hysteresis (current={current_load}, target={target_load})"
                    ),
                ));
                continue;
            }
            let transfer_attempts = report.transferred.len() + report.errors.len();
            if transfer_attempts >= MAX_AUTOMATIC_TRANSFERS_PER_PASS {
                report
                    .skipped
                    .push((gid, "per-pass automatic transfer bound reached".to_string()));
                continue;
            }
            if self.may_transfer(gid) {
                match raft.trigger().transfer_leader(target).await {
                    Ok(()) => report.transferred.push(gid),
                    Err(e) => report.errors.push((gid, e.to_string())),
                }
            }
        }
        report.transferred.sort_unstable();
        report.skipped.sort_unstable_by_key(|(gid, _)| *gid);
        report
    }

    /// True iff the balancer may issue a leader transfer for `gid` now (cooldown
    /// elapsed), recording the attempt so the next call within [`TRANSFER_COOLDOWN`] is
    /// refused.
    fn may_transfer(&self, gid: GroupId) -> bool {
        let now = Instant::now();
        if let Some(prev) = self.last_transfer.get(&gid) {
            if now.duration_since(*prev) < TRANSFER_COOLDOWN {
                return false;
            }
        }
        self.last_transfer.insert(gid, now);
        true
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
        let route = self.route_graph(graph_name).await;
        self.group(route.group).await
    }

    /// The placement catalog (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1) — the ONE placement
    /// authority. Exposed for admin/diagnostic queries (`route`, `redirect_if_stale`,
    /// `all_entries`); mutations go through this manager's `placement_*` methods so
    /// they commit through Raft.
    pub fn placement(&self) -> Arc<PlacementCatalog> {
        self.placement.clone()
    }

    /// Resolve `graph_name`'s group (CONCEPT:EG-KG.sharding.placement-catalog — the dispatch/routing seam):
    /// the placement catalog FIRST (an explicit virtual-partition placement for the
    /// graph's tenant), then applying the engine-owned unplaced policy when no
    /// durable row exists. The returned answer is authoritative in both cases;
    /// callers never hash locally.
    pub async fn route_graph(&self, graph_name: &str) -> PlacementRoute {
        let (tenant, sub_key) = placement::split_tenant_key(graph_name);
        self.placement
            .route(tenant, sub_key, self.router.group_of(graph_name))
            .await
    }

    /// Resolve a bounded cross-graph route vector from one placement-catalog scan.
    pub async fn route_graphs(&self, graph_names: &[String]) -> Vec<PlacementRoute> {
        let keys: Vec<(String, String, GroupId)> = graph_names
            .iter()
            .map(|graph_name| {
                let (tenant, sub_key) = placement::split_tenant_key(graph_name);
                (
                    tenant.to_string(),
                    sub_key.to_string(),
                    self.router.group_of(graph_name),
                )
            })
            .collect();
        self.placement.route_many(&keys).await
    }

    /// Authoritative route for the wire-level `(tenant, sub_key)` form.
    pub async fn route_partition(&self, tenant: &str, sub_key: &str) -> PlacementRoute {
        let graph_name = if tenant == sub_key {
            tenant.to_string()
        } else {
            format!("{tenant}:{sub_key}")
        };
        self.placement
            .route(tenant, sub_key, self.router.group_of(&graph_name))
            .await
    }

    /// Build one internal placement request. Placement mutations are ordinary
    /// graph methods, but they always use the DEFAULT group's replicated log;
    /// no local shortcut is permitted.
    async fn placement_method_request(&self, method: &Method) -> Result<RaftRequest, String> {
        self.ensure_group(DEFAULT_GROUP).await?;
        let server_secret = self.ctx.state.read().await.auth_secret.clone();
        Ok(RaftRequest {
            graph_fname: crate::persist::sanitize(placement::PLACEMENT_GRAPH),
            graph_name: placement::PLACEMENT_GRAPH.to_string(),
            graph_type: crate::protocol::GraphType::Commons,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-placement",
                placement::PLACEMENT_GRAPH,
                &crate::server::mutation_batch::opaque_request_key(
                    "placement-operation",
                    placement::PLACEMENT_GRAPH,
                    0,
                    method,
                ),
                0,
                0,
            ),
            command: super::ReplicatedMutation::graph(method.clone(), &server_secret)?,
        })
    }

    async fn commit_placement_method(&self, method: &Method) -> Result<RaftResponse, String> {
        let req = self.placement_method_request(method).await?;
        self.client_write_group(DEFAULT_GROUP, req).await
    }

    /// Commit a batch of placement-catalog mutations through the DEFAULT group's Raft
    /// consensus (CONCEPT:EG-KG.sharding.placement-catalog — the replication seam). Ensures the default
    /// group is running (idempotent) so the catalog is usable even on a fresh
    /// single-group deployment that never called [`configure_group_ring`](Self::configure_group_ring).
    async fn commit_placement(&self, methods: &[Method]) -> Result<(), String> {
        for method in methods {
            let response = self.commit_placement_method(method).await?;
            if let Some(error) = response.native_error {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Complete a [`placement::PendingWrite`] in two fenced parts: first reserve
    /// an epoch through the replicated counter CAS, then apply the placement row
    /// CAS/add/remove methods. The counter CAS result is surfaced instead of
    /// being discarded; callers re-plan on contention before any placement row
    /// is written.
    async fn commit_placement_plan(
        &self,
        plan: &placement::PendingWrite<'_>,
    ) -> Result<PlacementCommitOutcome, String> {
        if let Some(allocation) = plan.epoch_allocation {
            if allocation.seed_if_absent {
                let seed = placement::PlacementCatalog::epoch_seed_method(allocation.floor);
                let response = self.commit_placement_method(&seed).await?;
                if let Some(error) = response.native_error {
                    return Err(error);
                }
            }
            if allocation.expected != allocation.floor {
                let reconcile = placement::PlacementCatalog::epoch_reconcile_method(
                    allocation.expected,
                    allocation.floor,
                );
                let response = self.commit_placement_method(&reconcile).await?;
                if let Some(error) = response.native_error {
                    return Err(error);
                }
                if !matches!(response.native_result, Some(ResultPayload::Bool(true))) {
                    return Ok(PlacementCommitOutcome::EpochConflict);
                }
            }
            let reserve = placement::PlacementCatalog::epoch_cas_method(
                allocation.floor,
                allocation.allocated,
            );
            let response = self.commit_placement_method(&reserve).await?;
            if let Some(error) = response.native_error {
                return Err(error);
            }
            if !matches!(response.native_result, Some(ResultPayload::Bool(true))) {
                return Ok(PlacementCommitOutcome::EpochConflict);
            }
        }

        for method in &plan.methods {
            let response = self.commit_placement_method(method).await?;
            if let Some(error) = response.native_error {
                return Err(error);
            }
            if plan.require_success
                && !matches!(response.native_result, Some(ResultPayload::Bool(true)))
            {
                return Err("placement row CAS fence rejected the stale proposal".to_string());
            }
        }
        Ok(PlacementCommitOutcome::Committed)
    }

    /// Placement plans must be built on the current DEFAULT-group leader. A
    /// follower's catalog can legitimately lag the leader, so silently planning
    /// from its local image would make the CAS retry loop unable to reconstruct
    /// the right placement pre-image. Callers must retry through the leader.
    async fn require_placement_leader(&self) -> Result<(), String> {
        self.ensure_group(DEFAULT_GROUP).await?;
        let group = self
            .group(DEFAULT_GROUP)
            .await
            .ok_or_else(|| "placement group is not running on this node".to_string())?;
        match group.current_leader().await {
            Some(leader) if leader == self.node_id => Ok(()),
            Some(leader) => Err(format!(
                "placement mutation must be issued to current leader {leader}"
            )),
            None => Err("placement group has no current leader".to_string()),
        }
    }

    /// Commit this node's [`super::node_info_store::NodeInfo`][ni] self-report
    /// through the DEFAULT group's Raft consensus (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1 —
    /// the cluster-topology replication seam; mirrors [`commit_placement`](Self::commit_placement)).
    /// Ensures the default group is running (idempotent) so a fresh single-group
    /// deployment that never called [`configure_group_ring`](Self::configure_group_ring)
    /// can still self-report. [`client_write_group`](Self::client_write_group)
    /// transparently forwards to the current leader when this node is a follower,
    /// so a node calling this immediately after its OWN startup (almost never the
    /// leader) still succeeds. Every node's local `NodeInfoStore` converges
    /// identically because the SAME committed log entry applies deterministically
    /// on every replica — NOT graph nodes (placement's O(N) lesson).
    ///
    /// [ni]: crate::server::persistence::node_info_store::NodeInfo
    pub(crate) async fn commit_node_info(
        &self,
        cluster_id: String,
        node_id: NodeId,
        member_identity: String,
        raft_addr: String,
        advertised_client_addr: String,
        tls_server_name: Option<String>,
        certificate_id: Option<String>,
        certificate_rotation_epoch: u64,
        certificate_not_before_ms: Option<u64>,
        certificate_not_after_ms: Option<u64>,
    ) -> Result<(), String> {
        self.ensure_group(DEFAULT_GROUP).await?;
        let server_secret = self.ctx.state.read().await.auth_secret.clone();
        let method = Method::NodeInfoUpsert {
            cluster_id,
            node_id,
            member_identity,
            raft_addr,
            advertised_client_addr,
            tls_server_name,
            certificate_id,
            certificate_rotation_epoch,
            certificate_not_before_ms,
            certificate_not_after_ms,
        };
        let command =
            crate::raft::NativeMutationCommand::from_public_method(method, &server_secret)
                .map_err(|_| "node info command has no bounded native domain".to_string())?;
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(placement::PLACEMENT_GRAPH),
            graph_name: placement::PLACEMENT_GRAPH.to_string(),
            graph_type: crate::protocol::GraphType::Commons,
            committed_at_ms: 0,
            mutation: super::RaftMutationContext::internal(
                "raft-node-info",
                placement::PLACEMENT_GRAPH,
                &format!("node-{node_id}"),
                0,
                0,
            ),
            command: super::ReplicatedMutation::Native { command },
        };
        let response = self.client_write_group(DEFAULT_GROUP, req).await?;
        if let Some(error) = response.native_error {
            return Err(error);
        }
        Ok(())
    }

    /// Persist one crash-recovery journal transition through the placement Raft
    /// group before the move driver performs its next side effect.
    pub(crate) async fn persist_move_journal(
        &self,
        journal: &placement::PartitionMoveJournal,
    ) -> Result<(), String> {
        if let Some(current) = self.placement.move_journal(&journal.move_id).await? {
            if !current.permits_successor(journal) {
                return Err("partition move journal transition is not monotonic".to_string());
            }
        }
        let method = placement::PlacementCatalog::move_journal_method(journal)?;
        self.commit_placement(&[method]).await
    }

    async fn persist_membership_shrink_journal(
        &self,
        journal: &MembershipShrinkJournal,
    ) -> Result<(), String> {
        if let Some(current) = self
            .placement
            .membership_shrink_journal(&journal.operation_id)
            .await?
        {
            if !current.permits_successor(journal) {
                return Err("membership shrink journal transition is not monotonic".to_string());
            }
        }
        let method = PlacementCatalog::membership_shrink_method(journal)?;
        self.commit_placement(&[method]).await
    }

    /// Assign the WHOLE keyspace of `tenant` to `group` (CONCEPT:EG-KG.sharding.placement-catalog admin
    /// API). Collapses any prior split. Returns the new routing epoch.
    pub async fn placement_assign(&self, tenant: &str, group: GroupId) -> Result<u64, String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        for _ in 0..MAX_PLACEMENT_EPOCH_RETRIES {
            let plan = self.placement.plan_assign(tenant, group).await?;
            let epoch = plan.epoch;
            match self.commit_placement_plan(&plan).await? {
                PlacementCommitOutcome::Committed => return Ok(epoch),
                PlacementCommitOutcome::EpochConflict => continue,
            }
        }
        Err("placement epoch CAS contention exceeded retry bound".to_string())
    }

    /// Split `tenant`'s partition covering `at` into `[.., at) → group_a` and
    /// `[at, ..] → group_b` (CONCEPT:EG-KG.sharding.placement-catalog admin API — one tenant spans two
    /// groups). Returns the new routing epoch (shared by both halves).
    pub async fn placement_split(
        &self,
        tenant: &str,
        at: u64,
        group_a: GroupId,
        group_b: GroupId,
    ) -> Result<u64, String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        for _ in 0..MAX_PLACEMENT_EPOCH_RETRIES {
            let plan = self
                .placement
                .plan_split(tenant, at, group_a, group_b)
                .await?;
            let epoch = plan.epoch;
            match self.commit_placement_plan(&plan).await? {
                PlacementCommitOutcome::Committed => return Ok(epoch),
                PlacementCommitOutcome::EpochConflict => continue,
            }
        }
        Err("placement epoch CAS contention exceeded retry bound".to_string())
    }

    /// Merge every one of `tenant`'s ranged partitions back onto `group` (CONCEPT:EG-KG.sharding.placement-catalog
    /// admin API — the inverse of `placement_split`; also how independent small
    /// tenants are made to share a group, by `placement_assign`ing each to it).
    pub async fn placement_merge(&self, tenant: &str, group: GroupId) -> Result<u64, String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        for _ in 0..MAX_PLACEMENT_EPOCH_RETRIES {
            let plan = self.placement.plan_merge(tenant, group).await?;
            let epoch = plan.epoch;
            match self.commit_placement_plan(&plan).await? {
                PlacementCommitOutcome::Committed => return Ok(epoch),
                PlacementCommitOutcome::EpochConflict => continue,
            }
        }
        Err("placement epoch CAS contention exceeded retry bound".to_string())
    }

    /// Mark `(tenant, range)` mid-move to `target` (CONCEPT:EG-KG.sharding.placement-catalog admin API —
    /// online-move step 1). `route` keeps answering with the source group until
    /// [`placement_fence_cutover`](Self::placement_fence_cutover) commits. Prefer
    /// [`super::reshard::TenantManager::move_partition`], which drives this + the
    /// per-graph data move + the cutover as one state machine.
    pub(crate) async fn placement_start_move(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<(), String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        let plan = self
            .placement
            .plan_start_move(tenant, range, target)
            .await?;
        self.commit_placement_plan(&plan).await?;
        Ok(())
    }

    /// Fence the cutover of `(tenant, range)` to `target` (CONCEPT:EG-KG.sharding.placement-catalog admin API
    /// — online-move step 3): bumps the epoch and flips the authoritative group in one
    /// commit. Returns the new epoch.
    pub(crate) async fn placement_fence_cutover(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<u64, String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        for _ in 0..MAX_PLACEMENT_EPOCH_RETRIES {
            let plan = self
                .placement
                .plan_fence_cutover(tenant, range, target)
                .await?;
            let epoch = plan.epoch;
            match self.commit_placement_plan(&plan).await? {
                PlacementCommitOutcome::Committed => return Ok(epoch),
                PlacementCommitOutcome::EpochConflict => continue,
            }
        }
        Err("placement epoch CAS contention exceeded retry bound".to_string())
    }

    /// Roll a move back only while the placement entry is still behind the cutover
    /// fence.  Post-cutover callers must reconcile forward.
    pub(crate) async fn placement_abort_move(
        &self,
        tenant: &str,
        range: (u64, u64),
        source: GroupId,
        target: GroupId,
        original_epoch: u64,
    ) -> Result<(), String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_tenant_is_prepared(tenant) {
            return Err("placement change conflicts with a prepared transaction".to_string());
        }
        self.require_placement_leader().await?;
        let plan = self
            .placement
            .plan_abort_move(tenant, range, source, target, original_epoch)
            .await?;
        self.commit_placement_plan(&plan).await?;
        Ok(())
    }

    /// Close (shut down + drop) a group on this node — group lifecycle, the elastic
    /// resharding/destroy seam. The group's durable log/meta rows are NOT deleted
    /// here; durable graph lifecycle/GC owns data destruction separately. This stops replication
    /// and frees the in-RAM group. Idempotent.
    pub async fn close_group(&self, gid: GroupId) -> Result<(), String> {
        let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
        if crate::server::txn::consensus_has_prepared_graphs() {
            return Err("group close conflicts with a prepared transaction".to_string());
        }
        let raft = self.groups.write().await.remove(&gid);
        if let Some(raft) = raft {
            let _ = raft.shutdown().await;
            tracing::info!("Raft group {gid} closed on node {}", self.node_id);
        }
        Ok(())
    }

    /// A placement-aware [`RaftHandle`] that routes a graph through ITS group. The
    /// returned epoch must travel with the operation/response as its fencing token.
    /// Returns `None` if the authoritative group isn't running on this node.
    pub async fn handle_for_graph(self: &Arc<Self>, graph_name: &str) -> Option<RoutedRaftHandle> {
        let route = self.route_graph(graph_name).await;
        let raft = self.groups.read().await.get(&route.group).cloned()?;
        Some(RoutedRaftHandle {
            handle: RaftHandle {
                raft,
                node_id: self.node_id,
            },
            group_id: route.group,
            epoch: route.epoch,
            placed: route.placed,
        })
    }

    /// Shut down the listener (and, by drop, stop accepting). Groups keep running
    /// until dropped/closed; used by graceful shutdown + tests.
    pub fn stop_listener(&self) {
        if let Some(listener) = self
            .listener_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            listener.abort();
        }
        self.connection_tasks.stop();
    }

    /// Fully stop this node's Raft resources in dependency order. The sequence is
    /// idempotent so startup-error cleanup and an explicit caller shutdown may
    /// safely race or run more than once: listener/tasks stop first, groups are
    /// drained and joined, and only then are persistence writer threads stopped.
    pub async fn shutdown(&self) {
        let _shutdown = self.shutdown_lock.lock().await;

        // Stop control-plane workers before draining groups so no new heartbeat
        // waiter or leader-transfer decision can be created while resources are
        // being torn down.
        self.heartbeat_coalescer.stop();
        let heartbeat_task = self
            .heartbeat_flush_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = heartbeat_task {
            task.abort();
            let _ = task.await;
        }
        let leader_task = self
            .leader_balance_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = leader_task {
            task.abort();
            let _ = task.await;
        }

        let listener = {
            let mut listener = self
                .listener_handle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            listener.take()
        };
        if let Some(listener) = listener {
            listener.abort();
            let _ = listener.await;
        }
        self.connection_tasks.stop_and_wait().await;

        let groups = {
            let mut groups = self.groups.write().await;
            std::mem::take(&mut *groups)
        };
        for (_, raft) in groups {
            let _ = raft.shutdown().await;
        }
        let backend = self.backend.clone();
        if let Err(error) = tokio::task::spawn_blocking(move || backend.shutdown()).await {
            tracing::warn!(%error, "Raft persistence shutdown task failed");
        }
    }
}

/// Serve one connection on the shared listener, demuxing each framed RPC to the
/// group it is tagged for (CONCEPT:EG-KG.sharding.raft-resharding). An RPC for a group this node doesn't
/// run gets a per-variant error reply (openraft treats it as a transient failure).
async fn serve_conn(
    stream: tokio::net::TcpStream,
    groups: Arc<RwLock<BTreeMap<GroupId, EgRaft>>>,
    auth: Option<Arc<network::RaftTransportAuth>>,
    frame_budget: Arc<tokio::sync::Semaphore>,
    read_service: Arc<super::xread::ReadPageService>,
) -> std::io::Result<()> {
    let mut stream = network::RaftConnection::accept(stream, auth.as_deref(), frame_budget).await?;
    loop {
        let body = match tokio::time::timeout(network::RAFT_FRAME_IO_TIMEOUT, stream.read_payload())
            .await
        {
            Ok(Ok(body)) => body,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "raft frame read timed out",
                ))
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(e)) => return Err(e),
        };
        let frame: RaftFrame = network::decode_wire(&body)?;
        // A `One` frame demuxes to one group; a `Batch` (coalesced heartbeats,
        // CONCEPT:EG-KG.storage.concept-2) demuxes each tagged sub-RPC to ITS group and replies in the
        // SAME order so each awaiting caller matches its own reply.
        let reply = match frame {
            RaftFrame::One(rpc) => {
                let gid = rpc.group_id();
                let raft = groups.read().await.get(&gid).cloned();
                RaftFrameReply::One(network::dispatch_group(raft, gid, *rpc, &read_service).await)
            }
            RaftFrame::Batch(rpcs) => {
                if rpcs.is_empty()
                    || rpcs.len() > network::MAX_RAFT_BATCH_RPCS
                    || rpcs
                        .iter()
                        .any(|rpc| !network::HeartbeatCoalescer::is_heartbeat(rpc))
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid raft heartbeat batch",
                    ));
                }
                let mut replies: Vec<GroupRpcReply> = Vec::with_capacity(rpcs.len());
                for rpc in rpcs {
                    let gid = rpc.group_id();
                    let raft = groups.read().await.get(&gid).cloned();
                    replies.push(network::dispatch_group(raft, gid, rpc, &read_service).await);
                }
                RaftFrameReply::Batch(replies)
            }
        };
        let out = rmp_serde::to_vec_named(&reply)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        tokio::time::timeout(network::RAFT_FRAME_IO_TIMEOUT, stream.write_payload(&out))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "raft frame write timed out")
            })??;
    }
}
