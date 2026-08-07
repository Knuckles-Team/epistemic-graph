//! Placement catalog — the ONE placement authority for virtual partitions
//! (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1).
//!
//! ## Why this exists
//!
//! [`super::multi::GroupRouter`] gives every un-pinned graph a STATELESS hash-ring
//! route (`graph_name → GroupId`), and its per-graph `assign` override plus
//! [`super::reshard::TenantManager`] give ONE graph an online move. That is not yet a
//! placement AUTHORITY: there is no durable, replicated, versioned record of "this
//! tenant's keyspace lives here" that spans multiple graphs, supports splitting a
//! hot tenant across groups, or lets a caller detect it is talking to a group that no
//! longer owns the data.
//!
//! [`PlacementCatalog`] is that authority. A **virtual partition** is a tenant plus an
//! (optional) sub-range of its workspace/session/entity keyspace
//! ([`PartitionKey`]) — the SAME tenant can have ONE whole-keyspace partition (the
//! common case: one tenant, one group) or be [`split`](MultiRaft::placement_split)
//! into several ranged partitions on DIFFERENT groups (one tenant spans shards); many
//! small tenants can independently point their whole-keyspace partition at the SAME
//! group (small tenants share). Every placement change bumps a single, cluster-wide,
//! monotonic **routing epoch** ([`next_epoch`](PlacementCatalog::next_epoch)), so a
//! caller holding a stale `(group, epoch)` pair can be told to redirect
//! ([`PlacementCatalog::redirect_if_stale`]) instead of being served against the
//! wrong shard.
//!
//! ## Persisted AND Raft-replicated — reusing the engine's own graph machinery
//!
//! Rather than inventing a second replicated store, the catalog IS a graph:
//! [`PLACEMENT_GRAPH`] (`__placement_catalog__`) is an ordinary control graph whose
//! nodes are `msgpack([`PlacementEntry`])` blobs written through
//! [`Method::AddNode`]/[`Method::RemoveNode`] — the SAME durable mutation type every
//! tenant graph uses. [`MultiRaft`](super::multi::MultiRaft)'s placement admin methods
//! commit these through the DEFAULT group's `client_write`, so:
//!
//! * **Durable** — the mutation lands in the authoritative shard through the same state-backed
//!   MutationBatch kernel as any other graph (CONCEPT:EG-KG.storage.one-fsync-covers-raft), so a restart
//!   reloads it with `load_all` like any graph.
//! * **Raft-replicated** — when Raft is active, the SAME committed log entry applies
//!   on every node via [`super::store::EgStore::apply_request`] with explicit opaque
//!   internal mutation authority, so every node's local
//!   `__placement_catalog__` graph converges identically.
//!
//! [`PlacementCatalog`] itself holds no separate cache: [`PlacementCatalog::route`]
//! reads the control graph fresh (cheap — the catalog holds far fewer rows than user
//! data), so a follower that just applied a replicated placement change sees it on the
//! very next `route` call with no extra invalidation plumbing.
//!
//! ## Online move — snapshot → CDC catch-up → fenced cutover
//!
//! [`super::reshard::TenantManager::move_partition`] is the online-move state machine,
//! reusing [`super::reshard::TenantManager::reshard_graph`] (already proven: quiesce →
//! durability-barrier snapshot → re-point → resume) as the per-graph data-move
//! primitive:
//!
//! 1. **Start move** ([`MultiRaft::placement_start_move`]) marks the partition
//!    [`PartitionState::Moving`] — [`route`](PlacementCatalog::route) still returns the
//!    SOURCE group/epoch (no client is redirected yet); this is the window the source
//!    keeps serving while the target "catches up".
//! 2. **Snapshot + catch-up** — for every graph the partition covers,
//!    `reshard_graph` durably checkpoints it and re-points that graph's OWN
//!    [`GroupRouter`](super::multi::GroupRouter) entry to the target group. Because
//!    every group shares ONE authoritative shard/registry, this durability barrier IS the
//!    data transfer (no bulk copy needed) — the same invariant `reshard_graph`
//!    documents.
//! 3. **Fenced cutover** ([`MultiRaft::placement_fence_cutover`]) bumps the epoch and
//!    flips the partition's authoritative group to the target, atomically (one
//!    control-graph write). From this instant, [`PlacementCatalog::redirect_if_stale`]
//!    answers ANY caller still presenting the pre-cutover epoch with a redirect to
//!    `(target, new_epoch)` rather than serving it — the fence.
//!
//! The immutable graph inventory (derived from the engine catalog, never supplied as
//! a caller-maintained remainder), original route, completed graph set, and every
//! stage transition are stored as a [`PartitionMoveJournal`] in the same replicated
//! control graph.  Startup reconciliation resumes any non-terminal journal and
//! re-verifies durable graph presence before cutover.  Before the epoch fence an
//! operator can abort and restore the source route; after the fence recovery is
//! deliberately roll-forward only.
//!
//! ## What this increment does NOT do (documented follow-ups)
//!
//! * **Concurrent admin ops on a live multi-writer cluster.** `next_epoch` reads the
//!   catalog then writes the incremented value under a LOCAL (this-node) lock; two
//!   placement admin calls issued concurrently on the SAME node serialize correctly,
//!   but Raft's single-leader-writer property is what actually prevents two DIFFERENT
//!   nodes computing the same next epoch — fine for the low-frequency admin path this
//!   targets, but a documented limitation vs. a CAS-style engine-level guard.
//! External clients consume this authority through `PlacementRoute`. An absent
//! catalog row returns the engine-owned unplaced policy; it never delegates a
//! placement decision to the caller.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, RwLock};

use super::GroupId;
use crate::protocol::Method;
use crate::server::ServerState;

/// The control graph every placement entry is durably stored in as one node per
/// virtual partition (CONCEPT:EG-KG.sharding.placement-catalog). An ordinary graph — created
/// on first write via the SAME registry auto-create every Raft-applied graph
/// mutation uses — so it rides the engine's existing persistence + replication with
/// no new storage code.
pub const PLACEMENT_GRAPH: &str = "__placement_catalog__";
pub const MAX_PARTITION_MOVE_GRAPHS: usize = 16_384;
const MAX_PARTITION_MOVE_GRAPH_BYTES: usize = 4 * 1024 * 1024;
const MOVE_JOURNAL_NODE_PREFIX: &str = "partition-move:";

fn is_move_journal_node_id(node_id: &str) -> bool {
    node_id
        .strip_prefix(MOVE_JOURNAL_NODE_PREFIX)
        .is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

/// A virtual partition key (CONCEPT:EG-KG.sharding.placement-catalog): a tenant plus an inclusive
/// `[range_start, range_end]` sub-range over a stable hash of the tenant's
/// workspace/session/entity id space. `range_start == 0 && range_end == u64::MAX` is
/// the common "whole tenant, one group" partition; splitting a tenant produces two (or
/// more) narrower, non-overlapping ranges so one tenant can span multiple groups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionKey {
    pub tenant: String,
    pub range_start: u64,
    pub range_end: u64,
}

impl PartitionKey {
    /// The whole-keyspace partition for `tenant` — every workspace/session/entity id
    /// of this tenant hashes into it.
    pub fn whole(tenant: &str) -> Self {
        PartitionKey {
            tenant: tenant.to_string(),
            range_start: 0,
            range_end: u64::MAX,
        }
    }

    /// `true` when the stable hash `h` of a workspace/session/entity id falls inside
    /// this partition's range.
    pub(crate) fn contains(&self, h: u64) -> bool {
        h >= self.range_start && h <= self.range_end
    }

    /// The durable control-graph node id for this key. Unique per (tenant, range) —
    /// the actual key/state live in the node's `properties_msgpack` ([`PlacementEntry`]),
    /// so this id never needs to be parsed back.
    fn node_id(&self) -> String {
        format!(
            "{}\u{1}{:020}\u{1}{:020}",
            self.tenant, self.range_start, self.range_end
        )
    }
}

/// A partition's lifecycle state (CONCEPT:EG-KG.sharding.placement-catalog — the online-move state
/// machine). `Active` is steady-state; `Moving` is the snapshot/CDC-catch-up window
/// BEFORE the fenced cutover — [`PlacementCatalog::route`] still resolves to the
/// CURRENT (source) group while `Moving`, only the cutover flips it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    Active,
    Moving { target: GroupId },
}

/// One durable placement record (CONCEPT:EG-KG.sharding.placement-catalog): the control-graph node
/// body for a [`PartitionKey`]. `epoch` is this entry's routing epoch — the value of
/// the cluster-wide monotonic counter ([`PlacementCatalog::next_epoch`]) at the moment
/// this entry's AUTHORITATIVE group was last set (assign/split/merge/fenced cutover;
/// `start_move` does NOT bump it — the group hasn't changed yet).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementEntry {
    pub key: PartitionKey,
    pub group: GroupId,
    pub epoch: u64,
    pub state: PartitionState,
}

/// Crash-recovery stages for an online partition move.  Every transition is stored
/// in [`PLACEMENT_GRAPH`] through Raft before the next side effect begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveStage {
    Planned,
    Moving,
    Transferring,
    ReadyForCutover,
    CutoverCommitted,
    Aborting,
    Completed,
    Aborted,
}

impl MoveStage {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted)
    }
}

/// Durable move intent and progress.  `graphs` is immutable after planning;
/// `completed_graphs` advances only after each graph passes the durable-presence
/// barrier.  A restart can therefore resume without a caller-supplied remainder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionMoveJournal {
    pub move_id: String,
    pub key: PartitionKey,
    pub source: GroupId,
    pub target: GroupId,
    pub original_epoch: u64,
    pub graphs: Vec<String>,
    pub completed_graphs: Vec<String>,
    pub stage: MoveStage,
}

impl PartitionMoveJournal {
    pub fn new(
        entry: &PlacementEntry,
        target: GroupId,
        mut graphs: Vec<String>,
    ) -> Result<Self, String> {
        if entry.group == target || !matches!(entry.state, PartitionState::Active) {
            return Err(
                "partition move requires an active source and a distinct target".to_string(),
            );
        }
        if graphs.len() > MAX_PARTITION_MOVE_GRAPHS
            || graphs
                .iter()
                .try_fold(0usize, |total, graph| total.checked_add(graph.len()))
                .is_none_or(|total| total > MAX_PARTITION_MOVE_GRAPH_BYTES)
        {
            return Err("partition move graph inventory exceeds the limit".to_string());
        }
        graphs.sort();
        graphs.dedup();
        if graphs
            .iter()
            .any(|graph| graph.is_empty() || graph.len() > 4_096)
        {
            return Err("partition move graph inventory is invalid".to_string());
        }
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph/partition-move/v1\0");
        digest.update(entry.key.tenant.as_bytes());
        digest.update(entry.key.range_start.to_be_bytes());
        digest.update(entry.key.range_end.to_be_bytes());
        digest.update(entry.epoch.to_be_bytes());
        digest.update(target.to_be_bytes());
        for graph in &graphs {
            digest.update((graph.len() as u64).to_be_bytes());
            digest.update(graph.as_bytes());
        }
        let move_id = hex::encode(digest.finalize());
        Ok(Self {
            move_id,
            key: entry.key.clone(),
            source: entry.group,
            target,
            original_epoch: entry.epoch,
            graphs,
            completed_graphs: Vec::new(),
            stage: MoveStage::Planned,
        })
    }

    fn node_id(&self) -> String {
        format!("{MOVE_JOURNAL_NODE_PREFIX}{}", self.move_id)
    }

    pub fn validate(&self) -> bool {
        use sha2::{Digest, Sha256};
        let sorted_unique = |values: &[String]| values.windows(2).all(|pair| pair[0] < pair[1]);
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph/partition-move/v1\0");
        digest.update(self.key.tenant.as_bytes());
        digest.update(self.key.range_start.to_be_bytes());
        digest.update(self.key.range_end.to_be_bytes());
        digest.update(self.original_epoch.to_be_bytes());
        digest.update(self.target.to_be_bytes());
        for graph in &self.graphs {
            digest.update((graph.len() as u64).to_be_bytes());
            digest.update(graph.as_bytes());
        }
        let progress_matches_stage = match self.stage {
            MoveStage::Planned | MoveStage::Moving => self.completed_graphs.is_empty(),
            MoveStage::ReadyForCutover | MoveStage::CutoverCommitted | MoveStage::Completed => {
                self.completed_graphs == self.graphs
            }
            MoveStage::Transferring | MoveStage::Aborting | MoveStage::Aborted => true,
        };
        self.move_id == hex::encode(digest.finalize())
            && self.key.range_start <= self.key.range_end
            && self.source != self.target
            && self.graphs.len() <= MAX_PARTITION_MOVE_GRAPHS
            && self
                .graphs
                .iter()
                .try_fold(0usize, |total, graph| total.checked_add(graph.len()))
                .is_some_and(|total| total <= MAX_PARTITION_MOVE_GRAPH_BYTES)
            && self
                .graphs
                .iter()
                .all(|graph| !graph.is_empty() && graph.len() <= 4_096)
            && sorted_unique(&self.graphs)
            && sorted_unique(&self.completed_graphs)
            && self
                .completed_graphs
                .iter()
                .all(|graph| self.graphs.binary_search(graph).is_ok())
            && progress_matches_stage
    }

    /// Whether `next` is a monotonic update of this exact durable move. This
    /// rejects a stale driver regressing an abort/cutover or dropping completed
    /// graph evidence. An aborted move may be explicitly retried from `Planned`;
    /// its deterministic id is stable because abort does not bump the route epoch.
    pub(crate) fn permits_successor(&self, next: &Self) -> bool {
        if !next.validate()
            || self.move_id != next.move_id
            || self.key != next.key
            || self.source != next.source
            || self.target != next.target
            || self.original_epoch != next.original_epoch
            || self.graphs != next.graphs
        {
            return false;
        }
        let retry = self.stage == MoveStage::Aborted && next.stage == MoveStage::Planned;
        if !retry
            && self
                .completed_graphs
                .iter()
                .any(|graph| next.completed_graphs.binary_search(graph).is_err())
        {
            return false;
        }
        retry
            || matches!(
                (self.stage, next.stage),
                (MoveStage::Planned, MoveStage::Planned)
                    | (MoveStage::Planned, MoveStage::Moving)
                    | (MoveStage::Planned, MoveStage::Transferring)
                    | (MoveStage::Planned, MoveStage::Aborting)
                    | (MoveStage::Moving, MoveStage::Moving)
                    | (MoveStage::Moving, MoveStage::Transferring)
                    | (MoveStage::Moving, MoveStage::Aborting)
                    | (MoveStage::Transferring, MoveStage::Transferring)
                    | (MoveStage::Transferring, MoveStage::ReadyForCutover)
                    | (MoveStage::Transferring, MoveStage::Aborting)
                    | (MoveStage::ReadyForCutover, MoveStage::ReadyForCutover)
                    | (MoveStage::ReadyForCutover, MoveStage::CutoverCommitted)
                    | (MoveStage::ReadyForCutover, MoveStage::Aborting)
                    | (MoveStage::CutoverCommitted, MoveStage::CutoverCommitted)
                    | (MoveStage::CutoverCommitted, MoveStage::Completed)
                    | (MoveStage::Aborting, MoveStage::Aborting)
                    | (MoveStage::Aborting, MoveStage::CutoverCommitted)
                    | (MoveStage::Aborting, MoveStage::Aborted)
                    | (MoveStage::Completed, MoveStage::Completed)
                    | (MoveStage::Aborted, MoveStage::Aborted)
            )
    }
}

/// The authoritative routing answer for one graph/partition.
///
/// `placed` distinguishes a durable catalog row from the catalog's current
/// unplaced policy. Both answers are authoritative: callers never hash or
/// otherwise invent a group. A durable placement always has a non-zero epoch;
/// the unplaced policy uses epoch zero until its first catalog assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementRoute {
    pub group: GroupId,
    pub epoch: u64,
    pub placed: bool,
}

impl PlacementRoute {
    pub fn catalog(group: GroupId, epoch: u64) -> Self {
        debug_assert!(epoch > 0, "catalog placements must have a non-zero epoch");
        Self {
            group,
            epoch,
            placed: true,
        }
    }

    pub fn unplaced(group: GroupId) -> Self {
        Self {
            group,
            epoch: 0,
            placed: false,
        }
    }

    /// Numeric state-machine fence. Together with `epoch` this is the complete
    /// placement token persisted in every authoritative MutationBatch.
    pub fn fencing_token(self) -> GroupId {
        self.group
    }
}

/// Split `graph_name` into `(tenant, sub_key)` (CONCEPT:EG-KG.sharding.placement-catalog): the substring
/// before the FIRST `:` is the tenant, the rest is the workspace/session/entity
/// sub-key that hashes into a tenant's partition range. A name with no `:` is its own
/// tenant AND sub-key (so a whole-tenant placement degenerates to a per-graph pin,
/// consistent with [`super::multi::GroupRouter::assign`]'s per-graph override).
pub fn split_tenant_key(graph_name: &str) -> (&str, &str) {
    match graph_name.split_once(':') {
        Some((tenant, rest)) if !tenant.is_empty() => (tenant, rest),
        _ => (graph_name, graph_name),
    }
}

/// A batch of durable control-graph writes plus the local write-serialization guard
/// (CONCEPT:EG-KG.sharding.placement-catalog). Held by the caller across the ACTUAL Raft commit (see
/// [`super::multi::MultiRaft::commit_placement`]) so a second placement admin call on
/// THIS node cannot compute the same "next epoch" concurrently. Dropping it (after the
/// commit resolves) releases the lock.
pub struct PendingWrite<'a> {
    _guard: MutexGuard<'a, ()>,
    /// The `Method::AddNode`/`Method::RemoveNode` mutations to commit, in order, to
    /// [`PLACEMENT_GRAPH`].
    pub methods: Vec<Method>,
    /// The epoch this plan settles on (unchanged from the pre-op max for a
    /// `start_move`, which does not bump it).
    pub epoch: u64,
}

/// W0.3 in-memory routing index snapshot: the [`PLACEMENT_GRAPH`] core version
/// it was built from, paired with `tenant → its entries`. Named (rather than a
/// bare tuple) purely to satisfy `clippy::type_complexity` on
/// [`PlacementCatalog::index`] — same shape, same semantics.
type PlacementIndexSnapshot = (u64, HashMap<String, Vec<PlacementEntry>>);

/// The ONE placement authority (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1). Reads/plans are
/// pure queries over [`PLACEMENT_GRAPH`]'s current nodes; committing a plan's
/// `methods` through Raft is the caller's job (see
/// [`super::multi::MultiRaft`]'s `placement_*` methods) — this keeps the catalog's
/// logic testable without a running cluster.
pub struct PlacementCatalog {
    state: Arc<RwLock<ServerState>>,
    write_lock: Mutex<()>,
    /// W0.3 in-memory routing index (CONCEPT:EG-KG.sharding.placement-catalog): `tenant → its entries`, paired
    /// with the [`PLACEMENT_GRAPH`] core version it was built from.
    /// `route`/`tenant_entries`/`route_many`/epoch-alloc read this instead of
    /// decoding every node on every call. Rebuilt (one full scan) only when the
    /// paired version no longer matches the graph's CURRENT version
    /// ([`Self::ensure_index_fresh`]) — which covers a LOCAL admin commit (this
    /// node's own `plan_*` calls, which run through the SAME apply path as any
    /// other write) exactly like it covers a placement change this node only
    /// observes via Raft-replicated apply: a follower never calls `plan_*`
    /// locally, so a delta only the admin call sites maintained would go stale on
    /// every follower. [`crate::graph::GraphCore::version`] is the graph's
    /// universal per-commit counter (bumped by `mark_dirty` on every committed
    /// write regardless of which path applied it), so it is the correct
    /// invalidation signal for either case. `None` until first access.
    index: RwLock<Option<PlacementIndexSnapshot>>,
    /// Monotonic high-water mark of every epoch this index has ever observed, so
    /// `plan_assign`/`plan_split`/`plan_fence_cutover` allocate `max_epoch + 1` in
    /// O(1) instead of a full-catalog `max()` scan. Recomputed from scratch on
    /// every index rebuild (see [`Self::ensure_index_fresh`]), which is always
    /// correct because a full rescan reflects the graph's true current state.
    max_epoch: AtomicU64,
}

impl PlacementCatalog {
    pub fn new(state: Arc<RwLock<ServerState>>) -> Self {
        PlacementCatalog {
            state,
            write_lock: Mutex::new(()),
            index: RwLock::new(None),
            max_epoch: AtomicU64::new(0),
        }
    }

    /// [`PLACEMENT_GRAPH`]'s current core version, or `0` if the graph does not
    /// exist yet (an untouched catalog) — the invalidation signal
    /// [`Self::ensure_index_fresh`] compares against.
    async fn graph_version(&self) -> u64 {
        let s = self.state.read().await;
        s.registry
            .get(PLACEMENT_GRAPH)
            .map(|e| e.core.version())
            .unwrap_or(0)
    }

    /// Full scan + decode of [`PLACEMENT_GRAPH`]'s CURRENT nodes (CONCEPT:EG-KG.sharding.placement-catalog) —
    /// the ONE full-scan pass [`Self::ensure_index_fresh`] performs when (and only
    /// when) the index is stale relative to the graph's current version.
    async fn scan_all_entries(&self) -> Vec<PlacementEntry> {
        let core = {
            let s = self.state.read().await;
            s.registry.get(PLACEMENT_GRAPH).map(|e| e.core.clone())
        };
        match core {
            Some(core) => core
                .get_nodes()
                .iter()
                .filter_map(|(_, blob)| {
                    eg_types::msgpack::decode_bounded::<PlacementEntry>(
                        blob,
                        eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 4_096, 16),
                    )
                    .ok()
                    .filter(|entry| {
                        !entry.key.tenant.is_empty()
                            && entry.key.tenant.len() <= 4_096
                            && entry.key.range_start <= entry.key.range_end
                    })
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Ensure the in-memory routing index reflects [`PLACEMENT_GRAPH`]'s CURRENT
    /// core version (CONCEPT:EG-KG.sharding.placement-catalog, W0.3 — the O(N)→O(1) routing fix). A cheap O(1)
    /// version compare when nothing has changed since the last build; a full
    /// scan + decode ONLY when the version has advanced (this node's own admin
    /// commit, or a Raft-replicated apply from the group leader).
    async fn ensure_index_fresh(&self) {
        let current_version = self.graph_version().await;
        if let Some((built_version, _)) = self.index.read().await.as_ref() {
            if *built_version == current_version {
                return;
            }
        }
        let mut guard = self.index.write().await;
        // Re-read: the version may have advanced again, or a racing caller may
        // have already rebuilt for this exact version, while we waited for the
        // write lock.
        let current_version = self.graph_version().await;
        if let Some((built_version, _)) = guard.as_ref() {
            if *built_version == current_version {
                return;
            }
        }
        let mut by_tenant: HashMap<String, Vec<PlacementEntry>> = HashMap::new();
        let mut max_epoch = 0u64;
        for entry in self.scan_all_entries().await {
            max_epoch = max_epoch.max(entry.epoch);
            by_tenant
                .entry(entry.key.tenant.clone())
                .or_default()
                .push(entry);
        }
        self.max_epoch.store(max_epoch, Ordering::Release);
        *guard = Some((current_version, by_tenant));
    }

    /// Allocate the next cluster-wide routing epoch (CONCEPT:EG-KG.sharding.placement-catalog epoch allocation,
    /// W0.3): O(1) off the maintained high-water mark instead of a full-catalog
    /// `max()` scan.
    async fn next_epoch(&self) -> u64 {
        self.ensure_index_fresh().await;
        self.max_epoch.load(Ordering::Acquire) + 1
    }

    /// Every durably-recorded placement entry (CONCEPT:EG-KG.sharding.placement-catalog). Index-backed (W0.3):
    /// reads the in-memory routing index, rebuilt from a full scan only when
    /// [`PLACEMENT_GRAPH`]'s version has advanced since the last build. Empty
    /// (not an error) when the graph doesn't exist yet — an untouched catalog.
    pub async fn all_entries(&self) -> Vec<PlacementEntry> {
        self.ensure_index_fresh().await;
        self.index
            .read()
            .await
            .as_ref()
            .map(|(_, by_tenant)| by_tenant.values().flatten().cloned().collect())
            .unwrap_or_default()
    }

    /// This tenant's placement entries (0, 1 whole-keyspace, or several ranged
    /// after a split). Index-backed (W0.3): an O(1) `HashMap` lookup instead of a
    /// full-catalog filter.
    pub async fn tenant_entries(&self, tenant: &str) -> Vec<PlacementEntry> {
        self.ensure_index_fresh().await;
        self.index
            .read()
            .await
            .as_ref()
            .and_then(|(_, by_tenant)| by_tenant.get(tenant).cloned())
            .unwrap_or_default()
    }

    /// Every durable move journal, including terminal records retained for audit.
    /// A journal-shaped record that cannot be decoded or whose digest/id does not
    /// validate fails closed: silently skipping it could strand an `Active` source
    /// immediately before cutover without a trustworthy recovery driver.
    pub async fn move_journals(&self) -> Result<Vec<PartitionMoveJournal>, String> {
        let core = {
            let state = self.state.read().await;
            state
                .registry
                .get(PLACEMENT_GRAPH)
                .map(|entry| entry.core.clone())
        };
        let mut journals = Vec::new();
        if let Some(core) = core {
            for (node_id, blob) in core.get_nodes() {
                if !is_move_journal_node_id(&node_id) {
                    continue;
                }
                let journal = eg_types::msgpack::decode_bounded::<PartitionMoveJournal>(
                    &blob,
                    eg_types::msgpack::MsgpackLimits::new(16 * 1024 * 1024, 131_072, 32),
                )
                .map_err(|_| "partition move journal is corrupt".to_string())?;
                if !journal.validate() || journal.node_id() != node_id {
                    return Err("partition move journal failed its integrity fence".to_string());
                }
                journals.push(journal);
            }
        }
        journals.sort_by(|left, right| left.move_id.cmp(&right.move_id));
        Ok(journals)
    }

    pub async fn move_journal(
        &self,
        move_id: &str,
    ) -> Result<Option<PartitionMoveJournal>, String> {
        if move_id.len() != 64 || !move_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(None);
        }
        let node_id = format!("{MOVE_JOURNAL_NODE_PREFIX}{move_id}");
        let core = {
            let state = self.state.read().await;
            state
                .registry
                .get(PLACEMENT_GRAPH)
                .map(|entry| entry.core.clone())
        };
        let Some(blob) = core.and_then(|core| core.get_node_properties(&node_id)) else {
            return Ok(None);
        };
        let journal = eg_types::msgpack::decode_bounded::<PartitionMoveJournal>(
            &blob,
            eg_types::msgpack::MsgpackLimits::new(16 * 1024 * 1024, 131_072, 32),
        )
        .map_err(|_| "partition move journal is corrupt".to_string())?;
        if !journal.validate() || journal.node_id() != node_id {
            return Err("partition move journal failed its integrity fence".to_string());
        }
        Ok(Some(journal))
    }

    /// Validate that every placement stuck in `Moving` has exactly one durable
    /// recovery driver and that no two active journals claim the same partition.
    /// Startup calls this before serving; an orphaned or contradictory state fails
    /// closed instead of being silently ignored.
    pub async fn validate_move_recovery_state(&self) -> Result<Vec<PartitionMoveJournal>, String> {
        let entries = self.all_entries().await;
        let active: Vec<PartitionMoveJournal> = self
            .move_journals()
            .await?
            .into_iter()
            .filter(|journal| !journal.stage.terminal())
            .collect();
        let mut claimed = std::collections::HashSet::new();
        for journal in &active {
            let key = (
                journal.key.tenant.as_str(),
                journal.key.range_start,
                journal.key.range_end,
            );
            if !claimed.insert(key) {
                return Err("multiple active move journals claim one partition".to_string());
            }
            let entry = entries
                .iter()
                .find(|entry| entry.key == journal.key)
                .ok_or_else(|| "active move journal has no placement entry".to_string())?;
            let active_source = entry.group == journal.source
                && entry.epoch == journal.original_epoch
                && entry.state == PartitionState::Active;
            let moving_source = entry.group == journal.source
                && entry.epoch == journal.original_epoch
                && entry.state
                    == (PartitionState::Moving {
                        target: journal.target,
                    });
            let active_target = entry.group == journal.target
                && entry.epoch > journal.original_epoch
                && entry.state == PartitionState::Active;
            // These are the only stage/placement combinations a crash can leave.
            // In particular, a transferring journal cannot legitimately be behind
            // an already-committed cutover: ReadyForCutover is persisted first.
            let consistent = match journal.stage {
                MoveStage::Planned => active_source || moving_source,
                MoveStage::Moving | MoveStage::Transferring => moving_source,
                MoveStage::ReadyForCutover => moving_source || active_target,
                MoveStage::CutoverCommitted => active_target,
                // An abort can race the irreversible fence. Recovery recognizes
                // the target route and rolls forward rather than attempting rollback.
                MoveStage::Aborting => active_source || moving_source || active_target,
                MoveStage::Completed | MoveStage::Aborted => false,
            };
            if !consistent {
                return Err("move journal and placement fence disagree".to_string());
            }
        }
        for entry in entries {
            if let PartitionState::Moving { target } = entry.state {
                let drivers = active
                    .iter()
                    .filter(|journal| {
                        journal.key == entry.key
                            && journal.source == entry.group
                            && journal.target == target
                            && journal.original_epoch == entry.epoch
                    })
                    .count();
                if drivers != 1 {
                    return Err(
                        "moving partition has no unique durable recovery journal".to_string()
                    );
                }
            }
        }
        Ok(active)
    }

    /// Resolve `(tenant, sub_key)` through the catalog's one authoritative policy.
    /// `unplaced_group` is supplied by the engine-owned [`super::multi::GroupRouter`]
    /// and is returned as an explicit unplaced answer; it is never delegated to a
    /// client. `Moving` partitions still resolve to their current source group.
    pub async fn route(
        &self,
        tenant: &str,
        sub_key: &str,
        unplaced_group: GroupId,
    ) -> PlacementRoute {
        let h = super::multi::fnv1a(sub_key);
        for e in self.tenant_entries(tenant).await {
            if e.key.contains(h) {
                return PlacementRoute::catalog(e.group, e.epoch);
            }
        }
        PlacementRoute::unplaced(unplaced_group)
    }

    /// Resolve a complete route vector from one catalog snapshot (CONCEPT:EG-KG.sharding.placement-catalog).
    /// Index-backed (W0.3): one [`Self::ensure_index_fresh`] call (amortized O(1)
    /// absent a placement change since the last build) then ONE read-lock
    /// acquisition for the whole batch — still exactly one pass over `keys`,
    /// avoiding the O(graphs × catalog) rescan a cross-graph read with many legs
    /// would otherwise pay, and no longer reconstructing a fresh local map on
    /// every call the way the pre-index version did.
    pub async fn route_many(&self, keys: &[(String, String, GroupId)]) -> Vec<PlacementRoute> {
        self.ensure_index_fresh().await;
        let guard = self.index.read().await;
        let by_tenant = guard.as_ref().map(|(_, by_tenant)| by_tenant);
        keys.iter()
            .map(|(tenant, sub_key, unplaced_group)| {
                let hash = super::multi::fnv1a(sub_key);
                by_tenant
                    .and_then(|by_tenant| by_tenant.get(tenant))
                    .and_then(|entries| entries.iter().find(|entry| entry.key.contains(hash)))
                    .map_or_else(
                        || PlacementRoute::unplaced(*unplaced_group),
                        |entry| PlacementRoute::catalog(entry.group, entry.epoch),
                    )
            })
            .collect()
    }

    /// The fenced-cutover redirect check (CONCEPT:EG-KG.sharding.placement-catalog): `None` when the
    /// caller's `client_epoch` is current (or there is no explicit placement — nothing
    /// to be stale against); `Some(route)` with the NEW group+epoch when the caller is
    /// behind, so it can be redirected instead of served against a partition that has
    /// already moved.
    pub async fn redirect_if_stale(
        &self,
        tenant: &str,
        sub_key: &str,
        client_epoch: u64,
    ) -> Option<PlacementRoute> {
        let route = self.route(tenant, sub_key, super::DEFAULT_GROUP).await;
        if route.placed && client_epoch < route.epoch {
            return Some(route);
        }
        None
    }

    fn entry_method(entry: &PlacementEntry) -> Method {
        Method::AddNode {
            node_id: entry.key.node_id(),
            properties_msgpack: rmp_serde::to_vec_named(entry)
                .expect("PlacementEntry always encodes"),
        }
    }

    fn remove_method(key: &PartitionKey) -> Method {
        Method::RemoveNode {
            node_id: key.node_id(),
        }
    }

    pub(crate) fn move_journal_method(journal: &PartitionMoveJournal) -> Result<Method, String> {
        if !journal.validate() {
            return Err("invalid partition move journal".to_string());
        }
        Ok(Method::AddNode {
            node_id: journal.node_id(),
            properties_msgpack: rmp_serde::to_vec_named(journal)
                .map_err(|_| "unable to encode partition move journal".to_string())?,
        })
    }

    /// Plan a whole-tenant assignment to `group` (CONCEPT:EG-KG.sharding.placement-catalog). Collapses any
    /// prior split/ranged entries for this tenant back to one whole-keyspace entry —
    /// also used by `merge` (assigning to the merge target is the same operation).
    /// Bumps the cluster-wide epoch (the authoritative group changed).
    pub async fn plan_assign(&self, tenant: &str, group: GroupId) -> PendingWrite<'_> {
        let guard = self.write_lock.lock().await;
        let epoch = self.next_epoch().await;
        let mut methods: Vec<Method> = self
            .tenant_entries(tenant)
            .await
            .iter()
            .map(|e| Self::remove_method(&e.key))
            .collect();
        let entry = PlacementEntry {
            key: PartitionKey::whole(tenant),
            group,
            epoch,
            state: PartitionState::Active,
        };
        methods.push(Self::entry_method(&entry));
        PendingWrite {
            _guard: guard,
            methods,
            epoch,
        }
    }

    /// Merge every one of `tenant`'s ranged partitions back onto a single group
    /// (CONCEPT:EG-KG.sharding.placement-catalog) — the inverse of `split`. Same operation as `assign`.
    pub async fn plan_merge(&self, tenant: &str, group: GroupId) -> PendingWrite<'_> {
        self.plan_assign(tenant, group).await
    }

    /// Plan splitting `tenant`'s partition covering `at` into `[start, at-1] → group_a`
    /// and `[at, end] → group_b` (CONCEPT:EG-KG.sharding.placement-catalog — lets one tenant span two
    /// groups). Splits the IMPLICIT whole range `[0, u64::MAX]` when the tenant has no
    /// explicit placement yet. Bumps the cluster-wide epoch once for the pair.
    pub async fn plan_split(
        &self,
        tenant: &str,
        at: u64,
        group_a: GroupId,
        group_b: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let epoch = self.next_epoch().await;
        let tenant_entries = self.tenant_entries(tenant).await;
        let covering = tenant_entries
            .iter()
            .find(|e| e.key.contains(at) && at > e.key.range_start);
        let (old_start, old_end) = match covering {
            Some(e) => (e.key.range_start, e.key.range_end),
            None if tenant_entries.is_empty() => (0u64, u64::MAX),
            None => {
                return Err(format!(
                    "split point {at} is not the interior of any of tenant '{tenant}''s existing partitions"
                ))
            }
        };
        if !(at > old_start && at <= old_end) {
            return Err(format!(
                "split point {at} is not inside range [{old_start}, {old_end}] for tenant '{tenant}'"
            ));
        }
        let mut methods = Vec::new();
        if let Some(e) = covering {
            methods.push(Self::remove_method(&e.key));
        }
        let entry_a = PlacementEntry {
            key: PartitionKey {
                tenant: tenant.to_string(),
                range_start: old_start,
                range_end: at - 1,
            },
            group: group_a,
            epoch,
            state: PartitionState::Active,
        };
        let entry_b = PlacementEntry {
            key: PartitionKey {
                tenant: tenant.to_string(),
                range_start: at,
                range_end: old_end,
            },
            group: group_b,
            epoch,
            state: PartitionState::Active,
        };
        methods.push(Self::entry_method(&entry_a));
        methods.push(Self::entry_method(&entry_b));
        Ok(PendingWrite {
            _guard: guard,
            methods,
            epoch,
        })
    }

    /// Plan marking the partition `(tenant, range)` [`PartitionState::Moving`] to
    /// `target` (CONCEPT:EG-KG.sharding.placement-catalog — online-move step 1). Does NOT bump the epoch:
    /// the authoritative group is unchanged until [`plan_fence_cutover`](Self::plan_fence_cutover)
    /// — `route` keeps answering with the SOURCE group while the target catches up.
    pub(crate) async fn plan_start_move(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let entries = self.tenant_entries(tenant).await;
        let entry = entries
            .into_iter()
            .find(|e| e.key.range_start == range.0 && e.key.range_end == range.1)
            .ok_or_else(|| {
                format!("no placement entry for tenant '{tenant}' range {range:?} — assign/split it first")
            })?;
        match entry.state {
            PartitionState::Active if entry.group != target => {}
            PartitionState::Moving {
                target: active_target,
            } if active_target == target => {
                return Ok(PendingWrite {
                    _guard: guard,
                    methods: Vec::new(),
                    epoch: entry.epoch,
                });
            }
            PartitionState::Moving { .. } => {
                return Err("partition already has a move to another target".to_string())
            }
            PartitionState::Active => {
                return Err("partition move target already owns the partition".to_string())
            }
        }
        let moving = PlacementEntry {
            state: PartitionState::Moving { target },
            ..entry
        };
        let epoch = moving.epoch;
        Ok(PendingWrite {
            _guard: guard,
            methods: vec![Self::entry_method(&moving)],
            epoch,
        })
    }

    /// Plan the fenced cutover of `(tenant, range)` to `target` (CONCEPT:EG-KG.sharding.placement-catalog —
    /// online-move step 3): bumps the cluster-wide epoch AND flips the partition's
    /// authoritative group, atomically (one control-graph write). After this commits,
    /// [`redirect_if_stale`](Self::redirect_if_stale) answers any caller still on the
    /// pre-cutover epoch with a redirect to `(target, new_epoch)`.
    pub(crate) async fn plan_fence_cutover(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let epoch = self.next_epoch().await;
        let entry = self
            .tenant_entries(tenant)
            .await
            .into_iter()
            .find(|e| e.key.range_start == range.0 && e.key.range_end == range.1)
            .ok_or_else(|| {
                format!("no placement entry for tenant '{tenant}' range {range:?} to cut over")
            })?;
        if entry.state != (PartitionState::Moving { target }) {
            return Err("partition is not moving to the requested target".to_string());
        }
        let cut = PlacementEntry {
            key: entry.key,
            group: target,
            epoch,
            state: PartitionState::Active,
        };
        Ok(PendingWrite {
            _guard: guard,
            methods: vec![Self::entry_method(&cut)],
            epoch,
        })
    }

    /// Restore a pre-cutover `Moving` entry to its original active route without
    /// changing the epoch.  Once cutover committed, rollback is rejected and recovery
    /// must roll forward.
    pub(crate) async fn plan_abort_move(
        &self,
        tenant: &str,
        range: (u64, u64),
        source: GroupId,
        target: GroupId,
        original_epoch: u64,
    ) -> Result<PendingWrite<'_>, String> {
        let guard = self.write_lock.lock().await;
        let entry = self
            .tenant_entries(tenant)
            .await
            .into_iter()
            .find(|entry| entry.key.range_start == range.0 && entry.key.range_end == range.1)
            .ok_or_else(|| "partition move no longer has a placement entry".to_string())?;
        if entry.group != source
            || entry.epoch != original_epoch
            || entry.state != (PartitionState::Moving { target })
        {
            return Err("partition move passed its rollback fence".to_string());
        }
        let active = PlacementEntry {
            state: PartitionState::Active,
            ..entry
        };
        Ok(PendingWrite {
            _guard: guard,
            methods: vec![Self::entry_method(&active)],
            epoch: active.epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_whole_contains_everything() {
        let k = PartitionKey::whole("acme");
        assert!(k.contains(0));
        assert!(k.contains(u64::MAX));
        assert!(k.contains(123456789));
    }

    #[test]
    fn split_tenant_key_parses_prefix() {
        assert_eq!(split_tenant_key("acme:ws1"), ("acme", "ws1"));
        assert_eq!(split_tenant_key("acme:ws:nested"), ("acme", "ws:nested"));
        assert_eq!(split_tenant_key("solo"), ("solo", "solo"));
        assert_eq!(split_tenant_key(":leading"), (":leading", ":leading"));
    }

    #[test]
    fn node_id_is_stable_and_unique_per_range() {
        let a = PartitionKey {
            tenant: "acme".into(),
            range_start: 0,
            range_end: 10,
        };
        let b = PartitionKey {
            tenant: "acme".into(),
            range_start: 11,
            range_end: 20,
        };
        assert_ne!(a.node_id(), b.node_id());
        assert_eq!(a.node_id(), a.node_id());
    }

    #[test]
    fn move_journal_is_deterministic_sorted_and_integrity_checked() {
        let entry = PlacementEntry {
            key: PartitionKey::whole("tenant"),
            group: 7,
            epoch: 11,
            state: PartitionState::Active,
        };
        let journal = PartitionMoveJournal::new(
            &entry,
            9,
            vec!["z".to_string(), "a".to_string(), "a".to_string()],
        )
        .unwrap();
        assert_eq!(journal.graphs, vec!["a", "z"]);
        assert!(journal.validate());

        let same =
            PartitionMoveJournal::new(&entry, 9, vec!["a".to_string(), "z".to_string()]).unwrap();
        assert_eq!(journal.move_id, same.move_id);

        let mut tampered = journal;
        tampered.target = 10;
        assert!(!tampered.validate());
    }

    #[test]
    fn move_journal_transitions_are_monotonic_and_cutover_requires_all_graphs() {
        let entry = PlacementEntry {
            key: PartitionKey::whole("tenant"),
            group: 7,
            epoch: 11,
            state: PartitionState::Active,
        };
        let planned =
            PartitionMoveJournal::new(&entry, 9, vec!["a".to_string(), "b".to_string()]).unwrap();
        let mut transferring = planned.clone();
        transferring.stage = MoveStage::Transferring;
        transferring.completed_graphs.push("a".to_string());
        assert!(planned.permits_successor(&transferring));

        let mut ready_too_early = transferring.clone();
        ready_too_early.stage = MoveStage::ReadyForCutover;
        assert!(!ready_too_early.validate());

        let mut aborting = transferring.clone();
        aborting.stage = MoveStage::Aborting;
        assert!(transferring.permits_successor(&aborting));
        assert!(
            !aborting.permits_successor(&transferring),
            "a stale driver cannot regress an abort intent"
        );
    }
}
