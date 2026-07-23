//! Online resharding + cold-tenant hibernation (CONCEPT:EG-KG.storage.100m-tenant — the 100M-tenant
//! lever).
//!
//! Two elastic-tenant operations on a live [`MultiRaft`] cluster, both feature-gated
//! to `raft`/cluster so a default / `pi` / `full` build links neither (and the Pi
//! contract holds):
//!
//! ## Online resharding — move a graph A→B with NO downtime
//!
//! A graph belongs to exactly one Raft [`GroupId`] via the [`GroupRouter`]
//! (CONCEPT:EG-KG.sharding.raft-resharding). Resharding re-points that ownership from a source group to a
//! target group while the cluster keeps serving. The KEY simplification — and why
//! this is safe — is the M2 architecture: **every group applies into ONE shared
//! registry + ONE shared authoritative shard** (`store::EgStore` holds the shared
//! [`AppCtx`]; the durable rows are keyed by GRAPH NAME, not by group). So a graph's
//! durable + in-memory state already lives in a place BOTH groups reach; "moving" it
//! is therefore not a bulk data copy but a **quiesce → durable-presence gate → re-point
//! → resume** of which consensus group replicates the graph's FUTURE writes.
//!
//! The steps [`reshard_graph`] runs, in order:
//!   1. **Quiesce.** Take the graph's per-tenant migration lock so no NEW reshard /
//!      hibernate races, and snapshot the source group is the current owner.
//!   2. **Durable-presence gate.** Confirm the graph's committed rows exist in the
//!      shared authoritative store the target group already reads.
//!   3. **Re-point the router.** `router.assign(graph, target)` — every subsequent
//!      write for the graph now routes through the target group's `client_write`.
//!      The target group must be running on this node (a precondition / created via
//!      `ensure_group`).
//!   4. **Resume.** Drop the migration lock. Reads never stopped (they hit the shared
//!      registry); writes now land on the target group. No data moved off disk, so
//!      there is no window where the graph is unreadable — zero downtime.
//!
//! The proof ([`super::reshard_harness`]) writes into a graph on group A, reshareds it
//! A→B, then asserts (a) every pre-reshard node is still present + readable and (b) a
//! post-reshard write routes through B and lands — data preserved, correctness intact.
//!
//! ## Cold-tenant hibernation — evict a graph's RAM, rehydrate on access
//!
//! A COLD tenant wastes RAM holding a `GraphCore` that is never read. [`hibernate_graph`]
//! reuses the read-through/eviction machinery (CONCEPT:EG-KG.storage.read-through-seam-exercised) at WHOLE-GRAPH
//! granularity: confirm every resident node in durable authority, then
//! `GraphCore::hibernate()` drops its in-RAM topology /
//! properties / vectors. The durable redb rows remain. [`rehydrate_graph`] reads the
//! graph's durable dump back and rebuilds the core (the same path `load_all` uses) on
//! the next access. Proof: a hibernated graph rehydrates with every node intact.

use std::sync::Arc;

use super::multi::MultiRaft;
use super::placement::{MoveStage, PartitionMoveJournal, PartitionState, PlacementEntry};
use super::GroupId;
use crate::server::persistence::redb_backend::rehydrate_core_from_dump;
use crate::server::persistence::PersistenceBackend;

/// The outcome of a reshard, for observability + the proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReshardReport {
    pub graph: String,
    pub from_group: GroupId,
    pub to_group: GroupId,
    /// Nodes confirmed in durable authority at the transfer barrier.
    pub nodes_transferred: usize,
}

/// The outcome of an online placement-catalog move (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1) —
/// see [`TenantManager::move_partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementMoveReport {
    pub tenant: String,
    pub range: (u64, u64),
    pub target: GroupId,
    /// The routing epoch the catalog settled on after the fenced cutover.
    pub epoch: u64,
    /// One [`ReshardReport`] per graph the partition covered, in canonical graph-id
    /// order (the same order retained by the durable journal).
    pub graphs: Vec<ReshardReport>,
}

/// Elastic-tenant operations on a live [`MultiRaft`] cluster (CONCEPT:EG-KG.storage.100m-tenant).
/// Holds the manager + the shared durable backend; every op runs under the manager's
/// per-tenant migration guard so a reshard and a hibernate of the same graph cannot
/// race.
pub struct TenantManager {
    multi: Arc<MultiRaft>,
    backend: Arc<dyn PersistenceBackend>,
}

impl TenantManager {
    pub fn new(multi: Arc<MultiRaft>, backend: Arc<dyn PersistenceBackend>) -> Self {
        Self { multi, backend }
    }

    /// Online-reshard `graph_name` from its current owning group to `to_group`
    /// WITHOUT downtime (CONCEPT:EG-KG.storage.100m-tenant). See the module docs for the quiesce →
    /// durable-presence gate → re-point → resume protocol.
    ///
    /// Preconditions: the graph exists; `to_group` is running on this node (use
    /// [`MultiRaft::create_group`] / a higher-level `ensure_group` first). The router
    /// is re-pointed only AFTER the durability barrier, so a crash mid-reshard leaves
    /// the graph owned by its ORIGINAL group with all data durable — re-runnable.
    pub(crate) async fn reshard_graph(
        &self,
        graph_name: &str,
        to_group: GroupId,
    ) -> Result<ReshardReport, String> {
        // ── 1. Quiesce: take the per-tenant migration lock ──────────────────────
        let _guard = self.multi.tenant_lock(graph_name).await;

        let router = self.multi.router();
        // Resolve the CURRENT owner the same way the dispatch/routing seam does: the
        // placement catalog first (an explicit virtual-partition placement for the
        // graph's tenant), falling back to the bare hash-ring `GroupRouter` only when
        // no catalog entry exists. Reading `router.group_of` directly here would miss
        // a catalog-only tenant (it always answers `DEFAULT_GROUP` for a graph the
        // router itself was never told to `assign`), so `move_partition` would report
        // the wrong `from_group` and never move the graph's actual owning group's data.
        let from_route = self.multi.route_graph(graph_name).await;
        let from_group = from_route.group;
        if from_group == to_group {
            return Err(format!(
                "graph '{graph_name}' is already owned by group {to_group}"
            ));
        }
        // The target group must be live on this node to take ownership of writes.
        if self.multi.group(to_group).await.is_none() {
            return Err(format!(
                "reshard target group {to_group} is not running on this node"
            ));
        }

        // ── 2. Durable-presence gate ────────────────────────────────────────────
        // The shared store is already authoritative. Confirm every resident node is
        // present there before moving its consensus ownership.
        let nodes_transferred = self.verify_graph_durable(graph_name).await?;

        // ── 3. Re-point the router ──────────────────────────────────────────────
        router.assign(graph_name, to_group);

        // ── 4. Resume (guard drops here, releasing the migration lock) ──────────
        tracing::info!(
            from_group,
            to_group,
            nodes_transferred,
            "reshard graph passed durable transfer barrier"
        );
        Ok(ReshardReport {
            graph: graph_name.to_string(),
            from_group,
            to_group,
            nodes_transferred,
        })
    }

    /// Online-move the virtual partition `(tenant, range)` to `target`
    /// (CONCEPT:EG-KG.sharding.placement-catalog, DIST-P2-1) — the full snapshot → CDC-catch-up →
    /// fenced-cutover state machine, reusing [`Self::reshard_graph`] (already-proven
    /// quiesce → durability-barrier → re-point → resume) as the per-graph data move:
    ///
    /// 1. **Mark [`Moving`](super::placement::PartitionState::Moving)** — `route` keeps
    ///    answering with the SOURCE group; nothing is redirected yet.
    /// 2. **Snapshot + catch-up** — derive every graph covered by the partition from
    ///    the engine catalog, then `reshard_graph` each one to `target`. Because every group shares ONE
    ///    shard/registry, the durability barrier IS the transfer — there is no
    ///    separate bulk-copy phase to run.
    /// 3. **Fenced cutover** — bump the epoch and flip the partition's authoritative
    ///    group to `target` in one commit. From this instant a caller presenting the
    ///    pre-cutover epoch gets redirected (`PlacementCatalog::redirect_if_stale`)
    ///    rather than served against data that has moved.
    ///
    /// The complete immutable graph inventory and every completed graph are journaled
    /// in the replicated placement graph.  A crash therefore needs no caller-supplied
    /// remainder: [`Self::reconcile_moves`] resumes the exact durable plan.  Before
    /// cutover, [`Self::abort_move`] restores the source route; after cutover, recovery
    /// is roll-forward only.
    pub async fn move_partition(
        &self,
        tenant: &str,
        range: (u64, u64),
        target: GroupId,
    ) -> Result<PlacementMoveReport, String> {
        let _move_guard = self.partition_move_guard(tenant, range).await;
        self.multi
            .require_local_group_leader(super::DEFAULT_GROUP)
            .await?;
        let requested_graphs = self.partition_graphs(tenant, range).await?;
        let existing = self
            .multi
            .placement()
            .validate_move_recovery_state()
            .await?
            .into_iter()
            .find(|journal| {
                journal.key.tenant == tenant
                    && journal.key.range_start == range.0
                    && journal.key.range_end == range.1
                    && !journal.stage.terminal()
            });
        let journal = if let Some(journal) = existing {
            if journal.target != target || journal.graphs != requested_graphs {
                return Err("partition already has a different active move plan".to_string());
            }
            journal
        } else {
            let entry = self.partition_entry(tenant, range).await?;
            let journal = PartitionMoveJournal::new(&entry, target, requested_graphs)?;
            self.multi.persist_move_journal(&journal).await?;
            journal
        };
        self.drive_move(journal).await
    }

    /// Resume every non-terminal journal found at startup.  The list is sorted by
    /// opaque move id, making recovery deterministic across replicas.  Any unsafe or
    /// unverifiable move fails closed and prevents serving with ambiguous placement.
    pub async fn reconcile_moves(&self) -> Result<Vec<PlacementMoveReport>, String> {
        self.multi
            .require_local_group_leader(super::DEFAULT_GROUP)
            .await?;
        let journals = self
            .multi
            .placement()
            .validate_move_recovery_state()
            .await?;
        let mut completed = Vec::new();
        for listed in journals {
            let _move_guard = self
                .partition_move_guard(
                    &listed.key.tenant,
                    (listed.key.range_start, listed.key.range_end),
                )
                .await;
            let Some(journal) = self.multi.placement().move_journal(&listed.move_id).await? else {
                return Err("partition move journal disappeared during recovery".to_string());
            };
            if journal.stage.terminal() {
                continue;
            }
            if journal.stage == MoveStage::Aborting {
                if self.move_is_post_cutover(&journal).await? {
                    completed.push(self.drive_move(journal).await?);
                } else {
                    self.abort_move_unlocked(journal).await?;
                }
            } else {
                completed.push(self.drive_move(journal).await?);
            }
        }
        Ok(completed)
    }

    /// Abort a move only before its cutover fence.  All graph router overrides are
    /// restored to the journaled source, including a graph moved immediately before a
    /// crash whose progress update did not commit.
    pub async fn abort_move(&self, move_id: &str) -> Result<(), String> {
        let listed = self
            .multi
            .placement()
            .move_journal(move_id)
            .await?
            .ok_or_else(|| "partition move journal not found".to_string())?;
        let _move_guard = self
            .partition_move_guard(
                &listed.key.tenant,
                (listed.key.range_start, listed.key.range_end),
            )
            .await;
        self.multi
            .require_local_group_leader(super::DEFAULT_GROUP)
            .await?;
        let journal = self
            .multi
            .placement()
            .move_journal(move_id)
            .await?
            .ok_or_else(|| "partition move journal not found".to_string())?;
        self.abort_move_unlocked(journal).await
    }

    async fn abort_move_unlocked(&self, mut journal: PartitionMoveJournal) -> Result<(), String> {
        if journal.stage.terminal() {
            return Err("partition move is already terminal".to_string());
        }
        if self.move_is_post_cutover(&journal).await? {
            if journal.stage == MoveStage::Aborting {
                self.drive_move(journal).await?;
            }
            return Err("partition move passed its rollback fence".to_string());
        }
        if journal.stage != MoveStage::Aborting {
            journal.stage = MoveStage::Aborting;
            self.multi.persist_move_journal(&journal).await?;
        }
        let entry = self
            .partition_entry(
                &journal.key.tenant,
                (journal.key.range_start, journal.key.range_end),
            )
            .await?;
        match entry.state {
            PartitionState::Moving { target }
                if entry.group == journal.source
                    && target == journal.target
                    && entry.epoch == journal.original_epoch =>
            {
                self.multi
                    .placement_abort_move(
                        &journal.key.tenant,
                        (journal.key.range_start, journal.key.range_end),
                        journal.source,
                        journal.target,
                        journal.original_epoch,
                    )
                    .await?;
            }
            PartitionState::Active
                if entry.group == journal.source && entry.epoch == journal.original_epoch => {}
            _ if entry.group == journal.target
                && entry.epoch > journal.original_epoch
                && entry.state == PartitionState::Active =>
            {
                // The cutover won a race with the abort intent. The epoch is the
                // irreversible fence, so make the journal truthful and finish
                // forward; never strand `Aborting` behind the target route.
                journal.stage = MoveStage::CutoverCommitted;
                self.multi.persist_move_journal(&journal).await?;
                self.drive_move(journal).await?;
                return Err(
                    "partition move passed its rollback fence and was reconciled forward"
                        .to_string(),
                );
            }
            _ => return Err("partition move placement no longer matches its journal".to_string()),
        }
        for graph in &journal.graphs {
            self.multi.router().assign(graph, journal.source);
        }
        journal.stage = MoveStage::Aborted;
        self.multi.persist_move_journal(&journal).await
    }

    async fn drive_move(
        &self,
        mut journal: PartitionMoveJournal,
    ) -> Result<PlacementMoveReport, String> {
        if self.multi.group(journal.target).await.is_none() {
            return Err("partition move target group is unavailable".to_string());
        }
        let range = (journal.key.range_start, journal.key.range_end);
        let mut entry = self.partition_entry(&journal.key.tenant, range).await?;

        // A crash after the cutover commit but before its journal update is resolved
        // strictly forward; the epoch itself is the irreversible fence.
        if matches!(entry.state, PartitionState::Active)
            && entry.group == journal.target
            && entry.epoch > journal.original_epoch
        {
            journal.stage = MoveStage::CutoverCommitted;
            self.multi.persist_move_journal(&journal).await?;
        } else {
            if journal.stage == MoveStage::Aborting {
                return Err("aborting partition move cannot be driven forward".to_string());
            }
            if matches!(entry.state, PartitionState::Active)
                && entry.group == journal.source
                && entry.epoch == journal.original_epoch
            {
                self.multi
                    .placement_start_move(&journal.key.tenant, range, journal.target)
                    .await?;
                journal.stage = MoveStage::Moving;
                self.multi.persist_move_journal(&journal).await?;
                entry = self.partition_entry(&journal.key.tenant, range).await?;
            }
            if entry.group != journal.source
                || entry.epoch != journal.original_epoch
                || entry.state
                    != (PartitionState::Moving {
                        target: journal.target,
                    })
            {
                return Err("partition move placement no longer matches its journal".to_string());
            }

            journal.stage = MoveStage::Transferring;
            self.multi.persist_move_journal(&journal).await?;
            for graph in journal.graphs.clone() {
                if journal.completed_graphs.binary_search(&graph).is_err() {
                    self.reshard_graph(&graph, journal.target).await?;
                    journal.completed_graphs.push(graph);
                    journal.completed_graphs.sort();
                    journal.completed_graphs.dedup();
                    self.multi.persist_move_journal(&journal).await?;
                }
            }
            journal.stage = MoveStage::ReadyForCutover;
            self.multi.persist_move_journal(&journal).await?;

            // Re-verify from authoritative storage even when every graph was marked
            // complete before a prior crash.  Journal progress alone never authorizes
            // cutover.
            for graph in &journal.graphs {
                self.verify_graph_durable(graph).await?;
            }
            // Re-read the durable intent immediately before the irreversible epoch
            // fence. An abort that won the journal race stops this driver here.
            journal = self
                .multi
                .placement()
                .move_journal(&journal.move_id)
                .await?
                .ok_or_else(|| "partition move journal disappeared before cutover".to_string())?;
            if journal.stage != MoveStage::ReadyForCutover {
                return Err("partition move intent changed before cutover".to_string());
            }
            self.multi
                .require_local_group_leader(super::DEFAULT_GROUP)
                .await?;
            let epoch = self
                .multi
                .placement_fence_cutover(&journal.key.tenant, range, journal.target)
                .await?;
            entry = PlacementEntry {
                key: journal.key.clone(),
                group: journal.target,
                epoch,
                state: PartitionState::Active,
            };
            journal.stage = MoveStage::CutoverCommitted;
            self.multi.persist_move_journal(&journal).await?;
        }

        let mut reports = Vec::with_capacity(journal.graphs.len());
        for graph in &journal.graphs {
            self.multi.router().assign(graph, journal.target);
            reports.push(ReshardReport {
                graph: graph.clone(),
                from_group: journal.source,
                to_group: journal.target,
                nodes_transferred: self.verify_graph_durable(graph).await?,
            });
        }
        journal.stage = MoveStage::Completed;
        self.multi.persist_move_journal(&journal).await?;
        Ok(PlacementMoveReport {
            tenant: journal.key.tenant,
            range,
            target: journal.target,
            epoch: entry.epoch,
            graphs: reports,
        })
    }

    async fn move_is_post_cutover(&self, journal: &PartitionMoveJournal) -> Result<bool, String> {
        let entry = self
            .partition_entry(
                &journal.key.tenant,
                (journal.key.range_start, journal.key.range_end),
            )
            .await?;
        Ok(entry.group == journal.target
            && entry.epoch > journal.original_epoch
            && entry.state == PartitionState::Active)
    }

    async fn partition_move_guard(
        &self,
        tenant: &str,
        range: (u64, u64),
    ) -> tokio::sync::OwnedMutexGuard<()> {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph/partition-move-lock/v1\0");
        digest.update(tenant.as_bytes());
        digest.update(range.0.to_be_bytes());
        digest.update(range.1.to_be_bytes());
        self.multi
            .tenant_lock(&format!("move-lock:{}", hex::encode(digest.finalize())))
            .await
    }

    async fn partition_entry(
        &self,
        tenant: &str,
        range: (u64, u64),
    ) -> Result<PlacementEntry, String> {
        self.multi
            .placement()
            .tenant_entries(tenant)
            .await
            .into_iter()
            .find(|entry| entry.key.range_start == range.0 && entry.key.range_end == range.1)
            .ok_or_else(|| "partition placement entry not found".to_string())
    }

    /// Derive the immutable move inventory from the engine's graph catalog.  The
    /// caller supplies only the partition key and target; it cannot omit graphs or
    /// provide a hand-maintained remainder.
    async fn partition_graphs(
        &self,
        tenant: &str,
        range: (u64, u64),
    ) -> Result<Vec<String>, String> {
        let key = super::placement::PartitionKey {
            tenant: tenant.to_string(),
            range_start: range.0,
            range_end: range.1,
        };
        if key.range_start > key.range_end {
            return Err("partition move range is invalid".to_string());
        }
        let state = self.multi.app_state();
        let state = state.read().await;
        let mut graphs: Vec<String> = state
            .registry
            .list()
            .into_iter()
            .filter_map(|(graph_name, _)| {
                let (graph_tenant, sub_key) = super::placement::split_tenant_key(&graph_name);
                (graph_tenant == tenant && key.contains(super::multi::fnv1a(sub_key)))
                    .then_some(graph_name)
            })
            .collect();
        graphs.sort();
        graphs.dedup();
        if graphs.len() > super::placement::MAX_PARTITION_MOVE_GRAPHS {
            return Err("partition move graph inventory exceeds the limit".to_string());
        }
        Ok(graphs)
    }

    async fn verify_graph_durable(&self, graph_name: &str) -> Result<usize, String> {
        let graph_fname = crate::persist::sanitize(graph_name);
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || {
            let mut cursor = None;
            let mut node_count = 0usize;
            let mut incarnation = None;
            let mut version = None;
            loop {
                let page = backend
                    .read_graph_material_page_blocking(&graph_fname, cursor, 4_096)?
                    .ok_or_else(|| {
                        "partition move graph is absent from durable authority".to_string()
                    })?;
                let page_incarnation = page.incarnation_id.ok_or_else(|| {
                    "partition move graph has no durable incarnation fence".to_string()
                })?;
                let page_version = page.source_snapshot_version.ok_or_else(|| {
                    "partition move graph has no durable version fence".to_string()
                })?;
                if incarnation
                    .as_ref()
                    .is_some_and(|expected| expected != &page_incarnation)
                    || version.is_some_and(|expected| expected != page_version)
                {
                    return Err(
                        "partition move graph changed during durable verification".to_string()
                    );
                }
                incarnation = Some(page_incarnation);
                version = Some(page_version);
                node_count = node_count
                    .checked_add(page.nodes.len())
                    .ok_or_else(|| "partition move graph node count overflow".to_string())?;
                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => return Ok(node_count),
                }
            }
        })
        .await
        .map_err(|_| "partition move durable verification task failed".to_string())?
    }

    /// Hibernate a COLD graph after confirming its resident rows are durable, then drop its in-RAM state
    /// (CONCEPT:EG-KG.storage.100m-tenant). The durable redb rows remain; [`rehydrate_graph`] rebuilds
    /// the core on next access. Returns the node count freed. Idempotent — hibernating
    /// an already-hibernated (empty-in-RAM) graph frees 0 and stays durable.
    pub async fn hibernate_graph(&self, graph_name: &str) -> Result<usize, String> {
        let _guard = self.multi.tenant_lock(graph_name).await;

        let state = self.multi.app_state();
        let core = {
            let s = state.read().await;
            s.registry.get(graph_name).map(|e| e.core.clone())
        };
        let core = core.ok_or_else(|| format!("graph '{graph_name}' not found"))?;
        let freed = crate::server::persistence::cold_offload::offload_graph_core(
            &core,
            &self.backend,
            graph_name,
        )
        .ok_or_else(|| format!("graph '{graph_name}' is not fully present in durable authority"))?;
        tracing::info!(freed, "graph hibernated after durable-presence gate");
        Ok(freed)
    }

    /// Rehydrate a hibernated graph from its durable dump (CONCEPT:EG-KG.storage.100m-tenant). Reads the
    /// graph's redb rows back and rebuilds its `GraphCore` (the same path `load_all`
    /// uses). Idempotent — a re-rehydrate clears + reloads. Returns the node count
    /// restored. A graph with no durable rows (genuinely absent) restores 0.
    pub async fn rehydrate_graph(&self, graph_name: &str) -> Result<usize, String> {
        let _guard = self.multi.tenant_lock(graph_name).await;

        let fname = crate::persist::sanitize(graph_name);
        let dump = match self.read_dump(&fname)? {
            Some(d) => d,
            None => return Ok(0),
        };
        let n = dump.nodes.len();

        let state = self.multi.app_state();
        let core = {
            let mut s = state.write().await;
            // The graph entry must exist in the registry (hibernation keeps it — it
            // only clears the core). If a fresh process is rehydrating a graph it
            // never created, recreate the registry entry from the durable identity.
            if !s.registry.exists(graph_name) {
                let _ = s.registry.create_graph(&dump.name, dump.graph_type, None);
            }
            s.registry.get(graph_name).map(|e| e.core.clone())
        };
        let core = core.ok_or_else(|| format!("graph '{graph_name}' not found after recreate"))?;
        rehydrate_core_from_dump(&core, &dump);
        tracing::info!(n, "graph rehydrated from durable authority");
        Ok(n)
    }

    /// Read ONE graph's durable dump via the redb backend (CONCEPT:EG-KG.storage.100m-tenant). Errors if
    /// the backend is not the redb tier (resharding/hibernation require it).
    fn read_dump(&self, graph_fname: &str) -> Result<Option<crate::redb_store::GraphDump>, String> {
        let redb = self.backend.as_redb().ok_or_else(|| {
            "resharding/hibernation require the redb persistence backend".to_string()
        })?;
        redb.read_graph_dump_blocking(graph_fname)
    }
}
