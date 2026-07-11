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
//! registry + ONE shared `graph.redb`** (`store::EgStore` holds the shared
//! [`AppCtx`]; the durable rows are keyed by GRAPH NAME, not by group). So a graph's
//! durable + in-memory state already lives in a place BOTH groups reach; "moving" it
//! is therefore not a bulk data copy but a **quiesce → durability-barrier → re-point
//! → resume** of which consensus group replicates the graph's FUTURE writes.
//!
//! The steps [`reshard_graph`] runs, in order:
//!   1. **Quiesce.** Take the graph's per-tenant migration lock so no NEW reshard /
//!      hibernate races, and snapshot the source group is the current owner.
//!   2. **Durability barrier (snapshot + transfer).** Force the graph's accumulated
//!      state durable to redb via a checkpoint of THIS graph (the dump that the
//!      target group will own). Because the durable store is shared and keyed by
//!      graph name, this dump IS the transfer — the target group reads the same rows.
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
//! granularity: force the graph durable (checkpoint it — the same durability gate
//! per-node eviction uses), then `GraphCore::hibernate()` drops its in-RAM topology /
//! properties / vectors. The durable redb rows remain. [`rehydrate_graph`] reads the
//! graph's durable dump back and rebuilds the core (the same path `load_all` uses) on
//! the next access. Proof: a hibernated graph rehydrates with every node intact.

use std::sync::Arc;

use super::multi::MultiRaft;
use super::GroupId;
use crate::server::persistence::redb_backend::rehydrate_core_from_dump;
use crate::server::persistence::PersistenceBackend;

/// The outcome of a reshard, for observability + the proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReshardReport {
    pub graph: String,
    pub from_group: GroupId,
    pub to_group: GroupId,
    /// Nodes durably checkpointed at the transfer barrier.
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
    /// One [`ReshardReport`] per graph the partition covered, in the order moved.
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
    /// durability-barrier → re-point → resume protocol.
    ///
    /// Preconditions: the graph exists; `to_group` is running on this node (use
    /// [`MultiRaft::create_group`] / a higher-level `ensure_group` first). The router
    /// is re-pointed only AFTER the durability barrier, so a crash mid-reshard leaves
    /// the graph owned by its ORIGINAL group with all data durable — re-runnable.
    pub async fn reshard_graph(
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
        let (from_group, _from_epoch) = self.multi.route_graph(graph_name).await;
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

        // ── 2. Durability barrier (snapshot + transfer) ─────────────────────────
        // Force THIS graph's accumulated state durable to redb. Because the durable
        // store is shared and keyed by graph name, this checkpoint IS the transfer:
        // the target group reads the same rows. We checkpoint the whole registry's
        // durable tier (the only checkpoint primitive); the graph's rows are flushed
        // within it. Then read the graph's dump back to confirm + count.
        let state = self.multi.app_state();
        self.backend.checkpoint_all(&state).await?;
        let fname = crate::persist::sanitize(graph_name);
        let nodes_transferred = match self.read_dump(&fname)? {
            Some(dump) => dump.nodes.len(),
            None => {
                // No durable rows for an empty/new graph is fine — the re-point still
                // transfers OWNERSHIP of future writes.
                0
            }
        };

        // ── 3. Re-point the router ──────────────────────────────────────────────
        router.assign(graph_name, to_group);

        // ── 4. Resume (guard drops here, releasing the migration lock) ──────────
        tracing::info!(
            "reshard: graph '{graph_name}' {from_group}→{to_group} ({nodes_transferred} nodes durable at barrier)"
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
    /// 2. **Snapshot + catch-up** — `reshard_graph` each of `graphs` (every graph this
    ///    partition currently covers) to `target`. Because every group shares ONE
    ///    `graph.redb`/registry, the durability barrier IS the transfer — there is no
    ///    separate bulk-copy phase to run.
    /// 3. **Fenced cutover** — bump the epoch and flip the partition's authoritative
    ///    group to `target` in one commit. From this instant a caller presenting the
    ///    pre-cutover epoch gets redirected (`PlacementCatalog::redirect_if_stale`)
    ///    rather than served against data that has moved.
    ///
    /// Preconditions: `(tenant, range)` must already have an explicit placement entry
    /// (`assign`/`split` it first) and `target` must be running on this node (same
    /// precondition `reshard_graph` enforces per graph). A failure partway through
    /// step 2 leaves the partition `Moving` and every already-reshareded graph durably
    /// on `target` — re-running `move_partition` with the SAME `graphs` list is safe
    /// (`reshard_graph` is idempotent on an already-owned graph only insofar as it
    /// errors "already owned"; callers recovering from a partial failure should pass
    /// only the NOT-yet-moved remainder and then fence the cutover).
    pub async fn move_partition(
        &self,
        tenant: &str,
        range: (u64, u64),
        graphs: &[String],
        target: GroupId,
    ) -> Result<PlacementMoveReport, String> {
        self.multi
            .placement_start_move(tenant, range, target)
            .await?;

        let mut per_graph = Vec::with_capacity(graphs.len());
        for graph in graphs {
            per_graph.push(self.reshard_graph(graph, target).await?);
        }

        let epoch = self
            .multi
            .placement_fence_cutover(tenant, range, target)
            .await?;
        Ok(PlacementMoveReport {
            tenant: tenant.to_string(),
            range,
            target,
            epoch,
            graphs: per_graph,
        })
    }

    /// Hibernate a COLD graph: force it durable, then drop its in-RAM state
    /// (CONCEPT:EG-KG.storage.100m-tenant). The durable redb rows remain; [`rehydrate_graph`] rebuilds
    /// the core on next access. Returns the node count freed. Idempotent — hibernating
    /// an already-hibernated (empty-in-RAM) graph frees 0 and stays durable.
    pub async fn hibernate_graph(&self, graph_name: &str) -> Result<usize, String> {
        let _guard = self.multi.tenant_lock(graph_name).await;

        // Durability gate: checkpoint so the graph's full state is on disk BEFORE we
        // drop it from RAM (the same gate per-node eviction uses — never lose data).
        let state = self.multi.app_state();
        self.backend.checkpoint_all(&state).await?;

        // Drop the in-RAM topology / properties / vectors; read-through left intact.
        let core = {
            let s = state.read().await;
            s.registry.get(graph_name).map(|e| e.core.clone())
        };
        let core = core.ok_or_else(|| format!("graph '{graph_name}' not found"))?;
        let freed = core.hibernate();
        tracing::info!("hibernate: graph '{graph_name}' evicted {freed} node(s) from RAM");
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
        tracing::info!("rehydrate: graph '{graph_name}' restored {n} node(s) from redb");
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
