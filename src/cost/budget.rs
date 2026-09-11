//! Per-tenant memory-budget enforcement: the sweep that keeps resident graph
//! memory inside the configured ceiling.
//!
//! It is the only part of the cost model that MUTATES engine state — every
//! other item in [`super`] measures or estimates — so it owns its own module.
//! The sweep is deliberately three phases: take one census of what is
//! resident, derive the budget that census implies, then reclaim per tenant
//! and per graph. Before the split those phases were one function of
//! cyclomatic 17 / cognitive 42.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::{
    cost_state, forget_graph_incarnation, is_hibernated, note_hibernated, tenant_of, CostConfig,
};
use crate::graph::GraphCore;
use crate::registry::GraphHandle;
use crate::server::ServerState;

/// Enforce the per-tenant memory budgets ONCE (CONCEPT:EG-KG.compute.lane-v). For every tenant over
/// its effective budget (the smaller of its configured budget and its fair share of the
/// global ceiling), reclaim memory from its COLDEST graphs until it is back under budget:
///
/// 1. **Evict** the graph's LRU nodes (durability-gated, reusing the per-graph eviction
///    path) down toward an empty resident set.
/// 2. If the graph is still resident and the tenant is still over budget, **hibernate**
///    the graph (drop all its in-RAM state; durable in redb, read-through serves reads).
///
/// "Coldest" = fewest nodes touched (the graph with the smallest node count is reclaimed
/// first, preserving the hot working set). Returns `(nodes_evicted, graphs_hibernated)`.
/// Pure-Rust; reuses the existing durability-gated evict + hibernate ops, so it never
/// loses data. No-op when budgeting is disabled.
pub async fn enforce_memory_budgets(
    state: &Arc<RwLock<ServerState>>,
    config: CostConfig,
) -> (u64, u64) {
    let census = MemoryCensus::take(state).await;
    let budget = census.effective_budget(&config);
    let backend = census.backend;
    let mut total_evicted = 0u64;
    let mut total_hibernated = 0u64;
    for (tenant, graphs) in census.per_tenant {
        let resident = census.resident.get(&tenant).copied().unwrap_or(0);
        let (evicted, hibernated) = reclaim_tenant(state, graphs, resident, budget, &backend).await;
        total_evicted += evicted;
        total_hibernated += hibernated;
    }
    (total_evicted, total_hibernated)
}

/// The durable authority a reclaim writes through before dropping RAM.
type DurableBackend = Option<Arc<dyn crate::server::persistence::PersistenceBackend>>;

/// One sweep's view of what is resident: every graph's modeled footprint,
/// grouped by the tenant whose budget it counts against.
struct MemoryCensus {
    /// tenant → its graphs, each with the bytes its core currently models.
    per_tenant: HashMap<String, Vec<(GraphHandle, u64)>>,
    /// tenant → the sum of those bytes.
    resident: HashMap<String, u64>,
    /// The durable authority captured with the same registry read.
    backend: DurableBackend,
}

impl MemoryCensus {
    /// Snapshot the registry once: taking the footprint and the durable
    /// authority under one read is what makes the rest of the sweep lock-free.
    async fn take(state: &Arc<RwLock<ServerState>>) -> Self {
        let (entries, backend) = {
            let s = state.read().await;
            let entries: Vec<GraphHandle> = s
                .registry
                .all_entries()
                .iter()
                .filter_map(|e| s.registry.handle(&e.name))
                .collect();
            (entries, s.persistence.clone())
        };
        let mut census = Self {
            per_tenant: HashMap::new(),
            resident: HashMap::new(),
            backend,
        };
        for handle in entries {
            let mem = handle.core.memory_estimate();
            let tenant = tenant_of(&handle.name).to_string();
            *census.resident.entry(tenant.clone()).or_default() += mem;
            census
                .per_tenant
                .entry(tenant)
                .or_default()
                .push((handle, mem));
        }
        census
    }

    /// The budget one tenant is actually held to: the smaller of its configured
    /// budget and an even split of the global ceiling across the tenants active
    /// in this census, so one hot tenant cannot consume the ceiling and starve
    /// the others.
    ///
    /// The split is the plain `ceiling / active_tenants`, deliberately NOT
    /// scaled by `process_rss_bytes()`: real RSS is not commensurable with the
    /// tenants' modeled `memory_estimate()` sum — it also carries the binary
    /// image, allocator arenas, thread stacks and every other subsystem's heap.
    /// Scaling by `global_ceiling / rss` crushed the target toward zero whenever
    /// that fixed overhead dominated RSS, reclaiming tenants comfortably under
    /// budget by any measure of their OWN data (the
    /// `fair_cap_protects_small_tenant` regression: an 8 KiB ceiling against a
    /// multi-hundred-MB test-process RSS zeroed the share and evicted a tenant
    /// holding one tiny node).
    fn effective_budget(&self, config: &CostConfig) -> u64 {
        let active_tenants = self.per_tenant.len().max(1) as u64;
        let fair_share = config.global_ceiling_bytes / active_tenants;
        config.per_tenant_budget_bytes.min(fair_share.max(1))
    }
}

/// Reclaim one tenant's coldest graphs — fewest nodes first, so the hot working
/// set is preserved — until it is back under `budget`. Returns
/// `(nodes_evicted, graphs_hibernated)`.
async fn reclaim_tenant(
    state: &Arc<RwLock<ServerState>>,
    mut graphs: Vec<(GraphHandle, u64)>,
    mut resident: u64,
    budget: u64,
    backend: &DurableBackend,
) -> (u64, u64) {
    let mut evicted = 0u64;
    let mut hibernated = 0u64;
    if resident <= budget {
        return (evicted, hibernated);
    }
    graphs.sort_by_key(|(handle, _)| handle.core.node_count());
    for (handle, mem) in graphs {
        if resident <= budget {
            break;
        }
        // The `__commons__` shared graph is never reclaimed for a budget — it is
        // not a tenant's private working set and every agent needs it.
        if handle.name.as_str() == "__commons__" {
            continue;
        }
        let Some(reclaimed) = reclaim_graph(state, &handle, mem, resident, budget, backend).await
        else {
            continue;
        };
        evicted += reclaimed.evicted;
        hibernated += u64::from(reclaimed.hibernated);
        resident = resident
            .saturating_sub(mem)
            .saturating_add(reclaimed.residual);
    }
    (evicted, hibernated)
}

/// What reclaiming one graph freed, and what it still holds.
struct Reclaimed {
    /// Nodes this sweep evicted from the graph's resident set.
    evicted: u64,
    /// Whether the graph was hibernated.
    hibernated: bool,
    /// The graph's modeled footprint after the sweep.
    residual: u64,
}

/// Reclaim one graph under the lifecycle lock. `None` means the captured handle
/// went stale before the sweep reached it, so nothing was touched and the
/// tenant's total is unchanged.
async fn reclaim_graph(
    state: &Arc<RwLock<ServerState>>,
    handle: &GraphHandle,
    mem: u64,
    tenant_resident: u64,
    budget: u64,
    backend: &DurableBackend,
) -> Option<Reclaimed> {
    let name = &handle.name;
    // A budget sweep performs durable I/O outside the registry lock. Serialize
    // it with lifecycle and graph writes, then reject a stale captured handle
    // before touching either RAM or bookkeeping.
    let _lifecycle_guard = crate::server::mutation_batch::lock_graph(name).await;
    if !state.read().await.registry.is_current_handle(handle) {
        return None;
    }

    // Step 1: durability-gated LRU eviction down to empty (`max_nodes = 0`
    // evicts every durable node; a node whose durability cannot be confirmed
    // stays).
    let evicted = evict_graph_to(name, &handle.core, 0, backend).await as u64;
    if evicted > 0 {
        cost_state()
            .evicted_total
            .fetch_add(evicted, std::sync::atomic::Ordering::Relaxed);
        crate::metrics::budget_evicted(evicted);
    }

    // Step 2: if still resident and the tenant is still over budget, hibernate
    // only after confirming every remaining node in durable authority.
    let still_over = tenant_resident.saturating_sub(mem) < budget;
    let hibernated =
        handle.core.node_count() > 0 && still_over && hibernate_graph(state, handle, backend).await;

    // U-148 / BUG-130: steps 1-2 only drop this graph's RAM
    // (`GraphCore::evict_resident_nodes`/`hibernate`) — they never touch the
    // REGISTRY's residency bookkeeping. When they leave the
    // durability-confirmed core fully empty, the registry still reports
    // `is_resident(name) == true` with zero topology: `cold_offload::lazy_open`
    // short-circuits on that residency check and never rehydrates it, so every
    // whole-graph read (Cypher, counts, traversal) observes a permanently empty
    // snapshot while a point `get_node_properties` still finds data through the
    // separate read-through seam. Mirror `admit_capacity`'s already-correct
    // pattern: once fully durable-confirmed-empty, transition the registry entry
    // to catalog-only so the NEXT access takes the existing bounded durable
    // lazy-open instead of silently serving the stale empty resident image.
    if handle.core.node_count() == 0 {
        let mut s = state.write().await;
        if s.registry.evict_resident_if_current(handle) {
            // Eviction is a complete lifecycle transition for this incarnation;
            // its hibernation marker must not survive.
            forget_graph_incarnation(name, &handle.incarnation_id);
        }
    }

    // A graph that came back resident (rehydrated by access between sweeps) is
    // no longer hibernated.
    if handle.core.node_count() > 0 && is_hibernated(name, &handle.incarnation_id) {
        note_hibernated(name, &handle.incarnation_id, false);
    }

    Some(Reclaimed {
        evicted,
        hibernated,
        residual: handle.core.memory_estimate(),
    })
}

/// Hibernate one graph if every remaining node is confirmed durable, publishing
/// the hibernation marker behind an explicit registry fence.
async fn hibernate_graph(
    state: &Arc<RwLock<ServerState>>,
    handle: &GraphHandle,
    backend: &DurableBackend,
) -> bool {
    if hibernate_graph_if_durable(&handle.name, &handle.core, backend) == 0 {
        return false;
    }
    // The lifecycle lane is held, but keep the registry fence explicit before
    // publishing hibernation state.
    let s = state.write().await;
    if !s.registry.is_current_handle(handle) {
        return false;
    }
    note_hibernated(&handle.name, &handle.incarnation_id, true);
    crate::metrics::budget_hibernated();
    true
}

/// Evict one graph's LRU nodes down to `max_nodes` only after durable presence is
/// confirmed (CONCEPT:EG-KG.storage.read-through-seam-exercised).
async fn evict_graph_to(
    fname_graph: &str,
    core: &Arc<GraphCore>,
    max_nodes: usize,
    backend: &Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> usize {
    let backend = match backend {
        Some(b) => b,
        None => return 0, // nothing durable to confirm against ⇒ never drop
    };
    let candidates = core.lru_eviction_candidates(max_nodes);
    if candidates.is_empty() {
        return 0;
    }
    let fname = crate::persist::sanitize(fname_graph);
    let presence = match backend.durable_node_presence(&fname, &candidates) {
        Ok(value) if value.len() == candidates.len() => value,
        Ok(_) | Err(_) => return 0,
    };
    let durable: Vec<String> = candidates
        .into_iter()
        .zip(presence)
        .filter_map(|(node_id, present)| present.then_some(node_id))
        .collect();
    core.evict_resident_nodes(&durable)
}

fn hibernate_graph_if_durable(
    graph_name: &str,
    core: &GraphCore,
    backend: &Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> usize {
    let Some(backend) = backend else {
        return 0;
    };
    let node_ids: Vec<String> = core
        .get_nodes()
        .into_iter()
        .map(|(node_id, _)| node_id)
        .collect();
    if node_ids.is_empty() {
        return 0;
    }
    let fname = crate::persist::sanitize(graph_name);
    match backend.durable_node_presence(&fname, &node_ids) {
        Ok(presence)
            if presence.len() == node_ids.len() && presence.iter().all(|present| *present) =>
        {
            core.hibernate()
        }
        Ok(_) | Err(_) => 0,
    }
}
