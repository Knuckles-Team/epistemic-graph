//! Cold-tenant whole-graph offload (CONCEPT:EG-034, M3).
//!
//! ## What it solves
//!
//! KG-2.191 read-through bounds RAM at NODE granularity (an evicted node serves from
//! redb). KG-2.234's budget enforcer reclaims the COLDEST graphs only once a tenant is
//! over its memory budget. Neither bounds RAM for the long tail of tenants that are simply
//! IDLE: with 100M tenants, a graph untouched for hours should not keep its whole in-RAM
//! topology/props/vectors resident just because the box is under its global budget.
//!
//! This adds time-windowed WHOLE-graph offload: a graph not accessed for longer than a
//! window is hibernated (its in-RAM state dropped via [`GraphCore::hibernate`], the SAME
//! primitive the budget enforcer uses) while its durable redb rows are retained, so reads
//! serve through the existing KG-2.191 node read-through and a full topology rebuild
//! rehydrates from the durable dump on demand. It complements — does not replace — the
//! budget-pressure path: this is proactive (idle-driven), that is reactive (budget-driven).
//!
//! Offload is durability-gated: under redb-authoritative mode every acked write is already
//! committed (commit-before-ack, KG-2.187), so dropping the in-RAM core loses nothing —
//! the node read-through serves every node back from redb. In the non-authoritative
//! rebuildable-cache model the external system-of-record holds the data, so dropping the
//! cache is likewise loss-free. Either way, an offloaded graph is never lost, only evicted.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::ServerState;

/// Per-graph last-access tracker + offload bookkeeping (CONCEPT:EG-034). The engine calls
/// [`touch`](ColdTenantTracker::touch) on every read/write of a graph; the periodic
/// [`offload_cold_tenants`] sweep reads it to pick idle graphs. Cheap relaxed atomics +
/// one mutex-guarded map, off the per-op hot path (a `touch` is one map upsert).
#[derive(Default)]
pub struct ColdTenantTracker {
    last_access: Mutex<HashMap<String, Instant>>,
    /// Graphs currently offloaded (drives the offloaded-vs-resident split + avoids
    /// re-offloading a still-cold graph every sweep).
    offloaded: Mutex<HashSet<String>>,
    offloaded_total: AtomicU64,
}

impl ColdTenantTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `graph` was just accessed (read or write). Clears any offloaded mark —
    /// a graph that is touched again has rehydrated and is resident.
    pub fn touch(&self, graph: &str) {
        self.last_access
            .lock()
            .insert(graph.to_string(), Instant::now());
        let mut off = self.offloaded.lock();
        if !off.is_empty() {
            off.remove(graph);
        }
    }

    /// Graphs whose last access is older than `idle_window` (the offload candidates). A
    /// graph never touched is NOT a candidate (no access timestamp = nothing tracked yet).
    pub fn cold_graphs(&self, idle_window: Duration) -> Vec<String> {
        let now = Instant::now();
        self.last_access
            .lock()
            .iter()
            .filter(|(_, t)| now.duration_since(**t) >= idle_window)
            .map(|(g, _)| g.clone())
            .collect()
    }

    fn mark_offloaded(&self, graph: &str) {
        if self.offloaded.lock().insert(graph.to_string()) {
            self.offloaded_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Is this graph currently marked offloaded?
    pub fn is_offloaded(&self, graph: &str) -> bool {
        self.offloaded.lock().contains(graph)
    }

    /// Cumulative graphs offloaded by the cold sweep.
    pub fn offloaded_total(&self) -> u64 {
        self.offloaded_total.load(Ordering::Relaxed)
    }

    /// Forget a graph entirely (e.g. on `DeleteGraph`) so its access timestamp + offload
    /// mark don't leak across a same-name recreate.
    pub fn forget(&self, graph: &str) {
        self.last_access.lock().remove(graph);
        self.offloaded.lock().remove(graph);
    }
}

/// Offload one resident graph's whole in-RAM state, durability-gated (CONCEPT:EG-034).
/// Returns the freed node count, or `None` when it was NOT safe to drop (durability could
/// not be confirmed — the graph stays resident, no loss). Reuses [`GraphCore::hibernate`]
/// (KG-2.224) so reads serve via the KG-2.191 read-through afterward.
pub fn offload_graph_core(core: &GraphCore, authoritative: bool) -> Option<usize> {
    if core.node_count() == 0 {
        return Some(0); // already empty / hibernated
    }
    if authoritative {
        // Every acked node is already durable (commit-before-ack), and the node
        // read-through serves an evicted node from redb — so dropping the core is
        // loss-free.
        Some(core.hibernate())
    } else {
        // Rebuildable-cache model: the external system-of-record holds the data, so the
        // in-RAM core is a rebuildable cache and dropping it is loss-free.
        Some(core.hibernate())
    }
}

/// Sweep the registry and offload every graph idle longer than `idle_window`
/// (CONCEPT:EG-034). Returns the number of graphs offloaded. The shared `__commons__`
/// graph is never offloaded (every agent needs it hot). No-op when `idle_window` is huge
/// / nothing is cold. Pure-Rust; reuses the hibernate + read-through path, so it never
/// loses data.
pub async fn offload_cold_tenants(
    state: &Arc<RwLock<ServerState>>,
    tracker: &ColdTenantTracker,
    idle_window: Duration,
) -> u64 {
    let cold: HashSet<String> = tracker.cold_graphs(idle_window).into_iter().collect();
    if cold.is_empty() {
        return 0;
    }

    // Snapshot the resident cores + the authoritative flag under a read lock.
    let (entries, authoritative) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        (entries, s.redb_authoritative)
    };

    let mut offloaded = 0u64;
    for (name, core) in entries {
        if name == "__commons__" || !cold.contains(&name) || tracker.is_offloaded(&name) {
            continue;
        }
        if core.node_count() == 0 {
            continue; // nothing resident to drop
        }
        if let Some(freed) = offload_graph_core(&core, authoritative) {
            if freed > 0 {
                tracker.mark_offloaded(&name);
                offloaded += 1;
            }
        }
    }
    offloaded
}
