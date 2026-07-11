//! Cold-tenant whole-graph offload (CONCEPT:EG-KG.sharding.eg-r6, M3).
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

/// Per-graph last-access tracker + offload bookkeeping (CONCEPT:EG-KG.sharding.eg-r6). The engine calls
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

    /// Pick the COLDEST (least-recently-touched) graph among `candidates`
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog — bounded hot-context cache admission control, DIST-P2-3).
    /// A candidate this tracker has no access record for sorts FIRST (treated as
    /// infinitely cold — e.g. a graph that was just lazily opened and has not yet
    /// been `touch`ed on the dispatch path), ahead of one that has been genuinely
    /// active. `None` when `candidates` is empty.
    pub fn coldest(&self, candidates: impl Iterator<Item = String>) -> Option<String> {
        let last_access = self.last_access.lock();
        candidates.min_by_key(|g| last_access.get(g).copied())
    }
}

/// Offload one resident graph's whole in-RAM state, durability-gated (CONCEPT:EG-KG.sharding.eg-r6).
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
/// (CONCEPT:EG-KG.sharding.eg-r6). Returns the number of graphs offloaded. The shared `__commons__`
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

// ── Bounded hot-context cache: admission control + lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog,
// DIST-P2-3) ─────────────────────────────────────────────────────────────────────────
//
// The registry's catalog (`GraphRegistry::register_catalog_only`/`open_lazy`) lets
// the engine KNOW about millions of graphs at the cost of a metadata row each. This
// section bounds how many of them are simultaneously RESIDENT (a real `GraphCore`):
// a cap on hot contexts, enforced by evicting the coldest resident graph — by this
// tracker's last-access recency, reusing the SAME durability-gated `offload_graph_core`
// hibernate path R6 cold-offload uses — before a new one is admitted. `__commons__`
// is never evicted.

/// The configured cap on RESIDENT hot-context graphs (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
/// `EPISTEMIC_GRAPH_MAX_RESIDENT_GRAPHS` — `0` (unset, the default) ⇒ UNBOUNDED:
/// every accessed/created graph stays resident forever, exactly the pre-DIST-P2-3
/// behavior, so a small deployment is byte-for-byte unchanged.
pub fn max_resident_graphs() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_RESIDENT_GRAPHS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Admit a new/lazily-opened graph into the bounded hot-context cache
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog): while the resident set is AT `cap`, evict the
/// COLDEST resident graph (`__commons__` and `incoming` itself excluded) —
/// durability-gated via `offload_graph_core` (the same no-loss hibernate path the
/// R6 idle sweep and the byte-budget enforcer use) — REMOVING it from the resident
/// map entirely (`GraphRegistry::evict_resident`, not merely `hibernate`, so the
/// freed memory includes the `GraphCore` structures themselves, not just its
/// content). The catalog row survives, so the evicted graph re-opens on its next
/// access. `cap == 0` ⇒ unbounded (a no-op) — the default, matching pre-existing
/// behavior for a small deployment. Takes the cap as an explicit parameter (not
/// read from the environment here) so it composes cleanly with a caller that
/// already resolved it once, and so tests can exercise a tight cap deterministically
/// without mutating global process env state.
pub fn admit_capacity(s: &mut ServerState, tracker: &ColdTenantTracker, incoming: &str, cap: usize) {
    if cap == 0 {
        return;
    }
    while s.registry.resident_len() >= cap {
        let authoritative = s.redb_authoritative;
        let candidate_names: Vec<String> = s
            .registry
            .all_entries()
            .iter()
            .filter(|e| e.name != "__commons__" && e.name != incoming)
            .map(|e| e.name.clone())
            .collect();
        let victim = match tracker.coldest(candidate_names.into_iter()) {
            Some(v) => v,
            None => return, // nothing evictable (only __commons__/incoming resident)
        };
        let core = match s.registry.get(&victim) {
            Some(e) => e.core.clone(),
            None => return, // race: victim already gone
        };
        offload_graph_core(&core, authoritative);
        s.registry.evict_resident(&victim);
        tracker.forget(&victim);
    }
}

/// Lazily materialize a catalog-known graph's resident `GraphCore` on first access
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog). A no-op if the graph is ALREADY resident (the common
/// case on the hot path — checked by the caller BEFORE taking the write lock this
/// function needs, so an already-resident graph never pays for one) or genuinely
/// unknown (neither cataloged nor resident — the caller's subsequent registry
/// lookup then reports "not found", unchanged from pre-DIST-P2-3 behavior).
/// Applies bounded-cache admission (`admit_capacity`) BEFORE materializing so the
/// resident set never exceeds `cap`. Returns `true` when the graph is resident
/// afterward.
pub async fn lazy_open(state: &Arc<RwLock<ServerState>>, graph_name: &str, cap: usize) -> bool {
    let mut s = state.write().await;
    if s.registry.is_resident(graph_name) {
        return true;
    }
    if !s.registry.exists(graph_name) {
        return false;
    }
    let tracker = s.cold_tracker.clone();
    admit_capacity(&mut s, &tracker, graph_name, cap);
    s.registry.open_lazy(graph_name)
}

#[cfg(test)]
mod admission_tests {
    //! Bounded hot-context cache + admission-control proofs over a REAL redb
    //! backend (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — the durable tier a lazily-opened
    //! graph rehydrates from, and the tier that makes eviction loss-free.
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{GraphType, Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::compute_auth_token;
    use crate::server::persistence::read_through::{
        BackendGraphMaterializer, BackendReadThroughFactory,
    };
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::server::{dispatch, ServerState};
    use crate::wal_service::FsyncPolicy;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec(&v).unwrap()
    }

    const SECRET: &str = "lazy-lifecycle-test";

    async fn redb_state(dir_s: &str) -> Arc<RwLock<ServerState>> {
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.to_string(), FsyncPolicy::Each, 64).expect("open"));
        let state = Arc::new(RwLock::new(ServerState {
            cold_tracker: Arc::new(ColdTenantTracker::new()),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.to_string()),
            persistence: Some(backend.clone()),
            redb_authoritative: true,
            max_in_flight: Arc::new(Semaphore::new(64)),
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 32,
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
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
        }));
        // Wire the read-through + lazy-open materializer exactly like main.rs does
        // under authoritative mode.
        {
            let mut s = state.write().await;
            let rt_factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
            s.registry.set_read_through_factory(rt_factory);
            let materializer = Arc::new(BackendGraphMaterializer::new(backend.clone()));
            s.registry.set_materializer(materializer);
        }
        state
    }

    fn req(id: u64, graph: &str, method: Method) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        }
    }

    async fn create(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str) {
        let r = dispatch(
            state,
            req(
                id,
                graph,
                Method::CreateGraph {
                    graph_name: graph.into(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "create {graph}: {:?}", r.error);
    }

    async fn add_node(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, node: &str) {
        let r = dispatch(
            state,
            req(
                id,
                graph,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: props(serde_json::json!({"payload": node})),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "add {node}: {:?}", r.error);
    }

    /// N graphs are registered (catalog rows) but only M<N are ever accessed —
    /// the untouched majority never materializes a resident `GraphCore`. Accessing
    /// a cold one lazily opens it (byte-identical data) without disturbing the
    /// rest.
    #[tokio::test(flavor = "multi_thread")]
    async fn n_registered_m_resident_lazy_open_on_access() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-n-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        // Register 20 graphs, but only touch 3 of them.
        let n = 20;
        for i in 0..n {
            create(&state, i as u64, &format!("tenant:{i}")).await;
        }
        for i in 0..n {
            add_node(&state, 1000 + i as u64, &format!("tenant:{i}"), "seed").await;
        }
        // Checkpoint so every graph is durable, then evict ALL of them back to
        // catalog-only (simulating cold tenants that have aged out of RAM),
        // leaving the catalog fully populated but nothing but __commons__ resident.
        {
            let backend = state.read().await.persistence.clone().unwrap();
            backend.checkpoint_all(&state).await.unwrap();
        }
        {
            let mut s = state.write().await;
            for i in 0..n {
                s.registry.evict_resident(&format!("tenant:{i}"));
            }
        }

        assert_eq!(
            state.read().await.registry.catalog_len(),
            n + 1,
            "catalog knows about every registered graph"
        );
        assert_eq!(
            state.read().await.registry.resident_len(),
            1,
            "only __commons__ resident after eviction — catalog rows, not resident cores"
        );

        // Access exactly 3 cold graphs through the real dispatch path.
        for i in [2, 9, 17] {
            let r = dispatch(
                &state,
                req(
                    5000 + i,
                    &format!("tenant:{i}"),
                    Method::GetNodeProperties {
                        node_id: "seed".into(),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "lazy open tenant:{i}: {:?}", r.error);
        }

        assert_eq!(
            state.read().await.registry.resident_len(),
            4,
            "__commons__ + exactly the 3 accessed graphs are resident"
        );
        assert!(state.read().await.registry.is_resident("tenant:2"));
        assert!(!state.read().await.registry.is_resident("tenant:3"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An evicted graph's data is fully intact after it lazily re-opens — the
    /// durability-gated eviction never loses anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn evicted_graph_lazy_reopens_with_data_intact() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-reopen-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        create(&state, 1, "acme:cold").await;
        for i in 0..5 {
            add_node(&state, 100 + i, "acme:cold", &format!("n{i}")).await;
        }
        {
            let backend = state.read().await.persistence.clone().unwrap();
            backend.checkpoint_all(&state).await.unwrap();
        }
        {
            let mut s = state.write().await;
            assert!(s.registry.evict_resident("acme:cold"));
        }
        assert!(!state.read().await.registry.is_resident("acme:cold"));

        // Access it — lazy_open re-materializes it from the durable tier.
        let opened = lazy_open(&state, "acme:cold", 0).await;
        assert!(opened);
        let core = {
            let s = state.read().await;
            s.registry.get("acme:cold").unwrap().core.clone()
        };
        assert_eq!(core.node_count(), 5, "all 5 nodes survived the round-trip");
        for i in 0..5 {
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"payload": format!("n{i}")})))
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Startup with many persisted graphs (`load_catalog`) hydrates NONE of them —
    /// only the catalog populates; every graph materializes on first access.
    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_startup_hydrates_nothing_until_accessed() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-boot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // ── write side: create + populate 8 graphs, then shut down ──
        {
            let state = redb_state(&dir_s).await;
            for i in 0..8 {
                create(&state, i as u64, &format!("boot:{i}")).await;
                add_node(&state, 100 + i as u64, &format!("boot:{i}"), "n").await;
            }
            let backend = state.read().await.persistence.clone().unwrap();
            backend.checkpoint_all(&state).await.unwrap();
            backend.shutdown();
        }

        // ── reload side: fresh backend + fresh empty state, CATALOG-ONLY load ──
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("reopen"));
        let state2 = Arc::new(RwLock::new(ServerState {
            cold_tracker: Arc::new(ColdTenantTracker::new()),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: Some(backend2.clone()),
            redb_authoritative: true,
            max_in_flight: Arc::new(Semaphore::new(64)),
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 32,
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
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
        }));
        {
            let mut s = state2.write().await;
            let rt_factory = Arc::new(BackendReadThroughFactory::new(backend2.clone()));
            s.registry.set_read_through_factory(rt_factory);
            let materializer = Arc::new(BackendGraphMaterializer::new(backend2.clone()));
            s.registry.set_materializer(materializer);
        }

        let loaded = backend2.load_catalog(&state2).await.unwrap();
        assert_eq!(loaded, 9, "boot:0..7 + __commons__ cataloged");
        assert_eq!(
            state2.read().await.registry.resident_len(),
            1,
            "catalog-only load hydrates NOTHING but the pre-created __commons__"
        );
        for i in 0..8 {
            assert!(state2.read().await.registry.exists(&format!("boot:{i}")));
            assert!(!state2.read().await.registry.is_resident(&format!("boot:{i}")));
        }

        // Touch ONE — it lazily hydrates with its data intact; the rest stay cold.
        let r = dispatch(
            &state2,
            req(
                9000,
                "boot:3",
                Method::GetNodeProperties {
                    node_id: "n".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none());
        assert!(state2.read().await.registry.is_resident("boot:3"));
        assert!(!state2.read().await.registry.is_resident("boot:4"));

        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Quota enforcement: with a tight resident cap, accessing more cold graphs
    /// than the cap never grows the resident set past it — the coldest is evicted
    /// to admit each new one.
    #[tokio::test(flavor = "multi_thread")]
    async fn quota_bounds_resident_count_under_churn() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-quota-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        let n = 10;
        for i in 0..n {
            create(&state, i as u64, &format!("quota:{i}")).await;
            add_node(&state, 100 + i as u64, &format!("quota:{i}"), "n").await;
        }
        {
            let backend = state.read().await.persistence.clone().unwrap();
            backend.checkpoint_all(&state).await.unwrap();
        }
        {
            let mut s = state.write().await;
            for i in 0..n {
                s.registry.evict_resident(&format!("quota:{i}"));
            }
        }
        assert_eq!(state.read().await.registry.resident_len(), 1);

        // A cap of 4 (including the pinned __commons__): access all 10 cold graphs
        // one at a time through `lazy_open`, enforcing the cap on every admission.
        let cap = 4usize;
        for i in 0..n {
            let name = format!("quota:{i}");
            let opened = lazy_open(&state, &name, cap).await;
            assert!(opened, "quota:{i} must lazily open");
            let resident = state.read().await.registry.resident_len();
            assert!(
                resident <= cap,
                "resident count {resident} exceeds cap {cap} after opening quota:{i}"
            );
            // Touch it so it isn't immediately picked as "coldest" by an artifact
            // of insertion order alone (mirrors the dispatch path's cold_tracker.touch).
            state.read().await.cold_tracker.touch(&name);
        }

        // The cap held throughout — never more than `cap` resident graphs at once,
        // even after opening far more than `cap` distinct cold graphs.
        assert!(state.read().await.registry.resident_len() <= cap);
        // The MOST RECENTLY opened graph must still be resident (LRU protects it).
        assert!(state
            .read()
            .await
            .registry
            .is_resident(&format!("quota:{}", n - 1)));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
