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
//! Offload is durability-gated: every resident node must be confirmed in the
//! authoritative backend before the in-RAM core is dropped. An offloaded graph is
//! therefore never lost, only evicted.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::persistence::PersistenceBackend;
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
pub fn offload_graph_core(
    core: &GraphCore,
    backend: &Arc<dyn PersistenceBackend>,
    graph_name: &str,
) -> Option<usize> {
    if core.node_count() == 0 {
        return Some(0); // already empty / hibernated
    }
    let node_ids: Vec<String> = core
        .get_nodes()
        .into_iter()
        .map(|(node_id, _)| node_id)
        .collect();
    let fname = crate::persist::sanitize(graph_name);
    let presence = backend.durable_node_presence(&fname, &node_ids).ok()?;
    if presence.len() != node_ids.len() || presence.iter().any(|present| !present) {
        return None;
    }
    Some(core.hibernate())
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

    // Snapshot the resident cores + durable authority under a read lock.
    let (entries, persistence) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        (entries, s.persistence.clone())
    };
    let Some(persistence) = persistence else {
        return 0;
    };

    let mut offloaded = 0u64;
    for (name, core) in entries {
        if name == "__commons__" || !cold.contains(&name) || tracker.is_offloaded(&name) {
            continue;
        }
        if core.node_count() == 0 {
            continue; // nothing resident to drop
        }
        if let Some(freed) = offload_graph_core(&core, &persistence, &name) {
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

pub const DEFAULT_PRODUCTION_MAX_RESIDENT_GRAPHS: usize = 1024;
pub const DEFAULT_PRODUCTION_LAZY_OPEN_PAGE_SIZE: usize = 4096;

/// The configured finite cap on resident hot-context graphs.
pub fn max_resident_graphs() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_RESIDENT_GRAPHS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PRODUCTION_MAX_RESIDENT_GRAPHS)
}

/// Admit a new/lazily-opened graph into the bounded hot-context cache
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog): while the resident set is AT `cap`, evict the
/// COLDEST resident graph (`__commons__` and `incoming` itself excluded) —
/// durability-gated via `offload_graph_core` (the same no-loss hibernate path the
/// R6 idle sweep and the byte-budget enforcer use) — REMOVING it from the resident
/// map entirely (`GraphRegistry::evict_resident`, not merely `hibernate`, so the
/// freed memory includes the `GraphCore` structures themselves, not just its
/// content). The catalog row survives, so the evicted graph re-opens on its next
/// access. Takes the cap as an explicit parameter so tests can exercise a tight cap
/// deterministically without mutating global process environment.
pub fn admit_capacity(
    s: &mut ServerState,
    tracker: &ColdTenantTracker,
    incoming: &str,
    cap: usize,
) {
    let cap = cap.max(1);
    while s.registry.resident_len() >= cap {
        let Some(persistence) = s.persistence.clone() else {
            return;
        };
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
        if offload_graph_core(&core, &persistence, &victim).is_none() {
            return;
        }
        s.registry.evict_resident(&victim);
        tracker.forget(&victim);
    }
}

/// The configured positive page size for a paged lazy-open.
/// [`eg_core::registry::GraphRegistry::open_lazy_paged`]: the graph becomes resident
/// after one bounded page (nodes+edges combined, per page), instead of after a full
/// rehydrate. Operations receive explicit partial-materialization state until all
/// pages and maintained indexes are valid — the concrete fix for the honest limitation
/// `docs/architecture/epistemic-os-hardening.md` names as open ledger item L38
/// ("first access to a lazily-opened graph still fully rehydrates it"). The rest of
/// the graph's material is paged in by [`page_in_remaining`] as a background task;
/// no incomplete whole-graph result is served in the interim.
pub fn lazy_open_page_size() -> usize {
    std::env::var("EPISTEMIC_GRAPH_LAZY_OPEN_PAGE_SIZE")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_PRODUCTION_LAZY_OPEN_PAGE_SIZE)
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
///
/// `page_size` (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged adjacency") — resolved by the
/// caller via [`lazy_open_page_size`], same convention as `cap`/[`max_resident_graphs`]
/// (an explicit parameter, not read from the environment HERE, so tests can exercise a
/// specific page size deterministically without mutating global process env state).
/// The positive page size materializes only one bounded page inline and spawns
/// [`page_in_remaining`] to fetch the rest off the request path. The graph is resident
/// but explicitly partial on return; a paged open never blocks the triggering request
/// on a full rehydrate of a very large graph.
///
/// Gated on `redb`: the ONLY caller (`dispatch.rs`'s durable-read path) invokes this
/// from inside its own `#[cfg(feature = "redb")]` block, and the body reads
/// `ServerState::cold_tracker`, which only exists on a `redb` build.
#[cfg(feature = "redb")]
pub async fn lazy_open(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    cap: usize,
    page_size: usize,
) -> bool {
    // The same striped per-graph lane used by Create/Delete/MutationBatch fences
    // lifecycle work without serializing unrelated graphs. Durable page reads run
    // outside the server-wide state lock.
    let _lifecycle_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let ticket = {
        let s = state.read().await;
        if s.registry.is_resident(graph_name) {
            return true;
        }
        if !s.registry.exists(graph_name) {
            return false;
        }
        match s.registry.prepare_lazy_open(graph_name) {
            Some(ticket) => ticket,
            None => return false,
        }
    };

    let page_size = page_size.max(1);
    let fetch_ticket = ticket.clone();
    let page =
        match tokio::task::spawn_blocking(move || fetch_ticket.materialize_page(None, page_size))
            .await
        {
            Ok(page) => page,
            Err(_) => return false,
        };
    let mut s = state.write().await;
    let tracker = s.cold_tracker.clone();
    admit_capacity(&mut s, &tracker, graph_name, cap);
    let outcome = s.registry.publish_lazy_first_page(&ticket, page);
    if !outcome.resident {
        return false;
    }
    if let Some(cursor) = outcome.cursor {
        drop(s);
        tokio::spawn(page_in_remaining(state.clone(), ticket, cursor, page_size));
    }
    true
}

/// Background continuation of a paged [`lazy_open`] (CONCEPT:EG-KG.sharding.paged-lazy-open, L38): keeps
/// calling [`eg_core::registry::GraphRegistry::page_in`] until the cursor is exhausted
/// (the graph's ENTIRE durable material has landed — the "no full rehydrate on first
/// touch, load on demand" property, just spread across bounded off-request-path
/// steps instead of one inline blocking fetch). Stops early, as a benign no-op, if the
/// graph is evicted (cold-offloaded) or deleted out from under it before paging
/// finishes — there is nothing left to continue into, and the NEXT lazy-open (paged
/// or not) starts a fresh rehydrate from the durable tier regardless. Never awaited by
/// a caller; entirely off the request path.
///
async fn page_in_remaining(
    state: Arc<RwLock<ServerState>>,
    ticket: crate::registry::LazyOpenTicket,
    mut cursor: crate::registry::MaterializeCursor,
    page_size: usize,
) {
    loop {
        let _lifecycle_guard = crate::server::mutation_batch::lock_graph(ticket.name()).await;
        if ticket.is_cancelled() {
            return;
        }
        let fetch_ticket = ticket.clone();
        let fetch_cursor = cursor;
        let page = match tokio::task::spawn_blocking(move || {
            fetch_ticket.materialize_page(Some(fetch_cursor), page_size)
        })
        .await
        {
            Ok(Some(page)) => page,
            _ => return,
        };
        let (handle, manifest) = {
            let s = state.read().await;
            let Some(handle) = s.registry.handle(ticket.name()) else {
                return;
            };
            let Some(manifest) = s.registry.materialization_handle(ticket.name()) else {
                return;
            };
            (handle, manifest)
        };
        let next = crate::registry::GraphRegistry::apply_lazy_page_to_handle(
            &ticket, &handle, &manifest, page,
        );
        match next {
            Some(c) => cursor = c,
            None => {
                // `None` is either complete or a failed/mixed source snapshot.
                // A failed partial core is evicted so the next access restarts
                // from page zero; it is never advertised as complete.
                let mut s = state.write().await;
                if s.registry
                    .materialization_manifest(ticket.name())
                    .is_some_and(|manifest| {
                        manifest.phase == crate::registry::MaterializationPhase::Failed
                    })
                {
                    s.registry.evict_resident(ticket.name());
                }
                return;
            }
        }
    }
}

// Every test here drives a REAL `RedbBackend` through `lazy_open`/`admit_capacity`
// (both `redb`-gated), so — like every other `ServerState`-constructing test module
// in this crate — the whole module only builds under the `redb` feature.
#[cfg(all(test, feature = "redb"))]
mod admission_tests {
    //! Bounded hot-context cache + admission-control proofs over a REAL redb
    //! backend (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — the durable tier a lazily-opened
    //! graph rehydrates from, and the tier that makes eviction loss-free.
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::channels::ChannelManager;
    use crate::durability::DurabilityPolicy;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{GraphType, Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::persistence::read_through::{
        BackendGraphMaterializer, BackendReadThroughFactory,
    };
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::server::{
        compute_verified_envelope_token, dispatch, ServerState, VerifiedEnvelopeParams,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::{RwLock, Semaphore};

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec(&v).unwrap()
    }

    const SECRET: &str = "lazy-lifecycle-test";
    const TEST_AGENT: &str = "unit-test-agent";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    /// BUG-044-class: keep the full (`--features full`) dispatcher's state machine
    /// behind one heap indirection. `dispatch_on_heap()` bottoms out in `dispatch_inner`
    /// (`src/server/dispatch.rs`), one very large async fn whose generated future is
    /// enormous; awaiting it inline inside a test's own future can exhaust the
    /// harness thread's stack before the first request is even polled, SIGABRTing
    /// the whole test binary. Mirrors `server::mod::tests::dispatch_on_heap` (8e00e0b).
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::protocol::Response> + Send + 'a>>
    {
        Box::pin(dispatch(state, request))
    }

    fn current_isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    async fn redb_state(dir_s: &str) -> Arc<RwLock<ServerState>> {
        let backend: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_s.to_string(), DurabilityPolicy::Each, 64).expect("open"),
        );
        let state = Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: Arc::new(ColdTenantTracker::new()),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.to_string()),
            persistence: Some(backend.clone()),
            max_in_flight: Arc::new(Semaphore::new(64)),
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 32,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(dashmap::DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
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
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
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
        // See `cost.rs`'s `req()` for why this is `Once`-guarded: process-global
        // `set_var`, called from every request built by every test in this module.
        static TEST_AUTH_ENV: std::sync::Once = std::sync::Once::new();
        TEST_AUTH_ENV.call_once(|| {
            std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
            std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
            std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
            std::env::set_var(
                "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
                std::env::temp_dir()
                    .join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
            );
        });
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "cold-offload-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("cold-offload-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    async fn create(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str) {
        let r = dispatch_on_heap(
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
        let r = dispatch_on_heap(
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
            let r = dispatch_on_heap(
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
            let mut s = state.write().await;
            assert!(s.registry.evict_resident("acme:cold"));
        }
        assert!(!state.read().await.registry.is_resident("acme:cold"));

        // Access it — lazy_open re-materializes it from the durable tier.
        let opened = lazy_open(
            &state,
            "acme:cold",
            DEFAULT_PRODUCTION_MAX_RESIDENT_GRAPHS,
            DEFAULT_PRODUCTION_LAZY_OPEN_PAGE_SIZE,
        )
        .await;
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

    /// CONCEPT:EG-KG.sharding.paged-lazy-open (L38 "paged adjacency") — the literal "no full rehydrate"
    /// proof: `open_lazy_paged` with a small `page_size` over a REAL redb-backed
    /// `RedbBackend::read_graph_material_page_blocking` materializes ONLY that page's
    /// worth of nodes immediately, not the whole graph, and repeated deterministic
    /// `page_in` calls land the rest one bounded batch at a time.
    #[tokio::test(flavor = "multi_thread")]
    async fn open_lazy_paged_materializes_one_page_then_page_in_completes_deterministically() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-paged-manual-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        create(&state, 1, "paged:manual").await;
        let total_nodes = 20u64;
        for i in 0..total_nodes {
            add_node(&state, 100 + i, "paged:manual", &format!("n{i}")).await;
        }
        {
            let mut s = state.write().await;
            assert!(s.registry.evict_resident("paged:manual"));
        }
        assert!(!state.read().await.registry.is_resident("paged:manual"));

        let page_size = 4usize;
        let mut s = state.write().await;
        let outcome = s.registry.open_lazy_paged("paged:manual", page_size);
        assert!(outcome.resident, "paged open reports resident immediately");
        let node_count_after_first_page = s.registry.get("paged:manual").unwrap().core.node_count();
        assert!(
            node_count_after_first_page < total_nodes as usize,
            "the FIRST page must NOT fully rehydrate the graph (got {node_count_after_first_page} \
             of {total_nodes} — a full rehydrate here would defeat the whole point of L38)"
        );
        assert!(
            node_count_after_first_page > 0,
            "the first page still materializes SOMETHING (resident ⇒ queryable right away)"
        );
        assert!(
            outcome.cursor.is_some(),
            "more material remains ⇒ a resume cursor must be returned"
        );

        // Drive page_in to completion — deterministic, no background task / timing.
        let mut cursor = outcome.cursor;
        let mut iterations = 0;
        while let Some(c) = cursor {
            cursor = s.registry.page_in("paged:manual", c, page_size);
            iterations += 1;
            assert!(iterations < 1000, "runaway paging — cursor never exhausted");
        }
        assert!(
            iterations > 1,
            "a {total_nodes}-node graph at page_size={page_size} needs more than one page_in call"
        );

        let final_count = s.registry.get("paged:manual").unwrap().core.node_count();
        assert_eq!(
            final_count, total_nodes as usize,
            "page_in eventually lands EVERY node — no data lost across the paged sequence"
        );
        for i in 0..total_nodes {
            assert_eq!(
                s.registry
                    .get("paged:manual")
                    .unwrap()
                    .core
                    .get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"payload": format!("n{i}")}))),
                "node n{i} survived the paged round-trip byte-identical to a full rehydrate"
            );
        }
        drop(s);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-KG.sharding.paged-lazy-open (L38) — `lazy_open`'s wiring: with `page_size > 0` it
    /// returns resident/queryable immediately (without inline-blocking on a full
    /// rehydrate), and the spawned [`page_in_remaining`] background task eventually
    /// lands the rest of a graph bigger than one page.
    #[tokio::test(flavor = "multi_thread")]
    async fn paged_lazy_open_wiring_is_resident_immediately_then_completes_in_background() {
        let dir = std::env::temp_dir().join(format!("eg-lazy-paged-bg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let state = redb_state(&dir_s).await;

        create(&state, 1, "paged:big").await;
        let total_nodes = 25u64;
        for i in 0..total_nodes {
            add_node(&state, 100 + i, "paged:big", &format!("n{i}")).await;
        }
        {
            let mut s = state.write().await;
            assert!(s.registry.evict_resident("paged:big"));
        }
        assert!(!state.read().await.registry.is_resident("paged:big"));

        // page_size well under total_nodes forces a background continuation.
        let opened = lazy_open(
            &state,
            "paged:big",
            DEFAULT_PRODUCTION_MAX_RESIDENT_GRAPHS,
            5,
        )
        .await;
        assert!(opened, "paged lazy-open must report resident immediately");
        assert!(
            state.read().await.registry.is_resident("paged:big"),
            "graph is queryable right away, even though not fully paged in yet"
        );

        // The background page_in_remaining task eventually lands every node — poll
        // with a bounded deadline rather than a fixed sleep (deterministic pass/fail,
        // no race with the tokio scheduler's timing).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let count = {
                let s = state.read().await;
                s.registry.get("paged:big").map(|e| e.core.node_count())
            };
            if count == Some(total_nodes as usize) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background page-in did not complete within the deadline (last count: {count:?})"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Byte-identical to a full rehydrate: every node's properties survived.
        let core = state
            .read()
            .await
            .registry
            .get("paged:big")
            .unwrap()
            .core
            .clone();
        for i in 0..total_nodes {
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
        // Held for the whole test: it opens `redb_state` (write side), shuts it down,
        // then opens a SECOND `RedbBackend` on the same dir (reload side) — both must
        // resolve the same `EPISTEMIC_GRAPH_ENCRYPTION_KEY` cipher. See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
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
            backend.shutdown();
        }

        // ── reload side: fresh backend + fresh empty state, CATALOG-ONLY load ──
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 64).expect("reopen"));
        let state2 = Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: Arc::new(ColdTenantTracker::new()),
            registry: GraphRegistry::new(),
            // RBAC (CONCEPT:EG-KG.compute.feature) is mandatory for every non-System
            // identity: a bare `IsolationLayer::new()` (zero agents registered)
            // fails `check_graph_access`'s `has_rules()` gate outright the moment
            // the later `GetNodeProperties` reaches dispatch. `redb_state()` above
            // already builds this same fixture's isolation via `current_isolation()`
            // (registers `TEST_AGENT` as `System`); this second, hand-rolled
            // `ServerState` for the reload side needs the identical treatment.
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_s.clone()),
            persistence: Some(backend2.clone()),
            max_in_flight: Arc::new(Semaphore::new(64)),
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 32,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(dashmap::DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
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
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }));
        {
            let mut s = state2.write().await;
            let rt_factory = Arc::new(BackendReadThroughFactory::new(backend2.clone()));
            s.registry.set_read_through_factory(rt_factory);
            let materializer = Arc::new(BackendGraphMaterializer::new(backend2.clone()));
            s.registry.set_materializer(materializer);
        }

        let loaded = backend2.load_catalog(&state2).await.unwrap();
        // Only the 8 `boot:N` graphs are durably cataloged (a `graph_meta` row is
        // written on `create`/first write — see `read_all_graph_meta`): the
        // write-side setup above never creates OR writes to "__commons__", so it
        // has no durable footprint to catalog. `__commons__`'s residency below
        // comes from `GraphRegistry::new()` unconditionally pre-creating it
        // in-memory (every fresh registry's baseline, independent of `load_catalog`
        // — see `resident_len() == 1` next), NOT from the durable catalog scan.
        assert_eq!(loaded, 8, "boot:0..7 cataloged");
        assert_eq!(
            state2.read().await.registry.resident_len(),
            1,
            "catalog-only load hydrates NOTHING but the pre-created __commons__"
        );
        for i in 0..8 {
            assert!(state2.read().await.registry.exists(&format!("boot:{i}")));
            assert!(!state2
                .read()
                .await
                .registry
                .is_resident(&format!("boot:{i}")));
        }

        // Touch ONE — it lazily hydrates with its data intact; the rest stay cold.
        let r = dispatch_on_heap(
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
            let opened =
                lazy_open(&state, &name, cap, DEFAULT_PRODUCTION_LAZY_OPEN_PAGE_SIZE).await;
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
