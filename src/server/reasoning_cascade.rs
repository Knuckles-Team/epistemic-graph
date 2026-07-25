//! Reasoning auto-cascade (W3.6 / E16, opt-in) — CDC-triggered, DEBOUNCED
//! re-materialization of the OWL/RL closure, per graph.
//!
//! **Opt-in, never default-on.** Materializing an OWL closure is genuine,
//! ontology-size-dependent CPU cost (`Reasoner::classify*` runs an EL⁺/RL
//! fixpoint). Doing that on every write would be an unbounded tax on the write
//! path, so this is armed ONLY for graphs named in `REASON_ON_WRITE` (a
//! comma-separated allowlist, config-contract style — see this crate's
//! `AGENTS.md` env table). Unset/empty ⇒ the whole mechanism is inert: no
//! background task is even spawned (see `main.rs`), and the one hook on the
//! write path ([`CdcHub::emit`]) costs exactly one `OnceLock::get()` (a single
//! atomic load) before returning.
//!
//! ## Mechanism (wiring, not a new reasoner)
//!
//! This module does not reimplement reasoning — it wires the EXISTING
//! `eg_rdf::owl::Reasoner` (the "delta re-seed" path: [`Reasoner::add_axioms`]
//! keeps the prior `S`/`R` closure and derives only the NEW consequences,
//! CONCEPT:EG-KG.ontology.incremental-materialization) into the write path
//! via the SAME CDC choke point `matview`/`cep` already ride
//! ([`CdcHub::emit`], the one place every committed mutation funnels through —
//! see `src/server/mutation.rs`'s `commit_finalize` step 7).
//!
//! 1. **Trigger** — [`CdcHub::emit`] calls [`ReasoningCascade::note_write`] for
//!    the graph. For a non-opted-in graph this is the ENTIRE cost (one hashset
//!    lookup, no lock, no allocation). For an opted-in graph it bumps a
//!    per-graph debounce clock + a pending-write counter — still O(1), no
//!    reasoning work happens here.
//! 2. **Debounce** — a periodic background sweep ([`spawn`], mirroring the
//!    `cold_offload`/`budget_enforcer` interval-task convention in `main.rs`)
//!    refreshes a graph only once its debounce window has elapsed with NO
//!    further writes — so a burst of N writes to the same graph coalesces into
//!    exactly ONE refresh, not N.
//! 3. **Refresh** — off the tokio reactor (`spawn_blocking`, matching
//!    `compute_off_lock`'s convention for CPU-bound reasoning work): diff the
//!    graph's CURRENT TBox triples (`tbox_triples_from_view`) against the
//!    triples the persistent per-graph `Reasoner` already ingested, and feed
//!    ONLY the new ones to `add_axioms` — the genuine incremental delta
//!    re-seed. EL⁺ completion is MONOTONE (it can only ADD subsumers, never
//!    retract), so a write that REMOVES a TBox triple can't be handled
//!    incrementally without serving a stale (unsound) closure; on a detected
//!    retraction this rebuilds the reasoner from scratch over the current
//!    triple set instead (still just calling the reasoner's own two entry
//!    points as designed, never touching `eg-rdf`).
//!
//! The refreshed [`Classification`] is cached per graph (readable via
//! [`ReasoningCascade::last_refresh`]) — this module owns no query/wire
//! surface of its own; a live `OwlExplain`/`OwlReason` request always
//! recomputes on demand regardless (unchanged, see `server/handlers/rdf.rs`),
//! so the cascade is additive materialization, not a correctness dependency.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use eg_rdf::owl::{parse_ontology, tbox_triples_from_view, Classification, Reasoner};
use eg_rdf::oxrdf::Triple;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::state::ServerState;

/// Opt-in graph allowlist env var — comma-separated exact graph names. Absent
/// or empty ⇒ disabled (the default; NEVER default-on).
pub const REASON_ON_WRITE_ENV: &str = "REASON_ON_WRITE";

/// Debounce-window override env var, milliseconds. Absent/invalid/zero ⇒
/// [`DEFAULT_DEBOUNCE_MS`].
pub const DEBOUNCE_ENV: &str = "EPISTEMIC_GRAPH_REASON_ON_WRITE_DEBOUNCE_MS";

/// Default debounce window when `DEBOUNCE_ENV` is unset/invalid.
const DEFAULT_DEBOUNCE_MS: u64 = 500;

/// Sweep poll granularity. A fixed implementation constant, not an operator
/// knob (like the redb group-linger's shallow-batch threshold): small enough
/// that any sane debounce window is observed promptly, large enough it never
/// busy-loops. Only graphs actually named in `REASON_ON_WRITE` are examined
/// each tick — an unrelated graph's write volume never grows this loop's work.
const SWEEP_TICK: Duration = Duration::from_millis(20);

/// Parse `REASON_ON_WRITE`'s value into the opted-in graph set. A pure
/// function (no env access) so config parsing is unit-testable independent of
/// process env, and so the one real env read happens exactly ONCE, explicitly,
/// at startup (`ReasoningCascade::from_env`) — never behind a lazily-
/// initialized global that would freeze the config at whatever the FIRST
/// caller in the process happened to observe.
pub fn parse_opted_in_graphs(raw: &str) -> HashSet<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Parse the debounce-window override; invalid/zero/absent ⇒ the default.
pub fn parse_debounce_ms(raw: Option<&str>) -> Duration {
    let ms = raw
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_DEBOUNCE_MS);
    Duration::from_millis(ms)
}

/// One completed closure refresh, cached for inspection/observability.
#[derive(Clone)]
pub struct CascadeRefresh {
    /// The freshly-materialized classification.
    pub classification: Classification,
    /// When this refresh completed.
    pub refreshed_at: Instant,
    /// How many `note_write` calls this ONE refresh coalesced (>1 ⇒ a burst
    /// was batched, the debounce doing its job).
    pub coalesced_writes: u64,
    /// New (sub, sup) subsumption pairs vs. the PRIOR cached classification —
    /// the "closure delta" tracing field.
    pub closure_delta: usize,
    /// `true` when a retraction forced a from-scratch rebuild instead of an
    /// incremental `add_axioms` delta (see module docs — EL completion cannot
    /// retract).
    pub rebuilt_from_scratch: bool,
}

/// Per-graph cascade bookkeeping: the debounce clock, the coalesced-write
/// counter, and the persistent incremental reasoning state (the delta re-seed
/// mechanism — kept warm across refreshes so `add_axioms` is a genuine
/// incremental step, not a from-scratch rebuild every time).
#[derive(Default)]
struct GraphCascadeState {
    last_write_at: Option<Instant>,
    pending_writes: u64,
    reasoner: Reasoner,
    known_triples: HashSet<Triple>,
    last: Option<CascadeRefresh>,
    refresh_count: u64,
}

/// The opt-in reasoning cascade: immutable config (opted-in graphs + debounce
/// window, fixed at construction) plus live per-graph debounce/closure state.
///
/// One instance is installed on [`CdcHub`](super::cdc::CdcHub) once at startup
/// (`main.rs`, when `REASON_ON_WRITE` is armed). Tests construct their own
/// instance directly via [`ReasoningCascade::new`] — never through a
/// process-global singleton or by mutating `std::env` — so per-test config is
/// fully deterministic and never entangled with another test/process.
pub struct ReasoningCascade {
    opted_in: HashSet<String>,
    debounce: Duration,
    graphs: DashMap<String, GraphCascadeState>,
}

impl ReasoningCascade {
    /// Construct with an explicit config. Preferred by tests.
    pub fn new(opted_in: HashSet<String>, debounce: Duration) -> Self {
        Self {
            opted_in,
            debounce,
            graphs: DashMap::new(),
        }
    }

    /// Read `REASON_ON_WRITE` + the debounce override exactly once. Called
    /// exactly once by `main.rs` at server startup.
    pub fn from_env() -> Self {
        let opted_in = std::env::var(REASON_ON_WRITE_ENV)
            .ok()
            .map(|raw| parse_opted_in_graphs(&raw))
            .unwrap_or_default();
        let debounce = parse_debounce_ms(std::env::var(DEBOUNCE_ENV).ok().as_deref());
        Self::new(opted_in, debounce)
    }

    /// Whether ANY graph is opted in — gates whether it's worth installing
    /// this cascade on the hub / spawning the sweep task at all.
    pub fn is_active(&self) -> bool {
        !self.opted_in.is_empty()
    }

    pub fn debounce_window(&self) -> Duration {
        self.debounce
    }

    /// Record a write on `graph` — called from the CDC write-path hook
    /// ([`CdcHub::emit`](super::cdc::CdcHub::emit)). THE COST GATE: for a
    /// non-opted-in graph this is one hashset lookup and nothing else — no
    /// lock is taken, no allocation happens, no per-graph entry is created.
    pub fn note_write(&self, graph: &str) {
        if !self.opted_in.contains(graph) {
            return;
        }
        let mut entry = self.graphs.entry(graph.to_string()).or_default();
        entry.last_write_at = Some(Instant::now());
        entry.pending_writes += 1;
    }

    /// Whether `graph` currently has NO cascade bookkeeping at all — a
    /// structural (non-timing) proof that a non-opted-in write leaves zero
    /// trace, used by tests instead of a flaky nanosecond timing assertion.
    pub fn has_no_state(&self, graph: &str) -> bool {
        !self.graphs.contains_key(graph)
    }

    /// The last materialized closure for `graph`, if any refresh has run.
    pub fn last_refresh(&self, graph: &str) -> Option<CascadeRefresh> {
        self.graphs.get(graph).and_then(|e| e.last.clone())
    }

    /// Lifetime refresh count for `graph` (0 if never refreshed / not opted-in).
    pub fn refresh_count(&self, graph: &str) -> u64 {
        self.graphs.get(graph).map(|e| e.refresh_count).unwrap_or(0)
    }

    /// Sweep pass: refresh every opted-in graph whose debounce window has
    /// quietly elapsed since its last write. Returns how many graphs
    /// refreshed. A no-op (zero registry/graph access) when nothing is
    /// opted-in or nothing is due.
    pub async fn refresh_due(&self, state: &Arc<RwLock<ServerState>>) -> usize {
        if !self.is_active() {
            return 0;
        }
        let now = Instant::now();
        let due: Vec<String> = self
            .graphs
            .iter()
            .filter(|kv| {
                kv.pending_writes > 0
                    && kv
                        .last_write_at
                        .is_some_and(|t| now.duration_since(t) >= self.debounce)
            })
            .map(|kv| kv.key().clone())
            .collect();

        let mut refreshed = 0;
        for graph in due {
            if self.refresh_one(state, &graph).await {
                refreshed += 1;
            }
        }
        refreshed
    }

    /// Refresh one graph's closure. `false` when there was nothing to do
    /// (the graph vanished from the registry, or a race left no pending
    /// state — both benign).
    async fn refresh_one(&self, state: &Arc<RwLock<ServerState>>, graph: &str) -> bool {
        let core: Arc<GraphCore> = {
            let guard = state.read().await;
            match guard.registry.get(graph) {
                Some(entry) => entry.core.clone(),
                None => {
                    // The graph was deleted — drop its cascade state too.
                    self.graphs.remove(graph);
                    return false;
                }
            }
        };

        // Take the reasoner + known-triples OUT of the entry (a short,
        // synchronous critical section) so the reasoning pass itself never
        // holds a lock a concurrent `note_write` (for this or any other
        // graph) could block on.
        let (reasoner, known_triples, coalesced) = {
            let Some(mut e) = self.graphs.get_mut(graph) else {
                return false;
            };
            let reasoner = std::mem::take(&mut e.reasoner);
            let known = std::mem::take(&mut e.known_triples);
            let coalesced = e.pending_writes;
            e.pending_writes = 0;
            (reasoner, known, coalesced)
        };

        let view = core.analysis_snapshot();
        let current_vec: Vec<Triple> = tbox_triples_from_view(&view);
        let current_set: HashSet<Triple> = current_vec.iter().cloned().collect();
        let removed = known_triples
            .iter()
            .filter(|t| !current_set.contains(t))
            .count();
        let new_triples: Vec<Triple> = current_vec
            .iter()
            .filter(|t| !known_triples.contains(t))
            .cloned()
            .collect();
        // Only clone the full triple set when a retraction actually forces a
        // from-scratch rebuild (the uncommon path) — the common monotone-growth
        // path never pays this.
        let full_for_rebuild = (removed > 0).then(|| current_vec.clone());

        let prev_pairs = self
            .graphs
            .get(graph)
            .and_then(|e| e.last.as_ref().map(|l| pair_count(&l.classification)))
            .unwrap_or(0);

        // CPU-bound EL⁺/RL fixpoint work off the tokio reactor thread (matches
        // `server::compute::compute_off_lock`'s convention for reasoning/
        // analysis passes — this sweep task must never stall other work).
        let (reasoner, classification, rebuilt) = tokio::task::spawn_blocking(move || {
            match full_for_rebuild {
                Some(full) => {
                    // EL completion is MONOTONE — it cannot retract a subsumer
                    // derived from an axiom that no longer exists, so a
                    // retraction needs a fresh closure over the CURRENT triples
                    // rather than an incremental add. Still just the reasoner's
                    // own two entry points, used as designed.
                    let mut fresh = Reasoner::from_triples(&full);
                    let cls = fresh.classify_weighted();
                    (fresh, cls, true)
                }
                None => {
                    let mut reasoner = reasoner;
                    let cls = reasoner.add_axioms(parse_ontology(&new_triples));
                    (reasoner, cls, false)
                }
            }
        })
        .await
        .expect("reasoning cascade blocking task panicked");

        let closure_delta = pair_count(&classification).saturating_sub(prev_pairs);
        let refreshed_at = Instant::now();

        {
            let mut e = self.graphs.entry(graph.to_string()).or_default();
            e.reasoner = reasoner;
            e.known_triples = current_set;
            e.refresh_count += 1;
            e.last = Some(CascadeRefresh {
                classification,
                refreshed_at,
                coalesced_writes: coalesced,
                closure_delta,
                rebuilt_from_scratch: rebuilt,
            });
        }

        tracing::info!(
            graph,
            debounce_ms = self.debounce.as_millis() as u64,
            closure_delta,
            coalesced_writes = coalesced,
            rebuilt_from_scratch = rebuilt,
            "reasoning cascade: closure refreshed"
        );
        true
    }
}

fn pair_count(cls: &Classification) -> usize {
    cls.subsumers.values().map(|s| s.len()).sum()
}

/// Spawn the periodic debounce-sweep task. A no-op call site (`main.rs`) skips
/// this entirely when `cascade.is_active()` is false — an unarmed deployment
/// gets no background task at all, not merely a cheap idle one.
pub fn spawn(state: Arc<RwLock<ServerState>>, cascade: Arc<ReasoningCascade>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SWEEP_TICK);
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            let started = Instant::now();
            let refreshed = cascade.refresh_due(&state).await;
            if refreshed > 0 {
                tracing::debug!(refreshed, "reasoning cascade: sweep refreshed graph(s)");
            }
            crate::metrics::loop_tick("reasoning_cascade", started.elapsed().as_secs_f64());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use crate::protocol::GraphType;
    use crate::registry::GraphRegistry;
    use dashmap::DashMap as StdDashMap;
    use tokio::sync::Semaphore;

    const SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    // Same named-const convention every other server test module uses for its
    // fake `auth_secret` (e.g. `server::mod::tests::SECRET`,
    // `dispatch::tests::SECRET`) — never inlined directly into the struct
    // literal.
    const SECRET: &str = "reasoning-cascade-test-secret";

    fn edge_props(relationship: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({ "relationship": relationship })).unwrap()
    }

    /// A minimal `ServerState` (no redb/blob/raft/etc.) — the same
    /// bare-bones-literal convention every other server test file uses
    /// locally (see e.g. `server::mod::tests::test_state`).
    fn test_state() -> Arc<RwLock<ServerState>> {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".into(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(StdDashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(StdDashMap::new()),
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
            foreign_sources: Arc::new(StdDashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    /// Create `graph` and return a handle to its live `GraphCore` so the test
    /// can mutate axioms directly (mirrors how a real write lands: node/edge
    /// mutation on the SAME core the registry hands the cascade at refresh
    /// time).
    async fn create_graph(state: &Arc<RwLock<ServerState>>, graph: &str) -> Arc<GraphCore> {
        let mut guard = state.write().await;
        guard
            .registry
            .create_graph(graph, GraphType::Commons, None)
            .expect("create test graph");
        guard
            .registry
            .get(graph)
            .expect("graph just created")
            .core
            .clone()
    }

    fn add_class_node(core: &GraphCore, iri: &str) {
        core.add_node(
            iri.to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        );
    }

    fn add_subclass_edge(core: &GraphCore, sub: &str, sup: &str) {
        core.add_edge(sub.to_string(), sup.to_string(), edge_props(SUBCLASS_OF))
            .expect("add subClassOf edge");
    }

    // ── Pure config parsing ──────────────────────────────────────────────

    #[test]
    fn parses_comma_separated_graph_list_trimming_and_dropping_empties() {
        let set = parse_opted_in_graphs(" g1, g2 ,,g3");
        assert_eq!(
            set,
            ["g1", "g2", "g3"].into_iter().map(String::from).collect()
        );
    }

    #[test]
    fn empty_or_unset_reason_on_write_yields_an_empty_set() {
        assert!(parse_opted_in_graphs("").is_empty());
        assert!(parse_opted_in_graphs("   ").is_empty());
    }

    #[test]
    fn debounce_ms_defaults_on_absent_zero_or_invalid() {
        assert_eq!(
            parse_debounce_ms(None),
            Duration::from_millis(DEFAULT_DEBOUNCE_MS)
        );
        assert_eq!(
            parse_debounce_ms(Some("0")),
            Duration::from_millis(DEFAULT_DEBOUNCE_MS)
        );
        assert_eq!(
            parse_debounce_ms(Some("not-a-number")),
            Duration::from_millis(DEFAULT_DEBOUNCE_MS)
        );
        assert_eq!(parse_debounce_ms(Some("250")), Duration::from_millis(250));
    }

    // ── Cost gate: non-opted-in graphs ───────────────────────────────────

    #[test]
    fn note_write_on_a_non_opted_in_graph_leaves_zero_cascade_state() {
        let cascade = ReasoningCascade::new(
            ["g-opted".to_string()].into_iter().collect(),
            Duration::from_millis(50),
        );
        for _ in 0..1000 {
            cascade.note_write("g-other");
        }
        assert!(
            cascade.has_no_state("g-other"),
            "a non-opted-in graph must accumulate NO cascade bookkeeping at all"
        );
        assert_eq!(cascade.refresh_count("g-other"), 0);
    }

    #[test]
    fn note_write_on_a_non_opted_in_graph_is_cheap_at_scale() {
        // Not a hard perf gate (shared/contended CI boxes) — a generous smoke
        // check that the miss path is O(1) with no pathological cost (lock,
        // allocation, scan). 100k calls well under 200ms is >100x below the
        // measured ~187us p50 AddNode write latency (docs/benchmarks.md).
        let cascade = ReasoningCascade::new(HashSet::new(), Duration::from_millis(50));
        let started = Instant::now();
        for _ in 0..100_000 {
            cascade.note_write("never-opted-in");
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "100k note_write misses took {elapsed:?}, expected well under 200ms"
        );
    }

    // ── Debounce coalesces a burst into ONE refresh ──────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn a_burst_of_writes_coalesces_into_one_refresh() {
        let state = test_state();
        let core = create_graph(&state, "g-burst").await;
        add_class_node(&core, "<http://ex/A>");
        add_class_node(&core, "<http://ex/B>");
        add_subclass_edge(&core, "<http://ex/A>", "<http://ex/B>");

        let cascade = ReasoningCascade::new(
            ["g-burst".to_string()].into_iter().collect(),
            Duration::from_millis(40),
        );

        // A burst: several writes land back-to-back, well inside the debounce
        // window.
        for _ in 0..5 {
            cascade.note_write("g-burst");
        }

        // Immediately: nothing due yet (still inside the window).
        assert_eq!(cascade.refresh_due(&state).await, 0);

        // Past the debounce window: exactly one graph refreshes...
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cascade.refresh_due(&state).await, 1);
        // ...and it coalesced the whole 5-write burst into that one pass.
        let refresh = cascade.last_refresh("g-burst").expect("a refresh ran");
        assert_eq!(refresh.coalesced_writes, 5);
        assert_eq!(cascade.refresh_count("g-burst"), 1);

        // Nothing pending -> the next sweep is a no-op.
        assert_eq!(cascade.refresh_due(&state).await, 0);
        assert_eq!(cascade.refresh_count("g-burst"), 1);
    }

    // ── The opted-in refresh proof: a new inference appears ──────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn opted_in_graph_axiom_write_makes_a_new_inference_appear_after_debounce() {
        let state = test_state();
        let a = "<http://ex/A>";
        let b = "<http://ex/B>";
        let c = "<http://ex/C>";
        let core = create_graph(&state, "g-seed").await;
        add_class_node(&core, a);
        add_class_node(&core, b);
        add_class_node(&core, c);
        add_subclass_edge(&core, a, b); // A ⊑ B only, to start.

        let cascade = ReasoningCascade::new(
            ["g-seed".to_string()].into_iter().collect(),
            Duration::from_millis(40),
        );

        cascade.note_write("g-seed");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cascade.refresh_due(&state).await, 1);

        let first = cascade.last_refresh("g-seed").expect("first refresh ran");
        assert!(
            first.classification.entails_subclass(a, b),
            "A subClassOf B must already be entailed"
        );
        assert!(
            !first.classification.entails_subclass(a, c),
            "A subClassOf C must NOT be entailed yet (B subClassOf C not written)"
        );
        assert!(
            !first.rebuilt_from_scratch,
            "pure growth uses the incremental delta re-seed"
        );

        // The opted-in axiom write: B ⊑ C.
        add_subclass_edge(&core, b, c);
        cascade.note_write("g-seed");

        // Not yet due (still inside the debounce window).
        assert_eq!(cascade.refresh_due(&state).await, 0);
        assert!(
            !cascade
                .last_refresh("g-seed")
                .unwrap()
                .classification
                .entails_subclass(a, c),
            "the cached closure must not change before the debounce window elapses"
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cascade.refresh_due(&state).await, 1);

        // THE PROOF: the new axiom's transitive consequence now appears.
        let second = cascade.last_refresh("g-seed").expect("second refresh ran");
        assert!(
            second.classification.entails_subclass(a, c),
            "A subClassOf C must now be entailed: the CDC-triggered refresh picked up \
             the new axiom and re-derived the transitive closure"
        );
        assert!(
            second.closure_delta > 0,
            "the closure grew: the new inference is reflected in the delta count"
        );
        assert_eq!(cascade.refresh_count("g-seed"), 2);
    }

    // ── A pure retraction still yields a sound (rebuilt) closure ─────────

    #[tokio::test(flavor = "multi_thread")]
    async fn a_retraction_rebuilds_instead_of_serving_a_stale_entailment() {
        let state = test_state();
        let a = "<http://ex/A>";
        let b = "<http://ex/B>";
        let c = "<http://ex/C>";
        let core = create_graph(&state, "g-retract").await;
        add_class_node(&core, a);
        add_class_node(&core, b);
        add_class_node(&core, c);
        add_subclass_edge(&core, a, b);
        add_subclass_edge(&core, b, c);

        let cascade = ReasoningCascade::new(
            ["g-retract".to_string()].into_iter().collect(),
            Duration::from_millis(40),
        );
        cascade.note_write("g-retract");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cascade.refresh_due(&state).await, 1);
        assert!(cascade
            .last_refresh("g-retract")
            .unwrap()
            .classification
            .entails_subclass(a, c));

        // Retract B ⊑ C.
        core.remove_edge(b.to_string(), c.to_string());
        cascade.note_write("g-retract");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cascade.refresh_due(&state).await, 1);

        let after = cascade.last_refresh("g-retract").unwrap();
        assert!(
            after.rebuilt_from_scratch,
            "a retraction must force a from-scratch rebuild, not an incremental add"
        );
        assert!(
            !after.classification.entails_subclass(a, c),
            "the retracted axiom's consequence must be GONE, not served stale"
        );
    }
}
