//! The fused executor (CONCEPT:AU-KG.compute.vector) — sequences the EXISTING legs into ONE
//! pipeline over a single off-lock snapshot. Each operator takes the previous
//! [`RowSet`] and returns the next:
//!
//! * `Scan` — label scan over the `GraphView` property blobs (dep-free).
//! * `Filter` — **real DataFusion** via `eg_query::exec_sql` over the schema-on-read
//!   `nodes` provider. DataFusion stays the relational sub-engine; this op SEQUENCES
//!   it, it does not reimplement SQL.
//! * `Traverse` — petgraph BFS over the `GraphView` topology (variable-length hops,
//!   matching eg-query/cypher's `rel_matches`), dep-free.
//! * `Rank` — the vector kNN of the `SemanticStore` (`semantic_search`).
//! * `Limit` — order-respecting top-k.
//!
//! The intermediate is always a `RowSet` (ids + optional scores) — the cross-modal
//! currency — so the legs compose with no impedance mismatch beyond "Arrow id column
//! → ids" at the relational boundary. The whole module is gated behind `query`
//! because the FILTER leg needs DataFusion; without it, the algebra/cost/IR still
//! compile dep-free (the Pi contract).

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphView;

use crate::algebra::{Op, Plan, Pred};
use crate::rowset::RowSet;
use eg_types::wire::TimeAxis;

/// Everything an operator might touch, gathered from ONE consistent snapshot. In a
/// handler this is exactly what is already available off-lock: the `GraphView`
/// (topology + property blobs) and a `SemanticStore` clone, both taken at one
/// `GraphCore::version()` — so the cross-modal read is snapshot-isolated for free
/// (CONCEPT:EG-KG.txn.multi-op-occ-acid).
pub struct PlanCtx<'a> {
    pub view: &'a GraphView,
    pub semantic: &'a SemanticStore,
    /// The lexical BM25 search surface for the `RankText` / `FuseRrf` ops
    /// (CONCEPT:AU-KG.query.text-spatial-time / CONCEPT:EG-KG.query.served-text-index-binding). `None` when no text
    /// index is configured — a `RankText` then yields no hits (the plan degrades,
    /// never errs), exactly as an absent embedding does for `Rank`. A TRAIT OBJECT
    /// (not a concrete `eg_text::TextIndex`) so a facade can bind EITHER a
    /// snapshot-derived index built straight from the queried `GraphView` OR an
    /// adapter that reaches the server's MAINTAINED persistent index behind a lock
    /// (see [`TextSource`]'s docs) — the planner does not care which. Gated behind
    /// `text`, so a non-text build's `PlanCtx` is unchanged.
    #[cfg(feature = "text")]
    pub text: Option<&'a dyn TextSource>,
    /// The WASM UDF registry for the `Udf { id }` op (CONCEPT:EG-KG.query.rowset-execution). `None` when no
    /// registry is attached — a `Udf` op then errs (a UDF must be registered to run).
    /// Gated behind `wasm-udf`, so a non-wasm build's `PlanCtx` is unchanged.
    #[cfg(feature = "wasm-udf")]
    pub udf: Option<&'a eg_wasm::UdfRegistry>,
    /// The federation foreign-source registry backing `Op::Foreign` (the UQL
    /// `FOREIGN "<name>"` marker) and a `Named` `Op::ForeignScan` (CONCEPT:EG-KG.query.closure-backed-source).
    /// `None` when no foreign sources are registered — the name-resolving ops then keep
    /// their prior behavior (`Op::Foreign` passes its input through unchanged), so a
    /// default ctx is byte-for-byte the old one. Gated behind `federation`, so a
    /// non-federation build's `PlanCtx` is unchanged.
    #[cfg(feature = "federation")]
    pub foreign: Option<&'a crate::federation::ForeignSourceRegistry>,
    /// CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — the content-addressed [`eg_tensor::TensorStore`] into which the
    /// tensor executor WRITES BACK every derived tensor an `Op::TensorOp` produces, so a
    /// derived tensor becomes a durable, dedup-shared CAS blob addressable by its
    /// deterministic content hash — the EG-085 CAS-write-back follow-up that v1 left
    /// documented-but-unbuilt. `None` (the default) preserves today's validate-only
    /// behavior byte-for-byte: no write-back, fully backward-compatible. Held behind a
    /// [`std::sync::Mutex`] so the write-back is interior-mutable through the shared
    /// `&PlanCtx` the executor threads, while `PlanCtx` itself stays `Send + Sync`. An
    /// identical derived tensor content-addresses to the SAME blob, so re-running a plan
    /// (or many rows yielding the same result) DEDUPS to one blob. Gated behind `tensor`,
    /// so a non-tensor build's `PlanCtx` is unchanged.
    #[cfg(feature = "tensor")]
    pub tensor_store: Option<&'a std::sync::Mutex<eg_tensor::TensorStore>>,
    /// CONCEPT:EG-KG.query.native-time-series — the native time-series [`eg_tsdb::store::SeriesStore`] backing the
    /// `Op::TsScan` SOURCE op, so a plan can seed its RowSet from tsdb series and fuse the
    /// time-series leg with the graph/vector/relational legs in ONE plan (tsdb-in-plan
    /// fusion). `None` (the default) makes a `TsScan` yield no rows — the plan degrades,
    /// never errs, exactly as an absent embedding does for `Rank` or an absent text index
    /// for `RankText`. A server/facade owns the store and threads it in via
    /// [`Self::with_tsdb`]; Lane A's `run_unified` reconcile calls it. Gated behind
    /// `timeseries` (the eg-plan→eg-tsdb `redb-store` edge), so a non-timeseries build's
    /// `PlanCtx` is unchanged.
    #[cfg(feature = "timeseries")]
    pub tsdb: Option<&'a eg_tsdb::store::SeriesStore>,
    /// CONCEPT:EG-KG.query.txn-tsdb-read-your — an in-memory STAGED-series overlay consulted by `Op::TsScan`
    /// BEFORE the committed [`Self::tsdb`] store, so an in-txn UQL reading a series sees
    /// the transaction's OWN uncommitted staged points (read-your-own-writes). `None` (the
    /// default) makes a `TsScan` read committed series only — byte-for-byte the prior
    /// behavior. Lane A's `run_unified_overlaid` builds this from the resolved txn's staged
    /// `GraphTxnState.measurements` and threads it in via [`Self::with_staged_series`].
    /// Gated behind `timeseries` (same edge as `tsdb`), so a non-timeseries build's
    /// `PlanCtx` is unchanged.
    #[cfg(feature = "timeseries")]
    pub staged_series: Option<&'a StagedSeries>,
    /// CONCEPT:EG-KG.compute.no-embedder-bound-op — the server-side text→vector embedder backing the `Op::RankEmbed`
    /// op (the UQL `RANK BY ~ "text"` NL→vector seam). Resolves a natural-language query
    /// string to a query vector at exec time, which is then kNN-ranked over `semantic`
    /// exactly like a literal-vector `Op::Rank`. `None` (the default) makes an
    /// `Op::RankEmbed` a clean typed error — the documented "no embedder bound" behavior —
    /// while every other op is unaffected, so a default ctx is byte-for-byte the old one.
    /// The facade owns the model and threads it in via [`Self::with_embedder`]. Dep-free
    /// (a trait object), so no feature gate: a plan that never uses `Op::RankEmbed` is
    /// unchanged whether or not an embedder is bound.
    pub embedder: Option<&'a dyn TextEmbedder>,
    /// CONCEPT:EG-KG.query.reason-decay-in-plan — the wall-clock `(now, default_half_life)`
    /// that makes an `Op::Reason` compute TIME-DECAYED OWL confidence IN-PLAN, so a single
    /// fused plan can BOTH bi-temporal `AsOf`-reselect liveness AND Ebbinghaus-decay-reweight
    /// confidence (the [`crate::exec`] reason path threads it into
    /// `eg_rdf::owl::asserted_types_with_confidence_from_view`/`instances_of_weighted`). `None`
    /// (the default) keeps `Op::Reason` DECAY-NEUTRAL (`now = 0`, `half_life = 0` ⇒ recency
    /// weight `1.0`) — byte-for-byte the prior leaf-stable `Reason`, so a bare `Reason` stays a
    /// deterministic source and every existing plan is unchanged. Bound via [`Self::with_decay`].
    /// Gated behind `owl` (only the reasoner op consumes it), so a non-owl build's `PlanCtx` is
    /// unchanged. `now`/`half_life` share a unit (typically epoch seconds, the unit graph-node
    /// `last_access`/`valid_from` use); a per-node `half_life` blob field overrides the default.
    #[cfg(feature = "owl")]
    pub decay: Option<(u64, f64)>,
    /// CONCEPT:EG-KG.epistemic.epistemic-substrate — the confidence-weighting
    /// [`eg_epistemic::AuthorityPolicy`] the `Op::{ConfidenceOp,SourceReliability,BeliefAsOf,
    /// ExplainBelief}` ops apply during propagation (source reliability multiplier / attack
    /// discount / prior strength). `None` (the default) uses `AuthorityPolicy::default()`, so
    /// a default ctx behaves exactly as an explicit default policy would. A tenant-specific
    /// policy is never persisted on the graph — the facade threads one in per request via
    /// [`Self::with_belief_policy`]. Gated behind `epistemic`, so a non-epistemic build's
    /// `PlanCtx` is unchanged.
    #[cfg(feature = "epistemic")]
    pub belief_policy: Option<eg_epistemic::AuthorityPolicy>,
    /// The persistent spatial-index search surface for `Op::SpatialScan` (CONCEPT:EG-KG.storage.incremental-spatial,
    /// L37 — mirrors [`Self::text`]'s served-index-binding shape). `None` (the default)
    /// makes `Op::SpatialScan` fall back to its prior v1 behavior: build an ephemeral
    /// packed Hilbert R-tree from the queried snapshot on EVERY call (see
    /// [`spatial_scan`]). `Some` pushes the bbox query DOWN into a facade adapter that
    /// reaches a MAINTAINED index living behind a lock elsewhere (e.g. the server's
    /// per-graph `GraphSpatialIndex`, kept incrementally up to date by the write
    /// coalescer), so a served spatial scan does not rebuild the tree per query. A TRAIT
    /// OBJECT (not a concrete type) for the SAME reason `text` is: the planner does not
    /// care whether the bound index is snapshot-derived or server-maintained. Gated
    /// behind `geo`, so a non-geo build's `PlanCtx` is unchanged.
    #[cfg(feature = "geo")]
    pub spatial: Option<&'a dyn SpatialSource>,
}

/// The lexical BM25 search surface [`PlanCtx::text`] binds (CONCEPT:EG-KG.query.served-text-index-binding) —
/// abstracts OVER a concrete `eg_text::TextIndex` so the executor's `RankText`/`FuseRrf`
/// legs work identically whether the bound index is:
///   * a plain `eg_text::TextIndex` (an ephemeral snapshot-derived index, or an
///     in-process test fixture) — searched directly, `&self`, no locking needed; or
///   * a facade adapter that reaches a MAINTAINED, persistent index living behind a
///     lock/`Arc` elsewhere (e.g. the server's per-graph `GraphTextIndex`, which is
///     kept incrementally up to date by the write-coalescer) — the adapter takes
///     its own lock internally, per call, and returns owned hits.
///
/// This is what lets a served query push the lexical leg DOWN into the already-
/// maintained index instead of paying a snapshot-rebuild on every request — the
/// same shape [`crate::knowledge_batch`]'s Arrow projection and the vector `Rank`
/// leg's `SemanticStore` guard-borrow use elsewhere in this workstream (bind the
/// LIVE structure, don't clone/rebuild it per query).
#[cfg(feature = "text")]
pub trait TextSource: Send + Sync {
    /// BM25 top-`k` for `query` — same contract as `eg_text::TextIndex::search`:
    /// `(id, bm25_score)` descending, empty on an empty/unparsable query.
    fn search(&self, query: &str, k: usize) -> Vec<eg_text::TextHit>;
}

#[cfg(feature = "text")]
impl TextSource for eg_text::TextIndex {
    fn search(&self, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        eg_text::TextIndex::search(self, query, k)
    }
}

/// The spatial bbox search surface [`PlanCtx::spatial`] binds (CONCEPT:EG-KG.storage.incremental-spatial, L37) —
/// mirrors [`TextSource`]'s shape: abstracts OVER the concrete backing so `Op::SpatialScan`
/// works identically whether the bound index is a facade adapter over the server's
/// MAINTAINED per-graph `GraphSpatialIndex` (kept incrementally up to date by the write
/// coalescer, never rebuilt per query) or — via `spatial_scan`'s OWN fallback when
/// `PlanCtx::spatial` is `None` — an ephemeral snapshot-derived R-tree.
#[cfg(feature = "geo")]
pub trait SpatialSource: Send + Sync {
    /// Every node id in `layer` (the conventional `type` property, matching
    /// [`scan_label`]'s convention) whose maintained bounding box intersects `bbox`.
    /// Empty when `layer` is unknown to this index (never an error — degrades exactly
    /// like an absent index would).
    fn query_bbox(&self, layer: &str, bbox: [f64; 4]) -> Vec<String>;
}

/// A server-side text→vector embedder (CONCEPT:EG-KG.compute.no-embedder-bound-op) — the seam that resolves a UQL
/// `RANK BY ~ "text"` query string to a query vector at plan-exec time, so a caller need
/// not pre-embed client-side and pass a literal `~[…]` vector. A facade binds a concrete
/// implementation (an ONNX/remote embedding-service model that produces vectors in the
/// SAME space the graph's stored embeddings live in) onto the [`PlanCtx`] via
/// [`PlanCtx::with_embedder`]; the executor calls [`TextEmbedder::embed`] for each
/// `Op::RankEmbed`. `Send + Sync` so the shared `&dyn TextEmbedder` threads through the
/// off-lock plan the executor runs on the blocking pool.
pub trait TextEmbedder: Send + Sync {
    /// Resolve `text` to a query vector (in the graph's embedding space). An `Err`
    /// (e.g. an unreachable model service) surfaces as a clean typed plan error — never a
    /// panic.
    fn embed(&self, text: &str) -> Result<Vec<f32>, String>;
}

/// A deterministic, dependency-free [`TextEmbedder`] fallback (CONCEPT:EG-KG.compute.no-embedder-bound-op) for tests
/// and offline use: it hashes `text` into a fixed-dimension unit-norm vector, so the same
/// text always yields the same vector and two different texts (almost always) yield
/// different ones. It carries NO semantic meaning — it exists to exercise the resolver
/// seam end-to-end without a real model, and to be the documented drop-in a facade
/// swaps for a real embedding model. `dim` defaults to a small, cheap width.
#[derive(Clone, Copy, Debug)]
pub struct HashEmbedder {
    dim: usize,
}

impl Default for HashEmbedder {
    fn default() -> Self {
        Self { dim: 8 }
    }
}

impl HashEmbedder {
    /// A hash embedder producing `dim`-dimensional vectors (`dim` clamped to ≥ 1).
    pub fn new(dim: usize) -> Self {
        Self { dim: dim.max(1) }
    }
}

impl TextEmbedder for HashEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        use std::hash::{Hash, Hasher};
        // One independent component per dimension: hash (text, dim_index) with a distinct
        // seed so components are decorrelated, map to [-1, 1], then L2-normalize.
        let mut v: Vec<f32> = (0..self.dim)
            .map(|d| {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                (d as u64).hash(&mut h);
                text.hash(&mut h);
                // u64 → [-1, 1]
                (h.finish() as f64 / u64::MAX as f64) as f32 * 2.0 - 1.0
            })
            .collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        Ok(v)
    }
}

/// An in-memory overlay of a transaction's STAGED, uncommitted time-series points
/// (CONCEPT:EG-KG.query.txn-tsdb-read-your) — the in-txn tsdb read-your-own-writes source. The native
/// [`eg_tsdb::store::SeriesStore`] is redb-file-backed with no in-memory overlay, so an
/// in-txn `Op::TsScan` cannot see the txn's own staged `measurements` through it; this
/// dep-free map (series id → its staged `(ts_ns, field_values)` points) is consulted
/// alongside the committed store and MERGED so the txn reads its own writes while an
/// off-txn read (no overlay attached) sees committed only. Points are stored verbatim
/// as staged (`i64` nanoseconds, matching `GraphTxnState.measurements`); the scan trims
/// them to the requested window and takes the first field value, exactly as the
/// committed-store path does.
#[cfg(feature = "timeseries")]
#[derive(Debug, Default, Clone)]
pub struct StagedSeries {
    series: std::collections::HashMap<String, Vec<(i64, Vec<f64>)>>,
}

#[cfg(feature = "timeseries")]
impl StagedSeries {
    /// An empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a batch of `(ts_ns, field_values)` points for `series` (appended — a series
    /// may be staged across several `INSERT INTO series …` in one txn).
    pub fn push_points(&mut self, series: &str, points: impl IntoIterator<Item = (i64, Vec<f64>)>) {
        self.series
            .entry(series.to_string())
            .or_default()
            .extend(points);
    }

    /// True when nothing is staged (the executor then behaves as if no overlay were
    /// attached).
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// The staged points of `series` within `[from_ns, to_ns)`, as `(ts, first_value)` —
    /// the SAME `(id=ts, score=value)` shape `tsdb_scan_op` emits for the committed store.
    fn range(&self, series: &str, from_ns: i64, to_ns: i64) -> Vec<(i64, f32)> {
        self.series
            .get(series)
            .map(|pts| {
                pts.iter()
                    .filter(|(ts, _)| *ts >= from_ns && *ts < to_ns)
                    .filter_map(|(ts, vals)| vals.first().map(|&v| (*ts, v as f32)))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl<'a> PlanCtx<'a> {
    /// Construct a ctx with NO text index (the common case + every non-text plan).
    /// Keeps call-sites (and existing tests) feature-agnostic: the `text` field, when
    /// the feature is on, defaults to `None`. Use [`Self::with_text`] to attach one.
    pub fn new(view: &'a GraphView, semantic: &'a SemanticStore) -> Self {
        Self {
            view,
            semantic,
            #[cfg(feature = "text")]
            text: None,
            #[cfg(feature = "wasm-udf")]
            udf: None,
            #[cfg(feature = "federation")]
            foreign: None,
            #[cfg(feature = "tensor")]
            tensor_store: None,
            #[cfg(feature = "timeseries")]
            tsdb: None,
            #[cfg(feature = "timeseries")]
            staged_series: None,
            embedder: None,
            #[cfg(feature = "owl")]
            decay: None,
            #[cfg(feature = "epistemic")]
            belief_policy: None,
            #[cfg(feature = "geo")]
            spatial: None,
        }
    }

    /// Attach a server-side text→vector [`TextEmbedder`] so an `Op::RankEmbed` (the UQL
    /// `RANK BY ~ "text"` clause) resolves its query string to a vector at exec time
    /// (CONCEPT:EG-KG.compute.no-embedder-bound-op). Without this call an `Op::RankEmbed` is a clean typed error, so a
    /// default ctx is byte-for-byte the old one.
    pub fn with_embedder(mut self, embedder: &'a dyn TextEmbedder) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Attach a lexical BM25 [`TextSource`] so `RankText` / `FuseRrf` ops can run —
    /// either a plain `eg_text::TextIndex` or a facade adapter over a maintained
    /// persistent index (CONCEPT:EG-KG.query.served-text-index-binding).
    #[cfg(feature = "text")]
    pub fn with_text(mut self, text: &'a dyn TextSource) -> Self {
        self.text = Some(text);
        self
    }

    /// Attach a persistent [`SpatialSource`] so `Op::SpatialScan` pushes its bbox query
    /// down into a MAINTAINED index (CONCEPT:EG-KG.storage.incremental-spatial, L37) instead of
    /// `spatial_scan`'s per-call ephemeral R-tree build. Without this call a `SpatialScan`
    /// keeps that prior behavior, so a default ctx is byte-for-byte the old one.
    #[cfg(feature = "geo")]
    pub fn with_spatial(mut self, spatial: &'a dyn SpatialSource) -> Self {
        self.spatial = Some(spatial);
        self
    }

    /// Attach a `(now, default_half_life)` decay context so an `Op::Reason` computes
    /// TIME-DECAYED OWL confidence IN-PLAN (CONCEPT:EG-KG.query.reason-decay-in-plan) — the
    /// same fused plan can then reason with Ebbinghaus-decayed confidence ALONGSIDE a
    /// bi-temporal `Op::AsOf` liveness filter. Without this call an `Op::Reason` runs
    /// decay-neutral (`now = 0`, `half_life = 0`), so a default ctx is byte-for-byte the old
    /// one. The SAME plan executed under two different `now` values yields different decayed
    /// `Reason` scores — decay becomes a first-class in-plan signal, not an out-of-band
    /// reasoner-surface step. A per-node `half_life` property overrides `default_half_life`.
    #[cfg(feature = "owl")]
    pub fn with_decay(mut self, now: u64, default_half_life: f64) -> Self {
        self.decay = Some((now, default_half_life));
        self
    }

    /// Attach a tenant-specific [`eg_epistemic::AuthorityPolicy`] for the epistemic ops
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). Without this call they run under
    /// `AuthorityPolicy::default()`, so a default ctx is byte-for-byte the old one.
    #[cfg(feature = "epistemic")]
    pub fn with_belief_policy(mut self, policy: eg_epistemic::AuthorityPolicy) -> Self {
        self.belief_policy = Some(policy);
        self
    }

    /// Attach a WASM UDF registry so a `Udf { id }` op can run a sandboxed function.
    #[cfg(feature = "wasm-udf")]
    pub fn with_udf(mut self, udf: &'a eg_wasm::UdfRegistry) -> Self {
        self.udf = Some(udf);
        self
    }

    /// Attach a foreign-source registry so `Op::Foreign` / a `Named` `Op::ForeignScan`
    /// resolve their name → rows through it (CONCEPT:EG-KG.query.closure-backed-source). A server/facade builds the
    /// registry once at setup and threads it in here — the library seam that keeps the
    /// name→source binding out of the wire DTO.
    #[cfg(feature = "federation")]
    pub fn with_foreign(mut self, registry: &'a crate::federation::ForeignSourceRegistry) -> Self {
        self.foreign = Some(registry);
        self
    }

    /// Attach a content-addressed [`eg_tensor::TensorStore`] so the tensor executor
    /// WRITES BACK each derived tensor an `Op::TensorOp` produces into the CAS
    /// (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink). A server/facade owns the store (behind a [`std::sync::Mutex`])
    /// and can later `persist` it to disk for durability; without this call the executor
    /// keeps its prior validate-only behavior (no write-back), so a default ctx is
    /// byte-for-byte the old one.
    #[cfg(feature = "tensor")]
    pub fn with_tensor_store(
        mut self,
        store: &'a std::sync::Mutex<eg_tensor::TensorStore>,
    ) -> Self {
        self.tensor_store = Some(store);
        self
    }

    /// Attach a native time-series [`eg_tsdb::store::SeriesStore`] so an `Op::TsScan` op
    /// resolves its series → `(ts, value)` rows through it (CONCEPT:EG-KG.query.native-time-series). A
    /// server/facade opens the store once at setup and threads it in here — the library
    /// seam that keeps the store binding out of the wire DTO. Without this call a
    /// `TsScan` yields no rows (degrade, never err), so a default ctx is byte-for-byte
    /// the old one.
    #[cfg(feature = "timeseries")]
    pub fn with_tsdb(mut self, store: &'a eg_tsdb::store::SeriesStore) -> Self {
        self.tsdb = Some(store);
        self
    }

    /// Attach a [`StagedSeries`] overlay so an `Op::TsScan` reads a transaction's OWN
    /// staged, uncommitted time-series points BEFORE/merged-with the committed store
    /// (CONCEPT:EG-KG.query.txn-tsdb-read-your — in-txn tsdb read-your-own-writes). Without this call a `TsScan`
    /// reads committed series only, so a default ctx is byte-for-byte the old one.
    #[cfg(feature = "timeseries")]
    pub fn with_staged_series(mut self, staged: &'a StagedSeries) -> Self {
        self.staged_series = Some(staged);
        self
    }
}

/// Execute a [`Plan`] over `ctx`, returning the final `RowSet`. A free function (not
/// an inherent method) because `Plan` is the wire DTO defined in eg-types — the
/// orphan rule forbids an `impl Plan` here; behavior attaches to the foreign type
/// from this crate this way. See [`crate::PlanExt`] for the `plan.execute(&ctx)`
/// ergonomic form.
///
/// Runs the plan in TWO seamed phases so the cost optimizer (Lane A) and the physical
/// runtime (Lane B) can hang off ONE stable pipeline without re-touching the per-op
/// arms:
///  1. [`plan_optimize`] rewrites the logical plan (identity in this tier — Lane A fills
///     the body). A gets an OWNED plan to rewrite, hence the [`Plan::clone`].
///  2. [`crate::runtime::execute_ops`] dispatches the (possibly-rewritten) op list to a
///     physical [`Driver`] chosen from the environment (CONCEPT:EG-KG.query.parallel-runtime):
///     the Lane-0 [`SerialDriver`] by default (byte-for-byte the prior fold — the Pi
///     contract), or Lane B's rayon-morsel `ParallelDriver` when the opt-in `par-runtime`
///     feature is built and the environment asks for it.
pub fn execute(plan: &Plan, ctx: &PlanCtx) -> Result<RowSet, String> {
    let optimized = plan_optimize(plan.clone(), ctx);
    crate::runtime::execute_ops(&optimized.ops, ctx)
}

/// The logical-plan OPTIMIZATION seam (CONCEPT:EG-KG.query.plan-optimize-seam) — the single
/// point where a cost-based rewrite (Lane A's cross-modal optimizer) transforms a logical
/// [`Plan`] into a cheaper-but-equivalent one BEFORE execution. In this foundation tier it
/// is an IDENTITY passthrough: the plan runs exactly as lowered, so [`execute`] is
/// byte-for-byte the prior fold. Lane A fills the body (reordering `Filter`/`AsOf` ahead
/// of `Rank`, fusing RRF branches, …) WITHOUT re-touching the per-op arms or [`execute`];
/// the differential oracle proves the rewritten plan returns the same result set.
pub fn plan_optimize(plan: Plan, ctx: &PlanCtx) -> Plan {
    // Lane A fills the seam: the cross-modal cost optimizer rewrites the logical plan into a
    // cheaper-but-equivalent one (CONCEPT:EG-KG.query.xmodal-cost-optimizer). The runtime
    // kill-switch `EPISTEMIC_GRAPH_COST_OPT=0` short-circuits back to the Lane-0 identity
    // passthrough — byte-for-byte the pre-optimizer fold — leaving `execute` unchanged.
    if crate::optimizer::enabled() {
        crate::optimizer::optimize(&plan, ctx)
    } else {
        plan
    }
}

/// The physical EXECUTION-driver seam (CONCEPT:EG-KG.query.exec-driver-seam) — the trait a
/// plan's op list runs THROUGH, so the scheduling/materialization strategy is swappable
/// without touching either the per-op arms ([`apply`]) or [`execute`]. Lane B replaces the
/// [`SerialDriver`] impl with a parallel (rayon-morsel, memory-accounted, spilling) driver
/// behind this SAME trait; the physical per-op fns stay modality-specific.
pub trait Driver {
    /// Run `ops` in sequence over one [`PlanCtx`], threading each op's output [`RowSet`]
    /// into the next, and return the final `RowSet`.
    fn run(&self, ops: &[Op], ctx: &PlanCtx) -> Result<RowSet, String>;
}

/// The serial driver (CONCEPT:EG-KG.query.exec-driver-seam) — reproduces today's execution
/// EXACTLY when the runtime feedback loop finds nothing to correct: a left fold that seeds
/// an empty [`RowSet`] and applies each op in order via [`apply`].
///
/// **Adaptive re-optimization is auto-wired here (CONCEPT:EG-KG.query.adaptive-reoptimization), not
/// opt-in.** After EVERY op the driver compares the RowSet's ACTUAL output length against
/// the plan-time [`crate::cost::ModalityCardinality`] estimate for that op, and — when they
/// diverge by more than [`crate::optimizer::ADAPTIVE_REOPT_THRESHOLD`] — hands the
/// not-yet-executed tail to [`crate::optimizer::reoptimize_remaining`], which re-costs (and,
/// if beneficial, re-orders) it from the CORRECTED actual cardinality before it runs. Below
/// the threshold `reoptimize_remaining` is a no-op clone, so ordinary execution (the common
/// case — plan-time estimates are usually close enough) is byte-for-byte the prior left
/// fold; the differential oracle (`tests/differential_oracle.rs`) covers this driver, so the
/// existing answer-preserving proof applies to the auto-wired path too (`reoptimize_remaining`
/// reuses the SAME `best_permutation`/EG-405-safe machinery `optimize()` already used at
/// plan time — see that fn's docs).
///
/// Gating: like `plan_optimize` itself, this is active by default and driven off the SAME
/// `EPISTEMIC_GRAPH_COST_OPT` kill-switch ([`crate::optimizer::enabled`]) — a caller who
/// disabled the plan-time cost optimizer gets its runtime-feedback half disabled too
/// (`EPISTEMIC_GRAPH_COST_OPT=0` ⇒ the plain left fold, no per-op cardinality bookkeeping).
/// [`crate::runtime::ParallelDriver`] (the opt-in `par-runtime` feature) runs this SAME loop
/// too (CONCEPT:EG-KG.query.adaptive-reoptimization, L35 — both drivers now call
/// [`run_with_adaptive_reopt`], differing only in how ONE op is applied: this driver's `step`
/// is the plain serial [`apply`]; the parallel driver's morsel-splits a row-parallel op's
/// input across the rayon pool first (`crate::runtime::exec_op`) and memory-accounts the
/// result (`crate::runtime::spill_if_needed`) — see that module's docs for why "the actual
/// cardinality after op i" is still a single scalar there (the loop iterates over WHOLE ops,
/// never morsels, regardless of which driver runs them).
pub struct SerialDriver;

impl Driver for SerialDriver {
    fn run(&self, ops: &[Op], ctx: &PlanCtx) -> Result<RowSet, String> {
        run_with_adaptive_reopt(ops, ctx, apply)
    }
}

/// The adaptive-reoptimization runtime-feedback loop (CONCEPT:EG-KG.query.adaptive-reoptimization) —
/// factored out of [`SerialDriver::run`] so [`crate::runtime::ParallelDriver`]'s physical
/// driver (L35, `par-runtime`) runs the IDENTICAL policy instead of a re-implemented copy.
/// `step` applies ONE op to the current [`RowSet`] — the only thing that differs between the
/// two physical drivers (a plain serial [`apply`] here, vs the parallel driver's
/// morsel-aware `exec_op` + spill accounting) — everything else (the divergence check, the
/// `reoptimize_remaining` tail-splice, the kill-switch fallback) is shared verbatim, so the
/// two drivers can never drift out of sync on WHEN/HOW re-optimization triggers.
///
/// Gated off the SAME `EPISTEMIC_GRAPH_COST_OPT` kill-switch ([`crate::optimizer::enabled`])
/// as `plan_optimize`: disabled ⇒ a plain left fold over `step`, byte-for-byte the pre-
/// optimizer behavior for BOTH drivers.
pub(crate) fn run_with_adaptive_reopt<F>(
    ops: &[Op],
    ctx: &PlanCtx,
    mut step: F,
) -> Result<RowSet, String>
where
    F: FnMut(&Op, RowSet, &PlanCtx) -> Result<RowSet, String>,
{
    if !crate::optimizer::enabled() {
        // The global cost-optimizer kill-switch also disables the runtime feedback
        // loop below — byte-for-byte the pre-optimizer left fold.
        let mut cur = RowSet::new();
        for op in ops {
            cur = step(op, cur, ctx)?;
        }
        return Ok(cur);
    }

    use crate::cost::{Cardinality, ModalityCardinality, PlanStats};

    let card = ModalityCardinality::new(PlanStats::collect(ctx));
    let mut cur = RowSet::new();
    let mut estimated_in = 0.0_f64;
    // The not-yet-executed op list — `reoptimize_remaining` may replace its TAIL
    // (everything after the op about to run) each iteration, so this can grow/shrink
    // in content (never in a way that changes correctness — see the doc above) across
    // the loop, unlike the plain `ops` slice.
    let mut remaining: Vec<Op> = ops.to_vec();
    let mut i = 0;
    while i < remaining.len() {
        let op = remaining[i].clone();
        let estimated_out = card.rows_out(&op, estimated_in, ctx);
        cur = step(&op, cur, ctx)?;
        let actual_out = cur.len() as f64;

        if i + 1 < remaining.len() {
            let tail = crate::optimizer::reoptimize_remaining(
                &remaining[i + 1..],
                estimated_out,
                actual_out,
                &card,
                ctx,
            );
            remaining.splice(i + 1.., tail);
        }
        estimated_in = actual_out;
        i += 1;
    }
    Ok(cur)
}

/// Ergonomic `plan.execute(&ctx)` over the foreign [`Plan`] wire DTO (the orphan
/// rule forbids an inherent method; an extension trait is the idiomatic way to hang
/// behavior on a type from another crate).
pub trait PlanExt {
    fn execute(&self, ctx: &PlanCtx) -> Result<RowSet, String>;
}

impl PlanExt for Plan {
    fn execute(&self, ctx: &PlanCtx) -> Result<RowSet, String> {
        execute(self, ctx)
    }
}

pub(crate) fn apply(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::Scan { label } => Ok(scan_label(ctx.view, label)),

        Op::Filter { preds } => filter_op(ctx, preds, input),

        Op::Traverse { rel, min, max } => Ok(traverse_op(ctx, rel, *min, *max, input)),

        Op::Rank { query } => Ok(rank_op(ctx, query, input)),

        Op::RankEmbed { text } => rank_embed_op(ctx, text, input),

        Op::RankNodeDistance { center } => Ok(rank_node_distance(ctx.view, input, center)),

        Op::RankMentions {} => Ok(rank_mentions(ctx.view, input)),

        Op::RankMmr { lambda, k } => Ok(rank_mmr(ctx.semantic, input, *lambda, *k)),

        #[cfg(feature = "text")]
        Op::RankText { query } => Ok(rank_text(ctx, &input, query)),

        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => fuse_rrf(ctx, &input, branches, *k),

        #[cfg(feature = "owl")]
        Op::Reason {
            target_class,
            ontology,
        } => Ok(reason_op(
            ctx.view,
            ctx.decay,
            input,
            target_class,
            ontology,
        )),

        #[cfg(feature = "owl")]
        Op::SparqlBgp { query, var } => sparql_source(ctx.view, query, var),

        #[cfg(feature = "wasm-udf")]
        Op::Udf { id } => udf_transform(ctx, &input, id),

        #[cfg(feature = "federation")]
        Op::ForeignScan { source, join } => foreign_scan(input, source, *join, ctx),

        // TIME — `AS OF [TX] @<ts>` is a real RowSet-narrowing temporal filter
        // (CONCEPT:AU-KG.compute.kg-2): drop rows whose fact is not live at `ts` on the chosen
        // timeline. Dep-free blob scan (no DataFusion), so it runs in the Pi tier.
        Op::AsOf { ts, axis } => Ok(as_of_filter(ctx.view, input, *ts, *axis)),

        // TIME (`WINDOW <dur>`, CONCEPT:EG-KG.query.streaming-execution) — a REAL windowed aggregate over the
        // input RowSet's time/value columns via eg-tsdb's Pi-path `time_bucket`. The
        // modality-availability boundary lives in the physical fn [`window_op`], NOT as a
        // `#[cfg]` fallback arm in this driver loop (Lane B, CONCEPT:EG-KG.query.driver-modality-fn):
        // ONE arm always, and `window_op` itself either runs the native aggregate
        // (`timeseries`) or passes the rows through (no feature) — so the plan runs either way.
        Op::Window { secs } => Ok(window_op(ctx, input, *secs)),

        // TIME (`WINDOW <dur> <agg>`, CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers) — the selectable-aggregate
        // windowed aggregate. Same modality-availability-in-the-physical-fn shape as
        // `Op::Window`: ONE arm, and [`window_agg_op`] resolves the `agg` selector + runs the
        // native windower (`timeseries`) or passes the rows through (no feature).
        Op::WindowAgg { secs, agg } => Ok(window_agg_op(ctx, input, *secs, agg)),

        // FEDERATION (`FOREIGN "<name>"`, CONCEPT:EG-KG.query.sparql-completeness / EG-073) — the name MARKER
        // the UQL clause lowers to. With a `ForeignSourceRegistry` attached to the ctx
        // (`federation` build) it now RESOLVES the name → rows through the registry (a
        // source op: the foreign rows replace the input); an unbound name is a clean
        // error. With NO registry attached — or a non-federation build — it passes the
        // rows through unchanged, exactly as before (existing behavior preserved).
        Op::Foreign { name } => foreign_named(name, input, ctx),

        // SOURCE (spatial, CONCEPT:EG-KG.ontology.singles-concept) — an eg-geo packed-Hilbert-R-tree bbox scan
        // over the layer's geometries. Gated behind `geo`; the variant only exists when
        // eg-types/geo is on (pulled by eg-plan/geo), so a non-geo build has neither the
        // variant nor this arm (the ForeignScan gating precedent).
        #[cfg(feature = "geo")]
        Op::SpatialScan { layer, bbox } => Ok(spatial_scan(ctx, layer, *bbox)),

        // TRANSFORM (spatial CRS, CONCEPT:EG-KG.domains.coordinate-reference-system) — reproject each row's geometry into
        // `to_epsg`; TRANSFORM (constructive, CONCEPT:EG-KG.ontology.concept-9) — derive a geometry per row
        // via the eg-geo `algebra` op. Both gated behind `geo`; the variants only exist
        // when eg-types/geo is on (the SpatialScan/ForeignScan gating precedent).
        #[cfg(feature = "geo")]
        Op::Reproject { to_epsg, from_epsg } => {
            Ok(spatial_reproject(ctx.view, input, *to_epsg, *from_epsg))
        }
        #[cfg(feature = "geo")]
        Op::SpatialOp { kind } => Ok(spatial_op(ctx.view, input, kind)),

        // SOURCE (tensor, CONCEPT:EG-KG.storage.content-addressed-dedup) — seed the RowSet with the `layer`'s
        // tensor-bearing nodes; TRANSFORM (tensor) — apply the eg-tensor op to each
        // row's stored tensor. Gated behind `tensor`; the variants only exist when
        // eg-types/tensor is on (pulled by eg-plan/tensor), so a non-tensor build has
        // neither the variants nor these arms (the geo/ForeignScan gating precedent).
        #[cfg(feature = "tensor")]
        Op::TensorScan { layer } => Ok(tensor_scan(ctx.view, layer)),
        #[cfg(feature = "tensor")]
        Op::TensorOp { kind } => Ok(tensor_op(ctx, input, kind)),

        // TRANSFORM (probabilistic, CONCEPT:EG-KG.compute.uncertainty-values) — score each row by a closed-form
        // probabilistic query (expectation / marginal / conditional posterior / seeded
        // sample) over its stored `Distribution` value and re-order by that score
        // descending, dropping rows without a valid distribution or where the query does
        // not apply. Gated behind `probabilistic`; the variant only exists when
        // eg-types/probabilistic is on (pulled by eg-plan/probabilistic), so a build
        // without it has neither the variant nor this arm (the tensor/stream precedent).
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { query } => Ok(probabilistic_op(ctx.view, input, query)),

        // SOURCE/FILTER (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — the
        // evidence FOR/AGAINST a claim, or the claims a node itself supports. Gated behind
        // `epistemic`; the variants only exist when eg-types/epistemic is on (pulled by
        // eg-plan/epistemic), so a non-epistemic build has neither the variants nor these arms
        // (the tensor/geo/probabilistic gating precedent).
        #[cfg(feature = "epistemic")]
        Op::EvidenceFor { claim_id } => Ok(evidence_for_op(ctx.view, input, claim_id)),
        #[cfg(feature = "epistemic")]
        Op::Contradicts { node_id } => Ok(contradicts_op(ctx.view, input, node_id)),
        #[cfg(feature = "epistemic")]
        Op::SupportedBy { node_id } => Ok(supported_by_op(ctx.view, input, node_id)),

        // TIME+TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) —
        // `BELIEF AS OF <ts>`: pin the transaction-time axis (reuses `as_of_filter`) then
        // re-score the survivors by their propagated belief confidence at that instant.
        #[cfg(feature = "epistemic")]
        Op::BeliefAsOf { ts } => Ok(belief_as_of_op(ctx, input, *ts)),

        // TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — re-weight
        // the current RowSet by one named source's propagated reliability.
        #[cfg(feature = "epistemic")]
        Op::SourceReliability { source_id } => Ok(source_reliability_op(ctx, input, source_id)),

        // TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — re-score
        // EACH row by its OWN propagated belief confidence, ranked descending.
        #[cfg(feature = "epistemic")]
        Op::ConfidenceOp {} => Ok(confidence_op(ctx, input)),

        // TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) —
        // `EXPLAIN BELIEF <id>`: flatten the recursive justification tree to scored rows.
        #[cfg(feature = "epistemic")]
        Op::ExplainBelief { node_id } => Ok(explain_belief_op(ctx, input, node_id)),

        // TRANSFORM (stream, CONCEPT:EG-KG.query.pipelined-execution) — interpret the input RowSet as a
        // time-ordered event stream and run the eg-stream bounded NFA CEP engine over it,
        // keeping the rows that participate in a match. Gated behind `stream`; the variant
        // only exists when eg-types/stream is on (pulled by eg-plan/stream), so a
        // non-stream build has neither the variant nor this arm (the tensor/geo gating
        // precedent).
        #[cfg(feature = "stream")]
        Op::Cep { pattern } => Ok(cep_op(ctx.view, input, pattern)),

        // FUSE (multimodal sensor fusion, CONCEPT:EG-KG.query.multi-rate-sensor-stream) — time-align the named sensor
        // streams to ONE common clock via eg-tsdb's ASOF-backed `sensor_fuse` and emit a
        // fused row per instant (id = the aligned ts, score = the present-channel count).
        // A SOURCE op: it resolves its streams off the snapshot and REPLACES the input,
        // exactly like `TensorScan`/`SpatialScan`. Gated behind `timeseries`; the variant
        // only exists when eg-types/timeseries is on (pulled by eg-plan/timeseries), so a
        // non-timeseries build has neither the variant nor this arm (the tensor/stream
        // precedent). Reuses the eg-plan→eg-tsdb edge `Op::Window` already opened.
        #[cfg(feature = "timeseries")]
        Op::SensorFuse {
            streams,
            tolerance_ns,
        } => Ok(sensor_fuse_op(ctx.view, streams, *tolerance_ns)),

        // SOURCE (time-series, CONCEPT:EG-KG.query.native-time-series) — seed the RowSet from native eg-tsdb
        // series via the `SeriesStore` attached to the ctx (`PlanCtx::with_tsdb`), so the
        // tsdb leg fuses with the graph/vector/relational legs in ONE plan. Gated behind
        // `timeseries` (the eg-plan→eg-tsdb `redb-store` edge); with no store attached it
        // yields no rows (degrade, never err — the SensorFuse/Rank precedent).
        #[cfg(feature = "timeseries")]
        Op::TsScan { series, from, to } => Ok(tsdb_scan_op(
            ctx.tsdb,
            ctx.staged_series,
            series,
            *from,
            *to,
        )),

        Op::Limit { k } => Ok(input.limit(*k)),
    }
}

/// GRAPH TRAVERSE (CONCEPT:EG-KG.query.exec-arm-dispatch) — petgraph BFS over the `GraphView`
/// topology for `min..=max` hops of relationship `rel`, seeded by the current candidate ids.
/// The behavior-identical extraction of the `Op::Traverse` arm so [`apply`] is a thin
/// dispatch table (Lane 0 de-conflict); the BFS itself lives in [`bfs_reached`].
fn traverse_op(ctx: &PlanCtx, rel: &str, min: usize, max: usize, input: RowSet) -> RowSet {
    let reached = bfs_reached(ctx.view, &input.ids(), rel, min, max);
    RowSet::from_ids(reached)
}

/// RANK (vector, CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter) — hybrid metadata pre-filter.
/// Push the current candidate set INTO the ANN scan as an allowlist so the returned top-k
/// already satisfies the predicate — filtering happens DURING the probe instead of
/// over-fetching `k*4` and post-filtering. Semantics are unchanged: with an empty candidate
/// set the allowlist rejects everything (a source `Rank` yields no rows, exactly as the
/// prior over-fetch-then-intersect did). The behavior-identical extraction of the `Op::Rank`
/// arm (Lane 0 de-conflict) so [`apply`] is a thin dispatch table.
fn rank_op(ctx: &PlanCtx, query: &[f32], input: RowSet) -> RowSet {
    let candidates = input.id_set();
    let k = candidates.len().max(1);
    let scored = ctx
        .semantic
        .semantic_search_filtered(query, k, |id| candidates.contains(id));
    RowSet::from_scored(scored)
}

/// RANK (vector-from-text, CONCEPT:EG-KG.compute.no-embedder-bound-op) — the UQL `RANK BY ~ "text"`
/// NL→vector seam. Resolve the text to a query vector via the server-side embedder bound on
/// the ctx, then kNN-rank the candidate set exactly like [`rank_op`]. With NO embedder bound
/// this is a clean typed error (never a panic) — the documented "no embedder bound" behavior.
/// The behavior-identical extraction of the `Op::RankEmbed` arm (Lane 0 de-conflict) so
/// [`apply`] is a thin dispatch table.
fn rank_embed_op(ctx: &PlanCtx, text: &str, input: RowSet) -> Result<RowSet, String> {
    let embedder = ctx.embedder.ok_or_else(|| {
        format!(
            "RANK BY ~ \"{text}\": no server-side text embedder is bound on this \
             query (bind one via PlanCtx::with_embedder, or pass an inline literal \
             vector `RANK BY ~[…]`)"
        )
    })?;
    let query = embedder.embed(text)?;
    let candidates = input.id_set();
    let k = candidates.len().max(1);
    let scored = ctx
        .semantic
        .semantic_search_filtered(&query, k, |id| candidates.contains(id));
    Ok(RowSet::from_scored(scored))
}

/// SOURCE (federation): read rows from an EXTERNAL source — a remote epistemic-graph
/// engine or a generic HTTP/JSON API (CONCEPT:EG-KG.query.query-federation) — into the RowSet. When `join`
/// is false the foreign rows REPLACE the input (a pure source, like `Scan`). When
/// `join` is true the foreign rows are intersected with the current candidate set keyed
/// on id (a foreign∩local JOIN), preserving the input's order — so a plan can seed
/// locally, then narrow to ids a foreign source also returns. The `fetch()` runs
/// synchronously on the blocking pool, exactly like the SQL/vector legs.
#[cfg(feature = "federation")]
fn foreign_scan(
    input: RowSet,
    source: &eg_types::wire::ForeignSourceSpec,
    join: bool,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    // SYMMETRIC leaf (CONCEPT:EG-KG.query.symmetric-foreign-scan): resolve the foreign
    // source to its rows through the SINGLE `federation::foreign_source_rows` seam — the
    // foreign analogue of `scan_label` for an internal `Op::Scan`. Every spec kind
    // (Named-via-registry / remote-engine / HTTP-JSON / SQL) resolves identically there,
    // yielding the SAME `RowSet` currency an internal `Scan` yields, so this arm is a thin
    // "resolve the leaf, then compose" mirror of the `Op::Scan` arm's thin `scan_label`
    // call. `fuse_foreign` then composes it exactly like a source (`join=false` REPLACES
    // the input like a leading `Scan`; `join=true` intersects on id — a foreign∩local
    // JOIN).
    let foreign = crate::federation::foreign_source_rows(source, ctx.foreign)?;
    Ok(fuse_foreign(input, foreign, join))
}

/// FEDERATION (`FOREIGN "<name>"`, CONCEPT:EG-KG.query.closure-backed-source) — resolve the named foreign source
/// through the registry on the ctx. With a registry attached the named source's rows
/// REPLACE the input (a source op) and an unbound name is a clean typed error; with NO
/// registry attached the op passes its input through unchanged (the prior behavior — so
/// existing plans are untouched).
#[cfg(feature = "federation")]
fn foreign_named(name: &str, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match ctx.foreign {
        Some(registry) => registry.resolve(name),
        None => Ok(input),
    }
}

/// Without `federation` there is no registry and no foreign machinery, so the
/// `FOREIGN "<name>"` marker keeps its pass-through behavior (CONCEPT:EG-KG.query.closure-backed-source).
#[cfg(not(feature = "federation"))]
fn foreign_named(_name: &str, input: RowSet, _ctx: &PlanCtx) -> Result<RowSet, String> {
    Ok(input)
}

/// Fuse a foreign RowSet with the local candidate set the SAME way for EVERY foreign
/// kind (remote-engine / HTTP-JSON / external-SQL): `join=false` ⇒ the foreign rows
/// REPLACE the input (a pure source); `join=true` ⇒ intersect keyed on id, preserving
/// the input's order (a foreign∩local JOIN). Factored out so a compose-join proof can
/// exercise the EXACT fuse the executor runs against a mock-fetched foreign RowSet,
/// without standing up a live external DB.
#[cfg(feature = "federation")]
pub(crate) fn fuse_foreign(input: RowSet, foreign: RowSet, join: bool) -> RowSet {
    if join && !input.is_empty() {
        let keep = foreign.id_set();
        input.intersect_keep_order(&keep)
    } else {
        foreign
    }
}

/// UDF (WASM): transform the current `RowSet` through a registered, SANDBOXED wasm
/// function (CONCEPT:EG-KG.query.rowset-execution). Serializes the input rows `[(id, score?), …]` to
/// MessagePack, runs the UDF `id` under fuel + memory limits with NO host caps, and
/// deserializes the SAME-shape output rows back into the pipeline. A registry-less ctx
/// or an unknown UDF id errs (a UDF must be registered to run). The bytes contract is
/// the engine's — a UDF reads `Vec<(String, Option<f32>)>` and returns the same.
#[cfg(feature = "wasm-udf")]
fn udf_transform(ctx: &PlanCtx, input: &RowSet, id: &str) -> Result<RowSet, String> {
    let Some(registry) = ctx.udf else {
        return Err("Udf op requires a UDF registry on the PlanCtx (none attached)".into());
    };
    let rows: Vec<(String, Option<f32>)> = input
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect();
    let payload = rmp_serde::to_vec_named(&rows).map_err(|e| format!("udf input encode: {e}"))?;
    let out = registry.run(id, &payload).map_err(|e| e.to_string())?;
    let out_rows: Vec<(String, Option<f32>)> =
        rmp_serde::from_slice(&out).map_err(|e| format!("udf output decode: {e}"))?;
    Ok(RowSet::from_rows(out_rows))
}

/// RANK (lexical, BM25): re-order the candidate set by BM25 relevance to `query`.
/// Symmetric to the vector `Rank` — BM25 top-k over the FULL index (over-fetch),
/// then keep only the current candidates, in BM25-score order. With no text index
/// configured the result is empty (degrade, never err), mirroring `Rank` over an
/// empty embedding store.
#[cfg(feature = "text")]
fn rank_text(ctx: &PlanCtx, input: &RowSet, query: &str) -> RowSet {
    let Some(index) = ctx.text else {
        return RowSet::new();
    };
    let candidates = input.id_set();
    let k = candidates.len().max(1);
    let want = (k * 4).max(k + 32);
    let hits = index.search(query, want);
    let scored: Vec<(String, f32)> = hits
        .into_iter()
        .filter(|h| candidates.contains(h.id.as_str()))
        .map(|h| (h.id, h.score))
        .collect();
    RowSet::from_scored(scored)
}

/// FUSE (hybrid): run each of the N SUB-PLAN `branches` over the SAME `input` seed
/// (snapshot-isolated, same `ctx`), then reciprocal-rank-fuse their ranked id lists
/// into one RowSet (CONCEPT:AU-KG.query.text-spatial-time / KG-2.253). Each branch is a normal `Vec<Op>`
/// plan, so the canonical tri-modal hybrid is `[[Rank{vec}], [RankText{q}],
/// [RankNodeDistance{c}]]`, but any number of ranking sub-plans compose. RRF fuses the
/// RANKS (not the incomparable cosine/BM25/distance scales), so a doc strong across
/// MORE branches out-ranks one strong in only one — the property that makes the fused
/// query beat any single branch alone.
#[cfg(feature = "text")]
fn fuse_rrf(ctx: &PlanCtx, input: &RowSet, branches: &[Vec<Op>], k: f32) -> Result<RowSet, String> {
    let mut ranked: Vec<Vec<String>> = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut cur = input.clone();
        for op in branch {
            cur = apply(op, cur, ctx)?;
        }
        ranked.push(cur.ids());
    }
    let k = if k > 0.0 { k } else { eg_text::RRF_K };
    let refs: Vec<&[String]> = ranked.iter().map(|v| v.as_slice()).collect();
    let fused = eg_text::rrf_fuse(&refs, k);
    Ok(RowSet::from_scored(fused))
}

/// SOURCE **or** FILTER (OWL, CONCEPT:EG-KG.query.native-time-series) — dispatches on whether `input` carries
/// rows. With an EMPTY input it is a pure SOURCE (today's behavior, unchanged): every
/// OWL-inferred, confidence-scored member of `target_class` seeds the RowSet. With a
/// NON-EMPTY input it acts MID-PIPELINE as a confidence-preserving FILTER: keep only the
/// incoming rows that are ALSO inferred members, preserving the incoming order/scores via
/// [`RowSet::intersect_keep_order`] — the SAME idiom `as_of_filter` uses. That makes
/// `Rank → Reason → Traverse` legal: reasoning narrows a vector-ranked candidate set in
/// the middle of a plan, not only as a leaf source. The reasoner runs identically either
/// way; only the compose step differs, so the empty-input path is byte-for-byte the old
/// `reason_source`.
#[cfg(feature = "owl")]
fn reason_op(
    view: &GraphView,
    // CONCEPT:EG-KG.query.reason-decay-in-plan — the ctx's `(now, half_life)` decay context, so
    // the inferred-membership scores are Ebbinghaus-decayed IN-PLAN. `None` ⇒ decay-neutral.
    decay: Option<(u64, f64)>,
    input: RowSet,
    target_class: &str,
    ontology: &str,
) -> RowSet {
    if input.is_empty() {
        return reason_source(view, decay, target_class, ontology);
    }
    let inferred = reason_source(view, decay, target_class, ontology);
    let keep = inferred.id_set();
    input.intersect_keep_order(&keep)
}

/// SOURCE (OWL): the individuals the native OWL 2 reasoner INFERS to be members of
/// `target_class` (CONCEPT:EG-KG.ontology.concept-12). Parses the `ontology` Turtle (or, when empty,
/// the axioms already present in the graph view's blobs — they round-trip as RDF),
/// runs EL⁺ classification, reads the graph's asserted instance types, and projects
/// every (possibly only-inferred) member of `target_class` into a `RowSet`. These are
/// ids the property-graph stored NO explicit `target_class` type edge for, yet they
/// then flow — like any RowSet — into a downstream `Traverse`/`Rank`/`Filter`/`Limit`.
#[cfg(feature = "owl")]
fn reason_source(
    view: &GraphView,
    decay: Option<(u64, f64)>,
    target_class: &str,
    ontology: &str,
) -> RowSet {
    use eg_rdf::owl::{asserted_types_with_confidence_from_view, instances_of_weighted, Reasoner};

    // Axioms: an explicit ontology document, else the triples already in the graph.
    let triples = if ontology.trim().is_empty() {
        eg_rdf::owl::tbox_triples_from_view(view)
    } else {
        eg_rdf::mapping::parse_turtle(ontology).unwrap_or_default()
    };
    let mut reasoner = Reasoner::from_triples(&triples);
    // Confidence-weighted (CONCEPT:EG-KG.ontology.concept-13): each inferred member carries its
    // membership confidence as the RowSet SCORE, so a bare `Reason` plan is already
    // ranked by confidence and composes with a downstream vector `Rank`/`Limit`. The
    // closure is identical to the unweighted one for a HARD ontology (every score 1.0).
    let cls = reasoner.classify_weighted();

    let target = normalize_class(target_class);
    // String-type↔IRI-class bridge (CONCEPT:EG-KG.ontology.string-type-iri-class): derive the bridge base from the
    // REASON target IRI's namespace, so a node with a BARE string `type` (e.g.
    // `{"type":"Sensor"}`) is resolved as `<base/Sensor>` and — through the TBox subclass
    // closure — becomes a member of `REASON <base/Device>` when `<base/Sensor> ⊑
    // <base/Device>`. A bare (non-IRI) target yields `None` ⇒ the prior bare-match path.
    let class_base = eg_rdf::owl::class_namespace(&target);
    // Asserted instance→class assignments + their per-fact confidence. CONCEPT:EG-KG.query.reason-decay-in-plan:
    // when a `(now, half_life)` decay context is bound on the `PlanCtx` (via `with_decay`), the
    // fact confidences are Ebbinghaus-decayed by each node's age relative to `now` RIGHT HERE,
    // so time-decayed OWL confidence composes IN-PLAN alongside `Op::AsOf` in ONE fused plan.
    // ABSENT a decay context, `now = 0` / `half_life = 0` keeps the op decay-NEUTRAL — a bare
    // `Reason` stays a stable, deterministic source/leaf (byte-for-byte the prior behavior). The
    // AXIOM confidence still flows through into the score either way.
    let (now, half_life) = decay.unwrap_or((0, 0.0));
    let asserted =
        asserted_types_with_confidence_from_view(view, now, half_life, class_base.as_deref());
    let scored: Vec<(String, f32)> = instances_of_weighted(&cls, &asserted, &target, 0.0)
        .into_iter()
        .map(|(id, conf)| (id, conf as f32))
        .collect();
    RowSet::from_scored(scored)
}

/// SOURCE (SPARQL): the node bindings of `var` in the SPARQL `query` over the view
/// (CONCEPT:EG-KG.ontology.concept-12) — a SPARQL-selected candidate set as a RowSet. Only resource
/// (node) bindings become ids; literal bindings are skipped (an id set is node ids).
#[cfg(feature = "owl")]
fn sparql_source(view: &GraphView, query: &str, var: &str) -> Result<RowSet, String> {
    let res = eg_rdf::sparql::run(view, query)?;
    let ids = res.solutions.iter().filter_map(|sol| {
        sol.get(var).and_then(|b| match b {
            eg_rdf::sparql::Binding::Node(n) => Some(n.clone()),
            eg_rdf::sparql::Binding::Literal(_) => None,
        })
    });
    Ok(RowSet::from_ids(ids))
}

/// Canonicalize a class id to the ontology's `<iri>` form (accept a bare IRI too).
#[cfg(feature = "owl")]
fn normalize_class(c: &str) -> String {
    if c.starts_with('<') {
        c.to_string()
    } else if c.starts_with("http") {
        format!("<{c}>")
    } else {
        c.to_string()
    }
}

/// SOURCE: all node ids whose `type` property equals `label`.
fn scan_label(view: &GraphView, label: &str) -> RowSet {
    let ids = view.node_properties.iter().filter_map(|(id, blob)| {
        let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
        (v.get("type").and_then(|t| t.as_str()) == Some(label)).then(|| id.clone())
    });
    RowSet::from_ids(ids)
}

// ── the temporal AS OF leg — bi-temporal point-in-time filter (KG-2.250) ────────

/// True when the fact in `blob` is live at instant `ts` on the chosen timeline.
/// Half-open window `[from, until)` in epoch seconds: a missing `from` means
/// "has always been" (0); a missing `until` means "still current" (open). Decodes
/// the SAME blob `scan_label` reads — dep-free, so the time path runs in the Pi tier.
fn live_at(blob: &[u8], ts: u64, from_key: &str, until_key: &str) -> bool {
    let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) else {
        return false;
    };
    let from = v.get(from_key).and_then(|x| x.as_u64()).unwrap_or(0);
    let until = v.get(until_key).and_then(|x| x.as_u64());
    from <= ts && until.is_none_or(|u| ts < u)
}

/// Narrow `input` to rows live at `ts` on the `axis` timeline (CONCEPT:AU-KG.compute.kg-2).
/// Order-preserving (a vector-`Rank`-then-`AS OF` plan stays ranked, via
/// [`RowSet::intersect_keep_order`]). When `input` is empty the op acts as a source,
/// yielding every node live at `ts`.
fn as_of_filter(view: &GraphView, input: RowSet, ts: f64, axis: TimeAxis) -> RowSet {
    let ts = ts.max(0.0) as u64;
    let (from_key, until_key) = match axis {
        TimeAxis::Valid => ("valid_from", "valid_until"),
        TimeAxis::Transaction => ("tx_from", "tx_to"),
    };
    if input.is_empty() {
        let ids = view
            .node_properties
            .iter()
            .filter(|(_, blob)| live_at(blob.as_slice(), ts, from_key, until_key))
            .map(|(id, _)| id.clone());
        return RowSet::from_ids(ids);
    }
    let kept: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            view.node_properties
                .get(r.id.as_str())
                .is_some_and(|b| live_at(b.as_slice(), ts, from_key, until_key))
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&kept)
}

// ── the WINDOW physical fns — modality availability lives HERE, not in the driver loop ──

/// WINDOW physical op (CONCEPT:EG-KG.query.driver-modality-fn) — the `Op::Window`
/// modality-availability boundary. With `timeseries` it runs the native tumbling-MEAN
/// windowed aggregate ([`window_aggregate`]); without it the op passes the rows through
/// unchanged. Keeping the `#[cfg]` in the PHYSICAL fn (not as a fallback arm in [`apply`])
/// is Lane B's rule: the driver loop carries ONE arm per op, and a modality's absence
/// degrades inside its own fn — so a non-timeseries build's plan still runs, byte-for-byte
/// the prior pass-through.
#[cfg(feature = "timeseries")]
fn window_op(ctx: &PlanCtx, input: RowSet, secs: f64) -> RowSet {
    window_aggregate(ctx.view, input, secs, eg_tsdb::query::Agg::Mean)
}
#[cfg(not(feature = "timeseries"))]
fn window_op(_ctx: &PlanCtx, input: RowSet, _secs: f64) -> RowSet {
    input
}

/// WINDOW-with-aggregate physical op (CONCEPT:EG-KG.query.driver-modality-fn) — the
/// `Op::WindowAgg` modality-availability boundary, mirroring [`window_op`]: with `timeseries`
/// it resolves the `agg` selector ([`parse_window_agg`]) and runs the SAME tumbling windower;
/// without it the op passes the rows through unchanged.
#[cfg(feature = "timeseries")]
fn window_agg_op(ctx: &PlanCtx, input: RowSet, secs: f64, agg: &str) -> RowSet {
    window_aggregate(ctx.view, input, secs, parse_window_agg(agg))
}
#[cfg(not(feature = "timeseries"))]
fn window_agg_op(_ctx: &PlanCtx, input: RowSet, _secs: f64, _agg: &str) -> RowSet {
    input
}

// ── the TIME WINDOW leg — native eg-tsdb windowed aggregate (CONCEPT:EG-KG.query.streaming-execution) ─────

/// TIME (`WINDOW <dur> [agg]`): a REAL tumbling windowed aggregate over the input
/// RowSet's `(ts, value)` rows, replacing the pass-through seam (CONCEPT:EG-KG.query.streaming-execution /
/// EG-413 / EG-414). Two row shapes are consumed, in this order:
///
///  1. **A graph-node row** — `view.node_properties` has the row's `id`. The event time
///     is read from that node's `valid_from` property blob (exactly as `Op::AsOf`'s Valid
///     axis; see [`live_at`]), the value from the row's carried `score` (what `Op::Rank`
///     produces), falling back to a numeric `value` property so a STANDALONE `Window`
///     over graph nodes (no preceding `Rank`) still aggregates. This path is byte-for-byte
///     the prior behavior.
///  2. **A time-series SOURCE row** — the row's `id` is NOT a graph node but parses as an
///     integer ts and carries a `score`. This is exactly what `Op::TsScan` (and a prior
///     `Op::Window`) emit: `id` = the point timestamp, `score` = the value. Adding this
///     path is CONCEPT:EG-KG.compute.tsscan-series-window-60s — it makes `TsScan(series) → Window(secs)` actually
///     consume the tsdb series and produce windowed aggregates (before, a TsScan row's
///     numeric id was not a node, so every row was dropped and the window was empty).
///
/// The resolved `(ts, value)` pairs become `eg_tsdb::Point`s, are ts-sorted (RowSet order
/// is rank/discovery order, but `time_bucket` needs a sorted series), and are fed to the
/// engine's Pi-path `time_bucket` primitive (`eg_tsdb::query` — no DataFusion) with
/// `width = secs` and the caller's [`Agg`] (`Op::Window` ⇒ `Mean`; `Op::WindowAgg` ⇒ the
/// selected aggregate). Each non-empty bucket becomes one output row — `id` = the
/// bucket's aligned start, `score` = the aggregate — so a downstream `Limit`/`Rank`
/// composes over the windowed series. `secs <= 0`, or no timed-and-valued rows, yields an
/// empty RowSet (degrade, never err — mirroring `Rank` over an empty store).
#[cfg(feature = "timeseries")]
fn window_aggregate(
    view: &GraphView,
    input: RowSet,
    secs: f64,
    agg: eg_tsdb::query::Agg,
) -> RowSet {
    use eg_tsdb::point::Point;
    use eg_tsdb::query::time_bucket;

    let width = secs.max(0.0) as i64;
    let mut pts: Vec<Point> = input
        .rows()
        .iter()
        .filter_map(|r| match view.node_properties.get(r.id.as_str()) {
            // (1) GRAPH-NODE row — ts from `valid_from`, value from `score`|`value` prop.
            // Byte-for-byte the prior behavior (a present node without a valid ts/value is
            // still dropped here, never reinterpreted as a ts).
            Some(blob) => {
                let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
                let ts = v.get("valid_from").and_then(|x| x.as_i64())?;
                let value = r
                    .score
                    .map(|s| s as f64)
                    .or_else(|| v.get("value").and_then(|x| x.as_f64()))?;
                Some(Point::single(ts, value))
            }
            // (2) TIME-SERIES SOURCE row (`Op::TsScan`/`Op::Window` output) — the id IS the
            // ts, the score IS the value. Only reached when the id is not a graph node.
            // `Op::TsScan` emits eg-tsdb NANOSECOND timestamps, but a `WINDOW <secs>` width
            // is in SECONDS (the unit graph-node `valid_from` / `Op::AsOf` use), so the ts
            // is normalized ns → s here — both sources then bucket consistently, and the
            // emitted bucket-start ids are in the same (second) unit as the width.
            None => {
                let ts_ns = r.id.parse::<i64>().ok()?;
                let value = r.score? as f64;
                Some(Point::single(ts_ns / 1_000_000_000, value))
            }
        })
        .collect();
    pts.sort_by_key(|p| p.ts);
    let scored = time_bucket(&pts, width, agg)
        .into_iter()
        .map(|b| (b.bucket_start.to_string(), b.value as f32));
    RowSet::from_scored(scored)
}

/// Resolve a UQL/wire `WINDOW … <agg>` selector string to an eg-tsdb [`Agg`]
/// (CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers). Case-insensitive; an unknown selector defaults to `Mean` (so a
/// forward-compat or mistyped selector degrades to the canonical aggregate rather than
/// erroring).
#[cfg(feature = "timeseries")]
fn parse_window_agg(agg: &str) -> eg_tsdb::query::Agg {
    use eg_tsdb::query::Agg;
    match agg.to_ascii_lowercase().as_str() {
        "sum" => Agg::Sum,
        "min" | "minimum" => Agg::Min,
        "max" | "maximum" => Agg::Max,
        "count" => Agg::Count,
        "first" => Agg::First,
        "last" => Agg::Last,
        // "mean" | "avg" | "average" and anything unknown ⇒ the canonical aggregate.
        _ => Agg::Mean,
    }
}

// ── the sensor-fusion leg — native eg-tsdb ASOF alignment (CONCEPT:EG-KG.query.multi-rate-sensor-stream) ───────

/// FUSE (multimodal sensor fusion): time-align N heterogeneous sensor `streams` to ONE
/// common clock and emit fused multi-channel rows (CONCEPT:EG-KG.query.multi-rate-sensor-stream). Each named stream is
/// a node LAYER (matched on the `type` property, exactly as [`scan_label`]) whose nodes
/// carry a `valid_from` event time (ns, the same column `Op::AsOf`/`Op::Window` read) and
/// EITHER a numeric `value` (a scalar reading) OR an opaque tensor-frame reference in a
/// `tensor`/`frame` string property (an EG-085 camera/LiDAR blob id, carried through
/// untouched). The resolved, ts-sorted per-stream samples feed eg-tsdb's Pi-path
/// `sensor_fuse` (`eg_tsdb::query` — no DataFusion), which reuses the ASOF backward-join to
/// carry each stream's latest sample at-or-before every reference instant within
/// `tolerance_ns` (`0` ⇒ exact-instant matches only). Each fused instant becomes one output
/// row — `id` = the aligned ts, `score` = the count of PRESENT (non-gap) channels — so a
/// downstream `Limit`/`Rank` composes over the fused series. No timed streams ⇒ an empty
/// RowSet (degrade, never err — mirroring `window_aggregate`).
#[cfg(feature = "timeseries")]
fn sensor_fuse_op(view: &GraphView, streams: &[String], tolerance_ns: u64) -> RowSet {
    use eg_tsdb::query::{sensor_fuse, Cell, Sample, SeriesRef};

    // Resolve each named stream (a node layer / `type`) into a ts-sorted sample series.
    let series: Vec<SeriesRef> = streams
        .iter()
        .map(|name| {
            let mut samples: Vec<Sample> = Vec::new();
            for blob in view.node_properties.values() {
                let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
                    continue;
                };
                if v.get("type").and_then(|t| t.as_str()) != Some(name.as_str()) {
                    continue;
                }
                let Some(ts) = v.get("valid_from").and_then(|x| x.as_i64()) else {
                    continue;
                };
                // Scalar reading, else an opaque tensor/frame blob reference.
                let cell = if let Some(val) = v.get("value").and_then(|x| x.as_f64()) {
                    Cell::Scalar(val)
                } else if let Some(r) = v
                    .get("tensor")
                    .or_else(|| v.get("frame"))
                    .and_then(|x| x.as_str())
                {
                    Cell::Blob(r.to_string())
                } else {
                    continue;
                };
                samples.push(Sample { ts, cell });
            }
            samples.sort_by_key(|s| s.ts);
            SeriesRef {
                name: name.clone(),
                samples,
            }
        })
        .collect();

    let scored = sensor_fuse(&series, Some(tolerance_ns as i64))
        .into_iter()
        .map(|row| {
            let present = row.channels.iter().filter(|c| c.is_some()).count();
            (row.ts.to_string(), present as f32)
        });
    RowSet::from_scored(scored)
}

// ── the time-series SOURCE leg — native eg-tsdb SeriesStore scan (CONCEPT:EG-KG.query.native-time-series) ─

/// SOURCE (time-series): read the points of each id in `series` within the `[from, to)`
/// window out of the native eg-tsdb [`eg_tsdb::store::SeriesStore`] and emit one row per
/// point — `id` = the point timestamp (stringified), `score` = the point's first field
/// value — so a downstream `Rank`/`Limit`/`Filter` fuses the time-series leg with the
/// graph/vector/relational legs in ONE plan (tsdb-in-plan fusion, CONCEPT:EG-KG.query.native-time-series).
///
/// Bounds are `f64` SECONDS on the wire (uniform with the other numeric plan ops); this
/// op lowers them to the store's `i64`-nanosecond range internally, and the store's
/// `range` is half-open `[from, to)` (t >= from && t < to), so the plan bound semantics
/// match. `None` store (no [`PlanCtx::with_tsdb`] attached), an unknown series, or a
/// point with no value all contribute nothing — the RowSet just stays empty/smaller:
/// degrade, never err, exactly as `Rank` over an empty store or `SensorFuse` over no
/// timed streams. Rows dedup by id (ts): on a ts shared across series the FIRST series'
/// point wins ([`RowSet::from_scored`] keeps the first occurrence).
///
/// CONCEPT:EG-KG.query.txn-tsdb-read-your — in-txn read-your-own-writes: when a [`StagedSeries`] overlay is
/// attached ([`PlanCtx::with_staged_series`]), the transaction's OWN staged points are
/// emitted FIRST (before the committed store), so on a ts a series staged in-txn shadows
/// the committed value (RYOW precedence) and staged-only points (a series the txn just
/// created) are visible before commit. With no overlay the committed store is read alone
/// — byte-for-byte the prior behavior.
#[cfg(feature = "timeseries")]
fn tsdb_scan_op(
    store: Option<&eg_tsdb::store::SeriesStore>,
    staged: Option<&StagedSeries>,
    series: &[String],
    from: f64,
    to: f64,
) -> RowSet {
    const NS_PER_S: f64 = 1e9;
    let from_ns = (from.max(0.0) * NS_PER_S) as i64;
    let to_ns = (to.max(0.0) * NS_PER_S) as i64;

    // Staged points first (RYOW precedence — `from_scored` keeps the first occurrence of
    // a ts), then the committed store's points.
    let staged_rows = staged.into_iter().flat_map(|s| {
        series
            .iter()
            .flat_map(move |sid| s.range(sid, from_ns, to_ns))
            .map(|(ts, v)| (ts.to_string(), v))
    });
    let committed_rows = store.into_iter().flat_map(|st| {
        series.iter().flat_map(move |sid| {
            st.range(sid, from_ns, to_ns)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| p.values.first().map(|&v| (p.ts.to_string(), v as f32)))
        })
    });
    RowSet::from_scored(staged_rows.chain(committed_rows))
}

// ── graph-native rerankers (CONCEPT:EG-KG.query.uql-parser-ops) ───────────────────────────────────

/// RANK by inverse shortest-path hop distance from `center` over the topology
/// (Graphiti's `node_distance`). Score `1/(1+hops)`; an unreachable or absent-center
/// candidate scores 0 but is kept (degrade, never err — mirrors `Rank` over an empty
/// store). Unweighted BFS from `center` following OUTGOING edges (the same topology
/// the `Traverse` leg walks). Order follows score desc, ties by id for determinism.
fn rank_node_distance(view: &GraphView, input: RowSet, center: &str) -> RowSet {
    use petgraph::visit::EdgeRef;
    let candidates = input.id_set();
    if candidates.is_empty() {
        return input;
    }
    // BFS hop distances from center over the whole topology.
    let mut dist: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    if let Some(&start) = view.node_map.get(center) {
        let mut visited: HashSet<petgraph::stable_graph::NodeIndex> = HashSet::new();
        visited.insert(start);
        dist.insert(center.to_string(), 0);
        let mut frontier = vec![start];
        let mut depth = 0usize;
        while !frontier.is_empty() {
            depth += 1;
            let mut next = Vec::new();
            for &node in &frontier {
                for e in view
                    .graph
                    .edges_directed(node, petgraph::Direction::Outgoing)
                {
                    let nbr = e.target();
                    if visited.insert(nbr) {
                        dist.entry(view.graph[nbr].clone()).or_insert(depth);
                        next.push(nbr);
                    }
                }
            }
            frontier = next;
        }
    }
    let scored: Vec<(String, f32)> = input
        .rows()
        .iter()
        .map(|r| {
            let s = match dist.get(&r.id) {
                Some(&h) => 1.0 / (1.0 + h as f32),
                None => 0.0,
            };
            (r.id.clone(), s)
        })
        .collect();
    RowSet::from_scored(sort_by_score_desc(scored))
}

/// RANK by provenance salience: how many edges point AT each candidate (incoming-edge
/// count), normalized to the max in the set (Graphiti's `episode_mentions`). Pure
/// topology, dep-free. Ties broken by id for determinism.
fn rank_mentions(view: &GraphView, input: RowSet) -> RowSet {
    let counts: Vec<(String, f32)> = input
        .rows()
        .iter()
        .map(|r| {
            let c = view
                .node_map
                .get(&r.id)
                .map(|&idx| {
                    view.graph
                        .edges_directed(idx, petgraph::Direction::Incoming)
                        .count()
                })
                .unwrap_or(0);
            (r.id.clone(), c as f32)
        })
        .collect();
    let max = counts.iter().map(|(_, c)| *c).fold(0.0f32, f32::max);
    let scored: Vec<(String, f32)> = counts
        .into_iter()
        .map(|(id, c)| (id, if max > 0.0 { c / max } else { 0.0 }))
        .collect();
    RowSet::from_scored(sort_by_score_desc(scored))
}

/// RANK by Maximal Marginal Relevance (CONCEPT:AU-KG.retrieval.mmr-diversification): greedily re-order the
/// candidates trading off relevance against diversity. Relevance is each row's
/// incoming score (from a prior `Rank`; defaults to a uniform rank-decay when scores
/// are absent). Similarity is cosine over the candidates' stored embeddings. Picks the
/// item maximizing `lambda*rel - (1-lambda)*max_sim_to_selected` each step. `k` caps
/// the output (0 ⇒ all). Candidates with no embedding still participate (sim treated as
/// 0, so they are neither boosted nor penalized for diversity). Degrades to the input
/// order when there are no embeddings at all (never errs).
fn rank_mmr(semantic: &SemanticStore, input: RowSet, lambda: f32, k: usize) -> RowSet {
    let rows = input.rows();
    let n = rows.len();
    if n <= 1 {
        return input;
    }
    let lambda = lambda.clamp(0.0, 1.0);
    // Relevance per row: use the carried score; if a row lacks one, fall back to a
    // position-based decay so earlier (already-ranked) rows are more relevant.
    let rel: Vec<f32> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| r.score.unwrap_or(1.0 / (1.0 + i as f32)))
        .collect();
    let embs: Vec<Option<Vec<f32>>> = rows.iter().map(|r| semantic.get_embedding(&r.id)).collect();
    let limit = if k == 0 { n } else { k.min(n) };

    let mut selected: Vec<usize> = Vec::with_capacity(limit);
    let mut remaining: Vec<usize> = (0..n).collect();
    while selected.len() < limit && !remaining.is_empty() {
        // pick the remaining index with the best MMR score
        let mut best_pos = 0usize;
        let mut best_val = f32::NEG_INFINITY;
        for (pos, &idx) in remaining.iter().enumerate() {
            let max_sim = selected
                .iter()
                .filter_map(|&s| match (&embs[idx], &embs[s]) {
                    (Some(a), Some(b)) => Some(cosine(a, b)),
                    _ => None,
                })
                .fold(0.0f32, f32::max);
            let mmr = lambda * rel[idx] - (1.0 - lambda) * max_sim;
            // tie-break deterministically by id
            if mmr > best_val || (mmr == best_val && rows[idx].id < rows[remaining[best_pos]].id) {
                best_val = mmr;
                best_pos = pos;
            }
        }
        selected.push(remaining.remove(best_pos));
    }
    let out: Vec<(String, f32)> = selected
        .iter()
        .enumerate()
        .map(|(rank, &idx)| (rows[idx].id.clone(), 1.0 / (1.0 + rank as f32)))
        .collect();
    RowSet::from_scored(out)
}

/// Cosine similarity over two equal-length vectors (0 on degenerate input).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|y| y * y).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// Stable score-desc sort (ties by id) so a reranker's output is deterministic.
fn sort_by_score_desc(mut scored: Vec<(String, f32)>) -> Vec<(String, f32)> {
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored
}

// ── the FILTER op — relational (DataFusion) + spatial (eg-geo) preds ─────────────

/// Execute a `Filter` op. Relational preds (`Eq`/`GtNum`/`LtNum`) run through real
/// DataFusion (`sql_filter_ids`), preserving the input order + candidate-set pushdown.
/// Spatial preds (`SpatialWithin`/`SpatialDWithin`, CONCEPT:EG-KG.ontology.singles-concept) are split OUT and
/// applied per-row by eg-geo against each row's stored geometry — DataFusion has no
/// spatial. A non-geo build has no spatial Pred variants, so every pred is relational
/// and this is byte-for-byte the original SQL path.
fn filter_op(ctx: &PlanCtx, preds: &[Pred], input: RowSet) -> Result<RowSet, String> {
    // Partition per-row preds (spatial via eg-geo, JSON via eg-core::jsonpath —
    // CONCEPT:EG-KG.compute.json-deep-indexing) from relational preds lowered to SQL. DataFusion has neither
    // spatial nor JSONPath, so each is split out and applied per-row against the stored
    // blob. A non-geo build has no spatial variants; JSONPath is always present under
    // `query`, so `relational` is every remaining pred.
    let mut relational: Vec<Pred> = Vec::with_capacity(preds.len());
    #[cfg(feature = "geo")]
    let mut spatial: Vec<Pred> = Vec::new();
    let mut jsonpath: Vec<Pred> = Vec::new();
    for p in preds {
        #[cfg(feature = "geo")]
        if is_spatial_pred(p) {
            spatial.push(p.clone());
            continue;
        }
        if is_jsonpath_pred(p) {
            jsonpath.push(p.clone());
            continue;
        }
        relational.push(p.clone());
    }

    // Predicate pushdown across the modality boundary: when the input is already a
    // candidate set (from a prior TRAVERSE/RANK), restrict the relational scan to those
    // ids (`id IN (...)`) instead of the whole graph.
    let restrict: Option<Vec<String>> = if input.is_empty() {
        None
    } else {
        Some(input.ids())
    };
    let passed = sql_filter_ids(ctx.view, &relational, restrict.as_deref())?;
    // Preserve the input's order (so a vector-first plan stays ranked); if there was no
    // input (Filter is the source), the SQL order is the order.
    let mut out = if input.is_empty() {
        RowSet::from_ids(passed)
    } else {
        let passed_set: HashSet<&str> = passed.iter().map(String::as_str).collect();
        input.intersect_keep_order(&passed_set)
    };

    // Apply each spatial predicate against the stored per-row geometry (order-preserving).
    #[cfg(feature = "geo")]
    for p in &spatial {
        out = spatial_filter(ctx.view, out, p)?;
    }
    // Apply each JSONPath predicate against the stored per-row JSON (CONCEPT:EG-KG.compute.json-deep-indexing),
    // order- and score-preserving — exactly the spatial leg's shape.
    for p in &jsonpath {
        out = jsonpath_filter(ctx.view, out, p)?;
    }
    Ok(out)
}

// ── the document/JSON leg — eg-core::jsonpath over the GraphView blobs (EG-084) ──

/// Is `pred` a JSONPath predicate (evaluated per-row by `eg_core::jsonpath`, NOT lowered
/// to SQL)? (CONCEPT:EG-KG.compute.json-deep-indexing.)
fn is_jsonpath_pred(pred: &Pred) -> bool {
    matches!(pred, Pred::JsonPath { .. })
}

/// FILTER (document/JSON, CONCEPT:EG-KG.compute.json-deep-indexing): keep rows whose stored JSON satisfies the
/// JSONPath predicate `pred`. Each row's blob is decoded to a `serde_json::Value` and the
/// path/op is evaluated by `eg_core::jsonpath` (existence / `->>`-equality / `@>`
/// containment). Rows with no/undecodable blob are dropped. Order- and score-preserving
/// via [`RowSet::intersect_keep_order`] — the same shape as `spatial_filter`.
fn jsonpath_filter(view: &GraphView, input: RowSet, pred: &Pred) -> Result<RowSet, String> {
    use eg_types::wire::JsonPathOp;
    let Pred::JsonPath { path, op } = pred else {
        // Non-JSON preds never reach here (`filter_op` splits them out).
        return Ok(input);
    };
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_json(view, &r.id).is_some_and(|v| match op {
                JsonPathOp::Exists => eg_core::jsonpath::path_exists(&v, path),
                JsonPathOp::Eq { value } => eg_core::jsonpath::path_eq(&v, path, value),
                JsonPathOp::Contains { value } => eg_core::jsonpath::path_contains(&v, path, value),
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    Ok(input.intersect_keep_order(&keep))
}

/// Decode node `id`'s stored property blob to a `serde_json::Value` (CONCEPT:EG-KG.compute.json-deep-indexing) —
/// the JSON leg's counterpart to `row_geometry`.
fn row_json(view: &GraphView, id: &str) -> Option<serde_json::Value> {
    let blob = view.node_properties.get(id)?;
    rmp_serde::from_slice(blob.as_slice()).ok()
}

// ── the spatial leg — eg-geo geometry / R-tree over the GraphView blobs (EG-083) ─

/// SOURCE (spatial): every node in `layer` (matched on the `type` property, exactly as
/// [`scan_label`]) whose stored geometry's bounding box intersects `bbox`, found via
/// eg-geo's packed Hilbert R-tree (CONCEPT:EG-KG.ontology.singles-concept).
///
/// L37 (CONCEPT:EG-KG.storage.incremental-spatial) — when `ctx.spatial` is bound (a served
/// query with a `ServerIndexFactory`-installed `GraphSpatialIndex`, via the
/// `ServedSpatialIndex` adapter), the bbox query pushes straight down into that
/// MAINTAINED persistent index — no per-call rebuild. Otherwise (`ctx.spatial` is
/// `None` — a bare test harness, or a graph created before the factory was wired) this
/// keeps the prior v1 behavior: each node's geometry is read as a WKT string from a
/// conventional `geometry` (or `geom`) property — a dep-free view scan, mirroring how
/// `scan_label`/`as_of_filter` decode the blob — and an EPHEMERAL R-tree is built over
/// the layer's geometries and queried once, exactly as before this fix.
#[cfg(feature = "geo")]
fn spatial_scan(ctx: &PlanCtx, layer: &str, bbox: [f64; 4]) -> RowSet {
    if let Some(src) = ctx.spatial {
        return RowSet::from_ids(src.query_bbox(layer, bbox));
    }
    let view = ctx.view;
    let mut ids: Vec<String> = Vec::new();
    let mut boxes: Vec<(usize, eg_geo::Bbox)> = Vec::new();
    for (id, blob) in view.node_properties.iter() {
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
            continue;
        };
        if v.get("type").and_then(|t| t.as_str()) != Some(layer) {
            continue;
        }
        let Some(bb) = geometry_from_value(&v).and_then(|g| g.bbox()) else {
            continue;
        };
        let idx = ids.len();
        ids.push(id.clone());
        boxes.push((idx, bb));
    }
    let rtree = eg_geo::RTree::build(&boxes);
    let hits = rtree.query_bbox(&eg_geo::Bbox::from_array(bbox));
    RowSet::from_ids(hits.into_iter().map(|i| ids[i].clone()))
}

/// Is `pred` a spatial predicate (evaluated per-row by eg-geo, NOT lowered to SQL)?
/// Covers EG-083's within/dwithin plus the EG-258 DE-9IM relation set.
#[cfg(feature = "geo")]
fn is_spatial_pred(pred: &Pred) -> bool {
    matches!(
        pred,
        Pred::SpatialWithin { .. }
            | Pred::SpatialDWithin { .. }
            | Pred::SpatialContains { .. }
            | Pred::SpatialCovers { .. }
            | Pred::SpatialTouches { .. }
            | Pred::SpatialCrosses { .. }
            | Pred::SpatialOverlaps { .. }
            | Pred::SpatialEquals { .. }
            | Pred::SpatialDisjoint { .. }
    )
}

/// FILTER (spatial): keep rows whose stored geometry satisfies `pred` (CONCEPT:EG-KG.ontology.singles-concept for
/// within/dwithin; CONCEPT:EG-KG.ontology.de-9im-relations for the DE-9IM relations), reading each row's geometry
/// as WKT from the node property named by the pred's `column`. The relation is evaluated
/// as `relation(row_geometry, query_geometry)` — e.g. `SpatialContains` keeps rows whose
/// geometry CONTAINS the query. Rows with no/invalid geometry are dropped. Order- and
/// score-preserving via [`RowSet::intersect_keep_order`].
#[cfg(feature = "geo")]
fn spatial_filter(view: &GraphView, input: RowSet, pred: &Pred) -> Result<RowSet, String> {
    /// The relation to test between the row geometry and the query geometry.
    enum Rel {
        Within,
        DWithin(f64),
        Contains,
        Covers,
        Touches,
        Crosses,
        Overlaps,
        Equals,
        Disjoint,
    }
    let (column, wkt, rel): (&str, &str, Rel) = match pred {
        Pred::SpatialWithin { column, wkt } => (column, wkt, Rel::Within),
        Pred::SpatialDWithin {
            column,
            wkt,
            distance,
        } => (column, wkt, Rel::DWithin(*distance)),
        Pred::SpatialContains { column, wkt } => (column, wkt, Rel::Contains),
        Pred::SpatialCovers { column, wkt } => (column, wkt, Rel::Covers),
        Pred::SpatialTouches { column, wkt } => (column, wkt, Rel::Touches),
        Pred::SpatialCrosses { column, wkt } => (column, wkt, Rel::Crosses),
        Pred::SpatialOverlaps { column, wkt } => (column, wkt, Rel::Overlaps),
        Pred::SpatialEquals { column, wkt } => (column, wkt, Rel::Equals),
        Pred::SpatialDisjoint { column, wkt } => (column, wkt, Rel::Disjoint),
        // Relational preds never reach here (`filter_op` splits them out).
        _ => return Ok(input),
    };
    let query_geom = eg_geo::parse_wkt(wkt)?;
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry(view, &r.id, column).is_some_and(|g| match rel {
                Rel::Within => eg_geo::within(&g, &query_geom),
                Rel::DWithin(d) => eg_geo::distance(&g, &query_geom) <= d,
                Rel::Contains => eg_geo::contains(&g, &query_geom),
                Rel::Covers => eg_geo::covers(&g, &query_geom),
                Rel::Touches => eg_geo::touches(&g, &query_geom),
                Rel::Crosses => eg_geo::crosses(&g, &query_geom),
                Rel::Overlaps => eg_geo::overlaps(&g, &query_geom),
                Rel::Equals => eg_geo::equals(&g, &query_geom),
                Rel::Disjoint => eg_geo::disjoint(&g, &query_geom),
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    Ok(input.intersect_keep_order(&keep))
}

/// TRANSFORM (spatial CRS, CONCEPT:EG-KG.domains.coordinate-reference-system): reproject each row's stored geometry into
/// `to_epsg`. The source CRS is the row geometry's EWKT `SRID=…;` tag, or `from_epsg` when
/// given (an explicit override always wins). Rows with no geometry, no resolvable source
/// CRS, or an unsupported/failed reprojection are DROPPED — order- and score-preserving,
/// exactly as the tensor/spatial legs. A v1 executor does NOT persist the reprojected
/// geometry back to the blob (mirroring `tensor_op`); it validates the transform per row.
#[cfg(feature = "geo")]
fn spatial_reproject(
    view: &GraphView,
    input: RowSet,
    to_epsg: u32,
    from_epsg: Option<u32>,
) -> RowSet {
    let to = eg_geo::Crs::from_epsg(to_epsg);
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry_srid(view, &r.id).is_some_and(|(srid, g)| {
                match from_epsg.or(srid).map(eg_geo::Crs::from_epsg) {
                    Some(from) => eg_geo::reproject(&g, from, to).is_ok(),
                    None => false, // unknown source CRS ⇒ cannot reproject
                }
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// TRANSFORM (constructive, CONCEPT:EG-KG.ontology.concept-9): apply the eg-geo `algebra` op `kind` to each
/// row's stored geometry (conventional `geometry`/`geom` property), producing a derived
/// geometry. Rows whose geometry is missing/invalid or where the op yields nothing (e.g.
/// an empty intersection, a degenerate centroid) are DROPPED — order- and score-preserving,
/// exactly as [`tensor_op`]. The derived geometry validates the op per row; a v1 executor
/// does NOT persist it back to the blob (the documented follow-up).
#[cfg(feature = "geo")]
fn spatial_op(view: &GraphView, input: RowSet, kind: &eg_types::wire::SpatialOpKind) -> RowSet {
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry_conv(view, &r.id).is_some_and(|g| apply_spatial_op(&g, kind).is_some())
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// Map the pure-serde wire `SpatialOpKind` (eg-types) to the eg-geo `algebra` op and run
/// it, returning the derived geometry (`None` when the op has no result). Unary ops always
/// succeed; the binary WKT-operand ops parse the second geometry then apply the op.
#[cfg(feature = "geo")]
fn apply_spatial_op(
    g: &eg_geo::Geometry,
    kind: &eg_types::wire::SpatialOpKind,
) -> Option<eg_geo::Geometry> {
    use eg_types::wire::SpatialOpKind;
    match kind {
        SpatialOpKind::Buffer { distance } => Some(eg_geo::buffer(g, *distance)),
        SpatialOpKind::ConvexHull => Some(eg_geo::convex_hull(g)),
        SpatialOpKind::Simplify { tolerance } => Some(eg_geo::simplify(g, *tolerance)),
        SpatialOpKind::Centroid => eg_geo::centroid(g).map(eg_geo::Geometry::Point),
        SpatialOpKind::Union { wkt } => eg_geo::parse_wkt(wkt).ok().map(|o| eg_geo::union(g, &o)),
        SpatialOpKind::Intersection { wkt } => eg_geo::parse_wkt(wkt)
            .ok()
            .and_then(|o| eg_geo::intersection(g, &o)),
        SpatialOpKind::Difference { wkt } => eg_geo::parse_wkt(wkt)
            .ok()
            .and_then(|o| eg_geo::difference(g, &o)),
    }
}

/// Read node `id`'s geometry from the WKT string in property `column` of its blob.
#[cfg(feature = "geo")]
fn row_geometry(view: &GraphView, id: &str, column: &str) -> Option<eg_geo::Geometry> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let wkt = v.get(column)?.as_str()?;
    eg_geo::parse_wkt(wkt).ok()
}

/// Read a geometry from a node's decoded blob via the conventional `geometry`/`geom`
/// WKT property (used by the layer-wide `SpatialScan`).
#[cfg(feature = "geo")]
fn geometry_from_value(v: &serde_json::Value) -> Option<eg_geo::Geometry> {
    let wkt = v.get("geometry").or_else(|| v.get("geom"))?.as_str()?;
    eg_geo::parse_wkt(wkt).ok()
}

/// Read node `id`'s geometry from the conventional `geometry`/`geom` WKT property (the
/// column-free read used by `Op::SpatialOp`, CONCEPT:EG-KG.ontology.concept-9).
#[cfg(feature = "geo")]
fn row_geometry_conv(view: &GraphView, id: &str) -> Option<eg_geo::Geometry> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    geometry_from_value(&v)
}

/// Read node `id`'s geometry AND its EWKT `SRID=…;` CRS tag from the conventional
/// `geometry`/`geom` WKT property (used by `Op::Reproject`, CONCEPT:EG-KG.domains.coordinate-reference-system). The `srid`
/// is `None` when the stored WKT carries no EWKT prefix.
#[cfg(feature = "geo")]
fn row_geometry_srid(view: &GraphView, id: &str) -> Option<(Option<u32>, eg_geo::Geometry)> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let wkt = v.get("geometry").or_else(|| v.get("geom"))?.as_str()?;
    eg_geo::parse_with_srid(wkt).ok()
}

// ── the tensor leg — eg-tensor N-D arrays over the GraphView blobs (EG-085) ──────

/// SOURCE (tensor): every node in `layer` (matched on the `type` property, exactly as
/// [`scan_label`]) that carries a valid dense N-D array in the conventional `tensor`
/// property (CONCEPT:EG-KG.storage.content-addressed-dedup). A dep-free view scan, mirroring `spatial_scan` — the
/// matching ids seed the RowSet; the tensor itself is read per-row by [`tensor_op`].
/// (Content-addressing the tensor bytes in the blob CAS — `ChunkStore` + EG-071 — is a
/// documented follow-up; v1 reads the tensor as a typed node property off the live
/// snapshot.)
#[cfg(feature = "tensor")]
fn tensor_scan(view: &GraphView, layer: &str) -> RowSet {
    let mut ids: Vec<String> = Vec::new();
    for (id, blob) in view.node_properties.iter() {
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
            continue;
        };
        if v.get("type").and_then(|t| t.as_str()) != Some(layer) {
            continue;
        }
        if tensor_from_value(&v).is_some() {
            ids.push(id.clone());
        }
    }
    RowSet::from_ids(ids)
}

/// TRANSFORM (tensor): apply the eg-tensor op `kind` (slice/reduce/elementwise) to each
/// row's stored tensor (CONCEPT:EG-KG.storage.content-addressed-dedup). Rows whose tensor is missing/invalid or where
/// the op fails (e.g. a slice out of bounds) are DROPPED — order- and score-preserving
/// via [`RowSet::intersect_keep_order`], exactly as the spatial `Filter` leg drops rows
/// with no geometry.
///
/// CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — CAS write-back: when a [`PlanCtx::tensor_store`] is attached, each
/// derived tensor that the op successfully produces is WRITTEN BACK into that
/// content-addressed [`eg_tensor::TensorStore`] (via [`eg_tensor::TensorStore::put`]),
/// so the result becomes a durable, dedup-shared blob addressable by its deterministic
/// content hash — closing the EG-085 follow-up. The write-back is additive and does NOT
/// change which rows survive: a row is kept iff the op succeeds, exactly as before, so a
/// `None` store yields byte-for-byte the prior (validate-only) behavior. Deterministic:
/// identical derived tensors content-address to one blob (idempotent dedup) regardless of
/// row order or how many times the plan runs.
#[cfg(feature = "tensor")]
fn tensor_op(ctx: &PlanCtx, input: RowSet, kind: &eg_types::wire::TensorOpKind) -> RowSet {
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            // Compute the derived tensor to VALIDATE the op per row; on success, write it
            // back to the CAS (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink) when a store is attached.
            match row_tensor(ctx.view, &r.id) {
                Some(t) => match apply_tensor_op(&t, kind) {
                    Ok(derived) => {
                        if let Some(store) = ctx.tensor_store {
                            // Recover from a poisoned lock: `put` is infallible, so the
                            // guard is never left in a bad state; keep the write-back
                            // durable rather than propagating a poison panic.
                            store
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .put(&derived);
                        }
                        true
                    }
                    Err(_) => false,
                },
                None => false,
            }
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// Read node `id`'s tensor from the conventional `tensor` property of its blob
/// (CONCEPT:EG-KG.storage.content-addressed-dedup), decoding the typed serde form.
#[cfg(feature = "tensor")]
fn row_tensor(view: &GraphView, id: &str) -> Option<eg_tensor::Tensor> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    tensor_from_value(&v)
}

/// Read a tensor from a node's decoded blob via the conventional `tensor` property.
#[cfg(feature = "tensor")]
fn tensor_from_value(v: &serde_json::Value) -> Option<eg_tensor::Tensor> {
    serde_json::from_value::<eg_tensor::Tensor>(v.get("tensor")?.clone()).ok()
}

/// Map the pure-serde wire `TensorOpKind` (eg-types) to the eg-tensor op and run it.
/// The wire enums are defined in eg-types (Pi-safe, no eg-tensor dep); this seam turns
/// them into the real N-D array math behind the `tensor` gate.
#[cfg(feature = "tensor")]
fn apply_tensor_op(
    t: &eg_tensor::Tensor,
    kind: &eg_types::wire::TensorOpKind,
) -> Result<eg_tensor::Tensor, String> {
    use eg_types::wire::{TensorElementwiseOp, TensorOpKind, TensorReduceKind};
    match kind {
        TensorOpKind::Slice { ranges } => t.slice(ranges),
        TensorOpKind::Reduce { axis, kind } => {
            let k = match kind {
                TensorReduceKind::Sum => eg_tensor::ReduceKind::Sum,
                TensorReduceKind::Mean => eg_tensor::ReduceKind::Mean,
                TensorReduceKind::Max => eg_tensor::ReduceKind::Max,
                TensorReduceKind::Min => eg_tensor::ReduceKind::Min,
            };
            t.reduce(*axis, k)
        }
        TensorOpKind::Elementwise { op, scalar } => {
            let o = match op {
                TensorElementwiseOp::Add => eg_tensor::ElementwiseOp::Add,
                TensorElementwiseOp::Sub => eg_tensor::ElementwiseOp::Sub,
                TensorElementwiseOp::Mul => eg_tensor::ElementwiseOp::Mul,
                TensorElementwiseOp::Div => eg_tensor::ElementwiseOp::Div,
            };
            Ok(t.elementwise(o, *scalar))
        }
    }
}

// ── the probabilistic leg — eg-types Distribution + eg-compute Bayes (EG-086) ────

/// TRANSFORM (probabilistic, CONCEPT:EG-KG.compute.uncertainty-values): evaluate `query` against each row's stored
/// `Distribution` value and SCORE the row with the closed-form result, returning the rows
/// re-ordered by that score DESCENDING — a ranked RowSet a downstream `Limit` respects,
/// exactly like `Rank`. Rows whose `distribution` property is missing/invalid, or where
/// the query does not apply (an unsupported `Conditional` conjugate pair), are DROPPED.
/// Deterministic: a `Sample { seed }` draws the SAME value every run (no RNG-from-clock).
#[cfg(feature = "probabilistic")]
fn probabilistic_op(view: &GraphView, input: RowSet, query: &eg_types::wire::ProbQuery) -> RowSet {
    let mut scored: Vec<(String, f32)> = input
        .rows()
        .iter()
        .filter_map(|r| {
            let dist = row_distribution(view, &r.id)?;
            let val = eval_prob_query(&dist, query)?;
            Some((r.id.clone(), val as f32))
        })
        .collect();
    // Descending by score → a ranked order (a NaN score sorts last, never above a real one).
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    RowSet::from_scored(scored)
}

/// Read node `id`'s distribution from the conventional `distribution` property of its
/// blob (CONCEPT:EG-KG.compute.uncertainty-values), decoding the tagged serde form of `eg_types::Distribution`.
#[cfg(feature = "probabilistic")]
fn row_distribution(view: &GraphView, id: &str) -> Option<eg_types::Distribution> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    serde_json::from_value::<eg_types::Distribution>(v.get("distribution")?.clone()).ok()
}

/// Lower the pure-serde wire `ProbQuery` (eg-types) onto the real distribution math: the
/// `Distribution` moment/pdf/pmf/sample methods (eg-types) + the conjugate
/// `bayesian_update` (eg-compute) behind the `probabilistic` gate (CONCEPT:EG-KG.compute.uncertainty-values).
/// Returns `None` when the query does not apply (unsupported conjugate pair) so the
/// caller drops the row.
#[cfg(feature = "probabilistic")]
fn eval_prob_query(
    dist: &eg_types::Distribution,
    query: &eg_types::wire::ProbQuery,
) -> Option<f64> {
    use eg_types::wire::{ProbEvidenceSpec, ProbQuery};
    match query {
        ProbQuery::Expectation => Some(dist.mean()),
        ProbQuery::Marginal { at, label } => Some(match label {
            Some(l) => dist.pmf(l),
            None => dist.pdf(*at),
        }),
        ProbQuery::Conditional { evidence } => {
            let ev = match evidence {
                ProbEvidenceSpec::Bernoulli {
                    successes,
                    failures,
                } => eg_compute::probabilistic::Evidence::Bernoulli {
                    successes: *successes,
                    failures: *failures,
                },
                ProbEvidenceSpec::Gaussian {
                    observations,
                    known_variance,
                } => eg_compute::probabilistic::Evidence::Gaussian {
                    observations: observations.clone(),
                    known_variance: *known_variance,
                },
            };
            eg_compute::probabilistic::bayesian_update(dist, &ev)
                .ok()
                .map(|post| post.mean())
        }
        ProbQuery::Sample { seed } => Some(dist.sample(*seed)),
    }
}

// ── the epistemic leg — belief/evidence over the support/contradiction/attack graph
// (CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) ────────────────────────────────

/// The [`eg_epistemic::AuthorityPolicy`] an epistemic op runs under: the ctx-attached
/// policy (`PlanCtx::with_belief_policy`) or `AuthorityPolicy::default()`.
#[cfg(feature = "epistemic")]
fn belief_policy(ctx: &PlanCtx) -> eg_epistemic::AuthorityPolicy {
    ctx.belief_policy.unwrap_or_default()
}

/// Which direction an epistemic edge-lookup reads (CONCEPT:EG-KG.epistemic.epistemic-substrate):
/// `Incoming` — edges pointing AT `id` (evidence bearing ON it, `EvidenceFor`/`Contradicts`);
/// `Outgoing` — edges pointing FROM `id` (what `id` itself supports, `SupportedBy`).
/// [`eg_epistemic::BeliefGraph`] only indexes incoming edges (the propagation walk only
/// ever needs a node's OWN evidence), so the outgoing direction is a linear scan over
/// every target's incoming list — cheap at plan scale (the belief substrate is a thin
/// projection, not a full traversal index).
#[cfg(feature = "epistemic")]
enum EdgeDir {
    Incoming,
    Outgoing,
}

/// Collect the ids linked to `id` by an edge classified as one of `kinds`, in `dir`
/// (CONCEPT:EG-KG.epistemic.epistemic-substrate). The shared lookup behind `EvidenceFor`/
/// `Contradicts` (incoming) and `SupportedBy` (outgoing).
#[cfg(feature = "epistemic")]
fn linked_ids(
    bg: &eg_epistemic::BeliefGraph,
    id: &str,
    kinds: &[eg_epistemic::EdgeKind],
    dir: EdgeDir,
) -> HashSet<String> {
    match dir {
        EdgeDir::Incoming => bg
            .in_edges
            .get(id)
            .into_iter()
            .flatten()
            .filter(|(_, k)| kinds.contains(k))
            .map(|(src, _)| src.clone())
            .collect(),
        EdgeDir::Outgoing => bg
            .in_edges
            .iter()
            .flat_map(|(target, edges)| {
                edges
                    .iter()
                    .filter(|(src, k)| src == id && kinds.contains(k))
                    .map(move |_| target.clone())
            })
            .collect(),
    }
}

/// SOURCE/FILTER shape shared by `EvidenceFor`/`Contradicts`/`SupportedBy`/`ExplainBelief`
/// (CONCEPT:EG-KG.epistemic.epistemic-substrate): an empty `input` (leaf position) SEEDS the
/// RowSet with `matches`; a non-empty `input` (mid-pipeline) narrows it to the rows that are
/// ALSO in `matches`, preserving `input`'s order — the same seed-or-filter shape
/// `as_of_filter` already uses.
#[cfg(feature = "epistemic")]
fn seed_or_filter(input: RowSet, matches: &HashSet<String>) -> RowSet {
    if input.is_empty() {
        return RowSet::from_ids(matches.iter().cloned());
    }
    let keep: HashSet<&str> = matches.iter().map(|s| s.as_str()).collect();
    input.intersect_keep_order(&keep)
}

/// `EvidenceFor { claim_id }` — the nodes with an INCOMING `Supports` edge into
/// `claim_id` (CONCEPT:EG-KG.epistemic.epistemic-substrate).
#[cfg(feature = "epistemic")]
fn evidence_for_op(view: &GraphView, input: RowSet, claim_id: &str) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let matches = linked_ids(
        &bg,
        claim_id,
        &[eg_epistemic::EdgeKind::Supports],
        EdgeDir::Incoming,
    );
    seed_or_filter(input, &matches)
}

/// `Contradicts { node_id }` — the nodes with an INCOMING `Contradicts` OR `Attacks` edge
/// into `node_id` (an attack is a stronger contradiction, so both count,
/// CONCEPT:EG-KG.epistemic.epistemic-substrate).
#[cfg(feature = "epistemic")]
fn contradicts_op(view: &GraphView, input: RowSet, node_id: &str) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let matches = linked_ids(
        &bg,
        node_id,
        &[
            eg_epistemic::EdgeKind::Contradicts,
            eg_epistemic::EdgeKind::Attacks,
        ],
        EdgeDir::Incoming,
    );
    seed_or_filter(input, &matches)
}

/// `SupportedBy { node_id }` — the nodes reached by an OUTGOING `Supports` edge FROM
/// `node_id` (the mirror direction of `EvidenceFor`, CONCEPT:EG-KG.epistemic.epistemic-substrate).
#[cfg(feature = "epistemic")]
fn supported_by_op(view: &GraphView, input: RowSet, node_id: &str) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(view);
    let matches = linked_ids(
        &bg,
        node_id,
        &[eg_epistemic::EdgeKind::Supports],
        EdgeDir::Outgoing,
    );
    seed_or_filter(input, &matches)
}

/// `BELIEF AS OF <ts>` (`Op::BeliefAsOf`) — pin the TRANSACTION-time axis (reuses
/// [`as_of_filter`]) then RE-SCORE every surviving row by its propagated belief confidence
/// at that instant (CONCEPT:EG-KG.epistemic.epistemic-substrate). The `BeliefGraph` is built
/// once over the WHOLE snapshot (propagation may need to walk beyond the post-filter
/// candidate set to reach a supporting/contradicting node not itself live at `ts`) and
/// pinned via [`eg_epistemic::BeliefGraph::pinned_at`] for provenance.
#[cfg(feature = "epistemic")]
fn belief_as_of_op(ctx: &PlanCtx, input: RowSet, ts: f64) -> RowSet {
    let filtered = as_of_filter(ctx.view, input, ts, TimeAxis::Transaction);
    let bg = eg_epistemic::BeliefGraph::from_graph_view(ctx.view)
        .pinned_at(eg_epistemic::TimeAxis::Transaction, ts.max(0.0) as u64);
    let policy = belief_policy(ctx);
    rescore_by_confidence(&bg, filtered, &policy)
}

/// `SourceReliability { source_id }` — re-weight every row currently in `input` by the
/// propagated reliability of `source_id` (CONCEPT:EG-KG.epistemic.epistemic-substrate): a
/// uniform scalar multiplier over existing scores (an unscored row is treated as `1.0`).
#[cfg(feature = "epistemic")]
fn source_reliability_op(ctx: &PlanCtx, input: RowSet, source_id: &str) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(ctx.view);
    let policy = belief_policy(ctx);
    let r = eg_epistemic::propagate_confidence(&bg, source_id, &policy).confidence as f32;
    let scored = input
        .rows()
        .iter()
        .map(|row| (row.id.clone(), row.score.unwrap_or(1.0) * r));
    RowSet::from_scored(scored)
}

/// `CONFIDENCE` (`Op::ConfidenceOp`) — re-score EACH row in `input` by its OWN propagated
/// belief confidence, ranked descending (CONCEPT:EG-KG.epistemic.epistemic-substrate).
#[cfg(feature = "epistemic")]
fn confidence_op(ctx: &PlanCtx, input: RowSet) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(ctx.view);
    let policy = belief_policy(ctx);
    rescore_by_confidence(&bg, input, &policy)
}

/// Re-score every row in `input` by its own propagated belief confidence, ranked
/// descending — the shared scoring pass `BeliefAsOf` and `ConfidenceOp` both reduce to.
#[cfg(feature = "epistemic")]
fn rescore_by_confidence(
    bg: &eg_epistemic::BeliefGraph,
    input: RowSet,
    policy: &eg_epistemic::AuthorityPolicy,
) -> RowSet {
    let mut scored: Vec<(String, f32)> = input
        .rows()
        .iter()
        .map(|row| {
            let c = eg_epistemic::propagate_confidence(bg, &row.id, policy).confidence;
            (row.id.clone(), c as f32)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    RowSet::from_scored(scored)
}

/// The FULL recursive [`eg_epistemic::JustificationGraph`] rooted at `node_id`
/// (CONCEPT:EG-KG.epistemic.epistemic-substrate, E1) — the un-flattened tree
/// `Op::ExplainBelief` (E2) projects down into the flat `RowSet` currency via
/// [`flatten_proof`]. A standalone `Method::ExplainBelief` (E5 phase 4, mirroring
/// `Method::OwlExplain`'s `ProofNodeWire`) wants the verbatim tree — rule names, premise
/// structure, per-node confidence — which the plan-`Op` surface cannot carry (E2's
/// documented scope boundary). Public so the facade handler can build the wire
/// projection directly, reusing the SAME [`belief_policy`] resolution `explain_belief_op`
/// uses.
#[cfg(feature = "epistemic")]
pub fn explain_belief_tree(ctx: &PlanCtx, node_id: &str) -> eg_epistemic::JustificationGraph {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(ctx.view);
    let policy = belief_policy(ctx);
    eg_epistemic::explain_belief(&bg, node_id, &policy)
}

/// `EXPLAIN BELIEF <node_id>` (`Op::ExplainBelief`) — build the recursive justification
/// tree ([`eg_epistemic::explain_belief`]) rooted at `node_id`, flatten it pre-order
/// (deduped) to `(claim_id, confidence)` pairs, and seed-or-filter the RowSet exactly like
/// `EvidenceFor`/`Contradicts` (CONCEPT:EG-KG.epistemic.epistemic-substrate). The FULL nested
/// tree (rule names, premise structure) does not fit the flat `RowSet` currency — this is
/// the queryable, composes-in-plan projection of it; [`explain_belief_tree`] above returns
/// the verbatim tree for the standalone `Method::ExplainBelief` wire surface.
#[cfg(feature = "epistemic")]
fn explain_belief_op(ctx: &PlanCtx, input: RowSet, node_id: &str) -> RowSet {
    let bg = eg_epistemic::BeliefGraph::from_graph_view(ctx.view);
    let policy = belief_policy(ctx);
    let jg = eg_epistemic::explain_belief(&bg, node_id, &policy);
    let mut flat: Vec<(String, f32)> = Vec::new();
    let mut seen = HashSet::new();
    flatten_proof(&jg.root, &mut flat, &mut seen);
    if input.is_empty() {
        return RowSet::from_scored(flat);
    }
    let scores: std::collections::HashMap<&str, f32> =
        flat.iter().map(|(id, s)| (id.as_str(), *s)).collect();
    let scored = input
        .rows()
        .iter()
        .filter_map(|r| scores.get(r.id.as_str()).map(|s| (r.id.clone(), *s)));
    RowSet::from_scored(scored)
}

/// Pre-order flatten of a [`eg_epistemic::ProofNode`] tree into `(claim_id, confidence)`
/// pairs, deduped by first occurrence (CONCEPT:EG-KG.epistemic.epistemic-substrate) — the
/// RowSet-currency projection of `ExplainBelief`'s justification tree.
#[cfg(feature = "epistemic")]
fn flatten_proof(
    node: &eg_epistemic::ProofNode,
    out: &mut Vec<(String, f32)>,
    seen: &mut HashSet<String>,
) {
    if seen.insert(node.claim.clone()) {
        out.push((node.claim.clone(), node.confidence as f32));
    }
    for premise in &node.premises {
        flatten_proof(premise, out, seen);
    }
}

// ── the stream / CEP leg — eg-stream bounded NFA over the GraphView blobs (EG-088) ─

/// The reserved attribute key under which [`cep_op`] stashes each event's originating
/// node id, so a detected [`eg_stream::Match`]'s events can be mapped back to the input
/// rows that participated. Namespaced with a leading `_` to avoid colliding with a
/// user's own event attributes.
#[cfg(feature = "stream")]
const CEP_ID_ATTR: &str = "_eg_row_id";

/// TRANSFORM (stream, CONCEPT:EG-KG.query.pipelined-execution): interpret the input RowSet as a time-ordered
/// event stream — each row's node blob supplies `ts` (u64), `key` (string) and `attrs`
/// (object) — run the eg-stream bounded NFA for `spec.pattern` over `spec.window`, and
/// keep the input rows that PARTICIPATE in a detected match (order- and score-preserving
/// via [`RowSet::intersect_keep_order`], exactly as the spatial/tensor legs narrow their
/// input). Rows whose blob lacks a numeric `ts` are not events and never match.
/// (EG-067 `Op::Window` is the plan-side windowing primitive; a live standing CEP query
/// fed by the EG-064 CDC `ChangeNotifier` bus — matching incrementally as the bus
/// advances the window — is the documented follow-up; this batch executor is what lands.)
#[cfg(feature = "stream")]
fn cep_op(view: &GraphView, input: RowSet, spec: &eg_types::wire::CepPatternSpec) -> RowSet {
    // Build the event stream from the input rows, tagging each event with its source row
    // id so matches map back. `run` sorts by `ts` internally.
    let mut events: Vec<eg_stream::Event> = Vec::new();
    for r in input.rows() {
        if let Some(mut ev) = row_event(view, &r.id) {
            ev.attrs.insert(
                CEP_ID_ATTR.to_string(),
                serde_json::Value::String(r.id.clone()),
            );
            events.push(ev);
        }
    }
    let pattern = cep_pattern_from_spec(&spec.pattern);
    let window = cep_window_from_spec(spec.window);
    let matches = eg_stream::run(&pattern, &events, window);

    let keep_ids: HashSet<String> = matches
        .iter()
        .flat_map(|m| m.events.iter())
        .filter_map(|e| e.attrs.get(CEP_ID_ATTR).and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .collect();
    let keep: HashSet<&str> = keep_ids.iter().map(|s| s.as_str()).collect();
    input.intersect_keep_order(&keep)
}

/// Read node `id`'s event from its blob (CONCEPT:EG-KG.query.pipelined-execution): the conventional `ts` (u64,
/// required — a node with no numeric `ts` is not an event), `key` (string, defaults to
/// empty) and `attrs` (object, defaults to empty).
#[cfg(feature = "stream")]
fn row_event(view: &GraphView, id: &str) -> Option<eg_stream::Event> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let ts = v.get("ts").and_then(|t| t.as_u64())?;
    let key = v
        .get("key")
        .and_then(|k| k.as_str())
        .unwrap_or_default()
        .to_string();
    let attrs = v
        .get("attrs")
        .and_then(|a| a.as_object())
        .cloned()
        .unwrap_or_default();
    Some(eg_stream::Event { ts, key, attrs })
}

/// Map the pure-serde wire `CepWindowSpec` (eg-types) to eg-stream's `Window`.
#[cfg(feature = "stream")]
fn cep_window_from_spec(w: eg_types::wire::CepWindowSpec) -> eg_stream::Window {
    use eg_types::wire::CepWindowSpec;
    match w {
        CepWindowSpec::Sliding { size } => eg_stream::Window::Sliding { size },
        CepWindowSpec::Tumbling { size } => eg_stream::Window::Tumbling { size },
    }
}

/// Map a pure-serde wire `CepMatcherSpec` (eg-types) to eg-stream's `EventMatcher`.
#[cfg(feature = "stream")]
fn cep_matcher_from_spec(m: &eg_types::wire::CepMatcherSpec) -> eg_stream::EventMatcher {
    use eg_types::wire::CepAttrPredSpec;
    let preds = m
        .preds
        .iter()
        .map(|p| match p {
            CepAttrPredSpec::Eq { field, value } => eg_stream::AttrPredicate::Eq {
                field: field.clone(),
                value: value.clone(),
            },
            CepAttrPredSpec::Gt { field, value } => eg_stream::AttrPredicate::Gt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Lt { field, value } => eg_stream::AttrPredicate::Lt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Exists { field } => eg_stream::AttrPredicate::Exists {
                field: field.clone(),
            },
        })
        .collect();
    eg_stream::EventMatcher {
        key: m.key.clone(),
        preds,
    }
}

/// Map the pure-serde wire `CepNodeSpec` tree (eg-types) to eg-stream's `CepPattern`.
/// The wire enums are Pi-safe (no eg-stream dep); this seam turns them into the real NFA
/// pattern behind the `stream` gate.
#[cfg(feature = "stream")]
fn cep_pattern_from_spec(p: &eg_types::wire::CepNodeSpec) -> eg_stream::CepPattern {
    use eg_types::wire::CepNodeSpec;
    match p {
        CepNodeSpec::Sequence(matchers) => {
            eg_stream::CepPattern::Sequence(matchers.iter().map(cep_matcher_from_spec).collect())
        }
        CepNodeSpec::Within { within, pattern } => eg_stream::CepPattern::Within {
            within: *within,
            pattern: Box::new(cep_pattern_from_spec(pattern)),
        },
        CepNodeSpec::Absence { a, b, within } => eg_stream::CepPattern::Absence {
            a: cep_matcher_from_spec(a),
            b: cep_matcher_from_spec(b),
            within: *within,
        },
    }
}

// ── the relational FILTER leg — real DataFusion via eg-query ────────────────────

/// Compile `preds` to a SQL `WHERE` fragment. Numeric literals are emitted bare;
/// string equality is single-quote-escaped. (The planner could equally hand
/// DataFusion a pre-built `LogicalPlan`; a string keeps the leg legible and reuses
/// `eg_query::exec_sql` verbatim — DataFusion as the relational sub-engine.)
fn where_clause(preds: &[Pred]) -> String {
    if preds.is_empty() {
        return "1=1".into();
    }
    preds
        .iter()
        .map(|p| match p {
            Pred::Eq { prop, value } => {
                format!("{prop} = '{}'", value.replace('\'', "''"))
            }
            Pred::GtNum { prop, n } => format!("{prop} > {n}"),
            Pred::LtNum { prop, n } => format!("{prop} < {n}"),
            // JSONPath preds (CONCEPT:EG-KG.compute.json-deep-indexing) are NOT lowered to SQL — `filter_op` splits
            // them out and applies them per-row via `eg_core::jsonpath`, so they never
            // reach here. This defensive arm keeps the match exhaustive: a no-op `1=1`.
            Pred::JsonPath { .. } => "1=1".into(),
            // Spatial preds (CONCEPT:EG-KG.ontology.singles-concept / EG-258) are NOT lowered to SQL — `filter_op`
            // splits them out and applies them per-row via eg-geo, so they never reach here.
            // This defensive arm keeps the match exhaustive under `geo`: a no-op `1=1`.
            #[cfg(feature = "geo")]
            Pred::SpatialWithin { .. }
            | Pred::SpatialDWithin { .. }
            | Pred::SpatialContains { .. }
            | Pred::SpatialCovers { .. }
            | Pred::SpatialTouches { .. }
            | Pred::SpatialCrosses { .. }
            | Pred::SpatialOverlaps { .. }
            | Pred::SpatialEquals { .. }
            | Pred::SpatialDisjoint { .. } => "1=1".into(),
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Run the FILTER leg through real DataFusion (`eg_query::exec_sql`) over the
/// schema-on-read `nodes` provider, returning the matching node ids in scan order.
///
/// `restrict_to`: when `Some`, an `id IN (...)` is appended — the
/// predicate-pushdown-across-the-modality-boundary primitive used when a prior
/// TRAVERSE/RANK already narrowed the candidate set (filter only those, not the
/// whole graph).
fn sql_filter_ids(
    view: &GraphView,
    preds: &[Pred],
    restrict_to: Option<&[String]>,
) -> Result<Vec<String>, String> {
    let mut sql = format!("SELECT id FROM nodes WHERE {}", where_clause(preds));
    if let Some(ids) = restrict_to {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let in_list = ids
            .iter()
            .map(|i| format!("'{}'", i.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" AND id IN ({in_list})"));
    }

    // `exec_sql` builds its own current-thread runtime to drive DataFusion's async
    // collect (safe to call inside spawn_blocking, exactly as the Sql handler does).
    let result = eg_query::exec_sql(view, &sql)?;
    // `result.rows[i]` is a MessagePack-encoded `Vec<serde_json::Value>` aligned to
    // `result.columns` (here a single `id` column). Decode the id cell of each row.
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let cells: Vec<serde_json::Value> =
            rmp_serde::from_slice(row).map_err(|e| format!("decode filter row: {e}"))?;
        match cells.first().and_then(|v| v.as_str()) {
            Some(id) => out.push(id.to_string()),
            None => return Err("filter result row had no string id cell".into()),
        }
    }
    Ok(out)
}

// ── the graph TRAVERSE leg — petgraph BFS over the GraphView topology ───────────

/// BFS over the petgraph topology following OUTGOING edges of relationship `rel`,
/// for `min..=max` hops. Returns the reached node ids — matching eg-query/cypher's
/// variable-length-hop traversal.
///
/// IMPORTANT (a real data-model detail): the petgraph edge *weight* is the synthetic
/// `"{src}:{tgt}"` string (`GraphCore::add_edge`), NOT the relationship type — the
/// relationship lives in the edge's property blob (`relationship`/`type` field),
/// exactly as eg-query/cypher's `rel_matches` reads it. So the BFS matches on the
/// blob, not the weight.
pub(crate) fn bfs_reached(
    view: &GraphView,
    seeds: &[String],
    rel: &str,
    min: usize,
    max: usize,
) -> Vec<String> {
    use petgraph::visit::EdgeRef;
    let mut reached: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    let mut frontier: Vec<petgraph::stable_graph::NodeIndex> = seeds
        .iter()
        .filter_map(|s| view.node_map.get(s).copied())
        .collect();
    let mut visited: HashSet<petgraph::stable_graph::NodeIndex> =
        frontier.iter().copied().collect();

    let mut depth = 0;
    while depth < max && !frontier.is_empty() {
        depth += 1;
        let mut next = Vec::new();
        for &node in &frontier {
            for e in view
                .graph
                .edges_directed(node, petgraph::Direction::Outgoing)
            {
                let from_id = &view.graph[e.source()];
                let to_id = &view.graph[e.target()];
                if !rel_matches(view, from_id, to_id, rel) {
                    continue;
                }
                let nbr = e.target();
                if visited.insert(nbr) {
                    next.push(nbr);
                }
                if depth >= min {
                    let id = view.graph[nbr].clone();
                    if reached.insert(id.clone()) {
                        out.push(id);
                    }
                }
            }
        }
        frontier = next;
    }
    out
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge's
/// property blobs (`relationship` or `type` field) — mirrors eg-query/cypher.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: &str) -> bool {
    let Some(blobs) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return false;
    };
    blobs.iter().any(|b| {
        rmp_serde::from_slice::<serde_json::Value>(b.as_slice())
            .ok()
            .map(|v| {
                let r = v.get("relationship").or_else(|| v.get("type"));
                r.and_then(|x| x.as_str()) == Some(rel)
            })
            .unwrap_or(false)
    })
}
