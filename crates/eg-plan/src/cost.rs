//! The cross-modal cost model (CONCEPT:EG-KG.query.concept-14): given a FILTER and a RANK over the
//! same seed set, which goes first?
//!
//! This is the kernel of the whole "unified planner" argument. The win of one engine
//! over three siloed round-trips is *cross-modal reordering* — the planner puts the
//! cheapest, most-selective operator first regardless of which modality it belongs
//! to. A caller stitching three surfaces in Python is locked into whatever fixed
//! order it hand-coded; the planner is not. The decision is real, with two
//! contrasting regimes documented in `repo://reports/spike-unified-findings.md`:
//!
//! * **filter-first** when the relational predicate is highly selective — it slashes
//!   the candidate set cheaply, so the expensive brute-force vector scoring runs over
//!   FEW rows;
//! * **vector-first** when the predicate is broad — filtering first barely shrinks
//!   anything, so do the index kNN top-k first and re-filter the small result.
//!
//! Both orders return the SAME set (filter ∧ rank commute as set operations); only
//! the *work* differs — which is the definition of a cost-based plan choice.
//!
//! The asymmetry rests on two facts about the legs:
//! (i) a property-index-backed `Filter` (CONCEPT:EG-KG.query.concept-12) yields survivors WITHOUT a
//!     full scan, but a `Rank` over an arbitrary id-set is **brute-force** — it cannot
//!     use the global kNN index for a caller-supplied subset;
//! (ii) the other order's `Rank` IS an index top-k, but must **over-fetch
//!     ≈ top_k/selectivity** candidates so enough survive the downstream filter.

use crate::algebra::Op;

/// Catalog-ish statistics the cost model reasons over. In a real engine these come
/// from histograms / the property index / embedding-store size; this increment
/// derives them from the snapshot (see [`Stats::estimate`]) and lets a test drive
/// either regime directly.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// Total nodes in the seed (post-Scan) set.
    pub seed_rows: usize,
    /// Estimated selectivity of the FILTER predicate in [0,1] (fraction that pass).
    pub filter_selectivity: f64,
    /// The final top-k the plan requests (a Rank feeding a Limit). This is what makes
    /// the orders asymmetric: a vector RANK is a top-k kNN, so VECTOR-FIRST produces
    /// only ~k rows for the cheap filter to re-check, whereas FILTER-FIRST must pay a
    /// vector distance for EVERY filter survivor.
    pub top_k: usize,
    /// Per-row cost of a relational predicate eval (cheap).
    pub cost_filter_per_row: f64,
    /// Per-row cost of a brute-force vector distance (the cost FILTER-FIRST pays per
    /// survivor — it has an arbitrary id-set, so it can't use the global kNN index).
    pub cost_vector_per_row: f64,
    /// Cost of ONE top-k query against the vector index (≈ log(N)·ef — independent of
    /// the candidate-set size). This is what VECTOR-FIRST pays.
    pub cost_vector_topk: f64,
}

impl Stats {
    /// Default per-op cost weights, calibrated to the crossover the spike measured: a
    /// brute-force vector distance is ~20× a relational predicate eval, and one index
    /// top-k costs ≈ log(N)·ef. These are the relative magnitudes that put the
    /// crossover at a realistic selectivity; the absolute units cancel in the
    /// comparison.
    const COST_FILTER_PER_ROW: f64 = 1.0;
    const COST_VECTOR_PER_ROW: f64 = 20.0;

    /// Build a `Stats` from the cheap quantities a planner actually has at hand: the
    /// seed-set size, an estimated predicate selectivity, the requested top-k, and the
    /// embedding-store size (which sets the index top-k cost ≈ log2(N)·ef). Keeps the
    /// cost currency in ONE place so a real catalog can later feed better estimates
    /// without touching the decision logic.
    pub fn estimate(
        seed_rows: usize,
        filter_selectivity: f64,
        top_k: usize,
        embedding_count: usize,
    ) -> Self {
        // One HNSW top-k ≈ log2(N)·ef heap pushes; ef defaults to a small constant.
        const EF: f64 = 64.0;
        let n = (embedding_count.max(2) as f64).log2();
        Self {
            seed_rows,
            filter_selectivity: filter_selectivity.clamp(0.0, 1.0),
            top_k,
            cost_filter_per_row: Self::COST_FILTER_PER_ROW,
            cost_vector_per_row: Self::COST_VECTOR_PER_ROW,
            cost_vector_topk: n * EF,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    FilterFirst,
    VectorFirst,
}

pub struct CostModel;

impl CostModel {
    /// Cost of FILTER then RANK. The filter is index-backed (a property-index lookup
    /// yields the survivors without a full scan — CONCEPT:EG-KG.query.concept-12), then RANK
    /// brute-force scores EACH survivor (it has an arbitrary id-set, not the global
    /// kNN index, so it can't use HNSW). Cost ∝ the survivor count → a SELECTIVE
    /// filter is cheap here.
    pub fn filter_first_cost(s: &Stats) -> f64 {
        let survivors = (s.seed_rows as f64) * s.filter_selectivity;
        survivors * (s.cost_filter_per_row + s.cost_vector_per_row)
    }

    /// Cost of RANK then FILTER. RANK is a top-k kNN against the HNSW index, but must
    /// OVER-FETCH to absorb rows the later filter rejects: to land `top_k` survivors
    /// at selectivity `sel`, fetch ≈ `top_k/sel` candidates — each a VECTOR comparison
    /// in the kNN heap, not a cheap filter eval. The cheap filter then re-checks them.
    /// A SELECTIVE filter forces a huge over-fetch → vector-first loses; a BROAD
    /// filter barely over-fetches → vector-first wins.
    pub fn vector_first_cost(s: &Stats) -> f64 {
        let fetch = Self::overfetch_pool_size(s.top_k, s.filter_selectivity) as f64;
        s.cost_vector_topk + fetch * (s.cost_vector_per_row + s.cost_filter_per_row)
    }

    /// How many candidates a `top_k`-target index probe must OVER-FETCH to survive a
    /// downstream narrower of `selectivity` (CONCEPT:EG-KG.query.index-method-seam) — the
    /// `top_k/selectivity` formula [`Self::vector_first_cost`] used inline, extracted so a
    /// PHYSICAL executor (not just this plan-time cost estimate) can size a real
    /// candidate-set-intersection over-fetch pool with the SAME canonical formula instead
    /// of a hand-rolled duplicate (the seam [`IndexMethod`] and its registered methods
    /// share). Used by ANY index-backed rerank that must coexist with an already-applied
    /// filter — e.g. the BM25 `RankText` leg (`crate::exec::rank_text`) sizing its
    /// over-fetch against the current candidate set's selectivity, mirroring the vector
    /// `Rank` leg's (now allowlist-during-probe, no over-fetch needed) prior approach.
    pub fn overfetch_pool_size(top_k: usize, selectivity: f64) -> usize {
        ((top_k as f64) / selectivity.max(1e-9)).ceil() as usize
    }

    /// Choose the cheaper order for a (FILTER, RANK) pair over the same set.
    pub fn order(s: &Stats) -> Order {
        if Self::filter_first_cost(s) <= Self::vector_first_cost(s) {
            Order::FilterFirst
        } else {
            Order::VectorFirst
        }
    }

    /// The adjacent-pair swap primitive used by the cross-modal optimizer's reorder
    /// rules (CONCEPT:EG-KG.query.filter-pushdown-rule). Given the
    /// index of the id-set NARROWER (`Filter`/`AsOf`/`Reason`) and the index of the `Rank`
    /// it is adjacent to, place the narrower first iff `narrower_first`. Pure list surgery —
    /// no cost logic — so both callers agree byte-for-byte on the mechanical rewrite while
    /// each supplies its own cost-derived decision.
    pub(crate) fn place_narrower(
        mut plan: Vec<Op>,
        narrower_idx: usize,
        rank_idx: usize,
        narrower_first: bool,
    ) -> Vec<Op> {
        let (lo, hi) = (narrower_idx.min(rank_idx), narrower_idx.max(rank_idx));
        let narrower_is_lo = narrower_idx == lo;
        if narrower_is_lo != narrower_first {
            plan.swap(lo, hi);
        }
        plan
    }
}

// ── Pluggable index-method cost seam: ANN + BM25 unified (CONCEPT:EG-KG.query.index-method-seam) ──
//
// eg's ANN (`Rank`/`RankEmbed`) and BM25 (`RankText`) rerankers were each hand-costed as
// their own inline match arm below — ANN got a real brute-force-vs-index-topk cost model,
// but BM25 fell through the wildcard pass-through (`_ => in_card`, ZERO cost reasoning),
// so the reorder rules (`crate::optimizer::is_rank`) never even considered reordering a
// narrower against a lexical rerank the way they already did for a vector one — one
// unified planner argument (cross-modal reordering), enforced for only ONE of its two
// index-backed rerankers. This trait — mirroring turso's `IndexMethod`/
// `IndexMethodCostEstimate` seam that unifies its IVF-vector and FTS pushdowns — closes
// that: both modalities' "index vs brute-force" cost now flows through ONE registered
// seam instead of two independently (and unevenly) hand-costed special cases.
#[cfg(feature = "query")]
mod index_method {
    use super::{ModalityCardinality, DEFAULT_TOP_K};
    use crate::algebra::Op;

    /// The seam's shared COST OUTPUT — mirrors turso's
    /// `IndexMethodCostEstimate{estimated_cost, estimated_rows}`. `estimated_cost` is the
    /// SAME abstract-unit currency [`super::CostEstimate::weight`] compares;
    /// `estimated_rows` is what [`super::Cardinality::rows_out`] returns for this op.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct IndexMethodCostEstimate {
        pub estimated_cost: f64,
        pub estimated_rows: f64,
    }

    /// One pluggable index-backed reranker — the trait ANN and BM25 both implement so
    /// the optimizer costs them through ONE seam instead of two independently hand-costed
    /// special cases. A future geo nearest-neighbor pushdown (the finding this closes
    /// names it explicitly as the natural third) registers the SAME way.
    pub trait IndexMethod: Send + Sync {
        /// A stable name (EXPLAIN / test introspection).
        fn name(&self) -> &'static str;
        /// Does this method drive `op`?
        fn matches(&self, op: &Op) -> bool;
        /// Cost + output cardinality of running `op` over `in_card` candidate rows.
        /// `in_card == 0` ⇒ this op is the plan's SOURCE (a pure index top-k); `in_card >
        /// 0` ⇒ it reranks/narrows an existing candidate set — brute-force per survivor,
        /// since neither index natively accepts an arbitrary caller-supplied id-set as
        /// its scan domain. Mirrors the SAME regime split
        /// [`ModalityCardinality::cost_of`]'s `Rank` arm already drew for ANN alone.
        fn estimate(&self, in_card: f64, card: &ModalityCardinality) -> IndexMethodCostEstimate;
    }

    /// ANN vector rerank (`Rank`/`RankEmbed`) as an [`IndexMethod`] — the SAME formulas
    /// [`ModalityCardinality`]'s per-row/top-k constants already used inline. Registered
    /// so [`is_index_method_op`] recognizes it uniformly alongside BM25 (the seam
    /// [`crate::optimizer::is_rank`] now dispatches through); `ModalityCardinality::
    /// cost_of`/`rows_out`'s pre-existing `Rank`/`RankEmbed` arms are deliberately left
    /// calling their own inline formula rather than this method (zero regression risk
    /// to the already-proven, extensively-tested vector leg) — this impl exists so the
    /// REGISTRY recognizes ANN uniformly with BM25 today, and is the drop-in body a
    /// follow-up can point `cost_of`/`rows_out` at once the vector arm is ready to be
    /// re-verified against it.
    pub(super) struct AnnIndexMethod;

    impl IndexMethod for AnnIndexMethod {
        fn name(&self) -> &'static str {
            "ann-vector"
        }
        fn matches(&self, op: &Op) -> bool {
            matches!(op, Op::Rank { .. } | Op::RankEmbed { .. })
        }
        fn estimate(&self, in_card: f64, card: &ModalityCardinality) -> IndexMethodCostEstimate {
            if in_card > 0.0 {
                IndexMethodCostEstimate {
                    estimated_cost: in_card * ModalityCardinality::COST_VECTOR_PER_ROW,
                    estimated_rows: in_card * card.embed_coverage(),
                }
            } else {
                IndexMethodCostEstimate {
                    estimated_cost: card.ann_topk_cost(),
                    estimated_rows: (card.stats.embedding_count as f64).min(DEFAULT_TOP_K as f64),
                }
            }
        }
    }

    /// BM25 lexical rerank (`RankText`) as an [`IndexMethod`] — the counterpart ANN
    /// already had and BM25 never did: a real cost for BOTH regimes (candidate-restricted
    /// brute force vs. a SOURCE top-k postings probe), instead of the wildcard
    /// pass-through every other unrecognized op falls to.
    #[cfg(feature = "text")]
    pub(super) struct Bm25IndexMethod;

    #[cfg(feature = "text")]
    impl IndexMethod for Bm25IndexMethod {
        fn name(&self) -> &'static str {
            "bm25-text"
        }
        fn matches(&self, op: &Op) -> bool {
            matches!(op, Op::RankText { .. })
        }
        fn estimate(&self, in_card: f64, card: &ModalityCardinality) -> IndexMethodCostEstimate {
            // A postings-list hit check is cheaper than a full vector distance (no
            // dot-product over an embedding dimension — a term-frequency lookup) but
            // pricier than a bare relational predicate eval; calibrated conservatively
            // between the two rather than duplicating either exactly.
            const COST_TEXT_PER_ROW: f64 = 5.0;
            const EF: f64 = 64.0;
            if in_card > 0.0 {
                IndexMethodCostEstimate {
                    estimated_cost: in_card * COST_TEXT_PER_ROW,
                    // No per-doc "missing embedding" analogue is modeled for text (every
                    // resident node is either indexed or not, uniformly) — unlike ANN's
                    // `embed_coverage`, a BM25 rerank is assumed to cover the full
                    // candidate set.
                    estimated_rows: in_card,
                }
            } else {
                let n = (card.stats.node_count.max(2) as f64).log2();
                IndexMethodCostEstimate {
                    estimated_cost: n * EF,
                    estimated_rows: (card.stats.node_count as f64).min(DEFAULT_TOP_K as f64),
                }
            }
        }
    }

    /// Every registered [`IndexMethod`] — ANN always, BM25 only under `text` (its wire
    /// `Op::RankText` variant does not exist otherwise).
    fn methods() -> Vec<Box<dyn IndexMethod>> {
        let mut m: Vec<Box<dyn IndexMethod>> = vec![Box::new(AnnIndexMethod)];
        #[cfg(feature = "text")]
        m.push(Box::new(Bm25IndexMethod));
        m
    }

    /// Is `op` driven by a registered [`IndexMethod`] — the generalized
    /// [`crate::optimizer::is_rank`] check spanning EVERY registered index-backed
    /// reranker, not just the vector one.
    pub fn is_index_method_op(op: &Op) -> bool {
        methods().iter().any(|m| m.matches(op))
    }

    /// The registered [`IndexMethod`]'s cost estimate for `op`, or `None` if no method
    /// claims it (a caller falls back to its own default costing).
    pub fn estimate(
        op: &Op,
        in_card: f64,
        card: &ModalityCardinality,
    ) -> Option<IndexMethodCostEstimate> {
        methods()
            .into_iter()
            .find(|m| m.matches(op))
            .map(|m| m.estimate(in_card, card))
    }
}

#[cfg(feature = "query")]
pub(crate) use index_method::{estimate as index_method_estimate, is_index_method_op};
#[cfg(feature = "query")]
pub use index_method::{IndexMethod, IndexMethodCostEstimate};

// ── A tiny Bloom filter for cross-modal candidate-set join probes ────────────────
// (CONCEPT:EG-KG.query.bloom-gate) — pairs with the index-method seam above: when an
// over-fetched index candidate pool (ANN or BM25) must be intersected against an
// already-narrowed candidate set, a compact bit-array membership PRE-check rejects the
// (usually large majority of) non-candidate hits before paying for the exact `HashSet`
// lookup — the SAME heuristic turso's `join.rs` `use_bloom_filter` applies to its build
// side of a join. Dep-free (two salted `DefaultHasher` passes, Kirsch-Mitzenmacher double
// hashing — no external crate), so it ships in every tier that already links `std`.
#[cfg(feature = "text")]
pub(crate) struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

#[cfg(feature = "text")]
impl BloomFilter {
    /// Size a filter for `expected_items` at a target false-positive rate `fp_rate`
    /// (standard formulas: `m = -n·ln(p)/ln(2)²` bits, `k = (m/n)·ln(2)` hash rounds).
    /// `expected_items == 0` still yields a small, usable (if maximally conservative)
    /// filter rather than a degenerate zero-bit one.
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let n = (expected_items.max(1)) as f64;
        let p = fp_rate.clamp(1e-6, 0.5);
        let m = ((-(n * p.ln())) / std::f64::consts::LN_2.powi(2))
            .ceil()
            .max(64.0);
        let k = ((m / n) * std::f64::consts::LN_2).round().clamp(1.0, 16.0);
        let num_bits = m as usize;
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits: num_words * 64,
            num_hashes: k as u32,
        }
    }

    /// Build a populated filter directly from an id iterator, sized off `count_hint`
    /// (the caller's already-known candidate-set cardinality — avoids a second pass).
    pub fn from_ids<'a, I: IntoIterator<Item = &'a str>>(ids: I, count_hint: usize) -> Self {
        let mut f = Self::new(count_hint, 0.01);
        for id in ids {
            f.insert(id);
        }
        f
    }

    /// Two independent hashes of `item`, salted so they are NOT the same value —
    /// Kirsch-Mitzenmacher then derives all `k` probe positions from this one pair
    /// without `k` separate hash passes.
    fn hashes(item: &str) -> (u64, u64) {
        use std::hash::{Hash, Hasher};
        let mut h1 = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut h1);
        let a = h1.finish();
        let mut h2 = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut h2);
        0x9E37_79B9_7F4A_7C15u64.hash(&mut h2); // salt so h2 != h1
        let b = h2.finish();
        (a, b)
    }

    fn bit_index(&self, round: u32, h1: u64, h2: u64) -> usize {
        // g_i(x) = h1 + i*h2 (mod m) — the standard double-hashing derivation that
        // simulates `k` independent hash functions from just two (Kirsch & Mitzenmacher).
        (h1.wrapping_add((round as u64).wrapping_mul(h2)) as usize) % self.num_bits
    }

    pub fn insert(&mut self, item: &str) {
        let (h1, h2) = Self::hashes(item);
        for i in 0..self.num_hashes {
            let idx = self.bit_index(i, h1, h2);
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// `false` ⇒ DEFINITELY absent (no false negatives, ever); `true` ⇒ probably
    /// present (bounded false-positive rate — the caller must still confirm with an
    /// exact check, exactly like turso's gate: a bloom filter narrows, never decides).
    pub fn might_contain(&self, item: &str) -> bool {
        let (h1, h2) = Self::hashes(item);
        (0..self.num_hashes).all(|i| {
            let idx = self.bit_index(i, h1, h2);
            self.bits[idx / 64] & (1u64 << (idx % 64)) != 0
        })
    }
}

// ── Cross-modal cost/cardinality catalog + per-modality estimators ───────────────
// (CONCEPT:EG-KG.query.cardinality-estimators) — the plan-time inputs Lane A's optimizer
// reads. Everything here stays in eg-plan (Rule R1): NOTHING is added to the wire `Op`.

/// Snapshot-scoped catalog statistics collected once per `plan_optimize` call
/// (CONCEPT:EG-KG.query.cardinality-estimators). Scalar counts come from O(1) map lengths.
/// Numeric column histograms require one O(N * F) property pass on a cold snapshot, then
/// are memoized on that immutable snapshot; a warm collection clones O(K) column records.
/// `N` is resident nodes, `F` top-level properties examined and `K` numeric columns.
#[cfg(feature = "query")]
#[derive(Clone, Debug, Default)]
pub struct PlanStats {
    /// Total resident nodes (the seed-set upper bound).
    pub node_count: usize,
    /// Distinct edge endpoints (≈ edge count) — feeds the average-degree estimate.
    pub edge_count: usize,
    /// Embeddings in the semantic store — sets the ANN index top-k cost (≈ log2(N)·ef) and
    /// the fraction of a candidate set a vector `Rank` can score.
    pub embedding_count: usize,
    /// Mean out-degree `edge_count / node_count` — the graph `Traverse` fan-out factor.
    pub avg_out_degree: f64,
    /// Per-numeric-column min/max + equi-width histogram (CONCEPT:EG-KG.query.column-range-stats).
    /// The non-O(1) member: derived from a single pass over the resident node
    /// property blobs so a `GtNum`/`LtNum` range predicate's selectivity is estimated from the
    /// REAL data distribution instead of a fixed heuristic. A fixed bucket count and capped
    /// sample bound memory per numeric column.
    pub column_stats: ColumnStats,
    /// Per-column APPROXIMATE DISTINCT-VALUE sketches (CONCEPT:EG-KG.query.approx-distinct-cardinality,
    /// W4.5/N5) — the planner's "sketch for cardinality" input: an `EQUALITY` predicate's
    /// selectivity is estimated from `1/distinct_count` (see [`DistinctStats::frac_eq`]) instead
    /// of the fixed heuristic, the same real-distribution sharpening `column_stats` already gave
    /// range predicates. A SEPARATE O(N) pass from `column_stats` (which visits numeric values
    /// only) — this one visits every top-level SCALAR value, numeric or not.
    pub distinct_stats: DistinctStats,
}

#[cfg(feature = "query")]
impl PlanStats {
    /// Collect the catalog from a [`crate::exec::PlanCtx`] snapshot. The scalar counts are
    /// O(1) (`.len()` on the snapshot maps); [`ColumnStats::collect`] and [`DistinctStats::collect`]
    /// each perform an O(N * F) property pass once on a cold snapshot and an O(K) clone from the
    /// snapshot memo when warm. The range/equality-selectivity estimates themselves do not
    /// rescan nodes per predicate.
    pub fn collect(ctx: &crate::exec::PlanCtx) -> Self {
        let node_count = ctx.view.node_properties.len();
        let edge_count = ctx.view.edge_properties.len();
        let embedding_count = ctx.semantic.len();
        let avg_out_degree = if node_count > 0 {
            edge_count as f64 / node_count as f64
        } else {
            0.0
        };
        Self {
            node_count,
            edge_count,
            embedding_count,
            avg_out_degree,
            column_stats: ColumnStats::collect(ctx.view),
            distinct_stats: DistinctStats::collect(ctx.view),
        }
    }
}

/// Number of equi-width buckets per numeric column histogram (CONCEPT:EG-KG.query.column-range-stats)
/// — a small fixed constant: enough resolution to distinguish a selective tail from a broad
/// range, cheap to build and store.
#[cfg(feature = "query")]
const HIST_BUCKETS: usize = 16;

/// Cap on the per-column value sample the histogram is built from
/// (CONCEPT:EG-KG.query.column-range-stats). The running min/max always see EVERY value (so the
/// range bounds are exact); only the bucket SHAPE is sampled, which bounds collection memory
/// to `O(HIST_BUCKETS + SAMPLE_CAP)` per column regardless of graph size.
#[cfg(feature = "query")]
const SAMPLE_CAP: usize = 8_192;

/// Per-numeric-column distribution statistics (CONCEPT:EG-KG.query.column-range-stats) the optimizer
/// consults to estimate a NUMERIC-RANGE predicate's true selectivity, instead of the fixed
/// per-predicate heuristic (the T-E1 ablation gap: a `GtNum`/`LtNum` range was NEVER pushed
/// filter-first regardless of how selective it actually was). Derived from ONE cheap pass
/// over the resident node property blobs during [`PlanStats::collect`] — the same snapshot the
/// plan runs over, so no extra I/O and no config flag.
///
/// Representation: per column an EXACT min/max plus a fixed-width equi-width histogram over
/// `[min, max]`. A histogram beats bare min/max on skewed data — the fraction above a
/// threshold reads off the bucket counts (with linear interpolation inside the boundary
/// bucket) rather than assuming a uniform spread; min/max alone is the histogram's degenerate
/// 1-bucket case. Bounded ([`HIST_BUCKETS`] buckets + a [`SAMPLE_CAP`]-capped sample), so the
/// stats stay O(1) in memory while collection is a single O(N) blob pass.
#[cfg(feature = "query")]
#[derive(Clone, Debug, Default)]
pub struct ColumnStats {
    cols: std::collections::HashMap<String, NumericColumn>,
}

/// One numeric column's exact `[min, max]` range and an equi-width histogram over it
/// (CONCEPT:EG-KG.query.column-range-stats). `buckets[i]` counts sampled values in
/// `[min + i·w, min + (i+1)·w)` with `w = (max-min)/HIST_BUCKETS`; `total` is the sampled
/// count the fractions are taken over.
#[cfg(feature = "query")]
#[derive(Clone, Debug)]
struct NumericColumn {
    min: f64,
    max: f64,
    buckets: Vec<u32>,
    total: u64,
}

#[cfg(feature = "query")]
impl ColumnStats {
    /// Per-column stats for `view`, MEMOIZED on the snapshot so repeated `optimize()` calls
    /// over the SAME snapshot pay the O(N) blob-decode pass ONCE (CONCEPT:EG-KG.query.column-range-stats).
    ///
    /// Correctness invariant (never serves stale stats): a [`eg_core::graph::GraphView`] is an
    /// IMMUTABLE point-in-time snapshot produced by `GraphCore::*_snapshot` at one OCC
    /// `version()`; its `node_properties` never change after construction. The memo is stored
    /// INSIDE that view (`GraphView::plan_stats_memo`, an interior-mutable [`std::sync::OnceLock`]),
    /// so it is physically bound to exactly the data it summarizes. A committed write produces a
    /// brand-NEW snapshot — a fresh `GraphView` whose memo starts empty — so the next `collect`
    /// recomputes from the new data. There is no version/identity key to get wrong and no ABA
    /// window: a stale hit is structurally impossible because the cache and the data it describes
    /// share one lifetime. The stored value is [`ColumnStats`] itself (type-erased through `dyn
    /// Any` so `eg-core` need not depend on `eg-plan`); we downcast and clone it (an O(#columns)
    /// copy — never the O(N) blob scan) to keep the returned `ColumnStats` byte-identical to the
    /// recompute path.
    pub fn collect(view: &eg_core::graph::GraphView) -> Self {
        let memo = view
            .plan_stats_memo
            .get_or_init(|| std::sync::Arc::new(Self::compute(view)));
        memo.downcast_ref::<Self>()
            .expect("plan_stats_memo holds ColumnStats")
            .clone()
    }

    /// Build the per-column stats in ONE pass over the resident node property blobs — the
    /// uncached computation [`Self::collect`] memoizes. Each blob is decoded to JSON; every
    /// TOP-LEVEL numeric property accumulates into its column's running min/max/count plus a
    /// capped value sample. After the pass each column's sample is bucketed into a
    /// [`HIST_BUCKETS`]-wide equi-width histogram over the exact `[min,max]`.
    fn compute(view: &eg_core::graph::GraphView) -> Self {
        use std::collections::HashMap;
        struct Acc {
            min: f64,
            max: f64,
            count: u64,
            sample: Vec<f64>,
        }
        let mut acc: HashMap<String, Acc> = HashMap::new();
        for blob in view.node_properties.values() {
            let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(obj) = v.as_object() else {
                continue;
            };
            for (k, val) in obj {
                let Some(x) = val.as_f64() else {
                    continue; // non-numeric (string `type`, arrays, …) — skip.
                };
                if !x.is_finite() {
                    continue;
                }
                let e = acc.entry(k.clone()).or_insert(Acc {
                    min: x,
                    max: x,
                    count: 0,
                    sample: Vec::new(),
                });
                e.min = e.min.min(x);
                e.max = e.max.max(x);
                e.count += 1;
                if e.sample.len() < SAMPLE_CAP {
                    e.sample.push(x);
                }
            }
        }
        let cols = acc
            .into_iter()
            .filter_map(|(k, a)| {
                if a.count == 0 || !a.min.is_finite() || !a.max.is_finite() {
                    return None;
                }
                let mut buckets = vec![0u32; HIST_BUCKETS];
                let span = a.max - a.min;
                if span <= 0.0 {
                    // A single-value column: all mass in the first bucket.
                    buckets[0] = a.sample.len() as u32;
                } else {
                    for &x in &a.sample {
                        let mut b = (((x - a.min) / span) * HIST_BUCKETS as f64) as usize;
                        if b >= HIST_BUCKETS {
                            b = HIST_BUCKETS - 1;
                        }
                        buckets[b] += 1;
                    }
                }
                Some((
                    k,
                    NumericColumn {
                        min: a.min,
                        max: a.max,
                        buckets,
                        total: a.sample.len() as u64,
                    },
                ))
            })
            .collect();
        Self { cols }
    }

    /// Estimated fraction of `column`'s values strictly GREATER than `n` — a `GtNum{prop:column,
    /// n}` predicate's selectivity. `None` when the column has no numeric stats (unknown /
    /// non-numeric key), so the caller falls back to the fixed heuristic.
    pub fn frac_gt(&self, column: &str, n: f64) -> Option<f64> {
        self.cols.get(column).map(|c| c.frac_gt(n))
    }

    /// Estimated fraction of `column`'s values LESS than `n` — a `LtNum{prop:column, n}`
    /// predicate's selectivity. `None` when the column has no numeric stats.
    pub fn frac_lt(&self, column: &str, n: f64) -> Option<f64> {
        self.cols.get(column).map(|c| c.frac_lt(n))
    }

    /// Number of numeric columns with collected stats (introspection / tests).
    pub fn len(&self) -> usize {
        self.cols.len()
    }

    /// Whether any numeric column stats were collected.
    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }
}

// ── Approximate DISTINCT-VALUE cardinality via HyperLogLog ───────────────────────
// (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5) — the planner's "sketch for
// cardinality" input: sharpens `Pred::Eq`'s selectivity from the fixed heuristic to
// `1/distinct_count`, exactly the kind of real-distribution improvement `ColumnStats`'s
// histogram already gave `GtNum`/`LtNum` above. A SEPARATE O(N) pass from `ColumnStats::compute`
// (which visits NUMERIC values only): this one hashes every top-level SCALAR value (numeric,
// string, or bool) into its column's `eg_compute::sketch::HyperLogLog`, since equality targets
// string-valued columns (`status`, `type`, …) at least as often as numeric ones.

/// Per-column approximate distinct-value sketches (CONCEPT:EG-KG.query.approx-distinct-cardinality,
/// W4.5/N5). See the module-level note above for why this is a sibling of, not a merge into,
/// [`ColumnStats`].
#[cfg(feature = "query")]
#[derive(Clone, Debug, Default)]
pub struct DistinctStats {
    sketches: std::collections::HashMap<String, eg_compute::sketch::HyperLogLog>,
}

#[cfg(feature = "query")]
impl DistinctStats {
    /// Per-column sketches for `view`, MEMOIZED on the snapshot (`GraphView::distinct_stats_memo`)
    /// exactly like [`ColumnStats::collect`] — see that method's doc for the full
    /// staleness-impossible argument; it applies identically here (a `GraphView` never changes
    /// after construction, so the memo can never go stale).
    pub fn collect(view: &eg_core::graph::GraphView) -> Self {
        let memo = view
            .distinct_stats_memo
            .get_or_init(|| std::sync::Arc::new(Self::compute(view)));
        memo.downcast_ref::<Self>()
            .expect("distinct_stats_memo holds DistinctStats")
            .clone()
    }

    /// One O(N) pass over the resident node property blobs: every top-level SCALAR value is
    /// hashed into its column's sketch. Arrays/objects/null are skipped (never an equality
    /// target in practice, and hashing a whole nested structure would defeat the sketch's O(1)
    /// memory point for no query-planning benefit).
    fn compute(view: &eg_core::graph::GraphView) -> Self {
        use std::collections::HashMap;
        let mut sketches: HashMap<String, eg_compute::sketch::HyperLogLog> = HashMap::new();
        for blob in view.node_properties.values() {
            let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(obj) = v.as_object() else {
                continue;
            };
            for (k, val) in obj {
                if matches!(
                    val,
                    serde_json::Value::Array(_)
                        | serde_json::Value::Object(_)
                        | serde_json::Value::Null
                ) {
                    continue;
                }
                // Canonical string form: two equal JSON scalars hash identically regardless of
                // where they came from, while distinct scalar KINDS with a similar textual form
                // (the number `1` vs the string `"1"`) still hash distinctly (`to_string` on a
                // `Value` includes the JSON quoting), matching how Cypher/SQL equality treats
                // typed values as distinct from their string rendering.
                let canon = val.to_string();
                sketches.entry(k.clone()).or_default().insert(&canon);
            }
        }
        Self { sketches }
    }

    /// Approximate distinct-value count for `column`, or `None` if never observed.
    pub fn approx_distinct(&self, column: &str) -> Option<f64> {
        self.sketches.get(column).map(|h| h.estimate())
    }

    /// Estimated selectivity of an EQUALITY predicate on `column`: `1/distinct_count` (the
    /// standard equality-selectivity heuristic assuming a roughly uniform value distribution —
    /// the SAME assumption the fixed `Pred::Eq` constant already made, now grounded in the REAL
    /// observed cardinality instead of a blind guess). `None` when the column was never
    /// observed, so the caller falls back to its fixed heuristic.
    pub fn frac_eq(&self, column: &str) -> Option<f64> {
        self.approx_distinct(column)
            .map(|d| (1.0 / d.max(1.0)).clamp(0.0, 1.0))
    }

    /// Number of columns with collected sketches (introspection / tests).
    pub fn len(&self) -> usize {
        self.sketches.len()
    }

    /// Whether any column sketches were collected.
    pub fn is_empty(&self) -> bool {
        self.sketches.is_empty()
    }
}

#[cfg(feature = "query")]
impl NumericColumn {
    /// Fraction of the sampled values `> n`, read off the equi-width histogram with linear
    /// interpolation inside the boundary bucket. Clamped to a threshold at/below `min`
    /// (⇒ 1.0 — everything passes) or at/above `max` (⇒ 0.0 — nothing passes).
    fn frac_gt(&self, n: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        if n <= self.min {
            return 1.0;
        }
        if n >= self.max {
            return 0.0;
        }
        let span = self.max - self.min;
        if span <= 0.0 {
            return 0.0; // degenerate single value, and n is inside (min,max) is impossible
        }
        let w = span / self.buckets.len() as f64;
        let bidx = (((n - self.min) / w).floor() as usize).min(self.buckets.len() - 1);
        // Linear share of the boundary bucket lying above n, plus every fuller bucket above.
        let bucket_hi = self.min + (bidx as f64 + 1.0) * w;
        let frac_above_in_bucket = ((bucket_hi - n) / w).clamp(0.0, 1.0);
        let mut above = frac_above_in_bucket * self.buckets[bidx] as f64;
        for b in (bidx + 1)..self.buckets.len() {
            above += self.buckets[b] as f64;
        }
        (above / self.total as f64).clamp(0.0, 1.0)
    }

    /// Fraction of the sampled values `< n` — the complement of `> n` (the interpolated point
    /// mass exactly at `n` is negligible for a plan-time estimate).
    fn frac_lt(&self, n: f64) -> f64 {
        (1.0 - self.frac_gt(n)).clamp(0.0, 1.0)
    }
}

/// The default top-k a `Rank` targets when no trailing `Limit` pins one — the over-fetch
/// denominator in the ANN cost.
#[cfg(feature = "query")]
pub const DEFAULT_TOP_K: usize = 10;

/// Per-modality cardinality + cost estimator (CONCEPT:EG-KG.query.cardinality-estimators) — the
/// concrete [`Cardinality`] Lane A's optimizer drives, one estimator per modality:
///  * graph `Traverse` — degree histogram (`avg_out_degree`) × path length (`min..=max`);
///  * ANN `Rank` — recall@k / over-fetch against the embedding-store size;
///  * OWL `Reason` — inferred-closure membership × confidence retention;
///  * bi-temporal `AsOf` — temporal range selectivity;
///  * relational `Filter` — per-predicate selectivity product.
///
/// Holds a snapshot-memoized [`PlanStats`] catalog; `rows_out` sizes an op's output and `cost_of`
/// budgets its work as a [`CostEstimate`]. All numbers are estimates in abstract units —
/// only their RELATIVE magnitudes drive a plan choice, exactly as with [`Stats`].
#[cfg(feature = "query")]
#[derive(Clone, Debug)]
pub struct ModalityCardinality {
    stats: PlanStats,
}

#[cfg(feature = "query")]
impl ModalityCardinality {
    /// Bind the estimator to a collected catalog.
    pub fn new(stats: PlanStats) -> Self {
        Self { stats }
    }

    /// The catalog these estimates read.
    pub fn stats(&self) -> &PlanStats {
        &self.stats
    }

    // Per-op default selectivities / cost weights (relative magnitudes calibrated to the
    // same crossover `Stats` uses: a brute-force vector distance ≈ 20× a predicate eval).
    const LABEL_SEL: f64 = 0.5; // a `Scan` label selects ≈ half the graph (no per-label catalog).
    const REL_SEL: f64 = 0.5; // fraction of edges whose relationship matches a `Traverse`.
    const DEDUP_DAMP: f64 = 0.7; // path expansion re-visits nodes → damp the raw fan-out.
    const TEMPORAL_SEL: f64 = 0.8; // most facts are live at a queried instant (`AsOf`).
                                   // Only read from the `Op::Reason` arms below, which are themselves `owl`-gated — a
                                   // build with `query` but without `owl` (e.g. eg-graphql's default feature set) never
                                   // reads them, so they must be gated too or clippy's dead-code lint fires.
    #[cfg(feature = "owl")]
    const REASON_MEMBERSHIP_SEL: f64 = 0.5; // fraction of a candidate set inferred into the class.
    #[cfg(feature = "owl")]
    const REASON_CONF_RETENTION: f64 = 0.9; // confidence-decay attrition of inferred members.
    const COST_FILTER_PER_ROW: f64 = 1.0;
    const COST_VECTOR_PER_ROW: f64 = 20.0;
    const COST_TRAVERSE_PER_EDGE: f64 = 2.0;
    const COST_ASOF_PER_ROW: f64 = 2.0;
    #[cfg(feature = "owl")]
    const REASON_FIXED_COST: f64 = 500.0; // EL⁺ classification is a fixed up-front closure cost.

    /// One ANN index top-k ≈ log2(N)·ef heap pushes (independent of the candidate count).
    fn ann_topk_cost(&self) -> f64 {
        const EF: f64 = 64.0;
        (self.stats.embedding_count.max(2) as f64).log2() * EF
    }

    /// The fraction of an arbitrary candidate set a vector `Rank` can actually score — it
    /// drops rows with no embedding, so coverage = embeddings / nodes, clamped to `[0,1]`.
    fn embed_coverage(&self) -> f64 {
        if self.stats.node_count == 0 {
            return 1.0;
        }
        (self.stats.embedding_count as f64 / self.stats.node_count as f64).clamp(0.0, 1.0)
    }

    /// Estimated output selectivity of an op given `in_card` rows in — `rows_out / in_card`,
    /// clamped to `[0,1]`. The single number the reorder rules compare: how much this op
    /// SHRINKS the candidate set (a `Rank`/`Reason`/`Filter`/`AsOf` narrower). `0` in ⇒ `1.0`.
    pub fn selectivity(&self, op: &Op, in_card: f64, ctx: &crate::exec::PlanCtx) -> f64 {
        if in_card <= 0.0 {
            return 1.0;
        }
        (self.rows_out(op, in_card, ctx) / in_card).clamp(0.0, 1.0)
    }

    /// Combined per-predicate selectivity of a relational `Filter` (independent-predicate
    /// product): JSONPath / spatial broad. EQUALITY is estimated from the REAL observed
    /// distinct-value count via the collected [`DistinctStats`] `HyperLogLog` sketch
    /// (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5) — `1/distinct_count`, so a
    /// highly-selective equality (a near-unique id column) reads as selective and reorders
    /// filter-first, while a broad one (a boolean-like column with 2 distinct values) stays;
    /// only when the column has NO sketch does it fall back to the fixed `EQ_SEL_FALLBACK`
    /// heuristic (the historical constant). A numeric RANGE (`GtNum`/`LtNum`) is estimated from
    /// the REAL column distribution via the collected [`ColumnStats`] histogram
    /// (CONCEPT:EG-KG.query.column-range-stats) — so a genuinely selective range (`year > 2028`
    /// in a 2000..2029 column) reads as selective and reorders filter-first, while a broad one
    /// (`year > 2001`) stays; only when the column has NO numeric stats does it fall back to the
    /// fixed `RANGE_SEL_FALLBACK` heuristic. JSONPath / spatial estimates are unchanged.
    fn filter_selectivity(&self, preds: &[crate::algebra::Pred]) -> f64 {
        use crate::algebra::Pred;
        /// Conservative range estimate used only when the column has no statistics.
        const RANGE_SEL_FALLBACK: f64 = 0.33;
        /// Conservative equality estimate used only when the column has no distinct-value
        /// sketch — the historical fixed constant this replaces as the estimator of record.
        const EQ_SEL_FALLBACK: f64 = 0.1;
        let mut sel = 1.0f64;
        for p in preds {
            // The wildcard catches the `geo` spatial `Pred` variants (and any future kind);
            // in a non-`geo` build the relational/JSONPath arms are already exhaustive, so the
            // wildcard is unreachable there — allowed so the SAME match compiles under every
            // feature subset (the `where_clause`/exec split-out precedent).
            #[allow(unreachable_patterns)]
            let s = match p {
                // Equality: consult the HyperLogLog distinct-count sketch, else the fixed fallback.
                Pred::Eq { prop, .. } => self
                    .stats
                    .distinct_stats
                    .frac_eq(prop)
                    .unwrap_or(EQ_SEL_FALLBACK),
                // Numeric range: consult the column histogram, else the fixed fallback.
                Pred::GtNum { prop, n } => self
                    .stats
                    .column_stats
                    .frac_gt(prop, *n)
                    .unwrap_or(RANGE_SEL_FALLBACK),
                Pred::LtNum { prop, n } => self
                    .stats
                    .column_stats
                    .frac_lt(prop, *n)
                    .unwrap_or(RANGE_SEL_FALLBACK),
                Pred::JsonPath { .. } => 0.4,
                // Spatial preds (geo) and any future kind: a broad default.
                _ => 0.25,
            };
            sel *= s;
        }
        sel.clamp(1e-6, 1.0)
    }

    /// The plan-time COST of an op given `in_card` rows in (CONCEPT:EG-KG.query.cardinality-estimators)
    /// — the [`CostEstimate`] triple Lane A compares to order a pair and to rank `FuseRrf`
    /// branches. `cpu` is the dominant term (per-row modality work); `io` counts index
    /// probes / blob reads. Only relative magnitudes matter.
    pub fn cost_of(&self, op: &Op, in_card: f64, ctx: &crate::exec::PlanCtx) -> CostEstimate {
        let rows = self.rows_out(op, in_card, ctx);
        let (cpu, io) = match op {
            // Relational predicate eval per input row (cheap).
            Op::Filter { .. } => (in_card * Self::COST_FILTER_PER_ROW, in_card * 0.1),
            // A candidate-restricted vector `Rank` is BRUTE-FORCE per survivor; a SOURCE
            // `Rank` (empty input) is one index top-k. This asymmetry is the whole reorder.
            Op::Rank { .. } | Op::RankEmbed { .. } => {
                if in_card > 0.0 {
                    (in_card * Self::COST_VECTOR_PER_ROW, in_card)
                } else {
                    (
                        self.ann_topk_cost(),
                        (self.stats.embedding_count.max(2) as f64).log2(),
                    )
                }
            }
            // Graph BFS: one edge visit per fan-out edge across the hop range.
            Op::Traverse { min, max, .. } => {
                let d = self.stats.avg_out_degree.max(0.0) * Self::REL_SEL;
                let edges = (*min..=*max).map(|h| d.powi(h as i32)).sum::<f64>();
                (
                    in_card * edges * Self::COST_TRAVERSE_PER_EDGE,
                    in_card * edges,
                )
            }
            // Cheap dep-free blob scan.
            Op::AsOf { .. } => (in_card * Self::COST_ASOF_PER_ROW, in_card * 0.2),
            // OWL EL⁺ classification: a fixed up-front closure cost + a per-candidate check.
            #[cfg(feature = "owl")]
            Op::Reason { .. } => (Self::REASON_FIXED_COST + in_card, in_card * 0.2),
            // Lexical BM25 rerank, through the SAME `IndexMethod` seam ANN uses just
            // above (CONCEPT:EG-KG.query.index-method-seam) — previously fell through the
            // wildcard pass-through below with NO cost reasoning at all.
            #[cfg(feature = "text")]
            Op::RankText { .. } => {
                let est = index_method_estimate(op, in_card, self)
                    .expect("RankText is a registered IndexMethod");
                (est.estimated_cost, est.estimated_rows.max(1.0) * 0.1)
            }
            // Everything else: a linear pass over the input (a rerank / narrow / source).
            _ => (in_card.max(1.0), in_card * 0.1),
        };
        CostEstimate { rows, cpu, io }
    }

    /// The plan-time cost of one candidate PERMUTATION of a reorderable segment — a
    /// contiguous run of `Filter`/`AsOf`/`Reason`/`Rank`(`Embed`) ops all drawing candidates
    /// from the SAME id-set (CONCEPT:EG-KG.query.global-plan-cost). This is the N-ARY
    /// generalization of the two-op [`CostModel::filter_first_cost`]/[`vector_first_cost`]
    /// asymmetry [`crate::optimizer`]'s pairwise rules draw on for a single `(narrower, Rank)`
    /// pair — the win it unlocks: a caller-written chain of 3+ narrowers/`Rank` can be
    /// reordered as ONE decision instead of two independent adjacent swaps, which structurally
    /// cannot see past their own pair (e.g. `[Rank, broad_filter, selective_filter]` — the
    /// pairwise rule commits `(Rank, broad_filter)` first and never revisits `selective_filter`
    /// against either).
    ///
    /// A `Rank`/`RankEmbed` placed FIRST in `perm` (`i == 0`) is costed exactly like the
    /// pairwise "vector-first" leg: one ANN top-k probe ([`Self::ann_topk_cost`]) that must
    /// OVER-FETCH enough candidates to survive EVERY narrower still ahead in this permutation
    /// — their COMBINED selectivity (the product), generalizing the pairwise
    /// [`CostModel::vector_first_cost`]'s single trailing narrower to the whole remaining
    /// chain — via [`CostModel::vector_first_cost`] (the SAME formula, called through
    /// [`Stats::estimate`] so the currency matches the pairwise decision byte-for-byte in the
    /// 2-element case). A `Rank` anywhere else, and every `Filter`/`AsOf`/`Reason`, costs+narrows
    /// via [`Self::cost_of`]/[`Self::rows_out`], folding the running cardinality through the
    /// chain — the SAME per-op interaction model [`crate::optimizer`]'s branch-cost folds use.
    ///
    /// Returns `None` when the permutation is UNSAFE under the EG-405 guard (CONCEPT:EG-405):
    /// some intermediate would drop below 1 row, which could flip a downstream op into SOURCE
    /// mode and change the answer — generalizing the pairwise rules' "both candidate
    /// intermediates stay ≥ 1 row" check to every step of an N-ary chain. The caller must skip
    /// an unsafe permutation, never treat it as free/cheapest.
    pub(crate) fn permutation_cost(
        &self,
        perm: &[&Op],
        seed: f64,
        top_k: usize,
        ctx: &crate::exec::PlanCtx,
    ) -> Option<f64> {
        let mut running = seed;
        let mut total = 0.0;
        for (i, op) in perm.iter().enumerate() {
            if running < 1.0 {
                return None; // EG-405 guard: an emptied intermediate must not be reordered past.
            }
            let is_rank = matches!(op, Op::Rank { .. } | Op::RankEmbed { .. });
            if is_rank && i == 0 {
                // Segment-source Rank: an ANN top-k that must over-fetch to survive every
                // narrower still ahead — their combined (product) selectivity.
                let remaining_sel: f64 = perm[1..]
                    .iter()
                    .map(|o| self.selectivity(o, running, ctx))
                    .product::<f64>()
                    .max(1e-6);
                let stats = Stats::estimate(
                    running.round().max(1.0) as usize,
                    remaining_sel,
                    top_k,
                    self.stats.embedding_count,
                );
                total += CostModel::vector_first_cost(&stats);
                running = ((top_k as f64) / remaining_sel).min(running);
            } else {
                total += self.cost_of(op, running, ctx).weight();
                running = self.rows_out(op, running, ctx);
            }
        }
        if running < 1.0 {
            return None; // the permutation's own final narrowing also mustn't go empty.
        }
        Some(total)
    }
}

/// The scalar comparison weight of a [`CostEstimate`] — `cpu + io` (CONCEPT:EG-KG.query.cardinality-estimators).
/// The single number the reorder rules and `FuseRrf` branch-sort minimize.
#[cfg(feature = "query")]
impl CostEstimate {
    /// Total abstract work — the scalar the optimizer minimizes.
    pub fn weight(&self) -> f64 {
        self.cpu + self.io
    }
}

#[cfg(feature = "query")]
impl ModalityCardinality {
    /// The STATIC, model-only cardinality estimate for `op` — one arm per modality
    /// (CONCEPT:EG-KG.query.cardinality-estimators). Feature-gated ops that are absent in this
    /// build fall to the pass-through wildcard, so the match compiles under ANY feature
    /// subset. This is [`Cardinality::rows_out`]'s ENTIRE body prior to the opt-in
    /// `learned-cost` tier (CONCEPT:EG-KG.query.adaptive-reoptimization, W4.12 phase 2) — split out
    /// under its own name (`pub(crate)`, not part of the public [`Cardinality`] trait) so
    /// [`crate::exec::run_with_adaptive_reopt`] can train [`crate::learned_cost::LearnedCostStore`]
    /// on the RAW estimate even when `rows_out` itself is returning a corrected number —
    /// training on an already-corrected value would have the curve recursively "correct
    /// its own correction" instead of converging on the true static-model bias. Every
    /// existing caller that reads `rows_out` gets EXACTLY this computation, unchanged,
    /// whenever `learned-cost` is not both compiled in and runtime-enabled.
    pub(crate) fn static_rows_out(
        &self,
        op: &Op,
        in_card: f64,
        _ctx: &crate::exec::PlanCtx,
    ) -> f64 {
        let n = self.stats.node_count as f64;
        match op {
            // SOURCE: a label selects a fraction of the graph (no per-label catalog).
            Op::Scan { .. } => (n * Self::LABEL_SEL).max(0.0),
            // FILTER: input × per-predicate selectivity product.
            Op::Filter { preds } => in_card * self.filter_selectivity(preds),
            // TRAVERSE: degree histogram × path length, deduped, capped at the graph size.
            Op::Traverse { min, max, .. } => {
                if in_card <= 0.0 {
                    return 0.0;
                }
                let d = self.stats.avg_out_degree.max(0.0) * Self::REL_SEL;
                let expansion = (*min..=*max).map(|h| d.powi(h as i32)).sum::<f64>();
                (in_card * expansion * Self::DEDUP_DAMP).min(n.max(in_card))
            }
            // RANK: a rerank preserves the candidate set MINUS rows with no embedding
            // (recall coverage); as a SOURCE (empty input) it is a top-k over the index.
            Op::Rank { .. } | Op::RankEmbed { .. } => {
                if in_card > 0.0 {
                    in_card * self.embed_coverage()
                } else {
                    (self.stats.embedding_count as f64).min(DEFAULT_TOP_K as f64)
                }
            }
            // ASOF: bi-temporal range selectivity (most facts live). As a SOURCE, over the
            // whole graph.
            Op::AsOf { .. } => {
                if in_card > 0.0 {
                    in_card * Self::TEMPORAL_SEL
                } else {
                    n * Self::TEMPORAL_SEL
                }
            }
            // LIMIT caps the row count.
            Op::Limit { k } => in_card.min(*k as f64),
            // REASON: OWL-inferred membership × confidence retention (a FILTER mid-pipeline);
            // as a SOURCE (empty input) an inferred-closure fraction of the graph.
            #[cfg(feature = "owl")]
            Op::Reason { .. } => {
                if in_card > 0.0 {
                    in_card * Self::REASON_MEMBERSHIP_SEL * Self::REASON_CONF_RETENTION
                } else {
                    n * Self::REASON_MEMBERSHIP_SEL
                }
            }
            // FUSE (RRF): the union of the branch rankings — bounded by the seed.
            #[cfg(feature = "text")]
            Op::FuseRrf { .. } => in_card.max(1.0),
            // RANK (lexical, BM25), through the SAME `IndexMethod` seam as vector `Rank`
            // above (CONCEPT:EG-KG.query.index-method-seam) — previously the wildcard
            // pass-through below (`in_card` unchanged) gave it NO real narrowing model.
            #[cfg(feature = "text")]
            Op::RankText { .. } => index_method_estimate(op, in_card, self)
                .map(|e| e.estimated_rows)
                .unwrap_or(in_card),
            // Rerankers / context / other sources: preserve the row count (pass-through).
            _ => in_card,
        }
    }
}

#[cfg(feature = "query")]
impl Cardinality for ModalityCardinality {
    /// Estimated rows OUT of `op` given `in_card` rows in (CONCEPT:EG-KG.query.cardinality-estimators)
    /// — [`Self::static_rows_out`]'s model estimate, nudged by the opt-in learned
    /// correction (CONCEPT:EG-KG.query.adaptive-reoptimization, W4.12 phase 2) when
    /// [`crate::learned_cost::enabled`] is true: every caller of `rows_out` (the
    /// plan-time optimizer, `reoptimize_remaining`'s permutation search, the exec loop's
    /// own per-op estimate) picks up a systematic bias learned from PAST executions
    /// automatically, with zero call-site changes. Byte-for-byte [`Self::static_rows_out`]
    /// whenever `learned-cost` is not both compiled in and runtime-enabled — the default.
    fn rows_out(&self, op: &Op, in_card: f64, ctx: &crate::exec::PlanCtx) -> f64 {
        let raw = self.static_rows_out(op, in_card, ctx);
        #[cfg(feature = "learned-cost")]
        {
            if crate::learned_cost::enabled() {
                if let Some(kind) = crate::learned_cost::op_kind(op) {
                    return crate::learned_cost::LearnedCostStore::global().correct(kind, raw);
                }
            }
        }
        raw
    }
}

// ── Lane-0 shared cost traits (A's optimizer + B's runtime both read these) ──────

/// A shared physical COST estimate (CONCEPT:EG-KG.query.cost-cardinality-traits) — the triple
/// Lane A's cross-modal optimizer compares to choose a plan and Lane B's runtime reads to
/// budget an op: estimated output `rows`, `cpu` work, and `io` (bytes / index probes). Kept
/// in eg-plan so cost/cardinality metadata NEVER leaks into the wire [`Op`] DTO (Rule R1).
/// All fields are estimates in abstract units; only their RELATIVE magnitudes matter to a
/// plan choice, exactly as with [`Stats`]. Fields are `pub` so a sibling optimizer/runtime
/// module reads them directly.
///
/// This tier only DEFINES the shape — no estimator is wired yet (identity execution); Lane A
/// populates it via the [`Cardinality`] estimators, so nothing here changes today's behavior.
#[cfg(feature = "query")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostEstimate {
    /// Estimated rows the op emits.
    pub rows: f64,
    /// Estimated CPU work (abstract units).
    pub cpu: f64,
    /// Estimated I/O — bytes read / index probes (abstract units).
    pub io: f64,
}

/// A per-op CARDINALITY estimator (CONCEPT:EG-KG.query.cost-cardinality-traits) — the shared
/// trait Lane A's cost optimizer and Lane B's physical runtime BOTH read to size an op's
/// output. Given an `op`, its input cardinality `in_card`, and the plan [`PlanCtx`], it
/// returns the estimated number of rows the op emits, so per-modality estimators (graph
/// degree/path, ANN recall@k / over-fetch, OWL closure + decay, `AsOf` selectivity, …) plug
/// in behind ONE interface without baking any estimate into the wire [`Op`] (Rule R1). Gated
/// on `query` because it borrows the `query`-tier [`PlanCtx`]; the dep-free
/// [`CostModel`] / [`Stats`] remain available in the Pi tier.
#[cfg(feature = "query")]
pub trait Cardinality {
    /// Estimated rows OUT of `op` given `in_card` rows flowing in, over `ctx`.
    fn rows_out(&self, op: &Op, in_card: f64, ctx: &crate::exec::PlanCtx) -> f64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::Pred;

    /// The cost model picks filter-first vs vector-first by selectivity — the
    /// cross-modal reordering decision a unified planner exists to make.
    #[test]
    fn cost_model_orders_by_selectivity() {
        // Highly selective filter (1% pass): only ~100 survivors to vector-score, far
        // cheaper than the index top-k overhead path → filter FIRST.
        let selective = Stats {
            seed_rows: 10_000,
            filter_selectivity: 0.01,
            top_k: 10,
            cost_filter_per_row: 1.0,
            cost_vector_per_row: 20.0,
            cost_vector_topk: 500.0,
        };
        assert_eq!(CostModel::order(&selective), Order::FilterFirst);

        // Non-selective filter (98% pass): filter-first must vector-score ~9800 rows
        // (9800*20), dwarfing one index top-k (500) + re-filtering 10 → vector FIRST.
        let broad = Stats {
            seed_rows: 10_000,
            filter_selectivity: 0.98,
            top_k: 10,
            cost_filter_per_row: 1.0,
            cost_vector_per_row: 20.0,
            cost_vector_topk: 500.0,
        };
        assert_eq!(CostModel::order(&broad), Order::VectorFirst);

        // The optimizer's one swap primitive places the pair according to the winner.
        let plan = vec![
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: "type".into(),
                    value: "Doc".into(),
                }],
            },
            Op::Rank {
                query: vec![1.0, 0.0],
            },
        ];
        let reordered = CostModel::place_narrower(plan.clone(), 0, 1, false);
        assert!(
            matches!(reordered[0], Op::Rank { .. }),
            "broad filter → vector-first puts Rank at the front"
        );
        let kept = CostModel::place_narrower(plan, 0, 1, true);
        assert!(
            matches!(kept[0], Op::Filter { .. }),
            "selective filter → filter-first keeps Filter at the front"
        );
    }

    /// `Stats::estimate` reproduces the same crossover from the cheap quantities a
    /// planner actually has — so the decision logic is fed by derivable stats, not
    /// hand-tuned magic numbers.
    #[test]
    fn estimate_derives_the_crossover() {
        let selective = Stats::estimate(10_000, 0.01, 10, 10_000);
        let broad = Stats::estimate(10_000, 0.98, 10, 10_000);
        assert_eq!(CostModel::order(&selective), Order::FilterFirst);
        assert_eq!(CostModel::order(&broad), Order::VectorFirst);
    }

    /// Per-regime cardinality proof (CONCEPT:EG-KG.query.cardinality-estimators), mirroring
    /// [`cost_model_orders_by_selectivity`] but driven by the LIVE per-modality estimators:
    /// equality is more selective than a numeric range, and the estimator-derived selectivity
    /// feeds `CostModel::order` to the SAME filter-first / vector-first crossover — the
    /// cross-modal reorder a unified planner exists to make.
    #[cfg(feature = "query")]
    #[test]
    fn cardinality_estimators_drive_the_regime() {
        let fx = crate::fixture::build();
        let ctx = crate::exec::PlanCtx::new(&fx.view, &fx.semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        // Relational regime: equality slashes the set harder than a numeric range.
        let eq = Op::Filter {
            preds: vec![Pred::Eq {
                prop: "type".into(),
                value: "Doc".into(),
            }],
        };
        let range = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2000.0,
            }],
        };
        let s_eq = card.selectivity(&eq, 1_000.0, &ctx);
        let s_range = card.selectivity(&range, 1_000.0, &ctx);
        assert!(s_eq < s_range, "Eq is more selective than a numeric range");

        // The SAME crossover the cost model draws, now over an estimator-scale seed: a very
        // selective predicate ⇒ filter-first; a broad one ⇒ vector-first.
        let emb = card.stats().embedding_count.max(2);
        assert_eq!(
            CostModel::order(&Stats::estimate(10_000, 0.01, 10, emb)),
            Order::FilterFirst
        );
        assert_eq!(
            CostModel::order(&Stats::estimate(10_000, 0.98, 10, emb)),
            Order::VectorFirst
        );

        // Time regime: `AsOf` is a BROAD temporal narrower (most facts live); a brute-force
        // vector `Rank` costs strictly more per row than the cheap relational `Filter`.
        assert!(
            card.selectivity(
                &Op::AsOf {
                    ts: 1.0,
                    axis: Default::default()
                },
                1_000.0,
                &ctx
            ) >= 0.5,
            "AsOf keeps most rows"
        );
        let rank = Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        };
        assert!(
            card.cost_of(&rank, 1_000.0, &ctx).weight() > card.cost_of(&eq, 1_000.0, &ctx).weight(),
            "a brute-force vector Rank is the expensive leg"
        );
    }

    /// The per-column histogram catalog is MEMOIZED on the snapshot (CONCEPT:EG-KG.query.column-range-stats):
    /// (a) the memoized `collect` is byte-identical to a fresh `compute` over the SAME snapshot,
    ///     and the identity carries through to the estimator's selectivity; (b) a write produces
    ///     a FRESH snapshot whose memo starts empty, so its stats are recomputed over the new
    ///     data — a stale hit is structurally impossible because the memo shares the snapshot's
    ///     lifetime. Proves both halves of the memo's correctness contract.
    #[cfg(feature = "query")]
    #[test]
    fn column_stats_memoized_and_snapshot_scoped() {
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        fn blob(v: serde_json::Value) -> Vec<u8> {
            rmp_serde::to_vec_named(&v).unwrap()
        }

        let core = GraphCore::new();
        for (id, year) in [("d1", 2020.0), ("d2", 2024.0), ("d3", 2028.0)] {
            core.add_node(
                id.into(),
                blob(serde_json::json!({ "type": "Doc", "year": year })),
            );
        }
        let v1 = core.analysis_snapshot();

        // (a) The memo is COLD before the first collect and WARM after — and the memoized
        //     result equals a fresh recompute over the same snapshot, bucket-for-bucket.
        assert!(
            v1.plan_stats_memo.get().is_none(),
            "a fresh snapshot's memo is empty"
        );
        let fresh = ColumnStats::compute(&v1);
        let memoized = ColumnStats::collect(&v1);
        assert!(
            v1.plan_stats_memo.get().is_some(),
            "collect populated the snapshot's memo"
        );
        assert_eq!(fresh.len(), memoized.len());
        let n = 2025.0;
        assert_eq!(fresh.frac_gt("year", n), memoized.frac_gt("year", n));
        assert_eq!(fresh.frac_lt("year", n), memoized.frac_lt("year", n));
        // A second collect returns the SAME (now cached) value — no rescan, same answer.
        assert_eq!(
            ColumnStats::collect(&v1).frac_gt("year", n),
            memoized.frac_gt("year", n)
        );

        // The identity carries through the live estimator: the range-filter selectivity is
        // the same whether the column stats came from the memo or a forced recompute.
        let sem = SemanticStore::new();
        let ctx = crate::exec::PlanCtx::new(&v1, &sem);
        let range = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n,
            }],
        };
        let card_memo = ModalityCardinality::new(PlanStats::collect(&ctx));
        let mut ps_fresh = PlanStats::collect(&ctx);
        ps_fresh.column_stats = ColumnStats::compute(&v1); // bypass the memo
        let card_fresh = ModalityCardinality::new(ps_fresh);
        assert_eq!(
            card_memo.selectivity(&range, 100.0, &ctx),
            card_fresh.selectivity(&range, 100.0, &ctx),
            "memoized and recomputed stats yield identical selectivity"
        );

        // (b) A write → a FRESH snapshot with an empty memo → stats recomputed over the new
        //     data. Adding a far-larger `year` extends the column range, so frac_gt shifts.
        core.add_node(
            "d4".into(),
            blob(serde_json::json!({ "type": "Doc", "year": 9000.0 })),
        );
        let v2 = core.analysis_snapshot();
        assert!(
            v2.plan_stats_memo.get().is_none(),
            "the post-write snapshot is a new GraphView with an empty memo"
        );
        let s2 = ColumnStats::collect(&v2);
        assert_ne!(
            s2.frac_gt("year", n),
            memoized.frac_gt("year", n),
            "the fresh snapshot recomputes over the new data — never a stale memo hit"
        );
        // The original snapshot's memo is untouched and still describes the ORIGINAL data.
        assert_eq!(
            ColumnStats::collect(&v1).frac_gt("year", n),
            memoized.frac_gt("year", n),
            "an old snapshot keeps its own stats — snapshots are independent"
        );
    }

    // ── RANK 2: the pluggable IndexMethod seam ────────────────────────────────────

    /// Both registered index methods claim exactly the `Op` variants they should, and
    /// nothing else — the dispatch [`crate::optimizer::is_rank`] now delegates to.
    #[test]
    fn index_method_registry_dispatches_ann_and_bm25() {
        let rank = Op::Rank {
            query: vec![1.0, 0.0],
        };
        let rank_embed = Op::RankEmbed { text: "q".into() };
        let filter = Op::Filter { preds: vec![] };
        assert!(is_index_method_op(&rank), "vector Rank is registered");
        assert!(
            is_index_method_op(&rank_embed),
            "RankEmbed is registered under the same ANN method"
        );
        assert!(
            !is_index_method_op(&filter),
            "Filter is not an index-backed reranker"
        );

        #[cfg(feature = "text")]
        {
            let rank_text = Op::RankText { query: "q".into() };
            assert!(
                is_index_method_op(&rank_text),
                "RankText is now registered under the BM25 IndexMethod"
            );
        }
    }

    /// Before this increment `Op::RankText` fell through `cost_of`/`rows_out`'s wildcard
    /// pass-through (`in_card` unchanged, no cost at all). It now gets a REAL cost model
    /// through the same [`IndexMethod`] seam ANN uses: as a SOURCE it is bounded by
    /// [`DEFAULT_TOP_K`] (not the raw node count), and a candidate-restricted rerank
    /// costs strictly more than a bare pass-through would.
    #[cfg(feature = "text")]
    #[test]
    fn bm25_rank_text_gets_a_real_cost_model() {
        let fx = crate::fixture::build();
        let ctx = crate::exec::PlanCtx::new(&fx.view, &fx.semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));
        let rank_text = Op::RankText { query: "q".into() };

        // As a SOURCE (empty input): bounded by DEFAULT_TOP_K, not a pass-through zero.
        let source_rows = card.rows_out(&rank_text, 0.0, &ctx);
        assert!(
            source_rows > 0.0 && source_rows <= DEFAULT_TOP_K as f64,
            "a SOURCE RankText is a bounded top-k probe, got {source_rows}"
        );

        // Candidate-restricted: the estimated cost is strictly positive and scales with
        // in_card (a real per-row cost), unlike the old wildcard `(in_card.max(1.0), ..)`
        // pass-through, which this now overrides with its own arm.
        let cost_10 = card.cost_of(&rank_text, 10.0, &ctx).weight();
        let cost_100 = card.cost_of(&rank_text, 100.0, &ctx).weight();
        assert!(
            cost_100 > cost_10,
            "BM25 rerank cost must scale with the candidate count: {cost_10} vs {cost_100}"
        );
    }

    /// [`CostModel::overfetch_pool_size`] reproduces the `top_k/selectivity` formula
    /// [`CostModel::vector_first_cost`] computes inline — the extraction changed
    /// nothing about the existing vector-leg numbers.
    #[test]
    fn overfetch_pool_size_matches_the_vector_first_cost_formula() {
        assert_eq!(CostModel::overfetch_pool_size(10, 0.1), 100);
        assert_eq!(CostModel::overfetch_pool_size(10, 1.0), 10);
        // A near-zero selectivity is clamped, never divides by zero / overflows.
        assert!(CostModel::overfetch_pool_size(10, 0.0) > 0);
    }

    // ── RANK 11: the Bloom filter join-probe gate ─────────────────────────────────

    #[cfg(feature = "text")]
    #[test]
    fn bloom_filter_never_false_negatives() {
        let ids: Vec<String> = (0..500).map(|i| format!("id-{i}")).collect();
        let filter = BloomFilter::from_ids(ids.iter().map(String::as_str), ids.len());
        for id in &ids {
            assert!(
                filter.might_contain(id),
                "every INSERTED id must test as present: {id}"
            );
        }
    }

    #[cfg(feature = "text")]
    #[test]
    fn bloom_filter_bounded_false_positive_rate() {
        let members: Vec<String> = (0..1_000).map(|i| format!("member-{i}")).collect();
        let filter = BloomFilter::from_ids(members.iter().map(String::as_str), members.len());
        let non_members: Vec<String> = (0..5_000).map(|i| format!("absent-{i}")).collect();
        let false_positives = non_members
            .iter()
            .filter(|id| filter.might_contain(id))
            .count();
        let rate = false_positives as f64 / non_members.len() as f64;
        // Sized at the default 1% target; a generous 10% ceiling keeps this robust to
        // hash-distribution noise while still catching a badly broken implementation
        // (e.g. one that degenerates to "always true").
        assert!(
            rate < 0.10,
            "false-positive rate too high: {rate} ({false_positives}/{})",
            non_members.len()
        );
    }

    #[cfg(feature = "text")]
    #[test]
    fn bloom_filter_empty_never_panics_and_rejects_everything_absent() {
        let filter = BloomFilter::from_ids(std::iter::empty(), 0);
        assert!(!filter.might_contain("anything"));
    }
}
