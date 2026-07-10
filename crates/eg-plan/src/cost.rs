//! The cross-modal cost model (CONCEPT:EG-KG.query.concept-14): given a FILTER and a RANK over the
//! same seed set, which goes first?
//!
//! This is the kernel of the whole "unified planner" argument. The win of one engine
//! over three siloed round-trips is *cross-modal reordering* — the planner puts the
//! cheapest, most-selective operator first regardless of which modality it belongs
//! to. A caller stitching three surfaces in Python is locked into whatever fixed
//! order it hand-coded; the planner is not. The decision is real, with two
//! contrasting regimes the spike proved (`~/workspace/reports/spike-unified-findings.md`):
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
        let fetch = (s.top_k as f64) / s.filter_selectivity.max(1e-9);
        s.cost_vector_topk + fetch * (s.cost_vector_per_row + s.cost_filter_per_row)
    }

    /// Choose the cheaper order for a (FILTER, RANK) pair over the same set.
    pub fn order(s: &Stats) -> Order {
        if Self::filter_first_cost(s) <= Self::vector_first_cost(s) {
            Order::FilterFirst
        } else {
            Order::VectorFirst
        }
    }

    /// Reorder an adjacent `Filter`/`Rank` pair (in either input order) into the
    /// cost-optimal sequence. Other ops pass through untouched — this is the single,
    /// focused reordering rule this increment needs to demonstrate the principle.
    ///
    /// Only an ADJACENT pair is reordered: adjacency is the structural proof the two
    /// commute over the same id-set (a `Traverse` between them would change the seed
    /// either operates on, so they no longer commute). A real optimizer would prove
    /// commutativity structurally across longer spans; that generalization is later
    /// optimizer work (predicate-pushdown-through-traversal).
    ///
    /// As of the cross-modal optimizer (CONCEPT:EG-KG.query.filter-pushdown-rule) this is the
    /// low-level swap PRIMITIVE the engine's `FilterAsOfBeforeRank` rule folds in: the rule
    /// derives its [`Stats`] from the plan-time [`crate::cost::Cardinality`] estimators and
    /// then calls the SAME [`Self::order`] decision + [`Self::place_narrower`] swap this fn
    /// uses, so there is ONE reorder implementation (No-Legacy). Kept `pub` for the
    /// facade/server caller that passes an already-built `Stats`.
    pub fn reorder_filter_rank(plan: Vec<Op>, s: &Stats) -> Vec<Op> {
        let filter_idx = plan.iter().position(|o| matches!(o, Op::Filter { .. }));
        let rank_idx = plan.iter().position(|o| matches!(o, Op::Rank { .. }));
        let (Some(fi), Some(ri)) = (filter_idx, rank_idx) else {
            return plan; // nothing to reorder
        };
        if fi.abs_diff(ri) != 1 {
            return plan; // not an adjacent (provably commuting) pair
        }
        let want = Self::order(s);
        Self::place_narrower(plan, fi, ri, want == Order::FilterFirst)
    }

    /// The adjacent-pair SWAP primitive shared by [`Self::reorder_filter_rank`] and the
    /// cross-modal optimizer's reorder rules (CONCEPT:EG-KG.query.filter-pushdown-rule). Given the
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

// ── Cross-modal cost/cardinality catalog + per-modality estimators ───────────────
// (CONCEPT:EG-KG.query.cardinality-estimators) — the plan-time inputs Lane A's optimizer
// reads. Everything here stays in eg-plan (Rule R1): NOTHING is added to the wire `Op`.

/// Cheap, O(1) catalog statistics collected ONCE per `plan_optimize` call
/// (CONCEPT:EG-KG.query.cardinality-estimators). Deliberately derived from `.len()` on the
/// snapshot's maps — NEVER a per-node blob scan — so optimizing a plan costs O(1), not O(N):
/// the estimators trade a little accuracy for a cost that never scales with graph size. A
/// real catalog (histograms / per-label counts) can later feed richer numbers through the
/// SAME [`Cardinality`] interface without touching the rules.
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
    /// The ONE non-O(1) member: derived from a single cheap pass over the resident node
    /// property blobs so a `GtNum`/`LtNum` range predicate's selectivity is estimated from the
    /// REAL data distribution instead of a fixed heuristic. Bounded (fixed buckets + a capped
    /// per-column sample), so its memory never scales with graph size.
    pub column_stats: ColumnStats,
}

#[cfg(feature = "query")]
impl PlanStats {
    /// Collect the catalog from a [`crate::exec::PlanCtx`] snapshot. The scalar counts are
    /// O(1) (`.len()` on the snapshot maps); [`ColumnStats::collect`] adds ONE cheap pass over
    /// the resident node blobs to derive per-column numeric distributions (the input the
    /// range-selectivity estimate reads) — done once per `plan_optimize`, not per predicate.
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
            let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
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
/// Holds the O(1) [`PlanStats`] catalog; `rows_out` sizes an op's output and `cost_of`
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
    /// product): equality is highly selective, JSONPath / spatial broad. A numeric RANGE
    /// (`GtNum`/`LtNum`) is estimated from the REAL column distribution via the collected
    /// [`ColumnStats`] histogram (CONCEPT:EG-KG.query.column-range-stats) — so a genuinely selective
    /// range (`year > 2028` in a 2000..2029 column) reads as selective and reorders
    /// filter-first, while a broad one (`year > 2001`) stays; only when the column has NO
    /// numeric stats does it fall back to the fixed `RANGE_SEL_FALLBACK` heuristic. Equality /
    /// JSONPath / spatial estimates are unchanged.
    fn filter_selectivity(&self, preds: &[crate::algebra::Pred]) -> f64 {
        use crate::algebra::Pred;
        /// The legacy fixed range heuristic — only used when the column is unknown to the stats.
        const RANGE_SEL_FALLBACK: f64 = 0.33;
        let mut sel = 1.0f64;
        for p in preds {
            // The wildcard catches the `geo` spatial `Pred` variants (and any future kind);
            // in a non-`geo` build the relational/JSONPath arms are already exhaustive, so the
            // wildcard is unreachable there — allowed so the SAME match compiles under every
            // feature subset (the `where_clause`/exec split-out precedent).
            #[allow(unreachable_patterns)]
            let s = match p {
                Pred::Eq { .. } => 0.1,
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
impl Cardinality for ModalityCardinality {
    /// Estimated rows OUT of `op` given `in_card` rows in — one arm per modality
    /// (CONCEPT:EG-KG.query.cardinality-estimators). Feature-gated ops that are absent in this build
    /// fall to the pass-through wildcard, so the match compiles under ANY feature subset.
    fn rows_out(&self, op: &Op, in_card: f64, _ctx: &crate::exec::PlanCtx) -> f64 {
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
            // Rerankers / context / other sources: preserve the row count (pass-through).
            _ => in_card,
        }
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
/// on `query` because it borrows the `query`-tier [`PlanCtx`]; the dep-free [`CostModel`] /
/// [`Stats`] / [`reorder_filter_rank`] stay available in the Pi tier, unchanged.
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

        // The reorder rewrites the plan to the winner in each regime.
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
        let reordered = CostModel::reorder_filter_rank(plan.clone(), &broad);
        assert!(
            matches!(reordered[0], Op::Rank { .. }),
            "broad filter → vector-first puts Rank at the front"
        );
        let kept = CostModel::reorder_filter_rank(plan, &selective);
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
}
