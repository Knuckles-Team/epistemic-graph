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
        let mut out = plan;
        let (lo, hi) = (fi.min(ri), fi.max(ri));
        let filter_is_lo = matches!(out[lo], Op::Filter { .. });
        let filter_should_be_first = want == Order::FilterFirst;
        if filter_is_lo != filter_should_be_first {
            out.swap(lo, hi);
        }
        out
    }
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
}
