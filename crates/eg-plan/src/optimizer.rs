//! The cross-modal cost-based optimizer (CONCEPT:EG-KG.query.xmodal-cost-optimizer) — a small
//! RULE ENGINE over the logical `Vec<Op>` that Lane 0's [`crate::exec::plan_optimize`] seam
//! calls BEFORE execution. It rewrites a plan into a cheaper-but-EQUIVALENT one by reordering
//! operators across modalities, the single decision a unified planner exists to make that a
//! caller stitching three siloed surfaces in Python cannot: the planner puts the cheapest,
//! most-selective operator first regardless of which modality (relational / vector / graph /
//! OWL / time) it belongs to.
//!
//! ## What it does
//!
//! Every rule is driven by the plan-time [`crate::cost::ModalityCardinality`] estimators
//! (graph `Traverse` degree×path-length, ANN `Rank` recall@k / over-fetch, OWL `Reason`
//! closure + confidence decay, bi-temporal `AsOf` range selectivity, relational `Filter`
//! predicate selectivity) and the shared [`crate::cost::CostEstimate`] triple — the Lane-0
//! contracts. The rules:
//!
//!  1. [`FilterAsOfBeforeRank`] (CONCEPT:EG-KG.query.filter-pushdown-rule) — push a SELECTIVE
//!     id-set narrower (`Filter` / `AsOf`) ahead of an adjacent expensive vector `Rank` when
//!     the cost model says filter-first wins; keep the `Rank` first when the narrower is
//!     broad (vector-first). Folds the legacy [`crate::cost::CostModel::reorder_filter_rank`]
//!     into the engine as one rule, sharing its decision + swap primitive (No-Legacy).
//!  2. [`ReorderReasonRank`] (CONCEPT:EG-KG.query.reason-rank-reorder-rule) — order an adjacent
//!     mid-pipeline OWL `Reason` (a confidence-preserving FILTER) vs a vector `Rank` by which
//!     shrinks the candidate set more, so the second leg runs over fewer rows.
//!  3. [`ReorderFuseBranches`] (CONCEPT:EG-KG.query.fuse-rrf-branch-reorder-rule) — reorder the
//!     `FuseRrf` branches by ascending branch cost. Reciprocal-rank fusion sums into a map and
//!     sorts by `(score, id)`, so the fused result is INDEPENDENT of branch order — reordering
//!     is byte-for-byte answer-preserving, it only runs the cheaper branches first.
//!
//! ## Why the reorders are answer-preserving (the EG-405 boundary)
//!
//! Op composition is NOT freely commutative: an intermediate EMPTY `RowSet` flips the FOLLOWING
//! op (`AsOf` / `Filter` / `Reason` / `Rank`) into SOURCE mode and re-seeds the whole graph
//! (EG-405, pinned by `plan_proptest::empty_intermediate_reseeds_source_breaks_commute`). So
//! this engine only ever:
//!  * reorders an ADJACENT pair (adjacency is the structural proof the two operate over the
//!    SAME id-set — a `Traverse`/`Scan` between them changes the seed, so they never cross it);
//!  * reorders a NARROWER against a `Rank` only (NEVER narrower-vs-narrower — that is the exact
//!    EG-405 witness, which must keep differing);
//!  * requires the pair's input to come from a preceding SOURCE (index ≥ 1), and BOTH candidate
//!    intermediates to stay ≥ 1 row (the proven non-empty regime), skipping the reorder
//!    otherwise. In that regime a single-`Rank` commuting pair yields the identical result SET
//!    and, because the lone `Rank` establishes the order either way, the identical ORDER.
//!
//! The differential oracle (`tests/differential_oracle.rs`) and the plan snapshots
//! (`tests/plan_snapshots.rs`) prove the rewritten plan returns the same `RowSet`.
//!
//! ## Gating
//!
//! Compiled under `query` (with the executor). Active by default; the runtime kill-switch
//! `EPISTEMIC_GRAPH_COST_OPT=0` makes [`crate::exec::plan_optimize`] an identity passthrough
//! (byte-for-byte the pre-optimizer fold). The `cost-opt` cargo feature (implies `query`) is
//! the facade tier selector — default-on in `full`.

use crate::algebra::{Op, Plan};
use crate::cost::{Cardinality, CostModel, ModalityCardinality, PlanStats, Stats, DEFAULT_TOP_K};
use crate::exec::PlanCtx;

/// Is the cross-modal optimizer active? True unless the runtime kill-switch
/// `EPISTEMIC_GRAPH_COST_OPT=0` is set (CONCEPT:EG-KG.query.xmodal-cost-optimizer) — then
/// [`crate::exec::plan_optimize`] is an identity passthrough.
pub fn enabled() -> bool {
    !matches!(
        std::env::var("EPISTEMIC_GRAPH_COST_OPT").ok().as_deref(),
        Some("0")
    )
}

/// Rewrite `plan` into a cheaper-but-equivalent plan (CONCEPT:EG-KG.query.xmodal-cost-optimizer).
/// Collects the O(1) [`PlanStats`] catalog once, binds the [`ModalityCardinality`] estimators,
/// and folds every rule over the op list in order. A no-op (identity clone) whenever no rule
/// finds a beneficial, provably-safe rewrite.
pub fn optimize(plan: &Plan, ctx: &PlanCtx) -> Plan {
    let card = ModalityCardinality::new(PlanStats::collect(ctx));
    let mut ops = plan.ops.clone();
    for rule in rules() {
        ops = rule.apply(ops, &card, ctx);
    }
    Plan::new(ops)
}

/// The ordered rule set the engine folds over a plan. `Reason`↔`Rank` and the `FuseRrf`
/// branch reorder only exist in a build that has the `owl` / `text` modality (their wire
/// variants are feature-gated), exactly like the executor arms.
fn rules() -> Vec<Box<dyn Rule>> {
    let mut rs: Vec<Box<dyn Rule>> = vec![Box::new(FilterAsOfBeforeRank)];
    #[cfg(feature = "owl")]
    rs.push(Box::new(ReorderReasonRank));
    #[cfg(feature = "text")]
    rs.push(Box::new(ReorderFuseBranches));
    rs
}

/// The names of the active optimizer rules, in application order
/// (CONCEPT:EG-KG.query.xmodal-cost-optimizer) — a small introspection surface (an EXPLAIN view /
/// tests). The set depends on the compiled modalities: `reason-rank-reorder` needs `owl`,
/// `fuse-rrf-branch-reorder` needs `text`.
pub fn rule_names() -> Vec<&'static str> {
    rules().iter().map(|r| r.name()).collect()
}

/// One cost-based rewrite over a logical op list (CONCEPT:EG-KG.query.xmodal-cost-optimizer). Each
/// rule takes ownership of the ops, rewrites in place, and returns them — so the engine folds
/// the rules into one pipeline. A rule MUST preserve the result set (see the module-level
/// EG-405 note): the differential oracle proves it.
trait Rule {
    /// A stable name for the rule (used in tests / any future EXPLAIN surface).
    fn name(&self) -> &'static str;
    /// Rewrite `ops` under the cardinality estimators, returning the (possibly reordered) list.
    fn apply(&self, ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op>;
}

// ── shared reorder machinery ────────────────────────────────────────────────────

/// The estimated cardinality flowing INTO position `j` — the left fold of the per-op
/// [`crate::cost::Cardinality`] estimator over `ops[..j]`, seeded empty (a leading source
/// ignores the `0` input). This is the pair's input size the reorder cost model reasons over.
fn card_before(ops: &[Op], j: usize, card: &ModalityCardinality, ctx: &PlanCtx) -> f64 {
    let mut c = 0.0;
    for op in &ops[..j] {
        c = card.rows_out(op, c, ctx);
    }
    c
}

/// The `top_k` a `Rank` at `rank_idx` ultimately feeds — the first trailing `Limit`'s `k`,
/// else [`DEFAULT_TOP_K`]. The over-fetch denominator in the vector-first cost.
fn trailing_top_k(ops: &[Op], rank_idx: usize) -> usize {
    ops[rank_idx + 1..]
        .iter()
        .find_map(|o| match o {
            Op::Limit { k } => Some(*k),
            _ => None,
        })
        .unwrap_or(DEFAULT_TOP_K)
        .max(1)
}

/// Is `op` a vector `Rank` (the expensive reranker a narrower is ordered against)?
fn is_rank(op: &Op) -> bool {
    matches!(op, Op::Rank { .. } | Op::RankEmbed { .. })
}

/// Reorder EVERY adjacent `(narrower, Rank)` pair (in either order) where `is_narrower` holds,
/// by the cost model, sharing [`CostModel::order`] + [`CostModel::place_narrower`] with the
/// legacy `reorder_filter_rank` (CONCEPT:EG-KG.query.filter-pushdown-rule). Guarded to the proven
/// EG-405-safe regime: the pair must sit AFTER a source (index ≥ 1) and BOTH candidate
/// intermediates must stay ≥ 1 row, else the pair is left untouched.
fn reorder_narrower_rank_pairs(
    mut ops: Vec<Op>,
    is_narrower: impl Fn(&Op) -> bool,
    card: &ModalityCardinality,
    ctx: &PlanCtx,
) -> Vec<Op> {
    let mut j = 1; // a reorderable pair never starts at index 0 (that op is the source seed).
    while j + 1 < ops.len() {
        let (a, b) = (&ops[j], &ops[j + 1]);
        // Exactly one of the pair is a Rank and the other a matching narrower.
        let narrower_first = is_narrower(a) && is_rank(b);
        let rank_first = is_rank(a) && is_narrower(b);
        if !(narrower_first || rank_first) {
            j += 1;
            continue;
        }
        let (narrower_idx, rank_idx) = if narrower_first {
            (j, j + 1)
        } else {
            (j + 1, j)
        };

        let in_card = card_before(&ops, j, card, ctx);
        let narrower_sel = card.selectivity(&ops[narrower_idx], in_card, ctx);
        let rank_sel = card.selectivity(&ops[rank_idx], in_card, ctx);

        // EG-405 guard: only reorder in the NON-EMPTY regime — BOTH orders must keep their
        // intermediate ≥ 1 row, else swapping could flip the trailing op into SOURCE mode and
        // change the answer. (Also skips the degenerate empty-graph case.)
        if in_card * narrower_sel < 1.0 || in_card * rank_sel < 1.0 {
            j += 2;
            continue;
        }

        let top_k = trailing_top_k(&ops, rank_idx);
        let stats = Stats::estimate(
            in_card.round().max(1.0) as usize,
            narrower_sel,
            top_k,
            card.stats().embedding_count,
        );
        let want_narrower_first = CostModel::order(&stats) == crate::cost::Order::FilterFirst;
        ops = CostModel::place_narrower(ops, narrower_idx, rank_idx, want_narrower_first);
        j += 2; // the pair is settled; skip past it.
    }
    ops
}

// ── Rule 1: push a selective Filter / AsOf before an adjacent Rank ───────────────

/// Push a SELECTIVE relational `Filter` or bi-temporal `AsOf` ahead of an adjacent vector
/// `Rank` when the cost model says filter-first wins; keep `Rank` first for a BROAD narrower
/// (CONCEPT:EG-KG.query.filter-pushdown-rule). Folds the legacy `reorder_filter_rank` in as one rule.
struct FilterAsOfBeforeRank;

impl Rule for FilterAsOfBeforeRank {
    fn name(&self) -> &'static str {
        "filter-asof-before-rank"
    }
    fn apply(&self, ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op> {
        reorder_narrower_rank_pairs(
            ops,
            |o| matches!(o, Op::Filter { .. } | Op::AsOf { .. }),
            card,
            ctx,
        )
    }
}

// ── Rule 2: order Reason vs Rank by candidate-set change ─────────────────────────

/// Order an adjacent mid-pipeline OWL `Reason` (a confidence-preserving FILTER over the
/// candidate set) vs a vector `Rank` by which shrinks the set more, so the second leg runs
/// over fewer rows (CONCEPT:EG-KG.query.reason-rank-reorder-rule). Same adjacency + EG-405 guard as
/// Rule 1; only a mid-pipeline `Reason` (input from a preceding source, index ≥ 1) is a
/// narrower — a LEADING `Reason` is a SOURCE and is never reordered.
#[cfg(feature = "owl")]
struct ReorderReasonRank;

#[cfg(feature = "owl")]
impl Rule for ReorderReasonRank {
    fn name(&self) -> &'static str {
        "reason-rank-reorder"
    }
    fn apply(&self, ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op> {
        reorder_narrower_rank_pairs(ops, |o| matches!(o, Op::Reason { .. }), card, ctx)
    }
}

// ── Rule 3: reorder FuseRrf branches by ascending cost ───────────────────────────

/// Reorder the sub-plan `branches` of every `FuseRrf` op by ascending branch cost
/// (CONCEPT:EG-KG.query.fuse-rrf-branch-reorder-rule). Reciprocal-rank fusion accumulates into a map
/// and sorts by `(score desc, id)`, so the fused result is INDEPENDENT of branch order — this
/// is byte-for-byte answer-preserving and only changes WHICH branch runs first (cheaper legs
/// first). Recurses into nested `FuseRrf` branches.
#[cfg(feature = "text")]
struct ReorderFuseBranches;

#[cfg(feature = "text")]
impl Rule for ReorderFuseBranches {
    fn name(&self) -> &'static str {
        "fuse-rrf-branch-reorder"
    }
    fn apply(&self, ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op> {
        // The cardinality flowing into each op (a `FuseRrf` runs every branch over the SAME
        // seed), so branch cost is estimated against the op's own input size.
        let mut in_card = 0.0;
        ops.into_iter()
            .map(|op| {
                let seed = in_card;
                in_card = card.rows_out(&op, in_card, ctx);
                match op {
                    Op::FuseRrf { branches, k } => Op::FuseRrf {
                        branches: sort_branches_by_cost(branches, seed, card, ctx),
                        k,
                    },
                    other => other,
                }
            })
            .collect()
    }
}

/// Sort `branches` (each a `Vec<Op>` sub-plan) by ascending total cost over `seed` input rows,
/// recursing so nested `FuseRrf` branches are ordered too. A stable sort keeps equal-cost
/// branches in their original order (determinism). `seed` is the shared input every branch
/// runs over.
#[cfg(feature = "text")]
fn sort_branches_by_cost(
    branches: Vec<Vec<Op>>,
    seed: f64,
    card: &ModalityCardinality,
    ctx: &PlanCtx,
) -> Vec<Vec<Op>> {
    // First recurse: reorder any nested FuseRrf branches within each branch.
    let mut branches: Vec<Vec<Op>> = branches
        .into_iter()
        .map(|branch| {
            let mut bc = seed;
            branch
                .into_iter()
                .map(|op| {
                    let s = bc;
                    bc = card.rows_out(&op, bc, ctx);
                    match op {
                        Op::FuseRrf { branches, k } => Op::FuseRrf {
                            branches: sort_branches_by_cost(branches, s, card, ctx),
                            k,
                        },
                        other => other,
                    }
                })
                .collect()
        })
        .collect();
    branches.sort_by(|a, b| {
        branch_cost(a, seed, card, ctx)
            .partial_cmp(&branch_cost(b, seed, card, ctx))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    branches
}

/// The total plan-time cost of one `FuseRrf` branch over `seed` input rows — the sum of each
/// op's [`crate::cost::CostEstimate::weight`], folding the running cardinality through the
/// branch (CONCEPT:EG-KG.query.fuse-rrf-branch-reorder-rule).
#[cfg(feature = "text")]
fn branch_cost(branch: &[Op], seed: f64, card: &ModalityCardinality, ctx: &PlanCtx) -> f64 {
    let mut c = seed;
    let mut total = 0.0;
    for op in branch {
        total += card.cost_of(op, c, ctx).weight();
        c = card.rows_out(op, c, ctx);
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::{Op, Plan, Pred};
    use crate::{execute, PlanCtx, RowSet};

    fn sorted_ids(rs: &RowSet) -> Vec<String> {
        let mut v = rs.ids();
        v.sort();
        v
    }

    /// The rule set exposes stable names via the public introspection surface.
    #[test]
    fn rule_names_are_stable() {
        let names = rule_names();
        assert!(names.contains(&"filter-asof-before-rank"));
        #[cfg(feature = "owl")]
        assert!(names.contains(&"reason-rank-reorder"));
        #[cfg(feature = "text")]
        assert!(names.contains(&"fuse-rrf-branch-reorder"));
    }

    /// The per-modality estimators produce sane relative magnitudes: a `Scan` seeds a
    /// fraction of the graph, a `Filter` narrows, a `Rank`'s coverage is in `(0,1]`, and a
    /// brute-force vector `Rank` costs more per row than a relational `Filter`.
    #[test]
    fn estimators_are_sane() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        let scan = Op::Scan {
            label: "Doc".into(),
        };
        let scan_rows = card.rows_out(&scan, 0.0, &ctx);
        assert!(scan_rows > 0.0 && scan_rows <= fx.view.node_properties.len() as f64);

        let filter = Op::Filter {
            preds: vec![Pred::Eq {
                prop: "lang".into(),
                value: "en".into(),
            }],
        };
        assert!(
            card.selectivity(&filter, 100.0, &ctx) < 1.0,
            "a Filter narrows"
        );

        let rank = Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        };
        let cov = card.selectivity(&rank, 100.0, &ctx);
        assert!(cov > 0.0 && cov <= 1.0, "Rank coverage in (0,1]");
        assert!(
            card.cost_of(&rank, 100.0, &ctx).weight() > card.cost_of(&filter, 100.0, &ctx).weight(),
            "a brute-force vector Rank costs more than a relational Filter"
        );
    }

    /// A selective `Filter` behind a `Rank` is pushed AHEAD of it, and the rewrite is
    /// answer-preserving (the in-crate differential proof).
    #[test]
    fn selective_filter_pushed_ahead_of_rank_and_preserves_result() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let original = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2022.0,
                }],
            },
        ]);
        let opt = optimize(&original, &ctx);
        assert!(
            matches!(opt.ops[1], Op::Filter { .. }) && matches!(opt.ops[2], Op::Rank { .. }),
            "selective filter pushed before Rank, got {:?}",
            opt.ops
        );
        assert_eq!(
            sorted_ids(&execute(&original, &ctx).unwrap()),
            sorted_ids(&execute(&opt, &ctx).unwrap()),
            "reorder must preserve the result set"
        );
    }

    /// A `Traverse` barrier between a `Rank` and a `Filter` forbids the reorder (the pair is
    /// not adjacent → identity), and the result is unchanged.
    #[test]
    fn traverse_barrier_blocks_reorder() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let original = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 1,
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2022.0,
                }],
            },
        ]);
        let opt = optimize(&original, &ctx);
        assert_eq!(
            original.ops, opt.ops,
            "no adjacent narrower/Rank pair → identity"
        );
    }

    /// The `FuseRrf` branch-reorder puts the CHEAPER branch first (a graph-native rerank
    /// before a brute-force vector `Rank`) — a change that RRF makes result-invariant.
    #[cfg(feature = "text")]
    #[test]
    fn fuse_branches_sorted_cheapest_first() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));
        let expensive = vec![Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        }];
        let cheap = vec![Op::RankMentions {}];
        let sorted =
            sort_branches_by_cost(vec![expensive.clone(), cheap.clone()], 100.0, &card, &ctx);
        assert_eq!(sorted[0], cheap, "cheapest branch runs first");
        assert_eq!(sorted[1], expensive);
    }
}
