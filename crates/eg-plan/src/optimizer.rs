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
    // Runs LAST: it re-scans the (possibly pairwise-touched) op list for runs of 3+
    // reorderable ops and replaces them with the true cost-minimal permutation, so it
    // always has the final say regardless of what the pairwise rules above already did
    // (CONCEPT:EG-KG.query.global-plan-cost).
    rs.push(Box::new(GlobalChainCost));
    rs
}

/// The names of the active optimizer rules, in application order
/// (CONCEPT:EG-KG.query.xmodal-cost-optimizer) — a small introspection surface (an EXPLAIN view /
/// tests). The set depends on the compiled modalities: `reason-rank-reorder` needs `owl`,
/// `fuse-rrf-branch-reorder` needs `text`.
pub fn rule_names() -> Vec<&'static str> {
    rules().iter().map(|r| r.name()).collect()
}

// ── DAG-aware optimizer (CONCEPT:EG-KG.query.dag-optimizer, E5 phase 3) ──────────

/// The DAG generalization of [`optimize`] (CONCEPT:EG-KG.query.dag-optimizer): reorder ops
/// WITHIN each maximal LINEAR CHAIN SEGMENT of `dag` — [`chain_segments`] — by splicing the
/// segment's op sequence through the UNCHANGED linear engine above (the SAME `rules()` over
/// the SAME [`Cardinality`]/[`crate::cost::CostEstimate`] contracts every existing plan
/// already reorders through; nothing here duplicates a cost formula).
///
/// **The EG-405 adjacency guard, reimplemented as a DAG narrowing.** The linear engine's
/// invariant — reorder only an ADJACENT run, never past an intermediate that could go empty
/// and re-seed a downstream op as a SOURCE — assumed a `Vec<Op>` has exactly one predecessor
/// per position. A DAG breaks that assumption at a BRANCH (a node with 2+ inputs) or a
/// FAN-OUT (a node whose output feeds 2+ others): reordering an op across such a boundary
/// could change what the OTHER branch/consumer observes, which the linear engine never had to
/// reason about. [`chain_segments`] computes exactly the runs where that risk is ABSENT — a
/// node extends its predecessor's segment iff it is that predecessor's ONLY consumer AND that
/// predecessor is its ONLY input — so a rewrite NEVER crosses a branch/fan-out boundary. This
/// is a STRICT NARROWING of the linear rule: a degenerate chain dag (every `From<Plan>`
/// conversion) has no branches at all, so `chain_segments` returns ONE segment spanning every
/// node and `optimize_dag` is IDENTICAL to `optimize` (proved by
/// `dag_optimize_matches_linear_optimize_for_a_chain` below); a genuine multi-branch dag gets
/// the SAME per-segment reorder, just never applied across a join — proved not to cross a
/// branch by `optimizer_never_reorders_across_a_branch_boundary`.
///
/// A branch/join node's own `op` position never moves (only nodes WITHIN a segment can swap
/// with each other); the multi-branch JOIN itself (combining 2+ parents) is `dag_exec`'s
/// capability, not a cost decision this optimizer makes.
pub fn optimize_dag(dag: &crate::dag::PlanDag, ctx: &PlanCtx) -> crate::dag::PlanDag {
    if dag.nodes.is_empty() {
        return dag.clone();
    }
    let card = ModalityCardinality::new(PlanStats::collect(ctx));
    let mut nodes = dag.nodes.clone();
    for segment in chain_segments(dag) {
        if segment.len() < 2 {
            continue; // nothing to reorder in a length-1 segment
        }
        let ops: Vec<Op> = segment.iter().map(|&id| nodes[id].op.clone()).collect();
        let mut rewritten = ops.clone();
        for rule in rules() {
            rewritten = rule.apply(rewritten, &card, ctx);
        }
        // Defensive: a `Rule` must never change the op count (it only reorders). Skip the
        // splice rather than risk mis-mapping ids onto the wrong node if one ever did.
        if rewritten.len() != segment.len() {
            continue;
        }
        for (&id, op) in segment.iter().zip(rewritten) {
            nodes[id].op = op;
        }
    }
    crate::dag::PlanDag::new(nodes)
}

/// Every maximal LINEAR CHAIN SEGMENT of `dag` (CONCEPT:EG-KG.query.dag-optimizer) — a run of
/// node ids, in dependency order, where each node past the first has EXACTLY the previous
/// node as its sole input, AND the previous node has EXACTLY this node as its sole consumer.
/// A branch point (2+ inputs) or a fan-out point (a node consumed by 2+ others) starts a NEW
/// segment — [`optimize_dag`]'s reorder never crosses that boundary. Every node appears in
/// EXACTLY ONE segment (singleton segments included), so the returned segments partition
/// `dag.nodes`. Falls back to a per-node partition (every node its own segment — a safe no-op
/// for the optimizer) on a malformed dag (`topo_order` errors) rather than panicking.
fn chain_segments(dag: &crate::dag::PlanDag) -> Vec<Vec<crate::dag::NodeId>> {
    let n = dag.nodes.len();
    let mut out_degree = vec![0usize; n];
    for node in &dag.nodes {
        for &input in &node.inputs {
            if input < n {
                out_degree[input] += 1;
            }
        }
    }
    let Ok(order) = dag.topo_order() else {
        return (0..n).map(|i| vec![i]).collect();
    };

    let mut segment_of: Vec<Option<usize>> = vec![None; n];
    let mut segments: Vec<Vec<crate::dag::NodeId>> = Vec::new();
    for id in order {
        let sole_parent = match dag.nodes[id].inputs.as_slice() {
            [only] => Some(*only),
            _ => None,
        };
        // Extend the parent's segment iff the seam is a clean 1:1 edge (this node's ONLY
        // input is the parent, AND the parent's ONLY consumer is this node) — topo order
        // guarantees the parent was already assigned a segment.
        let extend = sole_parent
            .filter(|&p| out_degree[p] == 1)
            .and_then(|p| segment_of[p]);
        match extend {
            Some(seg_idx) => {
                segments[seg_idx].push(id);
                segment_of[id] = Some(seg_idx);
            }
            None => {
                segment_of[id] = Some(segments.len());
                segments.push(vec![id]);
            }
        }
    }
    segments
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

// ── Rule 4: global cost-minimal reordering of a WHOLE reorderable chain ──────────

/// Is `op` a `Filter`/`AsOf`/`Reason`/`Rank`(`Embed`) — one of the kinds
/// [`GlobalChainCost`] (CONCEPT:EG-KG.query.global-plan-cost) may reorder because they all draw
/// candidates from the SAME id-set (the EG-405 commuting family the module doc defines).
/// `Traverse`/`Scan`/`FuseRrf`/time-series/etc. are BARRIERS: they change the seed (a fresh
/// candidate set), so a run never crosses one — mirrors the pairwise rules' adjacency rule,
/// generalized from "the next op" to "every op in this maximal run".
fn is_reorderable(op: &Op) -> bool {
    if matches!(op, Op::Filter { .. } | Op::AsOf { .. }) || is_rank(op) {
        return true;
    }
    #[cfg(feature = "owl")]
    if matches!(op, Op::Reason { .. }) {
        return true;
    }
    // EG-405 EXCLUSION (CONCEPT:EG-KG.epistemic.epistemic-substrate, E2): the epistemic ops do
    // NOT draw candidates from the same id-set the `Filter`/`AsOf`/`Reason`/`Rank` commuting
    // family shares — `EvidenceFor`/`Contradicts`/`SupportedBy` seed from a DIFFERENT edge-kind
    // projection than the graph's own topology, `BeliefAsOf`/`ConfidenceOp`/`SourceReliability`
    // re-score by a stateful belief-propagation walk (not a stateless per-row predicate), and
    // `ExplainBelief`'s flattened output depends on tree DEPTH/order — none of these commute
    // freely with a reorder the way a pure narrowing predicate does. Excluded EXPLICITLY (not
    // by omission) so a future edit to this match can't silently make them reorder-eligible.
    #[cfg(feature = "epistemic")]
    if matches!(
        op,
        Op::EvidenceFor { .. }
            | Op::Contradicts { .. }
            | Op::SupportedBy { .. }
            | Op::BeliefAsOf { .. }
            | Op::SourceReliability { .. }
            | Op::ConfidenceOp {}
            | Op::ExplainBelief { .. }
    ) {
        return false;
    }
    false
}

/// The bound on a single reorderable run's length before [`GlobalChainCost`] gives up on an
/// exhaustive permutation search (CONCEPT:EG-KG.query.global-plan-cost). `5! = 120` candidate
/// orderings is a trivial plan-time cost (each `optimize()` call already pays an O(1) stats
/// collection); a run longer than this is vanishingly rare in practice (it would mean 6+
/// `Filter`/`AsOf`/`Reason`/`Rank` ops chained with no `Traverse`/`Scan`/`Limit` between them) and
/// is left untouched rather than risk a combinatorial blowup.
const MAX_CHAIN_SEGMENT: usize = 5;

/// Reorder every maximal run of 3+ consecutive [`is_reorderable`] ops into its cost-minimal
/// permutation (CONCEPT:EG-KG.query.global-plan-cost) — the generalization of the pairwise
/// [`FilterAsOfBeforeRank`]/[`ReorderReasonRank`] rules from "one adjacent pair" to "the WHOLE
/// chain at once". A run of exactly 2 is left to those two rules (they already solve the pair
/// case optimally); this rule only fires where the pairwise mechanism structurally cannot see
/// far enough — 3 or more chained ops, where advancing by adjacent, non-overlapping pairs can
/// permanently strand a later op behind an already-decided pair (the module-level example: given
/// `[Rank, broad_filter, selective_filter]`, a pairwise pass commits `(Rank, broad_filter)` and
/// then jumps past `selective_filter` without ever comparing it to either). Runs never start at
/// index 0 (that op is the plan's SOURCE, not a narrower — same invariant the pairwise rules
/// enforce) and longer than [`MAX_CHAIN_SEGMENT`] are left untouched (see its doc).
struct GlobalChainCost;

impl Rule for GlobalChainCost {
    fn name(&self) -> &'static str {
        "global-chain-cost"
    }
    fn apply(&self, ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op> {
        reorder_chain_segments(ops, card, ctx)
    }
}

fn reorder_chain_segments(mut ops: Vec<Op>, card: &ModalityCardinality, ctx: &PlanCtx) -> Vec<Op> {
    let mut i = 1; // index 0 is always the source; never part of a reorderable run.
    while i < ops.len() {
        if !is_reorderable(&ops[i]) {
            i += 1;
            continue;
        }
        let start = i;
        let mut end = i + 1;
        while end < ops.len() && is_reorderable(&ops[end]) {
            end += 1;
        }
        let len = end - start;
        // Pairs are already optimal via the dedicated pairwise rules; only 3+ chains need
        // the exhaustive search, and only up to the bounded cap.
        if !(3..=MAX_CHAIN_SEGMENT).contains(&len) {
            i = end;
            continue;
        }
        let seed = card_before(&ops, start, card, ctx);
        let top_k = trailing_top_k(&ops, end - 1);
        let segment = &ops[start..end];
        if let Some(best) = best_permutation(segment, seed, top_k, card, ctx) {
            ops.splice(start..end, best);
        }
        i = end;
    }
    ops
}

/// Every permutation of `0..n`, depth-first, starting with the IDENTITY order — so a caller
/// that only replaces on a STRICTLY cheaper cost (see [`best_permutation`]) keeps the original
/// order on a tie (determinism, and "don't reorder for a wash"). `n` is bounded by
/// [`MAX_CHAIN_SEGMENT`], so the `O(n!)` blowup never exceeds 120 permutations.
fn permutations(indices: &[usize]) -> Vec<Vec<usize>> {
    if indices.is_empty() {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for (pos, &val) in indices.iter().enumerate() {
        let mut rest = indices.to_vec();
        rest.remove(pos);
        for mut tail in permutations(&rest) {
            let mut perm = Vec::with_capacity(tail.len() + 1);
            perm.push(val);
            perm.append(&mut tail);
            out.push(perm);
        }
    }
    out
}

/// The cost-minimal reordering of `segment` over `seed` input rows (CONCEPT:EG-KG.query.global-plan-cost)
/// — exhaustively costs every permutation via [`ModalityCardinality::permutation_cost`] and
/// returns the cheapest one that is EG-405-safe (skipping any permutation
/// [`ModalityCardinality::permutation_cost`] rejects). Returns `None` when every permutation is
/// unsafe (leaves `segment` untouched) or when the cheapest found is not STRICTLY cheaper than
/// the original (`permutations` yields the identity order first, so a tie keeps it).
fn best_permutation(
    segment: &[Op],
    seed: f64,
    top_k: usize,
    card: &ModalityCardinality,
    ctx: &PlanCtx,
) -> Option<Vec<Op>> {
    let n = segment.len();
    let mut best: Option<(f64, Vec<usize>)> = None;
    for perm in permutations(&(0..n).collect::<Vec<_>>()) {
        let refs: Vec<&Op> = perm.iter().map(|&idx| &segment[idx]).collect();
        let Some(cost) = card.permutation_cost(&refs, seed, top_k, ctx) else {
            continue;
        };
        let better = match &best {
            None => true,
            Some((best_cost, _)) => cost < *best_cost - 1e-9,
        };
        if better {
            best = Some((cost, perm));
        }
    }
    best.map(|(_, order)| order.into_iter().map(|idx| segment[idx].clone()).collect())
}

// ── adaptive re-optimization (CONCEPT:EG-KG.query.adaptive-reoptimization) ───────────────────

/// How far an ACTUAL cardinality may diverge from what the plan-time estimate predicted before
/// [`reoptimize_remaining`] bothers re-costing the remaining chain (CONCEPT:EG-KG.query.adaptive-reoptimization)
/// — a relative error threshold, so a small, ordinary estimation miss (histograms/degree
/// averages are approximations) doesn't cause needless plan churn. `0.5` ⇒ the actual must be
/// less than half or more than 1.5× the estimate.
pub const ADAPTIVE_REOPT_THRESHOLD: f64 = 0.5;

/// Re-cost and, if beneficial, RE-ORDER the not-yet-executed tail of a plan after an earlier op
/// reports its ACTUAL output cardinality (CONCEPT:EG-KG.query.adaptive-reoptimization) — the runtime
/// feedback loop beyond pure plan-time estimation: `optimize()` picks an order from the
/// [`ModalityCardinality`] estimates before ANY op has run, but a histogram/degree-average
/// estimate can be wrong, and a wrong estimate can pick the wrong order. This closes that loop
/// WITHOUT re-running the whole optimizer: given `remaining_ops` (the ops still to execute),
/// the `estimated` cardinality flowing into them (what `optimize()` assumed) and the `actual`
/// cardinality really observed, it re-applies [`reorder_chain_segments`] (plus the pairwise
/// rules) seeded from `actual` instead of `estimated` whenever they diverge by more than
/// [`ADAPTIVE_REOPT_THRESHOLD`] (relative). Below the threshold — or when `remaining_ops` has no
/// reorderable run — this is a no-op clone, so a caller can invoke it unconditionally after every
/// op without extra cost in the common (estimate was fine) case.
///
/// Opt-in-by-default-ON (no env flag): a caller decides WHEN to call it (after which op, with
/// what actual count) — this fn is pure re-costing logic, not a scheduler. Wiring it into
/// [`crate::exec::execute`]'s per-op loop so every `apply()` call reports its real `RowSet::len()`
/// and triggers this automatically is the documented follow-up (CONCEPT:EG-KG.query.adaptive-reoptimization) —
/// today a caller (or a future `Driver` impl) invokes it explicitly between ops.
pub fn reoptimize_remaining(
    remaining_ops: &[Op],
    estimated: f64,
    actual: f64,
    card: &ModalityCardinality,
    ctx: &PlanCtx,
) -> Vec<Op> {
    let mut ops = remaining_ops.to_vec();
    let denom = estimated.max(1.0);
    let rel_err = (actual - estimated).abs() / denom;
    if rel_err <= ADAPTIVE_REOPT_THRESHOLD {
        return ops; // the estimate was close enough — no churn.
    }
    // Unlike a fresh plan (where index 0 is always a real SOURCE op the reorder rules must
    // never move), `remaining_ops` has NO leading source of its own — the op that already ran
    // and produced `actual` rows isn't part of this slice — so the leading run is reorderable
    // from index 0. Find it directly and re-cost it from the CORRECTED `actual` seed via the
    // same [`best_permutation`] exhaustive search [`GlobalChainCost`] uses, capped at
    // [`MAX_CHAIN_SEGMENT`] exactly like the plan-time pass.
    let mut end = 0;
    while end < ops.len() && is_reorderable(&ops[end]) {
        end += 1;
    }
    let end = end.min(MAX_CHAIN_SEGMENT);
    if end >= 2 {
        let top_k = trailing_top_k(&ops, end - 1);
        if let Some(best) = best_permutation(&ops[..end], actual, top_k, card, ctx) {
            ops.splice(0..end, best);
        }
    }
    ops
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

    /// A GENUINELY selective NUMERIC-RANGE filter (`year > 980` over a 0..999 column) now
    /// reorders FILTER-FIRST because the optimizer reads the real column distribution from the
    /// collected [`crate::cost::ColumnStats`] histogram — while a BROAD range (`year > 20`) over
    /// the same column stays vector-first. Proves the T-E1 gap is closed: before, `GtNum` used
    /// a fixed 0.33 heuristic and NEVER pushed at this scale (asserted via `Stats::estimate`);
    /// after, the stats-derived selectivity flips the selective case to filter-first. Both
    /// reorders are result-set-preserving (the optimizer's invariant), asserted per case.
    #[test]
    fn selective_numeric_range_pushed_but_broad_range_stays() {
        use crate::cost::{CostModel, Order, PlanStats, Stats};
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        use serde_json::json;

        // 1000 `Doc` nodes with year = 0..999 (a WIDE spread so a high threshold is very
        // selective and a low one very broad) — each with an embedding so the vector `Rank`
        // has a real cost. The scale matters: at 1000 nodes the fixed-0.33 heuristic sits
        // ABOVE the filter-first/vector-first crossover, so only a stats-derived selectivity
        // can push a range filter (that is exactly the ablation's finding).
        const N: usize = 1_000;
        let core = GraphCore::new();
        let mut semantic = SemanticStore::new();
        for k in 0..N {
            let id = format!("d{k}");
            core.add_node(
                id.clone(),
                rmp_serde::to_vec_named(&json!({ "type": "Doc", "year": k as i64 })).unwrap(),
            );
            // A trivially separable embedding (all mass on one of 4 axes by k) — enough for
            // the `Rank` leg to score every node; the exact order is irrelevant to the SET.
            let mut v = vec![0.0f32; 4];
            v[k % 4] = 1.0;
            semantic.add_embedding(id, v);
        }
        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);

        // The seed size the pair's cost model reasons over: rows out of the leading `Scan`.
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));
        let seed = card.rows_out(
            &Op::Scan {
                label: "Doc".into(),
            },
            0.0,
            &ctx,
        );

        // ── the SELECTIVE range: year > 980 (≈ 1.9% of 0..999) ──────────────────────────
        let selective = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 980.0,
                }],
            },
        ]);
        // BEFORE (the fixed 0.33 heuristic): at this seed the range would NOT be pushed.
        assert_eq!(
            CostModel::order(&Stats::estimate(
                seed.round().max(1.0) as usize,
                0.33,
                crate::cost::DEFAULT_TOP_K,
                card.stats().embedding_count,
            )),
            Order::VectorFirst,
            "with the OLD fixed 0.33 range heuristic the filter stays behind the Rank"
        );
        // AFTER (stats-derived): the real selectivity is ~2%, well below the crossover.
        let sel_est = card.selectivity(&selective.ops[2], seed, &ctx);
        assert!(
            sel_est < 0.1,
            "the histogram estimates year>980 as selective, got {sel_est}"
        );
        assert_eq!(
            CostModel::order(&Stats::estimate(
                seed.round().max(1.0) as usize,
                sel_est,
                crate::cost::DEFAULT_TOP_K,
                card.stats().embedding_count,
            )),
            Order::FilterFirst,
            "stats-derived selectivity flips the selective range to filter-first"
        );
        let opt_sel = optimize(&selective, &ctx);
        assert!(
            matches!(opt_sel.ops[1], Op::Filter { .. })
                && matches!(opt_sel.ops[2], Op::Rank { .. }),
            "selective range pushed AHEAD of Rank, got {:?}",
            opt_sel.ops
        );
        assert_eq!(
            sorted_ids(&execute(&selective, &ctx).unwrap()),
            sorted_ids(&execute(&opt_sel, &ctx).unwrap()),
            "the selective-range reorder must preserve the result set"
        );

        // ── the BROAD range: year > 20 (≈ 98% of 0..999) — must NOT be pushed ───────────
        let broad = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 20.0,
                }],
            },
        ]);
        let broad_sel = card.selectivity(&broad.ops[2], seed, &ctx);
        assert!(
            broad_sel > 0.9,
            "the histogram estimates year>20 as broad, got {broad_sel}"
        );
        let opt_broad = optimize(&broad, &ctx);
        assert!(
            matches!(opt_broad.ops[1], Op::Rank { .. })
                && matches!(opt_broad.ops[2], Op::Filter { .. }),
            "broad range stays BEHIND the Rank (vector-first), got {:?}",
            opt_broad.ops
        );
        assert_eq!(
            sorted_ids(&execute(&broad, &ctx).unwrap()),
            sorted_ids(&execute(&opt_broad, &ctx).unwrap()),
            "even the untouched (broad) plan is result-equivalent"
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

    // ── Track H: global N-ary chain reorder + adaptive re-optimization ──────────────

    /// A THREE-op reorderable run `[Rank, broad_filter, selective_filter]` where the
    /// PAIRWISE rule alone is structurally stuck: it commits `(Rank, broad_filter)` first
    /// (broad → stays vector-first) and then advances PAST `selective_filter` without ever
    /// comparing it to anything — exactly the module-level gap [`GlobalChainCost`] closes.
    /// The full optimizer (which runs [`GlobalChainCost`] after the pairwise pass) finds the
    /// TRUE global minimum over all `3!` orderings and pushes the selective filter ahead of
    /// the Rank, a DIFFERENT (and cheaper) order than naive left-to-right. Result-set
    /// equivalence is asserted via the differential execute() proof, same as every other
    /// reorder test in this module.
    #[test]
    fn global_chain_reorders_three_op_segment_beyond_pairwise_reach() {
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        use serde_json::json;

        // 1000 `Doc` nodes with TWO independent wide numeric columns (mirrors
        // `selective_numeric_range_pushed_but_broad_range_stays`'s fixture) + an embedding
        // each so the `Rank` leg has real cost.
        const N: usize = 1_000;
        let core = GraphCore::new();
        let mut semantic = SemanticStore::new();
        for k in 0..N {
            let id = format!("d{k}");
            core.add_node(
                id.clone(),
                rmp_serde::to_vec_named(
                    &json!({ "type": "Doc", "year": k as i64, "score": k as i64 }),
                )
                .unwrap(),
            );
            let mut v = vec![0.0f32; 4];
            v[k % 4] = 1.0;
            semantic.add_embedding(id, v);
        }
        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        // `year > 20` is BROAD (~98% pass); `score > 980` is SELECTIVE (~2% pass) — the same
        // column-histogram-driven pair the range-pushdown ablation uses, just on two
        // independent columns so both can sit in ONE reorderable run.
        let broad_filter = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 20.0,
            }],
        };
        let selective_filter = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "score".into(),
                n: 980.0,
            }],
        };
        let rank = Op::Rank {
            query: crate::fixture::query_vec(),
        };

        // Naive, as a caller might write it: rank first, broad filter next, the selective
        // one tacked on last.
        let naive = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            rank.clone(),
            broad_filter.clone(),
            selective_filter.clone(),
            Op::Limit { k: 20 },
        ]);

        // The OLD pairwise-only mechanism cannot reach `selective_filter` at all: it commits
        // `(Rank, broad_filter)` — broad stays vector-first, no swap — then jumps past
        // `selective_filter` (its non-overlapping `j += 2` advance never re-examines it).
        let pairwise_only = FilterAsOfBeforeRank.apply(naive.ops.clone(), &card, &ctx);
        assert_eq!(
            pairwise_only, naive.ops,
            "the pairwise rule alone cannot reach the stranded selective filter"
        );

        // The FULL optimizer (pairwise passes + GlobalChainCost) finds the true minimum over
        // all 3! orderings of the run and pushes the selective filter ahead of the Rank.
        let opt = optimize(&naive, &ctx);
        let rank_pos = opt.ops.iter().position(|o| *o == rank).unwrap();
        let sel_pos = opt.ops.iter().position(|o| *o == selective_filter).unwrap();
        assert!(
            sel_pos < rank_pos,
            "the selective filter must be pushed ahead of Rank, got {:?}",
            opt.ops
        );
        assert_ne!(
            opt.ops, naive.ops,
            "GlobalChainCost must find a DIFFERENT order than naive left-to-right"
        );

        // Result-set equivalence: the SAME differential proof every other reorder test uses.
        let sorted_ids = |rs: &RowSet| {
            let mut v = rs.ids();
            v.sort();
            v
        };
        assert_eq!(
            sorted_ids(&execute(&naive, &ctx).unwrap()),
            sorted_ids(&execute(&opt, &ctx).unwrap()),
            "the 3-op global reorder must preserve the result set"
        );
    }

    /// The cross-modal case: `[Reason, Filter, Rank]` (an OWL confidence-filter, a relational
    /// predicate, and a vector rerank all drawing off the SAME candidate set). `optimize()`'s
    /// chosen order for the run must match the SAME cost-minimal permutation
    /// [`best_permutation`] computes directly — the white-box oracle this suite already uses
    /// for the numeric-range case — and that minimum must be a genuinely DIFFERENT (cheaper)
    /// order than the naive `[Reason, Filter, Rank]` a caller who reasons-then-filters would
    /// write. Proven result-preserving via the mid-pipeline `Reason` pattern
    /// [`crate::tsdb_scan_tests::reason_mid_pipeline`] establishes.
    #[cfg(feature = "owl")]
    #[test]
    fn global_chain_reorders_cross_modal_reason_filter_rank() {
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        use serde_json::json;

        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .
ex:p1 a ex:Paper .
ex:p2 a ex:Article .
ex:p3 a ex:Topic .
ex:p4 a ex:Paper .
"#;
        let core = GraphCore::new();
        let mut iris = eg_rdf::mapping::IriStore::default();
        eg_rdf::mapping::load_triples(
            &core,
            &mut iris,
            "g",
            eg_rdf::mapping::parse_turtle(ttl).unwrap(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();

        let blob = |v: serde_json::Value| rmp_serde::to_vec_named(&v).unwrap();
        // The 4 OWL individuals ALSO carry a `type`/`lang` property blob (the LPG property
        // store the DataFusion `Filter` leg reads is separate from the RDF quads the
        // reasoner reads, so this does not disturb the OWL classification below). p4 is
        // the ONLY `lang: fr` — a real, meaningful `Filter` split.
        for (p, lang) in [("p1", "en"), ("p2", "en"), ("p3", "en"), ("p4", "fr")] {
            core.add_node(
                format!("<http://example.org/{p}>"),
                blob(json!({ "type": "Doc", "lang": lang })),
            );
        }
        // Filler `Doc` nodes (no OWL membership) so the plan-time Scan estimate is a
        // realistic size — real execution still excludes them (`Reason` drops every
        // non-member regardless of position), so result-set equivalence is unaffected.
        for k in 0..200 {
            core.add_node(
                format!("filler{k}"),
                blob(json!({ "type": "Doc", "lang": if k % 2 == 0 { "en" } else { "fr" } })),
            );
        }

        let mut semantic = SemanticStore::new();
        semantic.add_embedding("<http://example.org/p1>".into(), vec![0.90, 0.44, 0.0, 0.0]);
        semantic.add_embedding("<http://example.org/p2>".into(), vec![0.99, 0.10, 0.0, 0.0]);
        semantic.add_embedding("<http://example.org/p3>".into(), vec![1.00, 0.00, 0.0, 0.0]);
        semantic.add_embedding("<http://example.org/p4>".into(), vec![0.20, 0.97, 0.0, 0.0]);

        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        let reason = Op::Reason {
            target_class: "<http://example.org/ScholarlyWork>".into(),
            ontology: String::new(),
        };
        let filter = Op::Filter {
            preds: vec![Pred::Eq {
                prop: "lang".into(),
                value: "en".into(),
            }],
        };
        let rank = Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        };
        let naive = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            reason.clone(),
            filter.clone(),
            rank.clone(),
        ]);

        // The white-box oracle: the SAME cost-minimal permutation search `optimize()` drives.
        let seed = card_before(&naive.ops, 1, &card, &ctx);
        let top_k = trailing_top_k(&naive.ops, 3);
        let expected = best_permutation(&naive.ops[1..4], seed, top_k, &card, &ctx)
            .unwrap_or_else(|| naive.ops[1..4].to_vec());
        assert_ne!(
            expected,
            naive.ops[1..4],
            "the cost-minimal cross-modal order must differ from naive Reason-then-Filter"
        );

        let opt = optimize(&naive, &ctx);
        assert_eq!(
            &opt.ops[1..4],
            expected.as_slice(),
            "optimize() must pick the SAME cross-modal order the cost model computes, got {:?}",
            opt.ops
        );

        // Result-set equivalence: p3 (Topic, not ScholarlyWork) and p4 (fr) are excluded
        // either way; p1/p2 survive, ranked p2 then p1.
        let sorted_ids = |rs: &RowSet| {
            let mut v = rs.ids();
            v.sort();
            v
        };
        assert_eq!(
            sorted_ids(&execute(&naive, &ctx).unwrap()),
            sorted_ids(&execute(&opt, &ctx).unwrap()),
            "the cross-modal reorder must preserve the result set"
        );
    }

    /// The adaptive hook: given a corrected ACTUAL cardinality wildly different from what was
    /// estimated, [`reoptimize_remaining`] re-costs the remaining chain and flips the order —
    /// the runtime-feedback half of Track H. `Eq` selectivity is a FIXED 0.1 regardless of
    /// scale, so the crossover lever here is pure SEED SIZE: `filter_first_cost` grows
    /// linearly with the input while `vector_first_cost`'s over-fetch is capped at
    /// `top_k/selectivity` (independent of the seed once the seed exceeds it) — so a small
    /// seed favors filter-first and a huge one favors vector-first, a real, derivable flip
    /// (not a hardcoded answer).
    #[test]
    fn adaptive_reopt_flips_order_on_a_wildly_corrected_cardinality() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        let filter = Op::Filter {
            preds: vec![Pred::Eq {
                prop: "type".into(),
                value: "Doc".into(),
            }],
        };
        let rank = Op::Rank {
            query: crate::fixture::query_vec(),
        };

        // What the plan-time optimizer picked at the (small) ESTIMATED seed: filter-first.
        let remaining = vec![filter.clone(), rank.clone()];

        // Below the divergence threshold — no reorder, even though a reorder might in
        // principle help; the whole point is bounded, needless-churn-free re-costing.
        let unchanged = reoptimize_remaining(&remaining, 100.0, 120.0, &card, &ctx);
        assert_eq!(
            unchanged, remaining,
            "a mere 20% miss is within ADAPTIVE_REOPT_THRESHOLD — no churn"
        );

        // A WILDLY corrected actual (the upstream op emitted 50,000x more rows than assumed)
        // blows past the threshold and flips the regime to vector-first.
        let flipped = reoptimize_remaining(&remaining, 100.0, 5_000_000.0, &card, &ctx);
        assert_ne!(
            flipped, remaining,
            "a wildly corrected actual cardinality must re-cost and change the order"
        );
        assert_eq!(
            flipped[0], rank,
            "at this corrected scale vector-first is cheaper, so Rank must lead, got {:?}",
            flipped
        );
    }

    // ── DAG-aware optimizer (CONCEPT:EG-KG.query.dag-optimizer, E5 phase 3) ──────

    use crate::dag::{PlanDag, PlanNode};

    /// `optimize_dag` over a DEGENERATE chain dag (every `From<Plan>` conversion) is
    /// IDENTICAL to `optimize` over the equivalent linear `Plan` — the zero-behavior-change
    /// half of the DAG-optimizer narrowing (a chain dag has exactly one segment spanning the
    /// whole plan, so nothing is narrowed away relative to today).
    #[test]
    fn dag_optimize_matches_linear_optimize_for_a_chain() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let plan = Plan::new(vec![
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

        let linear_opt = optimize(&plan, &ctx);

        let dag = PlanDag::from(plan);
        let dag_opt = optimize_dag(&dag, &ctx);
        let recovered = dag_opt
            .as_linear_plan()
            .expect("optimize_dag must not change the dag's edges — still a linear chain");

        assert_eq!(
            recovered.ops, linear_opt.ops,
            "optimize_dag over a chain must reorder IDENTICALLY to optimize over the Plan"
        );
    }

    /// THE EG-405-AS-DAG-NARROWING PROOF: two INDEPENDENT chain segments (one with a
    /// selective range filter that MUST reorder ahead of its Rank, one with a broad range
    /// filter that MUST stay put) feed a join node. `optimize_dag` must reorder EACH segment
    /// on its OWN merits — proving no rewrite crosses the branch boundary between them (the
    /// selective segment's cardinality context never leaks into the broad one or vice versa),
    /// and the join node's own op is never touched.
    #[test]
    fn optimizer_never_reorders_across_a_branch_boundary() {
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        use serde_json::json;

        // The SAME 1000-node/0..999-year fixture the linear ablation test above uses, so
        // `year > 980` is genuinely selective (~2%) and `year > 20` genuinely broad (~98%).
        const N: usize = 1_000;
        let core = GraphCore::new();
        let mut semantic = SemanticStore::new();
        for k in 0..N {
            let id = format!("d{k}");
            core.add_node(
                id.clone(),
                rmp_serde::to_vec_named(&json!({ "type": "Doc", "year": k as i64 })).unwrap(),
            );
            let mut v = vec![0.0f32; 4];
            v[k % 4] = 1.0;
            semantic.add_embedding(id, v);
        }
        let view = core.analysis_snapshot();
        let ctx = PlanCtx::new(&view, &semantic);

        let scan_doc = || Op::Scan {
            label: "Doc".into(),
        };
        let rank = || Op::Rank {
            query: crate::fixture::query_vec(),
        };
        let selective_filter = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 980.0,
            }],
        };
        let broad_filter = Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 20.0,
            }],
        };

        // Segment A: [Scan, Rank, Filter{selective}] — ids 0,1,2.
        // Segment B: [Scan, Rank, Filter{broad}]     — ids 3,4,5.
        // Join:      Limit joining both branch outputs — id 6.
        let dag = PlanDag::new(vec![
            PlanNode::new(scan_doc(), vec![]),
            PlanNode::new(rank(), vec![0]),
            PlanNode::new(selective_filter.clone(), vec![1]),
            PlanNode::new(scan_doc(), vec![]),
            PlanNode::new(rank(), vec![3]),
            PlanNode::new(broad_filter.clone(), vec![4]),
            PlanNode::new(Op::Limit { k: 5 }, vec![2, 5]),
        ]);

        let opt = optimize_dag(&dag, &ctx);

        // Segment A reordered filter-first (the selective range wins ahead of Rank).
        assert!(
            matches!(opt.nodes[0].op, Op::Scan { .. }),
            "segment A's source position never moves"
        );
        assert_eq!(
            opt.nodes[1].op, selective_filter,
            "segment A: the selective filter must be pushed AHEAD of Rank, got {:?}",
            opt.nodes[1].op
        );
        assert!(
            matches!(opt.nodes[2].op, Op::Rank { .. }),
            "segment A: Rank pushed behind the selective filter, got {:?}",
            opt.nodes[2].op
        );

        // Segment B is UNCHANGED (the broad filter stays vector-first) — proving segment A's
        // reorder decision never leaked across the join boundary into segment B.
        assert!(
            matches!(opt.nodes[3].op, Op::Scan { .. }),
            "segment B's source position never moves"
        );
        assert!(
            matches!(opt.nodes[4].op, Op::Rank { .. }),
            "segment B: broad filter stays BEHIND Rank (vector-first), got {:?}",
            opt.nodes[4].op
        );
        assert_eq!(
            opt.nodes[5].op, broad_filter,
            "segment B: the broad filter must stay untouched, got {:?}",
            opt.nodes[5].op
        );

        // The join node's own op and edges are completely untouched.
        assert_eq!(opt.nodes[6].op, Op::Limit { k: 5 });
        assert_eq!(opt.nodes[6].inputs, vec![2, 5]);

        // `chain_segments` partitions exactly as designed: two 3-node chains + one singleton
        // join — the structural proof the narrowing never merges across the branch.
        let segments = chain_segments(&dag);
        let mut sizes: Vec<usize> = segments.iter().map(|s| s.len()).collect();
        sizes.sort_unstable();
        assert_eq!(
            sizes,
            vec![1, 3, 3],
            "expected two independent 3-op chains plus the singleton join, got {segments:?}"
        );
    }
}
