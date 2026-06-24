//! The planner's runnable proofs (`cargo test -p eg-plan --features query`).

use crate::algebra::{Op, Plan, Pred};
use crate::cost::{CostModel, Order, Stats};
use crate::exec::{PlanCtx, PlanExt};
use crate::fixture::{build, query_vec};
use crate::oracle::separate_surfaces;

/// THE oracle proof (non-negotiable): the fused single-plan execution returns the
/// BYTE-IDENTICAL ordered id list as the three siloed surfaces done separately.
///
/// Query: "start from Doc nodes WHERE year > 2024 (relational filter) → traverse
/// -[:CITES]->{1,2} (graph) → rank reached by similarity to query (vector) → top-k".
///
/// Seed Docs with year>2024: {d1, d2, d5} (d5 year 2025; t1 is a Tool, dropped; old
///   is 2020). CITES 1..2 hops from {d1,d2,d5}:
///     d1->d2->d3 ; d1->d4 ; d2->d3   (d2->d5 is MENTIONS, excluded)
///     reached = {d2, d4, d3}
///   rank by sim to [1,0,0,0]: d2 > d4 > d3  =>  [d2, d4, d3]
#[test]
fn fused_plan_matches_separate_surfaces() {
    let fx = build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let preds = vec![Pred::GtNum {
        prop: "year".into(),
        n: 2024.0,
    }];

    // ── fused single pipeline ──
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: preds.clone(),
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 2,
        },
        Op::Rank { query: query_vec() },
        Op::Limit { k: 10 },
    ]);
    let fused = plan.execute(&ctx).unwrap();

    // ── the same query, three siloed surfaces wired by the caller (the oracle) ──
    let separate = separate_surfaces(
        &fx.view,
        &fx.semantic,
        "Doc",
        &preds,
        "CITES",
        1,
        2,
        &query_vec(),
        10,
    )
    .unwrap();

    let fused_ids = fused.ids();
    let separate_ids = separate.ids();
    assert_eq!(
        fused_ids, separate_ids,
        "fused pipeline must equal separate-surfaces (oracle)"
    );
    assert_eq!(fused_ids, vec!["d2", "d4", "d3"], "expected ranked order");
    // Scores are attached (the vector leg ran) and descending.
    let scores: Vec<f32> = fused.rows().iter().map(|r| r.score.unwrap()).collect();
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "ranked descending: {scores:?}"
    );
}

/// The cost-reorder produces the SAME result set whichever order it picks — proving
/// `Filter` and `Rank` commute across the modality boundary, so the reorder is a
/// pure cost optimization and never changes the answer. Both a selective and a broad
/// regime (which the cost model orders oppositely) must yield the identical set.
#[test]
fn reorder_preserves_result_set_both_regimes() {
    let fx = build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);

    // Candidate set from Scan+Filter+Traverse = {d2,d4,d3}; the commuting pair is the
    // tail (Filter year>2022, Rank). Both end with a rank-or-filter over the same set.
    let base = vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2024.0,
            }],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 2,
        },
    ];
    let tail_filter = Op::Filter {
        preds: vec![Pred::GtNum {
            prop: "year".into(),
            n: 2022.0,
        }],
    };
    let tail_rank = Op::Rank { query: query_vec() };

    // filter-first tail vs vector-first tail (the two commuting orders).
    let mut ff = base.clone();
    ff.push(tail_filter.clone());
    ff.push(tail_rank.clone());
    let mut vf = base;
    vf.push(tail_rank);
    vf.push(tail_filter);

    let ff_res = Plan::new(ff).execute(&ctx).unwrap();
    let vf_res = Plan::new(vf).execute(&ctx).unwrap();

    let mut a = ff_res.ids();
    let mut b = vf_res.ids();
    a.sort();
    b.sort();
    assert_eq!(
        a, b,
        "filter ∧ rank commute as sets across the modality boundary"
    );
}

/// The cost model picks filter-first for a selective predicate and vector-first for a
/// broad one, and `reorder_filter_rank` rewrites a real plan to the winner — over the
/// fixture, both resulting plans still produce the SAME result set.
#[test]
fn cost_reorder_picks_winner_same_result() {
    let fx = build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);

    // A Scan'd seed feeding a commuting (Filter, Rank) pair.
    let plan = vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2022.0,
            }],
        },
        Op::Rank { query: query_vec() },
    ];

    // Drive both regimes via Stats::estimate (derived, not hand-set magic numbers).
    let selective = Stats::estimate(10_000, 0.01, 10, fx.semantic.len());
    let broad = Stats::estimate(10_000, 0.98, 10, fx.semantic.len());
    assert_eq!(CostModel::order(&selective), Order::FilterFirst);
    assert_eq!(CostModel::order(&broad), Order::VectorFirst);

    // The (Filter, Rank) pair is at indices 1,2 (adjacent) — the reorder swaps them.
    let sel_plan = CostModel::reorder_filter_rank(plan.clone(), &selective);
    let broad_plan = CostModel::reorder_filter_rank(plan, &broad);
    assert!(
        matches!(sel_plan[1], Op::Filter { .. }) && matches!(sel_plan[2], Op::Rank { .. }),
        "selective → filter-first"
    );
    assert!(
        matches!(broad_plan[1], Op::Rank { .. }) && matches!(broad_plan[2], Op::Filter { .. }),
        "broad → vector-first"
    );

    // Both reordered plans yield the same result SET (the reorder is cost-only).
    let mut a = Plan::new(sel_plan).execute(&ctx).unwrap().ids();
    let mut b = Plan::new(broad_plan).execute(&ctx).unwrap().ids();
    a.sort();
    b.sort();
    assert_eq!(a, b, "cost reorder must not change the result set");
}
