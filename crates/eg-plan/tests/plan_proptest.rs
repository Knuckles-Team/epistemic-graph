//! Property-based query-surface proofs (CONCEPT:EG-KG.compute.eg-188). A constrained proptest
//! generator emits RANDOM but VALID UQL pipelines (the generator only assembles
//! grammatically legal op sequences), and we assert the invariants a unified query
//! engine must never violate:
//!
//!  * **no panic / no inconsistent state** — every generated valid UQL parses AND
//!    executes to a well-formed `RowSet` (unique ids);
//!  * **determinism / snapshot isolation** — the SAME plan over the SAME immutable
//!    snapshot yields the byte-identical `RowSet` across repeated executions (reads are
//!    a pure function of the snapshot — the read-side of snapshot isolation);
//!  * **`LIMIT` after `RANK` is a stable prefix** — `[… Rank, Limit k]` is exactly the
//!    first `k` rows of `[… Rank]`;
//!  * **a safe `FILTER` / `AS OF` commutes past the other** — both are row-set
//!    conjunctive filters, so `[Filter, AsOf]` and `[AsOf, Filter]` agree as sets.

mod common;

use common::*;
use eg_plan::{execute, Op, Plan, PlanCtx, Pred};
use proptest::prelude::*;

// ── a constrained generator of VALID UQL pipeline text ───────────────────────

fn label_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("Doc"), Just("Tool"), Just("NoSuchLabel")]
}

/// One legal pipeline stage rendered to UQL text. Every arm is grammatically valid on
/// its own, so any concatenation with `|>` is a valid pipeline.
fn stage_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // TRAVERSE with a well-formed hop range (max ≥ min, enforced by construction).
        (0usize..3, 0usize..3)
            .prop_map(|(min, d)| { format!("TRAVERSE -[:CITES]->{{{},{}}}", min, min + d) }),
        // RANK with an inline 4-component literal vector. Components are kept
        // NON-NEGATIVE: the UQL RANK-vector parser rejects a leading `-` (a real surface
        // limitation — the builder/wire accept negatives; tracked as EG-404 in
        // docs/north_star.md), so a negative here would make the generated UQL invalid.
        (0.0f64..2.0, 0.0f64..2.0, 0.0f64..2.0, 0.0f64..2.0)
            .prop_map(|(a, b, c, d)| format!("RANK BY ~[{a:.4},{b:.4},{c:.4},{d:.4}]")),
        // Graph-native rerankers.
        Just("RERANK MENTIONS".to_string()),
        Just("RERANK NODE_DISTANCE FROM d1".to_string()),
        (0.0f64..1.0, 0usize..6).prop_map(|(l, k)| format!("RERANK MMR {l:.3} {k}")),
        // A later-stage relational filter.
        (2000i64..2030).prop_map(|y| format!("WHERE year > {y}")),
        (2000i64..2030).prop_map(|y| format!("WHERE year < {y}")),
        // Time context ops.
        (0u64..2_000_000_000).prop_map(|ts| format!("AS OF @{ts}")),
        (1u64..7200).prop_map(|s| format!("WINDOW {s}")),
        // Federation name marker (pass-through).
        Just("FOREIGN \"peer\"".to_string()),
        // LIMIT.
        (0usize..20).prop_map(|k| format!("LIMIT {k}")),
    ]
}

fn pipeline_strategy() -> impl Strategy<Value = String> {
    (
        label_strategy(),
        prop::collection::vec(stage_strategy(), 0..6),
    )
        .prop_map(|(label, stages)| {
            let mut q = format!("MATCH (:{label})");
            for s in stages {
                q.push_str(" |> ");
                q.push_str(&s);
            }
            q
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Any generated VALID UQL parses, executes without panic/error, and its `RowSet`
    /// has unique ids — the closed algebra never lands in an inconsistent state.
    #[test]
    fn valid_uql_parses_executes_and_is_wellformed(q in pipeline_strategy()) {
        let (view, semantic) = build_docs();
        let plan = eg_plan::uql::parse(&q).expect("generated UQL must be valid");
        let ctx = PlanCtx::new(&view, &semantic);
        let rs = execute(&plan, &ctx).expect("a valid plan must execute without error");
        let ids = rs.ids();
        let uniq: std::collections::HashSet<&String> = ids.iter().collect();
        prop_assert_eq!(uniq.len(), ids.len(), "RowSet ids must be unique");
    }

    /// Determinism / snapshot-isolation: the same plan over the same immutable snapshot
    /// yields the byte-identical RowSet on repeated execution. Reads are a pure function
    /// of the snapshot.
    #[test]
    fn execution_is_deterministic(q in pipeline_strategy()) {
        let (view, semantic) = build_docs();
        let plan = eg_plan::uql::parse(&q).expect("valid UQL");
        let ctx = PlanCtx::new(&view, &semantic);
        let a = execute(&plan, &ctx).unwrap();
        let b = execute(&plan, &ctx).unwrap();
        prop_assert_eq!(stable_rows(&a), stable_rows(&b), "same plan → same RowSet");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// `LIMIT` after `RANK` is a stable prefix: `[Scan, Rank, Limit k]` equals the first
    /// `k` rows of `[Scan, Rank]`. The optimizer/executor must never reorder or reshuffle
    /// a ranked result under a trailing LIMIT.
    #[test]
    fn limit_after_rank_is_stable_prefix(
        k in 0usize..8,
        qa in -2.0f64..2.0, qb in -2.0f64..2.0, qc in -2.0f64..2.0, qd in -2.0f64..2.0,
    ) {
        let (view, semantic) = build_docs();
        let ctx = PlanCtx::new(&view, &semantic);
        let q = vec![qa as f32, qb as f32, qc as f32, qd as f32];

        let ranked = execute(
            &Plan::new(vec![Op::Scan { label: "Doc".into() }, Op::Rank { query: q.clone() }]),
            &ctx,
        ).unwrap();
        let limited = execute(
            &Plan::new(vec![
                Op::Scan { label: "Doc".into() },
                Op::Rank { query: q },
                Op::Limit { k },
            ]),
            &ctx,
        ).unwrap();

        let expected: Vec<String> = ranked.ids().into_iter().take(k).collect();
        prop_assert_eq!(limited.ids(), expected, "LIMIT k must be the first k ranked rows");
    }

    /// A safe `FILTER` and `AS OF` COMMUTE **in the non-emptying regime**: both are
    /// conjunctive row-set filters, so applying them in either order yields the SAME set —
    /// PROVIDED neither intermediate empties the candidate set (an empty intermediate flips
    /// the following op into SOURCE mode and breaks the law; see EG-405 in
    /// docs/north_star.md and `empty_intermediate_reseeds_source_breaks_commute` below).
    /// The bounds keep `AS OF` in `[150,200)` (e1 & e2 both live) and the level threshold
    /// below every Event's level, so BOTH orders keep `{e1,e2}` non-empty throughout.
    #[test]
    fn filter_and_asof_commute_in_nonempty_regime(ts in 150u64..200, level in 0i64..2) {
        let (view, semantic) = build_events();
        let ctx = PlanCtx::new(&view, &semantic);
        let filter = Op::Filter { preds: vec![Pred::GtNum { prop: "level".into(), n: level as f64 }] };
        let asof = Op::AsOf { ts: ts as f64, axis: Default::default() };

        let fa = execute(
            &Plan::new(vec![Op::Scan { label: "Event".into() }, filter.clone(), asof.clone()]),
            &ctx,
        ).unwrap();
        let af = execute(
            &Plan::new(vec![Op::Scan { label: "Event".into() }, asof, filter]),
            &ctx,
        ).unwrap();

        // Non-emptiness precondition of the law (also documents the regime).
        prop_assume!(!fa.is_empty() && !af.is_empty());
        prop_assert_eq!(ids_sorted(&fa), ids_sorted(&af), "FILTER and AS OF commute when non-empty");
    }
}

/// The REAL seam the oracle surfaced (EG-405): op composition is NOT freely commutative,
/// because an intermediate EMPTY RowSet flips the following `AsOf` into SOURCE mode. This
/// pins the exact non-commuting witness so a future change either preserves the documented
/// behavior or is forced to update this test + the north_star row. NOT a bug to paper
/// over — a caveat the north_star tracks for any op-reordering optimizer.
#[test]
fn empty_intermediate_reseeds_source_breaks_commute() {
    let (view, semantic) = build_events();
    let ctx = PlanCtx::new(&view, &semantic);
    // level > 9 removes ALL events (max level is 9) → the filter output is EMPTY.
    let filter = Op::Filter {
        preds: vec![Pred::GtNum {
            prop: "level".into(),
            n: 9.0,
        }],
    };
    let asof = Op::AsOf {
        ts: 100.0,
        axis: Default::default(),
    };

    // filter-first: the empty filter output makes AsOf a SOURCE → re-seeds every event
    // live at ts=100 → {e1}.
    let fa = execute(
        &Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            filter.clone(),
            asof.clone(),
        ]),
        &ctx,
    )
    .unwrap();
    // asof-first: AsOf narrows {e1,e2,e3}→{e1}; then the level>9 filter drops e1 → {}.
    let af = execute(
        &Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            asof,
            filter,
        ]),
        &ctx,
    )
    .unwrap();

    assert_eq!(
        ids_sorted(&fa),
        vec!["e1".to_string()],
        "empty filter → AsOf re-seeds as source"
    );
    assert!(
        af.is_empty(),
        "asof-first stays empty after the level>9 filter"
    );
    assert_ne!(
        ids_sorted(&fa),
        ids_sorted(&af),
        "documented non-commutativity: an empty intermediate flips AsOf into source mode (EG-405)"
    );
}
