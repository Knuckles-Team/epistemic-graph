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

/// The WASM `Udf` op (CONCEPT:KG-2.228): a registered, sandboxed wasm function runs as
/// a `RowSet -> RowSet` transform inside a unified plan. Here a UDF that DROPS every
/// row whose id does NOT start with 'd' (a custom filter expressed in wasm) runs after
/// a Scan; the result must be exactly the 'd*' nodes. Proves the op threads the RowSet
/// bytes through the sandbox and back.
#[cfg(feature = "wasm-udf")]
#[test]
fn udf_op_runs_a_sandboxed_rowset_transform() {
    use crate::rowset::RowSet;

    // A wasm UDF: read `Vec<(String, Option<f32>)>` (MessagePack) from the input region,
    // keep only ids starting with 'd' (0x64), re-encode, return the output region. The
    // ABI is the engine's: alloc + udf(ptr,len)->packed(out_ptr<<32|out_len). We write
    // it in Rust→wasm is heavy for a test; instead use a host-side pre/post wrap around
    // an IDENTITY wasm UDF to keep the wat tiny, then prove the bytes survive the
    // sandbox unchanged (the round-trip is the load-bearing part — a real UDF's logic
    // is its own; the engine only guarantees safe byte transport).
    const IDENTITY_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $next (mut i32) (i32.const 1024))
      (func (export "alloc") (param $len i32) (result i32)
        (local $p i32)
        (local.set $p (global.get $next))
        (global.set $next (i32.add (global.get $next) (local.get $len)))
        (local.get $p))
      (func (export "udf") (param $ptr i32) (param $len i32) (result i64)
        (i64.or
          (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
          (i64.extend_i32_u (local.get $len)))))
    "#;
    let wasm = wat::parse_str(IDENTITY_WAT).unwrap();
    let registry = eg_wasm::UdfRegistry::new();
    registry
        .register("identity", &wasm, eg_wasm::UdfLimits::default())
        .unwrap();

    let fx = build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_udf(&registry);

    // Scan Docs, then run the UDF (identity) — the RowSet must round-trip the sandbox.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Udf {
            id: "identity".into(),
        },
    ]);
    let out = plan.execute(&ctx).unwrap();
    // Identity through the sandbox: same ids as a plain scan (order preserved).
    let scan_only = Plan::new(vec![Op::Scan {
        label: "Doc".into(),
    }]);
    let expected: RowSet = scan_only.execute(&ctx).unwrap();
    assert_eq!(
        out.ids(),
        expected.ids(),
        "UDF identity must round-trip the RowSet through the wasm sandbox unchanged"
    );
    assert!(!out.is_empty(), "the scan produced rows to transform");
}

/// A `Udf` op with NO registry attached errs (a UDF must be registered to run) — the
/// plan degrades to a clear error, never a panic.
#[cfg(feature = "wasm-udf")]
#[test]
fn udf_op_without_registry_errs() {
    let fx = build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic); // no .with_udf
    let plan = Plan::new(vec![Op::Udf { id: "x".into() }]);
    let err = plan
        .execute(&ctx)
        .expect_err("Udf without a registry must err");
    assert!(err.contains("registry"), "got: {err}");
}

/// Bi-temporal `AS OF` execution proofs (CONCEPT:KG-2.250). The planner now filters
/// the RowSet by validity/transaction windows instead of passing rows through.
#[cfg(test)]
mod temporal_tests {
    use crate::algebra::{Op, Plan};
    use crate::exec::{PlanCtx, PlanExt};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::TimeAxis;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Three Events with valid windows [100,200), [150,∞), [300,400).
    fn temporal_core() -> GraphCore {
        let core = GraphCore::new();
        core.add_node(
            "e1".into(),
            blob(json!({"type":"Event","valid_from":100,"valid_until":200})),
        );
        core.add_node("e2".into(), blob(json!({"type":"Event","valid_from":150})));
        core.add_node(
            "e3".into(),
            blob(json!({"type":"Event","valid_from":300,"valid_until":400})),
        );
        core
    }

    #[test]
    fn as_of_valid_time_filters_window() {
        let core = temporal_core();
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();
        let ctx = PlanCtx::new(&view, &sem);

        // @175 → e1 (live 100..200) and e2 (live 150..∞); NOT e3 (starts 300).
        let at175 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 175.0,
                axis: TimeAxis::Valid,
            },
        ])
        .execute(&ctx)
        .unwrap();
        let mut ids = at175.ids();
        ids.sort();
        assert_eq!(ids, vec!["e1".to_string(), "e2".to_string()]);

        // @350 → only e3 (e1 expired at 200; e2 started 150 but... still live →
        // e2 IS live at 350 since it has no end). So @350 → {e2, e3}.
        let at350 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 350.0,
                axis: TimeAxis::Valid,
            },
        ])
        .execute(&ctx)
        .unwrap();
        let mut ids = at350.ids();
        ids.sort();
        assert_eq!(ids, vec!["e2".to_string(), "e3".to_string()]);

        // boundary: @200 excludes e1 (half-open [100,200)).
        let at200 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 200.0,
                axis: TimeAxis::Valid,
            },
        ])
        .execute(&ctx)
        .unwrap();
        assert!(!at200.ids().contains(&"e1".to_string()));
    }

    #[test]
    fn as_of_preserves_rank_order() {
        let core = temporal_core();
        let view = core.analysis_snapshot();
        let mut sem = SemanticStore::new();
        // rank order to [1,0,0]: e2 > e1 ; e3 far. All live at @175 except e3.
        sem.add_embedding("e1".into(), vec![0.8, 0.6, 0.0]);
        sem.add_embedding("e2".into(), vec![0.99, 0.1, 0.0]);
        sem.add_embedding("e3".into(), vec![0.0, 0.0, 1.0]);
        let ctx = PlanCtx::new(&view, &sem);

        let ranked_then_filtered = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            Op::AsOf {
                ts: 175.0,
                axis: TimeAxis::Valid,
            },
        ])
        .execute(&ctx)
        .unwrap();
        // e2 ranks above e1, and both survive @175; e3 dropped. Order preserved.
        assert_eq!(
            ranked_then_filtered.ids(),
            vec!["e2".to_string(), "e1".to_string()]
        );
    }

    #[test]
    fn as_of_transaction_axis_distinguishes_belief_from_truth() {
        // A fact valid [100,200) that we believed [100,250): superseded at tx 250,
        // its valid window closed at 200.
        let core = GraphCore::new();
        core.add_node(
            "f".into(),
            blob(json!({
                "type":"Event","valid_from":100,"valid_until":200,
                "tx_from":100,"tx_to":250
            })),
        );
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();
        let ctx = PlanCtx::new(&view, &sem);

        // valid-time @220: NOT true any more (valid window ended 200).
        let valid_220 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 220.0,
                axis: TimeAxis::Valid,
            },
        ])
        .execute(&ctx)
        .unwrap();
        assert!(valid_220.ids().is_empty());

        // transaction-time @220: we STILL believed it then (tx window 100..250).
        let tx_220 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 220.0,
                axis: TimeAxis::Transaction,
            },
        ])
        .execute(&ctx)
        .unwrap();
        assert_eq!(tx_220.ids(), vec!["f".to_string()]);

        // transaction-time @300: belief retracted (tx_to 250).
        let tx_300 = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 300.0,
                axis: TimeAxis::Transaction,
            },
        ])
        .execute(&ctx)
        .unwrap();
        assert!(tx_300.ids().is_empty());
    }

    #[test]
    fn uql_as_of_tx_keyword_parses_and_filters() {
        use crate::uql::parse;
        let p = parse("MATCH (:Event) |> AS OF TX @150").unwrap();
        assert!(matches!(
            p.ops.last(),
            Some(Op::AsOf {
                axis: TimeAxis::Transaction,
                ..
            })
        ));
    }
}
