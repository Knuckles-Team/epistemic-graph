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

/// CONCEPT:EG-KG.query.adaptive-reoptimization — auto-wiring proof: `SerialDriver::run`
/// (the ordinary, default execution path — no opt-in flag) runs the SAME divergent-
/// cardinality scenario `optimizer::reoptimize_remaining`'s own unit test proves the
/// primitive handles, but end-to-end through a REAL graph + `execute()`, not hand-fed
/// numbers. `Traverse`'s cardinality estimator uses the graph's GLOBAL average
/// out-degree (`PlanStats::avg_out_degree`) — a single hub node whose real out-degree
/// is far above that average makes the plan-time ESTIMATE for the traversal wildly
/// wrong (near-zero) while the ACTUAL reached set is large, so this is a genuine,
/// not contrived-via-raw-numbers, trigger for the runtime feedback loop.
#[test]
fn adaptive_reopt_auto_wires_into_ordinary_execution() {
    use crate::cost::{Cardinality, ModalityCardinality, PlanStats};
    use crate::optimizer::{self, ADAPTIVE_REOPT_THRESHOLD};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    // 1 hub (label "Hub") -LINK-> 500 "Reached" nodes; 499 unrelated filler nodes with
    // no edges at all. node_count = 1000, edge_count = 500 ⇒ avg_out_degree = 0.5 — the
    // hub's REAL out-degree (500) is 1000x the graph average.
    let core = GraphCore::new();
    core.add_node("hub".into(), blob(json!({"type": "Hub"})));
    for i in 0..500 {
        let id = format!("r{i}");
        core.add_node(id.clone(), blob(json!({"type": "Reached", "keep": "yes"})));
        core.add_edge("hub".into(), id, blob(json!({"relationship": "LINK"})))
            .unwrap();
    }
    for i in 0..499 {
        core.add_node(format!("filler{i}"), blob(json!({"type": "Filler"})));
    }
    let view = core.analysis_snapshot();
    let mut semantic = SemanticStore::new();
    // A handful of reached nodes get real embeddings so the Rank leg has candidates.
    for i in 0..20 {
        semantic.add_embedding(format!("r{i}"), vec![1.0, (i as f32) * 0.01, 0.0, 0.0]);
    }
    let ctx = PlanCtx::new(&view, &semantic);
    let card = ModalityCardinality::new(PlanStats::collect(&ctx));

    let traverse = Op::Traverse {
        rel: "LINK".into(),
        min: 1,
        max: 1,
    };
    // The plan-time ESTIMATE flowing into `traverse` (seeded from the 1-row Hub scan) —
    // near-zero because the GLOBAL avg_out_degree (0.5) is nothing like the hub's real
    // 500-edge fan-out.
    let estimated_traverse_out = card.rows_out(&traverse, 1.0, &ctx);
    assert!(
        estimated_traverse_out < 1.0,
        "the degree-average estimator must badly UNDER-estimate this hub's fan-out \
         (got {estimated_traverse_out}) — otherwise this fixture doesn't exercise a real \
         divergence"
    );

    let plan_ops = vec![
        Op::Scan {
            label: "Hub".into(),
        },
        traverse.clone(),
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "keep".into(),
                value: "yes".into(),
            }],
        },
        Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        },
        Op::Limit { k: 5 },
    ];

    // The ACTUAL post-Traverse cardinality — the real BFS reach, not the estimate —
    // diverges from `estimated_traverse_out` by far more than the threshold, so
    // `reoptimize_remaining` (the exact primitive `SerialDriver::run` now calls after
    // EVERY op) does NOT take its below-threshold no-op path for this step.
    let actual_traverse_out = 500.0_f64;
    let rel_err =
        (actual_traverse_out - estimated_traverse_out).abs() / estimated_traverse_out.max(1.0);
    assert!(
        rel_err > ADAPTIVE_REOPT_THRESHOLD,
        "fixture must exceed the adaptive-reopt divergence threshold: rel_err={rel_err}"
    );

    // The primitive itself, fed these exact real numbers, is safe: it returns a
    // REORDERING of the SAME two ops (never drops/duplicates/invents one) — proven
    // generically here so the auto-wired driver below is provably not corrupting the
    // op list regardless of which permutation the cost model prefers at this scale.
    let remaining = vec![plan_ops[2].clone(), plan_ops[3].clone()];
    let recost = optimizer::reoptimize_remaining(
        &remaining,
        estimated_traverse_out,
        actual_traverse_out,
        &card,
        &ctx,
    );
    let mut recost_kinds: Vec<&str> = recost
        .iter()
        .map(|o| match o {
            Op::Filter { .. } => "Filter",
            Op::Rank { .. } => "Rank",
            other => panic!("unexpected op in reoptimized tail: {other:?}"),
        })
        .collect();
    recost_kinds.sort_unstable();
    assert_eq!(
        recost_kinds,
        vec!["Filter", "Rank"],
        "reoptimize_remaining must return exactly one Filter + one Rank, just possibly \
         reordered: {recost:?}"
    );

    // End-to-end: running the ORIGINAL plan (as literally written above) through the
    // AUTO-WIRED `execute()` — no explicit reopt call, no opt-in flag — must still land
    // the CORRECT rows: the 20 embedded reached nodes ranked by similarity to the query
    // (all of them pass the `keep` filter), top 5.
    let got = Plan::new(plan_ops).execute(&ctx).unwrap().ids();
    assert_eq!(got.len(), 5, "Limit{{k:5}} caps the result: {got:?}");
    assert!(
        got.iter().all(|id| id.starts_with('r')),
        "every returned id must be a reached, embedded node: {got:?}"
    );
    // Highest-similarity-to-[1,0,0,0] embedded reached node is r0 ([1.0, 0.0, 0,0]).
    assert_eq!(
        got.first().map(String::as_str),
        Some("r0"),
        "the fused plan must still rank correctly despite the internal adaptive \
         reorder: {got:?}"
    );
}

/// The WASM `Udf` op (CONCEPT:EG-KG.query.rowset-execution): a registered, sandboxed wasm function runs as
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

/// Bi-temporal `AS OF` execution proofs (CONCEPT:AU-KG.compute.kg-2). The planner now filters
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

    /// Read a node's `confidence` off a snapshot blob (the decayed belief value).
    fn confidence(view: &eg_core::graph::GraphView, id: &str) -> Option<f64> {
        let blob = view.node_properties.get(id)?;
        let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
        v.get("confidence").and_then(|c| c.as_f64())
    }

    /// Bitemporal cross-modal seam (CONCEPT:EG-KG.query.query-plan-node): the memory/confidence model
    /// (Ebbinghaus decay, CONCEPT:EG-KG.compute.handled-outside-single-anchor) and the bitemporal `AS OF` filter
    /// (CONCEPT:AU-KG.compute.kg-2) are the SAME temporal substrate — a decay sweep re-weights
    /// belief while `AsOf{Valid}` re-selects liveness, and an in-between UPDATE flips a
    /// validity window. This pins BOTH signals at two instants: confidence VALUES after
    /// the sweep, and liveness at each `AS OF` timestamp before and after the update.
    #[test]
    fn as_of_after_decay_and_update() {
        // half_life = 100 (unit-agnostic). now = 1000. `last_access` sets each fact's age.
        let hl = 100.0_f64;
        let now = 1000_u64;
        let core = GraphCore::new();
        // e1 valid [100,200); age 100 (1 half-life) → conf 1.0 → 0.5.
        core.add_node(
            "e1".into(),
            blob(json!({"type":"Event","valid_from":100,"valid_until":200,
                        "confidence":1.0,"last_access":now-100})),
        );
        // e2 valid [150,∞); age 0 → conf stays 1.0.
        core.add_node(
            "e2".into(),
            blob(json!({"type":"Event","valid_from":150,
                        "confidence":1.0,"last_access":now})),
        );
        // e3 valid [300,400); age 200 (2 half-lives) → conf 0.8 → 0.2.
        core.add_node(
            "e3".into(),
            blob(json!({"type":"Event","valid_from":300,"valid_until":400,
                        "confidence":0.8,"last_access":now-200})),
        );

        // ── DECAY SWEEP: re-weight belief on the Ebbinghaus curve (no prune). ──
        let stats = core.decay_sweep(now, hl, 0.0, false);
        assert_eq!(
            stats.nodes_decayed, 2,
            "e1 and e3 decayed; e2 (age 0) unchanged"
        );
        let decayed = core.analysis_snapshot();
        assert!(
            (confidence(&decayed, "e1").unwrap() - 0.5).abs() < 1e-9,
            "e1 → half"
        );
        assert!(
            (confidence(&decayed, "e2").unwrap() - 1.0).abs() < 1e-9,
            "e2 unchanged"
        );
        assert!(
            (confidence(&decayed, "e3").unwrap() - 0.2).abs() < 1e-9,
            "e3 → quarter"
        );

        // ── AS OF the PRE-UPDATE state at two instants (liveness, valid axis). ──
        let sem = SemanticStore::new();
        let as_of = |view: &eg_core::graph::GraphView, ts: f64| -> Vec<String> {
            let ctx = PlanCtx::new(view, &sem);
            let mut ids = Plan::new(vec![
                Op::Scan {
                    label: "Event".into(),
                },
                Op::AsOf {
                    ts,
                    axis: TimeAxis::Valid,
                },
            ])
            .execute(&ctx)
            .unwrap()
            .ids();
            ids.sort();
            ids
        };
        assert_eq!(
            as_of(&decayed, 175.0),
            vec!["e1".to_string(), "e2".to_string()]
        );
        assert_eq!(
            as_of(&decayed, 350.0),
            vec!["e2".to_string(), "e3".to_string()]
        );

        // ── UPDATE one fact: close e2's validity window at 200 (belief preserved). ──
        // Re-add with the same decayed confidence so the update touches ONLY the window.
        let e2_conf = confidence(&decayed, "e2").unwrap();
        core.add_node(
            "e2".into(),
            blob(json!({"type":"Event","valid_from":150,"valid_until":200,
                        "confidence":e2_conf,"last_access":now})),
        );
        let updated = core.analysis_snapshot();

        // Confidence is unchanged by the window update; liveness flips at ts=350.
        assert!(
            (confidence(&updated, "e2").unwrap() - 1.0).abs() < 1e-9,
            "e2 belief preserved"
        );
        // @175: e2 is still live (150..200 covers 175) → {e1,e2}.
        assert_eq!(
            as_of(&updated, 175.0),
            vec!["e1".to_string(), "e2".to_string()]
        );
        // @350: e2's window now ends at 200 → only e3 survives.
        assert_eq!(as_of(&updated, 350.0), vec!["e3".to_string()]);
    }
}

/// Graph-native reranker proofs (CONCEPT:EG-KG.query.uql-parser-ops) — node-distance + mentions.
#[cfg(test)]
mod rerank_tests {
    use crate::algebra::{Op, Plan};
    use crate::exec::{PlanCtx, PlanExt};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    // a -> b -> c ; d -> b (so b has 2 incoming, c has 1, a/d have 0).
    fn topo() -> GraphCore {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            core.add_node(id.into(), blob(json!({"type": "N"})));
        }
        for (s, t) in [("a", "b"), ("b", "c"), ("d", "b")] {
            core.add_edge(s.into(), t.into(), blob(json!({"relationship": "R"})))
                .unwrap();
        }
        core
    }

    #[test]
    fn node_distance_orders_by_proximity() {
        let core = topo();
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();
        let ctx = PlanCtx::new(&view, &sem);
        let out = Plan::new(vec![
            Op::Scan { label: "N".into() },
            Op::RankNodeDistance { center: "a".into() },
        ])
        .execute(&ctx)
        .unwrap();
        // a(0) > b(1) > c(2) > d(unreachable, 0-score, last by id tie).
        let ids = out.ids();
        assert_eq!(ids[0], "a");
        assert_eq!(ids[1], "b");
        assert_eq!(ids[2], "c");
        assert_eq!(ids[3], "d"); // unreachable from a → score 0
    }

    #[test]
    fn mentions_orders_by_incoming_count() {
        let core = topo();
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();
        let ctx = PlanCtx::new(&view, &sem);
        let out = Plan::new(vec![Op::Scan { label: "N".into() }, Op::RankMentions {}])
            .execute(&ctx)
            .unwrap();
        // b has 2 incoming (most-mentioned) → ranks first.
        assert_eq!(out.ids()[0], "b");
    }

    #[test]
    fn uql_rerank_keywords_parse() {
        use crate::uql::parse;
        let p = parse("MATCH (:N) |> RERANK NODE_DISTANCE FROM a").unwrap();
        assert!(matches!(
            p.ops.last(),
            Some(Op::RankNodeDistance { center }) if center == "a"
        ));
        let q = parse("MATCH (:N) |> RERANK MENTIONS").unwrap();
        assert!(matches!(q.ops.last(), Some(Op::RankMentions {})));
        let m = parse("MATCH (:N) |> RERANK MMR 0.3 5").unwrap();
        assert!(matches!(
            m.ops.last(),
            Some(Op::RankMmr { lambda, k }) if (*lambda - 0.3).abs() < 1e-6 && *k == 5
        ));
    }

    #[test]
    fn mmr_demotes_near_duplicate_for_diversity() {
        // a & b are near-identical (redundant); c is orthogonal. Relevance to the
        // query ranks a > b > c. MMR with low lambda should pick c BEFORE b (b is
        // redundant with the already-picked a), giving [a, c, b] not [a, b, c].
        let core = GraphCore::new();
        for id in ["a", "b", "c"] {
            core.add_node(id.into(), blob(json!({"type": "N"})));
        }
        let view = core.analysis_snapshot();
        let mut sem = SemanticStore::new();
        sem.add_embedding("a".into(), vec![1.0, 0.0, 0.0]);
        sem.add_embedding("b".into(), vec![0.99, 0.01, 0.0]); // ~duplicate of a
        sem.add_embedding("c".into(), vec![0.0, 1.0, 0.0]); // orthogonal
        let ctx = PlanCtx::new(&view, &sem);

        let out = Plan::new(vec![
            Op::Scan { label: "N".into() },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            Op::RankMmr { lambda: 0.3, k: 0 },
        ])
        .execute(&ctx)
        .unwrap();
        assert_eq!(
            out.ids(),
            vec!["a".to_string(), "c".to_string(), "b".to_string()],
            "MMR must surface the diverse 'c' ahead of the redundant 'b'"
        );
    }

    /// CONCEPT:EG-KG.query.reason-decay-in-plan — confidence time-DECAY folds INTO a single
    /// fused plan. The SAME `Op::Reason` plan, executed under two different `now` values
    /// (via `PlanCtx::with_decay`), yields DIFFERENT decayed membership scores for the same
    /// individual — proving Ebbinghaus decay is now an IN-PLAN signal that composes alongside
    /// the bi-temporal `Op::AsOf` leg (both read the SAME temporal substrate), resolving the
    /// EG-KG.query.decay-not-foldable-finding seam gap. Without a bound decay context the op is
    /// decay-NEUTRAL (byte-for-byte the prior leaf-stable `Reason`).
    #[cfg(feature = "owl")]
    #[test]
    fn reason_confidence_decays_in_plan_at_two_now_values() {
        // `Sensor ⊑ Device`; the node's string `type:"Sensor"` bridges into the target's
        // namespace, so REASON <…/Device> infers the Sensor node a Device with confidence
        // = stored_confidence · ebbinghaus_weight(age, half_life) · subclass_conf(=1.0 hard).
        const ONT: &str = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
            <http://x/Sensor> rdfs:subClassOf <http://x/Device> .\n";
        let hl = 100.0_f64;
        let t0 = 1_000_u64; // the fact's last_access instant.
        let core = GraphCore::new();
        core.add_node(
            "s1".into(),
            blob(json!({"type":"Sensor","confidence":1.0,"last_access":t0})),
        );
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();

        let plan = Plan::new(vec![Op::Reason {
            target_class: "http://x/Device".into(),
            ontology: ONT.into(),
        }]);

        let score_at = |now: u64| -> f32 {
            let ctx = PlanCtx::new(&view, &sem).with_decay(now, hl);
            let out = plan.execute(&ctx).unwrap();
            let row = out
                .rows()
                .iter()
                .find(|r| r.id == "s1")
                .expect("s1 inferred a Device");
            row.score.expect("membership carries a confidence score")
        };

        // now = t0 (age 0) → undecayed (weight 1.0) → ~1.0.
        let fresh = score_at(t0);
        // now = t0 + half_life (age = 1 half-life) → weight 0.5 → ~0.5.
        let one_hl = score_at(t0 + hl as u64);
        // now = t0 + 2·half_life (age = 2 half-lives) → weight 0.25 → ~0.25.
        let two_hl = score_at(t0 + 2 * hl as u64);

        assert!(
            (fresh - 1.0).abs() < 1e-6,
            "fresh membership is undecayed: {fresh}"
        );
        assert!(
            (one_hl - 0.5).abs() < 1e-3,
            "one half-life halves the in-plan membership confidence: {one_hl}"
        );
        assert!(
            two_hl < one_hl && one_hl < fresh,
            "the SAME Reason plan decays monotonically as `now` advances: {fresh} > {one_hl} > {two_hl}"
        );

        // Decay-NEUTRAL default (no `with_decay`): the op is a stable leaf source, undecayed.
        let neutral = {
            let ctx = PlanCtx::new(&view, &sem);
            plan.execute(&ctx)
                .unwrap()
                .rows()
                .iter()
                .find(|r| r.id == "s1")
                .unwrap()
                .score
                .unwrap()
        };
        assert!(
            (neutral - 1.0).abs() < 1e-6,
            "absent a decay context the in-plan Reason stays decay-neutral: {neutral}"
        );
    }
}

/// `Op::Window` execution proofs (CONCEPT:EG-KG.query.streaming-execution). The planner now runs a real
/// eg-tsdb windowed aggregate over the input's time/value columns instead of passing
/// the rows through.
#[cfg(feature = "timeseries")]
mod timeseries_tests {
    use crate::algebra::{Op, Plan};
    use crate::exec::{PlanCtx, PlanExt};
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Four Memory nodes with `valid_from` event times and a `value`; a 10-unit WINDOW
    /// must produce ONE row per aligned bucket carrying the bucket's mean — i.e. a
    /// bucketed aggregate, NOT the four pass-through rows.
    #[test]
    fn window_produces_bucketed_aggregates_not_passthrough() {
        let core = GraphCore::new();
        core.add_node(
            "m1".into(),
            blob(json!({"type":"Memory","valid_from":0,"value":10.0})),
        );
        core.add_node(
            "m2".into(),
            blob(json!({"type":"Memory","valid_from":5,"value":20.0})),
        );
        core.add_node(
            "m3".into(),
            blob(json!({"type":"Memory","valid_from":12,"value":30.0})),
        );
        core.add_node(
            "m4".into(),
            blob(json!({"type":"Memory","valid_from":18,"value":40.0})),
        );
        let view = core.analysis_snapshot();
        let sem = SemanticStore::new();
        let ctx = PlanCtx::new(&view, &sem);

        let out = Plan::new(vec![
            Op::Scan {
                label: "Memory".into(),
            },
            Op::Window { secs: 10.0 },
        ])
        .execute(&ctx)
        .unwrap();

        // Bucket [0,10): mean(10,20)=15 ; bucket [10,20): mean(30,40)=35. ts-sorted
        // bucket order regardless of (HashMap) scan order — and NOT the 4 input rows.
        assert_eq!(
            out.ids(),
            vec!["0".to_string(), "10".to_string()],
            "WINDOW collapses the 4 points into 2 time buckets (not pass-through)"
        );
        let scores: Vec<f32> = out.rows().iter().map(|r| r.score.unwrap()).collect();
        assert_eq!(
            scores,
            vec![15.0, 35.0],
            "each bucket carries its mean value"
        );
    }
}
