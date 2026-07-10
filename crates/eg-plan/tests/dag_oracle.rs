//! DAG differential oracle (CONCEPT:EG-KG.query.plan-dag-exec, E5 phase 2 — the HARD GATE).
//!
//! Every plan fixture the existing surface-interchangeability suites exercise
//! (`differential_oracle.rs`'s curated hybrid chains, `composition_matrix.rs`'s
//! source×transform matrix + edge cases) is converted `Plan -> PlanDag` via
//! [`eg_plan::PlanDag::from`] and run through [`eg_plan::execute_dag`]. The result MUST be
//! the BYTE-IDENTICAL `RowSet` [`eg_plan::execute`] produces over the original linear
//! `Plan` — ids, scores (compared by bit pattern via `stable_rows`) and ORDER. Any
//! divergence here means `dag_exec` is not a behavior-preserving generalization of
//! `exec::execute`, which is the non-negotiable proof E5 phase 2 requires before any
//! later phase (the DAG optimizer, multi-branch join, EXPLAIN, D7 writeback) may build on
//! `PlanDag`.

mod common;

use common::*;
use eg_plan::{execute, Op, Plan, PlanCtx, PlanDag, Pred};
use eg_types::wire::TimeAxis;

/// Run `ops` through BOTH `exec::execute` (over the linear `Plan`) and `dag_exec::execute_dag`
/// (over the `PlanDag` converted from that SAME `Plan`), and assert byte-identical results —
/// the one assertion every fixture in this file makes.
fn assert_dag_matches_linear(ops: Vec<Op>, ctx: &PlanCtx) {
    let plan = Plan::new(ops);
    let via_linear = execute(&plan, ctx).expect("linear exec must succeed");
    let dag = PlanDag::from(plan.clone());
    let via_dag = eg_plan::execute_dag(&dag, ctx).expect("dag exec must succeed");
    assert_eq!(
        stable_rows(&via_linear),
        stable_rows(&via_dag),
        "PlanDag execution must be byte-identical to the linear exec for plan {:?}",
        plan
    );
}

// ── the curated hybrid chains from differential_oracle.rs ───────────────────────

#[test]
fn classic_filter_traverse_rank_limit() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);
    assert_dag_matches_linear(
        vec![
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
            Op::Rank { query: query_vec() },
            Op::Limit { k: 10 },
        ],
        &ctx,
    );
}

#[test]
fn composed_traverse_rank_asof_limit() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);
    assert_dag_matches_linear(
        vec![
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
            Op::Rank { query: query_vec() },
            Op::AsOf {
                ts: 1_700_000_000.0,
                axis: TimeAxis::Valid,
            },
            Op::Limit { k: 10 },
        ],
        &ctx,
    );
}

#[test]
fn asof_valid_and_transaction_axis() {
    let (view, semantic) = build_events();
    let ctx = PlanCtx::new(&view, &semantic);
    assert_dag_matches_linear(
        vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 175.0,
                axis: TimeAxis::Valid,
            },
        ],
        &ctx,
    );
    assert_dag_matches_linear(
        vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 175.0,
                axis: TimeAxis::Transaction,
            },
        ],
        &ctx,
    );
}

#[test]
fn rank_then_mentions_rerank() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);
    assert_dag_matches_linear(
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank { query: query_vec() },
            Op::RankMentions {},
            Op::Limit { k: 5 },
        ],
        &ctx,
    );
}

#[cfg(feature = "owl")]
#[test]
fn reason_rank_traverse() {
    let (view, semantic) = build_reason_graph();
    let ctx = PlanCtx::new(&view, &semantic);
    assert_dag_matches_linear(
        vec![
            Op::Reason {
                target_class: "<http://example.org/ScholarlyWork>".into(),
                ontology: String::new(),
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 1,
            },
            Op::Limit { k: 10 },
        ],
        &ctx,
    );
}

// ── the composition_matrix source × transform grid (base `query`) ───────────────

#[test]
fn source_times_transform_matrix() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);

    let src = Op::Scan {
        label: "Doc".into(),
    };
    let transforms: Vec<Op> = vec![
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
        Op::Rank { query: query_vec() },
        Op::RankNodeDistance {
            center: "d1".into(),
        },
        Op::RankMentions {},
        Op::RankMmr { lambda: 0.5, k: 0 },
        Op::AsOf {
            ts: 1_700_000_000.0,
            axis: Default::default(),
        },
        Op::Window { secs: 3600.0 },
        Op::Foreign {
            name: "peer".into(),
        },
        Op::Limit { k: 3 },
    ];
    for t in transforms {
        assert_dag_matches_linear(vec![src.clone(), t], &ctx);
    }
}

/// Edge case — EMPTY source: every transform on an empty candidate set must still
/// agree between the two executors (some ops re-seed as a SOURCE on empty input —
/// exactly the EG-405 behavior the DAG optimizer must never perturb).
#[test]
fn empty_source_every_transform() {
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);
    let transforms: Vec<Op> = vec![
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 9999.0,
            }],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 3,
        },
        Op::Rank { query: query_vec() },
        Op::RankNodeDistance {
            center: "nope".into(),
        },
        Op::RankMentions {},
        Op::RankMmr { lambda: 0.3, k: 5 },
        Op::Window { secs: 60.0 },
        Op::Foreign { name: "p".into() },
        Op::Limit { k: 10 },
    ];
    for t in transforms {
        assert_dag_matches_linear(
            vec![
                Op::Scan {
                    label: "NoSuchLabel".into(),
                },
                t,
            ],
            &ctx,
        );
    }
}

// ── epistemic ops (CI runs `--features "query,epistemic"`) ──────────────────────

#[cfg(feature = "epistemic")]
#[test]
fn epistemic_ops_dag_matches_linear() {
    let (view, semantic) = build_beliefs();
    let ctx = PlanCtx::new(&view, &semantic);

    assert_dag_matches_linear(
        vec![Op::EvidenceFor {
            claim_id: "claim1".into(),
        }],
        &ctx,
    );
    assert_dag_matches_linear(
        vec![Op::Contradicts {
            node_id: "claim1".into(),
        }],
        &ctx,
    );
    assert_dag_matches_linear(
        vec![Op::SupportedBy {
            node_id: "claim1".into(),
        }],
        &ctx,
    );
    assert_dag_matches_linear(vec![Op::ConfidenceOp {}], &ctx);
    assert_dag_matches_linear(
        vec![
            Op::Scan {
                label: "Claim".into(),
            },
            Op::EvidenceFor {
                claim_id: "claim1".into(),
            },
            Op::Rank {
                query: vec![0.0, 1.0, 0.0, 0.0],
            },
        ],
        &ctx,
    );
    assert_dag_matches_linear(
        vec![Op::ExplainBelief {
            node_id: "claim1".into(),
        }],
        &ctx,
    );
}

/// A small belief-substrate fixture mirroring the crate-private `epistemic_tests.rs`
/// fixture (not reachable from an integration test): a `Claim` supported by `evidence1`,
/// contradicted by `counter1`, attacked by `attacker1`, supporting `derived1`.
#[cfg(feature = "epistemic")]
fn build_beliefs() -> (
    eg_core::graph::GraphView,
    eg_core::compute::semantic::SemanticStore,
) {
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    let core = GraphCore::new();
    core.add_node(
        "claim1".into(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    core.add_node(
        "evidence1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "counter1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "attacker1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "derived1".into(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    core.add_edge(
        "evidence1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "SUPPORTS" })),
    )
    .unwrap();
    core.add_edge(
        "counter1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "CONTRADICTS" })),
    )
    .unwrap();
    core.add_edge(
        "attacker1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "ATTACKS" })),
    )
    .unwrap();
    core.add_edge(
        "claim1".into(),
        "derived1".into(),
        blob(json!({ "relationship_type": "SUPPORTS" })),
    )
    .unwrap();

    let mut semantic = SemanticStore::new();
    semantic.add_embedding("claim1".into(), vec![1.0, 0.0, 0.0, 0.0]);
    semantic.add_embedding("evidence1".into(), vec![0.0, 1.0, 0.0, 0.0]);
    (core.analysis_snapshot(), semantic)
}
