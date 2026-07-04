//! Cross-modal composition matrix (CONCEPT:EG-KG.compute.systematically-exercise) — systematically exercise
//! source-op × transform-op combinations and 3–5-op mixed chains, plus the edge cases
//! the review flagged (empty set, single row, high cardinality, low-selectivity RANK,
//! deep TRAVERSE, REASON feeding TRAVERSE/RANK). Each plan must (a) NOT panic and (b)
//! yield a structurally sane `RowSet` — ids unique (the closed-algebra dedup invariant),
//! a `Limit k` truncates to ≤ k, and no op emits a row absent from the reachable id
//! universe. This is the "does every op compose with every other op" breadth net that
//! backstops the depth proofs in `differential_oracle.rs`.

mod common;

use common::*;
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx, Pred, RowSet};
use serde_json::json;
use std::collections::HashSet;

/// A larger, denser graph for the high-cardinality / deep-traverse edge cases: a
/// 60-node `Item` chain (i0→i1→…→i59 via NEXT) plus embeddings, so a deep TRAVERSE and a
/// low-selectivity RANK have real fan-out to chew on.
fn build_chain(n: usize) -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    for k in 0..n {
        core.add_node(
            format!("i{k}"),
            blob(json!({ "type": "Item", "year": 2000 + (k as i64 % 30) })),
        );
    }
    for k in 0..n.saturating_sub(1) {
        core.add_edge(
            format!("i{k}"),
            format!("i{}", k + 1),
            blob(json!({ "relationship": "NEXT" })),
        )
        .unwrap();
    }
    let mut semantic = SemanticStore::new();
    for k in 0..n {
        // A spread of directions so RANK imposes a real (non-trivial) order.
        let a = (k as f32 * 0.11).sin();
        let b = (k as f32 * 0.11).cos();
        semantic.add_embedding(format!("i{k}"), vec![a, b, 0.0]);
    }
    (core.analysis_snapshot(), semantic)
}

/// Assert a `RowSet` is structurally sane: ids are unique, and (when a universe is
/// known) every id is a real node id. Returns the row count.
fn assert_sane(rs: &RowSet, universe: Option<&HashSet<String>>) -> usize {
    let ids = rs.ids();
    let uniq: HashSet<&String> = ids.iter().collect();
    assert_eq!(
        uniq.len(),
        ids.len(),
        "RowSet ids must be unique (dedup invariant)"
    );
    if let Some(u) = universe {
        for id in &ids {
            assert!(u.contains(id), "row id `{id}` is not in the node universe");
        }
    }
    ids.len()
}

fn run(ops: Vec<Op>, view: &GraphView, semantic: &SemanticStore) -> RowSet {
    execute(&Plan::new(ops), &PlanCtx::new(view, semantic)).expect("plan must execute (no error)")
}

/// SOURCE × TRANSFORM matrix: for each base-`query` source, run every base-`query`
/// transform after it and assert the composed 2-op plan produces a sane RowSet. Every
/// cell is a distinct cross-modal seam (relational source → graph/vector/time transform).
#[test]
fn source_times_transform_matrix_base_query() {
    let (view, semantic) = build_docs();
    let universe: HashSet<String> = ["d1", "d2", "d3", "d4", "d5", "old", "t1"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Sources that seed from the graph (each is `[Op]` — a one-op prefix).
    let sources: Vec<(&str, Op)> = vec![(
        "Scan(Doc)",
        Op::Scan {
            label: "Doc".into(),
        },
    )];

    // Transforms to hang off each source (name, op).
    let transforms: Vec<(&str, Op)> = vec![
        (
            "Filter",
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
        ),
        (
            "Traverse",
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
        ),
        ("Rank", Op::Rank { query: query_vec() }),
        (
            "RankNodeDistance",
            Op::RankNodeDistance {
                center: "d1".into(),
            },
        ),
        ("RankMentions", Op::RankMentions {}),
        ("RankMmr", Op::RankMmr { lambda: 0.5, k: 0 }),
        (
            "AsOf",
            Op::AsOf {
                ts: 1_700_000_000.0,
                axis: Default::default(),
            },
        ),
        ("Window", Op::Window { secs: 3600.0 }),
        (
            "Foreign",
            Op::Foreign {
                name: "peer".into(),
            },
        ),
        ("Limit", Op::Limit { k: 3 }),
    ];

    for (sname, src) in &sources {
        for (tname, t) in &transforms {
            let ops = vec![src.clone(), t.clone()];
            let rs = run(ops, &view, &semantic);
            let n = assert_sane(&rs, Some(&universe));
            if *tname == "Limit" {
                assert!(
                    n <= 3,
                    "{sname} |> {tname}: Limit 3 must bound the row count"
                );
            }
        }
    }
}

/// Edge case — EMPTY source: a `Scan` of a non-existent label yields an empty RowSet,
/// and EVERY transform on the empty set is a no-op that still produces a valid (empty or
/// source-mode) RowSet without panicking. (Note: `AsOf`/`Reason` act as SOURCES on empty
/// input, so they may re-seed — still sane.)
#[test]
fn empty_source_every_transform_is_safe() {
    let (view, semantic) = build_docs();
    let transforms = vec![
        // A valid column with an unsatisfiable bound (the FILTER leg resolves the schema
        // even on an empty input — an unknown column is a real SQL error, not a plan seam).
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
        let rs = run(
            vec![
                Op::Scan {
                    label: "NoSuchLabel".into(),
                },
                t,
            ],
            &view,
            &semantic,
        );
        assert_sane(&rs, None); // may be empty; must not panic and must stay well-formed.
    }
}

/// Edge case — SINGLE ROW: a filter that selects exactly one row (`old`, the only Doc
/// with year < 2021) composes through a rank + limit and stays a singleton without
/// panic. (NB: the engine's convention is "empty candidate set ⇒ a downstream FILTER/AsOf
/// re-seeds as a SOURCE", so this test keeps the candidate set NON-empty end-to-end — the
/// single row carries an embedding, so the vector RANK retains it.)
#[test]
fn single_row_source_chains_cleanly() {
    let (view, semantic) = build_docs();
    let rs = run(
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::LtNum {
                    prop: "year".into(),
                    n: 2021.0,
                }],
            },
            Op::Rank { query: query_vec() },
            Op::Limit { k: 5 },
        ],
        &view,
        &semantic,
    );
    let n = assert_sane(&rs, None);
    assert_eq!(
        n, 1,
        "exactly the single pre-2021 Doc (`old`) survives, got {n}"
    );
    assert_eq!(rs.ids(), vec!["old".to_string()]);
}

/// Edge case — HIGH CARDINALITY + LOW-SELECTIVITY RANK + DEEP TRAVERSE: over a 60-node
/// chain, a broad filter (keeps everything) → a deep 1..8-hop traverse → a rank over the
/// large reached set → a limit. Must not panic and must respect the limit.
#[test]
fn high_cardinality_deep_traverse_low_selectivity_rank() {
    let (view, semantic) = build_chain(60);
    let universe: HashSet<String> = (0..60).map(|k| format!("i{k}")).collect();
    let rs = run(
        vec![
            Op::Scan {
                label: "Item".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 1900.0,
                }],
            }, // keeps all
            Op::Traverse {
                rel: "NEXT".into(),
                min: 1,
                max: 8,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            Op::Limit { k: 12 },
        ],
        &view,
        &semantic,
    );
    let n = assert_sane(&rs, Some(&universe));
    assert!(
        n <= 12,
        "Limit 12 must bound the deep-traverse rank, got {n}"
    );
    assert!(
        n > 0,
        "the deep traverse over a 60-chain must reach SOMETHING"
    );
}

/// Mixed 5-op chains across every base ranker, proving RANK-family ops all compose in
/// the middle of a graph pipeline without dropping into an inconsistent state.
#[test]
fn mixed_five_op_chains_over_every_ranker() {
    let (view, semantic) = build_docs();
    let universe: HashSet<String> = ["d1", "d2", "d3", "d4", "d5", "old", "t1"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let rankers: Vec<Op> = vec![
        Op::Rank { query: query_vec() },
        Op::RankNodeDistance {
            center: "d1".into(),
        },
        Op::RankMentions {},
        Op::RankMmr { lambda: 0.4, k: 0 },
    ];
    for r in rankers {
        let ops = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2020.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 0,
                max: 2,
            },
            r,
            Op::Limit { k: 4 },
        ];
        let n = assert_sane(&run(ops, &view, &semantic), Some(&universe));
        assert!(n <= 4, "the 5-op chain's Limit 4 must hold, got {n}");
    }
}

// ── owl: REASON feeding TRAVERSE / RANK (the review's named edge case) ────────

#[cfg(feature = "owl")]
#[test]
fn reason_feeds_traverse_and_rank() {
    let (view, semantic) = build_reason_graph();
    let target = "<http://example.org/ScholarlyWork>";

    // REASON → TRAVERSE → RANK → LIMIT.
    let a = run(
        vec![
            Op::Reason {
                target_class: target.into(),
                ontology: String::new(),
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 1,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            Op::Limit { k: 5 },
        ],
        &view,
        &semantic,
    );
    assert!(
        !a.ids().is_empty(),
        "REASON→TRAVERSE→RANK must reach the CITES frontier"
    );

    // REASON → RANK (members ranked by their own embeddings) → TRAVERSE.
    let b = run(
        vec![
            Op::Reason {
                target_class: target.into(),
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
        ],
        &view,
        &semantic,
    );
    assert!(
        !b.ids().is_empty(),
        "REASON→RANK→TRAVERSE must compose (members carry embeddings)"
    );
}

// ── timeseries: SensorFuse / TsScan as sources feeding a downstream Rank/Limit ─

#[cfg(feature = "timeseries")]
#[test]
fn timeseries_sources_feed_downstream() {
    const NS: i64 = 1_000_000_000;
    let core = GraphCore::new();
    for k in 0..5i64 {
        core.add_node(
            format!("imu{k}"),
            blob(json!({ "type": "imu", "valid_from": k * NS, "value": k as f64 })),
        );
    }
    for t in [0i64, 2, 4] {
        core.add_node(
            format!("gps{t}"),
            blob(json!({ "type": "gps", "valid_from": t * NS, "value": 100.0 + t as f64 })),
        );
    }
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();

    // SensorFuse SOURCE → Limit: fused instants compose into a downstream Limit.
    let fused = run(
        vec![
            Op::SensorFuse {
                streams: vec!["imu".into(), "gps".into()],
                tolerance_ns: (10 * NS) as u64,
            },
            Op::Limit { k: 100 },
        ],
        &view,
        &semantic,
    );
    assert!(
        !fused.ids().is_empty(),
        "SensorFuse must emit fused instants"
    );
    assert_sane(&fused, None);
}
