//! EH-564 / EH-565 — a vector `Rank` over a filtered or traversed candidate set is
//! exact: it returns every candidate with an embedding, in exact cosine order, so a
//! trailing `Limit k` yields `min(k, eligible)` rows — never fewer because an ANN walk
//! missed candidates scattered through a large index.

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use serde_json::json;

use crate::algebra::{Op, Plan, Pred};
use crate::exec::{bfs_reached, PlanCtx, PlanExt};

const DOCS: usize = 3_000;
const DIM: usize = 16;

fn blob(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

/// A deterministic pseudo-random unit-ish vector for `seed`.
fn vector(seed: usize) -> Vec<f32> {
    let mut state = (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % 2_001) as f32 / 1_000.0 - 1.0
        })
        .collect()
}

/// `DOCS` Docs in 8 categories, each citing three pseudo-random earlier docs, all
/// embedded — far above the brute-force threshold, so the store builds its ANN index.
fn corpus() -> (GraphCore, SemanticStore) {
    let core = GraphCore::new();
    let mut semantic = SemanticStore::new();
    for doc in 0..DOCS {
        let id = format!("d{doc:04}");
        let props = json!({ "type": "Doc", "key": id, "category": format!("c{}", doc % 8) });
        core.add_node(id.clone(), blob(props));
        semantic.add_embedding(id, vector(doc)).unwrap();
    }
    for doc in 1..DOCS {
        for hop in 1..=3 {
            let target = (doc * 7_919 + hop * 104_729) % doc;
            let edge = blob(json!({ "relationship": "CITES" }));
            let _ = core.add_edge(format!("d{doc:04}"), format!("d{target:04}"), edge);
        }
    }
    (core, semantic)
}

/// Brute-force oracle, independent of the store: exact cosine over `ids`, best first,
/// ties by id.
fn oracle(semantic: &SemanticStore, query: &[f32], ids: &HashSet<String>, k: usize) -> Vec<String> {
    let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mut scored: Vec<(String, f32)> = ids
        .iter()
        .filter_map(|id| {
            let emb = semantic.get_embedding(id)?;
            let dot: f32 = emb.iter().zip(query).map(|(a, b)| a * b).sum();
            Some((id.clone(), dot / (norm(&emb) * norm(query))))
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

fn ids(plan: &Plan, ctx: &PlanCtx) -> Vec<String> {
    plan.execute(ctx).unwrap().ids()
}

/// The EH-564 query shape: one seed doc, a 1..2-hop traversal, then vector rank and
/// `Limit 10`. Every seed's answer equals the brute-force top-10 of its reached set.
#[test]
fn traversal_then_rank_returns_the_exact_top_k() {
    let (core, semantic) = corpus();
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic);
    for seed in (2_000..DOCS).step_by(97) {
        let key = format!("d{seed:04}");
        let query = vector(seed * 31 + 5);
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: "key".into(),
                    value: key.clone(),
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: query.clone(),
            },
            Op::Limit { k: 10 },
        ]);
        let reached: HashSet<String> = bfs_reached(&view, &[key.clone()], "CITES", 1, 2)
            .into_iter()
            .collect();
        let got = ids(&plan, &ctx);
        assert_eq!(got.len(), reached.len().min(10), "seed {key}: no shortfall");
        assert_eq!(got, oracle(&semantic, &query, &reached, 10), "seed {key}");
    }
}

/// The filtered-rank shape (EH-565): a category filter, then rank over its ~375
/// members — exact, complete and ordered.
#[test]
fn filtered_rank_ranks_every_member_exactly() {
    let (core, semantic) = corpus();
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic);
    let query = vector(424_242);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "category".into(),
                value: "c3".into(),
            }],
        },
        Op::Rank {
            query: query.clone(),
        },
    ]);
    let members: HashSet<String> = (0..DOCS)
        .filter(|doc| doc % 8 == 3)
        .map(|doc| format!("d{doc:04}"))
        .collect();
    let got = ids(&plan, &ctx);
    assert_eq!(got, oracle(&semantic, &query, &members, members.len()));
}
