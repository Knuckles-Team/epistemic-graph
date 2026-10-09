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

fn category_rank_plan(category: &str, query: Vec<f32>, limit: Option<usize>) -> Plan {
    let mut ops = vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "category".into(),
                value: category.into(),
            }],
        },
        Op::Rank { query },
    ];
    if let Some(k) = limit {
        ops.push(Op::Limit { k });
    }
    Plan::new(ops)
}

fn category_members(category: usize) -> HashSet<String> {
    (0..DOCS)
        .filter(|doc| doc % 8 == category)
        .map(|doc| format!("d{doc:04}"))
        .collect()
}

/// The EH-564 query shape: one seed doc, a 1..2-hop traversal, then vector rank and
/// `Limit 10`. Every seed's answer equals the brute-force top-10 of its reached set.
// spec: EG-DURABLE-KERNEL-R057, EG-FEDERATED-QUERY-R039
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
        let reached: HashSet<String> =
            bfs_reached(&view, std::slice::from_ref(&key), "CITES", 1, 2)
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
    let plan = category_rank_plan("c3", query.clone(), None);
    let members = category_members(3);
    let got = ids(&plan, &ctx);
    assert_eq!(got, oracle(&semantic, &query, &members, members.len()));
}

/// Recall of `got` against the exact top of the same length.
fn recall(got: &[(String, f32)], exact: &[String]) -> f64 {
    let exact: HashSet<&str> = exact.iter().map(String::as_str).collect();
    let hits = got
        .iter()
        .filter(|(id, _)| exact.contains(id.as_str()))
        .count();
    hits as f64 / exact.len().max(1) as f64
}

/// The top-`k` rank with `Limit` fused (EH-565): the plan's answer equals the exact
/// top-10 of the category (375 members, under the exact ceiling).
#[test]
fn rank_then_limit_is_the_exact_top_k_under_the_ceiling() {
    let (core, semantic) = corpus();
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic);
    let query = vector(77);
    let plan = category_rank_plan("c5", query.clone(), Some(10));
    let members = category_members(5);
    assert_eq!(ids(&plan, &ctx), oracle(&semantic, &query, &members, 10));
}

/// The filtered-ANN path (forced with a zero exact ceiling): exactly `k` rows, all
/// candidates — for a broad set (with near-exact recall) and for a small scattered set
/// the walk is likely to strand, where the exact top-up must still deliver `k`.
#[test]
fn filtered_ann_path_returns_k_candidates_with_high_recall() {
    use crate::exec::vector_rank::ranked;

    let (_core, semantic) = corpus();
    let broad: Vec<String> = (0..DOCS)
        .filter(|doc| doc % 8 == 2)
        .map(|doc| format!("d{doc:04}"))
        .collect();
    let scattered: Vec<String> = (0..12).map(|i| format!("d{:04}", i * 241 + 7)).collect();
    for (label, members) in [("broad", broad), ("scattered", scattered)] {
        let set: HashSet<&str> = members.iter().map(String::as_str).collect();
        let owned: HashSet<String> = members.iter().cloned().collect();
        let query = vector(9_001);
        let got = ranked(&semantic, &query, &set, 10, 0);
        assert_eq!(got.len(), 10, "{label}: k rows survive");
        assert!(
            got.iter().all(|(id, _)| set.contains(id.as_str())),
            "{label}: candidates only"
        );
        let exact = oracle(&semantic, &query, &owned, 10);
        assert!(
            label == "scattered" || recall(&got, &exact) >= 0.8,
            "{label}: recall {}",
            recall(&got, &exact)
        );
    }
}
