//! Hybrid lexical+vector proofs for the `RankText` op and the RRF `FuseRrf` op
//! (CONCEPT:KG-2.215, `cargo test -p eg-plan --features "text query"`).
//!
//! THE headline assertion: a HYBRID (RRF-fused vector + BM25) query beats EITHER
//! modality ALONE on a fixture engineered so the two signals disagree on the top
//! result but AGREE on the truly-relevant doc — which is exactly when fusion wins and
//! why modern agent retrieval fuses lexical + vector instead of picking one.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_text::TextIndex;
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::{PlanCtx, PlanExt};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A `Doc`-only fixture engineered for the hybrid win. Four candidate Docs; the
/// QUERY is "vector graph database search" with a target embedding ≈ [1,0,0].
///
///  * `target` — relevant to BOTH signals but #1 in NEITHER: vector rank #2, BM25
///    rank #2.
///  * `vec_only` — vector #1 (closest embedding) but lexically irrelevant (no query
///    terms) → BM25 misses it entirely.
///  * `lex_only` — BM25 #1 (densest query-term text) but a far embedding → vector
///    ranks it last.
///  * `noise` — weak in both.
///
/// So vector-alone tops `vec_only`, BM25-alone tops `lex_only`, but RRF (target is
/// #2+#2) lifts `target` to #1. That is the fused-beats-either-alone proof.
struct HybridFx {
    view: GraphView,
    semantic: SemanticStore,
    text: TextIndex,
}

fn build_hybrid() -> HybridFx {
    let core = GraphCore::new();
    for id in ["target", "vec_only", "lex_only", "noise"] {
        core.add_node(id.into(), blob(json!({ "type": "Doc" })));
    }

    // Query embedding is [1,0,0]. Distances: vec_only closest, target 2nd, then noise,
    // lex_only farthest.
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("vec_only".into(), vec![0.99, 0.10, 0.0]); // vector #1
    semantic.add_embedding("target".into(), vec![0.90, 0.40, 0.0]); // vector #2
    semantic.add_embedding("noise".into(), vec![0.50, 0.85, 0.0]); // vector #3
    semantic.add_embedding("lex_only".into(), vec![0.0, 0.10, 0.99]); // vector last

    // Query text terms: "graph database search". BM25 density:
    //   lex_only — packed with all three terms (BM25 #1)
    //   target   — has the terms but less dense (BM25 #2)
    //   noise    — one weak term
    //   vec_only — NONE of the terms (BM25 misses it)
    let mut text = TextIndex::in_memory().unwrap();
    text.upsert(
        "lex_only",
        "graph graph database database search search graph database search retrieval",
    );
    text.upsert("target", "a graph database with search over nodes");
    text.upsert(
        "noise",
        "search results unrelated to the rest of the corpus",
    );
    text.upsert(
        "vec_only",
        "astronomy stars galaxies nebulae cosmology orbital",
    );
    text.commit().unwrap();

    HybridFx {
        view: core.analysis_snapshot(),
        semantic,
        text,
    }
}

fn seed_plan() -> Vec<Op> {
    vec![Op::Scan {
        label: "Doc".into(),
    }]
}

const QUERY_TEXT: &str = "graph database search";
fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0]
}

/// THE proof: hybrid RRF beats vector-alone AND BM25-alone. The three plans share the
/// same `Scan Doc` seed; only the ranking tail differs.
#[test]
fn hybrid_beats_either_modality_alone() {
    let fx = build_hybrid();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_text(&fx.text);

    // ── vector-alone ──
    let vec_plan = {
        let mut ops = seed_plan();
        ops.push(Op::Rank { query: query_vec() });
        ops.push(Op::Limit { k: 4 });
        Plan::new(ops)
    };
    let vec_top = vec_plan.execute(&ctx).unwrap().ids();
    assert_eq!(
        vec_top.first().map(String::as_str),
        Some("vec_only"),
        "vector-alone tops the embedding-closest doc, not the truly-relevant one: {vec_top:?}"
    );

    // ── BM25-alone ──
    let text_plan = {
        let mut ops = seed_plan();
        ops.push(Op::RankText {
            query: QUERY_TEXT.into(),
        });
        ops.push(Op::Limit { k: 4 });
        Plan::new(ops)
    };
    let text_top = text_plan.execute(&ctx).unwrap().ids();
    assert_eq!(
        text_top.first().map(String::as_str),
        Some("lex_only"),
        "BM25-alone tops the term-densest doc, not the truly-relevant one: {text_top:?}"
    );

    // ── hybrid (RRF of vector ⊕ BM25) ──
    let hybrid_plan = {
        let mut ops = seed_plan();
        ops.push(Op::FuseRrf {
            left: vec![Op::Rank { query: query_vec() }],
            right: vec![Op::RankText {
                query: QUERY_TEXT.into(),
            }],
            k: 0.0, // ⇒ eg_text::RRF_K = 60
        });
        ops.push(Op::Limit { k: 4 });
        Plan::new(ops)
    };
    let hybrid_top = hybrid_plan.execute(&ctx).unwrap().ids();
    assert_eq!(hybrid_top.first().map(String::as_str), Some("target"),
        "HYBRID lifts the doc relevant to BOTH signals to #1 — beating either alone: {hybrid_top:?}");

    // And the win is strict: neither single modality ranked `target` first.
    assert_ne!(vec_top.first(), hybrid_top.first());
    assert_ne!(text_top.first(), hybrid_top.first());
}

/// `RankText` re-orders the candidate set by BM25, scores attached descending, and is
/// restricted to the input candidate set (it ranks candidates, not the whole index).
#[test]
fn rank_text_orders_candidates_by_bm25() {
    let fx = build_hybrid();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_text(&fx.text);

    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankText {
            query: QUERY_TEXT.into(),
        },
    ]);
    let res = plan.execute(&ctx).unwrap();
    let ids = res.ids();
    // lex_only (densest) first; vec_only (no query terms) absent.
    assert_eq!(
        ids.first().map(String::as_str),
        Some("lex_only"),
        "BM25 order: {ids:?}"
    );
    assert!(
        !ids.contains(&"vec_only".to_string()),
        "term-less doc excluded: {ids:?}"
    );
    let scores: Vec<f32> = res.rows().iter().map(|r| r.score.unwrap()).collect();
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "BM25 descending: {scores:?}"
    );
}

/// With NO text index attached, `RankText` degrades to empty (never errs the plan) —
/// the same graceful degradation as a vector `Rank` over an empty embedding store.
#[test]
fn rank_text_without_index_is_empty_not_error() {
    let fx = build_hybrid();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic); // no .with_text(...)
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankText {
            query: QUERY_TEXT.into(),
        },
    ]);
    let res = plan.execute(&ctx).unwrap();
    assert!(
        res.is_empty(),
        "no text index ⇒ no lexical hits (degrade, not error)"
    );
}
