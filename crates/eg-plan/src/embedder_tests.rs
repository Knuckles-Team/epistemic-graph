//! Server-side embedder seam proofs (CONCEPT:EG-KG.compute.no-embedder-bound-op/412) — the UQL `RANK BY ~ "text"`
//! NL→vector resolver, over a HAND-BUILT [`PlanCtx`] (no server, fully self-contained).
//!
//! Three headline properties:
//!  * `rank_embed_ranks_by_resolved_vector` — with an embedder bound, `RANK BY ~ "text"`
//!    (`Op::RankEmbed`) ranks IDENTICALLY to a literal-vector `Op::Rank` over the vector
//!    the embedder resolves the text to — proving the text is resolved and then kNN-ranked.
//!  * `rank_embed_without_embedder_is_typed_error` — with NO embedder bound, `Op::RankEmbed`
//!    is a clean typed error (never a panic), naming the missing binding.
//!  * `hash_embedder_is_deterministic` — the fallback `HashEmbedder` maps a text to a
//!    stable unit-norm vector (same text ⇒ same vector; a bound one makes the seam run).

use crate::exec::{execute, HashEmbedder, PlanCtx, TextEmbedder};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_types::wire::{Op, Plan};
use serde_json::json;

/// A tiny deterministic test embedder mapping a fixed set of texts to fixed vectors (an
/// exact-space stand-in for a real model), so a test can assert the resolved ranking.
struct MapEmbedder;

impl TextEmbedder for MapEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        match text {
            "hello" => Ok(vec![1.0, 0.0]),
            "world" => Ok(vec![0.0, 1.0]),
            other => Err(format!("MapEmbedder has no vector for {other:?}")),
        }
    }
}

/// Three `Doc` nodes with distinct 2-D embeddings, so a query near `[1,0]` ranks `a`
/// first, then `c`, then `b`.
fn fixture() -> (eg_core::graph::GraphView, SemanticStore) {
    let core = GraphCore::new();
    let blob = |v: serde_json::Value| rmp_serde::to_vec_named(&v).unwrap();
    for id in ["a", "b", "c"] {
        core.add_node(id.into(), blob(json!({ "type": "Doc" })));
    }
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("a".into(), vec![1.0, 0.0]).unwrap(); // closest to [1,0]
    semantic.add_embedding("b".into(), vec![0.0, 1.0]).unwrap(); // farthest
    semantic.add_embedding("c".into(), vec![0.7, 0.7]).unwrap(); // middle
    (core.analysis_snapshot(), semantic)
}

/// CONCEPT:EG-KG.compute.no-embedder-bound-op — with an embedder bound, `Scan → RankEmbed "hello"` ranks IDENTICALLY
/// to `Scan → Rank [1,0]` (the vector the embedder resolves "hello" to): the text is
/// resolved server-side, then kNN-ranked exactly like a literal vector.
#[test]
fn rank_embed_ranks_by_resolved_vector() {
    let (view, semantic) = fixture();
    let embedder = MapEmbedder;

    let embed_plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankEmbed {
            text: "hello".into(),
        },
    ]);
    let ctx = PlanCtx::new(&view, &semantic).with_embedder(&embedder);
    let embed_ids = execute(&embed_plan, &ctx).unwrap().ids();

    // The literal-vector oracle: the same vector the embedder maps "hello" to.
    let lit_plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Rank {
            query: vec![1.0, 0.0],
        },
    ]);
    let lit_ids = execute(&lit_plan, &PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();

    assert_eq!(
        embed_ids, lit_ids,
        "RANK BY ~ \"hello\" must rank exactly as RANK BY ~[1,0] (the resolved vector)"
    );
    assert_eq!(
        embed_ids.first().map(String::as_str),
        Some("a"),
        "a is closest to [1,0]"
    );
}

/// CONCEPT:EG-KG.compute.no-embedder-bound-op — with NO embedder bound, `Op::RankEmbed` is a clean typed error naming
/// the missing binding (never a panic).
#[test]
fn rank_embed_without_embedder_is_typed_error() {
    let (view, semantic) = fixture();
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankEmbed {
            text: "hello".into(),
        },
    ]);
    let ctx = PlanCtx::new(&view, &semantic); // no .with_embedder
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(
        err.contains("no server-side text embedder is bound"),
        "unbound RankEmbed must be a clear typed error, got: {err}"
    );
}

/// An embedder that ERRORS (e.g. an unreachable model) surfaces as a clean plan error,
/// not a panic.
#[test]
fn rank_embed_embedder_error_propagates() {
    let (view, semantic) = fixture();
    let embedder = MapEmbedder;
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankEmbed {
            text: "unknown-text".into(),
        },
    ]);
    let ctx = PlanCtx::new(&view, &semantic).with_embedder(&embedder);
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(
        err.contains("no vector for"),
        "embedder error must propagate, got: {err}"
    );
}

/// CONCEPT:EG-KG.compute.no-embedder-bound-op — the deterministic `HashEmbedder` fallback maps a text to a STABLE
/// unit-norm vector (same text ⇒ same vector), and a bound one makes the seam run without
/// erroring (its ranking is deterministic but semantically arbitrary, by design).
#[test]
fn hash_embedder_is_deterministic() {
    let e = HashEmbedder::new(8);
    let v1 = e.embed("hello").unwrap();
    let v2 = e.embed("hello").unwrap();
    let v3 = e.embed("world").unwrap();
    assert_eq!(v1.len(), 8);
    assert_eq!(v1, v2, "same text ⇒ same vector");
    assert_ne!(
        v1, v3,
        "different text ⇒ (almost always) a different vector"
    );
    let norm = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-5, "unit-norm, got {norm}");

    // Bound onto a ctx, an Op::RankEmbed runs (no error), producing a deterministic order.
    let (view, semantic) = fixture();
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankEmbed {
            text: "hello".into(),
        },
    ]);
    let ctx = PlanCtx::new(&view, &semantic).with_embedder(&e);
    let ids = execute(&plan, &ctx).unwrap().ids();
    assert_eq!(
        ids.len(),
        3,
        "the hash-embedded rank still ranks all candidates"
    );
}
