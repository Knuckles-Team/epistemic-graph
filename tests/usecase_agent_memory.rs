//! HIGH-VALUE USE-CASE SUITE #1 — agent-memory retrieval (CONCEPT:EG-KG.query.usecase-agent-memory).
//!
//! The core use-case for agent-utilities: an agent's episodic memory is ingested as
//! graph nodes + vector embeddings + a small OWL ontology, and ONE fused pipeline query
//! honors EVERY retrieval signal at once — semantic (vector kNN), lexical (BM25 text),
//! graph expansion (topology proximity / traversal), OWL inference (only real memories),
//! bi-temporal `AS OF` (what was live at instant t), and RRF fusion of the ranked legs.
//!
//! This runs through the REAL query engine — `eg_plan::execute` over a `PlanCtx` built
//! from the live `GraphView` + `SemanticStore` + `TextIndex`, the SAME executor the
//! server's `run_unified` calls. Each assertion proves the fused result honors the
//! modality it names; the hybrid winner beats every single-modality ranking.
//!
//! SEAMS exercised: vector⇄graph⇄text⇄OWL⇄time fusion in one plan.
//! RESOLVED (→ docs/north_star.md, CONCEPT:EG-KG.query.decay-not-foldable-finding →
//! CONCEPT:EG-KG.query.reason-decay-in-plan): confidence time-DECAY now FOLDS INTO the single
//! fused plan — `Op::Reason` decays confidence IN-PLAN when a `(now, half_life)` context is
//! bound via `PlanCtx::with_decay` (decay-neutral by default), so decay composes alongside the
//! bi-temporal `AS OF` leg in ONE plan. Proven both on the reasoner surface
//! (`eg_rdf::owl::fact_confidence`) AND in-plan below (`confidence_decay_folds_into_reason_plan`).
//! RESOLVED (CONCEPT:EG-KG.query.served-text-index-unbound-finding →
//! CONCEPT:EG-KG.query.served-text-index-binding): the SERVED `run_unified` now binds a
//! snapshot-derived BM25 index, so the lexical leg returns real hits through RPC (see
//! `tests/served_query_completeness.rs`); this suite still drives the executor directly to
//! isolate each modality.
//!
//! Module-gated on the modality legs it fuses; compiles + runs under `--features full`.
#![cfg(all(feature = "query", feature = "text", feature = "owl-plan"))]

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx};
use eg_text::TextIndex;
use eg_types::wire::TimeAxis;
use serde_json::json;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// The query the agent is remembering against.
const QUERY_TEXT: &str = "kubernetes deployment rollout failure";
fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0]
}

/// An agent's episodic memory graph. Five `Episode` memories + one `Chatter` non-memory,
/// each with an embedding, indexed text, a bitemporal validity window, and a graph edge
/// from a focal `session` node so topology-proximity is a real signal:
///
///  * `target`     — vector #2 AND lexical #2 AND 1 hop from `session` → the doubly+graph
///    relevant memory RRF must lift to #1. Live `[0,∞)`.
///  * `vec_only`   — vector #1, NONE of the query terms, far in the graph. Live.
///  * `lex_only`   — lexical #1 (term-dense), vector far. Live.
///  * `chatter`    — a `Chatter` node (NOT an `Episode`): weak signals; the OWL `Reason`
///    leg must drop it even if a raw rank would keep it.
///  * `expired`    — strong-ish signals but its validity window `[0,100)` has CLOSED, so
///    an `AS OF now` re-selection drops it (the bitemporal leg).
struct MemoryFx {
    view: GraphView,
    semantic: SemanticStore,
    text: TextIndex,
}

fn build_memory() -> MemoryFx {
    let core = GraphCore::new();
    // Focal node the agent is reasoning "near" (the current session/context).
    core.add_node(
        "session".into(),
        blob(json!({ "type": "Session", "valid_from": 0 })),
    );

    // Episodes (real memories) — valid_from/valid_until in epoch seconds.
    core.add_node(
        "target".into(),
        blob(json!({ "type": "Episode", "valid_from": 0 })),
    );
    core.add_node(
        "vec_only".into(),
        blob(json!({ "type": "Episode", "valid_from": 0 })),
    );
    core.add_node(
        "lex_only".into(),
        blob(json!({ "type": "Episode", "valid_from": 0 })),
    );
    core.add_node(
        "expired".into(),
        blob(json!({ "type": "Episode", "valid_from": 0, "valid_until": 100 })),
    );
    // A non-memory chatter node (must be reasoned OUT of a Memory retrieval).
    core.add_node(
        "chatter".into(),
        blob(json!({ "type": "Chatter", "valid_from": 0 })),
    );

    // Graph: session -RELATES-> target (1 hop); target -RELATES-> lex_only (lex_only 2
    // hops); vec_only left unlinked (unreachable ⇒ node-distance 0). This makes `target`
    // the graph-closest memory to the focal session.
    for (s, t) in [
        ("session", "target"),
        ("target", "lex_only"),
        ("session", "chatter"),
    ] {
        core.add_edge(
            s.into(),
            t.into(),
            blob(json!({ "relationship": "RELATES" })),
        )
        .unwrap();
    }

    // Vectors (query ≈ [1,0,0]): vec_only closest, target 2nd, then expired, chatter,
    // lex_only farthest.
    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("vec_only".into(), vec![0.99, 0.10, 0.0])
        .unwrap(); // vector #1
    semantic
        .add_embedding("target".into(), vec![0.90, 0.40, 0.0])
        .unwrap(); // vector #2
    semantic
        .add_embedding("expired".into(), vec![0.85, 0.52, 0.0])
        .unwrap();
    semantic
        .add_embedding("chatter".into(), vec![0.60, 0.80, 0.0])
        .unwrap();
    semantic
        .add_embedding("lex_only".into(), vec![0.0, 0.10, 0.99])
        .unwrap(); // vector last

    // BM25 text: lex_only term-densest (#1), target has the terms less densely (#2).
    let mut text = TextIndex::in_memory().unwrap();
    text.upsert(
        "lex_only",
        "kubernetes kubernetes deployment deployment rollout rollout failure failure kubernetes deployment rollout failure crashloop",
    );
    text.upsert(
        "target",
        "a kubernetes deployment whose rollout hit a failure",
    );
    text.upsert("expired", "an old kubernetes deployment rollout note");
    text.upsert("chatter", "lunch plans and unrelated small talk");
    text.upsert("vec_only", "quantum chromodynamics lattice gauge theory");
    text.commit().unwrap();

    MemoryFx {
        view: core.analysis_snapshot(),
        semantic,
        text,
    }
}

/// A small OWL ontology: `Episode ⊑ Memory` — so an `Episode`-typed node is an inferred
/// `Memory`, and `Op::Reason <http://mem/Memory>` keeps memories, drops `Chatter`.
const MEMORY_ONT: &str = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    <http://mem/Episode> rdfs:subClassOf <http://mem/Memory> .\n";

/// THE agent-memory retrieval proof (CONCEPT:EG-KG.query.usecase-agent-memory): ONE fused plan honors semantic +
/// lexical + graph-proximity (RRF), OWL inference, and bi-temporal `AS OF` — and the
/// hybrid winner beats every single-modality ranking.
#[test]
fn fused_memory_retrieval_honors_every_signal_eg434() {
    let fx = build_memory();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_text(&fx.text);

    let seed = || Op::Scan {
        label: "Episode".into(),
    };

    // ── single-modality baselines (to prove the hybrid strictly beats them) ──
    let vec_top = execute(
        &Plan::new(vec![
            seed(),
            Op::Rank { query: query_vec() },
            Op::Limit { k: 5 },
        ]),
        &ctx,
    )
    .unwrap()
    .ids();
    assert_eq!(
        vec_top.first().map(String::as_str),
        Some("vec_only"),
        "vector-alone tops the embedding-closest episode: {vec_top:?}"
    );

    let text_top = execute(
        &Plan::new(vec![
            seed(),
            Op::RankText {
                query: QUERY_TEXT.into(),
            },
            Op::Limit { k: 5 },
        ]),
        &ctx,
    )
    .unwrap()
    .ids();
    assert_eq!(
        text_top.first().map(String::as_str),
        Some("lex_only"),
        "BM25-alone tops the term-densest episode: {text_top:?}"
    );

    // ── the fused pipeline: RRF(semantic ⊕ lexical ⊕ graph-proximity) then OWL then AS OF ──
    let fused = |ts: f64| {
        Plan::new(vec![
            seed(),
            // hybrid fusion of THREE ranked legs over the same candidate set.
            Op::FuseRrf {
                branches: vec![
                    vec![Op::Rank { query: query_vec() }], // semantic
                    vec![Op::RankText {
                        query: QUERY_TEXT.into(),
                    }], // lexical BM25
                    vec![Op::RankNodeDistance {
                        center: "session".into(),
                    }], // graph expansion / proximity to the focal node
                ],
                k: 0.0, // ⇒ eg_text::RRF_K default
            },
            // OWL: keep only inferred `Memory` members — drops the `Chatter` node.
            Op::Reason {
                target_class: "<http://mem/Memory>".into(),
                ontology: MEMORY_ONT.into(),
            },
            // bi-temporal: keep only memories LIVE at `ts`.
            Op::AsOf {
                ts,
                axis: TimeAxis::Valid,
            },
            Op::Limit { k: 5 },
        ])
    };

    // At "now" (ts=200): `expired` is gone (window closed at 100). The doubly+graph-
    // relevant `target` wins the fusion; `chatter` is reasoned out.
    let now = execute(&fused(200.0), &ctx).unwrap().ids();
    assert_eq!(
        now.first().map(String::as_str),
        Some("target"),
        "HYBRID lifts the memory relevant across ALL signals to #1: {now:?}"
    );
    // Strict hybrid win — neither single modality ranked `target` first.
    assert_ne!(
        vec_top.first(),
        now.first(),
        "the fused winner differs from vector-alone"
    );
    assert_ne!(
        text_top.first(),
        now.first(),
        "the fused winner differs from lexical-alone"
    );
    // OWL leg: the non-memory chatter node is never retrieved.
    assert!(
        !now.contains(&"chatter".to_string()),
        "the OWL Reason leg drops the non-Memory Chatter node: {now:?}"
    );
    // Temporal leg: the expired memory is absent at ts=now.
    assert!(
        !now.contains(&"expired".to_string()),
        "the AS OF leg drops the memory whose validity window has closed: {now:?}"
    );

    // ── bi-temporal re-selection: AT an earlier instant the expired memory WAS live ──
    let past = execute(&fused(50.0), &ctx).unwrap().ids();
    assert!(
        past.contains(&"expired".to_string()),
        "at ts=50 the (then-live) `expired` memory IS retrieved — AS OF re-selects by instant: {past:?}"
    );
    assert!(
        !now.contains(&"expired".to_string()) && past.contains(&"expired".to_string()),
        "same fused plan, two instants → different live sets: the bitemporal seam is real"
    );
}

/// Graph EXPANSION as an explicit leg (CONCEPT:EG-KG.query.usecase-agent-memory): a `Traverse` from the focal
/// `session` node reaches its 1-hop memory `target`, and a 2-hop expansion additionally
/// reaches `lex_only` (session → target → lex_only) — the graph-walk retrieval modality.
#[test]
fn graph_expansion_reaches_related_memories_eg434() {
    let fx = build_memory();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);

    let one_hop = execute(
        &Plan::new(vec![
            Op::Scan {
                label: "Session".into(),
            },
            Op::Traverse {
                rel: "RELATES".into(),
                min: 1,
                max: 1,
            },
        ]),
        &ctx,
    )
    .unwrap()
    .ids();
    assert!(
        one_hop.contains(&"target".to_string()),
        "1-hop expansion from the focal session reaches its related memory: {one_hop:?}"
    );

    let two_hop = execute(
        &Plan::new(vec![
            Op::Scan {
                label: "Session".into(),
            },
            Op::Traverse {
                rel: "RELATES".into(),
                min: 1,
                max: 2,
            },
        ]),
        &ctx,
    )
    .unwrap()
    .ids();
    assert!(
        two_hop.contains(&"lex_only".to_string()),
        "2-hop expansion reaches session → target → lex_only: {two_hop:?}"
    );
}

/// Confidence DECAY (CONCEPT:EG-KG.query.usecase-agent-memory, and the EG-KG.query.decay-not-foldable-finding seam gap): a memory's contribution
/// weight decays with recency on the Ebbinghaus curve — a FRESH fact outranks a STALE one
/// of equal stored confidence. Proven deterministically on the reasoner's public
/// `fact_confidence(node_confidence, age, half_life)` surface. This is NOT foldable into
/// the single fused plan above (the plan's `Op::Reason` runs decay-neutral at `now=0`), so
/// it is exercised here on the surface where decay actually lives — the documented seam
/// gap tracked as CONCEPT:EG-KG.query.decay-not-foldable-finding / docs/north_star.md.
#[test]
fn confidence_decay_reweights_by_recency_eg434() {
    use eg_rdf::owl::fact_confidence;
    let half_life = 30.0_f64;

    // A fresh memory (age 0) keeps its full stored confidence.
    let fresh = fact_confidence(1.0, 0.0, half_life);
    assert!(
        (fresh - 1.0).abs() < 1e-9,
        "a fresh fact is undecayed: {fresh}"
    );

    // At one half-life the weight halves; at two half-lives it quarters (monotone decay).
    let at_1hl = fact_confidence(1.0, half_life, half_life);
    let at_2hl = fact_confidence(1.0, 2.0 * half_life, half_life);
    assert!(
        (at_1hl - 0.5).abs() < 1e-6,
        "one half-life halves the confidence weight: {at_1hl}"
    );
    assert!(
        at_2hl < at_1hl && at_1hl < fresh,
        "confidence decays monotonically with age: {fresh} > {at_1hl} > {at_2hl}"
    );

    // A high-confidence but OLD memory can be outweighed by a lower-confidence FRESH one —
    // the epistemic point of recency decay for agent memory.
    let old_strong = fact_confidence(0.95, 3.0 * half_life, half_life);
    let new_weaker = fact_confidence(0.55, 0.0, half_life);
    assert!(
        new_weaker > old_strong,
        "a fresh weaker memory outweighs a stale strong one ({new_weaker} > {old_strong})"
    );
}

/// Confidence DECAY, now folded IN-PLAN (CONCEPT:EG-KG.query.reason-decay-in-plan, resolving
/// the EG-KG.query.decay-not-foldable-finding seam gap). The SAME `Op::Reason <Memory>` plan,
/// executed under two `now` values via `PlanCtx::with_decay`, decays each memory's inferred
/// membership confidence on the Ebbinghaus curve — so a fresh memory outranks an equally-
/// confident stale one WITHIN the fused plan, no longer only on the out-of-band reasoner
/// surface. Decay-neutral by default (no `with_decay`), so the other suites are unchanged.
#[test]
fn confidence_decay_folds_into_reason_plan() {
    // Episode ⊑ Memory; two episodes with equal stored confidence but different recency.
    const ONT: &str = "@prefix rdfs: <http://mem/> .\n\
        <http://mem/Episode> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://mem/Memory> .\n";
    let half_life = 100.0_f64;
    let t0 = 10_000_u64;
    let core = GraphCore::new();
    // `fresh`: last accessed at t0. `stale`: last accessed 2 half-lives earlier.
    core.add_node(
        "fresh".into(),
        blob(json!({"type":"Episode","confidence":1.0,"last_access": t0})),
    );
    core.add_node(
        "stale".into(),
        blob(json!({"type":"Episode","confidence":1.0,"last_access": t0 - 2 * half_life as u64})),
    );
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let plan = Plan::new(vec![Op::Reason {
        target_class: "http://mem/Memory".into(),
        ontology: ONT.into(),
    }]);

    // Bind the wall clock at t0: `fresh` (age 0) keeps confidence ~1.0; `stale` (age 2·hl)
    // decays to ~0.25 — a difference produced ENTIRELY inside the executed plan.
    let ctx = PlanCtx::new(&view, &semantic).with_decay(t0, half_life);
    let out = execute(&plan, &ctx).unwrap();
    let score = |id: &str| {
        out.rows()
            .iter()
            .find(|r| r.id == id)
            .and_then(|r| r.score)
            .unwrap_or_else(|| panic!("{id} inferred a Memory in-plan"))
    };
    let (fresh, stale) = (score("fresh"), score("stale"));
    assert!(
        (fresh - 1.0).abs() < 1e-3,
        "the fresh memory keeps full in-plan confidence: {fresh}"
    );
    assert!(
        fresh > stale && stale < 0.4,
        "the SAME Reason plan decays the stale memory below the fresh one in-plan: fresh={fresh} stale={stale}"
    );
    // `Op::Reason` results are DESCENDING by confidence, so the fresher memory ranks first.
    assert_eq!(
        out.ids().first().map(String::as_str),
        Some("fresh"),
        "in-plan decay ranks the fresher memory ahead of the stale one: {:?}",
        out.ids()
    );
}
