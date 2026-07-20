//! Cross-modal SEAM regression proofs (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline).
//!
//! These pin the *mid-pipeline* composition seams the audit flagged as unproven: a
//! semantic / sensor / tensor SOURCE op feeding a DOWNSTREAM ranker or reorder in ONE
//! plan. The closed-algebra promise (every `Op` is `RowSet -> RowSet`, so any op's
//! output is a legal input to any op) is only *real* if these actually compose — so
//! each test either proves the fused plan equals a hand-wired oracle, or (where a leg
//! demonstrably drops the signal a downstream op needs) is an `#[ignore]`d executable
//! spec naming the exact missing seam.
//!
//! Gated at the module level on `query` (the executor); each individual proof is
//! additionally `cfg`-gated on the modality feature it exercises (`owl` / `timeseries`
//! / `tensor`), so the module compiles under any feature subset of `query`.

#![allow(unused_imports)]

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

use crate::algebra::{Op, Plan, Pred};
use crate::exec::{execute, PlanCtx, PlanExt};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Reason → Traverse → Rank  (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline, semantic-source mid-pipeline)
// ─────────────────────────────────────────────────────────────────────────────

/// An OWL fixture whose ONLY-INFERRED `ScholarlyWork`s (`p1` Paper, `p2` Article, `p4`
/// Paper — `p3` is a Topic, excluded) each `CITES` a distinct downstream doc, plus a
/// second CITES hop, with embeddings on the reachable docs so a downstream vector
/// `Rank` imposes a deterministic order.
///
/// TBox:  `Paper ⊑ ∃about.Topic`, `∃about.Topic ⊑ ScholarlyWork`, `Article ⊑ Paper`.
/// Edges: p1 -CITES-> c1 ; p2 -CITES-> c2 ; p4 -CITES-> c3 ; c1 -CITES-> c4 .
///   So CITES 1..1 from the inferred members {p1,p2,p4} reaches {c1,c2,c3}; c4 is a
///   second hop (excluded at max=1). Query ≈ [1,0,0]: c2 > c1 > c3.
#[cfg(feature = "owl")]
fn reason_traverse_fixture() -> (GraphView, SemanticStore) {
    let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .

ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .

ex:p1 a ex:Paper .
ex:p2 a ex:Article .
ex:p3 a ex:Topic .
ex:p4 a ex:Paper .
"#;
    let core = GraphCore::new();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        eg_rdf::mapping::parse_turtle(ttl).unwrap(),
    )
    .unwrap();

    // The downstream cited docs (plain nodes) + the CITES edges from the members.
    for id in [
        "<http://example.org/c1>",
        "<http://example.org/c2>",
        "<http://example.org/c3>",
        "<http://example.org/c4>",
    ] {
        core.add_node(id.into(), blob(json!({ "type": "Doc" })));
    }
    for (s, t) in [
        ("<http://example.org/p1>", "<http://example.org/c1>"),
        ("<http://example.org/p2>", "<http://example.org/c2>"),
        ("<http://example.org/p4>", "<http://example.org/c3>"),
        ("<http://example.org/c1>", "<http://example.org/c4>"),
    ] {
        core.add_edge(s.into(), t.into(), blob(json!({ "relationship": "CITES" })))
            .unwrap();
    }

    let mut semantic = SemanticStore::new();
    // query ≈ [1,0,0]: c2 closest, then c1, then c3; c4 is orthogonal (never reached).
    semantic.add_embedding("<http://example.org/c1>".into(), vec![0.90, 0.44, 0.0]);
    semantic.add_embedding("<http://example.org/c2>".into(), vec![0.99, 0.10, 0.0]);
    semantic.add_embedding("<http://example.org/c3>".into(), vec![0.60, 0.80, 0.0]);
    semantic.add_embedding("<http://example.org/c4>".into(), vec![0.0, 0.0, 1.0]);

    (core.analysis_snapshot(), semantic)
}

/// THE seam proof: `[Reason{ScholarlyWork} |> Traverse{CITES} |> Rank{vec} |> Limit]`
/// in ONE plan == (reason for the inferred members) then (BFS the CITES frontier) then
/// (an independent kNN over the reached set). Byte-identical ordered ids ⇒ the
/// OWL-inferred candidate set flows through the graph traversal into the SAME vector
/// ranker with no impedance mismatch — the reasoner's confidence score is *intentionally*
/// superseded by the downstream vector `Rank` (which re-scores the reached set), so the
/// mid-pipeline signal that matters (vector similarity of the reached docs) survives.
#[cfg(feature = "owl")]
#[test]
fn reason_then_traverse_then_rank_equals_separate() {
    use std::collections::HashSet;

    let (view, semantic) = reason_traverse_fixture();
    let target = "<http://example.org/ScholarlyWork>";
    let q = vec![1.0f32, 0.0, 0.0];

    // ── Fused: ONE plan. Reason seeds inferred members; Traverse walks CITES; Rank orders. ──
    let plan = Plan::new(vec![
        Op::Reason {
            target_class: target.into(),
            ontology: String::new(),
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 1,
        },
        Op::Rank { query: q.clone() },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let fused_ids: Vec<String> = execute(&plan, &ctx).unwrap().ids();

    // ── Separate oracle: reasoner ∘ BFS ∘ kNN, wired by hand. ──
    let triples = eg_rdf::owl::tbox_triples_from_view(&view);
    let mut reasoner = eg_rdf::owl::Reasoner::from_triples(&triples);
    let cls = reasoner.classify();
    let asserted = eg_rdf::owl::asserted_types_from_view(&view, "http://example.org/").unwrap();
    let members: Vec<String> = eg_rdf::owl::instances_of(&cls, &asserted, target);
    let reached = crate::exec::bfs_reached(&view, &members, "CITES", 1, 1);
    let reached_set: HashSet<&str> = reached.iter().map(String::as_str).collect();
    let ranked = semantic.semantic_search(&q, 64);
    let separate: Vec<String> = ranked
        .into_iter()
        .filter(|(id, _)| reached_set.contains(id.as_str()))
        .map(|(id, _)| id)
        .take(10)
        .collect();

    assert_eq!(
        fused_ids, separate,
        "fused Reason→Traverse→Rank must equal separate reasoner∘BFS∘kNN"
    );
    // Concretely: reached {c1,c2,c3} (c4 is a 2nd hop, excluded); ranked c2>c1>c3.
    assert_eq!(
        fused_ids,
        vec![
            "<http://example.org/c2>".to_string(),
            "<http://example.org/c1>".to_string(),
            "<http://example.org/c3>".to_string(),
        ],
        "vector rank over the CITES frontier of the inferred ScholarlyWorks"
    );
}

/// Plan-SHAPE proof (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline): the cost reorder still picks the winning
/// (Filter,Rank) order when a `Reason` SOURCE leg SEEDS the plan — the reasoning leg
/// stays pinned at the front (it is the source, not a commuting member of the pair)
/// while the adjacent relational/vector pair reorders per selectivity regime. Extends
/// the `cost.rs` reorder to a reasoning-seeded plan shape.
#[cfg(feature = "owl")]
#[test]
fn reason_seeded_reorder_picks_winner() {
    use crate::cost::{CostModel, Order, Stats};

    let reason = Op::Reason {
        target_class: "<http://example.org/ScholarlyWork>".into(),
        ontology: String::new(),
    };
    let filter = Op::Filter {
        preds: vec![Pred::GtNum {
            prop: "year".into(),
            n: 2022.0,
        }],
    };
    let rank = Op::Rank {
        query: vec![1.0, 0.0, 0.0],
    };
    let plan = vec![reason.clone(), filter.clone(), rank.clone()];

    // Selective predicate → filter-first; broad → vector-first. In BOTH the Reason
    // source leg is index 0 (untouched); the adjacent (Filter,Rank) pair at 1,2 reorders.
    let selective = Stats::estimate(10_000, 0.01, 10, 64);
    let broad = Stats::estimate(10_000, 0.98, 10, 64);
    assert_eq!(CostModel::order(&selective), Order::FilterFirst);
    assert_eq!(CostModel::order(&broad), Order::VectorFirst);

    let sel_plan = CostModel::place_narrower(plan.clone(), 1, 2, true);
    assert!(
        matches!(sel_plan[0], Op::Reason { .. }),
        "the Reason seed leg stays at the front"
    );
    assert!(
        matches!(sel_plan[1], Op::Filter { .. }) && matches!(sel_plan[2], Op::Rank { .. }),
        "selective → filter-first behind the reasoning seed"
    );

    let broad_plan = CostModel::place_narrower(plan, 1, 2, false);
    assert!(
        matches!(broad_plan[0], Op::Reason { .. }),
        "the Reason seed leg stays at the front"
    );
    assert!(
        matches!(broad_plan[1], Op::Rank { .. }) && matches!(broad_plan[2], Op::Filter { .. }),
        "broad → vector-first behind the reasoning seed"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// REASON <iri> mid-plan + string-type↔IRI-class bridge  (CONCEPT:EG-KG.query.reason-iri-parses-angle / EG-376)
// ─────────────────────────────────────────────────────────────────────────────

/// Two nodes carrying a BARE string `type` (no IRI, no `rdf:type` edge): `mm1` a
/// `Sensor`, `w1` a `Widget`. The ontology declares `<http://ex/Sensor> ⊑
/// <http://ex/Device>` (but says NOTHING about Widget). So `REASON <http://ex/Device>`
/// must include `mm1` ONLY IF the string type `"Sensor"` is bridged to the class IRI
/// `<http://ex/Sensor>` (the target's namespace + the local name) — the string→IRI bridge.
#[cfg(feature = "owl")]
const DEVICE_ONT: &str = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    <http://ex/Sensor> rdfs:subClassOf <http://ex/Device> .\n";

#[cfg(feature = "owl")]
fn string_typed_devices() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node("mm1".into(), blob(json!({ "type": "Sensor" })));
    core.add_node("w1".into(), blob(json!({ "type": "Widget" })));
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("mm1".into(), vec![1.0, 0.0, 0.0]);
    semantic.add_embedding("w1".into(), vec![1.0, 0.0, 0.0]);
    (core.analysis_snapshot(), semantic)
}

/// SEAM (CONCEPT:EG-KG.ontology.string-type-iri-class) — the string-type↔IRI-class bridge: a `REASON <http://ex/Device>`
/// SOURCE op includes the string-typed `{"type":"Sensor"}` node (bridged to
/// `<http://ex/Sensor>` ⊑ `<http://ex/Device>`) and EXCLUDES `{"type":"Widget"}` (no
/// subclass path to Device). Without the bridge a bare `"Sensor"` would never match the
/// IRI class, so this is the load-bearing proof.
#[cfg(feature = "owl")]
#[test]
fn reason_iri_bridges_string_typed_node() {
    let (view, semantic) = string_typed_devices();
    let plan = Plan::new(vec![Op::Reason {
        target_class: "<http://ex/Device>".into(),
        ontology: DEVICE_ONT.into(),
    }]);
    let ids = execute(&plan, &PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();
    assert_eq!(
        ids,
        vec!["mm1".to_string()],
        "REASON <http://ex/Device> must include the string-typed Sensor (bridged) and \
         exclude the Widget"
    );
}

/// SEAM (CONCEPT:EG-KG.query.reason-iri-parses-angle) — `REASON <iri>` composing MID-PIPELINE: a `Scan → Rank → REASON
/// <http://ex/Device>` intersects the vector-ranked candidate set with the reasoned
/// members of the explicit IRI class. `Scan(Sensor)` seeds the string-typed `mm1`, which
/// survives the mid-plan `REASON <http://ex/Device>` (bridged); a `Scan(Widget) → REASON
/// <http://ex/Device>` yields nothing (the non-member is filtered out mid-plan).
#[cfg(feature = "owl")]
#[test]
fn reason_iri_midplan_filters_string_typed() {
    let (view, semantic) = string_typed_devices();

    // Sensor survives the mid-plan REASON <Device> filter (kept, in rank order).
    let kept = Plan::new(vec![
        Op::Scan {
            label: "Sensor".into(),
        },
        Op::Rank {
            query: vec![1.0, 0.0, 0.0],
        },
        Op::Reason {
            target_class: "<http://ex/Device>".into(),
            ontology: DEVICE_ONT.into(),
        },
    ]);
    let kept_ids = execute(&kept, &PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();
    assert_eq!(
        kept_ids,
        vec!["mm1".to_string()],
        "the string-typed Sensor survives mid-plan REASON <http://ex/Device>"
    );

    // Widget is dropped by the same mid-plan filter (not a Device).
    let dropped = Plan::new(vec![
        Op::Scan {
            label: "Widget".into(),
        },
        Op::Reason {
            target_class: "<http://ex/Device>".into(),
            ontology: DEVICE_ONT.into(),
        },
    ]);
    let dropped_ids = execute(&dropped, &PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();
    assert!(
        dropped_ids.is_empty(),
        "the Widget is not a Device — mid-plan REASON must filter it out, got {dropped_ids:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// SensorFuse → Rank  (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline, timeseries-source mid-pipeline)
// ─────────────────────────────────────────────────────────────────────────────

const NS: i64 = 1_000_000_000;

/// Three sensor layers (imu/gps/lidar) at different rates — the sensor_fuse_tests
/// fixture — PLUS an embedding per fused-instant id so a downstream vector `Rank`
/// re-orders the fused series (the fused row `id` is the aligned ts as a string).
#[cfg(feature = "timeseries")]
fn sensors_with_instant_embeddings() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    for i in 0..5i64 {
        core.add_node(
            format!("imu{i}"),
            blob(json!({ "type": "imu", "valid_from": i * NS, "value": i as f64 })),
        );
    }
    for t in [0i64, 2, 4] {
        core.add_node(
            format!("gps{t}"),
            blob(json!({ "type": "gps", "valid_from": t * NS, "value": 100.0 + t as f64 })),
        );
    }
    core.add_node(
        "lidar0".into(),
        blob(json!({ "type": "lidar", "valid_from": 0, "tensor": "blob://frame0" })),
    );

    let mut semantic = SemanticStore::new();
    // Instant ids are the aligned ts strings "0","1e9",... . query ≈ [1,0]: ts=2e9
    // closest, then 1e9, 3e9, 0, 4e9 — an order that is NOT the natural time order,
    // so a passing assert proves the vector Rank actually re-ordered the fused series.
    let emb: [(&str, [f32; 2]); 5] = [
        ("0", [0.30, 0.95]),
        (&NS.to_string(), [0.80, 0.60]),
        (&(2 * NS).to_string(), [0.99, 0.10]),
        (&(3 * NS).to_string(), [0.60, 0.80]),
        (&(4 * NS).to_string(), [0.10, 0.99]),
    ];
    for (id, v) in emb {
        semantic.add_embedding(id.to_string(), v.to_vec());
    }
    (core.analysis_snapshot(), semantic)
}

/// SEAM proof: a `SensorFuse` SOURCE op's fused series feeds a downstream vector `Rank`
/// in ONE plan — the fused-instant rows compose into the ranker exactly like any other
/// RowSet, and the ranker re-orders them by similarity (NOT the natural time order).
/// This documents that the executor does NOT drop the fused rows mid-pipeline (the
/// audit's open question): downstream composition works.
#[cfg(feature = "timeseries")]
#[test]
fn sensorfuse_then_rank_composes() {
    let (view, semantic) = sensors_with_instant_embeddings();
    let q = vec![1.0f32, 0.0];

    let plan = Plan::new(vec![
        Op::SensorFuse {
            streams: vec!["imu".into(), "gps".into(), "lidar".into()],
            tolerance_ns: (10 * NS) as u64,
        },
        Op::Rank { query: q.clone() },
        Op::Limit { k: 5 },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let fused_ids: Vec<String> = plan.execute(&ctx).unwrap().ids();

    // Oracle: fuse yields the 5 union-clock instants; a kNN over their embeddings
    // re-orders by similarity to q. Expected: 2e9 > 1e9 > 3e9 > 0 > 4e9.
    assert_eq!(
        fused_ids,
        vec![
            (2 * NS).to_string(),
            NS.to_string(),
            (3 * NS).to_string(),
            "0".to_string(),
            (4 * NS).to_string(),
        ],
        "the fused series composes into a downstream vector Rank (re-ordered, not time order)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// TensorScan → Rank  (CONCEPT:EG-KG.ingest.timeseries-source-mid-pipeline, tensor-source mid-pipeline)
// ─────────────────────────────────────────────────────────────────────────────

/// A `Frame` layer of tensor-bearing nodes (F1,F2,F3) plus an embedding per frame so a
/// downstream vector `Rank` re-orders the scanned set.
#[cfg(feature = "tensor")]
fn frames_with_embeddings() -> (GraphView, SemanticStore) {
    use eg_tensor::{Buffer, Tensor};
    let core = GraphCore::new();
    let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
    let tv = serde_json::to_value(&t).unwrap();
    for id in ["F1", "F2", "F3"] {
        core.add_node(id.into(), blob(json!({ "type": "Frame", "tensor": tv })));
    }
    core.add_node("nd0".into(), blob(json!({ "type": "Doc" })));

    let mut semantic = SemanticStore::new();
    // query ≈ [1,0]: F2 > F1 > F3.
    semantic.add_embedding("F1".into(), vec![0.90, 0.44]);
    semantic.add_embedding("F2".into(), vec![0.99, 0.10]);
    semantic.add_embedding("F3".into(), vec![0.60, 0.80]);
    (core.analysis_snapshot(), semantic)
}

/// SEAM proof: a `TensorScan` SOURCE op's tensor-bearing rows feed a downstream vector
/// `Rank` in ONE plan — the scanned frames compose into the ranker and are re-ordered by
/// similarity, proving the tensor source composes downstream (not dropped).
#[cfg(feature = "tensor")]
#[test]
fn tensorscan_then_rank_composes() {
    let (view, semantic) = frames_with_embeddings();
    let q = vec![1.0f32, 0.0];

    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::Rank { query: q.clone() },
        Op::Limit { k: 5 },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let ids: Vec<String> = plan.execute(&ctx).unwrap().ids();
    assert_eq!(
        ids,
        vec!["F2".to_string(), "F1".to_string(), "F3".to_string()],
        "the tensor-scanned frames compose into a downstream vector Rank (re-ordered)"
    );
}
