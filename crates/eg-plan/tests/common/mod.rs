//! Shared fixtures + helpers for the query-surface test harness (CONCEPT:EG-KG.compute.highest-roi-proof-from).
//!
//! Integration tests are EXTERNAL crates against eg-plan's public API, so they cannot
//! reach the crate-private `src/fixture.rs`. This module rebuilds the same
//! representative graph (a `Doc` CITES chain + a vector store) through the public
//! `eg_core` API, so every harness file (differential oracle, composition matrix,
//! proptest, snapshots) shares ONE fixture and ONE set of comparison helpers.

#![allow(dead_code)] // each test binary uses a different subset of these helpers.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::RowSet;
use serde_json::json;

pub fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// The canonical planner fixture (mirrors the crate-private `src/fixture.rs`):
///
/// ```text
///   d1(2025) -CITES-> d2(2025) -CITES-> d3(2023)
///   d1(2025) -CITES-> d4(2024)
///   d2(2025) -MENTIONS-> d5(2025)   (wrong relationship — must NOT traverse)
///   old(2020, Doc) isolated; t1(type=Tool) distractor for the relational filter.
/// ```
///
/// Embeddings are 4-d; the query vector `[1,0,0,0]` is closest to d2, then d4, then d3.
pub fn build_docs() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    for (id, ty, year) in [
        ("d1", "Doc", 2025),
        ("d2", "Doc", 2025),
        ("d3", "Doc", 2023),
        ("d4", "Doc", 2024),
        ("d5", "Doc", 2025),
        ("old", "Doc", 2020),
        ("t1", "Tool", 2025),
    ] {
        core.add_node(id.into(), blob(json!({ "type": ty, "year": year })));
    }
    for (s, t, rel) in [
        ("d1", "d2", "CITES"),
        ("d2", "d3", "CITES"),
        ("d1", "d4", "CITES"),
        ("d2", "d5", "MENTIONS"),
    ] {
        core.add_edge(s.into(), t.into(), blob(json!({ "relationship": rel })))
            .unwrap();
    }

    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("d1".into(), vec![0.2, 0.9, 0.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("d2".into(), vec![0.98, 0.20, 0.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("d3".into(), vec![0.80, 0.60, 0.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("d4".into(), vec![0.90, 0.44, 0.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("d5".into(), vec![0.0, 0.0, 1.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("old".into(), vec![0.0, 1.0, 0.0, 0.0])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// The query embedding the harness ranks against.
pub fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0, 0.0]
}

/// A bi-temporal Event fixture for `AS OF` interchangeability proofs. Events with
/// valid windows: e1 [100,200), e2 [150,∞), e3 [300,400).
pub fn build_events() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node(
        "e1".into(),
        blob(json!({"type":"Event","valid_from":100,"valid_until":200,"level":5})),
    );
    core.add_node(
        "e2".into(),
        blob(json!({"type":"Event","valid_from":150,"level":2})),
    );
    core.add_node(
        "e3".into(),
        blob(json!({"type":"Event","valid_from":300,"valid_until":400,"level":9})),
    );
    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("e1".into(), vec![0.8, 0.6, 0.0])
        .unwrap();
    semantic
        .add_embedding("e2".into(), vec![0.99, 0.1, 0.0])
        .unwrap();
    semantic
        .add_embedding("e3".into(), vec![0.0, 0.0, 1.0])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// The ids in `RowSet` order (preserves a RANK's meaningful order — for exact,
/// order-sensitive equality of two surfaces that lower to the SAME plan).
pub fn ids(rs: &RowSet) -> Vec<String> {
    rs.ids()
}

/// The ids as a SORTED set (order-independent — for a set-equality check between two
/// surfaces where only membership is guaranteed identical, e.g. a cost reorder or an
/// unranked FILTER/TRAVERSE result).
pub fn ids_sorted(rs: &RowSet) -> Vec<String> {
    let mut v = rs.ids();
    v.sort();
    v
}

/// The `(id, score-bits)` rows in order — a fully stable, hashable rendering of a
/// `RowSet` (f32 scores compared by bit pattern) so two surfaces can be asserted
/// byte-for-byte identical including the vector scores a RANK attaches.
pub fn stable_rows(rs: &RowSet) -> Vec<(String, Option<u32>)> {
    rs.rows()
        .iter()
        .map(|r| (r.id.clone(), r.score.map(|s| s.to_bits())))
        .collect()
}

// ── OWL fixture (reasoning source) — only when the `owl` feature is on ────────

/// A TBox + individuals loaded INTO the graph so an `Op::Reason { ontology: "" }` (the
/// form UQL `REASON <iri>` lowers to) classifies the in-graph axioms. Inferred
/// `ScholarlyWork`s {p1,p2,p4} each CITES a downstream Doc; embeddings rank the reached
/// docs c2 > c1 > c3. Mirrors the crate-private `cross_modal_seam_tests` fixture so the
/// differential proof lines up with the in-crate seam proof.
#[cfg(feature = "owl")]
pub fn build_reason_graph() -> (GraphView, SemanticStore) {
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
    semantic
        .add_embedding("<http://example.org/c1>".into(), vec![0.90, 0.44, 0.0])
        .unwrap();
    semantic
        .add_embedding("<http://example.org/c2>".into(), vec![0.99, 0.10, 0.0])
        .unwrap();
    semantic
        .add_embedding("<http://example.org/c3>".into(), vec![0.60, 0.80, 0.0])
        .unwrap();
    semantic
        .add_embedding("<http://example.org/c4>".into(), vec![0.0, 0.0, 1.0])
        .unwrap();
    // The inferred ScholarlyWorks ALSO carry embeddings so a `REASON → RANK → TRAVERSE`
    // chain (rank the members BEFORE walking CITES) retains them through the vector RANK
    // — RANK drops rows with no embedding, so a member-first rank needs member vectors.
    semantic
        .add_embedding("<http://example.org/p1>".into(), vec![0.80, 0.60, 0.0])
        .unwrap();
    semantic
        .add_embedding("<http://example.org/p2>".into(), vec![0.99, 0.10, 0.0])
        .unwrap();
    semantic
        .add_embedding("<http://example.org/p4>".into(), vec![0.50, 0.86, 0.0])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}
