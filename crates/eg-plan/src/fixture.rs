//! A small representative graph for the planner tests: `Doc` nodes with a `year`,
//! CITES edges, distractor nodes of other types, and an embedding per Doc so the
//! vector leg has something to rank. Mirrors the real data model: msgpack node blobs
//! with a `type`, edge blobs with a `relationship`, and a `SemanticStore` keyed by
//! node id — all read off ONE `analysis_snapshot()` at a single version.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

pub struct Fixture {
    pub view: GraphView,
    pub semantic: SemanticStore,
}

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// Build the fixture. Topology (CITES chain + a branch + a non-CITES edge):
///
///   d1(2025) -CITES-> d2(2025) -CITES-> d3(2023)
///   d1(2025) -CITES-> d4(2024)
///   d2(2025) -MENTIONS-> d5(2025)         (wrong relationship — must NOT traverse)
///   old(2020, Doc) isolated; t1(type=Tool) distractor for the relational filter.
///
/// Embeddings are 4-d; the query vector is closest to d2, then d4, then d3 — so the
/// vector RANK imposes a deterministic order on the reached set.
pub fn build() -> Fixture {
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
    // query target ≈ [1,0,0,0]; d2 closest, then d4, then d3, then d5/d1 farther.
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

    Fixture {
        view: core.analysis_snapshot(),
        semantic,
    }
}

/// The query embedding used across tests.
pub fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0, 0.0]
}
