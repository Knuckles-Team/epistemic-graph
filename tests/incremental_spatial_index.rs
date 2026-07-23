//! L37 — incremental ≡ rebuild equivalence for the server-layer maintained spatial
//! index (`GraphSpatialIndex`) wired into the per-graph `IndexManager` seam
//! (CONCEPT:EG-KG.storage.incremental-spatial). Mirrors `tests/incremental_server_indexes.rs`'s
//! text/temporal equivalence proofs, applied to the spatial index: after a batch of
//! writes touching `type`/`geometry`, a bbox query returns the IDENTICAL id set to a
//! full-rebuild baseline built from the live nodes — including the "geometry-only CAS
//! update reuses the node's last-known layer" case `GraphSpatialIndex::apply_delta`
//! documents.
#![cfg(feature = "geo")]

use epistemic_graph::graph::GraphCore;
use epistemic_graph::index::{ChangeSet, NodeChange, SecondaryIndex};
use epistemic_graph::server::secondary_indexes::GraphSpatialIndex;

fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn point(x: f64, y: f64) -> String {
    format!("POINT ({x} {y})")
}

/// After ADD + a subsequent UPDATE batch (one geometry move via a geometry-only CAS
/// delta, one removal), a bbox query through the INCREMENTAL index must return the
/// EXACT same id set as a fresh index `full_rebuild`-ing from the final live node set.
#[test]
fn spatial_incremental_equals_rebuild() {
    let core = GraphCore::new();
    let incr = GraphSpatialIndex::new();
    core.register_index(Box::new(GraphSpatialIndex::new()));
    assert!(
        core.wants_change_content(),
        "spatial index flips content capture (needs_content=true)"
    );

    // Seed A..E in the "City" layer, plus a non-spatial "Doc" distractor.
    let mut cs = ChangeSet::new();
    for (id, x, y) in [
        ("A", 1.0, 1.0),
        ("B", 2.0, 2.0),
        ("C", 9.0, 9.0),
        ("D", 5.0, 5.0),
        ("E", 20.0, 20.0),
    ] {
        let blob = props(serde_json::json!({ "type": "City", "geometry": point(x, y) }));
        core.add_node(id.to_string(), blob.clone());
        cs.added_nodes
            .push(NodeChange::with_properties(id.to_string(), blob));
    }
    core.add_node(
        "nd0".into(),
        props(serde_json::json!({ "type": "Doc", "year": 2025 })),
    );
    cs.added_nodes.push(NodeChange::with_properties(
        "nd0".into(),
        props(serde_json::json!({ "type": "Doc", "year": 2025 })),
    ));
    incr.apply_delta(&core, &cs).unwrap();

    // Second batch: move D via a GEOMETRY-ONLY CAS update (no `type` in the delta — the
    // "reuse the node's last-known layer" path), and remove E entirely.
    let d_new_geom = point(0.5, 0.5);
    core.compare_and_set_fields(
        "D",
        &serde_json::Map::new(),
        serde_json::json!({ "geometry": d_new_geom })
            .as_object()
            .unwrap(),
    );
    core.remove_node("E".to_string());
    let mut cs2 = ChangeSet::new();
    cs2.updated_nodes.push(NodeChange::with_properties(
        "D".into(),
        props(serde_json::json!({ "geometry": d_new_geom })), // NOTE: no "type" key
    ));
    cs2.removed_nodes.push("E".into());
    incr.apply_delta(&core, &cs2).unwrap();

    // Baseline: a fresh index rebuilt from the live nodes of `core`.
    let baseline = GraphSpatialIndex::new();
    baseline.full_rebuild(&core).unwrap();

    for bbox in [
        [0.0, 0.0, 10.0, 10.0],
        [0.0, 0.0, 3.0, 3.0],
        [-100.0, -100.0, 100.0, 100.0],
    ] {
        let mut a = incr.query_bbox("City", bbox);
        let mut b = baseline.query_bbox("City", bbox);
        a.sort();
        b.sort();
        assert_eq!(
            a, b,
            "spatial incremental vs rebuild diverged for bbox {bbox:?}"
        );
    }

    // E was removed in both.
    assert!(!incr
        .query_bbox("City", [-100.0, -100.0, 100.0, 100.0])
        .contains(&"E".to_string()));
    assert!(!baseline
        .query_bbox("City", [-100.0, -100.0, 100.0, 100.0])
        .contains(&"E".to_string()));

    // D moved to (0.5, 0.5) — a tight bbox around the origin now hits it (it did NOT
    // before the move), proving the geometry-only CAS update actually re-indexed D
    // under its REUSED "City" layer rather than silently no-op'ing.
    let near_origin = [0.0, 0.0, 1.0, 1.0];
    assert!(incr
        .query_bbox("City", near_origin)
        .contains(&"D".to_string()));
    assert!(baseline
        .query_bbox("City", near_origin)
        .contains(&"D".to_string()));
}

/// A removed node disappears from EVERY layer (the index has no reverse `id -> layer`
/// index at removal time, so it must scan/clear from `items` directly regardless of
/// which layer it was last in).
#[test]
fn spatial_removal_clears_regardless_of_layer_lookup_path() {
    let core = GraphCore::new();
    let ix = GraphSpatialIndex::new();
    core.register_index(Box::new(GraphSpatialIndex::new()));

    let blob = props(serde_json::json!({ "type": "City", "geometry": point(1.0, 1.0) }));
    core.add_node("A".into(), blob.clone());
    let mut cs = ChangeSet::new();
    cs.added_nodes
        .push(NodeChange::with_properties("A".into(), blob));
    ix.apply_delta(&core, &cs).unwrap();
    assert!(ix
        .query_bbox("City", [-10.0, -10.0, 10.0, 10.0])
        .contains(&"A".to_string()));

    core.remove_node("A".to_string());
    let mut cs2 = ChangeSet::new();
    cs2.removed_nodes.push("A".into());
    ix.apply_delta(&core, &cs2).unwrap();
    assert!(!ix
        .query_bbox("City", [-10.0, -10.0, 10.0, 10.0])
        .contains(&"A".to_string()));
}

/// An UPDATE that touches NEITHER `type` nor `geometry` is a no-op — the prior entry
/// (if any) is left exactly as it was, mirroring the text index's identical posture for
/// a CAS update that never touched its own field.
#[test]
fn spatial_update_touching_neither_key_is_a_noop() {
    let core = GraphCore::new();
    let ix = GraphSpatialIndex::new();
    core.register_index(Box::new(GraphSpatialIndex::new()));

    let blob =
        props(serde_json::json!({ "type": "City", "geometry": point(1.0, 1.0), "pop": 100 }));
    core.add_node("A".into(), blob.clone());
    let mut cs = ChangeSet::new();
    cs.added_nodes
        .push(NodeChange::with_properties("A".into(), blob));
    ix.apply_delta(&core, &cs).unwrap();

    // CAS touches ONLY `pop` — neither `type` nor `geometry` present in the delta.
    core.compare_and_set_fields(
        "A",
        &serde_json::Map::new(),
        serde_json::json!({ "pop": 200 }).as_object().unwrap(),
    );
    let mut cs2 = ChangeSet::new();
    cs2.updated_nodes.push(NodeChange::with_properties(
        "A".into(),
        props(serde_json::json!({ "pop": 200 })),
    ));
    ix.apply_delta(&core, &cs2).unwrap();

    // A is still findable at its ORIGINAL position — untouched by the no-op update.
    assert!(ix
        .query_bbox("City", [0.0, 0.0, 2.0, 2.0])
        .contains(&"A".to_string()));
}
