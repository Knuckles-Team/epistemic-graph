//! Spatial modality executor proofs (CONCEPT:EG-083).
//!
//! A small `City` layer with a `geometry` (POINT WKT) property drives the three spatial
//! surfaces end-to-end through the fused executor:
//!  * `Op::SpatialScan { layer, bbox }` — an eg-geo packed-Hilbert-R-tree bbox scan.
//!  * `Pred::SpatialWithin { column, wkt }` — geometry-within-polygon FILTER.
//!  * `Pred::SpatialDWithin { column, wkt, distance }` — within-planar-distance FILTER.
//! And a compose proof: `SpatialScan` then a spatial `Filter` in ONE plan.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

use crate::algebra::{Op, Plan, Pred};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A layer of `City` points (x, y) plus a non-spatial `Doc` distractor.
///
///   A(1,1)  B(2,2)  C(9,9)  D(5,5)  E(20,20)   ; Doc nd0 has no geometry
fn cities() -> GraphView {
    let core = GraphCore::new();
    for (id, x, y) in [
        ("A", 1.0, 1.0),
        ("B", 2.0, 2.0),
        ("C", 9.0, 9.0),
        ("D", 5.0, 5.0),
        ("E", 20.0, 20.0),
    ] {
        core.add_node(
            id.into(),
            blob(json!({ "type": "City", "geometry": format!("POINT ({x} {y})") })),
        );
    }
    // A non-spatial distractor that must never appear in a spatial result.
    core.add_node("nd0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<String> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    let mut ids = plan.execute(&c).unwrap().ids();
    ids.sort();
    ids
}

#[test]
fn spatial_scan_bbox_selects_layer() {
    let view = cities();
    // bbox [0,0,10,10] covers A,B,C,D but NOT E(20,20); nd0 has no geometry.
    let plan = Plan::new(vec![Op::SpatialScan {
        layer: "City".into(),
        bbox: [0.0, 0.0, 10.0, 10.0],
    }]);
    assert_eq!(run(&plan, &view), vec!["A", "B", "C", "D"]);
}

#[test]
fn spatial_scan_tight_bbox() {
    let view = cities();
    // A tight window around A,B only.
    let plan = Plan::new(vec![Op::SpatialScan {
        layer: "City".into(),
        bbox: [0.0, 0.0, 3.0, 3.0],
    }]);
    assert_eq!(run(&plan, &view), vec!["A", "B"]);
}

#[test]
fn spatial_within_polygon_filter() {
    let view = cities();
    // Scan the whole layer, then keep only geometries WITHIN the polygon [0,0]-[6,6].
    let plan = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [-100.0, -100.0, 100.0, 100.0],
        },
        Op::Filter {
            preds: vec![Pred::SpatialWithin {
                column: "geometry".into(),
                wkt: "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))".into(),
            }],
        },
    ]);
    // A(1,1) B(2,2) D(5,5) are inside; C(9,9) and E(20,20) are outside.
    assert_eq!(run(&plan, &view), vec!["A", "B", "D"]);
}

#[test]
fn spatial_dwithin_distance_filter() {
    let view = cities();
    // Keep cities within planar distance 3 of the origin POINT(0 0):
    //   A dist √2≈1.41 ✓, B dist √8≈2.83 ✓, D dist √50≈7.07 ✗, C,E far ✗.
    let plan = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [-100.0, -100.0, 100.0, 100.0],
        },
        Op::Filter {
            preds: vec![Pred::SpatialDWithin {
                column: "geometry".into(),
                wkt: "POINT (0 0)".into(),
                distance: 3.0,
            }],
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["A", "B"]);
}

#[test]
fn spatial_scan_then_within_compose() {
    let view = cities();
    // Compose: R-tree bbox pre-filter [0,0,10,10] (A,B,C,D), THEN within polygon
    // [0,0]-[6,6] (A,B,D) — the intersection A,B,D, in ONE plan.
    let plan = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [0.0, 0.0, 10.0, 10.0],
        },
        Op::Filter {
            preds: vec![Pred::SpatialWithin {
                column: "geometry".into(),
                wkt: "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))".into(),
            }],
        },
        Op::Limit { k: 10 },
    ]);
    assert_eq!(run(&plan, &view), vec!["A", "B", "D"]);
}

/// A spatial pred mixed with a RELATIONAL pred: only the relational leg lowers to SQL;
/// the spatial leg filters per-row. Both must apply.
#[test]
fn mixed_relational_and_spatial_filter() {
    let core = GraphCore::new();
    for (id, x, y, pop) in [
        ("A", 1.0, 1.0, 100),
        ("B", 2.0, 2.0, 900),
        ("D", 5.0, 5.0, 500),
    ] {
        core.add_node(
            id.into(),
            blob(json!({ "type": "City", "geometry": format!("POINT ({x} {y})"), "pop": pop })),
        );
    }
    let view = core.analysis_snapshot();
    // WITHIN polygon [0,0]-[6,6] (A,B,D all inside) AND pop > 400 (B,D) ⇒ B,D.
    let plan = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [-100.0, -100.0, 100.0, 100.0],
        },
        Op::Filter {
            preds: vec![
                Pred::GtNum {
                    prop: "pop".into(),
                    n: 400.0,
                },
                Pred::SpatialWithin {
                    column: "geometry".into(),
                    wkt: "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))".into(),
                },
            ],
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["B", "D"]);
}
