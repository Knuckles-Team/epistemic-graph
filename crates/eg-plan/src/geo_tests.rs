//! Spatial modality executor proofs (CONCEPT:EG-KG.ontology.singles-concept).
//!
//! A small `City` layer with a `geometry` (POINT WKT) property drives the three spatial
//! surfaces end-to-end through the fused executor:
//!  * `Op::SpatialScan { layer, bbox }` — an eg-geo packed-Hilbert-R-tree bbox scan.
//!  * `Pred::SpatialWithin { column, wkt }` — geometry-within-polygon FILTER.
//!  * `Pred::SpatialDWithin { column, wkt, distance }` — within-planar-distance FILTER.
//!
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

// ── EG-258 DE-9IM relation filters + EG-255 reproject + EG-259 SpatialOp ──────────

/// A layer of `Parcel` polygons for the topological-relation proofs.
///   P1 = [0,0]-[4,4] ; P2 = [2,2]-[6,6] (overlaps P1) ; P3 = [10,10]-[12,12] (far)
fn parcels() -> GraphView {
    let core = GraphCore::new();
    for (id, wkt) in [
        ("P1", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"),
        ("P2", "POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))"),
        ("P3", "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))"),
    ] {
        core.add_node(
            id.into(),
            blob(json!({ "type": "Parcel", "geometry": wkt })),
        );
    }
    core.analysis_snapshot()
}

fn scan_parcels() -> Op {
    Op::SpatialScan {
        layer: "Parcel".into(),
        bbox: [-100.0, -100.0, 100.0, 100.0],
    }
}

#[test]
fn spatial_contains_filter() {
    let view = parcels();
    // Which parcels CONTAIN the point (1,1)? Only P1 (strictly inside).
    let plan = Plan::new(vec![
        scan_parcels(),
        Op::Filter {
            preds: vec![Pred::SpatialContains {
                column: "geometry".into(),
                wkt: "POINT (1 1)".into(),
            }],
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["P1"]);
}

#[test]
fn spatial_disjoint_and_overlaps_filters() {
    let view = parcels();
    // DISJOINT from P1's box [0,0]-[4,4]: only P3 (P2 overlaps it).
    let disj = Plan::new(vec![
        scan_parcels(),
        Op::Filter {
            preds: vec![Pred::SpatialDisjoint {
                column: "geometry".into(),
                wkt: "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))".into(),
            }],
        },
    ]);
    assert_eq!(run(&disj, &view), vec!["P3"]);
    // OVERLAPS P1's box: only P2 (P1 equals it — not an overlap; P3 is disjoint).
    let ovl = Plan::new(vec![
        scan_parcels(),
        Op::Filter {
            preds: vec![Pred::SpatialOverlaps {
                column: "geometry".into(),
                wkt: "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))".into(),
            }],
        },
    ]);
    assert_eq!(run(&ovl, &view), vec!["P2"]);
}

#[test]
fn reproject_keeps_tagged_rows_drops_untagged() {
    // Two cities: one WKT carries an EWKT SRID=4326 tag, the other does not.
    let core = GraphCore::new();
    core.add_node(
        "tagged".into(),
        blob(json!({ "type": "City", "geometry": "SRID=4326;POINT (2.35 48.85)" })),
    );
    core.add_node(
        "untagged".into(),
        blob(json!({ "type": "City", "geometry": "POINT (2.35 48.85)" })),
    );
    let view = core.analysis_snapshot();
    // Reproject to Web-Mercator: only the SRID-tagged row has a resolvable source CRS.
    let plan = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [-100.0, -100.0, 100.0, 100.0],
        },
        Op::Reproject {
            to_epsg: 3857,
            from_epsg: None,
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["tagged"]);
    // With an explicit `from_epsg` override, the untagged row reprojects too.
    let plan2 = Plan::new(vec![
        Op::SpatialScan {
            layer: "City".into(),
            bbox: [-100.0, -100.0, 100.0, 100.0],
        },
        Op::Reproject {
            to_epsg: 3857,
            from_epsg: Some(4326),
        },
    ]);
    assert_eq!(run(&plan2, &view), vec!["tagged", "untagged"]);
}

#[test]
fn spatial_op_buffer_and_intersection() {
    use crate::algebra::Op;
    use eg_types::wire::SpatialOpKind;
    let view = parcels();
    // Buffer succeeds for every parcel (derived geometry always exists).
    let buf = Plan::new(vec![
        scan_parcels(),
        Op::SpatialOp {
            kind: SpatialOpKind::Buffer { distance: 1.0 },
        },
    ]);
    assert_eq!(run(&buf, &view), vec!["P1", "P2", "P3"]);
    // Intersection with P1's box [0,0]-[4,4]: P1 (self) and P2 (overlap) yield a non-empty
    // polygon; P3 (disjoint) yields nothing and is DROPPED.
    let inter = Plan::new(vec![
        scan_parcels(),
        Op::SpatialOp {
            kind: SpatialOpKind::Intersection {
                wkt: "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))".into(),
            },
        },
    ]);
    assert_eq!(run(&inter, &view), vec!["P1", "P2"]);
}
