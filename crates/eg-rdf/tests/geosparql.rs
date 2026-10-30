//! GeoSPARQL function registration (EG-FEDERATED-QUERY-R709.1).
#![cfg(feature = "geosparql")]

use eg_rdf::geosparql::{
    eval_buffer, eval_distance, eval_envelope, eval_relation, geom_from_lexical,
    REGISTERED_FUNCTIONS,
};

// spec: EG-FEDERATED-QUERY-R709.1
#[test]
fn functions_registered() {
    let a = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
    let inside = "POINT(1 1)";
    let far = "POINT(9 9)";
    let mut seen = Vec::new();
    for name in REGISTERED_FUNCTIONS {
        match name {
            "distance" => {
                let d = eval_distance("POINT(0 0)", "POINT(3 4)", "").expect("distance");
                assert!((d - 5.0).abs() < 1e-9);
            }
            "buffer" => {
                let w = eval_buffer("POINT(0 0)", 1.0, "").expect("buffer");
                assert!(geom_from_lexical(&w).is_some());
            }
            "envelope" => {
                let w = eval_envelope("LINESTRING(1 2, 3 6)").expect("envelope");
                let env = geom_from_lexical(&w).expect("envelope is WKT");
                let b = env.bbox().unwrap();
                assert_eq!((b.minx, b.miny, b.maxx, b.maxy), (1.0, 2.0, 3.0, 6.0));
            }
            rel => {
                // Every relation is dispatched (Some), never an unknown-function None.
                assert!(eval_relation(rel, inside, a).is_some(), "{rel} unregistered");
                assert!(eval_relation(rel, far, a).is_some(), "{rel} unregistered");
            }
        }
        seen.push(name);
    }
    assert_eq!(seen.len(), 11);
    assert_eq!(eval_relation("sfWithin", inside, a), Some(true));
    assert_eq!(eval_relation("sfDisjoint", far, a), Some(true));
    assert_eq!(eval_relation("sfNope", inside, a), None);
    assert!(eval_envelope("NOTGEOM(1)").is_none());
}
