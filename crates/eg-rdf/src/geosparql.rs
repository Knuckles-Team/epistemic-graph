//! # GeoSPARQL baseline (CONCEPT:EG-261)
//!
//! The OGC **GeoSPARQL** vocabulary + geometry literals + spatial FILTER functions,
//! layered onto the native SPARQL surface (`sparql.rs`). It is a *thin lowering*: every
//! geometry type, WKT codec and spatial predicate already lives in the pure-Rust
//! `eg-geo` leaf crate (CONCEPT:EG-083/EG-256/EG-258/EG-259) — this module only
//!
//!   1. recognises the two GeoSPARQL namespaces — `geo:`
//!      (<http://www.opengis.net/ont/geosparql#>) and `geof:`
//!      (<http://www.opengis.net/def/function/geosparql/>);
//!   2. parses a `geo:wktLiteral` lexical form (an optional leading `<CRS-URI>` token,
//!      then WKT `POINT`/`LINESTRING`/`POLYGON`/…) into an [`eg_geo::Geometry`]; and
//!   3. dispatches the core `geof:` FILTER functions (`sfWithin`, `sfIntersects`,
//!      `sfContains`, `sfEquals`, `distance`, `buffer`) onto the corresponding `eg-geo`
//!      predicates/algebra — NO geometry math is re-implemented here.
//!
//! The common feature-access pattern
//! `?feature geo:hasGeometry ?g . ?g geo:asWKT ?wkt` is served for free: `?wkt` binds
//! to the `wktLiteral`'s lexical value through the ordinary BGP matcher, and the `geof:`
//! functions parse that lexical form on demand (see [`geom_from_lexical`]).
//!
//! Feature-gated behind `geosparql` (implies the crate's `sparql` + pulls `eg-geo`);
//! OUT of `pi` by construction (folded into `node`/`full` at the aggregate-feature layer).
//!
//! On top of the Simple-Features (`sf*`) relations, the **RCC8** (Region Connection
//! Calculus) and **Egenhofer** 9-intersection topological-relation families are served
//! through the SAME dispatch (CONCEPT:EG-155). Each is a boolean `geof:` FILTER function
//! over two geometry operands, composed from the `eg-geo` DE-9IM predicates
//! (CONCEPT:EG-258) — including the [`boundaries_intersect`](eg_geo::predicates::boundaries_intersect)
//! cell added under EG-155 to separate the *tangential* from the *non-tangential* part
//! relations (TPP/NTPP, `ehCoveredBy`/`ehInside`). No geometry math is re-implemented here.
//!
//! Deferred follow-ups: GML geometry literals (`geo:gmlLiteral`) — see the code note on
//! [`geom_from_lexical`].

use eg_geo::{algebra, centroid, geodesic, predicates, Geometry};

/// The GeoSPARQL **ontology** namespace (classes/properties: `geo:hasGeometry`,
/// `geo:asWKT`, `geo:wktLiteral`, …). (CONCEPT:EG-261)
pub const GEO_NS: &str = "http://www.opengis.net/ont/geosparql#";
/// The GeoSPARQL **function** namespace (`geof:sfWithin`, `geof:distance`, …). A
/// `geof:`-prefixed call parses to a `spargebra` `Function::Custom(<GEOF_NS + name>)`,
/// which the FILTER dispatch routes here. (CONCEPT:EG-261)
pub const GEOF_NS: &str = "http://www.opengis.net/def/function/geosparql/";

/// The `geo:wktLiteral` datatype IRI (CONCEPT:EG-261).
pub const WKT_LITERAL: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";
/// The `geo:gmlLiteral` datatype IRI — parsing DEFERRED (see [`geom_from_lexical`]).
pub const GML_LITERAL: &str = "http://www.opengis.net/ont/geosparql#gmlLiteral";
/// The OGC UOM base — a `geof:distance`/`geof:buffer` `units` argument is an IRI under
/// this base (e.g. `…/metre`, `…/degree`). (CONCEPT:EG-261)
pub const UOM_NS: &str = "http://www.opengis.net/def/uom/OGC/1.0/";

/// Parse a `geo:wktLiteral` lexical form into an [`eg_geo::Geometry`] (CONCEPT:EG-261).
///
/// A GeoSPARQL WKT literal MAY carry a leading absolute-URI CRS token
/// (`<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT(1 1)`); it is stripped here
/// (the default CRS84 is lon/lat, which is what `eg-geo` assumes). The remaining WKT —
/// including a tolerated EWKT `SRID=<n>;` prefix — is handed to `eg-geo`'s hand-written
/// WKT codec, so every variant (`POINT`/`LINESTRING`/`POLYGON`/`MULTI*`/`GEOMETRYCOLLECTION`)
/// is covered without re-implementing the parser.
///
/// GML literals (`geo:gmlLiteral`) are **deferred**: GeoSPARQL permits either
/// serialization, but the baseline lands WKT only. A future increment can branch on the
/// declared datatype and route `gmlLiteral` through a GML reader.
pub fn geom_from_lexical(lexical: &str) -> Option<Geometry> {
    let trimmed = lexical.trim();
    // Strip an optional leading `<CRS-URI>` token: `<uri> WKT…`.
    let wkt = match trimmed.strip_prefix('<') {
        Some(rest) => rest.split_once('>').map(|(_, tail)| tail.trim_start())?,
        None => trimmed,
    };
    eg_geo::parse_wkt(wkt).ok()
}

/// Dispatch a boolean `geof:` spatial relation given its LOCAL name (the part after
/// [`GEOF_NS`]) and the two operands' already-evaluated WKT lexical forms
/// (CONCEPT:EG-261). Returns `None` for an unknown function or unparseable geometry —
/// which fails the FILTER SAFE, matching the SPARQL error semantics of the sibling
/// built-ins.
///
/// The Simple-Features (`sf*`) relations lower directly onto `eg-geo`'s DE-9IM
/// predicates (CONCEPT:EG-258); `sfEquals` uses the spatial-equality predicate, not
/// lexical string equality.
pub fn eval_relation(local: &str, a_wkt: &str, b_wkt: &str) -> Option<bool> {
    let a = geom_from_lexical(a_wkt)?;
    let b = geom_from_lexical(b_wkt)?;
    Some(match local {
        "sfWithin" => predicates::within(&a, &b),
        "sfContains" => predicates::contains(&a, &b),
        "sfIntersects" => predicates::intersects(&a, &b),
        "sfEquals" => predicates::equals(&a, &b),
        "sfDisjoint" => predicates::disjoint(&a, &b),
        "sfTouches" => predicates::touches(&a, &b),
        "sfCrosses" => predicates::crosses(&a, &b),
        "sfOverlaps" => predicates::overlaps(&a, &b),

        // ── RCC8 (Region Connection Calculus), CONCEPT:EG-155 ──────────────────────
        // The 8 jointly-exhaustive pairwise-disjoint RCC8 base relations, each lowered
        // onto the eg-geo DE-9IM predicate set. `rcc8eq`/`rcc8dc`/`rcc8ec`/`rcc8po` map
        // directly to equals/disjoint/touches(externally-connected)/overlaps; the four
        // proper-part relations split on whether the boundaries meet (tangential) — the
        // inverses just swap the operands.
        "rcc8eq" => predicates::equals(&a, &b),
        "rcc8dc" => predicates::disjoint(&a, &b),
        "rcc8ec" => predicates::touches(&a, &b),
        "rcc8po" => predicates::overlaps(&a, &b),
        "rcc8tpp" => is_tangential_proper_part(&a, &b),
        "rcc8ntpp" => is_nontangential_proper_part(&a, &b),
        "rcc8tppi" => is_tangential_proper_part(&b, &a),
        "rcc8ntppi" => is_nontangential_proper_part(&b, &a),

        // ── Egenhofer 9-intersection relations, CONCEPT:EG-155 ─────────────────────
        // The 8 Egenhofer topological relations. equals/disjoint/meet/overlap coincide
        // with RCC8 eq/dc/ec/po; coveredBy/inside (and their inverses covers/contains)
        // are the tangential/non-tangential proper-part relations respectively.
        "ehEquals" => predicates::equals(&a, &b),
        "ehDisjoint" => predicates::disjoint(&a, &b),
        "ehMeet" => predicates::touches(&a, &b),
        "ehOverlap" => predicates::overlaps(&a, &b),
        "ehCoveredBy" => is_tangential_proper_part(&a, &b),
        "ehInside" => is_nontangential_proper_part(&a, &b),
        "ehCovers" => is_tangential_proper_part(&b, &a),
        "ehContains" => is_nontangential_proper_part(&b, &a),

        _ => return None,
    })
}

/// Is `a` a PROPER PART of `b` — spatially within `b` but not equal to it? The shared base
/// of the RCC8/Egenhofer part relations (CONCEPT:EG-155).
fn is_proper_part(a: &Geometry, b: &Geometry) -> bool {
    predicates::within(a, b) && !predicates::equals(a, b)
}

/// RCC8 `TPP` / Egenhofer `coveredBy`: `a` is a proper part of `b` whose boundary MEETS
/// `b`'s boundary — a *tangential* proper part (CONCEPT:EG-155).
fn is_tangential_proper_part(a: &Geometry, b: &Geometry) -> bool {
    is_proper_part(a, b) && predicates::boundaries_intersect(a, b)
}

/// RCC8 `NTPP` / Egenhofer `inside`: `a` is a proper part of `b` lying strictly in `b`'s
/// interior — boundaries DISJOINT, a *non-tangential* proper part (CONCEPT:EG-155).
fn is_nontangential_proper_part(a: &Geometry, b: &Geometry) -> bool {
    is_proper_part(a, b) && !predicates::boundaries_intersect(a, b)
}

/// `geof:distance(a, b, units)` (CONCEPT:EG-261): the distance between two geometries in
/// the requested unit-of-measure. A metre UOM (`…/metre`, or any `units` mentioning
/// "met") uses `eg-geo`'s WGS84 great-circle metric on the geometries' centroids
/// (lon/lat degrees → metres, CONCEPT:EG-256); otherwise the planar Euclidean
/// coordinate distance (CONCEPT:EG-083) is returned (the `…/degree` UOM). Returns `None`
/// on unparseable input.
pub fn eval_distance(a_wkt: &str, b_wkt: &str, units: &str) -> Option<f64> {
    let a = geom_from_lexical(a_wkt)?;
    let b = geom_from_lexical(b_wkt)?;
    if units_is_metric(units) {
        // Great-circle metres between representative points (centroids).
        let (pa, pb) = (centroid(&a)?, centroid(&b)?);
        Some(geodesic::haversine_distance(&pa, &pb))
    } else {
        // Planar Euclidean distance in coordinate units (CRS84 degrees).
        Some(predicates::distance(&a, &b))
    }
}

/// `geof:buffer(geom, radius, units)` (CONCEPT:EG-261): the geometry grown by `radius`,
/// returned as a WKT lexical form suitable for a `geo:wktLiteral`. Lowers onto
/// `eg-geo`'s planar `buffer` (CONCEPT:EG-259) — the `units` argument is accepted but
/// the buffer is computed in the geometry's own coordinate units (a documented baseline
/// simplification; a metre-accurate buffer needs a projected CRS). Cheap: it is a single
/// `eg-geo` call plus WKT serialization.
pub fn eval_buffer(geom_wkt: &str, radius: f64, _units: &str) -> Option<String> {
    let g = geom_from_lexical(geom_wkt)?;
    Some(eg_geo::to_wkt(&algebra::buffer(&g, radius)))
}

/// Whether a `geof:distance`/`geof:buffer` UOM IRI denotes metres (great-circle) rather
/// than the planar/degree default. Tolerant: matches the OGC `…/metre` IRI and any
/// spelling mentioning "met" (metre/meter). (CONCEPT:EG-261)
fn units_is_metric(units: &str) -> bool {
    let u = units.to_ascii_lowercase();
    u.contains("met")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EG-261: a bare WKT literal and one carrying a leading `<CRS-URI>` token both parse
    /// to the same geometry (the CRS prefix is stripped).
    #[test]
    fn eg261_wkt_literal_parse_with_and_without_crs() {
        let bare = geom_from_lexical("POINT(1 2)").expect("bare WKT parses");
        assert!(matches!(bare, Geometry::Point(_)));
        let crs = geom_from_lexical(
            "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT(1 2)",
        )
        .expect("CRS-prefixed WKT parses");
        assert!(matches!(crs, Geometry::Point(_)));
        // A polygon lexical parses too (the eg-geo codec covers every variant).
        assert!(geom_from_lexical("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))").is_some());
        // Malformed input fails SAFE (None).
        assert!(geom_from_lexical("NOTGEOM(1 2)").is_none());
    }

    /// EG-261: `geof:sfWithin` is true for a point inside the polygon, false outside —
    /// lowered onto eg-geo's planar `within` predicate.
    #[test]
    fn eg261_sfwithin_true_and_false() {
        let poly = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
        assert_eq!(eval_relation("sfWithin", "POINT(1 1)", poly), Some(true));
        assert_eq!(eval_relation("sfWithin", "POINT(5 5)", poly), Some(false));
    }

    /// EG-261: `sfIntersects`/`sfContains`/`sfEquals` route to the matching eg-geo
    /// predicate; `sfEquals` is spatial equality, not lexical.
    #[test]
    fn eg261_relations_intersects_contains_equals() {
        let poly = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";
        assert_eq!(eval_relation("sfIntersects", "POINT(2 2)", poly), Some(true));
        assert_eq!(eval_relation("sfContains", poly, "POINT(2 2)"), Some(true));
        assert_eq!(eval_relation("sfEquals", "POINT(1 1)", "POINT(1 1)"), Some(true));
        assert_eq!(eval_relation("sfEquals", "POINT(1 1)", "POINT(2 2)"), Some(false));
        // Unknown geof: local name → None (unsupported, fails SAFE).
        assert_eq!(eval_relation("sfNope", "POINT(1 1)", poly), None);
    }

    /// EG-261: `geof:distance` — planar Euclidean in coordinate units (a 3-4-5 triangle
    /// gives 5.0), and a metre UOM switches to the WGS84 great-circle metric.
    #[test]
    fn eg261_distance_planar_and_metric() {
        let planar = eval_distance("POINT(0 0)", "POINT(3 4)", "").unwrap();
        assert!((planar - 5.0).abs() < 1e-9, "planar distance {planar}");
        let metres = eval_distance(
            "POINT(0 0)",
            "POINT(0 1)",
            "http://www.opengis.net/def/uom/OGC/1.0/metre",
        )
        .unwrap();
        // ~1 degree of latitude ≈ 111 km on the mean-radius sphere.
        assert!(
            (100_000.0..120_000.0).contains(&metres),
            "one degree of latitude in metres was {metres}"
        );
    }

    /// EG-261: `geof:buffer` grows the geometry and returns a WKT lexical that re-parses.
    #[test]
    fn eg261_buffer_returns_reparseable_wkt() {
        let wkt = eval_buffer("POINT(0 0)", 1.0, "").expect("buffer produces WKT");
        assert!(geom_from_lexical(&wkt).is_some(), "buffer WKT re-parses: {wkt}");
    }

    // ── RCC8 / Egenhofer fixtures (CONCEPT:EG-155) ─────────────────────────────────
    /// The 10×10 container region used across the RCC8/Egenhofer tests.
    const BIG: &str = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";

    /// EG-155: `rcc8eq` — spatially equal regions are EQ (and only EQ); a different region
    /// is not.
    #[test]
    fn eg155_rcc8eq_true_and_false() {
        assert_eq!(eval_relation("rcc8eq", BIG, BIG), Some(true));
        assert_eq!(
            eval_relation("rcc8eq", BIG, "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))"),
            Some(false)
        );
    }

    /// EG-155: `rcc8dc` — two far-apart regions are DisConnected; touching ones are not.
    #[test]
    fn eg155_rcc8dc_true_and_false() {
        let far = "POLYGON((20 20, 21 20, 21 21, 20 21, 20 20))";
        assert_eq!(eval_relation("rcc8dc", BIG, far), Some(true));
        // Sharing an edge ⇒ connected ⇒ NOT disconnected.
        let abut = "POLYGON((10 0, 12 0, 12 10, 10 10, 10 0))";
        assert_eq!(eval_relation("rcc8dc", BIG, abut), Some(false));
    }

    /// EG-155: `rcc8ec` — regions sharing only a boundary edge are Externally Connected;
    /// overlapping ones are not.
    #[test]
    fn eg155_rcc8ec_true_and_false() {
        let abut = "POLYGON((10 0, 12 0, 12 10, 10 10, 10 0))";
        assert_eq!(eval_relation("rcc8ec", BIG, abut), Some(true));
        let overlap = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";
        assert_eq!(eval_relation("rcc8ec", BIG, overlap), Some(false));
    }

    /// EG-155: `rcc8po` — regions with overlapping interiors where neither contains the
    /// other are Partially Overlapping; a contained region is not.
    #[test]
    fn eg155_rcc8po_true_and_false() {
        let overlap = "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))";
        assert_eq!(eval_relation("rcc8po", BIG, overlap), Some(true));
        let inside = "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))";
        assert_eq!(eval_relation("rcc8po", BIG, inside), Some(false));
    }

    /// EG-155: `rcc8tpp` — a region sharing part of the container's boundary is a
    /// Tangential Proper Part; a strictly-interior region is NOT tpp (it is ntpp), and the
    /// inverse `rcc8tppi` swaps the roles.
    #[test]
    fn eg155_rcc8tpp_true_and_false() {
        // Corner square: its two edges lie along BIG's boundary ⇒ tangential.
        let corner = "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))";
        assert_eq!(eval_relation("rcc8tpp", corner, BIG), Some(true));
        assert_eq!(eval_relation("rcc8tppi", BIG, corner), Some(true));
        // Strictly interior ⇒ not tangential.
        let interior = "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))";
        assert_eq!(eval_relation("rcc8tpp", interior, BIG), Some(false));
    }

    /// EG-155: `rcc8ntpp` — a region strictly inside the container (boundaries disjoint) is
    /// a Non-Tangential Proper Part; a boundary-sharing one is NOT ntpp, and `rcc8ntppi`
    /// swaps the roles.
    #[test]
    fn eg155_rcc8ntpp_true_and_false() {
        let interior = "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))";
        assert_eq!(eval_relation("rcc8ntpp", interior, BIG), Some(true));
        assert_eq!(eval_relation("rcc8ntppi", BIG, interior), Some(true));
        // Boundary-touching corner square is tangential ⇒ NOT ntpp.
        let corner = "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))";
        assert_eq!(eval_relation("rcc8ntpp", corner, BIG), Some(false));
    }

    /// EG-155: the Egenhofer `ehMeet`/`ehCovers`/`ehInside` relations agree with their
    /// RCC8 counterparts (ec / tppi / ntpp) — meet on a shared edge, covers a tangential
    /// proper part, inside a strictly-interior region.
    #[test]
    fn eg155_egenhofer_meet_covers_inside() {
        let abut = "POLYGON((10 0, 12 0, 12 10, 10 10, 10 0))";
        assert_eq!(eval_relation("ehMeet", BIG, abut), Some(true));

        let corner = "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))";
        // BIG covers the corner square (boundary-sharing proper part).
        assert_eq!(eval_relation("ehCovers", BIG, corner), Some(true));
        assert_eq!(eval_relation("ehCoveredBy", corner, BIG), Some(true));

        let interior = "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))";
        assert_eq!(eval_relation("ehInside", interior, BIG), Some(true));
        assert_eq!(eval_relation("ehContains", BIG, interior), Some(true));
        // A strictly-interior region is inside, NOT (tangentially) covered.
        assert_eq!(eval_relation("ehCoveredBy", interior, BIG), Some(false));
    }

    // The full `FILTER(geof:rcc8ntpp(...))` SPARQL query is exercised end-to-end in the
    // `sparql` test module (`eg155_sparql_filter_rcc8ntpp_full_query`), alongside the
    // sibling EG-261 `sfWithin` query, where the graph-loading test helpers live.
}
