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
//! Deferred follow-ups: GML geometry literals (`geo:gmlLiteral`) — see the code note on
//! [`geom_from_lexical`]; the RCC8 / Egenhofer topological-relation families are tracked
//! separately under CONCEPT:EG-155.

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
        _ => return None,
    })
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
