//! # eg-geo — the spatial / geospatial modality (CONCEPT:EG-083)
//!
//! A pure-Rust leaf crate (a sibling of `eg-ann`) giving the engine a spatial data
//! type + index with **NO GEOS/PROJ/C dependency** — the Raspberry-Pi contract. It
//! provides:
//!
//! * [`Geometry`] — the full OGC model: `Point` / `LineString` / `Polygon`
//!   (with interior rings) plus `MultiPoint` / `MultiLineString` / `MultiPolygon` /
//!   `GeometryCollection` (CONCEPT:EG-257), each with a bounding box ([`Bbox`]);
//!   serde-serializable so a geometry persists as a typed value in the engine's redb
//!   per-graph store.
//! * A hand-written [`wkt`]/EWKT codec covering every variant (`SRID=…;` tolerated).
//! * Planar spatial [`predicates`]: [`within`] / [`intersects`] / [`distance`]
//!   (Euclidean).
//! * Geodesic metrics ([`geodesic`], CONCEPT:EG-256): Haversine + Vincenty distance and
//!   spherical polygon area on WGS84 — pure-Rust, no C deps.
//! * CRS + reprojection ([`crs`], CONCEPT:EG-255): EPSG codes (WGS84 / Web-Mercator / UTM)
//!   + a pure-Rust [`reproject`] and an SRID tag ([`SridGeometry`]).
//! * DE-9IM topological [`predicates`] (CONCEPT:EG-258): `contains` / `covers` / `touches`
//!   / `crosses` / `overlaps` / `equals` / `disjoint` beyond EG-083's within/intersects.
//! * Constructive geometry [`algebra`] (CONCEPT:EG-259): `buffer` / `convex_hull` /
//!   `simplify` / `centroid` / `union` / `intersection` / `difference`.
//! * An in-house packed Hilbert [`RTree`] over a set of bounding boxes supporting
//!   `query_bbox(bbox) -> Vec<id>`.
//! * A **CRS registry** ([`registry`], CONCEPT:EG-262): [`CrsRegistry`] keyed by EPSG code,
//!   generic [`Affine`]/Helmert transforms, and an [`st_transform`] `ST_Transform` API layered
//!   over the EG-255 reprojection.
//! * A **durable STR R-tree** ([`strtree`], CONCEPT:EG-263): a bulk-loaded Sort-Tile-Recursive
//!   [`StrTree`] that is serde-serialisable (persists alongside the store) and answers range,
//!   k-nearest-neighbour and containment queries.
//! * **Format I/O** (CONCEPT:EG-264): [`geojson`] (Feature/FeatureCollection with properties;
//!   behind the `geo-io` feature), [`wkb`] (Well-Known Binary + EWKB), and [`gpx`] (GPS track /
//!   route / waypoint reader).
//! * **Web-map tiling** ([`tiles`], CONCEPT:EG-265): XYZ/TMS [`Tile`] addressing over
//!   Web-Mercator (bounds ⇄ index, y-flip) + a hand-rolled Mapbox Vector Tile
//!   ([`encode_mvt`]/[`decode_mvt`]) codec — no protobuf codegen.
//!
//! The wire algebra (`Op::SpatialScan`/`SpatialOp`, `Pred::Spatial*`) lives in
//! `eg-types::wire` (pure-serde, Pi-safe); the executor that drives THIS crate lives in
//! `eg-plan::exec` behind eg-plan's `geo` feature. This crate itself is dependency-light
//! (serde, plus serde_json for GeoJSON behind `geo-io`) and is folded into the `node`/`full`
//! serving tiers, kept OUT of `pi`.
//!
//! **Follow-up (EG-263 SpatialScan hook):** `Op::SpatialScan`'s executor in `eg-plan::exec`
//! currently rebuilds an in-memory [`RTree`] per scan. A persisted [`strtree::StrTree`] could
//! be consulted there for candidate pruning without a rebuild; that exec wiring is deferred
//! (this crate exposes the index + serde surface it needs). Other deferred format readers:
//! Shapefile, KML/KMZ and GeoParquet.

pub mod algebra;
pub mod crs;
pub mod geodesic;
#[cfg(feature = "geo-io")]
pub mod geojson;
pub mod geometry;
pub mod gpx;
pub mod predicates;
pub mod registry;
pub mod rtree;
pub mod strtree;
pub mod tiles;
pub mod wkb;
pub mod wkt;

pub use algebra::{buffer, centroid, convex_hull, difference, intersection, simplify, union};
pub use crs::{reproject, Crs, SridGeometry};
pub use geodesic::{geodesic_area, geodesic_ring_area, haversine_distance, vincenty_distance};
pub use geometry::{Bbox, Geometry, LineString, Point, Polygon};
pub use gpx::{read_gpx, Gpx};
pub use predicates::{
    contains, covers, crosses, disjoint, distance, equals, intersects, overlaps, touches, within,
};
pub use registry::{st_transform, Affine, CrsDef, CrsRegistry};
pub use rtree::RTree;
pub use strtree::StrTree;
pub use tiles::{
    decode_mvt, encode_mvt, lonlat_to_tile, MvtFeature, MvtLayer, MvtValue, Tile, DEFAULT_EXTENT,
};
pub use wkb::{from_wkb, from_wkb_srid, to_ewkb, to_wkb};
pub use wkt::{parse as parse_wkt, parse_with_srid, to_wkt};

#[cfg(feature = "geo-io")]
pub use geojson::{
    read_feature_collection, write_feature_collection, Feature, FeatureCollection,
};
