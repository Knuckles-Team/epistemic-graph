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
//! * An in-house packed Hilbert [`RTree`] over a set of bounding boxes supporting
//!   `query_bbox(bbox) -> Vec<id>`.
//!
//! The wire algebra (`Op::SpatialScan`, `Pred::SpatialWithin`/`SpatialDWithin`) lives
//! in `eg-types::wire` (pure-serde, Pi-safe); the executor that drives THIS crate
//! lives in `eg-plan::exec` behind eg-plan's `geo` feature. This crate itself is
//! dependency-light (only serde) and is folded into the `node`/`full` serving tiers,
//! kept OUT of `pi`.

pub mod geodesic;
pub mod geometry;
pub mod predicates;
pub mod rtree;
pub mod wkt;

pub use geodesic::{geodesic_area, geodesic_ring_area, haversine_distance, vincenty_distance};
pub use geometry::{Bbox, Geometry, LineString, Point, Polygon};
pub use predicates::{distance, intersects, within};
pub use rtree::RTree;
pub use wkt::{parse as parse_wkt, to_wkt};
