//! # eg-geo — the spatial / geospatial modality (CONCEPT:EG-083)
//!
//! A pure-Rust leaf crate (a sibling of `eg-ann`) giving the engine a spatial data
//! type + index with **NO GEOS/PROJ/C dependency** — the Raspberry-Pi contract. It
//! provides:
//!
//! * [`Geometry`] — `Point` / `LineString` / `Polygon`, each with a bounding box
//!   ([`Bbox`]); serde-serializable so a geometry persists as a typed value in the
//!   engine's redb per-graph store.
//! * A hand-written [`wkt`] codec (`POINT`/`LINESTRING`/`POLYGON`).
//! * Planar spatial [`predicates`]: [`within`] / [`intersects`] / [`distance`]
//!   (Euclidean for v1; a geodesic metric is a documented follow-up).
//! * An in-house packed Hilbert [`RTree`] over a set of bounding boxes supporting
//!   `query_bbox(bbox) -> Vec<id>`.
//!
//! The wire algebra (`Op::SpatialScan`, `Pred::SpatialWithin`/`SpatialDWithin`) lives
//! in `eg-types::wire` (pure-serde, Pi-safe); the executor that drives THIS crate
//! lives in `eg-plan::exec` behind eg-plan's `geo` feature. This crate itself is
//! dependency-light (only serde) and is folded into the `node`/`full` serving tiers,
//! kept OUT of `pi`.

pub mod geometry;
pub mod predicates;
pub mod rtree;
pub mod wkt;

pub use geometry::{Bbox, Geometry, LineString, Point, Polygon};
pub use predicates::{distance, intersects, within};
pub use rtree::RTree;
pub use wkt::{parse as parse_wkt, to_wkt};
