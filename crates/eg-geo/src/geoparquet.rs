//! **GeoParquet** metadata + geometry-encoding layer (CONCEPT:EG-306).
//!
//! [GeoParquet](https://geoparquet.org) is the modern columnar interchange format for vector
//! geospatial data: a plain Apache Parquet file whose geometry column holds **WKB** blobs, with
//! a JSON `"geo"` entry in the Parquet file-level key/value metadata describing the geometry
//! column(s) (version, primary column, per-column encoding, geometry types, bbox, CRS).
//!
//! A *full* GeoParquet reader/writer needs a Parquet page/row-group codec, which in Rust means
//! the heavy `arrow` + `parquet` crates. Those are **out of scope here** — eg-geo is a
//! dependency-light, Pi-lean leaf crate — so this module delivers the two halves that are
//! genuinely eg-geo's job and are dependency-free (beyond the already-linked `serde_json`,
//! behind the `geo-io` feature):
//!
//! 1. the **GeoParquet `"geo"` metadata model** ([`GeoParquetMetadata`]) with JSON
//!    encode/decode, matching the GeoParquet 1.0 spec; and
//! 2. the **geometry column encoding** — eg-geo [`Geometry`] ⇄ per-row WKB via [`crate::wkb`]
//!    ([`encode_wkb_column`] / [`decode_wkb_column`]), the exact bytes that go in the Parquet
//!    geometry column.
//!
//! The remaining Parquet byte-container read/write is expressed against a documented seam,
//! [`ParquetGeometryTable`], and is a **B4 follow-up** (it is the step that would pull in
//! `arrow`/`parquet`). Wiring an `arrow`/`parquet` backend to that trait — outside this crate —
//! completes GeoParquet without eg-geo taking the heavy dep.

use serde_json::{json, Map, Value};

use crate::geometry::{Bbox, Geometry};
use crate::wkb::{from_wkb, to_wkb};

/// Column geometry encoding — GeoParquet 1.0 primarily uses `"WKB"` (CONCEPT:EG-306).
pub const ENCODING_WKB: &str = "WKB";

/// The GeoParquet per-geometry-column metadata (CONCEPT:EG-306).
#[derive(Clone, Debug, PartialEq)]
pub struct GeoColumnMetadata {
    /// Geometry encoding — `"WKB"` for GeoParquet 1.0.
    pub encoding: String,
    /// The geometry types present (e.g. `["Point"]`, `["Polygon", "MultiPolygon"]`); empty ⇒
    /// unconstrained (any type), which the spec writes as `[]`.
    pub geometry_types: Vec<String>,
    /// Optional column bounding box `[minx, miny, maxx, maxy]`.
    pub bbox: Option<Bbox>,
    /// Optional CRS — kept as a verbatim JSON value (PROJJSON per the spec) or `None`
    /// (⇒ the GeoParquet default, OGC:CRS84 / lon-lat WGS84).
    pub crs: Option<Value>,
}

impl GeoColumnMetadata {
    /// A WKB-encoded column with the given geometry types and no bbox/CRS override.
    pub fn wkb(geometry_types: Vec<String>) -> Self {
        Self {
            encoding: ENCODING_WKB.to_string(),
            geometry_types,
            bbox: None,
            crs: None,
        }
    }
}

/// The GeoParquet file-level `"geo"` metadata object (CONCEPT:EG-306).
#[derive(Clone, Debug, PartialEq)]
pub struct GeoParquetMetadata {
    /// The GeoParquet spec version this describes (e.g. `"1.0.0"`).
    pub version: String,
    /// The name of the primary geometry column.
    pub primary_column: String,
    /// Per-column metadata, keyed by column name.
    pub columns: Vec<(String, GeoColumnMetadata)>,
}

impl GeoParquetMetadata {
    /// A single-column GeoParquet 1.0.0 metadata for a WKB geometry column named `column`.
    pub fn single_wkb_column(column: &str, geometry_types: Vec<String>) -> Self {
        Self {
            version: "1.0.0".to_string(),
            primary_column: column.to_string(),
            columns: vec![(column.to_string(), GeoColumnMetadata::wkb(geometry_types))],
        }
    }

    /// Serialise to the JSON value stored under the Parquet `"geo"` key/value metadata entry
    /// (CONCEPT:EG-306).
    pub fn to_json(&self) -> Value {
        let mut cols = Map::new();
        for (name, c) in &self.columns {
            let mut cm = Map::new();
            cm.insert("encoding".into(), Value::String(c.encoding.clone()));
            cm.insert(
                "geometry_types".into(),
                Value::Array(
                    c.geometry_types
                        .iter()
                        .map(|t| Value::String(t.clone()))
                        .collect(),
                ),
            );
            if let Some(b) = &c.bbox {
                cm.insert("bbox".into(), json!([b.minx, b.miny, b.maxx, b.maxy]));
            }
            if let Some(crs) = &c.crs {
                cm.insert("crs".into(), crs.clone());
            }
            cols.insert(name.clone(), Value::Object(cm));
        }
        json!({
            "version": self.version,
            "primary_column": self.primary_column,
            "columns": Value::Object(cols),
        })
    }

    /// Serialise to the JSON *string* stored in the Parquet footer (CONCEPT:EG-306).
    pub fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }

    /// Parse the GeoParquet `"geo"` metadata object (CONCEPT:EG-306).
    pub fn from_json(v: &Value) -> Result<Self, String> {
        let version = v
            .get("version")
            .and_then(Value::as_str)
            .ok_or_else(|| "GeoParquet: metadata missing \"version\"".to_string())?
            .to_string();
        let primary_column = v
            .get("primary_column")
            .and_then(Value::as_str)
            .ok_or_else(|| "GeoParquet: metadata missing \"primary_column\"".to_string())?
            .to_string();
        let cols_obj = v
            .get("columns")
            .and_then(Value::as_object)
            .ok_or_else(|| "GeoParquet: metadata missing \"columns\" object".to_string())?;
        let mut columns = Vec::with_capacity(cols_obj.len());
        for (name, cv) in cols_obj {
            let encoding = cv
                .get("encoding")
                .and_then(Value::as_str)
                .unwrap_or(ENCODING_WKB)
                .to_string();
            let geometry_types = cv
                .get("geometry_types")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let bbox = cv.get("bbox").and_then(Value::as_array).and_then(|a| {
                if a.len() >= 4 {
                    Some(Bbox::new(
                        a[0].as_f64()?,
                        a[1].as_f64()?,
                        a[2].as_f64()?,
                        a[3].as_f64()?,
                    ))
                } else {
                    None
                }
            });
            let crs = cv.get("crs").cloned();
            columns.push((
                name.clone(),
                GeoColumnMetadata {
                    encoding,
                    geometry_types,
                    bbox,
                    crs,
                },
            ));
        }
        Ok(Self {
            version,
            primary_column,
            columns,
        })
    }

    /// Parse the GeoParquet `"geo"` metadata from its JSON *string* (CONCEPT:EG-306).
    pub fn from_json_string(s: &str) -> Result<Self, String> {
        let v: Value = serde_json::from_str(s)
            .map_err(|e| format!("GeoParquet: invalid metadata JSON: {e}"))?;
        Self::from_json(&v)
    }
}

// ── geometry column encoding (eg-geo ⇄ WKB, the Parquet cell bytes) ──────────────────

/// Encode a geometry column as per-row WKB blobs (CONCEPT:EG-306) — the exact bytes a
/// GeoParquet writer places in the geometry column. `None` rows become `None` (a null cell).
pub fn encode_wkb_column(geoms: &[Option<Geometry>]) -> Vec<Option<Vec<u8>>> {
    geoms.iter().map(|g| g.as_ref().map(to_wkb)).collect()
}

/// Decode a WKB geometry column back into geometries (CONCEPT:EG-306). `None` cells (nulls)
/// decode to `None`.
pub fn decode_wkb_column(cells: &[Option<Vec<u8>>]) -> Result<Vec<Option<Geometry>>, String> {
    cells
        .iter()
        .map(|c| c.as_ref().map(|b| from_wkb(b)).transpose())
        .collect()
}

/// Derive the distinct GeoParquet `geometry_types` tag list (e.g. `"Point"`, `"MultiPolygon"`)
/// from a set of geometries (CONCEPT:EG-306) — the value that goes into
/// [`GeoColumnMetadata::geometry_types`].
pub fn geometry_types_of(geoms: &[Geometry]) -> Vec<String> {
    let mut seen = Vec::new();
    for g in geoms {
        let tag = geometry_type_tag(g);
        if !seen.iter().any(|t| t == tag) {
            seen.push(tag.to_string());
        }
    }
    seen
}

fn geometry_type_tag(g: &Geometry) -> &'static str {
    match g {
        Geometry::Point(_) => "Point",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
    }
}

// ── the documented B4 seam ──────────────────────────────────────────────────────────

/// The **seam** an actual Parquet byte-container backend implements to complete GeoParquet
/// (CONCEPT:EG-306) — a **B4 follow-up**. eg-geo owns the two halves above (the `"geo"` metadata
/// model and the WKB geometry-column encoding); a backend outside this crate (e.g. built on
/// `arrow`/`parquet`, kept out of eg-geo's dep tree) implements this trait to read/write the
/// Parquet file itself:
///
/// * on **write**: take the WKB blobs from [`encode_wkb_column`] as one binary column, attach
///   [`GeoParquetMetadata::to_json_string`] under the file's `"geo"` key/value metadata, and
///   emit Parquet bytes;
/// * on **read**: pull the `"geo"` metadata (parse with [`GeoParquetMetadata::from_json_string`])
///   and the geometry column bytes, then hand the blobs to [`decode_wkb_column`].
///
/// Keeping this a trait lets the engine's serving tiers bind a Parquet backend without eg-geo
/// itself depending on `arrow`/`parquet`.
/// The result of reading a GeoParquet file through the [`ParquetGeometryTable`] seam: the
/// `"geo"` metadata plus the primary geometry column's per-row WKB blobs (nulls as `None`).
pub type GeoParquetRead = (GeoParquetMetadata, Vec<Option<Vec<u8>>>);

pub trait ParquetGeometryTable {
    /// The backend's error type.
    type Error;

    /// Write a geometry column (per-row WKB) plus its GeoParquet `"geo"` metadata to Parquet
    /// bytes. **B4 follow-up** — not implemented in eg-geo.
    fn write_geoparquet(
        &self,
        column: &str,
        wkb_rows: &[Option<Vec<u8>>],
        meta: &GeoParquetMetadata,
    ) -> Result<Vec<u8>, Self::Error>;

    /// Read a GeoParquet file's `"geo"` metadata and its primary geometry column's WKB blobs.
    /// **B4 follow-up** — not implemented in eg-geo.
    fn read_geoparquet(&self, bytes: &[u8]) -> Result<GeoParquetRead, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{LineString, Point, Polygon};

    #[test]
    fn eg306_geoparquet_metadata_json_round_trip() {
        let mut meta = GeoParquetMetadata::single_wkb_column("geometry", vec!["Polygon".into()]);
        // Attach a bbox + a CRS to exercise the optional fields.
        if let Some((_, col)) = meta.columns.first_mut() {
            col.bbox = Some(Bbox::new(-10.0, -5.0, 10.0, 5.0));
            col.crs = Some(json!({"id": {"authority": "OGC", "code": "CRS84"}}));
        }
        let s = meta.to_json_string();
        let back = GeoParquetMetadata::from_json_string(&s).expect("parse geo metadata");
        assert_eq!(back, meta);
        assert_eq!(back.version, "1.0.0");
        assert_eq!(back.primary_column, "geometry");
        assert_eq!(back.columns[0].1.encoding, ENCODING_WKB);
        assert_eq!(
            back.columns[0].1.bbox,
            Some(Bbox::new(-10.0, -5.0, 10.0, 5.0))
        );
    }

    #[test]
    fn eg306_geoparquet_wkb_column_round_trip() {
        let geoms = vec![
            Some(Geometry::Point(Point::new(1.0, 2.0))),
            None, // a null geometry cell
            Some(Geometry::Polygon(Polygon::new(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(0.0, 0.0),
            ])))),
        ];
        let column = encode_wkb_column(&geoms);
        assert!(column[1].is_none(), "null geometry stays null");
        let back = decode_wkb_column(&column).expect("decode WKB column");
        assert_eq!(back, geoms);
    }

    #[test]
    fn eg306_geoparquet_geometry_types_distinct() {
        let geoms = vec![
            Geometry::Point(Point::new(0.0, 0.0)),
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::LineString(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 1.0),
            ])),
        ];
        assert_eq!(geometry_types_of(&geoms), vec!["Point", "LineString"]);
    }

    #[test]
    fn eg306_geoparquet_rejects_incomplete_metadata() {
        assert!(GeoParquetMetadata::from_json_string("{}").is_err());
        assert!(GeoParquetMetadata::from_json_string(r#"{"version":"1.0.0"}"#).is_err());
        assert!(GeoParquetMetadata::from_json_string("not json").is_err());
    }
}
