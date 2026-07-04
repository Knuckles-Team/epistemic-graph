//! **GeoJSON** (RFC 7946) reader/writer (CONCEPT:EG-KG.domains.geojson-gpx-formats).
//!
//! GeoJSON is the lingua-franca of web mapping (Leaflet, Mapbox, OpenLayers, `ogr2ogr`). This
//! module round-trips eg-geo [`Geometry`]s to and from GeoJSON, and models the `Feature` /
//! `FeatureCollection` wrappers with their **`properties`** preserved verbatim as a JSON map —
//! so attribute data survives an import/export cycle. Built on `serde_json` (already linked
//! workspace-wide; behind the crate's `geo-io` feature) — NO GDAL/OGR C dependency.
//!
//! GeoJSON coordinates are `[x, y]` = `[longitude, latitude]` (RFC 7946 §3.1.1), matching the
//! eg-geo `(x = lon, y = lat)` convention. Polygon rings are `[exterior, hole, hole, …]`.
//! GeoJSON is defined to be WGS84 (EPSG:4326); any `crs` member (old GJ 2008) is ignored.

use serde_json::{json, Map, Value};

use crate::geometry::{Geometry, LineString, Point, Polygon};

/// A GeoJSON `Feature`: an optional [`Geometry`] plus a `properties` JSON object preserved
/// verbatim (CONCEPT:EG-KG.domains.geojson-gpx-formats). `id` (RFC 7946 §3.2) is kept when present.
#[derive(Clone, Debug, PartialEq)]
pub struct Feature {
    pub geometry: Option<Geometry>,
    pub properties: Map<String, Value>,
    pub id: Option<Value>,
}

impl Feature {
    /// A feature wrapping a geometry with empty properties.
    pub fn new(geometry: Geometry) -> Self {
        Self {
            geometry: Some(geometry),
            properties: Map::new(),
            id: None,
        }
    }
}

/// A GeoJSON `FeatureCollection` (CONCEPT:EG-KG.domains.geojson-gpx-formats).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FeatureCollection {
    pub features: Vec<Feature>,
}

// ── geometry ⇄ serde_json::Value ───────────────────────────────────────────────────

/// Encode a geometry as a GeoJSON geometry object (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn geometry_to_value(g: &Geometry) -> Value {
    match g {
        Geometry::Point(p) => json!({"type": "Point", "coordinates": coord(p)}),
        Geometry::LineString(l) => {
            json!({"type": "LineString", "coordinates": line_coords(l)})
        }
        Geometry::Polygon(pg) => json!({"type": "Polygon", "coordinates": poly_coords(pg)}),
        Geometry::MultiPoint(ps) => {
            let c: Vec<Value> = ps.iter().map(coord).collect();
            json!({"type": "MultiPoint", "coordinates": c})
        }
        Geometry::MultiLineString(ls) => {
            let c: Vec<Value> = ls.iter().map(line_coords).collect();
            json!({"type": "MultiLineString", "coordinates": c})
        }
        Geometry::MultiPolygon(pgs) => {
            let c: Vec<Value> = pgs.iter().map(poly_coords).collect();
            json!({"type": "MultiPolygon", "coordinates": c})
        }
        Geometry::GeometryCollection(gs) => {
            let geoms: Vec<Value> = gs.iter().map(geometry_to_value).collect();
            json!({"type": "GeometryCollection", "geometries": geoms})
        }
    }
}

/// Decode a GeoJSON geometry object into a [`Geometry`] (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn geometry_from_value(v: &Value) -> Result<Geometry, String> {
    let ty = v
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "GeoJSON: geometry missing string \"type\"".to_string())?;
    match ty {
        "Point" => Ok(Geometry::Point(parse_coord(coords(v)?)?)),
        "LineString" => Ok(Geometry::LineString(parse_line(coords(v)?)?)),
        "Polygon" => Ok(Geometry::Polygon(parse_poly(coords(v)?)?)),
        "MultiPoint" => {
            let arr = coords(v)?
                .as_array()
                .ok_or("GeoJSON: coordinates not array")?;
            Ok(Geometry::MultiPoint(
                arr.iter().map(parse_coord).collect::<Result<_, _>>()?,
            ))
        }
        "MultiLineString" => {
            let arr = coords(v)?
                .as_array()
                .ok_or("GeoJSON: coordinates not array")?;
            Ok(Geometry::MultiLineString(
                arr.iter().map(parse_line).collect::<Result<_, _>>()?,
            ))
        }
        "MultiPolygon" => {
            let arr = coords(v)?
                .as_array()
                .ok_or("GeoJSON: coordinates not array")?;
            Ok(Geometry::MultiPolygon(
                arr.iter().map(parse_poly).collect::<Result<_, _>>()?,
            ))
        }
        "GeometryCollection" => {
            let arr = v
                .get("geometries")
                .and_then(Value::as_array)
                .ok_or("GeoJSON: GeometryCollection missing \"geometries\"")?;
            Ok(Geometry::GeometryCollection(
                arr.iter()
                    .map(geometry_from_value)
                    .collect::<Result<_, _>>()?,
            ))
        }
        other => Err(format!("GeoJSON: unknown geometry type {other:?}")),
    }
}

// ── Feature / FeatureCollection ⇄ Value ─────────────────────────────────────────────

/// Encode a [`Feature`] as a GeoJSON `Feature` object (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn feature_to_value(f: &Feature) -> Value {
    let mut obj = Map::new();
    obj.insert("type".into(), Value::String("Feature".into()));
    obj.insert(
        "geometry".into(),
        f.geometry
            .as_ref()
            .map(geometry_to_value)
            .unwrap_or(Value::Null),
    );
    obj.insert("properties".into(), Value::Object(f.properties.clone()));
    if let Some(id) = &f.id {
        obj.insert("id".into(), id.clone());
    }
    Value::Object(obj)
}

/// Decode a GeoJSON `Feature` object into a [`Feature`] (CONCEPT:EG-KG.domains.geojson-gpx-formats), preserving
/// `properties` verbatim.
pub fn feature_from_value(v: &Value) -> Result<Feature, String> {
    let geometry = match v.get("geometry") {
        None | Some(Value::Null) => None,
        Some(g) => Some(geometry_from_value(g)?),
    };
    let properties = match v.get("properties") {
        Some(Value::Object(m)) => m.clone(),
        _ => Map::new(),
    };
    Ok(Feature {
        geometry,
        properties,
        id: v.get("id").cloned(),
    })
}

/// Encode a [`FeatureCollection`] as a GeoJSON object (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn feature_collection_to_value(fc: &FeatureCollection) -> Value {
    let feats: Vec<Value> = fc.features.iter().map(feature_to_value).collect();
    json!({"type": "FeatureCollection", "features": feats})
}

/// Decode a GeoJSON `FeatureCollection` object into a [`FeatureCollection`] (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn feature_collection_from_value(v: &Value) -> Result<FeatureCollection, String> {
    let arr = v
        .get("features")
        .and_then(Value::as_array)
        .ok_or("GeoJSON: FeatureCollection missing \"features\"")?;
    Ok(FeatureCollection {
        features: arr
            .iter()
            .map(feature_from_value)
            .collect::<Result<_, _>>()?,
    })
}

// ── string convenience (the reader/writer entry points) ─────────────────────────────

/// Serialise a [`FeatureCollection`] to a GeoJSON string (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn write_feature_collection(fc: &FeatureCollection) -> String {
    feature_collection_to_value(fc).to_string()
}

/// Parse a GeoJSON string into a [`FeatureCollection`] (CONCEPT:EG-KG.domains.geojson-gpx-formats). Accepts a bare
/// `Feature` (wrapped into a one-feature collection) or a bare geometry (wrapped as a feature).
pub fn read_feature_collection(s: &str) -> Result<FeatureCollection, String> {
    let v: Value = serde_json::from_str(s).map_err(|e| format!("GeoJSON: invalid JSON: {e}"))?;
    match v.get("type").and_then(Value::as_str) {
        Some("FeatureCollection") => feature_collection_from_value(&v),
        Some("Feature") => Ok(FeatureCollection {
            features: vec![feature_from_value(&v)?],
        }),
        Some(_) => Ok(FeatureCollection {
            features: vec![Feature::new(geometry_from_value(&v)?)],
        }),
        None => Err("GeoJSON: object missing \"type\"".to_string()),
    }
}

/// Serialise a bare [`Geometry`] to a GeoJSON geometry string (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn write_geometry(g: &Geometry) -> String {
    geometry_to_value(g).to_string()
}

/// Parse a bare GeoJSON geometry string into a [`Geometry`] (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn read_geometry(s: &str) -> Result<Geometry, String> {
    let v: Value = serde_json::from_str(s).map_err(|e| format!("GeoJSON: invalid JSON: {e}"))?;
    geometry_from_value(&v)
}

// ── coordinate helpers ───────────────────────────────────────────────────────────

fn coord(p: &Point) -> Value {
    json!([p.x, p.y])
}

fn line_coords(l: &LineString) -> Value {
    Value::Array(l.points.iter().map(coord).collect())
}

fn poly_coords(pg: &Polygon) -> Value {
    let mut rings = vec![line_coords(&pg.exterior)];
    rings.extend(pg.interiors.iter().map(line_coords));
    Value::Array(rings)
}

fn coords(v: &Value) -> Result<&Value, String> {
    v.get("coordinates")
        .ok_or_else(|| "GeoJSON: geometry missing \"coordinates\"".to_string())
}

fn parse_coord(v: &Value) -> Result<Point, String> {
    let arr = v.as_array().ok_or("GeoJSON: coordinate is not an array")?;
    if arr.len() < 2 {
        return Err("GeoJSON: coordinate needs at least [x, y]".to_string());
    }
    let x = arr[0].as_f64().ok_or("GeoJSON: x not a number")?;
    let y = arr[1].as_f64().ok_or("GeoJSON: y not a number")?;
    Ok(Point::new(x, y))
}

fn parse_line(v: &Value) -> Result<LineString, String> {
    let arr = v
        .as_array()
        .ok_or("GeoJSON: line coordinates not an array")?;
    Ok(LineString::new(
        arr.iter().map(parse_coord).collect::<Result<_, _>>()?,
    ))
}

fn parse_poly(v: &Value) -> Result<Polygon, String> {
    let rings = v
        .as_array()
        .ok_or("GeoJSON: polygon coordinates not an array")?;
    if rings.is_empty() {
        return Ok(Polygon::new(LineString::new(Vec::new())));
    }
    let exterior = parse_line(&rings[0])?;
    let interiors = rings[1..]
        .iter()
        .map(parse_line)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Polygon::with_interiors(exterior, interiors))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(g: Geometry) {
        let s = write_geometry(&g);
        let back = read_geometry(&s).expect("parse GeoJSON geometry");
        assert_eq!(g, back, "GeoJSON geometry round-trip mismatch: {s}");
    }

    #[test]
    fn eg264_geojson_geometry_round_trip_all_types() {
        round_trip(Geometry::Point(Point::new(30.0, 10.0)));
        round_trip(Geometry::LineString(LineString::new(vec![
            Point::new(30.0, 10.0),
            Point::new(10.0, 30.0),
        ])));
        round_trip(Geometry::Polygon(Polygon::with_interiors(
            LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(10.0, 0.0),
                Point::new(10.0, 10.0),
                Point::new(0.0, 0.0),
            ]),
            vec![LineString::new(vec![
                Point::new(2.0, 2.0),
                Point::new(4.0, 2.0),
                Point::new(4.0, 4.0),
                Point::new(2.0, 2.0),
            ])],
        )));
        round_trip(Geometry::MultiPoint(vec![
            Point::new(1.0, 2.0),
            Point::new(3.0, 4.0),
        ]));
        round_trip(Geometry::MultiPolygon(vec![Polygon::new(LineString::new(
            vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(0.0, 0.0),
            ],
        ))]));
        round_trip(Geometry::GeometryCollection(vec![
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::LineString(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(2.0, 2.0),
            ])),
        ]));
    }

    #[test]
    fn eg264_geojson_feature_preserves_properties() {
        let mut f = Feature::new(Geometry::Point(Point::new(2.3522, 48.8566)));
        f.properties
            .insert("name".into(), Value::String("Paris".into()));
        f.properties
            .insert("population".into(), serde_json::json!(2_161_000));
        f.id = Some(serde_json::json!("city-1"));
        let fc = FeatureCollection {
            features: vec![f.clone()],
        };
        let s = write_feature_collection(&fc);
        let back = read_feature_collection(&s).expect("parse FC");
        assert_eq!(back.features.len(), 1);
        assert_eq!(back.features[0], f);
        assert_eq!(
            back.features[0].properties.get("name").unwrap(),
            &Value::String("Paris".into())
        );
    }

    #[test]
    fn eg264_geojson_reads_bare_feature_and_geometry() {
        // A bare Feature.
        let feat = r#"{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"k":"v"}}"#;
        let fc = read_feature_collection(feat).unwrap();
        assert_eq!(fc.features.len(), 1);
        assert_eq!(
            fc.features[0].geometry,
            Some(Geometry::Point(Point::new(1.0, 2.0)))
        );
        // A bare geometry.
        let geom = r#"{"type":"LineString","coordinates":[[0,0],[1,1]]}"#;
        let fc2 = read_feature_collection(geom).unwrap();
        assert_eq!(fc2.features.len(), 1);
        assert!(fc2.features[0].geometry.is_some());
    }

    #[test]
    fn eg264_geojson_null_geometry_feature() {
        let s = r#"{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}]}"#;
        let fc = read_feature_collection(s).unwrap();
        assert_eq!(fc.features[0].geometry, None);
    }

    #[test]
    fn eg264_geojson_rejects_malformed() {
        assert!(read_geometry("not json").is_err());
        assert!(read_geometry(r#"{"type":"Point"}"#).is_err()); // no coordinates
        assert!(read_geometry(r#"{"coordinates":[1,2]}"#).is_err()); // no type
    }
}
