# GIS / spatial interface

epistemic-graph carries a native **geospatial modality** — a pure-Rust `eg-geo` crate (geo/geo-types + an
in-house packed Hilbert / STR R-tree; **no GEOS / PROJ C deps**) behind the `geo` feature (folded into
node/full, out of the lean `pi` tier). Geometries persist as a typed value in the redb per-graph store, a
durable R-tree indexes them, and spatial predicates compose with graph traversal / vector / SQL in one
plan. GeoSPARQL surfaces it over SPARQL; SQL `ST_*` functions surface it over pgwire.

> Status snapshot: geometry model + WKT/WKB/GeoJSON/GPX I/O (EG-083/257/264), DE-9IM + RCC8/Egenhofer
> relations (EG-258/155), constructive algebra (EG-259), CRS + reprojection + geodesic distance
> (EG-255/262/256), a durable R-tree index (EG-263), map tiles (EG-265), routing/isochrones/TSP (EG-266),
> and map-based task tracking (EG-267) are shipped. See the [capability matrix](../capabilities.md).

## Geometry model (EG-083/257)

The full OGC geometry set: `Point`, `LineString`, `Polygon` (with interior rings / holes),
`MultiPoint`, `MultiLineString`, `MultiPolygon`, and `GeometryCollection`, with **EWKT** parse/serialize.
Stored as a typed redb value with a per-shard durable R-tree (EG-263) consulted by `Op::SpatialScan` for
selectivity.

## Format I/O (EG-264)

Reader/writer for **GeoJSON** (Feature / FeatureCollection), **WKB** (binary geometry), and **GPX** tracks
— round-tripping eg-geo geometries. (Shapefile / KML / GeoParquet are documented follow-ups.)

## Predicates & relations (EG-258/155)

- **Core** (EG-083): `within` / `intersects` / `dwithin` as `Pred::SpatialWithin` / `SpatialDWithin`.
- **DE-9IM** (EG-258): the full relation matrix — `contains` / `covers` / `touches` / `crosses` /
  `overlaps` / `equals` / `disjoint`, as additional `Pred::Spatial*` variants.
- **RCC8 + Egenhofer** (EG-155): the OGC topological-relation families, lowered onto the DE-9IM
  intersection matrix (surfaced through GeoSPARQL, see below).

## Constructive algebra (EG-259)

Buffer, convex hull, union / intersection / difference, simplify, and centroid — the constructive ops for
urban-planning / logistics — executed via `Op::SpatialOp { kind }` in eg-plan (pure-Rust).

## CRS, reprojection & geodesics (EG-255/262/256)

A coordinate-reference-system registry (`crs` module — EPSG codes incl. 4326 WGS84 / 3857 Web-Mercator +
proj params) and pure-Rust reprojection (geographic ↔ Web-Mercator + affine / Helmert), so geometries
carry a CRS and `ST_Transform` / GeoSPARQL CRS-URIs reproject correctly — no PROJ C dependency. A
geometry's CRS tag selects planar-vs-geodesic distance: great-circle / Haversine + Vincenty distance and
geodesic area (EG-256) give accurate real-world measures for logistics / urban planning.

## Map tiling (EG-265)

Slippy-map tile addressing (XYZ / TMS `z/x/y` ↔ lon/lat/bbox via Web-Mercator) + **Mapbox Vector Tile**
(MVT protobuf-lite) encoding of features clipped to a tile, so a web map (Leaflet / MapLibre) renders the
graph's spatial data.

## Routing, isochrones & TSP (EG-266)

Graph routing over a spatial network: weighted shortest path (Dijkstra / A* with a geo heuristic),
isochrones (reachability within a cost budget), and a nearest-neighbour + 2-opt TSP tour — the
logistics / urban-planning primitives, reusing the engine's graph traversal.

## Map-based task tracking (EG-267)

Geo-anchored tasks: a `:GeoTask` node with a location + status + optional service-area geometry, spatial
queries (tasks within a bbox / polygon, nearest-N to a point, tasks along a route), and assignment to the
nearest resource — the field-ops / urban-planning task layer over the spatial store.

## Reaching it

- **SQL** (pgwire / in-engine): `st_within` / `st_distance` / `ST_Transform` etc. compose with the node
  store — see [sql](sql.md).
- **GeoSPARQL** (feature `geosparql`): the `geo:` / `geof:` vocab, WKT/GML literals, and
  `sfWithin`/`sfIntersects`/`distance` + the RCC8/Egenhofer function families — see
  [sparql](sparql.md#geosparql--spatial-sparql-eg-261155-feature-geosparql).
- **UQL**: `Op::SpatialScan` / `Op::SpatialOp` are first-class planner ops, so a spatial filter composes
  with `Traverse`/`Rank`/`Filter` in one [UQL](../uql.md) pipeline.
