# GIS / spatial interface

epistemic-graph carries a native **geospatial modality** — a pure-Rust `eg-geo` crate (geo/geo-types + an
in-house packed Hilbert / STR R-tree; **no GEOS / PROJ C deps**) behind the `geo` feature (folded into
in the one main build). Geometries persist as a typed value in the redb per-graph store, a
durable R-tree indexes them, and spatial predicates compose with graph traversal / vector / SQL in one
plan. GeoSPARQL surfaces it over SPARQL; SQL `ST_*` functions surface it over pgwire.

> Status snapshot: geometry model + WKT/WKB/GeoJSON/GPX I/O (EG-KG.ontology.singles-concept/257/264) — plus **Shapefile/KML/
> GeoParquet** (EG-KG.domains.geo-formats) — DE-9IM + RCC8/Egenhofer relations (EG-KG.ontology.de-9im-relations/155), constructive algebra (EG-KG.ontology.concept-9),
> CRS + reprojection + geodesic distance (EG-KG.domains.coordinate-reference-system/262/256), a durable R-tree index (EG-KG.domains.spatial-strtree-index), map tiles
> (EG-KG.domains.map-tiles), routing/isochrones/TSP (EG-KG.domains.geo-routing) with **turn-restrictions + time-windows** (EG-KG.domains.geo-partitioning), and
> map-based task tracking (EG-KG.domains.geo-task) are shipped. See the [capability matrix](../capabilities.md).

## Geometry model (EG-KG.ontology.singles-concept/257)

The full OGC geometry set: `Point`, `LineString`, `Polygon` (with interior rings / holes),
`MultiPoint`, `MultiLineString`, `MultiPolygon`, and `GeometryCollection`, with **EWKT** parse/serialize.
Stored as a typed redb value with a per-shard durable R-tree (EG-KG.domains.spatial-strtree-index) consulted by `Op::SpatialScan` for
selectivity.

## Format I/O (EG-KG.domains.geojson-gpx-formats/306)

Reader/writer for **GeoJSON** (Feature / FeatureCollection), **WKB** (binary geometry), and **GPX** tracks
— round-tripping eg-geo geometries. Program B completes the map-data ingest/export matrix with **ESRI
Shapefile** (.shp/.dbf/.shx), **KML/KMZ**, and **GeoParquet** (EG-KG.domains.geo-formats), round-tripping geometries **and**
their attributes — so field data from GIS tooling (QGIS/ArcGIS exports, Google Earth KML, columnar
GeoParquet lakes) loads and exports directly.

## Predicates & relations (EG-KG.ontology.de-9im-relations/155)

- **Core** (EG-KG.ontology.singles-concept): `within` / `intersects` / `dwithin` as `Pred::SpatialWithin` / `SpatialDWithin`.
- **DE-9IM** (EG-KG.ontology.de-9im-relations): the full relation matrix — `contains` / `covers` / `touches` / `crosses` /
  `overlaps` / `equals` / `disjoint`, as additional `Pred::Spatial*` variants.
- **RCC8 + Egenhofer** (EG-KG.ontology.concept-7): the OGC topological-relation families, lowered onto the DE-9IM
  intersection matrix (surfaced through GeoSPARQL, see below).

## Constructive algebra (EG-KG.ontology.concept-9)

Buffer, convex hull, union / intersection / difference, simplify, and centroid — the constructive ops for
urban-planning / logistics — executed via `Op::SpatialOp { kind }` in eg-plan (pure-Rust).

## CRS, reprojection & geodesics (EG-KG.domains.coordinate-reference-system/262/256)

A coordinate-reference-system registry (`crs` module — EPSG codes incl. 4326 WGS84 / 3857 Web-Mercator +
proj params) and pure-Rust reprojection (geographic ↔ Web-Mercator + affine / Helmert), so geometries
carry a CRS and `ST_Transform` / GeoSPARQL CRS-URIs reproject correctly — no PROJ C dependency. A
geometry's CRS tag selects planar-vs-geodesic distance: great-circle / Haversine + Vincenty distance and
geodesic area (EG-KG.ontology.concept-8) give accurate real-world measures for logistics / urban planning.

## Map tiling (EG-KG.domains.map-tiles)

Slippy-map tile addressing (XYZ / TMS `z/x/y` ↔ lon/lat/bbox via Web-Mercator) + **Mapbox Vector Tile**
(MVT protobuf-lite) encoding of features clipped to a tile, so a web map (Leaflet / MapLibre) renders the
graph's spatial data.

## Routing, isochrones & TSP (EG-KG.domains.geo-routing/312)

Graph routing over a spatial network: weighted shortest path (Dijkstra / A* with a geo heuristic),
isochrones (reachability within a cost budget), and a nearest-neighbour + 2-opt TSP tour — the
logistics / urban-planning primitives, reusing the engine's graph traversal. Program B extends the router
for **realistic logistics** (EG-KG.domains.geo-partitioning):

- **Turn restrictions** — per-turn penalties (via an edge/turn cost), so a banned or costly turn is avoided;
  one-way edges were already supported.
- **Time-windows / time-dependent weights** — an edge weight can be a **function of departure time**, so
  rush-hour congestion or time-restricted access is modelled and the shortest path depends on *when* you
  leave.

## Map-based task tracking (EG-KG.domains.geo-task)

Geo-anchored tasks: a `:GeoTask` node with a location + status + optional service-area geometry, spatial
queries (tasks within a bbox / polygon, nearest-N to a point, tasks along a route), and assignment to the
nearest resource — the field-ops / urban-planning task layer over the spatial store.

## Reaching it

- **SQL** (pgwire / in-engine): `st_within` / `st_distance` / `ST_Transform` etc. compose with the node
  store — see [sql](sql.md).
- **GeoSPARQL** (feature `geosparql`): the `geo:` / `geof:` vocab, WKT/GML literals, and
  `sfWithin`/`sfIntersects`/`distance` + the RCC8/Egenhofer function families — see
  [sparql](sparql.md#geosparql).
- **UQL**: `Op::SpatialScan` / `Op::SpatialOp` are first-class planner ops, so a spatial filter composes
  with `Traverse`/`Rank`/`Filter` in one [UQL](../uql.md) pipeline.

---

**See also:** [Capabilities matrix](../capabilities.md) · [SPARQL & RDF](sparql.md) · [Agent Memory](memory.md) · [SQL & pgwire](sql.md) · [Connecting (per-wire guide)](connecting.md).
