//! Per-domain graph-op handlers. The dispatch shell (super::dispatch_graph_op)
//! routes each method here; cross-cutting write side-effects (dirty/WAL/gauge)
//! stay centralized in the shell. Each module exposes `try_handle(...) ->
//! Result<Response, Method>`: `Ok` = handled, `Err(method)` = not mine, try next.

// Feature-gated compute domains: a slim `--features server` build (no `compute`)
// excludes these entirely; their Method::Finance*/Ds* variants then fall to the
// graph_ops catch-all (a "not available in this build" error). Mirrors the `ast`
// gating of ParseFile(s).
#[cfg(feature = "datascience")]
pub(crate) mod datascience;
#[cfg(feature = "finance")]
pub(crate) mod finance;
pub(crate) mod graph_ops;
// Data-mining domain (CONCEPT:EG-KG.mining.frequent-itemset-mining, feature `mining`).
// Association-rule mining. GRAPH-SCOPED (unlike finance/datascience): the
// graph-derived source + write-back need the live graph core, so the handler takes
// it like query/rdf. A build without `mining` omits the module and the
// `MineAssociate` variant falls to the graph_ops "not available" catch-all.
#[cfg(feature = "mining")]
pub(crate) mod mining;
// M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch): the wire surface that DRIVES online
// resharding (EG-032), the tenant catalog (EG-031) and the rebalance planner (EG-035) +
// its execution (EG-039). Always declared; the real logic is `redb`-gated (the only build
// where the catalog/reshard/planner exist), and a non-redb build returns "not available".
pub(crate) mod admin;
// Read-only query surface: SQL (CONCEPT:EG-KG.query.read-only-sql-query, DataFusion behind `query`),
// Cypher (CONCEPT:EG-KG.query.dep-free-behind, dep-free behind `cypher`), AND GraphQL (CONCEPT:EG-KG.query.sparql-completeness,
// dep-free behind `graphql`). Present when ANY of those features is on — a cypher- or
// graphql-only Pi build keeps the module WITHOUT DataFusion. With none of them the
// Sql/CypherQuery/GraphQl variants fall to the graph_ops "not available" catch-all.
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
pub(crate) mod query;
// Multi-op OCC ACID transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid). Always present with the
// `server` feature — the Txn* methods are stateful (need `state`) and not
// feature-gated to a compute domain.
pub(crate) mod txn;
// Native time-series store + primitives (CONCEPT:AU-KG.retrieval.god-nodes-communities/211, feature `tsdb`). The
// Ts* methods are stateful (need the `SeriesStore` on `state`); a slim build without
// `tsdb` omits the module and the variants fall to the graph_ops not-built catch-all.
#[cfg(feature = "tsdb")]
pub(crate) mod timeseries;
// Streamed content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace). Behind `blob`
// (which pulls `redb` + `server`); the stateful Blob* methods drive the CAS store
// + cursors on ServerState. A build without `blob` drops both the module and the
// Method variants, so the dispatch chain never routes them.
#[cfg(feature = "blob")]
pub(crate) mod blob;
// Streaming / CDC / subscriptions / triggers (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230, feature
// `streaming`). The Cdc*/ContinuousQuery/Watch/Trigger methods are stateful (they
// drive the `CdcHub` on `state`); a build without `streaming` omits the module and
// the variants fall to the graph_ops not-built catch-all.
#[cfg(feature = "streaming")]
pub(crate) mod streaming;
// Native RDF/SPARQL surface (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql/218, features `rdf`/`sparql`). The
// AddTriples/GetRdf/Sparql methods are graph-scoped (they target `req.graph`),
// so the handler takes the graph core like the query handler; AddTriples also reads
// the optional lossless quad store off `state`. A build without `rdf` omits the
// module and the variants fall to the graph_ops "not available" catch-all.
#[cfg(feature = "rdf")]
pub(crate) mod rdf;
// WASM-sandboxed UDF surface (CONCEPT:EG-KG.query.rowset-execution, feature `wasm-udf`). RegisterUdf/RunUdf
// drive the process-global UdfRegistry on ServerState; a build without `wasm-udf` omits
// the module and the variants fall to the graph_ops not-available catch-all.
#[cfg(feature = "wasm-udf")]
pub(crate) mod wasm_udf;
// Distributed graph compute (CONCEPT:EG-KG.storage.feature, feature `compute-dist`). The
// DistributedCompute/*MatView methods drive the cross-shard Pregel engine + the
// matview store on ServerState; a non-cluster build omits the module and the variants
// fall to the graph_ops not-available catch-all.
#[cfg(feature = "compute-dist")]
pub(crate) mod dist_compute;
// Query federation / foreign sources (CONCEPT:EG-KG.query.query-federation, feature `federation`).
// RegisterForeignSource records a named foreign source on ServerState (the inline-spec
// `Op::ForeignScan` path runs through the unified-query handler). A build without
// `federation` omits the module and the variant falls to the graph_ops not-available
// catch-all.
#[cfg(feature = "federation")]
pub(crate) mod federation;
// SQLite `.db` FILE import/export (CONCEPT:EG-KG.query.eg-feature/EG-332, feature `sqlite-file`). The
// ImportSqliteFile/ExportSqliteFile methods move rows between an on-disk `sqlite3` `.db`
// file and the process-global `eg_query::TableStore` (behind `query`), via the bundled
// C `rusqlite` (kept OUT of pi). NOT graph-scoped; a build without `sqlite-file` omits
// the module and the variants aren't in the enum (so the dispatch chain never routes them).
#[cfg(feature = "sqlite-file")]
pub(crate) mod sqlite_file;
