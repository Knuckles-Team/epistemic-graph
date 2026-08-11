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
// Graph-learning / neuro-symbolic domain (CONCEPT:EG-KG.graphlearn.link-predictor,
// feature `graphlearn`). A KAN link-predictor learned over a graph-derived subgraph;
// GRAPH-SCOPED (like mining) — the source + `:PredictedEdge`/`:EdgeFunction`
// write-back need the live core. A build without `graphlearn` omits the module and
// the `GraphLearn*` variants fall to the graph_ops "not available" catch-all.
#[cfg(feature = "graphlearn")]
pub(crate) mod graphlearn;
// ML pipeline domain (CONCEPT:EG-KG.mining.ml-pipeline, feature `ml-pipeline`). A
// composable train→eval→serve→predict lifecycle over a versioned `:Model` artifact
// that generalizes the KAN one-off. GRAPH-SCOPED (like mining/graphlearn) — it reuses
// graphlearn's subgraph builder + the eg-compute classify/estimator/KAN/embedding/
// metrics primitives. A build without `ml-pipeline` omits the module and the
// `MiningPipeline*` variants fall to the graph_ops "not available" catch-all.
#[cfg(feature = "ml-pipeline")]
pub(crate) mod pipeline;
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
// Present under EITHER `compute-dist` (the algo-only Pregel matview + DistributedCompute)
// OR `matview` (the plan-backed, single-node incremental matview) — the two families share
// this dispatch module but NOT a feature dependency (the plan-backed path uses no raft).
#[cfg(any(feature = "compute-dist", feature = "matview"))]
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
// file and the verified caller's owner-scoped `eg_query::TableStore` (behind `query`), via the pure-Rust
// `eg-sqlite-format` (no C sqlite). NOT graph-scoped; a build without `sqlite-file` omits
// the module and the variants aren't in the enum (so the dispatch chain never routes them).
#[cfg(feature = "sqlite-file")]
pub(crate) mod sqlite_file;
// Durable analytics-job plane (CONCEPT:INT-P2-1, feature `jobs`). The
// `Method::AnalyticsJob { op }` surface (submit/status/cancel/resume) over
// `eg-jobs`'s redb-backed `AnalyticsJob` state machine; a submitted job runs
// asynchronously (spawned off the request) and its success commits a provenance'd
// `:Claim`/`:Evidence` pair via `eg_jobs::claim::commit_result_claim`. NOT
// graph-scoped (own `jobs.redb`, like `TsAppend`/`Kv*`) — self-routes in
// `dispatch.rs` before the per-graph chain, so it needs `state` directly rather
// than a resolved `GraphCore`.
#[cfg(feature = "jobs")]
pub(crate) mod jobs;
// Native finite-state-machine / statechart engine (CONCEPT:INT-P2-2, feature
// `statechart`). The `Method::Statechart { op }` surface (define/instantiate/
// send_event/get_state/list) over `eg-statechart`'s redb-backed `StatechartDef` +
// `MachineInstance` store; a durable machine instance is just a `(state, context)`
// row, rehydrated on the next event. NOT graph-scoped (own `statecharts.redb`, like
// `AnalyticsJob`/`TsAppend`/`Kv*`) — self-routes in `dispatch.rs` before the per-graph
// chain, so it needs `state` directly (only for `persist_dir`) rather than a resolved
// `GraphCore`.
#[cfg(feature = "statechart")]
pub(crate) mod statechart;
// The agent-facing quantum control-plane surface (Q8, CONCEPT:EG-KG.compute.quantum-agent-api,
// root feature `quantum-agent-api`, which turns on `eg-types/quantum` -- the
// `Method::Quantum` wire variant itself -- + `eg-capabilities/quantum` + this
// crate's own `quantum-sim` in lockstep; see the root Cargo.toml's `quantum-agent-api`
// feature doc). The `Method::Quantum { op }` surface (quantum_rank/
// optimize_with_qaoa/quantum_expectation) over a registered `eg_quantum_core::
// backend::QuantumBackend` (`eg-quantum-sim`'s `sv-cpu`/`stabilizer`). NOT
// graph-scoped (reads no persisted graph state, writes nothing durable -- every
// result returns to the caller as a proposal) — self-routes in `dispatch.rs` before
// the per-graph chain, exactly like `AnalyticsJob`/`Statechart`. Needs no `state` at
// all (no persistence of its own).
#[cfg(feature = "quantum-agent-api")]
pub(crate) mod quantum;
// Governed document/image/audio/video serving. The wire carries an ephemeral
// source body, while this handler persists only an encrypted, opaque runtime
// snapshot through the graph mutation gateway.
#[cfg(feature = "modality-serving")]
pub(crate) mod modality;
// One governed, bounded Arrow KnowledgeBatch result plane for graph, SQL, RDF,
// vector, time-series, analytics jobs, and cross-modal plans.
#[cfg(feature = "knowledge-batch")]
pub(crate) mod knowledge_stream;
// Placement-catalog wire RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4): exposes the
// DIST-P2-1 `PlacementCatalog` (raft/placement.rs) over `Method::PlacementRoute`. Always
// declared (like `admin`); the real answer is `raft`-gated (the only build where
// `MultiRaft`/the catalog exist), and a non-raft build (or a raft build with no live
// cluster) answers a well-formed "no explicit placement" JSON rather than an error.
pub(crate) mod placement;
// Raft cluster-membership ADMIN RPC (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md
// §5 item 2): exposes `MultiRaft::add_group_learner`/`change_group_voters`
// (raft/multi.rs) over `Method::RaftAddLearner`/`Method::RaftChangeMembership`. Always
// declared (like `placement`); the real answer is `raft`-gated, and a non-raft build
// answers a clean typed error.
pub(crate) mod raft_admin;
// Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1):
// `Method::ClusterMembers`/`Method::NodeInfoUpsert` over the durable
// `server::persistence::node_info_store::NodeInfoStore`, replacing the static
// `GRAPH_RAFT_GROUP_ENDPOINTS` client map. Always declared (like `placement`/
// `raft_admin`); the real cross-referenced answer is `raft`-gated, and a
// non-raft build answers a well-formed empty topology / a clean typed error.
pub(crate) mod topology;

/// The caller/isolation-scoping fields shared by the graph-scoped read handlers'
/// `try_handle` entry points (`query::try_handle`, `rdf::try_handle`), bundled so
/// each stays under the clippy argument-count ceiling once the feature-gated `rls`
/// authority parameter is unified in.
#[cfg(any(
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "rdf"
))]
pub(crate) struct TryHandleContext<'a> {
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) read_authority: Option<&'a super::access::GraphReadAuthority>,
    pub(crate) caller: &'a str,
}
