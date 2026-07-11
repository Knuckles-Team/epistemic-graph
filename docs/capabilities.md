# Capabilities & parity matrix

This page is the **operation-by-operation truth table** for epistemic-graph as a universal database. It
is verified against the source, not against intent. Legend:

- **✅ supported** — implemented and covered by tests.
- **🔶 in-progress** — partially built or actively being added; the unsupported part errors honestly.
- **🗺 roadmap** — designed, not yet built.

The **Feature** column is the Cargo feature that gates the surface; the
[tiers page](architecture/tiers.md) shows which prebuilt binary carries it.

> **Machine-checked ledger available (EG-P0-1).** [`docs/capabilities.generated.md`](capabilities.generated.md)
> is generated from an exhaustive, compiler-enforced `MethodPolicy` match over every wire-protocol
> `Method` variant (`crates/eg-capabilities`, regenerate via `cargo run -p eg-capabilities --bin
> gen_ledger`) and is now the **authoritative** machine-checked source for per-method
> mutates/durability-domain/authz-action/idempotent/audited/emits-CDC/txn-participation facts. This
> page is **not yet reconciled** against it — it predates the ledger and answers a coarser question
> (which query SURFACE supports which OPERATION) rather than per-`Method` durability/security
> semantics, so the two are not redundant, but where they overlap and disagree on a durability or
> security claim, **the generated ledger wins**. Its own consistency test currently reports 106 open
> divergences (22 runtime-conditional, 23 WAL-durability gaps, 61 access.rs coverage gaps) — see
> [Known limitations](#known-limitations-in-progress-eg-p0-2p0-4p0-6) below, and note that of the 23
> WAL gaps the ledger counts, **5 were already closed by EG-P0-3** (`AddEmbedding`, `MineSequence`,
> `MineForecast`, `MineText` for `lda`/`nmf`, `MineSubgraph` for `gspan` — see [mining.md](mining.md))
> but the generated ledger has not yet been regenerated to drop them from its count, so its per-method
> Note column is stale for those five specifically until the next regeneration.

## SQL (`eg-query/sql` + pgwire)

| Operation | Status | Feature | Evidence |
|-----------|:------:|---------|----------|
| `SELECT` — joins, aggregates, GROUP BY/HAVING, window, CTE, subquery, UNION | ✅ | `query` | DataFusion 43; `classify.rs` routes `Query` → `Read`, executed in `exec.rs` |
| Predicate pushdown into the `nodes` scan | ✅ | `query` | `NodesTableProvider` returns `Inexact`, narrows rows via a per-column equality index (`providers.rs`) |
| `INSERT` / `UPDATE` / `DELETE` on the `nodes` table | ✅ | `query` | `classify_insert`/`classify_update`/`classify_delete` (EG-KG.query.follow-up); maps to `add_node` / `compare_and_set_fields` / `remove_node` |
| `RETURNING` on DML | ✅ | `query` | pgwire write path returns affected rows |
| Compound WHERE DML (`AND`/`OR`/`NOT`/`IN`/`BETWEEN`/ranges/`IS NULL`) | ✅ | `query` | `RowPredicate` in `classify.rs`; serializable `compare_and_set_fields_if`/`remove_node_if` re-check under the write guard (CONCEPT:EG-KG.query.compound-predicate-decode) |
| `INSERT INTO nodes … SELECT` (may JOIN user tables + graph) | ✅ | `query` | `InsertNodesSelect` via `apply_node_insert_row` (CONCEPT:EG-KG.query.insert-into-nodes-select) |
| Multi-table DML (`UPDATE … FROM` / `DELETE … USING`) | ✅ | `query` | matched ids resolved via DataFusion, applied via serializable CAS/remove gates (CONCEPT:EG-KG.query.update-delete-from) |
| `ON CONFLICT (cols) DO NOTHING/DO UPDATE` upsert + user-table `RETURNING` | ✅ | `query` | reuses unique/PK validation (CONCEPT:EG-KG.query.delete-returning-sees-row) |
| Mixed-store wire transactions (`BEGIN`/`COMMIT`/`ROLLBACK`, `TransactionStatus` `T`/`E`/`I`) | ✅ | `pgwire` | `GraphTxnBuffer` + user-table ops, read-your-own-writes overlay; documented non-2PC user-table window (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) |
| `CREATE VIEW` / `DROP VIEW` (durable catalog, expanded in `build_ctx`) | ✅ | `query` | CONCEPT:EG-KG.query.create-drop-view |
| `CREATE FUNCTION … LANGUAGE sql` (scalar + table UDFs, durable catalog) | ✅ | `query` | CONCEPT:EG-KG.query.create-drop-function |
| `CREATE FUNCTION … LANGUAGE plpgsql` procedural bodies (`DECLARE`/`IF`/`LOOP`/`WHILE`/`FOR`/`RETURN`/`RAISE`/`SELECT … INTO`) | ✅ | `query` | pure interpreter, `crates/eg-query/src/sql/plpgsql.rs` (CONCEPT:EG-KG.query.eg-validate-procedural-body/EG-KG.query.concept-7); set-returning `RETURN NEXT/QUERY`, cursors, exception handlers + DML-in-body are documented out of scope |
| Columnar (struct-of-arrays) segments + SQL window frames (`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`/`OVER(PARTITION BY … ROWS/RANGE …)`) | ✅ | `query` | CONCEPT:EG-KG.temporal.columnar-schema-inference |
| DML on arbitrary user tables (`INSERT`/`UPDATE`/`DELETE`, `INSERT … SELECT`, `COPY`) | ✅ | `query` | durable redb `TableStore` (EG-KG.query.register-user-tables-alongside/EG-KG.query.register-each-user-table); `run_insert_table`/`run_update_table`/`run_delete_table` |
| `CREATE` / `ALTER ADD COLUMN` / `DROP TABLE`, arbitrary user tables, DDL | ✅ | `query` | `crates/eg-query/src/tables/` durable catalog (EG-KG.query.register-user-tables-alongside); JOINable to the graph |
| `ALTER TABLE` beyond ADD COLUMN — `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN TYPE` (data migration), `DROP CONSTRAINT` | ✅ | `query` | durable user-table catalog rewrite (CONCEPT:EG-KG.query.rename-table-moves-catalog) |

### Postgres wire (`pgwire`, in the main build — CONCEPT:EG-KG.compute.capability-reference/EG-KG.sharding.deployment-tiers)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| TCP listener, gated on `EPISTEMIC_GRAPH_PGWIRE_ADDR` | ✅ | `src/server/pgwire/mod.rs` `serve`/`serve_with_auth` |
| Simple query protocol | ✅ | `SimpleQueryHandler` |
| Extended / prepared protocol (Parse/Bind/Describe/Execute, `$N` params) | ✅ | `ExtendedQueryHandler`, `substitute_params` |
| SCRAM-SHA-256 auth (pg user → engine ACL actor) | ✅ | `auth.rs` `PgWireAuthMode::Scram` (EG-KG.query.concept-13) |
| Trust auth (dev) | ✅ | `auth.rs` `PgWireAuthMode::Trust` |
| `pg_catalog` / `information_schema` introspection | ✅ | `register_pg_catalog` + DataFusion `with_information_schema` (EG-KG.query.datafusion) |
| `pg_catalog.pg_class/pg_namespace/pg_attribute/pg_type/pg_index/pg_proc` + `information_schema.tables/columns/schemata/views/routines` so `psql \d`/`\dt`/`\l`, ORMs + BI tools introspect | ✅ | synthesized from live table/view/function catalogs incl. `pg_table_is_visible`/`format_type`/`current_schema` (CONCEPT:EG-KG.query.route-create-view-create) |
| `SET graph = '<name>'` connection switch | ✅ | `mod.rs` |

### Postgres-extension drop-in (`pgwire` + eg-query/eg-ann/eg-tsdb/eg-text)

`CREATE EXTENSION` records the enabled extension in a durable catalog and gates the family surfaces — the keystone that lets an unmodified Postgres client/ORM connect.

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `CREATE EXTENSION vector/pg_age/timescaledb/pg_search` + durable extension catalog | ✅ | eg-query classify + pgwire (CONCEPT:EG-KG.query.create-drop-extension-over) |
| PostgreSQL array (`int[]`/`text[]`, `ANY`/`ALL`/`unnest`/`array_agg`/`@>`/`&&`) + range types + common scalar functions (`string_agg`/`split_part`/`regexp_replace`/`to_char`/`date_trunc`/`generate_series`) | ✅ | DataFusion UDF/UDTF registration (CONCEPT:EG-KG.query.greatest-least-int4range-tsrange) |
| **pgvector**: `vector(n)` type + `<->` (L2) / `<=>` (cosine) / `<#>` (neg-inner) operators, pgwire type OID | ✅ | CONCEPT:EG-KG.query.pgvector-binary-wire |
| **pgvector** index pushdown: `ORDER BY emb <-> $1 LIMIT k` → eg-ann index; `CREATE INDEX … USING hnsw/ivfflat` | ✅ | CONCEPT:EG-KG.query.real-ann-top-k; **real ANN top-k** pushed to the eg-ann HNSW/IVF index + exact re-rank (brute-force only as the no-index path) (CONCEPT:EG-KG.query.real-pgvector-ann-top) |
| **Apache AGE**: `SELECT * FROM cypher('graph', $$ MATCH … RETURN … $$) AS (a agtype)` routed to the eg-query cypher engine | ✅ | CONCEPT:EG-KG.query.postgres-family-extension-plan |
| **TimescaleDB**: `create_hypertable()`, `time_bucket()` gap-fill, `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` continuous aggregates | ✅ | lowered onto eg-tsdb + `Op::Window` (CONCEPT:EG-KG.query.continuous-aggregate-lowering) |
| **ParadeDB**: `@@@` BM25 search operator + `score()`/`snippet()` `paradedb.*` functions | ✅ | lowered onto eg-text BM25 (CONCEPT:EG-KG.query.paradedb-bm25); **real BM25 relevance scoring + highlighted snippets** (CONCEPT:EG-KG.query.bm25-ranking-snippets, replacing the placeholder `1.0`) |

## SPARQL (`eg-rdf`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `SELECT` query form | ✅ | `sparql.rs` `Query::Select` arm |
| Algebra: BGP, property paths, FILTER subset, OPTIONAL, UNION, GROUP/aggregate, BIND, DISTINCT, REDUCED, SLICE | ✅ | `eval_pattern` match in `sparql.rs` |
| Aggregates COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/SAMPLE | ✅ | `sparql.rs` aggregate evaluator |
| `ASK` / `CONSTRUCT` / `DESCRIBE` | ✅ | `sparql.rs` `Query::{Ask,Construct,Describe}` arms; `construct_graph`/`describe_resources` (gated by `rdf`, implied by `sparql`) |
| `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`) | ✅ | `eg-rdf/src/update.rs` over a `GraphStore`; `LOAD` intentionally deferred (no HTTP fetch in write path) |
| `/sparql` HTTP endpoint (W3C SPARQL 1.1 Protocol) | ✅ | `src/server/sparql_http.rs` (EG-017), feature `sparql-http`; GET + POST query/update |
| True named graphs (multi-graph quad querying) + `FROM` / `FROM NAMED` dataset spec | ✅ | `Dataset` over every registry graph; `GRAPH ?g`/constant-IRI scoping (CONCEPT:EG-KG.ontology.from-from-named) |
| `ORDER BY` total-ordering (multi-key ASC/DESC, top-k), `VALUES` inline data, `MINUS`, `EXISTS`/`NOT EXISTS` | ✅ | fixes the unordered-results correctness gap (CONCEPT:EG-KG.ontology.completing-eg-order-by / EG-KG.ontology.order-by-values-exists / EG-KG.ontology.minus) |
| Negated property set `!p` | ✅ | scan edges whose predicate ∉ set (CONCEPT:EG-KG.ontology.negated-property-set) |
| SPO/POS/PSO triple index + cardinality-based BGP reordering | ✅ | replaces full-scan-per-pattern (CONCEPT:EG-KG.ontology.capability-catalog) |
| Rich FILTER: arithmetic, `REGEX`, `IN`, `STR`/`LANG`/`DATATYPE`/`BOUND`, `isIRI`/`isLiteral`, string fns (`CONTAINS`/`STRSTARTS`/`SUBSTR`/`CONCAT`), `COALESCE`/`IF` | ✅ | datatype-aware comparison (CONCEPT:EG-KG.ontology.rich-filter) |
| Builtin-function library: term constructors (`IRI`/`BNODE`/`STRDT`/`UUID`), hashes (`MD5`/`SHA*`), date-time (`NOW`/`YEAR`…), numeric (`RAND`/`ABS`/`ROUND`), string extras (`STRBEFORE`/`REPLACE`/…) | ✅ | CONCEPT:EG-KG.ontology.concept-4 |
| Sub-SELECT (nested `{ SELECT … }`) | ✅ | restrict inner solutions to projected vars (CONCEPT:EG-KG.ontology.sub-select) |
| SERVICE federation (`SERVICE <ep> { … }`, SILENT, SSRF allowlist) | ✅ | `ureq` remote client, feature `sparql-service` (CONCEPT:EG-KG.query.sparql-service-federation-client) |
| Content negotiation (SPARQL-results JSON/XML/CSV/TSV, Turtle/N-Triples out) | ✅ | hand-written serializers + `oxttl` (CONCEPT:EG-KG.ontology.content-negotiation-serializers) |
| Graph Store Protocol + `COPY`/`MOVE`/`ADD` | ✅ | `/rdf-graphs/…?graph=` (CONCEPT:EG-KG.query.graph-store-http-protocol) |
| RDF-star / SPARQL-star (RDF 1.2 quoted triples) | ✅ | CONCEPT:EG-KG.ontology.concept-5 |

### RDF serialization + validation (`eg-rdf` + `eg-shacl`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Serialization matrix: JSON-LD 1.1 (expansion/compaction), TriG, N-Quads, RDF/XML | ✅ | CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix / EG-KG.ontology.completes-rdf-concrete-syntax / EG-KG.ontology.feature (alongside Turtle/N-Triples) |
| SHACL validation (node/property shapes, cardinality/datatype/pattern/`sh:in`/logical + SPARQL constraints → `sh:ValidationReport`) | ✅ | pure-Rust `eg-shacl`, `Method::ShaclValidate` (CONCEPT:EG-KG.ontology.concept-6) |
| ShEx (Shape Expressions) validation | ✅ | `Method::ShexValidate` (CONCEPT:EG-KG.compute.concept-2) |
| ICV integrity-constraint validation (Stardog-style, closed-world/UNA; guard mode rejects violating writes; also runs over the OWL-reasoned view) | ✅ | CONCEPT:EG-KG.ontology.wired-into-commit-write; wired into the **commit/write path** — a guard evaluates the change set and REJECTS a violating transaction, configurable enforce/warn (CONCEPT:EG-KG.ontology.rdf-update-guard) |

## OWL reasoning (`eg-rdf/owl`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| OWL 2 EL⁺ completion (CR-sub/conj/some/chain/subrole/bot/disjoint) | ✅ | `owl.rs` `saturate()` |
| OWL 2 RL property rules (transitive/symmetric/inverse/chains/domain) | ✅ | `parse_ontology` + completion |
| Classification, consistency (unsat → inconsistent) | ✅ | `owl.rs` `snapshot()` |
| Forward-chaining materialization + incremental `add_axioms` | ✅ | monotone fixpoint, resumed in place |
| Confidence-weighting (per-axiom `eg:confidence`, noisy-OR) | ✅ | `classify_weighted()` (EG-KG.ontology.concept-13) |
| Ebbinghaus time-decay of facts | ✅ | `eg_core::decay::ebbinghaus_weight`; `GRAPH_SERVICE_DECAY_HALF_LIFE` |
| Distributed / cross-shard reasoning (union TBox+ABox, one closure) | ✅ | `reason_distributed_weighted` |
| Query-time `Op::Reason` (reasoner seeds a RowSet) | ✅ | `wire.rs` `Op::Reason` under `owl-plan`; executor seeds class members |
| OWL-DL (tableau, cardinality, `complementOf`, nominals) | ✅ | pure-Rust description-logic tableau behind `owl-dl` (consistency → classification → instance checking); the EL/RL fast path stays default and DL-requiring ontologies route to the tableau (CONCEPT:EG-KG.ontology.concept-2) |
| SWRL user rules | ✅ | `rules.rs` Horn-rule DSL + SWRL/RuleML atoms + the `swrlb:` built-in library (comparison/math/string), head-var range-safe (CONCEPT:EG-KG.ontology.concept-3) |

## Cypher (`eg-query/cypher`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `MATCH … WHERE … RETURN … LIMIT` (read-only, one snapshot) | ✅ | `exec_cypher(&GraphView, …)` |
| Patterns: labels, typed edges, both directions, var-length single hop | ✅ | `parser.rs` / VF2 + petgraph BFS |
| WHERE: `=, <>, !=, <, <=, >, >=`, AND-joined | ✅ | `parse_predicates` |
| Writes (`CREATE`/`MERGE`/`SET`/`DELETE`+`DETACH`) | ✅ | `exec_cypher_write` → `apply_create`/`apply_merge`/`apply_set`/`apply_delete` over eg-core mutations |
| `REMOVE` (property delete + label removal) | ✅ | `WriteOp::Remove` → `apply_remove` (CONCEPT:EG-KG.query.cypher-execution) |
| `ORDER BY` / `SKIP` / `WITH` / `OPTIONAL MATCH` / `OR`+`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL`, aggregation (`count`/`collect`/`sum`/`avg`/`min`/`max`), `RETURN DISTINCT`/`*` | ✅ | parser + executor (CONCEPT:EG-KG.query.eg-extend-read-side) |
| Variable-length hop combined with fixed hops + path-variable binding | ✅ | relaxes the single-hop guard (CONCEPT:EG-KG.query.concept-2) |
| Quantified path patterns `((a)-[:REL]->(b)){m,n}` (Cypher 25) | ✅ | repeated whole-subpattern BFS (`walk_hops`/`group_reachable`); per-iteration var bindings NOT exposed (list values), CREATE unsupported (CONCEPT:EG-KG.query.quantified-path-pattern) |
| `UNWIND expr AS var` | ✅ | composes with WITH/MATCH pipeline (CONCEPT:EG-KG.query.param-list-drives-unwind) |
| `CALL { subquery }` + `CALL proc(args) YIELD …` procedure framework | ✅ | invocation registry → native/WASM procedures (CONCEPT:EG-KG.query.cypher-planning) |
| APOC-equivalent + GDS surface via `CALL gds.*` (PageRank, WCC/SCC, Louvain, Label Propagation, betweenness/degree centrality, Dijkstra, node similarity + top-k KNN) | ✅ | pure-Rust eg-compute (CONCEPT:EG-KG.query.eg-2 / EG-144); `CALL gds.<algo>(…) YIELD …` projects the live graph into the eg-compute adjacency + streams results as Cypher rows (CONCEPT:EG-KG.query.gds-call-procedures) |
| `gds.dbscan` (density clustering) + `gds.linkPrediction` (KAN link-predictor) | ✅ | routes to `mining::cluster::dbscan` / `graphlearn::link_predict` behind the `cypher-mining`/`cypher-graphlearn` features (out of the base Pi-lean `cypher` build) (CONCEPT:EG-KG.query.gds-procedure-routing) |

## GraphQL (`eg-graphql`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Read queries: root label fields, `first`/`limit`, property filters, aliases, nested edge selections | ✅ | `resolver.rs` scan + BFS; schema introspected from the live graph |
| Mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`) | ✅ | `mutation.rs` `execute` over eg-core mutations; OCC bumped once per batch |
| Apollo Federation v2 subgraph: `_service { sdl }` + `_entities(representations:[_Any!]!)`, `@key`/`@shareable`/`@external` directives | ✅ | so the engine is a federated subgraph in an Apollo supergraph (CONCEPT:EG-KG.query.apollo-federation-subgraph) |
| Enterprise hardening: automatic persisted queries (APQ), query depth + complexity/cost limits, field/node caps, introspection toggle | ✅ | protects the federated subgraph in production (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) |
| Subscriptions | ✅ | real CDC push: a `LiveQuery` over a `tokio::sync::broadcast` change-stream fed by `GraphCore` `mark_dirty`, WS/SSE carrier (CONCEPT:EG-KG.compute.cdc-event-emit) |
| Fragments / variables / directives / relay pagination | ✅ | `$`/`@` lexer, fragment-spread + inline fragments, variable defs/refs, `@skip`/`@include`, relay `edges`/`node`/`cursor`/`pageInfo` (CONCEPT:EG-KG.query.fragments-variables-directives/066) |

## Vector / ANN (`eg-ann` + `eg-core`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| IVF-PQ index (coarse quantizer + 8-bit PQ codes) | ✅ | `ivfpq.rs` `train` |
| HNSW index (hierarchical-navigable-small-world graph, higher recall-per-probe than IVF-PQ) | ✅ | insert/search/serde-persist, tuned by the EG-KG.query.concept-5 recall harness (CONCEPT:EG-KG.retrieval.hnsw-vector-index) |
| OPQ learned rotation (polar/SVD update) | ✅ | `ivfpq.rs` rotation update |
| SQ8 refine re-rank tier | ✅ | `ivfpq.rs` over-fetch + SQ8 re-rank (recall ≥ 0.95 in tests) |
| Persistent index — reopen WITHOUT rebuild (mmap codes, O(N) posting rebuild) | ✅ | `persist.rs` `open` |
| Parallel + SIMD brute-force fallback (rayon, contiguous arena, cached L2 norms) | ✅ | `semantic_store_ann.rs` |
| Warm-on-start (index built off the query path) | ✅ | `warm()` / `ensure_index` |
| Hybrid metadata pre-filter (kNN with an `allow(id)` predicate) | ✅ | `ivfpq.rs` `search_filtered` (EG-070); tested DURING the ADC probe, not post-filter |
| Exact/flat kNN index (ground-truth) + ANN-candidate re-rank + recall@k/precision self-eval harness | ✅ | brute-force exact + hybrid refinement (CONCEPT:EG-KG.query.concept-5) |
| Cross-shard kNN scatter-gather (fan a kNN query across per-shard eg-ann indexes → deterministic global top-k) | ✅ | server-layer scatter over per-shard indexes merged via the `merge_topk` leaf (CONCEPT:EG-KG.query.scatter-knn-merge, completing EG-KG.retrieval.scatter-gather) |
| GPU-accelerated batch distance (`DistanceBackend` seam + real CUDA backend; CPU fallback) | ✅ | `crates/eg-ann/src/distance.rs` — `FlatIndex::search` routes through `batch_distances`; pure-Rust CPU backend always compiled-in, `cudarc` NVRTC kernel behind `gpu-cuda` (dynamic-loading, `full-extras`-only) (CONCEPT:EG-KG.compute.gpu-distance-seam/3.6) |

## Time-series (`eg-tsdb`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb columnar store, group-amortized `append_batch`, range scan, retention | ✅ | `store.rs` |
| `time_bucket` (avg/min/max/sum/count/first/last) | ✅ | `query.rs` |
| ASOF backward join (the primitive DataFusion lacks) | ✅ | `query.rs` `asof_join_backward` |
| gap-fill LOCF, OHLC bars, downsample/rollup, decay-weighted mean, EWMA, rolling z-score | ✅ | `query.rs` |
| Time-ops as unified planner ops (`Op::Window` execution) | ✅ | real `window_aggregate` over the input RowSet's time/value columns via eg-tsdb `time_bucket`, behind `timeseries` (CONCEPT:EG-KG.query.streaming-execution) |
| Per-point retention trim (vs whole-bucket drop) | ✅ | `evict_before` trims a straddling bucket in place, point-by-point (CONCEPT:EG-KG.temporal.bucket-cutoff-trim) |

## Blob / CAS (`src/server/blob`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Content-addressed (sha256) streaming store, manifest of chunk digests | ✅ | `store.rs` `ChunkStore` |
| redb-native backend, group-commit, refcount mark-and-sweep GC | ✅ | `RedbChunkStore` |
| S3 / MinIO backend behind the same `ChunkStore` trait | ✅ | `s3.rs` `S3ChunkStore` (`blob-s3`) |
| Content-defined chunking | ✅ | Gear/FastCDC rolling-hash chunker (variable boundaries in `BlobManifest`); sha256 CAS dedup + refcount GC preserved (CONCEPT:EG-KG.storage.backward-manifest-read) |

## Key-value / embedded (`redb`, `embedded`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Embedded in-process engine over redb rows (no Tokio/socket/HMAC, commit-before-return) | ✅ | `src/embedded.rs` `EmbeddedEngine::open` (EG-KG.backend.engine-modes) |
| Durable graph-shaped tables (`nodes`/`edges`/`ledger`/`semantic_store`/`graph_meta`) | ✅ | `src/redb_store.rs` |
| Generic namespaced `get`/`put`/`delete`/`scan`/`cas` KV surface over redb | ✅ | `src/server/kv.rs` `KvStore` (EG-022); `Method::Kv*`; durable, commit-before-ack; not graph-scoped |

## Wire protocols & interop endpoints (`WireProtocol` + hand-rolled listeners)

Every listener is **opt-in** — it starts only when the binary is built with its feature
AND its `EPISTEMIC_GRAPH_*_ADDR` env var (or CLI flag) is set. The SQL wires share the
one wire-neutral `classify → dispatch → exec` core (`src/server/wire`, CONCEPT:EG-KG.compute.subsystems-reference) —
no SQL is reimplemented per wire. See [`interfaces/connecting.md`](interfaces/connecting.md)
for per-wire connect+query recipes and the full env-var/port table.

| Surface | Status | Feature | Evidence |
|---------|:------:|---------|----------|
| Wire-neutral SQL core (`WireProtocol`/`WireSession`) | ✅ | `wire` | `src/server/wire` (EG-KG.compute.subsystems-reference); shared by every SQL wire |
| Postgres wire (psql / BI / ORM) | ✅ | `pgwire` | `src/server/pgwire`; `EPISTEMIC_GRAPH_PGWIRE_ADDR` (AU-KG.query.raw-python) |
| MySQL / MariaDB wire (hand-rolled handshake v10 + `mysql_native_password`) | ✅ | `mysql-wire` | `src/server/mysql_wire`; `EPISTEMIC_GRAPH_MYSQL_ADDR` (EG-KG.query.kg-2) |
| MSSQL TDS wire (hand-rolled TDS) | ✅ | `mssql-wire` | `src/server/mssql_wire`; `EPISTEMIC_GRAPH_MSSQL_ADDR` (EG-KG.query.hand-rolled-tds-server) |
| SQLite-dialect NDJSON-over-TCP endpoint | ✅ | `sqlite-wire` | `src/server/sqlite_wire`; `EPISTEMIC_GRAPH_SQLITE_ADDR` (EG-KG.query.concept-3) |
| On-disk `sqlite3` `.db` file import/export (`Method::ImportSqliteFile`/`ExportSqliteFile`) | ✅ | `sqlite-file` | pulls `rusqlite` (bundled C sqlite3), in the main build (CONCEPT:EG-KG.query.eg-feature/EG-KG.query.full-protocol) |
| Neo4j Bolt v4.4 wire (PackStream v2, native Cypher) | ✅ | `bolt-wire` | `src/server/bolt_wire`; `EPISTEMIC_GRAPH_BOLT_ADDR` (EG-KG.query.bolt-wire-protocol) |
| AMQP 0.9.1 broker wire (exchanges/queues over the EG-KG.compute.atomically-claim-oldest-pending work-queue) | ✅ | `amqp-wire` (impl `broker`) | `src/server/amqp_wire`; `EPISTEMIC_GRAPH_AMQP_ADDR` (EG-275) |
| MQTT 3.1.1/5.0 broker wire (CONNECT/PUBLISH/SUBSCRIBE, QoS 0/1) | ✅ | `mqtt-wire` (impl `broker`) | `src/server/mqtt_wire`; `EPISTEMIC_GRAPH_MQTT_ADDR` (EG-281) |
| STOMP 1.2 broker wire (CONNECT/SEND/SUBSCRIBE/ACK) | ✅ | `stomp-wire` (impl `broker`) | `src/server/stomp_wire`; `EPISTEMIC_GRAPH_STOMP_ADDR` (EG-KG.ontology.stomp-frame-codec-unit) |
| Redis RESP2/RESP3 wire (GET/SET/DEL/EXPIRE/INCR, HSET/HGET, LPUSH/LRANGE, SADD/SMEMBERS, ZADD/ZRANGE, scan) over the KV surface | ✅ | `redis-wire` | `src/server/redis_wire`; `EPISTEMIC_GRAPH_REDIS_ADDR` (EG-KG.ontology.resp2-resp3-codec-round); **pub/sub** (`SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`/`UNSUBSCRIBE`) + `MULTI`/`EXEC` transactions (CONCEPT:EG-KG.txn.pubsub-transactions) |
| S3-compatible REST (bucket + object PUT/GET/DELETE/HEAD/List, SigV4-lite) over the blob CAS | ✅ | `s3-api` | `src/server/s3` (EG-KG.ontology.object-put-get-head); **multipart upload** (Create/Upload-Part/Complete/Abort) + **range GET** (CONCEPT:EG-KG.txn.pubsub-transactions) |
| GraphQL SSE subscription carrier | ✅ | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR`; real CDC push over WS/SSE (CONCEPT:EG-KG.compute.cdc-event-emit) |

## Message broker (`broker` — surpasses RabbitMQ)

Built on the EG-KG.compute.atomically-claim-oldest-pending native engine task queue (`ClaimNext`): durable exchanges/queues live as `__control__` graph nodes, drive additive `Method::*` ops, and are Raft/WAL-safe.

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Exchanges (direct/topic/fanout) + bindings/routing-keys + queues | ✅ | RabbitMQ-class routing (CONCEPT:EG-KG.compute.message-broker-exchanges) |
| Dead-letter queues (max-delivery / reject → DLX, metadata preserved) | ✅ | CONCEPT:EG-KG.compute.dead-letter-queues |
| Per-message + per-queue TTL / queue expiry (lazy sweep + reaper) | ✅ | CONCEPT:EG-KG.compute.message-ttl-expiry |
| Priority queues (priority band, FIFO within band) | ✅ | CONCEPT:EG-KG.compute.priority-queues |
| Delayed / scheduled delivery (deliver-after / deliver-at) | ✅ | due-time index (CONCEPT:EG-KG.compute.delayed-scheduled-delivery) |
| Consumer groups + per-consumer QoS/prefetch + fair round-robin + visibility leases | ✅ | CONCEPT:EG-KG.compute.groups-qos-prefetch-honoring |
| Replayable append-log streams (Kafka/RabbitMQ-Streams: retained ordered offset log, read-from-offset, retention by size/age) | ✅ | CONCEPT:EG-KG.compute.replayable-append-log |
| Publisher confirms (monotonic delivery-tag) + consumer manual ack/nack-with-requeue (at-least-once) | ✅ | CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos |
| Exactly-once (idempotent-producer dedup by producer-id + sequence) + stream/confirm/ack over the AMQP `confirm.select` + MQTT 5 wire frames | ✅ | effectively-exactly-once on top of at-least-once (CONCEPT:EG-KG.ingest.broker-reject-publish) |

## Observability endpoints (`obs` listener, CONCEPT:AU-KG.ingest.self-ingest/172/163)

The obs listener (`EPISTEMIC_GRAPH_OBS_ADDR`, default `127.0.0.1:5080`) fronts the
logs + metrics + traces trilogy over the durable eg-tsdb series + eg-text index.

| Surface | Status | Feature | Evidence |
|---------|:------:|---------|----------|
| Log ingest (OTLP/HTTP, Elasticsearch `_bulk`/`_doc`, JSON-lines, syslog) + Parquet-on-CAS segments | ✅ | `obs` | `src/server/obs` (AU-KG.ingest.self-ingest/161) |
| Log search + query API (DataFusion over Parquet segments ∪ hot tsdb ∪ eg-text BM25; O2/ES `/_search`) | ✅ | `obs` | CONCEPT:EG-KG.query.concept-4 |
| VRL-style ingest pipelines (parse/json-extract, filter/drop, set/rename/remove, coerce, route-to-stream; cross-modal graph enrichment) | ✅ | `obs` | pure-Rust DSL → staged executor (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook) |
| PromQL + Prometheus HTTP query API (`/api/v1/query[_range]`, `/api/v1/labels`) | ✅ | `promql` | `src/server/promql` + `eg-tsdb/promql` (EG-172); extended fn set — `_over_time` family, `delta`/`idelta`/`deriv`, `topk`/`bottomk`/`quantile`, `label_replace`/`label_join`, `clamp*` (CONCEPT:EG-KG.query.bottomk-selection) |
| Distributed traces (OTLP-JSON `POST /v1/traces`, `/api/traces` search, service-dependency graph) | ✅ | `traces` | `src/server/traces` (EG-163) |
| Super-cluster federated search (fan a read out to peer instances, merge/dedup/re-rank, per-peer timeout + partial-result tolerance) | ✅ | `federation`/`cluster` | `/federated` HTTP entry (CONCEPT:EG-KG.ontology.federation-client); **typed SQL + SPARQL result fusion** — schema-aware column union + typed dedup/merge, not just hashed-key union (CONCEPT:EG-KG.query.schema-typed-fusion-sql) |
| OpenTelemetry span export + slow-query log | ✅ | `otel` | CONCEPT:EG-OS.observability.slow-query-descriptor |
| OTel export (OTLP metrics + traces push to an external collector) + Prometheus remote-write receiver — the engine emits its own telemetry, not just ingests | ✅ | `otel-export` | protobuf (`prost`) dep behind the feature, in the main build (CONCEPT:EG-OS.observability.prometheus-ingest) |
| Prometheus `/metrics` exposition (engine's own counters/gauges) | ✅ | `metrics` (default) | `--metrics-addr` / `GRAPH_SERVICE_METRICS_ADDR`, default `127.0.0.1:9101` |

## Unified planner & UQL (`eg-plan`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| RowSet ops: `Scan, Filter, Traverse, Rank, RankNodeDistance, RankMentions, RankMmr, AsOf, Limit` | ✅ | `wire.rs` `Op`; `exec.rs` arms |
| `RankText`, `FuseRrf` | ✅ | `text` feature |
| `Reason`, `SparqlBgp` | ✅ | `owl-plan` feature |
| `Udf` (sandboxed WASM) | ✅ | `wasm-udf` feature |
| `ForeignScan` (remote engine / HTTP-JSON / external SQL) | ✅ | `federation` / `federation-sql` |
| `Window`, `Foreign` execution | ✅ | `Op::Window` = real eg-tsdb windowed aggregate (`timeseries`, CONCEPT:EG-KG.query.streaming-execution); `Op::Foreign` resolves the name → rows via the `ForeignSourceRegistry` (`federation`, CONCEPT:EG-KG.query.closure-backed-source) |
| UQL text DSL → `wire::Plan` (dependency-free parser) | ✅ | `eg-plan/src/uql` (AU-KG.query.top-nodes-by-degree) |
| `SpatialScan` / `Pred::SpatialWithin`/`SpatialDWithin` | ✅ | `geo` feature (EG-KG.ontology.singles-concept); eg-geo executor + leaf crate |
| `TensorScan` / `TensorOp`, `Cep` event-stream match | ✅ | `tensor` (EG-085) / `stream` (EG-KG.query.pipelined-execution) |
| Natural-language → query (`Method::NlQuery`, `/nl` route) | ✅ | `nl-query` (CONCEPT:EG-KG.query.core-query-input/080); complete LLM-optional NL→UQL seam — inert (clear "not configured" error, never a panic) until an OpenAI-compatible endpoint is set; AU-provider integration tracked on the agent-utilities side |

## Spatial / GIS (`geo` + `geosparql`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| CRS registry + affine/Helmert reprojection | ✅ | `eg-geo` (EG-KG.domains.geo-registry) |
| Durable STR-packed R-tree spatial index | ✅ | `eg-geo` (EG-KG.domains.spatial-strtree-index) |
| GeoJSON / WKB / GPX / WKT I/O | ✅ | `eg-geo` (EG-KG.domains.geo-registry/264, `geo-io`) |
| ESRI Shapefile (.shp/.dbf/.shx) / KML/KMZ / GeoParquet I/O | ✅ | round-trips geometries + attributes, completing the map-data ingest/export matrix (CONCEPT:EG-KG.domains.geo-formats) |
| `Op::SpatialScan` + `SpatialWithin`/`SpatialDWithin` predicates in a UnifiedQuery | ✅ | `geo` feature (EG-KG.ontology.singles-concept) |
| DE-9IM topological relations (contains/covers/touches/crosses/overlaps/equals/disjoint) | ✅ | `eg-geo/predicates.rs` (CONCEPT:EG-KG.ontology.de-9im-relations) |
| Constructive geometry algebra (buffer, convex hull, union/intersection/difference, simplify, centroid) | ✅ | `Op::SpatialOp` (CONCEPT:EG-KG.ontology.concept-9) |
| Geodesic ops (Haversine/Vincenty distance + geodesic area; CRS tag selects planar vs geodesic) | ✅ | CONCEPT:EG-KG.ontology.concept-8 |
| Full geometry model (Multi*/GeometryCollection + polygon holes + EWKT) | ✅ | CONCEPT:EG-KG.domains.geometry-collections |
| Map tiling: XYZ/TMS addressing + Mapbox Vector Tiles (MVT) clipped to a tile | ✅ | web-map render (CONCEPT:EG-KG.domains.map-tiles) |
| Raster tile pyramids (georeferenced coverage grid → XYZ raster tiles + full pyramid, dependency-free PNG codec) | ✅ | `crates/eg-geo/src/raster.rs` (CONCEPT:EG-KG.domains.raster-build/EG-KG.domains.raster-fetch); no `image`/`png`/`flate2` — Pi contract holds |
| Weighted routing (Dijkstra/A* geo-heuristic) + isochrones + nearest-neighbour/2-opt TSP | ✅ | logistics primitives (CONCEPT:EG-KG.domains.geo-routing); turn-restriction penalties + time-window/time-dependent edge weights (CONCEPT:EG-KG.domains.geo-partitioning) |
| Map-based task tracking (`:GeoTask` location/status/service-area; within-bbox/polygon, nearest-N, along-route, nearest-resource assignment) | ✅ | field-ops layer (CONCEPT:EG-KG.domains.geo-task) |
| OGC GeoSPARQL `geo:`/`geof:` vocabulary + spatial FILTER functions over SPARQL | ✅ | `geosparql` feature (EG-KG.ontology.concept-10); reuses eg-geo (no GEOS/PROJ) |
| RCC8 + Egenhofer topological relation families over GeoSPARQL | ✅ | lowered onto DE-9IM (CONCEPT:EG-KG.ontology.concept-7) |

## Agent-native memory (`eg-core` — arxiv 2606.24775; vs Zep / mem0 / LeanRAG)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Bi-temporal `AsOf` (valid-time + transaction-time `tx_from`/`tx_to`) | ✅ | `wire.rs` `Op::AsOf` + `TimeAxis` (EG-KG.compute.preserved/2.250) |
| Ebbinghaus time-decay recency weighting of facts | ✅ | `eg_core::decay::ebbinghaus_weight`; `GRAPH_SERVICE_DECAY_HALF_LIFE` |
| Hierarchical summary-node tier (`:SummaryNode` with level + provenance links; `summarize`/`rollup` primitive) | ✅ | representation-module ladder (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg) |
| Episodic→semantic consolidation primitive (localized, provenance + bitemporal preserving, importance-weighted) | ✅ | `graph.rs` `consolidate` (CONCEPT:EG-KG.compute.consolidate-cluster); tested |
| Memory maintenance: `reinforce`/`decay`/`evict_below`/`forget` (importance + access-count + last-access) | ✅ | deterministic, caller-supplied now (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) |
| LeanRAG hierarchical retrieval (vector-retrieve at summary level → drill down SUMMARIZES/CONSOLIDATES edges) | ✅ | bottom-up aggregation + top-down traversal (CONCEPT:EG-KG.retrieval.bounded-drill) |
| Natural-language → query (`Method::NlQuery`, `/nl`, `nl_query()` UDF) | ✅ | `nl-query` (CONCEPT:EG-KG.query.core-query-input/080); complete LLM-optional seam, inert until an OpenAI-compatible endpoint is set; AU-provider integration tracked on the agent-utilities side |
| Uncertainty-distribution-valued properties (Gaussian/Beta/Categorical/empirical) | ✅ | `graph.rs` `Distribution` accessors + Bayesian update/sampling (CONCEPT:EG-KG.compute.uncertainty-values) |
| Memory/scene/trajectory driven over the wire (additive `Method`s: CreateSummary/Consolidate/Maintain/SceneObject/Trajectory + dispatch + WAL replay) | ✅ | AU/MCP drive them remotely, no longer in-process only (CONCEPT:EG-KG.memory.eg-batch-decay-caller, exposing EG-087/099/220/221/222) |

## New data modalities (`eg-core` + leaf crates)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Document/JSON deep indexing: JSONPath + durable inverted path-index; PG `->`/`->>`/`@>`/`jsonb_path_query` + Mongo `$match` → `Pred::JsonPath` | ✅ | selectivity via the path index (CONCEPT:EG-KG.compute.json-deep-indexing); the inverted path-index **persists to redb + rehydrates at boot** and feeds planner cost `Stats` for selectivity (CONCEPT:EG-KG.storage.path-index-store) |
| Array/tensor store: dense/chunked N-D arrays CAS-backed + `Op::TensorScan`/`Op::TensorOp` (slice/reduce/elementwise) | ✅ | pure-Rust `eg-tensor`, feature `tensor` (CONCEPT:EG-KG.storage.content-addressed-dedup); derived tensors from `Op::TensorOp`/`Op::TensorScan` **write back** into the content-addressed store on the exec path — durable + dedup-shared (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink) |
| Scene-graph / 3D world model: `:SceneObject` pose + transform hierarchy + spatial relations + bounding volumes | ✅ | robotics/AR/urban-3D substrate (CONCEPT:EG-KG.compute.scene-graph-primitives) |
| Event-stream + CEP: windowed high-velocity ingest + `Op::Cep` bounded-NFA (sequence/within/absence) over sliding/tumbling windows | ✅ | `eg-stream`, feature `stream` (CONCEPT:EG-KG.query.pipelined-execution) |

## Robotics (`eg-core` + `eg-tensor` + `eg-tsdb`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Multimodal sensor fusion (camera/LiDAR/audio/tactile aligned via ASOF backward-join) → `Op::SensorFuse` | ✅ | composes EG-085 + EG-KG.query.pipelined-execution + tsdb ASOF (CONCEPT:EG-KG.query.multi-rate-sensor-stream) |
| Action/policy/trajectory memory (`:Trajectory` of `:Step{state,action,reward,next,t}`, discounted return, best/worst retrieval) | ✅ | policy-learning/replay substrate (CONCEPT:EG-KG.compute.discounted-return) |
| ROS2 bridge — engine CDC events ↔ ROS2 topics over the standard rosbridge-WebSocket JSON protocol (NO DDS C stack) | ✅ | `src/server/ros2_bridge.rs`, pure-Rust `tokio-tungstenite` client to a `rosbridge_server` (CONCEPT:EG-KG.domains.robotics-gpu-distribution); feature `ros2-bridge`, `full-extras`-only. A native DDS/RTPS wire (`ros2-dds`) is the second `full-extras` leg |

## LLM KV-cache (`eg-kvcache` — vs vLLM / LMCache)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Tiered hot/warm/cold KV-block cache (LRU + importance/recency; RAM → compressed-RAM → redb/blob; auto promote/demote) | ✅ | survives OOM by offloading (CONCEPT:EG-KG.memory.byte-bounded-tiers); warm tier uses real **zstd** (optional lz4) compression, not RLE (CONCEPT:EG-KG.storage.rle-codec-default) |
| Shared multi-instance KV backend (content-addressed, dedup, ref-count; lookup/publish by token-hash) | ✅ | `SharedKvBackend` (CONCEPT:EG-KG.enrichment.content-address-separation) |
| HTTP endpoint (GET/PUT/EXISTS a block by token-hash + stats) + vLLM/LMCache remote-backend connector contract | ✅ | `kvcache-server`, in the main build (CONCEPT:EG-KG.backend.is-configured-so-co) |

## OBDA / virtual graphs (`federation` + `eg-rdf`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| R2RML-style `:TriplesMap` mappings expose a foreign/relational source as a VIRTUAL RDF graph; SPARQL rewrites to `ForeignScan` (no materialization) | ✅ | completes Phase Q (CONCEPT:EG-KG.ontology.foreign-source-seam); parses standard **R2RML Turtle** documents (`rr:TriplesMap`/`rr:logicalTable`/`rr:subjectMap`/`rr:predicateObjectMap`/`rr:template`/`rr:column`) into the VirtualGraph model (CONCEPT:EG-KG.ontology.iri-template-object-map) |
| `Op::ForeignScan` resolution against a named `ForeignSource` registry (another graph / SQL table / remote endpoint) | ✅ | CONCEPT:EG-KG.query.closure-backed-source |

## Enterprise: security, backup, DR

| Operation | Status | Evidence |
|-----------|:------:|----------|
| RBAC-at-scale: durable roles + role hierarchy + resource/action grants over per-agent RLS + ACL + hash-chained audit | ✅ | `AgentRole` in eg-types::acl, `security` tier (CONCEPT:EG-KG.compute.feature); roles/grants + agent identities **persist to redb + reload at boot** (write-through on `RbacAdmin` mutations), so policy survives restart (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence) |
| Online backup / restore + PITR (`Method::Backup`/`Restore`, MVCC-snapshot verbatim bundle preserving encryption + audit chain) | ✅ | redb-only; clean "not available" on non-redb (CONCEPT:EG-KG.sharding.reshard-on-restore) |

## Lakehouse interop (eg-lake — LTAP)

Databricks-LTAP-interoperable: external lakehouse engines read the engine's own tables as open formats with
**zero ETL**. Gated `lake` (arrow/parquet + delta/iceberg deps), opt-in (not in the default build). See
[lakehouse-ltap](architecture/lakehouse_ltap.md).

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Parquet-on-object-store materialization of engine tables / columnar segments | ✅ | async columnar transcode → Parquet in the blob CAS / S3 (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) |
| Delta transaction log (`_delta_log`) so Delta / Databricks / delta-rs readers see a consistent version | ✅ | CONCEPT:EG-KG.storage.lsn-as-snapshot-returns |
| Iceberg-REST catalog + Iceberg snapshot metadata (Trino / Spark catalog resolution) | ✅ | CONCEPT:EG-KG.storage.lsn-as-snapshot-returns |
| Real Iceberg v2 **Avro** manifest + manifest-list writer (Spark/Trino/DuckDB read the tables) | ✅ | CONCEPT:EG-KG.storage.eg-iceberg-avro-manifest/EG-KG.storage.iceberg-manifest-list; `crates/eg-lake/src/iceberg_avro.rs`, `lake` feature (pure-Rust `apache-avro`); per-column stats (`value_counts`/`null_value_counts`/`lower_bounds`/`upper_bounds` by field-id) for predicate pushdown / file skipping (EG-KG.storage.iceberg-avro-manifest-carries); partition `field_summary` null by design (unpartitioned spec) |
| LSN-style as-of / time-travel snapshots (reusing versioned snapshots + `Op::AsOf`) | ✅ | a lake snapshot pins an exact engine LSN (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns) |

## Request scheduling & QoS

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Real-time QoS/SLO scheduler — per-tenant/priority admission + deadline scheduling + backpressure so latency-critical requests meet SLOs under load | ✅ | server/transport (CONCEPT:EG-KG.coordination.backpressure-busy-signal); complements the reserved read-admission lane (EG-KG.coordination.reserved-read-lane) |

## Analytics / numeric kernel (`eg-numeric` — feature `numeric`, in the main build)

The Analytics-Program kernel: one BLAS/LAPACK-free Rust kernel, two surfaces
(CONCEPT:AU-KG.compute.numeric-kernel). See [numeric_kernel.md](architecture/numeric_kernel.md).

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Array reductions / stats (`sum/mean/std/var/min/max/prod/argmin/argmax/argsort/percentile/quantile/cumsum/cumprod`) | ✅ | `crates/eg-numeric/src/reductions.rs`; parity `np.allclose` vs numpy |
| Element-wise (`sqrt/log/exp/abs/tanh/clip/maximum/minimum/where/nan_to_num/isnan`) | ✅ | `crates/eg-numeric/src/elementwise.rs`; nan/inf edge-cased |
| LAPACK-class linalg (`norm/dot/matmul/solve/svd/eigh/pinv/lstsq/qr/cholesky/det/inv/matrix_power`) — pure-Rust faer, **no system BLAS/LAPACK** | ✅ | `crates/eg-numeric/src/linalg.rs`; singular → LinAlgError (numpy parity) |
| Random (`normal/uniform/integers`, seedable, deterministic) | ✅ | `crates/eg-numeric/src/random.rs`; distributional parity |
| Surface A — Python extension `epistemic_graph.numeric` (zero-copy rust-numpy + `allow_threads`) → `agent_utilities.numeric.xp` | ✅ | feature `python` (separate maturin build), **folded into the one `epistemic-graph` wheel** — `pip install epistemic-graph[numeric]`, no separate `eg-numeric` package (AU-KG.compute.is-installed-kernel-discovery/EG-KG.compute.tensor-gpu-distance); consumed by AU-KG.backend.lmcache-native-connector shim |
| Surface B — SQL analytics UDFs/UDAFs over the kernel: `cosine_sim`/`l2_normalize`/`zscore` scalars + `covariance` UDAF (in-engine over resident columns) | ✅ | CONCEPT:EG-KG.query.surface-b-numeric-operators; `crates/eg-query/src/sql/numeric.rs`; `crates/eg-query/tests/numeric_udfs.rs` |
| Surface B — kernel-backed batch vector op via the client/Method path (`BatchL2Normalize`) | ✅ | CONCEPT:EG-KG.compute.l2-normalize-batch-vectors; `src/server/handlers/graph_ops.rs`; `client.batch_l2_normalize()` |
| Surface B — `svd(vec_col)`/`pca(vec_col,k)` column→matrix SQL UDAFs (aggregate a vector column into a dense matrix → faer `svdvals`/`eigh`; singular values / top-k principal-component directions) | ✅ | CONCEPT:EG-KG.query.svd-eg-pca-column/EG-KG.query.concept-6; `crates/eg-query/src/sql/numeric.rs`; `crates/eg-query/tests/numeric_udfs.rs` |
| Surface B — `kmeans(vec_col,k)` column→matrix clustering UDAF (one `List<Int64>` cluster label per row; pure-Rust Lloyd + k-means++ kernel, **no linfa/BLAS**, deterministic seed) | ✅ | CONCEPT:EG-KG.query.kmeans-clustering-half-one; `crates/eg-numeric/src/cluster.rs`; `crates/eg-query/src/sql/numeric.rs` |
| Surface B — **cross-modal join → analytics in-engine**: join graph ⋈ vector ⋈ timeseries, then `pca`/`kmeans`/`covariance` over the joined result set (the numpy-surpassing differentiator — no fetch-to-Python) | ✅ | CONCEPT:EG-KG.query.eg-3; `crates/eg-query/tests/cross_modal_analytics.rs` |
| Surface B — graph-algo/timeseries unification via native `Method` surfaces (beyond SQL) | 🗺 | next P4 increment |

## Durability & distribution

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb-authoritative, commit-before-ack (`kill -9`-safe) | ✅ | `redb_backend.rs` `record_durable` |
| Cross-modal ACID (graph + vector + blob in one WriteTransaction) | ✅ | shared redb transaction |
| Cross-modal ACID, time-series measurements included | ✅ (precise boundary below) | `graph`+`vector`+`blob-ref`+`measurement` (+ lowered axiom/CONSTRUCT/plan-writeback) land in ONE `graph.redb` `WriteTransaction` — that set, and only that set, is the atomic boundary (`redb_store.rs::commit_crossmodal`). The measurement is then replayed into the SERVED `series.redb` (`state.tsdb_store`) immediately after that commit succeeds, so `TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill`/UQL `Op::TsScan` see it post-commit AND post-restart (CONCEPT:EG-KG.backend.ts-served-materialize, EG-P0-4). That replay is a SEPARATE, non-atomic redb write on a different file: a crash strictly between the two commits leaves the measurement durable in `graph.redb` but not yet visible via the served store (no auto-reconciliation pass yet) — everything else in the set has no such second hop or gap. |
| openraft replication + automatic failover (`raft`/`cluster`) | ✅ | `src/raft/mod.rs` |
| Cross-shard 2PC (presumed-abort, crash-recoverable) | ✅ | `src/raft/cross_shard_txn.rs` |
| Multi-Raft groups (N-group ring, online reshard, hibernate/rehydrate) | ✅ | `src/raft/multi.rs` `MultiRaft`/`GroupRouter` (KG-2.266/267/268); `reshard.rs` online ownership move |
| Parallel-commit + read-only-participant fast path + non-blocking (Raft-replicated decision) commit | ✅ | parallel prepare + empty-write-set skip (CONCEPT:EG-KG.txn.cross-shard) and the Paxos-Commit-lite Raft-replicated commit decision (CONCEPT:EG-KG.txn.harness-crash) over the working 2PC |
| Full Calvin deterministic-ordering commit | ✅ | a global `CalvinSequencer` total order + Raft-replicated input log + vote-free deterministic execution + crash-replay recovery — a third commit branch opt-in via `calvin` alongside 2PC + Paxos-Commit-lite (CONCEPT:EG-KG.txn.calvin-deterministic-ordering). OLLP distributed read-lock + multi-node sequencer fan-in remain (see [roadmap](roadmap.md)) |
| Cross-region async read-replica tier + capacity guardrails | ✅ | bounded-LSN replication log + `/replicate` serve + async follower apply (CONCEPT:EG-KG.sharding.follower-pull-loop); circuit-breaker + per-tenant quota + backpressure guards (CONCEPT:EG-KG.coordination.circuit-breaker). `federation-search`, in the main build |
| Federation (remote/HTTP/external SQL) | ✅ | `federation`(`-sql`), in the main build; activates when a foreign source is registered |

See the [parity roadmap](roadmap.md) for the order in which the 🔶 / 🗺 items are being closed.

## Known limitations (in progress: EG-P0-2/P0-4/P0-6)

The EG-P0-1 capability ledger (`docs/capabilities.generated.md`) and its consistency test
(`crates/eg-capabilities/tests/consistency.rs`) are the authoritative source for exactly what is and
isn't closed at the per-`Method` level; this section restates the open items here too so this
hand-maintained page doesn't silently read as more complete than it is:

- **WAL-durability gaps.** EG-P0-3 closed 5 of the 23 write-classified-but-not-WAL-logged methods the
  ledger flags (`AddEmbedding`, `MineSequence`, `MineForecast`, `MineText` for `lda`/`nmf`,
  `MineSubgraph` for `gspan` — see [mining.md](mining.md)). **18 remain open** per the ledger today
  (e.g. `ApplyMutation`, `ApplyLedger`/`ClearLedger`, `Reconcile`, `RunDatalogReasoning`,
  `CompactNodesByType`, and the write-conditional `Sql`/`CypherQuery`/`GraphQl` paths) — a crash
  between checkpoints can still silently lose those. **Not solved by Wave 1**; do not read the
  matrix above as implying otherwise.
- **Runtime-conditional classification (EG-P0-2).** 22 methods (the `Mine*`/`GraphLearn*` family plus
  `Sql`/`CypherQuery`/`GraphQl`) only know whether they mutate/durably-log at **request time** (a
  `writeback` flag, or a parsed query) — the ledger's static `mutates: true` for these is a
  conservative upper bound, not an equality. Open.
- **Access-classifier coverage gaps.** 61 methods (channel/trigger/continuous-query/catalog/identity/
  RBAC/matview/foreign-source/UDF-registration/blob/`Txn*` ops) are absent from `access.rs`'s write
  classifier entirely. Some are legitimately governed by a different mechanism already (the `Txn*`
  family self-routes and is gated once at `BeginTxn`; KV/blob/series ops are namespace-scoped and
  self-route too); the rest are a genuinely open, unassigned triage item. Open, unassigned.
- **Time-series is a separate durability domain.** Time-series writes persist to their own
  `series.redb` (`DurabilityDomain::SeriesRedb`), entirely separate from the graph WAL/`graph.redb`
  this page and the ledger otherwise describe — the graph-durability discussion above does not cover
  the time-series write path.
- **No single enforcement gateway yet.** The ledger today is a **one-way audit** (does the
  declarative policy table agree with the existing imperative classifiers?), not yet the thing the
  handlers consult at request time — `access.rs`/`wal.rs`/`audit.rs`/`cdc.rs` each still carry their
  own separate logic. Unifying them behind one `policy()`-driven `MutationPlan` gateway is
  EG-P0-2/EG-P0-6 and has not started.

None of the above is new as of this note — it restates what EG-P0-1 already shipped documenting, so
that it's visible from this page and not only from the generated file.
