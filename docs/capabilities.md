# Capabilities & parity matrix

This page is the **operation-by-operation truth table** for epistemic-graph as a universal database. It
is verified against the source, not against intent. Legend:

- **✅ supported** — implemented and covered by tests.
- **🔶 in-progress** — partially built or actively being added; the unsupported part errors honestly.
- **🗺 roadmap** — designed, not yet built.

The **Feature** column is the Cargo feature that gates the surface; the
[tiers page](architecture/tiers.md) shows which prebuilt binary carries it.

## SQL (`eg-query/sql` + pgwire)

| Operation | Status | Feature | Evidence |
|-----------|:------:|---------|----------|
| `SELECT` — joins, aggregates, GROUP BY/HAVING, window, CTE, subquery, UNION | ✅ | `query` | DataFusion 43; `classify.rs` routes `Query` → `Read`, executed in `exec.rs` |
| Predicate pushdown into the `nodes` scan | ✅ | `query` | `NodesTableProvider` returns `Inexact`, narrows rows via a per-column equality index (`providers.rs`) |
| `INSERT` / `UPDATE` / `DELETE` on the `nodes` table | ✅ | `query` | `classify_insert`/`classify_update`/`classify_delete` (KG-2.198); maps to `add_node` / `compare_and_set_fields` / `remove_node` |
| `RETURNING` on DML | ✅ | `query` | pgwire write path returns affected rows |
| Compound WHERE DML (`AND`/`OR`/`NOT`/`IN`/`BETWEEN`/ranges/`IS NULL`) | ✅ | `query` | `RowPredicate` in `classify.rs`; serializable `compare_and_set_fields_if`/`remove_node_if` re-check under the write guard (CONCEPT:EG-045) |
| `INSERT INTO nodes … SELECT` (may JOIN user tables + graph) | ✅ | `query` | `InsertNodesSelect` via `apply_node_insert_row` (CONCEPT:EG-046) |
| Multi-table DML (`UPDATE … FROM` / `DELETE … USING`) | ✅ | `query` | matched ids resolved via DataFusion, applied via serializable CAS/remove gates (CONCEPT:EG-047) |
| `ON CONFLICT (cols) DO NOTHING/DO UPDATE` upsert + user-table `RETURNING` | ✅ | `query` | reuses unique/PK validation (CONCEPT:EG-048) |
| Mixed-store wire transactions (`BEGIN`/`COMMIT`/`ROLLBACK`, `TransactionStatus` `T`/`E`/`I`) | ✅ | `pgwire` | `GraphTxnBuffer` + user-table ops, read-your-own-writes overlay; documented non-2PC user-table window (CONCEPT:EG-049) |
| `CREATE VIEW` / `DROP VIEW` (durable catalog, expanded in `build_ctx`) | ✅ | `query` | CONCEPT:EG-072 |
| `CREATE FUNCTION … LANGUAGE sql` (scalar + table UDFs, durable catalog) | ✅ | `query` | CONCEPT:EG-118; PL/pgSQL control-flow is a documented follow-up |
| Columnar (struct-of-arrays) segments + SQL window frames (`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`/`OVER(PARTITION BY … ROWS/RANGE …)`) | ✅ | `query` | CONCEPT:EG-089 |
| DML on arbitrary user tables (`INSERT`/`UPDATE`/`DELETE`, `INSERT … SELECT`, `COPY`) | ✅ | `query` | durable redb `TableStore` (EG-018/EG-020); `run_insert_table`/`run_update_table`/`run_delete_table` |
| `CREATE` / `ALTER ADD COLUMN` / `DROP TABLE`, arbitrary user tables, DDL | ✅ | `query` | `crates/eg-query/src/tables/` durable catalog (EG-018); JOINable to the graph; `ALTER` beyond ADD COLUMN 🔶 |

### Postgres wire (`pgwire`, also pulled by `cluster`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| TCP listener, gated on `EPISTEMIC_GRAPH_PGWIRE_ADDR` | ✅ | `src/server/pgwire/mod.rs` `serve`/`serve_with_auth` |
| Simple query protocol | ✅ | `SimpleQueryHandler` |
| Extended / prepared protocol (Parse/Bind/Describe/Execute, `$N` params) | ✅ | `ExtendedQueryHandler`, `substitute_params` |
| SCRAM-SHA-256 auth (pg user → engine ACL actor) | ✅ | `auth.rs` `PgWireAuthMode::Scram` (KG-2.202) |
| Trust auth (dev) | ✅ | `auth.rs` `PgWireAuthMode::Trust` |
| `pg_catalog` / `information_schema` introspection | ✅ | `register_pg_catalog` + DataFusion `with_information_schema` (KG-2.201) |
| `pg_catalog.pg_class/pg_namespace/pg_attribute/pg_type/pg_index/pg_proc` + `information_schema.tables/columns/schemata/views/routines` so `psql \d`/`\dt`/`\l`, ORMs + BI tools introspect | ✅ | synthesized from live table/view/function catalogs incl. `pg_table_is_visible`/`format_type`/`current_schema` (CONCEPT:EG-103) |
| `SET graph = '<name>'` connection switch | ✅ | `mod.rs` |

### Postgres-extension drop-in (`pgwire` + eg-query/eg-ann/eg-tsdb/eg-text)

`CREATE EXTENSION` records the enabled extension in a durable catalog and gates the family surfaces — the keystone that lets an unmodified Postgres client/ORM connect.

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `CREATE EXTENSION vector/pg_age/timescaledb/pg_search` + durable extension catalog | ✅ | eg-query classify + pgwire (CONCEPT:EG-102) |
| PostgreSQL array (`int[]`/`text[]`, `ANY`/`ALL`/`unnest`/`array_agg`/`@>`/`&&`) + range types + common scalar functions (`string_agg`/`split_part`/`regexp_replace`/`to_char`/`date_trunc`/`generate_series`) | ✅ | DataFusion UDF/UDTF registration (CONCEPT:EG-104) |
| **pgvector**: `vector(n)` type + `<->` (L2) / `<=>` (cosine) / `<#>` (neg-inner) operators, pgwire type OID | ✅ | CONCEPT:EG-115 |
| **pgvector** index pushdown: `ORDER BY emb <-> $1 LIMIT k` → eg-ann index; `CREATE INDEX … USING hnsw/ivfflat` | ✅ | CONCEPT:EG-116 |
| **Apache AGE**: `SELECT * FROM cypher('graph', $$ MATCH … RETURN … $$) AS (a agtype)` routed to the eg-query cypher engine | ✅ | CONCEPT:EG-114 |
| **TimescaleDB**: `create_hypertable()`, `time_bucket()` gap-fill, `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` continuous aggregates | ✅ | lowered onto eg-tsdb + `Op::Window` (CONCEPT:EG-117) |
| **ParadeDB**: `@@@` BM25 search operator + `score()`/`snippet()` `paradedb.*` functions | ✅ | lowered onto eg-text BM25 (CONCEPT:EG-119) |

## SPARQL (`eg-rdf`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `SELECT` query form | ✅ | `sparql.rs` `Query::Select` arm |
| Algebra: BGP, property paths, FILTER subset, OPTIONAL, UNION, GROUP/aggregate, BIND, DISTINCT, REDUCED, SLICE | ✅ | `eval_pattern` match in `sparql.rs` |
| Aggregates COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/SAMPLE | ✅ | `sparql.rs` aggregate evaluator |
| `ASK` / `CONSTRUCT` / `DESCRIBE` | ✅ | `sparql.rs` `Query::{Ask,Construct,Describe}` arms; `construct_graph`/`describe_resources` (gated by `rdf`, implied by `sparql`) |
| `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`) | ✅ | `eg-rdf/src/update.rs` over a `GraphStore`; `LOAD` intentionally deferred (no HTTP fetch in write path) |
| `/sparql` HTTP endpoint (W3C SPARQL 1.1 Protocol) | ✅ | `src/server/sparql_http.rs` (EG-017), feature `sparql-http`; GET + POST query/update |
| True named graphs (multi-graph quad querying) + `FROM` / `FROM NAMED` dataset spec | ✅ | `Dataset` over every registry graph; `GRAPH ?g`/constant-IRI scoping (CONCEPT:EG-054) |
| `ORDER BY` total-ordering (multi-key ASC/DESC, top-k), `VALUES` inline data, `MINUS`, `EXISTS`/`NOT EXISTS` | ✅ | fixes the unordered-results correctness gap (CONCEPT:EG-135 / EG-125 / EG-055) |
| Negated property set `!p` | ✅ | scan edges whose predicate ∉ set (CONCEPT:EG-056) |
| SPO/POS/PSO triple index + cardinality-based BGP reordering | ✅ | replaces full-scan-per-pattern (CONCEPT:EG-057) |
| Rich FILTER: arithmetic, `REGEX`, `IN`, `STR`/`LANG`/`DATATYPE`/`BOUND`, `isIRI`/`isLiteral`, string fns (`CONTAINS`/`STRSTARTS`/`SUBSTR`/`CONCAT`), `COALESCE`/`IF` | ✅ | datatype-aware comparison (CONCEPT:EG-053) |
| Builtin-function library: term constructors (`IRI`/`BNODE`/`STRDT`/`UUID`), hashes (`MD5`/`SHA*`), date-time (`NOW`/`YEAR`…), numeric (`RAND`/`ABS`/`ROUND`), string extras (`STRBEFORE`/`REPLACE`/…) | ✅ | CONCEPT:EG-127 |
| Sub-SELECT (nested `{ SELECT … }`) | ✅ | restrict inner solutions to projected vars (CONCEPT:EG-051) |
| SERVICE federation (`SERVICE <ep> { … }`, SILENT, SSRF allowlist) | ✅ | `ureq` remote client, feature `sparql-service` (CONCEPT:EG-052) |
| Content negotiation (SPARQL-results JSON/XML/CSV/TSV, Turtle/N-Triples out) | ✅ | hand-written serializers + `oxttl` (CONCEPT:EG-050) |
| Graph Store Protocol + `COPY`/`MOVE`/`ADD` | ✅ | `/rdf-graphs/…?graph=` (CONCEPT:EG-134) |
| RDF-star / SPARQL-star (RDF 1.2 quoted triples) | ✅ | CONCEPT:EG-130 |

### RDF serialization + validation (`eg-rdf` + `eg-shacl`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Serialization matrix: JSON-LD 1.1 (expansion/compaction), TriG, N-Quads, RDF/XML | ✅ | CONCEPT:EG-136 / EG-137 / EG-131 (alongside Turtle/N-Triples) |
| SHACL validation (node/property shapes, cardinality/datatype/pattern/`sh:in`/logical + SPARQL constraints → `sh:ValidationReport`) | ✅ | pure-Rust `eg-shacl`, `Method::ShaclValidate` (CONCEPT:EG-132) |
| ShEx (Shape Expressions) validation | ✅ | `Method::ShexValidate` (CONCEPT:EG-133) |
| ICV integrity-constraint validation (Stardog-style, closed-world/UNA; guard mode rejects violating writes; also runs over the OWL-reasoned view) | ✅ | CONCEPT:EG-146 |

## OWL reasoning (`eg-rdf/owl`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| OWL 2 EL⁺ completion (CR-sub/conj/some/chain/subrole/bot/disjoint) | ✅ | `owl.rs` `saturate()` |
| OWL 2 RL property rules (transitive/symmetric/inverse/chains/domain) | ✅ | `parse_ontology` + completion |
| Classification, consistency (unsat → inconsistent) | ✅ | `owl.rs` `snapshot()` |
| Forward-chaining materialization + incremental `add_axioms` | ✅ | monotone fixpoint, resumed in place |
| Confidence-weighting (per-axiom `eg:confidence`, noisy-OR) | ✅ | `classify_weighted()` (KG-2.236) |
| Ebbinghaus time-decay of facts | ✅ | `eg_core::decay::ebbinghaus_weight`; `GRAPH_SERVICE_DECAY_HALF_LIFE` |
| Distributed / cross-shard reasoning (union TBox+ABox, one closure) | ✅ | `reason_distributed_weighted` |
| Query-time `Op::Reason` (reasoner seeds a RowSet) | ✅ | `wire.rs` `Op::Reason` under `owl-plan`; executor seeds class members |
| OWL-DL (tableau, cardinality, `complementOf`, nominals) | 🔶 | EL approximations of `allValuesFrom`/`hasValue` present; full tableau being added behind `owl-dl` |
| SWRL user rules | 🔶 | `rules.rs` Horn-rule DSL (`body → head @conf`) with range-safety; SWRL/RuleML atoms + built-in library being added |

## Cypher (`eg-query/cypher`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `MATCH … WHERE … RETURN … LIMIT` (read-only, one snapshot) | ✅ | `exec_cypher(&GraphView, …)` |
| Patterns: labels, typed edges, both directions, var-length single hop | ✅ | `parser.rs` / VF2 + petgraph BFS |
| WHERE: `=, <>, !=, <, <=, >, >=`, AND-joined | ✅ | `parse_predicates` |
| Writes (`CREATE`/`MERGE`/`SET`/`DELETE`+`DETACH`) | ✅ | `exec_cypher_write` → `apply_create`/`apply_merge`/`apply_set`/`apply_delete` over eg-core mutations |
| `REMOVE` (property delete + label removal) | ✅ | `WriteOp::Remove` → `apply_remove` (CONCEPT:EG-061) |
| `ORDER BY` / `SKIP` / `WITH` / `OPTIONAL MATCH` / `OR`+`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL`, aggregation (`count`/`collect`/`sum`/`avg`/`min`/`max`), `RETURN DISTINCT`/`*` | ✅ | parser + executor (CONCEPT:EG-062) |
| Variable-length hop combined with fixed hops + path-variable binding | ✅ | relaxes the single-hop guard (CONCEPT:EG-063) |
| `UNWIND expr AS var` | ✅ | composes with WITH/MATCH pipeline (CONCEPT:EG-141) |
| `CALL { subquery }` + `CALL proc(args) YIELD …` procedure framework | ✅ | invocation registry → native/WASM procedures (CONCEPT:EG-142) |
| APOC-equivalent + GDS surface via `CALL gds.*` (PageRank, WCC/SCC, Louvain, betweenness/degree centrality, Dijkstra, node similarity) | ✅ | pure-Rust eg-compute (CONCEPT:EG-143 / EG-144) |

## GraphQL (`eg-graphql`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Read queries: root label fields, `first`/`limit`, property filters, aliases, nested edge selections | ✅ | `resolver.rs` scan + BFS; schema introspected from the live graph |
| Mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`) | ✅ | `mutation.rs` `execute` over eg-core mutations; OCC bumped once per batch |
| Apollo Federation v2 subgraph: `_service { sdl }` + `_entities(representations:[_Any!]!)`, `@key`/`@shareable`/`@external` directives | ✅ | so the engine is a federated subgraph in an Apollo supergraph (CONCEPT:EG-295) |
| Enterprise hardening: automatic persisted queries (APQ), query depth + complexity/cost limits, field/node caps, introspection toggle | ✅ | protects the federated subgraph in production (CONCEPT:EG-296) |
| Subscriptions | 🔶 | poll-only stub (`subscription.rs`); broadcast/CDC + WS/SSE being added (EG-064) |
| Fragments / variables / directives / relay pagination | 🔶 | rejected at parse today; being added (EG-065/066) |

## Vector / ANN (`eg-ann` + `eg-core`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| IVF-PQ index (coarse quantizer + 8-bit PQ codes) | ✅ | `ivfpq.rs` `train` |
| OPQ learned rotation (polar/SVD update) | ✅ | `ivfpq.rs` rotation update |
| SQ8 refine re-rank tier | ✅ | `ivfpq.rs` over-fetch + SQ8 re-rank (recall ≥ 0.95 in tests) |
| Persistent index — reopen WITHOUT rebuild (mmap codes, O(N) posting rebuild) | ✅ | `persist.rs` `open` |
| Parallel + SIMD brute-force fallback (rayon, contiguous arena, cached L2 norms) | ✅ | `semantic_store_ann.rs` |
| Warm-on-start (index built off the query path) | ✅ | `warm()` / `ensure_index` |
| Hybrid metadata pre-filter (kNN with an `allow(id)` predicate) | ✅ | `ivfpq.rs` `search_filtered` (EG-070); tested DURING the ADC probe, not post-filter |
| Exact/flat kNN index (ground-truth) + ANN-candidate re-rank + recall@k/precision self-eval harness | ✅ | brute-force exact + hybrid refinement (CONCEPT:EG-297) |
| Cross-shard kNN merge | 🔶 | scatter-gather over per-shard eg-ann indexes → global top-k (CONCEPT:EG-069); `merge_topk` is the leaf primitive |

## Time-series (`eg-tsdb`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb columnar store, group-amortized `append_batch`, range scan, retention | ✅ | `store.rs` |
| `time_bucket` (avg/min/max/sum/count/first/last) | ✅ | `query.rs` |
| ASOF backward join (the primitive DataFusion lacks) | ✅ | `query.rs` `asof_join_backward` |
| gap-fill LOCF, OHLC bars, downsample/rollup, decay-weighted mean, EWMA, rolling z-score | ✅ | `query.rs` |
| Time-ops as unified planner ops (`Op::Window` execution) | 🔶 | `Op::Window` is a pass-through seam in `eg-plan/exec.rs` today |
| Per-point retention trim (vs whole-bucket drop) | 🗺 | deferred |

## Blob / CAS (`src/server/blob`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Content-addressed (sha256) streaming store, manifest of chunk digests | ✅ | `store.rs` `ChunkStore` |
| redb-native backend, group-commit, refcount mark-and-sweep GC | ✅ | `RedbChunkStore` |
| S3 / MinIO backend behind the same `ChunkStore` trait | ✅ | `s3.rs` `S3ChunkStore` (`blob-s3`) |
| Content-defined chunking | 🗺 | fixed 2 MiB chunks today |

## Key-value / embedded (`redb`, `embedded`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Embedded in-process engine over redb rows (no Tokio/socket/HMAC, commit-before-return) | ✅ | `src/embedded.rs` `EmbeddedEngine::open` (KG-2.216) |
| Durable graph-shaped tables (`nodes`/`edges`/`ledger`/`semantic_store`/`graph_meta`) | ✅ | `src/redb_store.rs` |
| Generic namespaced `get`/`put`/`delete`/`scan`/`cas` KV surface over redb | ✅ | `src/server/kv.rs` `KvStore` (EG-022); `Method::Kv*`; durable, commit-before-ack; not graph-scoped |

## Wire protocols & interop endpoints (`WireProtocol` + hand-rolled listeners)

Every listener is **opt-in** — it starts only when the binary is built with its feature
AND its `EPISTEMIC_GRAPH_*_ADDR` env var (or CLI flag) is set. The SQL wires share the
one wire-neutral `classify → dispatch → exec` core (`src/server/wire`, CONCEPT:EG-074) —
no SQL is reimplemented per wire. See [`interfaces/connecting.md`](interfaces/connecting.md)
for per-wire connect+query recipes and the full env-var/port table.

| Surface | Status | Feature | Evidence |
|---------|:------:|---------|----------|
| Wire-neutral SQL core (`WireProtocol`/`WireSession`) | ✅ | `wire` | `src/server/wire` (EG-074); shared by every SQL wire |
| Postgres wire (psql / BI / ORM) | ✅ | `pgwire` | `src/server/pgwire`; `EPISTEMIC_GRAPH_PGWIRE_ADDR` (KG-2.189) |
| MySQL / MariaDB wire (hand-rolled handshake v10 + `mysql_native_password`) | ✅ | `mysql-wire` | `src/server/mysql_wire`; `EPISTEMIC_GRAPH_MYSQL_ADDR` (EG-076) |
| MSSQL TDS wire (hand-rolled TDS) | ✅ | `mssql-wire` | `src/server/mssql_wire`; `EPISTEMIC_GRAPH_MSSQL_ADDR` (EG-077) |
| SQLite-dialect NDJSON-over-TCP endpoint | ✅ | `sqlite-wire` | `src/server/sqlite_wire`; `EPISTEMIC_GRAPH_SQLITE_ADDR` (EG-075); `.db` file I/O is a documented follow-up |
| Neo4j Bolt v4.4 wire (PackStream v2, native Cypher) | ✅ | `bolt-wire` | `src/server/bolt_wire`; `EPISTEMIC_GRAPH_BOLT_ADDR` (EG-159) |
| AMQP 0.9.1 broker wire (exchanges/queues over the KG-2.303 work-queue) | ✅ | `amqp-wire` (impl `broker`) | `src/server/amqp_wire`; `EPISTEMIC_GRAPH_AMQP_ADDR` (EG-275) |
| MQTT 3.1.1/5.0 broker wire (CONNECT/PUBLISH/SUBSCRIBE, QoS 0/1) | ✅ | `mqtt-wire` (impl `broker`) | `src/server/mqtt_wire`; `EPISTEMIC_GRAPH_MQTT_ADDR` (EG-281) |
| STOMP 1.2 broker wire (CONNECT/SEND/SUBSCRIBE/ACK) | ✅ | `stomp-wire` (impl `broker`) | `src/server/stomp_wire`; `EPISTEMIC_GRAPH_STOMP_ADDR` (EG-282) |
| Redis RESP2/RESP3 wire (GET/SET/DEL/EXPIRE/INCR, HSET/HGET, LPUSH/LRANGE, SADD/SMEMBERS, ZADD/ZRANGE, scan) over the KV surface | ✅ | `redis-wire` | `src/server/redis_wire`; `EPISTEMIC_GRAPH_REDIS_ADDR` (EG-174) |
| S3-compatible REST (bucket + object PUT/GET/DELETE/HEAD/List, SigV4-lite) over the blob CAS | ✅ | `s3-api` | `src/server/s3` (EG-176) |
| GraphQL SSE subscription carrier | 🔶 | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR`; poll-only broadcast today |

## Message broker (`broker` — surpasses RabbitMQ)

Built on the KG-2.303 native engine task queue (`ClaimNext`): durable exchanges/queues live as `__control__` graph nodes, drive additive `Method::*` ops, and are Raft/WAL-safe.

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Exchanges (direct/topic/fanout) + bindings/routing-keys + queues | ✅ | RabbitMQ-class routing (CONCEPT:EG-275) |
| Dead-letter queues (max-delivery / reject → DLX, metadata preserved) | ✅ | CONCEPT:EG-276 |
| Per-message + per-queue TTL / queue expiry (lazy sweep + reaper) | ✅ | CONCEPT:EG-277 |
| Priority queues (priority band, FIFO within band) | ✅ | CONCEPT:EG-278 |
| Delayed / scheduled delivery (deliver-after / deliver-at) | ✅ | due-time index (CONCEPT:EG-279) |
| Consumer groups + per-consumer QoS/prefetch + fair round-robin + visibility leases | ✅ | CONCEPT:EG-280 |
| Replayable append-log streams (Kafka/RabbitMQ-Streams: retained ordered offset log, read-from-offset, retention by size/age) | ✅ | CONCEPT:EG-283 |
| Publisher confirms (monotonic delivery-tag) + consumer manual ack/nack-with-requeue (at-least-once) | ✅ | CONCEPT:EG-284 |

## Observability endpoints (`obs` listener, CONCEPT:EG-160/172/163)

The obs listener (`EPISTEMIC_GRAPH_OBS_ADDR`, default `127.0.0.1:5080`) fronts the
logs + metrics + traces trilogy over the durable eg-tsdb series + eg-text index.

| Surface | Status | Feature | Evidence |
|---------|:------:|---------|----------|
| Log ingest (OTLP/HTTP, Elasticsearch `_bulk`/`_doc`, JSON-lines, syslog) + Parquet-on-CAS segments | ✅ | `obs` | `src/server/obs` (EG-160/161) |
| Log search + query API (DataFusion over Parquet segments ∪ hot tsdb ∪ eg-text BM25; O2/ES `/_search`) | ✅ | `obs` | CONCEPT:EG-162 |
| VRL-style ingest pipelines (parse/json-extract, filter/drop, set/rename/remove, coerce, route-to-stream; cross-modal graph enrichment) | ✅ | `obs` | pure-Rust DSL → staged executor (CONCEPT:EG-165) |
| PromQL + Prometheus HTTP query API (`/api/v1/query[_range]`, `/api/v1/labels`) | ✅ | `promql` | `src/server/promql` + `eg-tsdb/promql` (EG-172) |
| Distributed traces (OTLP-JSON `POST /v1/traces`, `/api/traces` search, service-dependency graph) | ✅ | `traces` | `src/server/traces` (EG-163) |
| Super-cluster federated search (fan a read out to peer instances, merge/dedup/re-rank, per-peer timeout + partial-result tolerance) | ✅ | `federation`/`cluster` | `/federated` HTTP entry (CONCEPT:EG-243) |
| OpenTelemetry span export + slow-query log | ✅ | `otel` | CONCEPT:EG-091 |
| Prometheus `/metrics` exposition (engine's own counters/gauges) | ✅ | `metrics` (default) | `--metrics-addr` / `GRAPH_SERVICE_METRICS_ADDR`, default `127.0.0.1:9101` |

## Unified planner & UQL (`eg-plan`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| RowSet ops: `Scan, Filter, Traverse, Rank, RankNodeDistance, RankMentions, RankMmr, AsOf, Limit` | ✅ | `wire.rs` `Op`; `exec.rs` arms |
| `RankText`, `FuseRrf` | ✅ | `text` feature |
| `Reason`, `SparqlBgp` | ✅ | `owl-plan` feature |
| `Udf` (sandboxed WASM) | ✅ | `wasm-udf` feature |
| `ForeignScan` (remote engine / HTTP-JSON / external SQL) | ✅ | `federation` / `federation-sql` |
| `Window`, `Foreign` execution | 🔶 | pass-through seams in `exec.rs` today |
| UQL text DSL → `wire::Plan` (dependency-free parser) | ✅ | `eg-plan/src/uql` (KG-2.214) |
| `SpatialScan` / `Pred::SpatialWithin`/`SpatialDWithin` | ✅ | `geo` feature (EG-083); eg-geo executor + leaf crate |
| `TensorScan` / `TensorOp`, `Cep` event-stream match | ✅ | `tensor` (EG-085) / `stream` (EG-088) |
| Natural-language → query (`Method::NlQuery`, `/nl` route) | 🔶 | `nl-query` (EG-078/080); the NL→UQL planning SEAM ships and is LLM-optional — inert (clear "not configured" error, never a panic) until an OpenAI-compatible endpoint is configured |

## Spatial / GIS (`geo` + `geosparql`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| CRS registry + affine/Helmert reprojection | ✅ | `eg-geo` (EG-262) |
| Durable STR-packed R-tree spatial index | ✅ | `eg-geo` (EG-263) |
| GeoJSON / WKB / GPX / WKT I/O | ✅ | `eg-geo` (EG-262/264, `geo-io`) |
| `Op::SpatialScan` + `SpatialWithin`/`SpatialDWithin` predicates in a UnifiedQuery | ✅ | `geo` feature (EG-083) |
| DE-9IM topological relations (contains/covers/touches/crosses/overlaps/equals/disjoint) | ✅ | `eg-geo/predicates.rs` (CONCEPT:EG-258) |
| Constructive geometry algebra (buffer, convex hull, union/intersection/difference, simplify, centroid) | ✅ | `Op::SpatialOp` (CONCEPT:EG-259) |
| Geodesic ops (Haversine/Vincenty distance + geodesic area; CRS tag selects planar vs geodesic) | ✅ | CONCEPT:EG-256 |
| Full geometry model (Multi*/GeometryCollection + polygon holes + EWKT) | ✅ | CONCEPT:EG-257 |
| Map tiling: XYZ/TMS addressing + Mapbox Vector Tiles (MVT) clipped to a tile | ✅ | web-map render (CONCEPT:EG-265) |
| Weighted routing (Dijkstra/A* geo-heuristic) + isochrones + nearest-neighbour/2-opt TSP | ✅ | logistics primitives (CONCEPT:EG-266) |
| Map-based task tracking (`:GeoTask` location/status/service-area; within-bbox/polygon, nearest-N, along-route, nearest-resource assignment) | ✅ | field-ops layer (CONCEPT:EG-267) |
| OGC GeoSPARQL `geo:`/`geof:` vocabulary + spatial FILTER functions over SPARQL | ✅ | `geosparql` feature (EG-261); reuses eg-geo (no GEOS/PROJ) |
| RCC8 + Egenhofer topological relation families over GeoSPARQL | ✅ | lowered onto DE-9IM (CONCEPT:EG-155) |

## Agent-native memory (`eg-core` — arxiv 2606.24775; vs Zep / mem0 / LeanRAG)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Bi-temporal `AsOf` (valid-time + transaction-time `tx_from`/`tx_to`) | ✅ | `wire.rs` `Op::AsOf` + `TimeAxis` (KG-2.249/2.250) |
| Ebbinghaus time-decay recency weighting of facts | ✅ | `eg_core::decay::ebbinghaus_weight`; `GRAPH_SERVICE_DECAY_HALF_LIFE` |
| Hierarchical summary-node tier (`:SummaryNode` with level + provenance links; `summarize`/`rollup` primitive) | ✅ | representation-module ladder (CONCEPT:EG-220) |
| Episodic→semantic consolidation primitive (localized, provenance + bitemporal preserving, importance-weighted) | ✅ | `graph.rs` `consolidate` (CONCEPT:EG-221); tested |
| Memory maintenance: `reinforce`/`decay`/`evict_below`/`forget` (importance + access-count + last-access) | ✅ | deterministic, caller-supplied now (CONCEPT:EG-222) |
| LeanRAG hierarchical retrieval (vector-retrieve at summary level → drill down SUMMARIZES/CONSOLIDATES edges) | ✅ | bottom-up aggregation + top-down traversal (CONCEPT:EG-195) |
| Natural-language → query (`Method::NlQuery`, `/nl`, `nl_query()` UDF) | 🔶 | `nl-query` (CONCEPT:EG-078/080); LLM-optional seam, inert until an OpenAI-compatible endpoint is configured |
| Uncertainty-distribution-valued properties (Gaussian/Beta/Categorical/empirical) | ✅ | `graph.rs` `Distribution` accessors + Bayesian update/sampling (CONCEPT:EG-086) |

## New data modalities (`eg-core` + leaf crates)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Document/JSON deep indexing: JSONPath + durable inverted path-index; PG `->`/`->>`/`@>`/`jsonb_path_query` + Mongo `$match` → `Pred::JsonPath` | ✅ | selectivity via the path index (CONCEPT:EG-084) |
| Array/tensor store: dense/chunked N-D arrays CAS-backed + `Op::TensorScan`/`Op::TensorOp` (slice/reduce/elementwise) | ✅ | pure-Rust `eg-tensor`, feature `tensor` (CONCEPT:EG-085) |
| Scene-graph / 3D world model: `:SceneObject` pose + transform hierarchy + spatial relations + bounding volumes | ✅ | robotics/AR/urban-3D substrate (CONCEPT:EG-087) |
| Event-stream + CEP: windowed high-velocity ingest + `Op::Cep` bounded-NFA (sequence/within/absence) over sliding/tumbling windows | ✅ | `eg-stream`, feature `stream` (CONCEPT:EG-088) |

## Robotics (`eg-core` + `eg-tensor` + `eg-tsdb`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Multimodal sensor fusion (camera/LiDAR/audio/tactile aligned via ASOF backward-join) → `Op::SensorFuse` | ✅ | composes EG-085 + EG-088 + tsdb ASOF (CONCEPT:EG-098) |
| Action/policy/trajectory memory (`:Trajectory` of `:Step{state,action,reward,next,t}`, discounted return, best/worst retrieval) | ✅ | policy-learning/replay substrate (CONCEPT:EG-099) |

## LLM KV-cache (`eg-kvcache` — vs vLLM / LMCache)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Tiered hot/warm/cold KV-block cache (LRU + importance/recency; RAM → compressed-RAM → redb/blob; auto promote/demote) | ✅ | survives OOM by offloading (CONCEPT:EG-185) |
| Shared multi-instance KV backend (content-addressed, dedup, ref-count; lookup/publish by token-hash) | ✅ | `SharedKvBackend` (CONCEPT:EG-186) |
| HTTP endpoint (GET/PUT/EXISTS a block by token-hash + stats) + vLLM/LMCache remote-backend connector contract | ✅ | gated feature, out of pi (CONCEPT:EG-187) |

## OBDA / virtual graphs (`federation` + `eg-rdf`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| R2RML-style `:TriplesMap` mappings expose a foreign/relational source as a VIRTUAL RDF graph; SPARQL rewrites to `ForeignScan` (no materialization) | ✅ | completes Phase Q (CONCEPT:EG-101) |
| `Op::ForeignScan` resolution against a named `ForeignSource` registry (another graph / SQL table / remote endpoint) | ✅ | CONCEPT:EG-073 |

## Enterprise: security, backup, DR

| Operation | Status | Evidence |
|-----------|:------:|----------|
| RBAC-at-scale: durable roles + role hierarchy + resource/action grants over per-agent RLS + ACL + hash-chained audit | ✅ | `AgentRole` in eg-types::acl, `security` tier (CONCEPT:EG-092) |
| Online backup / restore + PITR (`Method::Backup`/`Restore`, MVCC-snapshot verbatim bundle preserving encryption + audit chain) | ✅ | redb-only; clean "not available" on non-redb (CONCEPT:EG-090) |

## Durability & distribution

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb-authoritative, commit-before-ack (`kill -9`-safe) | ✅ | `redb_backend.rs` `record_durable` |
| Cross-modal ACID (graph + vector + blob in one WriteTransaction) | ✅ | shared redb transaction |
| openraft replication + automatic failover (`raft`/`cluster`) | ✅ | `src/raft/mod.rs` |
| Cross-shard 2PC (presumed-abort, crash-recoverable) | ✅ | `src/raft/cross_shard_txn.rs` |
| Multi-Raft groups (N-group ring, online reshard, hibernate/rehydrate) | ✅ | `src/raft/multi.rs` `MultiRaft`/`GroupRouter` (KG-2.266/267/268); `reshard.rs` online ownership move |
| Non-blocking commit / 3PC / Calvin / parallel-commit | 🔶 | 2PC works; parallel-commit + read-only-participant + Calvin/3PC being added |
| Federation (remote/HTTP/external SQL) | ✅ | `federation`(`-sql`), OFF by default, never in `pi` |

See the [parity roadmap](roadmap.md) for the order in which the 🔶 / 🗺 items are being closed.
</content>
