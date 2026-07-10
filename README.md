# epistemic-graph

<p align="center">
  <b>One durable, Rust-native engine that is a multi-modal analytical database — graph · SQL · vector · RDF/OWL · time-series · key-value/blob — behind one query planner and one durable store.</b><br>
  <sub>Every modality is a first-class view over one <code>RowSet</code> algebra, from a Raspberry Pi to a replicated Raft cluster, from one core.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-2.17.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/language-Rust%20%7C%20Python-orange" alt="Language">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> **Full documentation:** the architecture, per-interface guides, deployment recipes, the tier/binary
> map, and the concept registry live at the
> [official docs site](https://knuckles-team.github.io/epistemic-graph/). Start with the
> [capability & parity matrix](docs/capabilities.md) — every capability is tracked operation-by-operation
> (✅ supported · 🔶 in-progress · 🗺 roadmap). **If a doc and the code disagree, the code wins.**

---

## What it is

A modern data platform usually needs a graph database **and** a vector index **and** a SQL warehouse
**and** an RDF triple-store + reasoner **and** a time-series DB **and** a blob store **and** a full-text
index — plus a broker, an observability stack, a GIS engine, and an LLM KV-cache. That is a dozen
systems, a dozen copies of the data, brittle sync pipelines, and an application layer that stitches
results back together.

`epistemic-graph` collapses that rack into **one durable engine with one unified query planner**. Every
modality is a view over the same `RowSet` algebra, so a single plan can seed candidates from an OWL
inference or a SPARQL pattern, filter them with SQL, traverse the graph, re-rank by vector similarity
*and* BM25 text, fuse the results, join a time-series and a lakehouse table, and run a sandboxed WASM or
numeric UDF — **without ever leaving the engine or marshalling rows back to the client.**

It also speaks the **wire protocols** of the systems it replaces — Postgres, MySQL, MSSQL, SQLite,
Neo4j Bolt, Redis, S3, AMQP/MQTT/STOMP, PromQL/OTLP — so existing clients, drivers, BI tools and ORMs
connect **unmodified**, all resolving to ONE exec path over ONE store.

### It runs independently

**epistemic-graph is a self-contained database.** It has no dependency on
[`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities) or any agent framework — any
client, in any language, over any of its wire protocols, can use it directly:

```bash
# Point DBeaver / psql / a JDBC app at the Postgres wire and just use SQL:
psql -h 127.0.0.1 -p 5433 -U agent -d epistemic
```
```sql
CREATE TABLE metrics (id TEXT PRIMARY KEY, value DOUBLE PRECISION);
INSERT INTO metrics VALUES ('cpu', 0.42);
SELECT id, value FROM metrics WHERE value > 0.1;
```

### It is greatly enhanced by agent-utilities

epistemic-graph is also the **compute & storage engine for `agent-utilities`**, which exercises *every*
modality — using the graph + OWL/RDF layer as its ontology-driven knowledge graph, the vector/text
surfaces for hybrid retrieval, the agent-memory primitives for durable memory, the broker/streams for
dispatch, and the KV-cache tier under vLLM/LMCache. If you run the full ecosystem, agent-utilities turns
this engine into a reasoning substrate; if you don't, it is still a complete, durable, multi-modal
database on its own. See the agent-utilities
[Graph Engine guide](https://github.com/Knuckles-Team/agent-utilities/blob/main/docs/guides/graph_engine.md).

---

## Why it matters

- **One store, one transaction, one security model.** A graph mutation + a vector upsert + a blob
  reference land in **one** redb `WriteTransaction` — all modalities commit together or none do. One
  snapshot, one ACID boundary, one per-agent RLS model, one planner. → [Master-of-all engine](docs/architecture/engine.md)
- **Durable by default.** Built redb-authoritative: the persist directory is the source of truth and an
  acked write survives `kill -9` (commit-before-ack). → [Service mode](docs/service_mode.md)
- **Scales by configuration, not by rewrite.** The *same binary* runs embedded in-process, as a single
  durable server, or — with the opt-in `cluster` layer — as a multi-node Raft cluster with cross-shard
  transactions. → [One build, opt-in layers](docs/architecture/tiers.md) · [Cluster deployment](docs/architecture/cluster_deployment.md)
- **Drop-in wire compatibility.** Existing Postgres/Neo4j/Redis/S3/AMQP/PromQL clients connect
  unmodified. → [Connecting (per-wire guide)](docs/interfaces/connecting.md)
- **One full-featured build.** `cargo build` is the whole engine — every main feature that compiles
  without a GPU/robotics toolchain, in one published wheel; `cluster` (HA raft) and `full-extras`
  (GPU/ROS2) are opt-in build layers on top. Runs on Raspberry Pi 4+. → [One build, opt-in layers](docs/architecture/tiers.md)
- **Measured to win the agent-memory workload.** Against a conventional stitched stack (separate vector
  DB + BM25 + app-level fusion, no KV cache, no warm-fork), the unified engine matches recall (**1.000**)
  while retrieving **~3.6× faster**, reusing cross-modal context across a fan-out with **`retrieval_calls == 1`**
  (vs *N*), keeping writes **read-fresh in 25.7 ms** (incremental, not full-rebuild), and surviving a full
  restart with a **durable KV cold-tier (100% survival, >300× vs recompute)**. → [Benchmarks](docs/benchmarks.md#phase-2-agent-memory--kv-cache-benchmark-measured)

---

## Quick start

### Docker

```bash
docker volume create eg-data
docker run -d --name epistemic-graph \
  -e GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)" \
  -e EPISTEMIC_GRAPH_PGWIRE_ADDR=0.0.0.0:5433 \
  -p 127.0.0.1:9100:9100 -p 127.0.0.1:9101:9101 -p 127.0.0.1:5433:5433 \
  -v eg-data:/var/lib/epistemic-graph/data \
  <registry>/epistemic-graph:<tag>
```
> The server **refuses to start without `GRAPH_SERVICE_AUTH_SECRET`** (HMAC-SHA256 on the RPC transport).
> `--allow-insecure` opts out for development only. Full recipes (compose, HA cluster, prebuilt wheels):
> [deployment guide](docs/deployment.md).

### Binary

```bash
# start a complete single-node database with the Postgres wire listener
EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433 \
  epistemic-graph-server --persist-dir /var/lib/eg
```

### Python client

```bash
pip install epistemic-graph
```
```python
from epistemic_graph import SyncEpistemicGraphClient

g = SyncEpistemicGraphClient()                    # connects/attaches to the UDS engine
g.nodes.add("AgentA", {"type": "coordinator"})
g.edges.add("AgentA", "AgentB", {"weight": 1.5})
print("Order:", g.graph.topological_sort())
```

More entry points — the remote → shared-local → autostart resolver and the embedded in-process handle —
are in [engine modes](docs/engine_modes.md).

---

## Capabilities

Each surface has a deep-dive; the [capability & parity matrix](docs/capabilities.md) is the
operation-by-operation source of truth.

### Query surfaces & interfaces

| Surface | You can point at it… | Deep dive |
|---------|----------------------|-----------|
| **SQL / pgwire** | `psql`, DBeaver, JDBC/ODBC, BI tools, ORMs — DataFusion `SELECT` (joins/CTE/window), full DML, arbitrary user tables + DDL + `COPY` + `ALTER TABLE`, `CREATE FUNCTION` incl. **PL/pgSQL** bodies, `pg_catalog`/`information_schema`, pgvector/AGE/Timescale/ParadeDB compat. **pgwire is in the one main build.** | [SQL & pgwire](docs/interfaces/sql.md) |
| **SPARQL / RDF / OWL** | Stardog/GraphDB clients — SPARQL 1.1 `SELECT`/`ASK`/`CONSTRUCT`/`DESCRIBE`/`UPDATE`, the W3C `/sparql` endpoint, OWL 2 EL⁺/RL **and** DL-tableau + SWRL, SHACL/ShEx + ICV write-path enforcement, R2RML, GeoSPARQL | [SPARQL & RDF](docs/interfaces/sparql.md) · [Ontology lifecycle](docs/interfaces/ontology.md) |
| **Cypher / Bolt** | Neo4j drivers & `cypher-shell` — `MATCH`/writes/`WITH`/aggregation, GDS via `CALL gds.*`, native **Bolt v4.4** wire | [Cypher & Bolt](docs/interfaces/cypher.md) |
| **GraphQL** | Apollo clients — Federation v2 subgraph, subscriptions over CDC, fragments/variables/directives, relay pagination, APQ/depth/cost hardening | [GraphQL](docs/interfaces/graphql.md) |
| **Vector / ANN** | Pinecone/Milvus-style kNN — IVF-PQ + OPQ + SQ8 + **HNSW** + exact/flat, hybrid metadata pre-filter, cross-shard scatter-gather, real pgvector ANN pushdown | [Vector / ANN](docs/interfaces/vector.md) |
| **Time-series** | InfluxDB/Timescale-style — `time_bucket`, ASOF, gap-fill, OHLC, decay, SQL window frames, `Op::Window` planner op | [Time-series](docs/interfaces/timeseries.md) |
| **Key-value & Blob** | Redis (RESP2/3 + pub/sub + `MULTI`/`EXEC`) and S3/MinIO (REST, multipart, range GET); content-addressed streaming CAS with content-defined chunking; embedded in-process handle | [Key-value & Blob](docs/interfaces/kv_blob.md) |
| **Messaging & Broker** | RabbitMQ/Kafka clients — exchanges/routing, DLQ, TTL, priority, delayed delivery, consumer groups, publisher confirms + **exactly-once**; **AMQP/MQTT/STOMP** wires; replayable streams | [Messaging & Broker](docs/interfaces/messaging.md) |
| **Observability** | Prometheus/OpenObserve/Jaeger clients — log ingest, **PromQL** (extended fn set), OTLP traces, VRL pipelines, federated search; the engine also **emits** its own OTLP + Prometheus remote-write | [Observability](docs/interfaces/observability.md) |
| **GIS / Spatial** | PostGIS-style — CRS/reprojection, R-tree, GeoJSON/WKB/GPX + **Shapefile/KML/GeoParquet**, MVT + **raster tile pyramids**, routing (turn-restrictions/time-windows)/isochrones/TSP | [GIS / Spatial](docs/interfaces/gis.md) |
| **KV-cache (LLM)** | vLLM/LMCache — tiered hot/warm/cold KV-block cache (zstd/lz4) + shared dedup backend + HTTP endpoint: the durable **L2 tier** under vLLM's GPU cache and LMCache's CPU tier | [KV-cache](docs/interfaces/kvcache.md) |
| **Agent memory** | Zep/mem0/LeanRAG-style — bi-temporal `AsOf`, summary tier, episodic→semantic consolidation, decay/reinforce, hierarchical retrieval, scene/trajectory — all drivable over the wire | [Agent memory](docs/interfaces/memory.md) |
| **Clients** | Python (full) · JS / Go (thin) over framed MessagePack, no PyO3/FFI | [Client drivers](docs/interfaces/clients.md) |

The unified [UQL planner](docs/uql.md) is what lets these compose in a single cross-modal plan;
natural-language → query (`NlQuery`) is a complete, LLM-optional seam.

### Analytics & advanced capabilities

- **[Analytics Program](docs/architecture/analytics_program.md) — "one kernel, two surfaces."** A
  BLAS/LAPACK-free Rust numeric kernel ([`eg-numeric`](docs/architecture/numeric_kernel.md), faer +
  ndarray) exposed as (A) an in-process Python extension + numpy-shim and (B) **in-database DataFusion
  UDFs/UDAFs** — `cosine_sim`/`l2_normalize`/`zscore`/`covariance` scalars + `svd`/`pca`/`kmeans`
  column→matrix aggregates, and the differentiator: **cross-modal join → analytics in-engine** (join
  graph ⋈ vector ⋈ time-series, then run `pca`/`kmeans` over the joined set — impossible in numpy, which
  has no data layer).
- **[Lakehouse LTAP interop](docs/architecture/lakehouse_ltap.md).** The engine's tables materialize as
  open **Parquet + Delta + Iceberg** (real Iceberg v2 **Avro** manifests, per-column stats for predicate
  pushdown) with an Iceberg-REST catalog + LSN as-of, so Databricks/Spark/Trino/DuckDB read them with
  **zero ETL**.
- **[Distribution / Robotics / GPU tail](docs/architecture/distribution_robotics_gpu.md).** Cross-region
  async read-replicas, Calvin deterministic commit, ROS2 bridge (rosbridge-WS + pure-Rust RTPS), and a
  GPU distance/tensor dispatch seam with a real CUDA backend + CPU fallback.

### Distribution & durability

- **redb-authoritative, commit-before-ack** (`kill -9`-safe), folded into every tier.
- **In-engine Raft replication** (`cluster` tier, openraft) with automatic failover; off ⇒ byte-for-byte
  single-node.
- **Cross-shard 2PC** (presumed-abort + parallel-commit + read-only-participant + non-blocking
  Raft-replicated decision), multi-Raft groups + online resharding, and an opt-in **Calvin**
  deterministic-ordering branch.
- **Cross-modal ACID** across all modalities in one transaction.

→ [Engine scaling program](docs/architecture/scaling_program.md) ·
[Multi-Raft status](docs/architecture/m2_raft_status.md) ·
[Catalog-driven resharding](docs/architecture/m3_resharding.md)

### Security & isolation

- **Mandatory auth** — every RPC carries `HMAC-SHA256(secret, request_id)`; pgwire adds SCRAM-SHA-256.
- **Per-agent Row-Level Security** applied before any query surface touches the graph; the result cache
  keys on the caller's RLS context.
- **Encryption-at-rest** (ChaCha20-Poly1305, pure-Rust) + a **hash-chained tamper-evident audit log**.

→ [Service mode](docs/service_mode.md)
| Interface | Operation | Status | Feature | Notes |
|-----------|-----------|:------:|---------|-------|
| **SQL** | `SELECT` (joins, aggregates, CTE, window, subquery) | ✅ | `query` | DataFusion 43 over `nodes` + `edges`; real predicate pushdown (Inexact) |
| **SQL** | `INSERT` / `UPDATE` / `DELETE` (+ `RETURNING`) | ✅ | `query` | `nodes` + user tables (EG-KG.query.follow-up); serializable CAS gates |
| **SQL** | Compound/`AND`/`OR`/`IN`/`BETWEEN`/`IS NULL` WHERE DML, `INSERT…SELECT`, `UPDATE…FROM`/`DELETE…USING`, `ON CONFLICT` upsert | ✅ | `query` | EG-045/046/047/048; serializable re-check under the write guard |
| **SQL** | Mixed-store wire transactions (`BEGIN`/`COMMIT`/`ROLLBACK` + `TransactionStatus`) | ✅ | `pgwire` | EG-KG.compute.kg-transaction-is-pinned; node + user-table ops, read-your-own-writes; documented non-2PC user-table window |
| **SQL** | `CREATE VIEW`/`DROP VIEW`, `CREATE FUNCTION`, arrays/ranges + common functions | ✅ | `query` | EG-072/118/104; durable view + function catalog |
| **SQL** | Arbitrary user tables + DDL (`CREATE`/`ALTER ADD COLUMN`/`DROP`), `COPY` | ✅ | `query` | durable redb table catalog (EG-KG.query.register-user-tables-alongside/EG-KG.query.register-each-user-table); JOINable to the graph |
| **SQL** | `ALTER TABLE` beyond ADD COLUMN — `DROP`/`RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN TYPE`, `DROP CONSTRAINT` | ✅ | `query` | durable catalog rewrite with data migration (EG-KG.query.rename-table-moves-catalog) |
| **SQL** | Columnar segments + window functions (`ROW_NUMBER`/`RANK`/`LAG`/`LEAD`/`OVER(…)`) | ✅ | `query` | EG-089; struct-of-arrays analytical scan |
| **Postgres compat** | `pg_catalog` + `information_schema` system views (`\d`/`\dt`/`\l`) | ✅ | `pgwire` | EG-KG.query.route-create-view-create; synthesized from live table/view/function catalogs |
| **Postgres compat** | `CREATE EXTENSION` catalog · pgvector `vector` + `<->`/`<=>`/`<#>` + **real ANN pushdown** to HNSW/IVF + exact re-rank | ✅ | `pgwire` | EG-KG.query.create-drop-extension-over/115/116; real top-k pushdown (EG-KG.query.real-pgvector-ann-top) |
| **Postgres compat** | AGE `cypher()` set-returning function, TimescaleDB hypertables + continuous aggregates, ParadeDB `@@@` **real BM25 ranking + snippets** | ✅ | `pgwire` | EG-KG.query.postgres-family-extension-plan/117/119; real BM25 (EG-311) |
| **Postgres wire** | listener, simple + extended/prepared protocol | ✅ | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR`; in the one main build (EG-KG.compute.capability-reference/EG-KG.sharding.deployment-tiers) |
| **Postgres wire** | SCRAM-SHA-256 / trust auth, `pg_catalog` introspection | ✅ | `pgwire` | EG-KG.query.concept-13 / EG-KG.query.datafusion; pg user → engine ACL actor |
| **SPARQL** | `SELECT` (BGP, paths, FILTER subset, OPTIONAL, UNION, GROUP/agg, BIND, DISTINCT, SLICE) | ✅ | `sparql` | spargebra parser compiled to LPG scans |
| **SPARQL** | `ASK` / `CONSTRUCT` / `DESCRIBE` | ✅ | `sparql` | template instantiation + bounded description (gated by `rdf`, implied by `sparql`) |
| **SPARQL** | `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`) | ✅ | `sparql` | `eg-rdf/src/update.rs`; `LOAD` intentionally deferred (no HTTP fetch in write path) |
| **SPARQL** | `/sparql` HTTP endpoint (W3C SPARQL 1.1 Protocol) | ✅ | `sparql-http` | `src/server/sparql_http.rs`; GET + POST query/update |
| **SPARQL** | true named graphs (quad dataset) + `FROM`/`FROM NAMED` | ✅ | `sparql` | `GRAPH ?g`/constant-IRI over registry graphs (EG-KG.ontology.from-from-named) |
| **SPARQL** | `ORDER BY` total-ordering, `VALUES`, `MINUS`, `EXISTS`/`NOT EXISTS`, negated property set | ✅ | `sparql` | EG-KG.ontology.completing-eg-order-by/125/055/056; fixes the unordered-results correctness gap |
| **SPARQL** | content negotiation (JSON/XML/CSV/TSV/Turtle/N-Triples), rich FILTER, sub-SELECT, SERVICE federation | ✅ | `sparql` | EG-KG.ontology.content-negotiation-serializers/053/051/052; SSRF allowlist on SERVICE |
| **SPARQL** | SHACL + ShEx validation, ICV integrity constraints (**enforced on the commit/write path**), GeoSPARQL + RCC8/Egenhofer | ✅ | `sparql`/`geosparql` | EG-KG.ontology.concept-6/133/146/261/155; ICV commit-guard (EG-KG.ontology.rdf-update-guard) |
| **RDF I/O** | JSON-LD 1.1, TriG, N-Quads, RDF/XML serialization matrix | ✅ | `rdf` | EG-KG.ontology.eg-concrete-syntax-matrix/137 (alongside Turtle/N-Triples) |
| **Cypher** | `MATCH … WHERE … RETURN … LIMIT` (var-length `[*m..n]`) | ✅ | `cypher` | read over a snapshot; WHERE supports `OR`/`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL` (EG-KG.query.eg-extend-read-side) |
| **Cypher** | writes (`CREATE`/`MERGE`/`SET`/`DELETE`+`DETACH`/`REMOVE`) | ✅ | `cypher` | native eg-core mutations (EG-KG.query.cypher-execution) |
| **Cypher** | `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/`DISTINCT`/`UNWIND`/`CALL` | ✅ | `cypher` | EG-KG.query.eg-extend-read-side/141/142; GDS via `CALL gds.<algo>(…) YIELD …` streams eg-compute results as rows (EG-KG.query.eg-2/144/298) |
| **Cypher** | Neo4j **Bolt v4.4** wire (PackStream v2) | ✅ | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` (EG-KG.query.bolt-wire-protocol); neo4j drivers / cypher-shell |
| **GraphQL** | read queries (scan + BFS, schema-from-graph, aliases, `first`/`limit`, filters) | ✅ | `graphql` | byte-equal to Cypher path |
| **GraphQL** | mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`) | ✅ | `graphql` | native eg-core mutations |
| **GraphQL** | Apollo Federation v2 subgraph (`_service`/`_entities`, `@key`) + APQ/depth/complexity hardening | ✅ | `graphql` | EG-295/296 |
| **GraphQL** | subscriptions / fragments / variables / directives / relay pagination | ✅ | `graphql` | real CDC push (`LiveQuery`, WS/SSE) + `$`/`@` lexer + fragments + `@skip`/`@include` + relay envelope (EG-064/065/066) |
| **OWL** | EL⁺ + RL forward-chaining materialization & classification | ✅ | `owl` | pure-Rust; consistency + incremental + justifications |
| **OWL** | confidence-weighting + Ebbinghaus time-decay | ✅ | `owl` | EG-KG.ontology.concept-13; per-axiom `eg:confidence`, fact decay |
| **OWL** | query-time `Op::Reason` (reasoner seeds a RowSet) | ✅ | `owl-plan` | distributed/cross-shard union supported |
| **OWL** | OWL-DL (tableau, cardinality, `allValuesFrom`), SWRL user rules | ✅ | `owl-dl`/`owl` | pure-Rust DL tableau (consistency→classification→instance) + `swrlb:` built-in library; EL/RL fast path stays default (EG-KG.ontology.concept-2/060) |
| **Vector / ANN** | IVF-PQ + OPQ + SQ8-refine, persistent (reopen w/o rebuild), warm-on-start | ✅ | `ann` | parallel/SIMD brute-force fallback below threshold |
| **Vector / ANN** | hybrid metadata pre-filter (kNN + `allow(id)` predicate) | ✅ | `ann` | `search_filtered` (EG-070); filtered during the ADC probe |
| **Vector / ANN** | HNSW index (higher recall-per-probe than IVF-PQ) | ✅ | `ann` | insert/search/serde-persist, recall-harness-tuned (EG-KG.retrieval.hnsw-vector-index) |
| **Vector / ANN** | exact/flat kNN index + ANN-vs-exact re-rank + recall@k/precision harness | ✅ | `ann` | EG-KG.query.concept-5 |
| **Vector / ANN** | cross-shard kNN scatter-gather → deterministic global top-k | ✅ | `ann` | server-layer scatter over per-shard indexes merged via the `merge_topk` leaf (EG-319, completing EG-KG.retrieval.scatter-gather) |
| **Time-series** | store + `time_bucket`, ASOF join, gap-fill LOCF, OHLC, downsample, decay | ✅ | `tsdb` | native redb columnar, no DataFusion |
| **Time-series** | time-ops as unified planner ops (`Op::Window`) | ✅ | `tsdb` | real `window_aggregate` over the RowSet via eg-tsdb `time_bucket` (EG-KG.query.streaming-execution); per-point retention trim (EG-KG.temporal.bucket-cutoff-trim) |
| **Blob / CAS** | content-addressed streaming store (redb-native) | ✅ | `blob` | refcount mark-and-sweep GC; bounded RAM |
| **Blob / CAS** | S3 / MinIO backend behind the same `ChunkStore` trait | ✅ | `blob-s3` | manifest/linkage byte-identical |
| **Blob / CAS** | content-defined chunking | ✅ | `blob` | Gear/FastCDC rolling-hash chunker (variable boundaries); sha256 CAS dedup + refcount GC preserved (EG-KG.storage.backward-manifest-read) |
| **Key-value** | embedded in-process engine API over redb rows | ✅ | `embedded` | `EmbeddedEngine` — no Tokio/socket/HMAC (EG-KG.backend.engine-modes) |
| **Key-value** | generic namespaced `get`/`put`/`scan`/`cas` KV surface over redb | ✅ | `redb` | `src/server/kv.rs` (EG-022); durable, commit-before-ack; not graph-scoped |
| **Multi-wire** | wire-neutral SQL core (`WireProtocol`/`WireSession`, one classify→exec path) | ✅ | `wire` | `src/server/wire` (EG-KG.compute.subsystems-reference); shared by every SQL wire |
| **Multi-wire** | MySQL / MariaDB wire (handshake v10 + `mysql_native_password`) | ✅ | `mysql-wire` | `EPISTEMIC_GRAPH_MYSQL_ADDR` (EG-KG.query.kg-2) |
| **Multi-wire** | MSSQL TDS wire | ✅ | `mssql-wire` | `EPISTEMIC_GRAPH_MSSQL_ADDR` (EG-KG.query.hand-rolled-tds-server) |
| **Multi-wire** | SQLite-dialect NDJSON-over-TCP endpoint | ✅ | `sqlite-wire` | `EPISTEMIC_GRAPH_SQLITE_ADDR` (EG-KG.query.concept-3); `.db` file I/O 🔶 forward roadmap |
| **Multi-wire** | Neo4j Bolt v4.4 wire (PackStream v2, native Cypher) | ✅ | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` (EG-KG.query.bolt-wire-protocol) |
| **Broker** | exchanges (direct/topic/fanout) + bindings/routing over the EG-KG.compute.atomically-claim-oldest-pending work-queue | ✅ | `broker` | RabbitMQ-class (EG-275) |
| **Broker** | DLQ · message/queue TTL · priority · delayed/scheduled delivery · consumer-groups + QoS/prefetch | ✅ | `broker` | EG-KG.compute.dead-letter-queues/277/278/279/280 |
| **Broker** | replayable append-log streams (Kafka-style offsets/retention) + publisher confirms + manual ack/nack | ✅ | `broker` | EG-283/284 |
| **Broker** | exactly-once (idempotent-producer dedup) + stream/confirm/ack over AMQP `confirm.select` + MQTT 5 frames | ✅ | `broker` | effectively-exactly-once atop at-least-once (EG-314) |
| **Broker wires** | AMQP 0.9.1 · MQTT 3.1.1/5.0 · STOMP 1.2 listeners | ✅ | `amqp-wire`/`mqtt-wire`/`stomp-wire` | `EPISTEMIC_GRAPH_{AMQP,MQTT,STOMP}_ADDR` (EG-275/281/282) |
| **KV / structures** | Redis RESP2/3 wire (strings/hashes/lists/sets/sorted-sets) + **pub/sub** + `MULTI`/`EXEC` | ✅ | `redis-wire` | `EPISTEMIC_GRAPH_REDIS_ADDR` (EG-KG.ontology.resp2-resp3-codec-round/307) |
| **Object store** | S3-compatible REST (bucket/object CRUD, SigV4-lite) over the blob CAS + **multipart upload** + **range GET** | ✅ | `s3-api` | EG-KG.ontology.object-put-get-head/307 |
| **Observability** | log ingest + PromQL `/api/v1/query` (extended fn set) + OTLP traces `/v1/traces` + service-dependency map | ✅ | `obs`/`promql`/`traces` | `EPISTEMIC_GRAPH_OBS_ADDR`, default `:5080` (AU-KG.ingest.self-ingest/172/163); PromQL `_over_time`/`topk`/`label_replace`/`clamp*` (EG-302) |
| **Observability** | VRL-style ingest pipelines (parse/filter/enrich, cross-modal) + super-cluster federated search (typed SQL/SPARQL fusion) | ✅ | `obs`/`federation` | EG-165/243; typed result fusion (EG-KG.query.schema-typed-fusion-sql) |
| **Observability** | engine's own telemetry egress — OTLP metrics/traces export + Prometheus remote-write receiver | ✅ | `otel-export` | the engine emits, not just ingests (EG-316) |
| **Lakehouse (LTAP)** | Parquet-on-object-store + Delta log + Iceberg-REST catalog + LSN as-of — external lakehouse engines read with zero ETL | ✅ | `lake` | `eg-lake` (EG-KG.storage.lsn-as-snapshot-returns); Iceberg Avro manifest is a documented stub |
| **Spatial / GIS** | `SpatialScan` + `ST_Within`/`ST_DWithin`, GeoSPARQL + RCC8/Egenhofer, CRS/reproject, R-tree, GeoJSON/WKB/GPX + **Shapefile/KML/GeoParquet** | ✅ | `geo`/`geosparql` | eg-geo (EG-KG.ontology.singles-concept/261/155/262/263/264); Shapefile/KML/GeoParquet I/O (EG-KG.domains.geo-formats); no GEOS/PROJ |
| **Spatial / GIS** | XYZ/TMS + Mapbox Vector Tiles · weighted routing (+ **turn-restrictions/time-windows**)/isochrones/TSP · map-based task tracking | ✅ | `geo` | EG-KG.domains.map-tiles/266/267; turn-restrictions + time-dependent weights (EG-KG.domains.geo-partitioning) |
| **Document / JSON** | deep JSONPath query + durable inverted path-index (persists to redb + boot-rehydrate + cost `Stats`); PG `->`/`->>`/`@>` + Mongo `$match` | ✅ | (core)/`query` | `Pred::JsonPath` (EG-084); durable path-index (EG-308) |
| **Tensor / probabilistic** | N-D array store (CAS-backed) + `TensorScan`/`TensorOp` (results **written back** to the CAS store); distribution-valued properties | ✅ | `tensor` | EG-085/086; tensor-op CAS write-back (EG-304) |
| **Scene-graph / 3D** | `:SceneObject` pose + transform hierarchy + spatial relations (robotics/AR/urban-3D) | ✅ | (core) | EG-087 |
| **CEP / streams** | windowed event ingest + `Op::Cep` bounded-NFA pattern match over sliding/tumbling windows | ✅ | `stream` | EG-KG.query.pipelined-execution |
| **Robotics** | multimodal sensor fusion (ASOF-aligned) + action/trajectory memory | ✅ | `tensor` | EG-KG.query.multi-rate-sensor-stream/099 |
| **KV-cache (LLM)** | tiered hot/warm/cold KV-block cache (real **zstd/lz4** warm-tier compression) + shared dedup backend + HTTP endpoint (vLLM/LMCache contract) — the durable **L2 tier** under vLLM (L0 GPU) → LMCache (L1 CPU) | ✅ | `kvcache-server` | eg-kvcache (EG-185/186/187); real compression codec (EG-315); see [kvcache interface](docs/interfaces/kvcache.md) |
| **Agent memory** | bi-temporal `AsOf`, decay/reinforce, summary-node tier, episodic→semantic consolidation, LeanRAG retrieval, scene/trajectory — **drivable over the wire** | ✅ | (core) | `Op::AsOf` (AU-KG.compute.kg-2); EG-KG.compute.hierarchical-summary-tier-eg/221/222/195; wire-Op surface (EG-KG.memory.eg-batch-decay-caller) |
| **OBDA** | R2RML virtual graphs — SPARQL over a foreign source rewrites to `ForeignScan` (no materialization); parses **R2RML Turtle** documents | ✅ | `federation` | EG-101; R2RML Turtle parse (EG-305) |
| **RBAC** | durable roles + role hierarchy + resource/action grants over per-agent RLS (persist to redb + boot-reload) | ✅ | `security` | EG-KG.compute.feature; durable persistence (EG-KG.compute.durable-rbac-identity-persistence) |
| **Scheduling / QoS** | real-time QoS/SLO scheduler — per-tenant/priority admission + deadline scheduling + backpressure | ✅ | (server) | EG-320; complements the reserved read-admission lane (EG-KG.coordination.reserved-read-lane) |
| **Backup / DR** | consistent online backup + restore CLI + PITR (`Method::Backup`/`Restore`) | ✅ | `redb` | EG-090 |
| **Full-text** | Tantivy BM25 inverted index, `RankText` + reciprocal-rank fusion | ✅ | `text` | composes in the unified planner |
| **Unified planner** | `Scan·Filter·Traverse·Rank·RankText·FuseRrf·Reason·SparqlBgp·Udf·ForeignScan·AsOf·Limit` | ✅ | `query`+ | each op feature-gated; see [UQL](docs/uql.md) |
| **Unified planner** | `Op::Window` / `Op::Foreign` execution | ✅ | `query` | `Op::Window` = real eg-tsdb windowed aggregate (`timeseries`, EG-KG.query.streaming-execution); `Op::Foreign` resolves the name via the `ForeignSourceRegistry` (`federation`, EG-073) |
| **UQL** | text DSL → `wire::Plan` (one parse, zero new exec path) | ✅ | (front-end always ships) | dependency-free parser |
| **UQL** | natural-language → query (`Method::NlQuery`, `/nl`, `nl_query()` UDF) | ✅ | `nl-query` | EG-078/080; complete LLM-optional seam — inert until an OpenAI-compatible endpoint is set; AU-provider integration tracked on the agent-utilities side |
| **Durability** | redb-authoritative, commit-before-ack (`kill -9`-safe) | ✅ | `redb` | folded into every tier |
| **Distribution** | openraft replication + automatic failover | ✅ | `raft` | `cluster` tier; off ⇒ byte-for-byte single-node |
| **Distribution** | cross-shard 2PC + parallel-commit + read-only-participant + non-blocking (Raft-replicated decision) commit | ✅ | `raft` | presumed-abort 2PC (EG-KG.storage.lane-n-increment) + parallel prepare/empty-write-set skip (EG-KG.txn.cross-shard) + Paxos-Commit-lite replicated decision (EG-KG.txn.harness-crash) |
| **Distribution** | Calvin deterministic-ordering cross-shard commit (order-first, vote-free, crash-replay) | ✅ opt-in | `calvin` (⇒ `nonblocking`) | EG-KG.txn.calvin-deterministic-ordering; sequencer + Raft-replicated input log + vote-free execution shipped; distributed OLLP read-lock phase + multi-node epoch fan-in documented-deferred — see [distribution-robotics-gpu](docs/architecture/distribution-robotics-gpu.md) |
| **Distribution** | multi-Raft groups (N-group ring, online reshard, hibernate/rehydrate) | ✅ | `raft` | `GroupRouter` + `MultiRaft` (KG-2.266/267/268); online ownership move |
| **Distribution** | cross-region async read-replica tier (bounded-LSN log + follower pull → `wal::apply`) + capacity guardrails (circuit breaker / per-tenant quota / backpressure) | ✅ opt-in | `federation-search` | EG-322/323; off by default (`EPISTEMIC_GRAPH_REPLICATE`); complements the EG-320 QoS scheduler with absolute ceilings |
| **Federation** | remote engine / HTTP-JSON / external SQL (`sqlx`) as a `ForeignScan` | ✅ | `federation`(`-sql`) | in the main build; activates only when a foreign source is registered |
| **Robotics** | ROS2 bridge over rosbridge-WebSocket (CDC↔topic, no DDS/C toolchain) | ✅ opt-in | `ros2-bridge` | EG-KG.domains.robotics-gpu-distribution; pure-Rust `tokio-tungstenite` |
| **Robotics** | native DDS/RTPS ROS2 wire — TWO legs behind one `DdsTransport` trait, both zero-config `rmw`-mangled | ✅ opt-in | `ros2-dds` (pure-Rust `rustdds`) / `ros2-rmw` (real CycloneDDS-C, S5) | EG-KG.ingest.dds-transport / EG-KG.ingest.rmw-cyclonedds-leg; `ros2-rmw` vendors + cmake-builds the CycloneDDS C sources (no network/libclang at build time) for genuine live-`ros2` interop |
| **GPU** | GPU distance/tensor dispatch seam + real CUDA backend (NVRTC, `dynamic-loading`) | ✅ opt-in | `gpu` / `gpu-cuda` | EG-KG.compute.gpu-distance-seam/327; pure-Rust CPU backend is always compiled + the byte-for-byte ground truth; CUDA auto-falls-back on a GPU-less host; live-GPU kernel validation deferred (none in CI) |
| **Numeric / analytics** | BLAS/LAPACK-free Rust numeric kernel (`eg-numeric`: reductions/stats · element-wise · linalg via faer · seedable random) | ✅ P1 (Surface A) · 🗺 Surface B | `pip install epistemic-graph[numeric]` (Surface A) · cargo `numeric`/`full` (Surface B) | AU-KG.compute.numeric-kernel; **P1 of the [Analytics Program](docs/architecture/analytics-program.md)** ("one kernel, two surfaces"). Surface A = in-process Python — the kernel `.so` is **folded into the one `epistemic-graph` wheel** as `epistemic_graph.numeric` (no separate `eg-numeric` package; + the AU `xp` numpy-shim, 847 parity checks); Surface B (in-DB DataFusion UDFs/graph/vector/ts analytics) is P2–P5 follow-on. Surface-B `numeric` cargo feature is part of the main build — see [numeric-kernel](docs/architecture/numeric-kernel.md) |
| **Clients** | multi-language client drivers — Python (full) · JS / Go (thin: broker/streams/RBAC/backup/NL) over framed MessagePack (no PyO3/FFI) | ✅ | (client) | EG-328; wire-parity gated (`test_protocol_parity.py`) — see [clients](docs/interfaces/clients.md) |

---

## Architecture at a glance

```mermaid
flowchart TB
    subgraph Clients["Clients (unmodified)"]
        AU["agent-utilities / graph-os"]
        PG["psql / DBeaver / BI / ORM (pgwire)"]
        DRV["Neo4j · Redis · S3 · AMQP · PromQL drivers"]
        EMB["Embedded in-process caller (Pi/edge)"]
    end

    subgraph Engine["epistemic-graph-server (one Rust process)"]
        WIRE["Wire adapters (one WireProtocol exec path)"]
        SEC["Security: per-agent RLS + audit chain + encryption-at-rest"]
        PLAN["Unified RowSet planner (cost-reordered, cross-modal)"]
        CORE["GraphCore: petgraph + ledger + result cache"]
        MOD["Modalities (feature-gated): vector · SQL · RDF/OWL · TSDB · text · blob · GIS · tensor · stream"]
        DUR["Durability: redb-authoritative + Raft + cross-shard 2PC + CDC"]
    end

    AU --> WIRE
    PG --> WIRE
    DRV --> WIRE
    EMB --> CORE
    WIRE --> SEC --> PLAN --> CORE
    CORE --> MOD
    CORE --> DUR
```

See [technical overview](docs/overview.md) for the crate DAG and the planner pipeline, and
[subsystems](docs/architecture/subsystems.md) for how each modality composes on the one store.

---

## One build, opt-in layers

The engine ships as **one prebuilt binary** — the full-featured build — plus two opt-in build layers.
The target host pulls a prebuilt wheel and never compiles. Full map: [one build, opt-in layers](docs/architecture/tiers.md).

| Build | Carries | For |
|-------|---------|-----|
| **main** (default wheel) | the whole single-node DB: redb-authoritative + cypher + DataFusion SQL + graphql + ann + tsdb + blob + Tantivy text + rdf/sparql/owl + wasm-udf + security + federation + the wire family (**pgwire**/mysql/mssql/sqlite/bolt/redis/amqp/mqtt/stomp) + the `numeric` kernel + kvcache-server + broker | anything single-node, Raspberry Pi 4+ to workstation |
| **+ cluster** | main + **Raft** replication + cross-shard 2PC + distributed compute | multi-node HA |
| **+ full-extras** | main + accelerator legs (`gpu-cuda`, `ros2-bridge`, `ros2-dds`) | GPU / robotics |

`cluster` and `full-extras` are opt-in build flags layered on top of the one main build (openraft is
cluster-only; cudarc/rustdds are full-extras-only) — they are built from source, not published as
separate wheels.

---

## Documentation

- [Capabilities & parity matrix](docs/capabilities.md) — the operation-by-operation truth table.
- [Technical overview](docs/overview.md) · [Master-of-all engine](docs/architecture/engine.md) ·
  [UQL & the unified planner](docs/uql.md).
- Interfaces: [Connecting](docs/interfaces/connecting.md) · [SQL](docs/interfaces/sql.md) ·
  [SPARQL](docs/interfaces/sparql.md) · [Cypher](docs/interfaces/cypher.md) ·
  [GraphQL](docs/interfaces/graphql.md) · [Vector](docs/interfaces/vector.md) ·
  [Time-series](docs/interfaces/timeseries.md) · [KV & Blob](docs/interfaces/kv_blob.md) ·
  [Messaging](docs/interfaces/messaging.md) · [Observability](docs/interfaces/observability.md) ·
  [GIS](docs/interfaces/gis.md) · [KV-cache](docs/interfaces/kvcache.md) ·
  [Agent memory](docs/interfaces/memory.md) · [Ontology](docs/interfaces/ontology.md) ·
  [Clients](docs/interfaces/clients.md).
- Analytics & distribution: [Analytics Program](docs/architecture/analytics_program.md) ·
  [Analytics in UQL](docs/analytics_in_uql.md) · [Numeric kernel](docs/architecture/numeric_kernel.md) ·
  [Lakehouse LTAP](docs/architecture/lakehouse_ltap.md) ·
  [Distribution / Robotics / GPU](docs/architecture/distribution_robotics_gpu.md).
- Deploy & operate: [Standalone deployment](docs/standalone_deployment.md) ·
  [DBeaver / psql quickstart](docs/dbeaver_quickstart.md) ·
  [Deployment topology](docs/deployment_topology.md) · [Tiers & binaries](docs/architecture/tiers.md) ·
  [Deployment](docs/deployment.md) · [Engine modes](docs/engine_modes.md) ·
  [Service mode](docs/service_mode.md) · [Cost model & capacity](docs/cost_model.md) ·
  [Operations runbook](docs/operations/runbook.md).
- Contributing: [AGENTS.md](AGENTS.md) · [Rust compute guide](docs/rust_compute_guide.md) ·
  [CONTRIBUTING.md](CONTRIBUTING.md).

> **Design for a network boundary.** Every out-of-process call is a serialize → socket → deserialize
> round trip, not a function call. **Batch, never per-element:** ship work as one op over data already in
> the graph, and keep tight per-element math in-process. See the
> [Rust compute guide](docs/rust_compute_guide.md).

## License

MIT — see [LICENSE](LICENSE).
