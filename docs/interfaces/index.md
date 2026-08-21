# Interfaces

epistemic-graph is a **drop-in substrate**: existing clients connect to it as if it were the
database they already speak. This is the curated map of the `interfaces/` tree (17 pages) — start
with the connection guide, then jump straight to the surface you need.

## Start here

| Page | What it covers |
|---|---|
| [Connecting — per-wire connection guide](connecting.md) | The single connect-and-query recipe per surface: the Cargo feature to build with, the env var (or CLI flag) that sets the listen address, and a working example. |
| [Atomic batch updates](batch_update.md) | `BatchUpdate` — an ordered list of graph and vector mutations applied to one graph, decoded and validated before any RAM change, committed through the authoritative redb path. |

## Structured query surfaces

| Page | What it covers |
|---|---|
| [SQL & pgwire](sql.md) | `psql`, DBeaver, JDBC/ODBC, BI tools, ORMs — DataFusion `SELECT` (joins/CTE/window), full DML, DDL, `COPY`, `ALTER TABLE`, `CREATE FUNCTION` incl. PL/pgSQL, pgvector/AGE/Timescale/ParadeDB compat. |
| [SPARQL & RDF](sparql.md) | SPARQL 1.1 `SELECT`/`ASK`/`CONSTRUCT`/`DESCRIBE`/`UPDATE` over the same property graph — RDF and the graph are the same data. |
| [Cypher & Bolt](cypher.md) | `MATCH`/writes/`WITH`/aggregation over the engine's native graph primitives, plus GDS via `CALL gds.*`, on native Bolt v4.4. |
| [GraphQL](graphql.md) | A native, pure-Rust GraphQL surface introspected from the live graph, with Federation v2 and authenticated SSE subscriptions over CDC. |

## Modality-specific surfaces

| Page | What it covers |
|---|---|
| [Vector / ANN](vector.md) | A native, pure-Rust approximate-nearest-neighbour index (`eg-ann`) as the `SemanticStore` backend — IVF-PQ, OPQ, SQ8, HNSW, exact/flat. |
| [Time-series](timeseries.md) | A native, redb-backed time-series store (`eg-tsdb`) with ASOF joins, gap-fill, OHLC, decay, and SQL window frames. |
| [Key-value, embedded & blob](kv_blob.md) | The embedded in-process engine (the SQLite-style edge path), the content-addressed blob store, and the generic key-value surface. |
| [GIS / Spatial](gis.md) | A native geospatial modality (`eg-geo`, pure-Rust, no GEOS/PROJ) with CRS/reprojection, an R-tree, and routing/isochrones/TSP. |

## Messaging, caching & observability

| Page | What it covers |
|---|---|
| [Messaging & broker](messaging.md) | A RabbitMQ-class native message broker — exchanges, queues, bindings, DLQ, TTL, priority, delayed delivery, consumer groups, replayable streams, publisher confirms. |
| [Observability](observability.md) | A full observability backend (logs + metrics + traces) served from the same durable engine — PromQL, OTLP traces, VRL pipelines; the engine also emits its own OTLP + Prometheus remote-write. |
| [KV-cache (vLLM / LMCache)](kvcache.md) | A tiered, shared LLM KV-block cache acting as the durable L2 tier under vLLM's GPU cache and LMCache's CPU tier. See also the [remote KV-cache HTTP backend](../architecture/kvcache_remote_backend.md) for multi-worker sharing. |

## Agent & knowledge surfaces

| Page | What it covers |
|---|---|
| [Agent-native memory](memory.md) | A multi-level summary/abstraction ladder, episodic→semantic consolidation, importance decay/reinforcement, hierarchical retrieval, and a scene-graph world model. |
| [Ontology hosting & lifecycle](ontology.md) | Hosting OWL/RDFS ontologies, mapping them onto the property graph, and reasoning over them in-engine (OWL 2 EL⁺/RL, pure-Rust). |
| [Epistemic Operations Protocol](epistemic_operations.md) | The strict Rust projection of the shared protocol — one current-only vocabulary for request authority, mutation, ingestion, delegation, artifacts, and streamed results. |

## Client integration

| Page | What it covers |
|---|---|
| [Client drivers (Python / JS / Go)](clients.md) | The Python package is the complete current client; JavaScript and Go are thin bindings for the broker, streams, RBAC admin, backup/restore, and NL→query surfaces. |
