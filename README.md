# epistemic-graph

<p align="center">
  <b>One durable, Rust-native engine that is a multi-modal analytical database — graph · SQL · vector · RDF/OWL · time-series · key-value/blob — behind one query planner and one durable store.</b><br>
  <sub>Every modality is a first-class view over one <code>RowSet</code> algebra, from a Raspberry Pi to a replicated Raft cluster, from one core.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-2.2.0-blue" alt="Version">
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
- **Scales by configuration, not by rewrite.** The *same binary family* runs embedded in-process on a
  Pi, as a single durable server, or as a multi-node Raft cluster with cross-shard transactions.
  → [Tiers & binaries](docs/architecture/tiers.md) · [Cluster deployment](docs/architecture/cluster_deployment.md)
- **Drop-in wire compatibility.** Existing Postgres/Neo4j/Redis/S3/AMQP/PromQL clients connect
  unmodified. → [Connecting (per-wire guide)](docs/interfaces/connecting.md)
- **Pi-lean core.** Heavy modalities are Cargo-feature-gated; a Pi build links no DataFusion, no
  Tantivy, no CUDA, no C toolchain. → [Tiers & binaries](docs/architecture/tiers.md)

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
| **SQL / pgwire** | `psql`, DBeaver, JDBC/ODBC, BI tools, ORMs — DataFusion `SELECT` (joins/CTE/window), full DML, arbitrary user tables + DDL + `COPY` + `ALTER TABLE`, `CREATE FUNCTION` incl. **PL/pgSQL** bodies, `pg_catalog`/`information_schema`, pgvector/AGE/Timescale/ParadeDB compat. **pgwire ships in `node`/`full`/`cluster`.** | [SQL & pgwire](docs/interfaces/sql.md) |
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

## Deployment tiers

The engine ships as a small family of prebuilt, size-optimized binaries. A Pi pulls a prebuilt wheel and
never compiles. Full map: [tiers & binaries](docs/architecture/tiers.md).

| Binary | Carries | For |
|--------|---------|-----|
| **pi** | redb-authoritative + cypher + ann + rdf/sparql/owl + streaming — **no DataFusion, no Tantivy, no Raft** | Raspberry Pi / edge, ultra-lean |
| **pi-max** | pi + tsdb + blob + security — all pure-Rust, no C toolchain | Pi "everything without a C compiler" |
| **node** (default wheel) | pi + DataFusion SQL + GraphQL + Tantivy text + wasm-udf + federation + **pgwire** — a complete single-node DB | single durable server / SQL clients |
| **cluster** | node (incl. pgwire) + Raft + cross-shard 2PC + the extra wire protocols | multi-node HA |
| **full** | every single-node feature (incl. pgwire, the `numeric` kernel, kvcache-server, broker, LTAP), no raft | workstation / one binary, every feature |
| **full-extras** | `full` + optional accelerator legs (`gpu-cuda`, `ros2-bridge`) | GPU / robotics — out of `pi` |

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
  [Numeric kernel](docs/architecture/numeric_kernel.md) ·
  [Lakehouse LTAP](docs/architecture/lakehouse_ltap.md) ·
  [Distribution / Robotics / GPU](docs/architecture/distribution_robotics_gpu.md).
- Deploy & operate: [Tiers & binaries](docs/architecture/tiers.md) ·
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
