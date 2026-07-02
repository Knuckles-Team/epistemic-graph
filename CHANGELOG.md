# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [2.2.0] - 2026-07-02

> **Minor, additive, backward-compatible.** The "Universal-DB parity" session (waves 18–22,
> ~115 shipped concepts). Every feature below is behind its own Cargo feature and its own
> opt-in listener/env — a default/`pi`/`node`/`full` build that sets no new address is
> byte-for-byte the 2.1.0 engine. All pre-commit gates green. New surfaces fold into the
> deployment tiers per [`docs/operations/runbook.md`](docs/operations/runbook.md) (broker /
> PromQL / traces / S3 / NL-query / GeoSPARQL / federated-search / kvcache-server → `node`+`full`;
> the AMQP/MQTT/STOMP/Bolt/Redis wires → `cluster`).

### Added

- **Multi-wire keystone + the new protocol wires (`CONCEPT:EG-074`..`077`/`159`/`174`/`176`/`275`/`281`/`282`)** —
  the wire-agnostic `WireProtocol` trait (`EG-074`, parse→classify→`eg_query` exec→encode) that Postgres
  refactored behind with no behavior change now backs a family of hand-rolled listeners, each reusing the
  ONE exec path: **SQLite** NDJSON served surface (`EG-075`), **MySQL/MariaDB** handshake-v10 protocol
  (`EG-076`), **MSSQL** TDS (`EG-077`), **Neo4j Bolt** v4.4 / PackStream v2 routing RUN's Cypher to eg-query
  (`EG-159`), **Redis** RESP2/RESP3 over the durable KV surface (`EG-174`), an **S3-compatible** REST object
  surface over the BLOB CAS with SigV4-lite auth (`EG-176`), and the **AMQP 0.9.1** broker wire (`EG-275`).
- **Message broker + streams (`CONCEPT:EG-275`..`284`)** — the `KG-2.303` claim/ack work-queue is extended
  into a RabbitMQ-class broker: durable direct/topic/fanout exchanges + bindings/routing-keys + queues as
  `__control__` nodes (`EG-275`), **DLQs** (`EG-276`), **message TTL + queue expiry** (`EG-277`), **priority
  queues** (`EG-278`), **delayed/scheduled delivery** (`EG-279`), Kafka-style **replayable append-log streams**
  (`EG-283`), and **publisher confirms + consumer QoS acks** (`EG-284`). Reached over AMQP (`EG-275`), **MQTT
  3.1.1** (`EG-281`), and **STOMP 1.2** (`EG-282`) wires that map pub/sub onto the same primitives.
- **Observability suite (`CONCEPT:EG-163`/`165`/`172`/`243`)** — a **PromQL** evaluator + Prometheus HTTP query
  API over the eg-tsdb metric series (`EG-172`); **distributed traces** — OTLP/OTLP-JSON span ingest on
  `/v1/traces` into a span store + trace search (`EG-163`); **VRL-style ingest pipelines** (parse/filter/set/
  rename transforms at log/event ingest) (`EG-165`); and **super-cluster federated search** — a `/federated`
  entry that fans a read across a peer registry, unions/de-dups + RRF-re-ranks, tolerating slow/dead peers
  (`EG-243`). These build on the `EG-160`/`161` OpenObserve-style log ingestion + Parquet segments.
- **Postgres parity (`CONCEPT:EG-089`/`103`/`104`/`114`/`116`/`117`/`118`/`119`)** — `pg_catalog` +
  `information_schema` system views (`EG-103`), array/range types + common functions (`EG-104`), Apache-AGE
  `cypher()` over pgwire (`EG-114`), pgvector index pushdown (`EG-116`), TimescaleDB hypertables + continuous
  aggregates (`EG-117`), SQL stored functions via `CREATE FUNCTION` (`EG-118`), ParadeDB `@@@` BM25 full-text
  (`EG-119`), and columnar storage + SQL window frames (`EG-089`) — the drop-in surface for unmodified
  Postgres clients/ORMs and the pg-extension ecosystem (gated by the `EG-102` `CREATE EXTENSION` catalog).
- **RDF / SPARQL / OWL completeness (`CONCEPT:EG-101`/`133`..`137`/`146`/`155`/`261`)** — OGC **GeoSPARQL**
  baseline (`geo:`/`geof:` vocab, WKT/GML literals, `sfWithin`/`sfIntersects`/`distance`) (`EG-261`); **RCC8 +
  Egenhofer** topological relation families (`EG-155`); **JSON-LD** serialize/parse (`EG-136`); **TriG +
  N-Quads + RDF/XML** serialization (`EG-137`); SPARQL algebra completeness — ORDER BY/VALUES/MINUS/EXISTS
  (`EG-135`); **Integrity Constraint Validation** (ICV) (`EG-146`); **OBDA virtual graphs** (R2RML) (`EG-101`);
  **ShEx** shape-expression validation (`EG-133`); and the SPARQL 1.1 **Graph Store Protocol** + `COPY`/`MOVE`/
  `ADD` (`EG-134`).
- **Graph / Cypher / GraphQL (`CONCEPT:EG-144`/`159`/`295`/`296`)** — graph-data-science algorithms
  (centrality/community/pathfinding, reusing eg-compute) (`EG-144`); Neo4j **Bolt** wire (`EG-159`); GraphQL
  **Apollo Federation v2** subgraph support — `_service{sdl}` + `_entities` + `@key`/`@shareable`/`@external`
  (`EG-295`); and GraphQL **enterprise hardening** — APQ, query depth/complexity limits (`EG-296`).
- **New modalities (`CONCEPT:EG-084`..`089`)** — document/JSON deep indexing (`EG-084`), array/tensor store
  (`EG-085`), probabilistic / uncertainty distribution-valued properties (`EG-086`), scene-graph / 3D world
  model (`EG-087`), event-stream + complex-event-processing (`EG-088`), and columnar storage + SQL window
  frames (`EG-089`).
- **GIS / logistics (`CONCEPT:EG-255`..`267`)** — coordinate-reference-systems + reprojection (`EG-255`, CRS
  registry `EG-262`), geodesic ops (`EG-256`), full geometry model incl. multi-geometries + holes (`EG-257`),
  DE-9IM topological relations (`EG-258`), constructive geometry algebra (`EG-259`), durable **R-tree** spatial
  index (`EG-263`), geospatial format I/O — GeoJSON/WKB/GPX (`EG-264`), **map tiling** XYZ/TMS + Mapbox Vector
  Tiles (`EG-265`), **weighted routing + isochrones + TSP** (`EG-266`), and geo-anchored **map-based task
  tracking** (`EG-267`). Pure-Rust, no PROJ/C dep.
- **Agent-native memory + retrieval (`CONCEPT:EG-078`/`080`/`195`/`220`/`221`/`222`)** — the hierarchical
  summary-node memory tier (`EG-220`), episodic→semantic consolidation primitive (`EG-221`), memory decay +
  reinforcement maintenance (`EG-222`), **LeanRAG** hierarchical retrieval that drills summary→supporting
  through provenance edges (`EG-195`), and the **NL→query** seam: an injected `NlPlanner` (`EG-078`) plus the
  standalone `Method::NlQuery` + `/nl` HTTP route + `nl_query('…')` SQL UDF (`EG-080`).
- **LLM KV-cache tier (`CONCEPT:EG-185`/`186`/`187`)** — a new `eg-kvcache` crate: a tiered hot/warm/cold
  key→block cache with promotion/demotion (`EG-185`), a content-addressed `SharedKvBackend` so parallel
  instances dedup + share KV blocks by token-hash (`EG-186`), and a gated HTTP endpoint + vLLM/LMCache
  connector (`EG-187`).
- **Robotics, OBDA, vector, RBAC, backup/PITR, docs & benchmarks** — multimodal sensor fusion (`EG-098`),
  action/policy/trajectory episodic memory (`EG-099`), OBDA virtual graphs (`EG-101`), an exact/flat vector
  index + recall harness alongside IVF-PQ ANN (`EG-297`), RBAC-at-scale — durable roles + hierarchy + grants
  on the `security` tier (`EG-092`), online backup / restore + **PITR** (`EG-090`), the massive-scale benchmark
  harness (`EG-096`), and the comprehensive interface + operations documentation pass (`EG-095`).

## [2.1.0] - 2026-06-29

### Documentation
- **Universal-DB documentation accuracy pass** — `README.md`, `docs/capabilities.md`,
  `docs/roadmap.md`, `docs/interfaces/{sparql,sql,cypher,graphql}.md`, and
  `docs/architecture/engine.md` now reflect the engine's true, source-verified state. Features
  previously marked `🔶`/`🗺` but actually shipped are flipped to `✅`: SQL DDL + arbitrary user
  tables + `COPY`; SPARQL `ASK`/`CONSTRUCT`/`DESCRIBE` + `UPDATE` + the W3C `/sparql` HTTP
  endpoint + the named-graph quad dataset; Cypher writes (`CREATE`/`MERGE`/`SET`/`DELETE`);
  GraphQL mutations; the generic namespaced KV surface; multi-Raft groups + `GroupRouter` +
  online resharding; and N-participant cross-shard 2PC. `roadmap.md` is rewritten around the
  remaining "Universal-DB Program" items (`CONCEPT:EG-045..082`), each of which flips its
  capability-matrix row to `✅` as it lands.

### Added
- **Reserved read-admission lane (`CONCEPT:EG-044`)** — an interactive MCP read/query is now
  NEVER shed to `BUSY` behind an ingestion write firehose. The transport admission classifies
  read vs write (`requires_write`) and routes through a pure, unit-testable `admit_request`: a
  READ that loses the normal global+per-graph admission FALLS BACK to a dedicated
  `ServerState::read_admission` semaphore (auto-sized `max_inflight/8`, clamped 8..1024; env
  `EPISTEMIC_GRAPH_READ_RESERVED`) that writes can never touch and that BYPASSES the per-graph
  cap — so even when the `__commons__` firehose saturates both the global pool and that graph's
  cap, reads keep an open lane. Writes stay strictly back-pressured (shed `BUSY`, retry; never
  dropped). Only a genuine read flood that also fills the reserved lane is shed. New counter
  `epistemic_graph_read_reserved_admitted_total`. Reads continue to serve from MVCC snapshots
  (in-memory `GraphCore` snapshot for Cypher/SQL/GraphQL; `begin_read()` for the redb
  read-through, `CONCEPT:EG-027`), so the engine's redb tier never returns "database is locked".
  Proven by 3 new `transport::tests` (saturated-pool read admit, read-lane bound, 200 concurrent
  reads survive max write load on the hot graph).

## [0.32.0]

### Added
- **Per-graph write coalescer (`CONCEPT:KG-2.182`)** — concurrent single-op writes to ONE hot
  graph (the `__commons__` ingestion firehose) now batch onto a lazily-created per-graph writer
  (`src/write_coalescer.rs`) and apply under ONE `topo.write()` per batch, collapsing N
  topology-lock acquisitions into ⌈N/batch⌉. Writers are keyed by graph name in a `DashMap`
  (`ServerState.write_coalescer`) — created automatically for any new graph/connector, no
  hardcoded list (mirrors `per_graph_inflight`). `dirty`/WAL/gauge side-effects stay centralized
  in the dispatch shell, so durability and checkpoint contracts are byte-for-byte unchanged; CAS
  stays exactly-once; a full bounded queue falls back to the inline single-op path (never a stall
  or a drop). Default ON, batch auto-sized from cpu count; opt out with
  `EPISTEMIC_GRAPH_WRITE_COALESCE=0`. New Prometheus counters
  `epistemic_graph_write_batches_total` / `epistemic_graph_write_batched_ops_total` per graph.
  Micro-benchmark (50k writes, 64 pipelined producers, one graph): **57.5× fewer lock
  acquisitions, ~2× wall-clock**. See `docs/architecture/write-coalescer.md`.

### Added (prior, unreleased)
- **`GetTriples` bulk RDF export op** — exports a graph as RDF triples in one round-trip, the
  fast path backing local SPARQL over any durable backend (eliminates the per-node export loop).
- **Per-graph memory cap (E1/E3)** — `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH` (0=off): a periodic
  sweep (`EPISTEMIC_GRAPH_MEMCAP_INTERVAL`, default 10s) evicts any over-cap graph back to the cap
  via the existing LRU, so a shard **degrades instead of OOM-killing every tenant on it** (evicted
  nodes re-hydrate from the durable tier). Sweep never touches the write hot path. Also documents
  the durability model in `AGENTS.md`: the engine is a **rebuildable cache** over the abstracted
  durable backend (Postgres/neo4j/falkordb/ladybug), not the source of truth — hence no in-engine
  replication/consensus.
- **Rust CI gate (D1)** — `rust-ci.yml` runs `clippy -D warnings` + tests across the cargo feature
  matrix, the mechanical gate keeping the workspace from re-forming into a monolith.

### Changed
- **Cargo workspace decomposition** — the engine is now a 4-crate workspace along an acyclic
  dependency DAG `eg-types → eg-core → eg-compute → epistemic-graph` (imports point left only; a
  cycle won't compile). The Tokio server is decomposed into a thin `server/dispatch.rs` routing
  table over one `handlers/<domain>.rs` per protocol section, with write side-effects (in-flight
  gauge / `mark_dirty` / WAL enqueue) centralized in the shell. Cargo feature flags are now **real**
  (a slim `--features server` build links neither nalgebra nor tree-sitter), and a gated-out method
  falls to an explicit "not available in this build" arm. Dead `compute`/`execution` modules deleted
  (No-Legacy). `tokio` trimmed from `"full"` to its used feature set + `deny(unsafe_code)`.
- **`__bus__` commons graph renamed to `__commons__` (C3)** — the default commons graph was never a
  message bus; the misleading name is gone (atomic across every consumer, no alias kept).

### Removed
- **In-engine Kafka event bus (C1)** — deleted as dead code; event distribution is the durable
  backend's job, not the cache layer's.

### Fixed
- **WAL append moved off the tokio reactor (A1/A5)** — the synchronous WAL write that stalled the
  reactor is now off the hot path with group-commit fsync.
- **Per-call RPC timeout in the Python client (B1)** — every client RPC is now bounded by a
  per-call timeout; a hung shard no longer blocks the caller forever.
- **Batched node/edge property reads (A2)** — property reads are batched into one round-trip
  instead of per-element calls.
- **HNSW tombstones + deferred compaction (A4)** — vector overwrites tombstone instead of rebuilding
  the index per write, with compaction deferred.
- **Kyle insider/stealth surveillance kernels (CONCEPT:KG-2.20k)** — `kyle_lambda`
  (empirical Kyle's λ price impact, OLS of Δprice on signed net flow) and
  `surveillance_risk` (informed-flow share via `vpin_pm`, detection hazard,
  cumulative suspicion, stealth ratio, and a squashed `legal_risk_score` ∈ [0,1])
  in `crates/eg-compute/src/finance/quant.rs`, wired across `protocol.rs`,
  `handlers/finance.rs` and the Python client (`finance.kyle_lambda` /
  `finance.surveillance_risk`). Distils arXiv:2605.27684; defensive
  surveillance + maker adverse-selection protection only. Round-trip + unit
  tests added.
- **Protocol drift gate (CONCEPT:KG-2.19)** — `tests/test_protocol_parity.py` asserts the
  hand-written Python client and the Rust `Method` enum (165 variants) stay in lockstep across the
  PyO3-free MessagePack boundary: no client `_send("X")` without a matching variant, and the set of
  variants with no client binding is ratcheted against `tests/protocol_unbound_baseline.txt`. Wired
  into `rust-ci.yml` as a fast `--noconftest` job (no wheel build). Documented in
  `docs/transport_boundary_adr.md`.

- **`ParseFiles` batch AST op (CONCEPT:KG-2.16)** — parse N files in ONE round-trip instead of N
  per-file `ParseFile` calls. `Method::ParseFiles { files_msgpack }` (a MessagePack
  `Vec<(path, source)>`) → `parser::tree_sitter::parse_files`, which fans the files across rayon
  (each parse is stateless), returns an **ordered** `Vec<ParseResult>` (1:1 with input), and maps a
  per-file parse failure to an empty result so the batch never aborts. `Health` now advertises
  `version` + `ops` (e.g. `["ParseFiles"]`) for client capability negotiation. Client:
  `GraphOperationsClient.parse_files()` + `EpistemicGraphClient.supports()`. Version → 0.27.0.
- **Training loss / optimizer kernels (CONCEPT:KG-2.22)** — `src/datascience/training.rs`: pure-Rust
  `softmax` / `log_softmax`, `cross_entropy` (+ analytic grad), `dpo_loss` (Bradley-Terry, + chosen/rejected
  grads), `grpo_surrogate` (PPO/GRPO clipped, + grad with zero-grad clip region), `kl_divergence` (Schulman k3),
  and `adam_step` / `sgd_step` optimizers. The Wave-C / C1 performance path for the in-house training substrate
  — mirrors the pure-Python reference (`agent-utilities graph/training_signals.py`) and the torch kernels
  (`data-science-mcp trainers/objectives.py`), letting a trainer batch a step over the wire in one round-trip.
  Exposed end-to-end: `Method::Ds*` variants (`src/protocol.rs`), dispatch arms (`src/server.rs`), and
  `client.datascience.{softmax,log_softmax,cross_entropy,dpo_loss,grpo_surrogate,kl_divergence,adam_step,sgd_step}`
  (`epistemic_graph/client.py`, auto-exposed on the sync client). No candle/GPU — matches the existing pure-Rust
  `datascience` style. Tests: 8 inline Rust unit tests + 8 Python round-trip tests (`tests/test_compute_primitives.py`).

## [0.1.0] — 2026-05-24

### Added
- Initial Rust `epistemic-graph` engine implementation using `petgraph` stable graph.
- PyO3-based Python native extension bindings.
- DFS-based cycle detection returning exact cycle paths.
- BFS-based shortest path search and blast radius calculator.
- Applied ecosystem package standards including pre-commit, bumpversion, gitattributes, codespell, and pytest suite.
- Multi-stage testing Dockerfile and compose layout.
