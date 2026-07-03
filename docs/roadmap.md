# Universal-DB parity roadmap

epistemic-graph converges many modalities under one durable engine and one planner. This page tracks the
**remaining gaps** to full drop-in parity, with an honest status. It is the companion to the
[capability matrix](capabilities.md) and the [subsystems reference](architecture/subsystems.md). Status:
**🔶 in-progress** · **🗺 designed, not started**.

> The **Universal-DB Program** (EG-045..297) that this page used to track is essentially complete: the
> whole SQL/SPARQL/OWL/Cypher/GraphQL parity backlog shipped, and the 2.2.0 cycle (waves 18–22) added the
> multi-wire adapters, the message broker, the observability stack, GIS, tensors, streams, the LLM
> KV-cache, and the agent-memory primitives. **Program B** (waves B-1..B-6, EG-298..320) then turned the
> deferred tail from stubs into real implementations — the LTAP lakehouse tier, real pgvector ANN pushdown,
> GDS-over-Cypher, real ParadeDB BM25, exactly-once broker delivery, OTel egress, a QoS scheduler, durable
> RBAC/JSONPath, `ALTER TABLE`, R2RML, Shapefile/KML, routing turn-restrictions, HNSW, cross-shard kNN, and
> the memory/scene/trajectory wire surface. Those are now in [**Shipped in 2.2.0**](#shipped-in-220) and
> [**Shipped in Program B**](#shipped-in-program-b) below. What remains is a short list of
> **genuinely-deferred** items.

## Forward roadmap — genuinely deferred

| Item | Status | Notes |
|------|:------:|-------|
| **ROS2 real DDS/RTPS wire** | 🗺 | The rosbridge-WebSocket JSON bridge now ships (EG-325 — engine CDC ↔ ROS2 topics via `rosbridge_server`, no DDS C stack). A NATIVE DDS/RTPS wire (CycloneDDS/rmw) remains a documented optional leg (needs the CycloneDDS C toolchain, so it is not folded into the workspace `--all-features` build). |
| **Admin console UI** | 🗺 | A browser admin surface (tenants, shards, RBAC, backup/PITR). The engine exposes the APIs; the UI is unbuilt. |
| **Live dashboards UI** | 🗺 | A Grafana-style dashboard front-end over the PromQL/logs/traces query APIs (EG-172/302/162/163). The query side ships; the UI does not. |
| **Python LMCache connector (driver)** | ✅ | Shipped: `epistemic_graph.kvcache` (CONCEPT:EG-337) — a `pip`-installable vLLM/LMCache remote-backend driver for the EG-187 KV-cache endpoint. `RemoteKVConnector` (`get`/`put`/`contains`/`exists`/`stats` over `/kv/<hash>` + `/kv/stats`, bearer-token) + `RemoteKVL2Connector` (LMCache `native_plugin` L2 adapter). Stdlib-only import; `httpx` behind the `[lmcache]` extra. |
| **CUDA distance/tensor kernel — live GPU validation** | 🔶 | The GPU dispatch seam + real `cudarc` CUDA backend ship (EG-326/3.6): the CUDA-C kernels compile via NVRTC and launch on a device, with a CPU fallback. The kernels are correctness-matched to the CPU ground truth but await validation on real GPU hardware (none in CI); reasoning/ANN-build GPU offload beyond distance/elementwise remains open. |
| **Full Iceberg Avro manifest** | ✅ | The LTAP lakehouse tier (EG-317) ships a real spec-compliant Iceberg v2 **Avro** manifest + manifest-list writer (EG-333/EG-334, `crates/eg-lake/src/iceberg_avro.rs`, behind `lake` via pure-Rust `apache-avro`) — a committed snapshot's `metadata.json` now references real Avro that Spark/Trino/DuckDB follow. Documented omission: per-column stats + partition field-summary bounds (eg-lake tracks per-file counts only, unpartitioned spec). |
| **Raster tile pyramids** | 🗺 | Raster tile pyramids beyond the shipped vector-tile (MVT) + Shapefile/KML/GeoParquet GIS I/O (EG-265/306). |
| **PL/pgSQL procedural bodies** | ✅ | `CREATE FUNCTION … LANGUAGE plpgsql` bodies execute (EG-340/EG-341, `crates/eg-query/src/sql/plpgsql.rs`, folded into the `sql` feature — pure interpreter, no new deps). A hand-written statement interpreter runs `DECLARE` vars, `BEGIN..END`, `:=`, `IF/ELSIF/ELSE`, `LOOP`/`WHILE`/`FOR … LOOP` (integer range), `EXIT`/`CONTINUE`, `RETURN`, `RAISE`, and `SELECT … INTO var` over a variable environment; a bare `SELECT fn(args)` / `CALL proc(args)` triggers it and every embedded SQL/expression runs back through the existing read path. Documented out of scope: set-returning `RETURN NEXT/QUERY`, `FOR row IN <query>`, cursors, composite/`%ROWTYPE` vars, exception handlers, dynamic `EXECUTE`, and DML-in-body (read path). |
| **Raster tile pyramids** | ✅ | Shipped: the **raster** complement to EG-265's vector-tile (MVT) server (EG-338/EG-339, `crates/eg-geo/src/raster.rs`). A `Raster` (georeferenced Web-Mercator coverage grid: bbox + width×height × bands, serde-persistable like a `Geometry`) resamples per XYZ tile — `Raster::tile(z,x,y)` fetches a 256² `RasterTile` (nearest-neighbour, `nodata` outside), `Raster::build_pyramid(z_min,z_max)` emits the full pyramid with per-zoom tile counts. Includes a hand-rolled, **dependency-free** PNG codec (`to_png`/`decode_png`: stored-DEFLATE + hand-computed CRC-32/Adler-32, NO `image`/`png`/`flate2`), so eg-geo adds zero deps and the Pi contract holds (eg-geo is already out of `pi`; `cargo tree --features pi` links no `image`/`png`/eg-geo). Raw band tiles via `RasterTile::data`. Extends EG-265/306. |
| **PL/pgSQL procedural bodies** | 🗺 | Stored-procedure/function *procedural* execution (loops, variables, control flow) beyond the shipped SQL views + DDL. |
| **Memory → weights distillation** | 🗺 | Distilling consolidated agent-memory (EG-220/221) into model weights (a fine-tune/LoRA export), beyond the retrieval-time context assembly (EG-195). |
| **Calvin distributed read-lock (OLLP)** | ✅ | The Calvin global-sequencer total order + deterministic vote-free execution + crash-replay recovery ship (EG-324); the distributed OLLP *reconnaissance*/read-lock phase now SHIPS (EG-342): an OLLP reconnaissance read predicts a data-dependent txn's read/write set and a deterministic `OrderedLockManager` (keyed by the `GlobalSeq` total order) acquires read+write locks in sequence order, giving full serializable isolation of conflicting sequenced txns (the deterministic-execution phase then runs lock-free); and the multi-node sequencer epoch fan-in ships too (EG-343: `epoch_fan_in` deterministically merges each node's per-epoch sequenced inputs into one identical global order per epoch). `src/raft/cross_shard_txn.rs`, gated `calvin`/`harness` (out of `full`); proven live in `xshard_harness`. Documented remaining follow-up: the OLLP staleness DETECTION (`recon_still_valid`) ships, but the automatic re-sequence/restart of a txn whose recon went stale is not yet wired (single-active-sequencer / stable-recon deployments are unaffected). |
| **SQLite `.db` file I/O** | ✅ | The SQLite-dialect NDJSON-over-TCP wire ships (EG-075); reading/writing an on-disk `sqlite3` `.db` file now SHIPS too (EG-331 import / EG-332 export — `Method::ImportSqliteFile`/`ExportSqliteFile` over the user-table store). Behind the `sqlite-file` feature: it pulls `rusqlite` with the bundled C sqlite3 (no pure-Rust SQLite-file writer exists), so it is kept OUT of `pi`/`default` and folded into `full`/`node` — the Pi contract holds (`cargo tree --features pi` links no rusqlite/libsqlite3-sys). |

---

## Shipped in Program B — remaining engine tail (EG-3.x)

The last engine features of Program B, closing the distribution / robotics / GPU tail. Each
lands with tests + a [`docs/concepts.md`](concepts.md) entry and is feature-gated OUT of the
Pi tier (the heavy deps stay optional).

### Distribution
- **Cross-region async read-replica tier** — a bounded monotone-LSN replication log the primary
  serves over `/replicate?since=<lsn>` + an async follower pull-loop that applies the tail via the
  canonical `wal::apply` path, so a distant region gets a local eventually-consistent read copy
  with bounded staleness (EG-322). Beyond the synchronous multi-Raft groups + EG-243 federated read.
- **Capacity guardrails** — a per-target circuit breaker (Closed→Open→HalfOpen), a per-tenant hard
  concurrency quota, and global-high-water backpressure fronting the replica/transport paths (EG-323).
  Complements the EG-320 QoS scheduler with absolute ceilings + fail-fast.
- **Full Calvin deterministic-ordering commit** — a global sequencer total order + Raft-replicated
  input log + vote-free deterministic execution + crash-replay recovery, a third cross-shard commit
  branch opt-in alongside 2PC + Paxos-Commit-lite (EG-324). *(Distributed OLLP read-lock phase +
  multi-node sequencer fan-in remain — see forward roadmap.)*

### Robotics
- **ROS2 bridge over rosbridge-WebSocket** — engine CDC events ↔ ROS2 topics via the standard
  `rosbridge_suite` JSON-over-WebSocket protocol to a `rosbridge_server` (advertise/subscribe/publish/
  unsubscribe), with NO CycloneDDS/rmw/DDS C stack — a pure-Rust `tokio-tungstenite` client (EG-325).

### GPU
- **GPU-accelerated distance/tensor** — a `DistanceBackend`/`TensorBackend` dispatch seam (EG-326) with
  the pure-Rust CPU backend as the compiled-in fallback, plus a real `cudarc`-backed CUDA backend
  (EG-327) that NVRTC-compiles the batch-distance/elementwise kernel and launches it on a device, else
  falls back to CPU. `cudarc` is `dynamic-loading`, so the leg builds with no CUDA toolkit and a `pi`
  build links none of it. *(Live GPU-hardware validation of the kernels remains — see forward roadmap.)*

---

## Shipped in Program B

Program B (waves B-1..B-6, `CONCEPT:EG-298..320`) closed the deferred tail. Every item below is now ✅ —
see the [capability matrix](capabilities.md) for the per-operation evidence and [concepts](concepts.md) for
the authoritative `CONCEPT:EG-*` definitions.

### SQL / tables
- **`ALTER TABLE` beyond `ADD COLUMN`** — `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN TYPE`
  (with data migration), `DROP CONSTRAINT` on the durable user-table catalog (EG-310).
- **Real ParadeDB BM25** — real relevance scoring + highlighted snippets behind `@@@` / `paradedb.score()` /
  `snippet()` (EG-311, replacing the placeholder `1.0`).
- **Real pgvector ANN pushdown** — `ORDER BY col <-> $1 LIMIT k` pushes to the eg-ann HNSW/IVF index with an
  exact re-rank (EG-313).

### Lakehouse interop (LTAP)
- **eg-lake LTAP tier** — Parquet-on-object-store materialization + Delta transaction log + Iceberg-REST
  catalog + real Iceberg v2 **Avro** manifest/manifest-list writer (EG-333/EG-334) + LSN-style as-of snapshots,
  so external lakehouse engines (Databricks/Spark/Trino/DuckDB) read the engine's tables with zero ETL (EG-317).

### Graph / Cypher
- **GDS over Cypher** — `CALL gds.<algo>(…) YIELD …` (PageRank/Louvain/WCC/SCC/betweenness/Dijkstra/similarity)
  over eg-compute (EG-298).

### Vector / ANN
- **HNSW index** — hierarchical-navigable-small-world graph index for higher recall-per-probe than IVF-PQ,
  with insert/search/serde-persist (EG-301).
- **Cross-shard kNN scatter-gather** — a kNN query scatters across per-shard eg-ann indexes and merges to a
  deterministic global top-k via the `merge_topk` leaf (EG-319, completing EG-069).

### Broker / streams
- **Exactly-once broker delivery** — idempotent-producer dedup (producer id + sequence) + the stream/confirm/ack
  ops exposed over the AMQP `confirm.select` + MQTT 5 wire frames (EG-314).
- **Live CEP subscription** — register a CEP pattern, subscribe, receive pushed matches fed by the CDC bus
  (EG-299).
- **Redis pub/sub + S3 multipart** — RESP `SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH` + `MULTI`/`EXEC` (EG-174) and S3
  multipart upload + range GET (EG-176), both EG-307.

### RDF / OBDA
- **ICV write-path enforcement** — a commit guard rejects a transaction that would introduce a SHACL-as-constraint
  violation, configurable enforce/warn (EG-300).
- **Full R2RML Turtle parse** — standard R2RML mapping documents drive an OBDA virtual graph (EG-305).

### Observability
- **PromQL extended function set** — `_over_time` family, `delta`/`idelta`/`deriv`, `topk`/`bottomk`/`quantile`,
  `label_replace`/`label_join`, `clamp*` (EG-302).
- **OTel export + Prometheus remote-write + OTLP** — the engine emits its own metrics/traces to an external OTel
  collector + accepts a Prometheus remote-write receiver (EG-316).

### GIS
- **Shapefile / KML / GeoParquet I/O** — ESRI Shapefile, KML/KMZ, and GeoParquet reader/writer round-tripping
  eg-geo geometries + attributes (EG-306).
- **Routing turn-restrictions + time-windows** — turn-restriction penalties + time-dependent edge weights on the
  EG-266 router (EG-312).

### KV-cache
- **Real KV warm-tier compression** — zstd (optional lz4) replacing the RLE fallback (EG-315).

### Durability / wire surface / scheduling
- **Durable RBAC persistence** — roles/grants + agent identities persist to redb + reload at boot (EG-303).
- **Tensor-op CAS write-back** — derived tensors persist into the content-addressed tensor store on the exec path
  (EG-304).
- **Durable JSONPath index** — the inverted JSONPath index persists to redb + rehydrates at boot + feeds planner
  cost `Stats` (EG-308).
- **Federated typed result fusion** — schema-aware SQL + SPARQL column-union + typed dedup/merge (EG-309).
- **Memory/scene/trajectory wire-Ops** — the agent-memory + scene-graph + trajectory library APIs exposed over the
  wire as additive `Method`s + dispatch + WAL replay (EG-318).
- **Real-time QoS/SLO scheduler** — per-tenant/priority admission + deadline scheduling + backpressure (EG-320).

### Analytics program — numeric kernel (one kernel, two surfaces)
- **P1 (this release) — numeric kernel + Python shim** — `crates/eg-numeric` (faer + ndarray, BLAS/LAPACK-free):
  the curated numpy op-surface (reductions/stats, element-wise, the linalg-6 + LinAlgError, random) as a pure Rust
  rlib, exposed on **Surface A** (Python extension `epistemic_graph.numeric`, feature `python`) and consumed by
  `agent_utilities.numeric.xp` (KG-2.311); gated behind a `numeric` cargo feature (in `full`, out of `pi`); parity-proven
  `np.allclose` vs numpy incl. nan/inf/singular/empty edge cases (EG-321). **Done.**
- **P2–P3 — migrate the 598 numpy sites** — swap the 32 light-op files then the 6 linalg files in agent-utilities from
  numpy to the `xp` shim. 🗺
- **P4 — Surface B engine operators** — DataFusion SQL UDFs/UDAFs over the kernel + kernel-backed batch vector ops.
  **In progress:** `cosine_sim`/`l2_normalize`/`zscore` scalar UDFs + `covariance` UDAF registered on the SQL surface
  (`SELECT zscore(price) FROM …`, `SELECT cosine_sim(a.emb, b.emb) …` run in-engine), plus the `BatchL2Normalize` engine
  Method (EG-329/EG-330), all behind the `numeric` feature (in `full`, out of `pi`). **Also shipped:** the
  `svd(vec_col)`/`pca(vec_col,k)` column→matrix UDAFs (EG-336/EG-335 — a row-buffering accumulator marshals a column of
  vectors into a dense `ndarray::Array2`, then faer `svdvals`/`eigh`), plus `kmeans(vec_col,k)` (EG-344 — the same
  column→matrix accumulator driving a new pure-Rust `eg-numeric::cluster` Lloyd + k-means++ kernel, **no linfa/BLAS**;
  one `List<Int64>` cluster label per row). **The differentiator — shipped (EG-345):** *cross-modal join → analytics
  in-engine* — one SQL statement joins graph ⋈ vector ⋈ timeseries and runs `pca`/`kmeans`/`covariance` over the joined
  result set (**impossible in numpy** — no data layer), E2E-proven in `crates/eg-query/tests/cross_modal_analytics.rs`.
  **Deferred (next P4 increment):** graph-algo/timeseries unification under the kernel via native `Method` surfaces. ▶ ✅
- **P5 — drop numpy/scipy** from agent-utilities; the `eg-numeric` wheel is the dep. 🗺

See [`architecture/numeric-kernel.md`](architecture/numeric-kernel.md).
### Client drivers
- **Multi-language client drivers (B1.7)** — thin client bindings for the Program-B engine `Method`s that had no
  client surface: the native broker + append-log streams (EG-275..284/314), RBAC admin (EG-092), online
  backup/restore (EG-090), and NL→query (EG-080). A FULL Python surface (`client.broker`/`.rbac`/`.admin` +
  `query.nl_query`) plus THIN generated-from-the-Method-list JS (`clients/js`) and Go (`clients/go`) bindings over
  the same framed-MessagePack transport; Pi-contract preserved (no heavy deps). See
  [`interfaces/clients.md`](interfaces/clients.md) (EG-328).

---

## Shipped in 2.2.0

Everything below was previously listed here as roadmap and is now ✅ — see the
[capability matrix](capabilities.md) for the per-operation evidence and [concepts](concepts.md) for the
authoritative `CONCEPT:EG-*` definitions.

### SQL — full Postgres + multi-wire parity
- Compound/complex DML WHERE, `INSERT … SELECT`, `UPDATE…FROM`/`DELETE…USING` (EG-045/046/047).
- `ON CONFLICT` upsert + `RETURNING` (EG-048); wire transactions `BEGIN`/`COMMIT` mixing graph + user tables
  with RYOW + `TransactionStatus` (EG-049); `CREATE VIEW` / `DROP VIEW` durable catalog (EG-072).
- SQL DDL + arbitrary user tables + `COPY`.

### SPARQL — full Stardog/GraphDB parity
- Content negotiation (XML/CSV/TSV/Turtle; EG-050), rich FILTER (regex/arithmetic/`IN`/builtins; EG-053),
  `FROM`/`FROM NAMED` (EG-054), sub-SELECT · SERVICE federation · MINUS · negated property sets
  (EG-051/052/055/056), SPO/POS triple index + selectivity join-ordering (EG-057).
- `ASK`/`CONSTRUCT`/`DESCRIBE` + `UPDATE` + the W3C `/sparql` endpoint; true named-graph quad dataset.

### OWL / RDF validation
- `rdfs:range` in EL completion (EG-058), OWL-DL tableau (EG-059), SWRL/RuleML rules (EG-060).
- **SHACL Core** validation (`eg-shacl`, EG-132), **ShEx** validation (`eg-shex`, EG-133), and
  **Integrity Constraint Validation** with guard mode (EG-146).

### Graph — full Neo4j parity + Bolt wire
- Cypher `REMOVE` (EG-061), `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/`DISTINCT` (EG-062),
  var-length + fixed hops + path binding (EG-063); writes (`CREATE`/`MERGE`/`SET`/`DELETE`).
- **Neo4j Bolt v4.4** wire adapter so native drivers connect (EG-159).

### GraphQL
- Subscriptions over CDC (EG-064), fragments/variables/directives (EG-065), Relay pagination (EG-066).
- **Apollo Federation v2 subgraph** (EG-295) + **enterprise hardening** (APQ, depth/cost limits,
  introspection toggle; EG-296); GraphQL mutations.

### Time-series · Vector · Blob
- `Op::Window` execution (EG-067), per-point retention trim (EG-068),
  hybrid metadata pre-filtering (EG-070), content-defined chunking (EG-071).
- **Exact/flat vector index + recall@k harness** (EG-297).

### Multi-wire adapters (one `WireProtocol` trait)
- The **`WireProtocol`/`WireSession` keystone** with pgwire refactored behind it (EG-074), plus
  **SQLite** (EG-075), **MySQL/MariaDB** (EG-076), **MSSQL TDS** (EG-077), **Redis RESP** (EG-174), and the
  **S3 REST** surface (EG-176).

### Message broker (Phase Y)
- The native engine task queue `ClaimNext` (KG-2.303) grown into a RabbitMQ-class broker: exchanges/routing +
  **AMQP** wire (EG-275), **MQTT** (EG-281) and **STOMP** (EG-282) wires, DLQ (EG-276), TTL/expiry (EG-277),
  priority queues (EG-278), delayed/scheduled delivery (EG-279), consumer groups + prefetch/QoS (EG-280),
  publisher confirms + consumer acks (EG-284), and replayable append-log streams (EG-283).

### Observability (Phase T)
- Log ingestion (OTLP/`_bulk`/syslog; EG-160), Parquet-on-object-store segments (EG-161), log search +
  `_search` API (EG-162), distributed traces (EG-163), VRL ingest pipelines (EG-165), **PromQL** query API
  (EG-172), and **super-cluster federated search** (EG-243).

### GIS (eg-geo)
- Spatial modality (EG-083) built to real-GIS depth: full geometry model (EG-257), DE-9IM + RCC8/Egenhofer
  (EG-258/155), constructive algebra (EG-259), geodesic ops (EG-256), CRS registry + reprojection
  (EG-255/262), durable R-tree (EG-263), GeoJSON/WKB/GPX I/O (EG-264), map tiling + MVT (EG-265),
  raster tile pyramids + hand-rolled PNG (EG-338/339), Shapefile/KML/GeoParquet I/O (EG-306),
  routing/isochrones/TSP (EG-266), map-anchored tasks (EG-267), and GeoSPARQL (EG-261).

### New modality engines + subsystems
- **eg-tensor** N-D array store (EG-085), **eg-stream** windowed CEP (EG-088), **eg-kvcache** tiered +
  shared KV-cache with the vLLM/LMCache server endpoint (EG-185/186/187), and the **agent-memory**
  primitives — summary tier (EG-220), episodic→semantic consolidation (EG-221), decay/reinforcement
  (EG-222), trajectory memory (EG-099), LeanRAG retrieval (EG-195).

### Unified planner / UQL · Distribution
- `Op::Foreign` name → `ForeignSourceSpec` resolution (EG-073), NL → query dual-mode (EG-078/079/080).
- Multi-Raft groups + `GroupRouter` + online resharding, N-participant cross-shard 2PC with crash recovery,
  parallel-commit + read-only-participant fast path (EG-081), non-blocking commit (EG-082).

---

**Reading this as a contributor?** The forward-roadmap table above is the whole of what's left. Each item
lands with tests + a `docs/concepts.md` entry, and flips to ✅ in the [capability matrix](capabilities.md)
as it merges.
