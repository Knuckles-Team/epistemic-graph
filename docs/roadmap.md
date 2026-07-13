# Forward roadmap — genuinely deferred

> This is an **internal, contributor-facing** page, not a headline doc. The authoritative,
> operation-by-operation status lives in the **[capability matrix](capabilities.md)**; every shipped
> capability has a deep-dive under [Query Surfaces](interfaces/sql.md) or
> [Analytics & Distribution](architecture/analytics_program.md), and the authoritative `CONCEPT:EG-*`
> definitions are in [concepts](concepts.md). The historical "Universal-DB Program" backlog
> (EG-045..345 — SQL/SPARQL/OWL/Cypher/GraphQL parity, multi-wire adapters, broker, observability, GIS,
> tensors, streams, KV-cache, agent-memory, the LTAP lakehouse, real ANN pushdown, GDS, PL/pgSQL, SQLite
> `.db` I/O, raster pyramids, the numeric kernel + Surface-B analytics UDFs, Calvin, ROS2/DDS) is
> **shipped** — see the capability matrix and `CHANGELOG.md`.

The **[North Star: Seamless](north_star.md)** page tracks the *cross-modal seam* backlog specifically —
every write→read path that crosses modalities, per surface (RPC, SQL wires, SPARQL, GraphQL). The
in-transaction cross-modal seam is **done** over RPC (EG-359..363) and **pgwire** (EG-KG.txn.isolation-ryow-begin-set); mysql/mssql
wire parity, GraphQL cross-modal, and in-txn tsdb read-your-own-writes are the open rows there.

What remains is a short list of genuinely-deferred items. Each is folded, as a note, into the deep-dive
that owns it.

| Item | Status | Owning deep-dive |
|------|:------:|------------------|
| **Admin console UI** — browser surface for tenants/shards/RBAC/backup-PITR (the engine exposes the APIs; the UI is unbuilt) | 🗺 | [Operations Runbook](operations/runbook.md) |
| **Live dashboards UI** — a Grafana-style front-end over the shipped PromQL/logs/traces query APIs (the query side ships; the UI does not) | 🗺 | [Observability](interfaces/observability.md) |
| **GPU offload beyond distance/elementwise** — reasoning / ANN-build kernels on the GPU (the distance + elementwise CUDA kernels ship and auto-validate on any GPU host) | 🔶 | [Distribution / Robotics / GPU](architecture/distribution_robotics_gpu.md) |
| **Calvin multi-node epoch routing** — the RESTART-EPOCH ROUTING LOGIC ships (`CrossShardCoordinator::acquire_ollp_with_restart_epoch` + `epoch_for_restart`/`epoch_packed_seq`/`EpochFanInRegistry`, `src/raft/cross_shard_txn.rs`, gated `calvin`/`harness`; commit `88c9f34`): a restarted OLLP txn is routed into a specific, deterministic epoch of the multi-node fan-in (attempt N → `base_epoch + (N-1)`), packed into a cross-epoch-comparable `GlobalSeq`, so every node re-deriving the same exchanged per-epoch batches lands the restart at the identical epoch + seq. Proven by pure-logic tests (`eg349_*`) + the live multi-node harness `calvin_ollp_epoch_routing_restart_agrees_across_nodes`. **Remaining (genuine deferral, not the routing itself):** the real cross-node GOSSIP/REPLICATION TRANSPORT that populates each node's `EpochFanInRegistry` with every peer's per-epoch batch before an epoch closes — participants are local groups on the coordinator node today; a real cross-host soak of that transport is the open step. | 🔶 (routing ✅; cross-node transport deferred) | [Distribution / Robotics / GPU](architecture/distribution_robotics_gpu.md) |
| **Numeric Surface-B — graph/timeseries unification** — bringing graph-algo + timeseries ops under the `eg-numeric` kernel via native `Method` surfaces (the vector/stat/linalg UDFs + cross-modal analytics ship) | 🗺 | [Analytics Program](architecture/analytics_program.md) · [Numeric Kernel](architecture/numeric_kernel.md) |
| **Numeric migration P2/P3/P5** — the *agent-utilities-side* swap of its 598 numpy sites to the `xp` shim and the eventual numpy/scipy drop (the kernel wheel now publishes to PyPI, so this is a downstream dependency swap) | 🗺 | [Numeric Kernel](architecture/numeric_kernel.md) |

Legend: **🔶 in-progress** · **🗺 designed, not started**.
| Item | Status | Notes |
|------|:------:|-------|
| **ROS2 real DDS/RTPS wire** | ✅ | The rosbridge-WebSocket JSON bridge ships (EG-KG.domains.robotics-gpu-distribution — engine CDC ↔ ROS2 topics via `rosbridge_server`, no DDS C stack). **EG-KG.ingest.dds-transport** adds the transport SEAM (`src/server/dds.rs`, the `DdsTransport` trait unifying the WS bridge and TWO native DDS legs behind ONE interface): a real, CI-buildable native DDS/RTPS leg behind the `ros2-dds` feature via the **pure-Rust `rustdds`** crate (native Rust DDS + RTPS — mio/pnet/speedy/cdr-encoding, **no CycloneDDS/rmw/C toolchain**), and — S5 — the **CycloneDDS-C-backed `rmw` leg** behind the `ros2-rmw` feature via the safe `cyclonedds` Rust crate (vendored C sources, cmake-built, no libclang/network at build time), the REAL `rmw_cyclonedds` stack for genuine zero-config live-`ros2` interop. Both legs are exercised by a real RTPS loopback pub/sub test and share the SAME `rmw` topic-name/type-name mangling (EG-KG.ingest.rmw-topic-prefix). Kept OUT of the main build — only the opt-in `full-extras` layer (the `default`/`full` build links no rustdds/cyclonedds). **Residual:** the ROS 2 Iron/Jazzy type-*hash* (`RIHS01_…`) typesupport descriptor is deferred on both legs — Humble matches by mangled type *name* only, which both satisfy. |
| **Admin console UI** | 🗺 | A browser admin surface (tenants, shards, RBAC, backup/PITR). The engine exposes the APIs; the UI is unbuilt. |
| **Live dashboards UI** | 🗺 | A Grafana-style dashboard front-end over the PromQL/logs/traces query APIs (EG-172/302/162/163). The query side ships; the UI does not. |
| **Python LMCache connector (driver)** | ✅ | Shipped: `epistemic_graph.kvcache` (CONCEPT:EG-KG.backend.shipped-pip-installable-python) — a `pip`-installable vLLM/LMCache remote-backend driver for the EG-KG.backend.is-configured-so-co KV-cache endpoint. `RemoteKVConnector` (`get`/`put`/`contains`/`exists`/`stats` over `/kv/<hash>` + `/kv/stats`, bearer-token) + `RemoteKVL2Connector` (LMCache `native_plugin` L2 adapter). Stdlib-only import; `httpx` behind the `[lmcache]` extra. |
| **CUDA distance/tensor kernel — live GPU validation** | 🔶 | The GPU dispatch seam + real `cudarc` CUDA backend ship (EG-KG.compute.gpu-distance-seam/3.6): the CUDA-C kernels compile via NVRTC and launch on a device, with a CPU fallback. The kernels are correctness-matched to the CPU ground truth but await validation on real GPU hardware (none in CI); reasoning/ANN-build GPU offload beyond distance/elementwise remains open. |
| **Full Iceberg Avro manifest** | ✅ | The LTAP lakehouse tier (EG-KG.storage.lsn-as-snapshot-returns) ships a real spec-compliant Iceberg v2 **Avro** manifest + manifest-list writer (EG-KG.storage.eg-iceberg-avro-manifest/EG-KG.storage.iceberg-manifest-list, `crates/eg-lake/src/iceberg_avro.rs`, behind `lake` via pure-Rust `apache-avro`) — a committed snapshot's `metadata.json` now references real Avro that Spark/Trino/DuckDB follow. Per-column stats (`column_sizes`/`value_counts`/`null_value_counts`/`nan_value_counts`/`lower_bounds`/`upper_bounds`, keyed by field-id) are gathered at materialize time and emitted so external readers do predicate pushdown / file skipping (EG-KG.storage.iceberg-avro-manifest-carries). Partition `field_summary` is null by design — the spec is unpartitioned (zero partition fields). |
| **CUDA distance/tensor kernel — live GPU validation** | ✅ | The GPU dispatch seam + real `cudarc` CUDA backend ship (EG-KG.compute.gpu-distance-seam/EG-KG.backend.real-cuda-tensor-backend): the CUDA-C kernels compile via NVRTC and launch on a device, with a CPU fallback. **EG-KG.compute.tensor-gpu-distance** closes the validation gap with a GPU-gated parity test in each crate (eg-ann distance + eg-tensor elementwise) that detects a device at runtime and, when one is present, asserts the real CUDA kernel == the CPU ground truth (bitwise-close — f32 relative tolerance for the accumulating distance kernel, exact for the single-op f64 elementwise kernel), else SKIPS cleanly. So the kernels now **auto-validate wherever a GPU exists** (e.g. the GB10 box) while the test remains a no-op in GPU-less CI. Documented remaining: reasoning/ANN-build GPU offload beyond distance/elementwise. |
| **Full Iceberg Avro manifest** | ✅ | The LTAP lakehouse tier (EG-KG.storage.lsn-as-snapshot-returns) ships a real spec-compliant Iceberg v2 **Avro** manifest + manifest-list writer (EG-KG.storage.eg-iceberg-avro-manifest/EG-KG.storage.iceberg-manifest-list, `crates/eg-lake/src/iceberg_avro.rs`, behind `lake` via pure-Rust `apache-avro`) — a committed snapshot's `metadata.json` now references real Avro that Spark/Trino/DuckDB follow. Documented omission: per-column stats + partition field-summary bounds (eg-lake tracks per-file counts only, unpartitioned spec). |
| **Raster tile pyramids** | 🗺 | Raster tile pyramids beyond the shipped vector-tile (MVT) + Shapefile/KML/GeoParquet GIS I/O (EG-KG.domains.map-tiles/306). |
| **PL/pgSQL procedural bodies** | ✅ | `CREATE FUNCTION … LANGUAGE plpgsql` bodies execute (EG-KG.query.eg-validate-procedural-body/EG-KG.query.concept-7, `crates/eg-query/src/sql/plpgsql.rs`, folded into the `sql` feature — pure interpreter, no new deps). A hand-written statement interpreter runs `DECLARE` vars, `BEGIN..END`, `:=`, `IF/ELSIF/ELSE`, `LOOP`/`WHILE`/`FOR … LOOP` (integer range), `EXIT`/`CONTINUE`, `RETURN`, `RAISE`, and `SELECT … INTO var` over a variable environment; a bare `SELECT fn(args)` / `CALL proc(args)` triggers it and every embedded SQL/expression runs back through the existing read path. Documented out of scope: set-returning `RETURN NEXT/QUERY`, `FOR row IN <query>`, cursors, composite/`%ROWTYPE` vars, exception handlers, dynamic `EXECUTE`, and DML-in-body (read path). |
| **Raster tile pyramids** | ✅ | Shipped: the **raster** complement to EG-KG.domains.map-tiles's vector-tile (MVT) server (EG-KG.domains.raster-build/EG-KG.domains.raster-fetch, `crates/eg-geo/src/raster.rs`). A `Raster` (georeferenced Web-Mercator coverage grid: bbox + width×height × bands, serde-persistable like a `Geometry`) resamples per XYZ tile — `Raster::tile(z,x,y)` fetches a 256² `RasterTile` (nearest-neighbour, `nodata` outside), `Raster::build_pyramid(z_min,z_max)` emits the full pyramid with per-zoom tile counts. Includes a hand-rolled, **dependency-free** PNG codec (`to_png`/`decode_png`: stored-DEFLATE + hand-computed CRC-32/Adler-32, NO `image`/`png`/`flate2`), so eg-geo adds zero deps and the no-new-C-deps invariant holds (`cargo tree` links no `image`/`png`/`flate2`). Raw band tiles via `RasterTile::data`. Extends EG-KG.domains.map-tiles/306. |
| **PL/pgSQL procedural bodies** | 🗺 | Stored-procedure/function *procedural* execution (loops, variables, control flow) beyond the shipped SQL views + DDL. |
| **Memory → weights distillation** | ✅ | Distilling consolidated agent-memory (EG-KG.compute.hierarchical-summary-tier-eg/221) into model weights (a fine-tune/LoRA export), beyond the retrieval-time context assembly (EG-195), now SHIPS — on the `agent-utilities` side (CONCEPT:AU-KG.memory.memory-weights-distillation-export): `MemoryWeightsDistiller` reads the consolidated/procedural memory tiers and renders SFT/DPO corpora, plus a GRPO renderer for EG-099 `:Trajectory`/`:Step` reward sequences, then hands off via `graph_analyze action=distill_memory` to the `train-model` workflow, which runs the LoRA/SFT/GRPO fine-tune in `agents/data-science-mcp` (GPU-gated) and deploys the checkpoint through the model-registry role-bind seam, optionally hot-loading it onto a live vLLM/SGLang server (`POST /v1/load_lora_adapter`). See [Agent Memory](interfaces/memory.md). |
| **Calvin distributed read-lock (OLLP)** | ✅ | The Calvin global-sequencer total order + deterministic vote-free execution + crash-replay recovery ship (EG-KG.txn.calvin-deterministic-ordering); the distributed OLLP *reconnaissance*/read-lock phase now SHIPS (EG-KG.txn.concept-2): an OLLP reconnaissance read predicts a data-dependent txn's read/write set and a deterministic `OrderedLockManager` (keyed by the `GlobalSeq` total order) acquires read+write locks in sequence order, giving full serializable isolation of conflicting sequenced txns (the deterministic-execution phase then runs lock-free); and the multi-node sequencer epoch fan-in ships too (EG-KG.txn.concept-3: `epoch_fan_in` deterministically merges each node's per-epoch sequenced inputs into one identical global order per epoch). `src/raft/cross_shard_txn.rs`, gated `calvin`/`harness` (out of `full`); proven live in `xshard_harness`. The OLLP recon-staleness RESTART now ships too (EG-KG.txn.concept-4): when `recon_still_valid` is false under the held ordered locks, `acquire_ollp_with_restart` re-reconnoiters and re-submits the txn at a fresh `GlobalSeq` bounded by `max_restarts`, deterministic within the single-active-sequencer ordering domain (proven by `calvin_ollp_stale_recon_is_restarted_and_commits_serializably`). Remaining open (EG-KG.txn.concept-3 scope, not a correctness gap here): routing a restarted txn into a specific epoch of the multi-node fan-in. |
| **SQLite `.db` file I/O** | ✅ | The SQLite-dialect NDJSON-over-TCP wire ships (EG-KG.query.concept-3); reading/writing an on-disk `sqlite3` `.db` file now SHIPS too (EG-KG.query.eg-feature import / EG-KG.query.full-protocol export — `Method::ImportSqliteFile`/`ExportSqliteFile` over the user-table store). Behind the `sqlite-file` feature: it pulls `rusqlite` with the bundled C sqlite3 (no pure-Rust SQLite-file writer exists), it is part of the one main build (`rusqlite` bundles its C sqlite3, so no external toolchain is needed). |

---

## Shipped in Program B — remaining engine tail (EG-3.x)

The last engine features of Program B, closing the distribution / robotics / GPU tail. Each
lands with tests + a [`docs/concepts.md`](concepts.md) entry and is feature-gated OUT of the
Pi tier (the heavy deps stay optional).

### Distribution
- **Cross-region async read-replica tier** — a bounded monotone-LSN replication log the primary
  serves over `/replicate?since=<lsn>` + an async follower pull-loop that applies the tail via the
  canonical `wal::apply` path, so a distant region gets a local eventually-consistent read copy
  with bounded staleness (EG-322). Beyond the synchronous multi-Raft groups + EG-KG.ontology.federation-client federated read.
- **Capacity guardrails** — a per-target circuit breaker (Closed→Open→HalfOpen), a per-tenant hard
  concurrency quota, and global-high-water backpressure fronting the replica/transport paths (EG-323).
  Complements the EG-320 QoS scheduler with absolute ceilings + fail-fast.
- **Full Calvin deterministic-ordering commit** — a global sequencer total order + Raft-replicated
  input log + vote-free deterministic execution + crash-replay recovery, a third cross-shard commit
  branch opt-in alongside 2PC + Paxos-Commit-lite (EG-KG.txn.calvin-deterministic-ordering). *(Distributed OLLP read-lock phase +
  multi-node sequencer fan-in remain — see forward roadmap.)*

### Robotics
- **ROS2 bridge over rosbridge-WebSocket** — engine CDC events ↔ ROS2 topics via the standard
  `rosbridge_suite` JSON-over-WebSocket protocol to a `rosbridge_server` (advertise/subscribe/publish/
  unsubscribe), with NO CycloneDDS/rmw/DDS C stack — a pure-Rust `tokio-tungstenite` client (EG-KG.domains.robotics-gpu-distribution).
- **Native DDS/RTPS transport seam + leg** — the `DdsTransport` trait (`src/server/dds.rs`, EG-KG.ingest.dds-transport)
  puts the WS bridge and a native DDS/RTPS transport behind ONE interface, so the CDC↔ROS2 path targets
  EITHER (both put the identical `std_msgs/String` payload on the wire via the shared EG-KG.domains.robotics-gpu-distribution shaping).
  The native leg (`ros2-dds` feature) speaks real RTPS via the pure-Rust `rustdds` crate — NO
  CycloneDDS/rmw/C toolchain, so it actually builds in CI and is covered by a real RTPS loopback
  pub/sub test. Kept OUT of the main build (only the opt-in `full-extras` layer). The CycloneDDS-C `rmw` leg
  stays a toolchain-gated future option.

### GPU
- **GPU-accelerated distance/tensor** — a `DistanceBackend`/`TensorBackend` dispatch seam (EG-KG.compute.gpu-distance-seam) with
  the pure-Rust CPU backend as the compiled-in fallback, plus a real `cudarc`-backed CUDA backend
  (EG-KG.backend.real-cuda-tensor-backend) that NVRTC-compiles the batch-distance/elementwise kernel and launches it on a device, else
  falls back to CPU. `cudarc` is `dynamic-loading`, so the leg builds with no CUDA toolkit and a `pi`
  build links none of it. **EG-KG.compute.tensor-gpu-distance** adds a GPU-gated parity test (compiled under `gpu-cuda`) that asserts
  the real CUDA kernel == the CPU ground truth whenever a device is present and SKIPs cleanly otherwise —
  so the kernels auto-validate on any GPU host (e.g. GB10) without breaking GPU-less CI.

---

## Shipped in Program B

Program B (waves B-1..B-6, `CONCEPT:EG-KG.query.gds-call-procedures..320`) closed the deferred tail. Every item below is now ✅ —
see the [capability matrix](capabilities.md) for the per-operation evidence and [concepts](concepts.md) for
the authoritative `CONCEPT:EG-*` definitions.

### SQL / tables
- **`ALTER TABLE` beyond `ADD COLUMN`** — `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN TYPE`
  (with data migration), `DROP CONSTRAINT` on the durable user-table catalog (EG-KG.query.rename-table-moves-catalog).
- **Real ParadeDB BM25** — real relevance scoring + highlighted snippets behind `@@@` / `paradedb.score()` /
  `snippet()` (EG-311, replacing the placeholder `1.0`).
- **Real pgvector ANN pushdown** — `ORDER BY col <-> $1 LIMIT k` pushes to the eg-ann HNSW/IVF index with an
  exact re-rank (EG-KG.query.real-pgvector-ann-top).

### Lakehouse interop (LTAP)
- **eg-lake LTAP tier** — Parquet-on-object-store materialization + Delta transaction log + Iceberg-REST
  catalog + real Iceberg v2 **Avro** manifest/manifest-list writer (EG-KG.storage.eg-iceberg-avro-manifest/EG-KG.storage.iceberg-manifest-list) + LSN-style as-of snapshots,
  so external lakehouse engines (Databricks/Spark/Trino/DuckDB) read the engine's tables with zero ETL (EG-KG.storage.lsn-as-snapshot-returns).

### Graph / Cypher
- **GDS over Cypher** — `CALL gds.<algo>(…) YIELD …` (PageRank/Louvain/WCC/SCC/betweenness/Dijkstra/similarity)
  over eg-compute (EG-298).

### Vector / ANN
- **HNSW index** — hierarchical-navigable-small-world graph index for higher recall-per-probe than IVF-PQ,
  with insert/search/serde-persist (EG-KG.retrieval.hnsw-vector-index).
- **Cross-shard kNN scatter-gather** — a kNN query scatters across per-shard eg-ann indexes and merges to a
  deterministic global top-k via the `merge_topk` leaf (EG-319, completing EG-KG.retrieval.scatter-gather).

### Broker / streams
- **Exactly-once broker delivery** — idempotent-producer dedup (producer id + sequence) + the stream/confirm/ack
  ops exposed over the AMQP `confirm.select` + MQTT 5 wire frames (EG-314).
- **Live CEP subscription** — register a CEP pattern, subscribe, receive pushed matches fed by the CDC bus
  (EG-KG.query.protocol-types).
- **Redis pub/sub + S3 multipart** — RESP `SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH` + `MULTI`/`EXEC` (EG-KG.ontology.resp2-resp3-codec-round) and S3
  multipart upload + range GET (EG-KG.ontology.object-put-get-head), both EG-307.

### RDF / OBDA
- **ICV write-path enforcement** — a commit guard rejects a transaction that would introduce a SHACL-as-constraint
  violation, configurable enforce/warn (EG-KG.ontology.rdf-update-guard).
- **Full R2RML Turtle parse** — standard R2RML mapping documents drive an OBDA virtual graph (EG-305).

### Observability
- **PromQL extended function set** — `_over_time` family, `delta`/`idelta`/`deriv`, `topk`/`bottomk`/`quantile`,
  `label_replace`/`label_join`, `clamp*` (EG-302).
- **OTel export + Prometheus remote-write + OTLP** — the engine emits its own metrics/traces to an external OTel
  collector + accepts a Prometheus remote-write receiver (EG-316).

### GIS
- **Shapefile / KML / GeoParquet I/O** — ESRI Shapefile, KML/KMZ, and GeoParquet reader/writer round-tripping
  eg-geo geometries + attributes (EG-KG.domains.geo-formats).
- **Routing turn-restrictions + time-windows** — turn-restriction penalties + time-dependent edge weights on the
  EG-KG.domains.geo-routing router (EG-KG.domains.geo-partitioning).

### KV-cache
- **Real KV warm-tier compression** — zstd (optional lz4) replacing the RLE fallback (EG-315).

### Durability / wire surface / scheduling
- **Durable RBAC persistence** — roles/grants + agent identities persist to redb + reload at boot (EG-KG.compute.durable-rbac-identity-persistence).
- **Tensor-op CAS write-back** — derived tensors persist into the content-addressed tensor store on the exec path
  (EG-304).
- **Durable JSONPath index** — the inverted JSONPath index persists to redb + rehydrates at boot + feeds planner
  cost `Stats` (EG-308).
- **Federated typed result fusion** — schema-aware SQL + SPARQL column-union + typed dedup/merge (EG-KG.query.schema-typed-fusion-sql).
- **Memory/scene/trajectory wire-Ops** — the agent-memory + scene-graph + trajectory library APIs exposed over the
  wire as additive `Method`s + dispatch + WAL replay (EG-KG.memory.eg-batch-decay-caller).
- **Real-time QoS/SLO scheduler** — per-tenant/priority admission + deadline scheduling + backpressure (EG-320).

### Analytics program — numeric kernel (one kernel, two surfaces)
- **P1 (this release) — numeric kernel + Python shim** — `crates/eg-numeric` (faer + ndarray, BLAS/LAPACK-free):
  the curated numpy op-surface (reductions/stats, element-wise, the linalg-6 + LinAlgError, random) as a pure Rust
  rlib, exposed on **Surface A** (Python extension `epistemic_graph.numeric`, feature `python`) and consumed by
  `agent_utilities.numeric.xp` (AU-KG.backend.lmcache-native-connector); gated behind a `numeric` cargo feature (in the one main build); parity-proven
  `np.allclose` vs numpy incl. nan/inf/singular/empty edge cases (AU-KG.compute.numeric-kernel). **Done.**
- **P2–P3 — migrate the 598 numpy sites** — swap the 32 light-op files then the 6 linalg files in agent-utilities from
  numpy to the `xp` shim. ✅ **Shipped** (agent-utilities 1.2.0/1.3.0, KG-2.313): 34 files migrated, zero numeric
  regressions; numpy now survives only in the `[test]` extra as the `np.allclose` parity reference.
- **P4 — Surface B engine operators** — DataFusion SQL UDFs/UDAFs over the kernel + kernel-backed batch vector ops.
  **In progress:** `cosine_sim`/`l2_normalize`/`zscore` scalar UDFs + `covariance` UDAF registered on the SQL surface
  (`SELECT zscore(price) FROM …`, `SELECT cosine_sim(a.emb, b.emb) …` run in-engine), plus the `BatchL2Normalize` engine
  Method (EG-KG.query.surface-b-numeric-operators/EG-KG.compute.l2-normalize-batch-vectors), all behind the `numeric` feature (in the one main build). **Also shipped:** the
  `svd(vec_col)`/`pca(vec_col,k)` column→matrix UDAFs (EG-KG.query.svd-eg-pca-column/EG-KG.query.concept-6 — a row-buffering accumulator marshals a column of
  vectors into a dense `ndarray::Array2`, then faer `svdvals`/`eigh`), plus `kmeans(vec_col,k)` (EG-KG.query.kmeans-clustering-half-one — the same
  column→matrix accumulator driving a new pure-Rust `eg-numeric::cluster` Lloyd + k-means++ kernel, **no linfa/BLAS**;
  one `List<Int64>` cluster label per row). **The differentiator — shipped (EG-KG.query.eg-3):** *cross-modal join → analytics
  in-engine* — one SQL statement joins graph ⋈ vector ⋈ timeseries and runs `pca`/`kmeans`/`covariance` over the joined
  result set (**impossible in numpy** — no data layer), E2E-proven in `crates/eg-query/tests/cross_modal_analytics.rs`.
  **Deferred (next P4 increment):** graph-algo/timeseries unification under the kernel via native `Method` surfaces. ▶ ✅
- **P5 — drop numpy/scipy** from agent-utilities; the kernel is the dep, shipped as **`epistemic-graph[numeric]`** (ONE package). 🗺 The publish blocker is **cleared (EG-KG.compute.tensor-gpu-distance)**: `release-build.yml` builds the pyo3 Surface-A kernel and **folds its `.so` into the `epistemic-graph` node wheel** as `epistemic_graph.numeric` (`scripts/inject_numeric_kernel.py`) — no separate `eg-numeric` package on PyPI. So downstreams hard-depend on `epistemic-graph[numeric]` and the numpy drop becomes purely an agent-utilities dependency swap.

See [`architecture/numeric-kernel.md`](architecture/numeric-kernel.md).
### Client drivers
- **Multi-language client drivers (B1.7)** — thin client bindings for the Program-B engine `Method`s that had no
  client surface: the native broker + append-log streams (EG-275..284/314), RBAC admin (EG-KG.compute.feature), online
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
- `ON CONFLICT` upsert + `RETURNING` (EG-KG.query.delete-returning-sees-row); wire transactions `BEGIN`/`COMMIT` mixing graph + user tables
  with RYOW + `TransactionStatus` (EG-KG.compute.kg-transaction-is-pinned); `CREATE VIEW` / `DROP VIEW` durable catalog (EG-072).
- SQL DDL + arbitrary user tables + `COPY`.

### SPARQL — full Stardog/GraphDB parity
- Content negotiation (XML/CSV/TSV/Turtle; EG-KG.ontology.content-negotiation-serializers), rich FILTER (regex/arithmetic/`IN`/builtins; EG-KG.ontology.rich-filter),
  `FROM`/`FROM NAMED` (EG-KG.ontology.from-from-named), sub-SELECT · SERVICE federation · MINUS · negated property sets
  (EG-KG.ontology.sub-select/052/055/056), SPO/POS triple index + selectivity join-ordering (EG-KG.ontology.capability-catalog).
- `ASK`/`CONSTRUCT`/`DESCRIBE` + `UPDATE` + the W3C `/sparql` endpoint; true named-graph quad dataset.

### OWL / RDF validation
- `rdfs:range` in EL completion (EG-KG.ontology.owl-reasoning), OWL-DL tableau (EG-KG.ontology.concept-2), SWRL/RuleML rules (EG-KG.ontology.concept-3).
- **SHACL Core** validation (`eg-shacl`, EG-KG.ontology.concept-6), **ShEx** validation (`eg-shex`, EG-KG.compute.concept-2), and
  **Integrity Constraint Validation** with guard mode (EG-KG.ontology.wired-into-commit-write).

### Graph — full Neo4j parity + Bolt wire
- Cypher `REMOVE` (EG-KG.query.cypher-execution), `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/`DISTINCT` (EG-KG.query.eg-extend-read-side),
  var-length + fixed hops + path binding (EG-KG.query.concept-2); writes (`CREATE`/`MERGE`/`SET`/`DELETE`).
- **Neo4j Bolt v4.4** wire adapter so native drivers connect (EG-KG.query.bolt-wire-protocol).

### GraphQL
- Subscriptions over CDC (EG-064), fragments/variables/directives (EG-KG.query.fragments-variables-directives), Relay pagination (EG-KG.query.graphql-cursors).
- **Apollo Federation v2 subgraph** (EG-295) + **enterprise hardening** (APQ, depth/cost limits,
  introspection toggle; EG-KG.domains.graphql-enterprise-hardening); GraphQL mutations.

### Time-series · Vector · Blob
- `Op::Window` execution (EG-KG.query.streaming-execution), per-point retention trim (EG-KG.temporal.bucket-cutoff-trim),
  hybrid metadata pre-filtering (EG-070), content-defined chunking (EG-KG.storage.backward-manifest-read).
- **Exact/flat vector index + recall@k harness** (EG-KG.query.concept-5).

### Multi-wire adapters (one `WireProtocol` trait)
- The **`WireProtocol`/`WireSession` keystone** with pgwire refactored behind it (EG-KG.compute.subsystems-reference), plus
  **SQLite** (EG-KG.query.concept-3), **MySQL/MariaDB** (EG-KG.query.kg-2), **MSSQL TDS** (EG-KG.query.hand-rolled-tds-server), **Redis RESP** (EG-KG.ontology.resp2-resp3-codec-round), and the
  **S3 REST** surface (EG-KG.ontology.object-put-get-head).

### Message broker (Phase Y)
- The native engine task queue `ClaimNext` (EG-KG.compute.atomically-claim-oldest-pending) grown into a RabbitMQ-class broker: exchanges/routing +
  **AMQP** wire (EG-275), **MQTT** (EG-281) and **STOMP** (EG-KG.ontology.stomp-frame-codec-unit) wires, DLQ (EG-KG.compute.dead-letter-queues), TTL/expiry (EG-KG.compute.message-ttl-expiry),
  priority queues (EG-KG.compute.priority-queues), delayed/scheduled delivery (EG-KG.compute.delayed-scheduled-delivery), consumer groups + prefetch/QoS (EG-KG.compute.groups-qos-prefetch-honoring),
  publisher confirms + consumer acks (EG-KG.compute.publisher-confirms-consumer-qos), and replayable append-log streams (EG-283).

### Observability (Phase T)
- Log ingestion (OTLP/`_bulk`/syslog; AU-KG.ingest.self-ingest), Parquet-on-object-store segments (EG-KG.retrieval.observability-search), log search +
  `_search` API (EG-KG.query.concept-4), distributed traces (EG-163), VRL ingest pipelines (EG-165), **PromQL** query API
  (EG-172), and **super-cluster federated search** (EG-KG.ontology.federation-client).

### GIS (eg-geo)
- Spatial modality (EG-KG.ontology.singles-concept) built to real-GIS depth: full geometry model (EG-KG.domains.geometry-collections), DE-9IM + RCC8/Egenhofer
  (EG-KG.ontology.de-9im-relations/155), constructive algebra (EG-KG.ontology.concept-9), geodesic ops (EG-KG.ontology.concept-8), CRS registry + reprojection
  (EG-KG.domains.coordinate-reference-system/262), durable R-tree (EG-KG.domains.spatial-strtree-index), GeoJSON/WKB/GPX I/O (EG-KG.domains.geojson-gpx-formats), map tiling + MVT (EG-KG.domains.map-tiles),
  raster tile pyramids + hand-rolled PNG (EG-KG.domains.raster-build/339), Shapefile/KML/GeoParquet I/O (EG-KG.domains.geo-formats),
  routing/isochrones/TSP (EG-KG.domains.geo-routing), map-anchored tasks (EG-KG.domains.geo-task), and GeoSPARQL (EG-KG.ontology.concept-10).

### New modality engines + subsystems
- **eg-tensor** N-D array store (EG-085), **eg-stream** windowed CEP (EG-KG.query.pipelined-execution), **eg-kvcache** tiered +
  shared KV-cache with the vLLM/LMCache server endpoint (EG-185/186/187), and the **agent-memory**
  primitives — summary tier (EG-KG.compute.hierarchical-summary-tier-eg), episodic→semantic consolidation (EG-KG.compute.consolidate-cluster), decay/reinforcement
  (EG-222), trajectory memory (EG-099), LeanRAG retrieval (EG-195).

### Unified planner / UQL · Distribution
- `Op::Foreign` name → `ForeignSourceSpec` resolution (EG-073), NL → query dual-mode (EG-078/079/080).
- Multi-Raft groups + `GroupRouter` + online resharding, N-participant cross-shard 2PC with crash recovery,
  parallel-commit + read-only-participant fast path (EG-KG.txn.cross-shard), non-blocking commit (EG-KG.txn.harness-crash).

---

**Reading this as a contributor?** The forward-roadmap table above is the whole of what's left. Each item
lands with tests + a `docs/concepts.md` entry, and flips to ✅ in the [capability matrix](capabilities.md)
as it merges.
