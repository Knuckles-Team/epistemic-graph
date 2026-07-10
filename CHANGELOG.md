# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added — CycloneDDS-C `rmw` ROS2 leg (S5, follow-on to EG-347/EG-349)
- **`ros2-rmw` feature** — a THIRD `DdsTransport` impl (`CycloneDdsTransport`,
  `src/server/dds.rs`), alongside the WS bridge (`ros2-bridge`) and the pure-Rust
  `rustdds` leg (`ros2-dds`). Links the REAL `rmw_cyclonedds`/CycloneDDS-C stack via the
  safe `cyclonedds` Rust crate (`cyclonedds` → `cyclonedds-rust-sys` → `cyclonedds-src`,
  which vendors the CycloneDDS C sources IN the crate — no network fetch at build time;
  `cmake`-built static lib + prebuilt bindgen output, no libclang needed). Genuine
  zero-config live-`ros2` interop: a real `ros2` node discovers/pubs/subs with no bridge.
- Reuses the SAME `mangle_topic_name`/`mangle_type_name` rmw mangling as the `ros2-dds`
  leg (no forked shaping) and the SAME `std_msgs/String` CDC↔ROS2 payload convention. A
  hand-written `DdsType` impl for the single-string sample type (no IDL compiler needed)
  with a `clone_out` override to avoid a double-free across the DDS loan boundary.
  Exercised by a real RTPS loopback pub/sub test over the CycloneDDS-C wire
  (`eg347_cyclone_dds_loopback_pub_sub_roundtrip`), mirroring the `ros2-dds` leg's test.
- Toolchain-gated (needs `cc`/`cmake`) + heavy — kept OUT of `pi`/`default`/`node`/`full`,
  folded ONLY into the opt-in `full-extras` bundle alongside `ros2-dds`/`gpu-cuda`
  (a `pi`/`full` build links no cyclonedds/cyclonedds-rust-sys/cyclonedds-src, asserted
  by `cargo tree`).

## [2.11.0] - 2026-07-04

> **Minor, additive.** Closes **handoff-1** in full — the cross-modal query engine gains a
> cost-based optimizer, a single physical/parallel runtime, unified materialized views with
> federation and an RLS-aware result cache, incremental write-path index coherence, and a
> query test harness (bench/recall gate + fuzz/chaos). Five end-to-end use-case validation
> suites and the advanced cross-modal correctness specs are green; the served-query surface is
> completeness-closed; and the warm-fork cross-modal fan-out lands (in agent-utilities). No
> wire-shape change; new capabilities are feature-gated (`cost-opt` folded into `full`,
> `par-runtime` opt-in, `matview` onto `cluster`).

### Added — cross-modal cost optimizer (track A)
- **Cross-modal cost optimizer** (`eg-plan/optimizer.rs`) — cardinality + `CostEstimate`
  estimators over the Lane-0 exec-dispatch seam, driving `plan_optimize`; routes cross-modal
  plans (graph / vector / text / timeseries / SQL) by estimated cost. Gated by the `cost-opt`
  feature, folded into `full`.

### Added — single physical / parallel runtime (track B)
- **Physical runtime** (`eg-plan/runtime.rs`) — one pluggable `ParallelDriver`
  (rayon morsel-parallel with spill) behind the Lane-0 `Driver` seam, plus `Window` /
  `WindowAgg` execution arms. Opt-in via the `par-runtime` feature.

### Added — unified materialized views + federation + RLS-aware result cache (track C)
- **Materialized-view subsystem** — plan-backed matviews with CDC invalidation, a symmetric
  `ForeignScan` federation path, and an **RLS-scoped result cache** (row-level-security-aware,
  so cache hits never leak across security scopes). Persisted via `plan_matviews` redb rows.
  Gated by the `matview` feature (onto `cluster`).

### Added — incremental write-path index coherence (track D, incl. D3)
- **IndexManager write seam** — change-set–driven incremental maintenance of ANN, text,
  temporal, and derived-OWL indexes on the write path, with a `write_coalescer` and index
  state persisted to redb rows. **D3** completes the text / temporal / derived-OWL incremental
  maintenance behind the D1 seam, so all four index families stay coherent without full rebuilds.

### Added — query test harness: bench / recall gate + fuzz / chaos (track E, EG-420..426)
- **Criterion benchmarks + recall gate** and a **query fuzzing / chaos harness**, wired into a
  CI bench job. Includes **EG-426** — a libFuzzer target for the UQL parser.

### Added — 5 use-case validation suites (track G, EG-434..EG-440)
- End-to-end validation suites covering hybrid RAG + analytics (EG-436), observability RCA
  (EG-437, fused dependency/vector/TSDB root-cause + native ts-scan), KG lifecycle
  (validate → commit → infer → reindex under concurrency, EG-438), and related flows —
  exercising the full cross-modal seam at the engine surface.

### Fixed / Closed — advanced cross-modal specs (track F, EG-427..433)
- Advanced cross-modal correctness specs brought green, plus a **post-overlay RLS filter seam**
  that fixes a row-level-security leak in the overlaid result path.

### Closed — advanced specs: cross-shard 2PC + fuzz (cluster-fuzz)
- **EG-396** — cross-shard two-phase-commit **coordinator-kill** harness un-ignored and green
  under the `cluster` feature (both the raft-level `cross_shard_raft_2pc_single_decision_eg396`
  and the cross-shard modality harness variant).
- **EG-426** — libFuzzer UQL-parser fuzz target (see track E).

### Closed — served-query completeness
- **EG-439** — decay factored into the plan (decay-in-plan) rather than only post-hoc.
- **EG-440** — served `TextIndex` (the served surface now serves the real text index).
- Optimizer routing surfaced on the served path.
- **EG-405** — intentional-semantics determination documented (behaviour is by-design, not a gap).

### Added — warm-fork cross-modal fan-out (agent-utilities, ORCH-1.106)
- Warm-fork cross-modal fan-out lands in **agent-utilities** (`feat/warmfork-crossmodal`,
  ORCH-1.106); the north_star row is flipped to *implemented*.

> **Documented remainders (not regressions):** cross-**node** Raft soak validation needs real
> multi-host hardware; per-graph Tantivy heap tuning is a follow-up knob; and **EG-405**
> intentional-semantics is by-design.

## [2.10.0] - 2026-07-04

> **Minor, additive.** Closes the cross-modal seam backlog `docs/north_star.md` tracked: the
> in-transaction cross-modal seam is now proven at EVERY SQL wire (per-wire roundtrip tests),
> reaches the GraphQL surface with a DURABLE atomic commit, and the advanced cross-modal
> correctness set + a query-surface differential test harness are in the tree — plus four UQL
> front-end fold-ins that close builder-vs-UQL expressiveness asymmetries. No wire-shape change.

### Added — per-wire cross-modal roundtrip tests
- **EG-377 / EG-378** — executable mysql-wire (`tests/mysql_roundtrip.rs`, hand-rolled
  Handshake-v10 / `COM_QUERY` client) and mssql-wire TDS (`tests/mssql_roundtrip.rs`,
  hand-rolled `SQLBatch` client) cross-modal roundtrip tests: an in-txn `UPDATE` + `SET
  EMBEDDING` read back by an in-txn `UQL` (RYOW), and `BEGIN; SET EMBEDDING; INSERT INTO
  series; <UQL join>; COMMIT` with off-txn isolation until COMMIT. Proves both wires inherit
  the shared EG-074/EG-372 seam — test-only, no src change.

### Added — GraphQL cross-modal transaction seam (durable)
- **EG-379..383** — the GraphQL surface gains the in-txn cross-modal verbs
  (`beginTransaction` / `stageEmbedding` / `addMeasurement` / `sparqlUpdate` /
  `sparqlConstruct` / in-txn `unifiedQuery` / `commitTransaction` / `rollbackTransaction`) via
  a multi-request `txnId` handle in an `eg_graphql::CrossModalTxnRegistry`. `eg-graphql` sits
  below the facade in the crate DAG, so it routes onto the SAME lower primitives the facade's
  `GraphTxnState` / `run_unified_overlaid` are built on — no plan/lowering duplicated.
- **EG-419** — GraphQL cross-modal **durable** commit: the facade GraphQL carrier
  (`handlers::query.rs` `Method::GraphQl` → `handlers::txn::commit_graphql_cross_modal`) routes
  a `commitTransaction` into a facade `GraphTxnState` and lands ALL modalities (graph + vector
  + tsdb measurements) in ONE redb `WriteTransaction` via `commit_cross_modal_txn` — exactly as
  pgwire's `commit_txn_state`, replacing the crate's in-memory-only tier. The facade `full` tier
  enables `eg-graphql/crossmodal-tsdb` so `addMeasurement` rides `full`. Proven durable (survives
  a persist-dir reopen) by `tests/graphql_crossmodal_durable.rs`.

### Added — advanced cross-modal correctness + query test harness
- **EG-384..398** — 14 advanced cross-modal seam tests (`crates/eg-plan/src/advanced_crossmodal_tests.rs`
  + `tests/advanced_crossmodal_roundtrip.rs`): 8 green (bitemporal AsOf×vector×OWL×graph,
  federation fusion, geo×vector×temporal, tensor×graph×vector + CAS dedup, CEP×graph×tsdb,
  probabilistic×OWL×vector + MMR, the 5-modality in-txn RYOW capstone, concurrent serializable
  phantom) + 6 `#[ignore]`d as explicit north_star open rows (RLS, multi-listener snapshot,
  encryption wrong-key, CDC matview, cross-shard Raft 2PC, KV warm-fork). Includes **EG-398** —
  an RPC-routing leak fix: `TxnAddMeasurement`/`TxnAxiom`/`TxnConstruct` staging handlers existed
  but `dispatch.rs` never routed them (worked over pgwire, errored over native RPC); three
  cfg-gated dispatch arms close the leak.
- **EG-400..406** — query-surface test harness proving modality interchangeability: a differential
  multi-surface oracle (`differential_oracle.rs`), a cross-modal composition matrix
  (`composition_matrix.rs`), property-based proofs (`plan_proptest.rs`, proptest), and plan
  snapshots (`plan_snapshots.rs`, insta). It surfaced two documented open findings — **EG-404**
  (UQL `RANK` rejects negative vector components) and **EG-405** (op composition is not freely
  commutative under empty-intermediate reseed).

### Added — UQL cross-modal fold-ins
- **EG-411 / EG-412** — `RANK BY ~ "text"` server-side NL→vector: a quoted rank ref lowers to
  `Op::RankEmbed` resolved by a `TextEmbedder` bound on `PlanCtx` (facade injection point in
  `run_unified`); unbound ⇒ a clean typed error.
- **EG-413 / EG-414** — real tumbling `WINDOW` time-series aggregate: `WINDOW <secs>` /
  `WINDOW <secs> <agg>` (`Op::WindowAgg`) consumes `(ts,value)` rows and emits one row per
  bucket via eg-tsdb `time_bucket` (closes EG-404's sibling — the TsScan→Window gap).
- **EG-417** — negative vector components in UQL `RANK BY ~[-0.1, …]` (closes the lexer/parser
  asymmetry EG-404 flagged; the builder/wire always accepted them).
- **EG-418** — UQL `FUSE` stage dispatch to the same `Op::FuseRrf` the builder/wire construct
  (RRF was builder/wire-only though the grammar listed it).

## [2.9.0] - 2026-07-03

> **Minor, additive.** Extends the in-transaction cross-modal seam onto the SQL wire
> family: a single psql `BEGIN…COMMIT` can now stage and atomically commit ALL modalities
> and read its own uncommitted writes across them — routed onto the SAME committed RPC seam
> the native transport uses (no re-implementation). Plus the North Star "Seamless" doc that
> makes the seam-at-every-surface discipline explicit and tracks the remaining open sub-seams.

### Added — pgwire / mysql / mssql cross-modal transaction seam
- **EG-372** — pgwire (+ inherited mysql-wire / mssql-wire) in-transaction cross-modal
  read-your-own-writes over the wire: inside a psql `BEGIN…COMMIT`, `UQL …`,
  `SET EMBEDDING FOR …`, `INSERT INTO series …`, `SPARQL UPDATE …`, and `SPARQL CONSTRUCT …`
  stage into the txn's write-set and read their own uncommitted writes across modalities,
  committing atomically. Each SQL-wire statement is a thin parser/router onto the existing
  committed RPC seam (`WireSession` → the EG-359..363 machinery) — never a re-implementation.
  The two previously-`#[ignore]`d pgwire specs are un-ignored + green, plus a new in-txn
  cross-modal isolation test. mysql-wire / mssql-wire inherit the verbs structurally via the
  shared EG-074 core (per-wire executable roundtrip tests still pending — see north_star.md).

### Added — North Star
- **EG-373** — `docs/north_star.md` "Seamless" goal doc: every cross-modal seam must be fully
  implemented at EVERY surface (RPC, SQL wire family, SPARQL, GraphQL), never merely flagged;
  a seam that works over RPC but errors over psql is a leak. Includes the seam backlog table
  tracking the remaining open sub-seams as explicit concept-owned rows rather than buried TODOs:
  REASON-by-IRI mid-plan, the string→IRI class bridge, an in-txn tsdb read-your-own-writes
  overlay, per-wire mysql/mssql roundtrip tests, and GraphQL cross-modal.

## [2.8.0] - 2026-07-03

> **Minor, additive.** Closes the in-transaction cross-modal seam so a single ACID
> transaction can stage and atomically commit ALL modalities (graph + vector + time-series
> + OWL + SPARQL CONSTRUCT), read its own uncommitted writes across modalities, and fuse a
> time-series leg into a unified plan — plus a KV-cache data-version invalidation and the
> collapse of the pi/pi-max/node deployment tiers into ONE full-featured build.

### Added — in-transaction cross-modal seam
- **EG-359** — in-txn cross-modal read-your-own-writes: `Method::TxnUnifiedQuery{,Text}` run
  the SAME `wire::Plan`/UQL over the committed snapshot OVERLAID with the txn's staged
  write-set, so a staged node/edge/embedding is visible to THIS txn pre-commit and invisible
  off-txn until commit. Client: `client.txn.unified_query` / `unified_query_plan`.
- **EG-360/361/362** — five-modality atomic staging: `TxnAddMeasurement` (time-series),
  `TxnAxiom` (OWL Turtle), and `TxnConstruct` (SPARQL CONSTRUCT) stage into the SAME redb
  `WriteTransaction` as the txn's graph/vector/blob writes, so all modalities land atomically
  at commit or none do. Client: `client.txn.add_measurement` / `axiom` / `construct`.
- **EG-363** — tsdb-in-plan + planner reason-mid-pipeline: `wire::Op::TsScan{series,from,to}`
  seeds a plan's RowSet from the native eg-tsdb `SeriesStore` (threaded via `PlanCtx::with_tsdb`)
  and mid-pipeline `Op::Reason` composes so `Scan→Rank→Reason→Traverse` / `TsScan→Rank→Limit`
  fuse the time-series and OWL legs with the graph/vector/relational legs in ONE plan. The
  committed unified-query path now attaches the tsdb store for time-series fusion.

### Added — KV-cache data-version invalidation
- **EG-364** — `DataVersion{Agnostic,At(u64)}` stamps each shared/tiered KV-cache entry with
  the `GraphCore::version()` epoch it was derived at (mirrors the version-keyed result cache);
  a graph write retires stale entries so no stale agent/LLM context is served, while pure
  content-addressed (`Agnostic`) KV pages are never invalidated. HTTP surface wires
  `X-EG-Data-Version` + `PUT|GET /kv/version/<n>`.

### Added — cross-modal seam regression suite
- **EG-365..370** — planner mid-pipeline composition proofs, bitemporal decay×AsOf, vector⇄
  reasoning cross-txn consistency, SPARQL-UPDATE→reasoning visibility, SQL result-cache
  coherence, and a served E2E cross-modal seam.

### Changed — one full-featured build (tier collapse)
- **EG-371** — the compiled pi/pi-max/node deployment tiers are collapsed into ONE main build:
  `default == full` (`cargo build` links every MAIN feature that compiles without an external
  GPU/robotics toolchain), removing the shared-wheel-filename collision (one build ⇒ one wheel
  per platform). `cluster` (openraft HA) and `full-extras` (GPU/ROS2) remain opt-in build
  layers, NOT published wheels. Preserved invariants: no pyo3 (`bindings=bin`), no openraft in
  default, no cudarc/rustdds in default. The release wheel matrix collapses to one wheel per
  platform while retaining the org-standard `DOCKER_REGISTRY/USERNAME/PASSWORD` image auth.

## [2.7.0] - 2026-07-03

> **Minor, additive.** Completes the `eg-numeric` Surface-A kernel so `agent-utilities` can drop
> its numpy/scipy dependency on the hot path, and fixes the published wheel so the injected kernel
> binary stays executable/importable.

### Added — numeric kernel surface (Surface-A completion)
- **EG-354** — array constructors + dtype/module-attr surface re-exported from
  `epistemic_graph.numeric`: `array`/`asarray`/`zeros`/`ones`/`empty`/`full`/`arange`/`linspace`/
  `eye`/`diag`/`fill_diagonal`/`diff`/`sort`/`concatenate`/`reshape`/`vstack`/`stack` plus
  `float64`/`float32`/`int64`/`ndarray`/`newaxis`/`pi`/`inf`/`nan`. numpy is the engine-private
  container substrate; the `agent_utilities` `xp` shim imports containers from the kernel, not numpy.
- **EG-355** — axis/keepdims/integer-array reductions: `sum`/`prod`/`mean`/`std`/`var`/`amin`/
  `amax`/`argmin`/`argmax` now accept `axis` (`int|None`) + `keepdims` and coerce `int64`/`int32`/
  `float32` input, closing the bare-1D-float64 gap so e.g. `np.mean(m, axis=1)` routes through the kernel.
- **EG-356** — `eigsh_smallest`: partial symmetric eigensolver returning the k smallest-magnitude
  eigenpairs of a dense Laplacian (faer selfadjoint eig + k-select), matching
  `scipy.sparse.linalg.eigsh(L, k, which="SM")`; used by `spectral_navigator`.
- **EG-357** — `spearmanr` (rank-transform + Pearson + Student-t two-sided p) and `ks_2samp`
  (two-sample Kolmogorov-Smirnov D + asymptotic p via the Kolmogorov Q series) — the `scipy.stats`
  ops `agent-utilities` needs (analytics feature; `statrs` for the t p-value).
- **EG-358** — normal-distribution `ppf` (inverse CDF) + `pdf` via `statrs` (analytics feature, out of
  the pi/default tier) — replaces `scipy.stats.norm.ppf`/`pdf`.

numpy/scipy parity verified (13/13).

### Fixed
- **EG-346** — the numeric-kernel wheel-packaging step (`scripts/inject_numeric_kernel.py`) now
  preserves the zip `external_attr` unix mode when injecting the kernel `.so` into the node wheel, so
  the server binary stays `0755` (executable) and the published wheel is installable/importable. The
  release workflow's strict kernel smoke test was restored, running under `$RUNNER_TEMP` to avoid
  source-directory shadowing of the installed package.

## [2.6.0] - 2026-07-03

> **Minor, additive.** Reclaims two orphaned-branch capabilities as finished, validated code, and
> makes the release image build in seconds instead of ~25 min/arch.

### Added
- **KG-2.132** — hybrid `Discover` engine op: dense HNSW retrieve + lexical keyword re-rank + text
  hydration (`{id,name,description,type,score}`) in one round-trip, complementing `SemanticSearch`.
- **EG-352** — release Docker image now `uv pip install`s the published `node`-tier wheel (no in-image
  cargo compile; multi-arch clean) and **pgwire** is folded into the `node`/`full` tiers so the node
  wheel is a complete Postgres-wire single-node DB. Pi contract preserved (pgwire never enters pi/pi-max).

### Fixed
- **CI publish** — eg-numeric aarch64 pyo3 cross-build made best-effort so it can't block `publish-pypi`
  (x86_64 wheel + sdist still ship; arm builds from sdist).

## [2.5.0] - 2026-07-03

> **Minor, additive.** Closes the remaining *deferred/follow-on* items from the 2.4.0 roadmap tail,
> publishes the `eg-numeric` kernel wheel to PyPI (unblocking a numpy-free agent-utilities), and fixes
> the release pipeline so the PyPI publish no longer stalls on best-effort macOS/windows runners.

### Fixed
- **CI publish gating** — dropped the best-effort macOS/windows wheel legs from the gating `wheels`
  matrix so `publish-pypi` fires on the always-available `ubuntu-latest` legs (they no longer block the
  release when a macOS runner is unavailable). macOS/Windows users install the sdist.

### Added — deferred follow-ons closed
- **EG-348** — Calvin OLLP recon-staleness **restart**: a txn whose reconnaissance-predicted read/write
  set changes before lock acquisition is deterministically re-sequenced + re-reconnoitred and re-run,
  closing the last EG-342 gap (bounded retry; all nodes make the same restart decision).
- **EG-349** — ROS2 **rmw topic/type name mangling** (`rt/<topic>`, `<pkg>::msg::dds_::<Msg>_`, CDR-LE
  encapsulation) so the EG-347 native DDS leg is discoverable by a live `ros2` daemon.
- **EG-350** — Iceberg **per-column stats** (the deferred EG-333 follow-on): value/null/nan counts, typed
  min/max bounds and Parquet column sizes in the Avro manifest, for external predicate pushdown / file skipping.
- **EG-351** — **numeric kernel folded into the `epistemic-graph` wheel** (`epistemic-graph[numeric]` — ONE
  published package, the kernel `.so` injected into the node wheel as `epistemic_graph.numeric`; the separate
  `eg-numeric` publish leg is removed, unblocking a numpy-free agent-utilities) + a **GPU-gated CUDA parity test**
  that asserts the cudarc kernels == the CPU ground truth where a GPU is present and skips cleanly in CI.

### Added — lakehouse interop
- **EG-350** — Iceberg **per-column stats** (the deferred EG-333 follow-on). The LTAP tier gathers per-column `value_count`/`null_count`/`nan_count`, typed min/max bounds and Parquet `column_size` as each data file is materialized (`materialize_with_column_stats`), carries them on `FileEntry.column_stats`, and the Avro manifest writer emits the six spec stats maps (`column_sizes`/`value_counts`/`null_value_counts`/`nan_value_counts`/`lower_bounds`/`upper_bounds`, keyed by field-id, bounds in single-value binary) so external readers (Spark/Trino/DuckDB) do predicate pushdown / file skipping. Partition `field_summary` stays null by design — the spec is unpartitioned. Same `lake` gate; `pi`/`full` still link no `apache-avro` (`cargo tree` = 0).

## [2.4.0] - 2026-07-03

> **Minor, additive.** Closes the analytics-kernel axis and the remaining "master of all databases"
> roadmap tail. All new capability is feature-gated per the Pi-contract (a `pi`/`full` build links no
> new heavy dep — asserted by `cargo tree`). Also fixes two test-isolation bugs that were red/hanging CI.

### Fixed
- **Blob CAS test deadlock** — a manifest test issued a raw `begin_write` while the group-commit batch
  txn was open (redb single-writer), hanging `Test (facade full)` for hours. Flush-first, as the API does.
- **Process-RSS memory tests** — `bounded_memory_*` assert whole-process RSS; moved to `#[ignore]` +
  a serial isolated CI step so the parallel harness no longer pollutes the measurement.

### Added — analytics (Surface B, in-DB numeric)
- **EG-329/330** — Surface-B numeric SQL UDFs (`cosine_sim`/`l2_normalize`/`zscore`/`covariance`) + in-engine `BatchL2Normalize`.
- **EG-335/336** — `pca`/`svd` column→matrix UDAFs (faer-backed).
- **EG-344/345** — `kmeans` UDAF + **cross-modal join→analytics in-engine** (graph⋈vector⋈timeseries → pca/kmeans/covariance; impossible in numpy — no data layer).
- **EG-346** — `eg-numeric` pyo3 Surface-A wheel + a `numeric-parity` CI gate asserting the compiled kernel == numpy (np.allclose) op-for-op.

### Added — database parity tail
- **EG-331/332** — on-disk `sqlite3` `.db` file import/export (gated `sqlite-file`, out of `pi`/`default`).
- **EG-333/334** — real Iceberg v2 Avro manifest + manifest-list writer (LTAP tier; pure-Rust `apache-avro`).
- **EG-337** — Python LMCache/vLLM remote-backend driver for the EG-187 KV-cache endpoint.
- **EG-338/339** — raster tile pyramids (hand-rolled zero-dep PNG codec; Web-Mercator XYZ).
- **EG-340/341** — PL/pgSQL procedural interpreter (`DECLARE`/`IF`/`LOOP`/`WHILE`/`FOR`/`RETURN`/`RAISE`/`SELECT INTO`).
- **EG-342/343** — Calvin OLLP reconnaissance + `GlobalSeq`-ordered read-lock manager (serializable) + multi-node sequencer epoch fan-in (raft tier).
- **EG-347** — native DDS/RTPS ROS2 transport seam via pure-Rust `rustdds` (real RTPS loopback; `ros2-dds` in `full-extras` only).

## [Unreleased — Program B]

> **Minor, additive, backward-compatible.** Program B (waves B-1..B-6) turns the previously-deferred
> roadmap tail from stubs into real implementations and pushes a fresh batch of "master of all databases"
> depth: an **LTAP lakehouse-interop tier** (Databricks-interoperable), real pgvector ANN pushdown,
> GDS-over-Cypher, real ParadeDB BM25 ranking, exactly-once broker delivery, OTel/remote-write egress,
> a QoS/SLO scheduler, durable RBAC/JSONPath, `ALTER TABLE`, R2RML, Shapefile/KML, routing
> turn-restrictions, an HNSW index, real KV-cache compression, the memory/scene/trajectory wire surface,
> and cross-shard kNN. Every feature stays behind its own Cargo feature + opt-in listener/env; a
> default/`pi`/`node`/`full` build that sets no new address is byte-for-byte the prior engine. All feature
> work is merged + tested on `feat/program-b`; all pre-commit gates green. See
> [`docs/roadmap.md`](docs/roadmap.md#shipped-in-program-b) for the per-item status and
> [`docs/concepts.md`](docs/concepts.md) for the authoritative `CONCEPT:EG-*` definitions.

### Added

- **LTAP lakehouse interop (`CONCEPT:EG-317`)** — a new async columnar-materialization crate **`eg-lake`**
  transcodes engine table/columnar data → **Parquet-on-object-store** with **Delta** + **Iceberg**
  transaction logs, an **Iceberg-REST catalog**, and **LSN-style as-of snapshots** (reusing the versioned
  snapshots + `Op::AsOf`), so external lakehouse engines (Databricks / Spark / Trino / DuckDB) read our
  tables with **zero ETL** — making epistemic-graph an **LTAP** (lakehouse-transactional-analytical) superset.
  `arrow`/`parquet` + delta/iceberg deps behind a `lake` feature; out of `pi`. (The Iceberg Avro **manifest**
  writer is still a stub — the Delta path + Iceberg-REST catalog are the complete surfaces; see
  [`docs/architecture/lakehouse-ltap.md`](docs/architecture/lakehouse-ltap.md).)
- **Real pgvector ANN pushdown (`CONCEPT:EG-313`)** — `ORDER BY col <-> $1 LIMIT k` now pushes down to the
  eg-ann **HNSW/IVF** index for a real top-k (with an exact re-rank tier) whenever a matching
  `CREATE INDEX … USING hnsw/ivfflat` (EG-116) exists, replacing the brute-force fallback (which stays as the
  no-index path). eg-query/pgwire + eg-ann.
- **HNSW vector index (`CONCEPT:EG-301`)** — a hierarchical-navigable-small-world graph index in eg-ann for
  higher recall-per-probe than IVF-PQ, with insert/search/serde-persist, tuned by the EG-297 recall harness.
- **Cross-shard kNN scatter-gather (`CONCEPT:EG-319`)** — a vector kNN query scatters across per-shard
  eg-ann indexes and merges to a deterministic global top-k via the existing `merge_topk` leaf (EG-069),
  for cluster-wide vector search. eg-ann + server (raft/router).
- **GDS over Cypher `CALL gds.*` (`CONCEPT:EG-298`)** — the EG-144 graph-data-science library
  (PageRank/Louvain/WCC/SCC/betweenness/Dijkstra/similarity) is wired into the Cypher surface as
  `CALL gds.<algo>(…) YIELD …`, projecting the current graph into the eg-compute adjacency and streaming
  results as Cypher rows. eg-query/cypher.
- **Real ParadeDB BM25 ranking + snippets (`CONCEPT:EG-311`)** — real BM25 relevance scoring + highlighted
  snippets via the eg-text index behind the EG-119 `@@@` / `paradedb.score()` / `paradedb.snippet()` surface
  (previously a placeholder `1.0`). eg-text (+ minimal eg-query lowering).
- **`ALTER TABLE` beyond `ADD COLUMN` (`CONCEPT:EG-310`)** — `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`,
  `ALTER COLUMN … TYPE` (with data migration), and `DROP CONSTRAINT` on the durable user-table catalog
  (EG-018). eg-query/tables.
- **Live CEP standing-query subscription (`CONCEPT:EG-299`)** — a server surface (Method + subscription
  stream) over the EG-088 live-CEP standing-query engine: register a CEP pattern, subscribe, and receive
  pushed matches fed by the EG-064 CDC broadcast bus. server + eg-stream.
- **Broker exactly-once + AMQP/MQTT frame exposure (`CONCEPT:EG-314`)** — idempotent-producer dedup
  (producer id + sequence → drop duplicate publishes) for effectively-exactly-once delivery, plus the
  EG-283/284 stream/confirm/ack ops exposed over the **AMQP** `confirm.select` and **MQTT 5** wire frames
  (previously reachable only via engine Methods). eg-core/broker + amqp_wire/mqtt_wire.
- **Redis pub/sub + S3 multipart completeness (`CONCEPT:EG-307`)** — Redis
  `SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`/`UNSUBSCRIBE` pub-sub + `MULTI`/`EXEC` transactions on the RESP wire
  (EG-174), and S3 **multipart upload** (Create/Upload-Part/Complete/Abort) + **range GET** on the S3
  surface (EG-176). server/redis_wire + server/s3.
- **ICV write-path enforcement (`CONCEPT:EG-300`)** — EG-146 integrity-constraint-validation wired into the
  commit/write path: a guard evaluates the proposed change set against registered SHACL-as-constraints and
  **rejects** a transaction that would introduce a violation (constraint-enforced transactions), configurable
  enforce/warn. eg-rdf/eg-shacl + commit hook.
- **OBDA full R2RML Turtle parse (`CONCEPT:EG-305`)** — standard R2RML mapping documents in Turtle
  (`rr:TriplesMap`/`rr:logicalTable`/`rr:subjectMap`/`rr:predicateObjectMap`/`rr:template`/`rr:column`) parse
  into the EG-101 VirtualGraph model, so a real R2RML file drives an OBDA virtual graph. eg-rdf.
- **Geospatial format I/O: Shapefile/KML/GeoParquet (`CONCEPT:EG-306`)** — a reader/writer for ESRI
  **Shapefile** (.shp/.dbf/.shx), **KML/KMZ**, and **GeoParquet**, round-tripping eg-geo geometries +
  attributes and completing the map-data ingest/export matrix alongside GeoJSON/WKB/GPX (EG-264). eg-geo.
- **Routing turn-restrictions + time-windows (`CONCEPT:EG-312`)** — EG-266 routing extended with
  turn-restriction penalties (via edge/turn cost) and **time-window / time-dependent** edge weights (cost as
  a function of departure time) for realistic logistics routing. eg-geo.
- **PromQL extended function set (`CONCEPT:EG-302`)** — the EG-172 PromQL evaluator gains the `_over_time`
  family (sum/avg/min/max/count/stddev/quantile), `delta`/`idelta`/`deriv`, `topk`/`bottomk`/`quantile`,
  `label_replace`/`label_join`, and `clamp*`. eg-tsdb.
- **OTel export + Prometheus remote-write + OTLP (`CONCEPT:EG-316`)** — the engine exports its **own**
  metrics/traces to an external OTel collector (OTLP push) and accepts a **Prometheus remote-write**
  receiver, closing the observability loop (the engine ingests **and** emits). Adds a protobuf (`prost`) dep
  behind an `otel-export` feature; out of `pi`. server/obs.
- **Real KV warm-tier compression (`CONCEPT:EG-315`)** — the eg-kvcache warm tier's RLE fallback is replaced
  with real **zstd** (optional lz4) compression behind a feature, for effective RAM offload of KV blocks.
  eg-kvcache; out of `pi`.
- **Memory/scene/trajectory wire-Op + MCP surface (`CONCEPT:EG-318`)** — the eg-core agent-memory +
  scene-graph + trajectory library APIs (EG-087/099/220/221/222) are exposed over the wire as additive
  `Method`s (CreateSummary/Consolidate/Maintain/SceneObject/Trajectory ops) + dispatch handlers + WAL replay,
  so AU/MCP can drive them remotely (previously in-process only). eg-core + protocol/dispatch.
- **Real-time QoS/SLO scheduler (`CONCEPT:EG-320`)** — a QoS/SLO-aware request scheduler in the
  server/transport: per-tenant/priority admission + deadline scheduling + backpressure so latency-critical
  requests meet SLOs under load. server.
- **Durable persistence hardening (`CONCEPT:EG-303`/`304`/`308`/`309`)** — RBAC roles/grants + agent
  identities persist to redb and reload at boot (EG-303, previously in-memory); derived tensors from
  `Op::TensorOp`/`Op::TensorScan` write **back** into the EG-085 content-addressed tensor store on the exec
  path (EG-304, durable + dedup-shared); the EG-084 inverted JSONPath index persists to redb + rehydrates at
  boot and feeds planner cost `Stats` (EG-308); and EG-243 federated search gains **typed** SQL + SPARQL
  result fusion (schema-aware column union + typed dedup/merge, not just hashed-key union) (EG-309).

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
