# M3 — Catalog-driven resharding (current-main status handoff)

> **Evidence status:** source paths are labelled `IMPLEMENTED` when present on
> current `main`; focused fixtures are labelled `UNIT-PROVEN`; bounded throwaway
> runs are `LAB-PROVEN`; deployed observations are `LIVE`; and exact one-million
> workload reports are `1M-CERTIFIED`. This handoff contains no `LIVE` or
> `1M-CERTIFIED` claim. Historical branch names below identify provenance only.

> **Scope.** M3 is the "horizontal scale spine" wave addressing two **P0** gaps —
> "Elastic sharding / resharding-rebalancing with live data migration" and
> "Scalable tenant catalog". These gaps were originally scoped in a
> `epistemic-graph-master-engine-gaps-2026-06-23.md` planning document; that
> document could not be located in this repo's history or in any known
> workspace archive as of 2026-08-25 (an audit found several sibling
> `reports/*.md` citations from the same era that DO survive under
> `plans/_archive/` — this one apparently did not). Treat the gap scope above
> as the operative statement of intent; this document reconciles current
> source paths, focused repository evidence, and the remaining cross-node/object-tier work.
>
> **Concept IDs:** `CONCEPT:EG-KG.sharding.atomic-shard-swap` (offline K-shard migration tool), `CONCEPT:EG-KG.sharding.empty-catalog-routing`
> (tenant catalog routing-override seam), `CONCEPT:EG-KG.backend.catalog-shard-resolve` (online single-node resharding,
> R1), `CONCEPT:EG-KG.sharding.r5-feature` (catalog auto-attach gate, R5), `CONCEPT:EG-KG.sharding.eg-r6` (cold-tenant
> whole-graph offload, R6), `CONCEPT:EG-KG.sharding.even-load-rebalance` (R3 rebalancing planner), `CONCEPT:EG-KG.sharding.m3-r4`
> (R4 in-process BLOB streaming facade). Registered in [`docs/concepts.md`](../concepts.md).

> **Current-main reconciliation — `IMPLEMENTED` / `UNIT-PROVEN` where named.**
> `online_reshard.rs`, the catalog gate, `cold_offload.rs`, `rebalance.rs`, and
> `blob/stream.rs` are present on current `main`; the focused tests named in the
> sections below are the repository evidence. Historical feature branches are
> retained only as provenance.
>
> **Current-main finishing wires — `IMPLEMENTED` / `UNIT-PROVEN` where named.**
> Parallel cross-shard read fan-out
> (`AU-KG.backend.roadmap-f-parallel-cross` — `load_into` off concurrent `begin_read()` snapshots, off the writer), the M3
> admin RPC (`EG-038` — `Reshard`/`Catalog*`/`RebalancePlan`/`RebalanceExecute` +
> `handlers/admin.rs` + `ReshardingClient`), rebalance plan EXECUTION (`EG-KG.backend.r3-plan-execution`
> `RedbBackend::rebalance_execute`), the R6 `touch()` wiring + interval offload sweep
> (`EG-KG.backend.r6-feature`), and the R1 snapshot+delta copy (`EG-KG.backend.flush-pending-first` `bulk_copy`/`delta_flip_purge`). These are current source paths; focused tests are repository evidence, not a deployment or scale certification. **Still
> REMAINING:** R2 (cross-node, needs M2), and the original R4-gated object-store arm of R6
> (cold tenants colder than redb spilled to `cold-tier-s3`/`blob-s3`).

Builds on EG-KG.backend.sharded-k-way-durable (sharded K-way durable writer) — see
[`engine.md` § Sharded K-way durable writer](engine.md). The whole point of EG-KG.backend.sharded-k-way-durable is that
a graph routes to `graph-<FNV-1a(name) % K>.redb` and the on-disk layout is HONORED at open
(`reconcile_shard_layout`, `src/redb_layout.rs`). That makes K
immutable per persist-dir without a migration — which is exactly what M3 removes.

---

## `UNIT-PROVEN` — offline migration and catalog core (historical `feat/m3-catalog-migration`)

The cited deliverables have focused unit/integration evidence. That evidence is
repository-scoped and does not assert a live deployment or 1M certification.

### 1. Offline K-shard migration tool (`CONCEPT:EG-KG.sharding.atomic-shard-swap`)

**Files:**
- `src/server/persistence/shard_migrate.rs` — the engine.
- `src/bin/migrate_shards.rs` — the `migrate-shards` CLI (`[[bin]]` gated
  `required-features = ["redb", "server"]` in `Cargo.toml`).
- Shared layout helpers live in server-independent `src/redb_layout.rs`; routing and
  Raft metadata stay in `src/server/persistence/redb_backend.rs`.

**What it does.** OFFLINE (engine stopped — redb holds an exclusive per-file lock), reads a
contiguous current `graph-<n>.redb` set or the retired unindexed K=1 `graph.redb`, then
rewrites every durable row into canonical `graph-<n>.redb` files for the requested K,
routing each graph with the **same** EG-KG.backend.sharded-k-way-durable
`shard_index`, so every graph lands in exactly the shard the running engine will look for it
in. Rows are copied **verbatim** (no decode/unseal/re-derive):

- Per-graph tables — `NODES` / `EDGES` / `LEDGER` / `SEMANTIC` / `GRAPH_META` — moved row for
  row, value blob unchanged (encryption-at-rest blobs survive without the key).
- The tamper-evident hash-chained `AUDIT` log (`CONCEPT:EG-KG.sharding.row-level-security`) is copied verbatim
  `(graph, seq) → blob`, so the chain stays verifiable (re-deriving would break verification).
- Global, non-per-graph records — Raft log/meta (`RAFT_LOG`/`RAFT_META`), cross-shard 2PC
  (`XSHARD_PREPARE`/`XSHARD_DECISION`), matviews (`MATVIEWS`, `compute-dist` only) — re-home to
  the NEW shard 0 (EG-KG.backend.sharded-k-way-durable's `shard0()` home), regardless of graph.

**Public API** (`shard_migrate`):
- `migrate_shards(src_dir, dst_dir, new_k) -> MigrationReport` — out-of-place for
  `new_k` in `1..=64`; refuses to clobber any destination shard file.
- `migrate_in_place(persist_dir, new_k) -> MigrationReport` — writes to a temp subdir, moves
  the source files aside to a timestamped `.shard-migrate-backup-<ts>` dir (recoverable if
  interrupted), swaps the new files in.
- `discover_source_shards(dir)`, `MigrationReport { source_shards, dest_shards, graphs, nodes,
  edges, ledger, semantic, audit, global }`.

**How to run.** Engine STOPPED, then:
```text
# In-place (default): swap shard files, leave a recoverable backup.
migrate-shards --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}" --shards 4

# One-time retired K=1 filename migration; normal startup rejects graph.redb.
migrate-shards --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}" --shards 1

# Out-of-place: write the new K into a fresh dir, swap manually after verifying.
migrate-shards --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}" --shards 4 --dest-dir "${TARGET_PERSIST_DIR:?}"
```
(`--persist-dir` also reads `GRAPH_SERVICE_PERSIST_DIR`.) On success prints per-table counts.

**Round-trip proof.** `retired_k1_layout_migrates_to_canonical_k1` proves the retired
file is readable only by the offline tool and becomes `graph-0.redb` before startup.
`roundtrip_k1_to_k4_preserves_all_graphs`
(`src/server/persistence/shard_migrate.rs`, `#[tokio::test]`): seeds 7 graphs (each 2 nodes +
1 edge) through a real K=1 `RedbBackend`, migrates K=1→K=4, reopens at K=4
(`shard_count() == 4`), and asserts every graph reads back with its exact nodes/edges AND the
graph-tagged node proves no cross-graph mixing. Also `in_place_migration_swaps_and_backs_up`
(verifies the file swap + backup dir), `migration_discovery_rejects_mixed_and_sparse_layouts`,
and `refuses_existing_destination` (clobber guard).

### 2. Tenant catalog core (`CONCEPT:EG-KG.sharding.empty-catalog-routing`)

**File:** `src/server/persistence/tenant_catalog.rs`. Seam wired into
`src/server/persistence/redb_backend.rs` (`RedbBackend.catalog: Option<Arc<TenantCatalog>>`,
builder `with_catalog`, consulted in `shard_for`).

**What it is.** A durable, rebalanceable `graph/tenant → ShardAssignment { shard, node }` map
that OVERRIDES EG-KG.backend.sharded-k-way-durable hash routing per graph:
- `TenantCatalog::resolve_shard(graph_fname, k)` returns the catalog's explicit shard if the
  graph has an entry (clamped into `0..k`), **else falls back to the exact EG-KG.backend.sharded-k-way-durable
  `shard_index`**. So an EMPTY catalog is byte-for-byte identical to no catalog — pure FNV-1a.
- The seam in `RedbBackend::shard_for` is gated on a catalog being attached; default
  (`catalog: None`) is unchanged EG-026. The catalog only ever stores the *exceptions* to the
  hash (moved/rebalanced tenants) — it never has to enumerate all 100M graphs.
- Durability is opt-in: `TenantCatalog::open(persist_dir)` backs it with `catalog.redb`
  (assignments survive restart); `TenantCatalog::in_memory()` is non-durable for tests.

**API:** `lookup`, `resolve_shard`, `assign(graph, shard, node)`, `reassign(graph, new_shard)`
(preserves `node`), `remove`, `len`/`is_empty`, `entries()`. `ShardAssignment::local(shard)`
is the this-node helper (`node: None`).

**Tests** (`tenant_catalog::tests`): `empty_catalog_is_pure_fnv1a` (the no-regression
guarantee — matches `shard_index` for every graph at K∈{1,2,4,8,16}),
`assign_overrides_then_falls_back`, `reassign_moves_route_and_clamps`,
`remove_reverts_to_hash`, `durable_catalog_survives_reopen`.

> **The current safety invariant.** An empty/unattached catalog preserves FNV-1a
> routing. `RedbBackend::open` auto-attaches only when explicitly enabled or when
> an existing durable `catalog.redb` must be honoured; the online move orders
> destination import, durable catalog flip, and source purge. The explicit-K test
> constructor remains unattached. This is `IMPLEMENTED`/`UNIT-PROVEN` source
> behavior, not a claim that an operator has enabled it in production.

---

## M3 status matrix — remaining cross-node/object-tier work is `DESIGNED`

Each task below is self-contained: module/I/O, dependencies & ordering, and whether it can run
in parallel or must sequence behind M2 (the sibling's multi-Raft work in `src/raft/`,
`CONCEPT:EG-KG.sharding.raft-resharding/2.207`). The catalog (EG-031) and migration tool (EG-030) are the substrate
all of these compose.

### R1 — Online per-tenant resharding execution (single node) **[P0, no M2 dep] — `UNIT-PROVEN` (CONCEPT:EG-KG.backend.catalog-shard-resolve)**
Move ONE graph's rows between shards on the same node without stopping the
engine, then flip the catalog route. The moved graph has a bounded quiesce; the
offline tool has no online write path.

> **Current implementation.** `src/server/persistence/online_reshard.rs` (`export_graph_raw`/`import_graph_raw`
> verbatim raw-blob copy — encryption + EG-KG.sharding.row-level-security audit chain preserved — + `execute_online_reshard`)
> driven by `RedbBackend::reshard_graph` (`redb_backend.rs`). New `Cmd::ExportGraphRaw`/
> `ImportGraphRaw` run on the two shard writer threads. **Quiesce:** a backend `routing_epoch`
> `RwLock` — every catalog-attached `record_durable`/`commit_crossmodal` holds a SHARED READ guard
> across resolve+enqueue; the move holds the EXCLUSIVE WRITE guard, so the flip can never lose or
> misroute a write. **Ordering** is `import(dst) committed → catalog flip durable → purge(src)`
> (crash-consistent). Test `online_reshard_moves_graph_live_no_loss` hammers concurrent writes to
> `g` across the flip (20 nodes incl. 10 concurrent), asserts all nodes/edge survive on the new
> shard, audit verifies, and an unrelated graph is untouched. The current path
> uses snapshot-plus-delta copying: bulk work runs while writes continue, then
> `delta_flip_purge` performs only the bounded quiesced delta and preserves the
> durable ordering `import(dst) → catalog flip → purge(src)`.

Original design sketch (superseded by the current snapshot-plus-delta implementation):
- **Module:** new `src/server/persistence/online_reshard.rs`. Composes the EG-030 verbatim
  row-copy (extract the per-graph copy loop in `shard_migrate.rs` into a reusable
  `copy_graph_rows(src_shard, dst_shard, graph_fname)` helper) with the EG-031 catalog.
- **Algorithm:** (1) `catalog.assign(g, dst, node)` is NOT flipped yet; (2) snapshot-copy the
  graph's rows from source shard to dest shard under a read txn (MVCC — writes continue to the
  source); (3) drain/quiesce in-flight writes for `g` (reuse the write-coalescer's per-graph
  writer, `CONCEPT:EG-KG.sharding.per-graph-write-coalescer`, `src/write_coalescer.rs` — `drop_writer`/quiesce one key);
  (4) copy the delta; (5) atomically flip `catalog.reassign(g, dst)` and resume; (6) GC the old
  rows from the source shard.
- **I/O:** redb read/write txns on two shards in the same `RedbBackend`; the catalog write.
- **Ordering / parallel:** independent of M2 (single node). The design sketch
  predates the current R5 gate and is retained only to explain the evolution of
  the contract; it is not an open implementation task.
- **Historical risk covered by the focused fixture:** the quiesce/flip window is
  the correctness crux; `online_reshard_moves_graph_live_no_loss` hammers writes
  to `g` across the flip and asserts zero lost/misrouted rows.

### R2 — Cross-NODE tenant distribution **[P0, MUST sequence behind M2] — `DESIGNED`**
Make `ShardAssignment.node` real: move a tenant to a shard owned by a *different* cluster node.
- **Module:** `online_reshard.rs` (cross-node arm) + a transport for shipping rows.
- **Deps:** REQUIRES M2 multi-Raft landed (`src/raft/multi.rs` `MultiRaft`, `CONCEPT:EG-KG.sharding.raft-resharding`;
  leader transfer/membership `CONCEPT:EG-KG.sharding.semantic-embedding-store-backed`). Cross-node row movement must replicate through
  the destination node's Raft group, not a raw file copy, or the move isn't consensus-durable.
- **I/O:** Raft `propose` of the migrated rows on the destination group; catalog `node` flip.
- **Ordering:** strictly after R1 (reuses its copy/quiesce/flip) AND after M2. Do NOT start the
  cross-node arm until `src/raft/` stabilizes (sibling-owned — coordinate).

### R3 — Rebalancing planner **[`UNIT-PROVEN` — `CONCEPT:EG-KG.sharding.even-load-rebalance`]**
Decide *which* tenants to move and *where*, from live shard load — the policy layer over R1/R2.
- **Module:** `src/server/persistence/rebalance.rs` (NEW, landed). A PURE, deterministic
  `plan_rebalance(&[ShardLoad], RebalanceOptions) -> RebalancePlan` emitting an ordered
  `Vec<ReshardMove { graph, from_shard, to_shard }>`. Greedy hottest→coldest: each step moves
  the ONE graph off the most-loaded shard onto the least-loaded that most shrinks the
  hottest-coldest gap WITHOUT overshooting (new gap `|gap − 2·load|`), stopping at
  `tolerance·mean`, on non-improvement (an indivisible hot graph ⇒ no thrash), or `max_moves`.
- **Inputs / integration hook:** the pure planner is decoupled from live sources
  for testability. The current admin surface supplies a bounded live
  `(sanitized graph, resident node count)` view through `live_graph_loads`; that
  is a resident-size signal, not a complete multi-axis workload model.
  `shard_loads_from_graph_loads(&[(graph, shard, load)], k)` is the pure grouping half;
  `shard_loads_from_catalog(&TenantCatalog, &[(graph, load)], k)` routes each graph through the
  EG-031 catalog (explicit assignment wins, else EG-KG.backend.sharded-k-way-durable FNV-1a). The remaining wiring line —
  sourcing richer queue/CPU/fsync workload axes remains an **integration follow-up**,
  not part of the pure planner.
- **Execution stays R1/R2:** the planner only EMITS the plan; the current admin
  execution path applies a move through R1 online resharding, while R2 remains
  cross-node design work.
- **Tests** (`rebalance::tests`): `balances_a_skewed_shard_set_deterministically` (skewed K=4 →
  imbalance strictly reduced, floored by the largest indivisible graph, deterministic re-plan),
  `divisible_load_reaches_tolerance` (12×25 over K=4 → within the 10% band),
  `already_balanced_yields_empty_plan`, `single_shard_is_a_noop`,
  `indivisible_hot_graph_does_not_thrash`, `grouping_routes_and_fills_empty_shards`,
  `catalog_routing_feeds_the_planner`.

### R4 — BLOB streaming substrate (`CONCEPT:EG-KG.storage.blob-namespace` + `CONCEPT:EG-KG.sharding.m3-r4`) **[`UNIT-PROVEN`]**
Content-addressed, chunked, streamed large-object store off the inline KV path (a 650 MB inline
property blob is wrong). Listed as its own P0 in the gaps report (Wave 5).
- **Substrate (`CONCEPT:EG-KG.storage.blob-namespace`, landed on `main` — commit `6734607`):** the `blob` feature
  ships `blob.redb` + the full begin/chunk/commit/fetch/ref/unref/gc protocol END-TO-END:
  `src/server/blob/store.rs` (`RedbChunkStore` CAS — group-commit, dedup, mark-and-sweep
  refcount GC, capped page cache for bounded RSS), `src/server/blob/mod.rs` (`BlobCursors` +
  TTL reaper), `src/server/handlers/blob.rs` (handler on the blocking pool), and the
  `Blob*` dispatch routing in `src/server/dispatch.rs`. Bounded-memory proven by
  `store::tests::bounded_memory_large_blob_group_commit` and the dispatch-level streamed-blob
  test. `blob-s3` fronts the SAME `ChunkStore` trait with an object-store backend.
- **In-process streaming facade (`CONCEPT:EG-KG.sharding.m3-r4`, current `main`):**
  `src/server/blob/stream.rs` — `stream_blob_put<R: Read>` / `stream_blob_get<W: Write>` stream
  a multi-GB blob between an arbitrary byte source/sink (a local media file, a decompressor, an
  embedding writer) and the CAS WITHOUT buffering the whole blob (one chunk resident). The
  in-process twin of the wire upload/fetch cursor; composes `ChunkStore` verbatim, so it is the
  same dedup'd/refcount-GC'd store. Available to file import/export and to a
  future cold-tier offload (R6); its presence does not mean the R6 object-tier
  integration is complete.
  Test `stream::tests::bounded_memory_streams_large_blob` round-trips a 256 MB blob
  (`EG_BLOB_RSS_MB` → 1 GB+) through a generating `Read` and a hashing `Write`, asserting peak
  RSS growth stays bounded by the capped page cache + one chunk, NOT the blob size.
- **Resharding tie-in (REMAINING, integration follow-up):** when blobs become shardable, EG-030's
  verbatim copy must learn the `blob.redb` tables (extend `copy_global_tables` / per-graph copy)
  and the catalog must route blob refs alongside graph rows. Sequences after both this and R1 land.

### R5 — Catalog auto-attach + admin surface **[P1 — `IMPLEMENTED` / `UNIT-PROVEN`; CONCEPT:EG-KG.sharding.r5-feature]**
Wire the catalog into the live open path and expose assign/reassign over the protocol.

> **Current implementation.** `RedbBackend::open` calls
> `maybe_attach_catalog_from_env`: it attaches `TenantCatalog::open(dir)` when
> `EPISTEMIC_GRAPH_TENANT_CATALOG=1` OR a durable `catalog.redb` already exists (a populated
> catalog from a prior run is honored); otherwise NO catalog — pure EG-026. Empty/absent = byte-
> for-byte FNV-1a, verified by `catalog_auto_attach_gate_and_empty_is_fnv1a`. `RedbBackend::catalog()`
> exposes the catalog for the admin/API to populate/persist (`assign`/`reassign`/`remove` are durable).
> The current protocol surface includes `Reshard`, `Catalog*`, `RebalancePlan`,
> and `RebalanceExecute`; the explicit-K test constructor `open_with_shards`
> deliberately never auto-attaches. Empty/absent catalog routing remains FNV-1a.
> This is source/repository evidence, not a claim that the gate is enabled in a
> live deployment.

### R6 — Cold-tenant hibernation / object-store offload **[P1 — `UNIT-PROVEN` for redb hibernation; `DESIGNED` for object tier; CONCEPT:EG-KG.sharding.eg-r6]**
The RAM-bounding half of the "scalable tenant catalog" is implemented in the
redb tier; colder-than-redb object storage remains a separate design.

> **Current implementation (no object tier dependency).** `src/server/persistence/cold_offload.rs`:
> a `ColdTenantTracker` (`touch`/`cold_graphs(window)`/offload bookkeeping) + `offload_cold_tenants`
> which hibernates every graph idle longer than a window via `GraphCore::hibernate` (EG-KG.storage.100m-tenant),
> retaining the durable redb rows so reads serve through the EG-KG.storage.read-through-seam-exercised node read-through (extends
> the node-level read-through to whole-graph cold offload, exactly as R6 asks). Durability-gated,
> `__commons__` never offloaded. Complements the EG-KG.compute.lane-v budget enforcer (idle-driven vs budget-
> driven). Test `cold_offload_evicts_then_serves_on_access` proves evict-then-serve. This needed
> **no R4 dep** — it bounds RAM by dropping in-RAM state, not by spilling to an object store.
> The current dispatch `touch()` wiring and interval scheduler are present on
> `main`; the remaining arm is actual object-store offload via
> `cold-tier-s3`/`blob-s3` for tenants colder than redb.
- **Designed follow-on:** reuse the `cold-tier` / `cold-tier-s3` features and the
  `blob-s3` CAS seam (R4) only after a versioned location/retention contract is
  implemented. It remains `DESIGNED`, not an available deployment capability.

---

## Current status and follow-ons

1. **`UNIT-PROVEN`:** R3 (planner) and R4 (BLOB substrate/facade) are on current
   `main` with focused fixtures.
2. **`UNIT-PROVEN`:** R1 (online single-node execution) and R5 (auto-attach/admin)
   are wired on current `main`; the gate remains default-off unless explicitly
   enabled or an existing durable catalog is present.
3. **`UNIT-PROVEN`:** R6 redb whole-graph hibernation, dispatch touch, and interval
   sweep are present; object-store spill is still `DESIGNED`.
4. **`DESIGNED`:** R2 cross-node movement must use the destination Raft group and
   a consensus-durable transfer, not a raw file copy.

**Integration follow-ups:** (a) expand the current resident-node load signal to
the queue/CPU/fsync axes needed for a production placement policy; (b) teach
EG-030's verbatim copy the `blob.redb` tables so blobs ride resharding; and (c)
record lab/live/1M evidence against exact promoted artifacts. These are not
claims that the current source has been deployed or certified.

Ground every change in the Wave 4/5 P0 gap scope stated above (its originating
planning doc is not locatable — see the note at the top of this file) and the
EG-KG.backend.sharded-k-way-durable sharding contract in [`engine.md`](engine.md).

---

**See also:** [Capabilities matrix](../capabilities.md) · [Engine Scaling Program](scaling_program.md) · [Multi-Raft Cluster Status](m2_raft_status.md) · [Cluster Deployment](cluster_deployment.md) · [Per-Graph Write Coalescer](write_coalescer.md).
