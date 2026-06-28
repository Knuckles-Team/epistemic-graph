# M3 — Catalog-driven resharding (handoff: DONE vs REMAINING)

> **Scope.** M3 is the "horizontal scale spine" wave from
> [`reports/epistemic-graph-master-engine-gaps-2026-06-23.md`](../../../../reports/epistemic-graph-master-engine-gaps-2026-06-23.md)
> — the two **P0** gaps "Elastic sharding / resharding-rebalancing with live data
> migration" and "Scalable tenant catalog". This document is a DONE-vs-REMAINING handoff:
> the two building blocks that landed on `feat/m3-catalog-migration`, then every remaining
> piece as an independent, pick-up-able task.
>
> **Concept IDs:** `CONCEPT:EG-030` (offline K-shard migration tool), `CONCEPT:EG-031`
> (tenant catalog routing-override seam), `CONCEPT:EG-032` (R3 rebalancing planner),
> `CONCEPT:EG-033` (R4 in-process BLOB streaming facade). Registered in
> [`docs/concepts.md`](../concepts.md).

Builds on EG-026 (sharded K-way durable writer) — see
[`engine.md` § Sharded K-way durable writer](engine.md). The whole point of EG-026 is that
a graph routes to `graph-<FNV-1a(name) % K>.redb` and the on-disk layout is HONORED at open
(`reconcile_shard_layout`, `src/server/persistence/redb_backend.rs:472`). That makes K
immutable per persist-dir without a migration — which is exactly what M3 removes.

---

## DONE — landed on `feat/m3-catalog-migration`

Both deliverables build clean under `--features "full,cluster"` and their unit/integration
tests pass.

### 1. Offline K-shard migration tool (`CONCEPT:EG-030`)

**Files:**
- `src/server/persistence/shard_migrate.rs` — the engine.
- `src/bin/migrate_shards.rs` — the `migrate-shards` CLI (`[[bin]]` gated
  `required-features = ["redb", "server"]` in `Cargo.toml`).
- Routing helpers made `pub(crate)`: `shard_index` / `shard_filename` / `RAFT_META`
  in `src/server/persistence/redb_backend.rs`.

**What it does.** OFFLINE (engine stopped — redb holds an exclusive per-file lock), reads an
existing shard set (`graph.redb` for K=1, or a `graph-<n>.redb` set) and rewrites every
durable row into `graph-<n>.redb` for a NEW K, routing each graph with the **same** EG-026
`shard_index`, so every graph lands in exactly the shard the running engine will look for it
in. Rows are copied **verbatim** (no decode/unseal/re-derive):

- Per-graph tables — `NODES` / `EDGES` / `LEDGER` / `SEMANTIC` / `GRAPH_META` — moved row for
  row, value blob unchanged (encryption-at-rest blobs survive without the key).
- The tamper-evident hash-chained `AUDIT` log (`CONCEPT:KG-2.231`) is copied verbatim
  `(graph, seq) → blob`, so the chain stays verifiable (re-deriving would break verification).
- Global, non-per-graph records — Raft log/meta (`RAFT_LOG`/`RAFT_META`), cross-shard 2PC
  (`XSHARD_PREPARE`/`XSHARD_DECISION`), matviews (`MATVIEWS`, `compute-dist` only) — re-home to
  the NEW shard 0 (EG-026's `shard0()` home), regardless of graph.

**Public API** (`shard_migrate`):
- `migrate_shards(src_dir, dst_dir, new_k) -> MigrationReport` — out-of-place; refuses to
  clobber an existing destination shard file.
- `migrate_in_place(persist_dir, new_k) -> MigrationReport` — writes to a temp subdir, moves
  the OLD files aside to a timestamped `.shard-migrate-backup-<ts>` dir (recoverable if
  interrupted), swaps the new files in.
- `discover_source_shards(dir)`, `MigrationReport { source_shards, dest_shards, graphs, nodes,
  edges, ledger, semantic, audit, global }`.

**How to run.** Engine STOPPED, then:
```text
# In-place (default): swap shard files, leave a recoverable backup.
migrate-shards --persist-dir /var/lib/epistemic-graph --shards 4

# Out-of-place: write the new K into a fresh dir, swap manually after verifying.
migrate-shards --persist-dir /var/lib/eg --shards 4 --dest-dir /var/lib/eg-k4
```
(`--persist-dir` also reads `GRAPH_SERVICE_PERSIST_DIR`.) On success prints per-table counts.

**Round-trip proof.** `roundtrip_k1_to_k4_preserves_all_graphs`
(`src/server/persistence/shard_migrate.rs`, `#[tokio::test]`): seeds 7 graphs (each 2 nodes +
1 edge) through a real K=1 `RedbBackend`, migrates K=1→K=4, reopens at K=4
(`shard_count() == 4`), and asserts every graph reads back with its exact nodes/edges AND the
graph-tagged node proves no cross-graph mixing. Also `in_place_migration_swaps_and_backs_up`
(verifies the file swap + backup dir) and `refuses_existing_destination` (clobber guard).

### 2. Tenant catalog core (`CONCEPT:EG-031`)

**File:** `src/server/persistence/tenant_catalog.rs`. Seam wired into
`src/server/persistence/redb_backend.rs` (`RedbBackend.catalog: Option<Arc<TenantCatalog>>`,
builder `with_catalog`, consulted in `shard_for`).

**What it is.** A durable, rebalanceable `graph/tenant → ShardAssignment { shard, node }` map
that OVERRIDES EG-026 hash routing per graph:
- `TenantCatalog::resolve_shard(graph_fname, k)` returns the catalog's explicit shard if the
  graph has an entry (clamped into `0..k`), **else falls back to the exact EG-026
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

> **The safety invariant.** The catalog is a *read-only seam* until something is assigned, and
> `with_catalog` is never called on the default open path. The running engine's routing is
> therefore unchanged by this branch. Do NOT add an auto-attach until online execution
> (below) is built and tested, or routes will flip without the rows having moved.

---

## REMAINING — the larger M3, as independent pick-up-able tasks

Each task below is self-contained: module/I/O, dependencies & ordering, and whether it can run
in parallel or must sequence behind M2 (the sibling's multi-Raft work in `src/raft/`,
`CONCEPT:KG-2.205/2.207`). The catalog (EG-031) and migration tool (EG-030) are the substrate
all of these compose.

### R1 — Online per-tenant resharding execution (single node) **[P0, no M2 dep]**
Move ONE graph's rows between shards on the same node with no downtime, then flip the catalog
route. The hard part the offline tool skips.
- **Module:** new `src/server/persistence/online_reshard.rs`. Composes the EG-030 verbatim
  row-copy (extract the per-graph copy loop in `shard_migrate.rs` into a reusable
  `copy_graph_rows(src_shard, dst_shard, graph_fname)` helper) with the EG-031 catalog.
- **Algorithm:** (1) `catalog.assign(g, dst, node)` is NOT flipped yet; (2) snapshot-copy the
  graph's rows from source shard to dest shard under a read txn (MVCC — writes continue to the
  source); (3) drain/quiesce in-flight writes for `g` (reuse the write-coalescer's per-graph
  writer, `CONCEPT:KG-2.182`, `src/write_coalescer.rs` — `drop_writer`/quiesce one key);
  (4) copy the delta; (5) atomically flip `catalog.reassign(g, dst)` and resume; (6) GC the old
  rows from the source shard.
- **I/O:** redb read/write txns on two shards in the same `RedbBackend`; the catalog write.
- **Ordering / parallel:** independent of M2 (single node). Must land AFTER the catalog auto-
  attach decision (R5). Parallel-safe with R3/R4. **This is the keystone remaining item.**
- **Risk:** the quiesce/flip window is the correctness crux — needs a test that hammers writes
  to `g` across the flip and asserts zero lost/misrouted rows.

### R2 — Cross-NODE tenant distribution **[P0, MUST sequence behind M2]**
Make `ShardAssignment.node` real: move a tenant to a shard owned by a *different* cluster node.
- **Module:** `online_reshard.rs` (cross-node arm) + a transport for shipping rows.
- **Deps:** REQUIRES M2 multi-Raft landed (`src/raft/multi.rs` `MultiRaft`, `CONCEPT:KG-2.205`;
  leader transfer/membership `CONCEPT:KG-2.207`). Cross-node row movement must replicate through
  the destination node's Raft group, not a raw file copy, or the move isn't consensus-durable.
- **I/O:** Raft `propose` of the migrated rows on the destination group; catalog `node` flip.
- **Ordering:** strictly after R1 (reuses its copy/quiesce/flip) AND after M2. Do NOT start the
  cross-node arm until `src/raft/` stabilizes (sibling-owned — coordinate).

### R3 — Rebalancing planner **[DONE — `CONCEPT:EG-032`]**
Decide *which* tenants to move and *where*, from live shard load — the policy layer over R1/R2.
- **Module:** `src/server/persistence/rebalance.rs` (NEW, landed). A PURE, deterministic
  `plan_rebalance(&[ShardLoad], RebalanceOptions) -> RebalancePlan` emitting an ordered
  `Vec<ReshardMove { graph, from_shard, to_shard }>`. Greedy hottest→coldest: each step moves
  the ONE graph off the most-loaded shard onto the least-loaded that most shrinks the
  hottest-coldest gap WITHOUT overshooting (new gap `|gap − 2·load|`), stopping at
  `tolerance·mean`, on non-improvement (an indivisible hot graph ⇒ no thrash), or `max_moves`.
- **Inputs / integration hook:** decoupled from any live source for testability.
  `shard_loads_from_graph_loads(&[(graph, shard, load)], k)` is the pure grouping half;
  `shard_loads_from_catalog(&TenantCatalog, &[(graph, load)], k)` routes each graph through the
  EG-031 catalog (explicit assignment wins, else EG-026 FNV-1a). The remaining wiring line —
  sourcing the live `(graph, load)` stats from the EG-026 sharded writer + the `CONCEPT:KG-2.51`
  per-graph size gauges in a backend/admin surface — is an **integration follow-up**, NOT in the
  pure planner.
- **Execution stays R1/R2:** the planner only EMITS the plan; applying a move (snapshot-copy the
  rows, then `catalog.reassign(graph, to_shard)`) is R1 online-reshard (sibling-owned).
- **Tests** (`rebalance::tests`): `balances_a_skewed_shard_set_deterministically` (skewed K=4 →
  imbalance strictly reduced, floored by the largest indivisible graph, deterministic re-plan),
  `divisible_load_reaches_tolerance` (12×25 over K=4 → within the 10% band),
  `already_balanced_yields_empty_plan`, `single_shard_is_a_noop`,
  `indivisible_hot_graph_does_not_thrash`, `grouping_routes_and_fills_empty_shards`,
  `catalog_routing_feeds_the_planner`.

### R4 — BLOB streaming substrate (`CONCEPT:KG-2.206` + `CONCEPT:EG-033`) **[DONE]**
Content-addressed, chunked, streamed large-object store off the inline KV path (a 650 MB inline
property blob is wrong). Listed as its own P0 in the gaps report (Wave 5).
- **Substrate (`CONCEPT:KG-2.206`, landed on `main` — commit `6734607`):** the `blob` feature
  ships `blob.redb` + the full begin/chunk/commit/fetch/ref/unref/gc protocol END-TO-END:
  `src/server/blob/store.rs` (`RedbChunkStore` CAS — group-commit, dedup, mark-and-sweep
  refcount GC, capped page cache for bounded RSS), `src/server/blob/mod.rs` (`BlobCursors` +
  TTL reaper), `src/server/handlers/blob.rs` (handler on the blocking pool), and the
  `Blob*` dispatch routing in `src/server/dispatch.rs`. Bounded-memory proven by
  `store::tests::bounded_memory_large_blob_group_commit` and the dispatch-level streamed-blob
  test. `blob-s3` fronts the SAME `ChunkStore` trait with an object-store backend.
- **In-process streaming facade (`CONCEPT:EG-033`, NEW this branch):**
  `src/server/blob/stream.rs` — `stream_blob_put<R: Read>` / `stream_blob_get<W: Write>` stream
  a multi-GB blob between an arbitrary byte source/sink (a local media file, a decompressor, an
  embedding writer) and the CAS WITHOUT buffering the whole blob (one chunk resident). The
  in-process twin of the wire upload/fetch cursor; composes `ChunkStore` verbatim, so it is the
  same dedup'd/refcount-GC'd store. Used by file import/export and the cold-tier offload (R6).
  Test `stream::tests::bounded_memory_streams_large_blob` round-trips a 256 MB blob
  (`EG_BLOB_RSS_MB` → 1 GB+) through a generating `Read` and a hashing `Write`, asserting peak
  RSS growth stays bounded by the capped page cache + one chunk, NOT the blob size.
- **Resharding tie-in (REMAINING, integration follow-up):** when blobs become shardable, EG-030's
  verbatim copy must learn the `blob.redb` tables (extend `copy_global_tables` / per-graph copy)
  and the catalog must route blob refs alongside graph rows. Sequences after both this and R1 land.

### R5 — Catalog auto-attach + admin surface **[P1, gate before R1 goes live]**
Wire the catalog into the live open path and expose assign/reassign over the protocol.
- **Module:** `RedbBackend::open` decides whether to `with_catalog(TenantCatalog::open(dir))`
  based on a flag (`EPISTEMIC_GRAPH_TENANT_CATALOG=1`, default off); a new `Method::Reshard{...}`
  / admin RPC drives `assign`/`reassign`/`remove`/`entries`.
- **Deps:** the catalog core (DONE). MUST NOT auto-attach until R1 exists, or a route flip
  strands rows. This is the controlled on-ramp: ship behind a default-off flag, attach, then
  enable R1.
- **Parallel:** the admin RPC can be built in parallel with R1; the auto-attach flip is the
  sequencing gate.

### R6 — Cold-tenant hibernation / object-store offload **[P1, depends on R4]**
The other half of the "scalable tenant catalog" P0: 100M graphs can't all be resident; cold
tenants offload to an object tier and rehydrate on access.
- **Module:** reuse `cold-tier` / `cold-tier-s3` features (`Cargo.toml`) + the `blob-s3` CAS
  seam (R4). The catalog grows a `cold: bool` / location field on `ShardAssignment`.
- **Deps:** R4 (BLOB/object substrate). Independent of R1–R3 routing once the schema field
  lands. Sequence after R4.

---

## Suggested order for a fresh agent

1. ~~**R3** (planner) and **R4** (BLOB)~~ — **DONE** (`EG-032` planner; `KG-2.206` substrate +
   `EG-033` streaming facade). Both independent, no M2 dep.
2. **R1** (online single-node execution) — highest leverage, unblocks real resharding, no M2 dep.
   Consumes the R3 plan (`plan_rebalance`) and flips `catalog.reassign`.
3. **R5** (auto-attach gate) once R1 is proven.
4. **R2** (cross-node) only after M2 lands in `src/raft/`.
5. **R6** after R4 (reuses the `EG-033` streaming facade for object-store offload).

**Integration follow-ups (not started):** (a) source live `(graph, load)` stats into
`rebalance::shard_loads_from_catalog` from the EG-026 writer + KG-2.51 gauges in a backend/admin
surface; (b) teach EG-030's verbatim copy the `blob.redb` tables so blobs ride resharding.

Ground every change in `reports/epistemic-graph-master-engine-gaps-2026-06-23.md` (Wave 4/5
P0s) and the EG-026 sharding contract in [`engine.md`](engine.md).
