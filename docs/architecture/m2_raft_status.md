# M2 Raft Hardening — Status & Handoff

> Branch `feat/m2-raft-hardening` (off `main`, which carries the full M1 stack).
> Scope: the multi-Raft follow-ups documented on `CONCEPT:EG-KG.sharding.raft-resharding` —
> pooled per-peer connections, group-per-tenant-range routing, per-group snapshot
> scoping, leader balancing across groups, heartbeat coalescing.
>
> This branch is **NOT pushed, NOT deployed**. Single-group behavior is byte-for-byte
> unchanged; every addition activates only under a multi-group / multi-node config.

All file:line references are against this branch's tree.

---

## DONE — implemented + lib-tested

### 1. Pooled per-peer Raft connections — `CONCEPT:AU-KG.ontology.manage-arbitrary`
- **What:** the scaffold opened a fresh `TcpStream` per append/vote/snapshot RPC and
  dropped it after one round-trip (a connect+handshake per heartbeat, per peer, per
  group). A shared `PeerPool` now keeps a bounded set of WARM connections **per peer
  address**, reused across RPCs and across ALL groups on the node (one pool per
  `MultiRaft`, mirroring the already-shared inbound listener).
- **Where:**
  - `src/raft/network.rs:94` `struct PeerPool` (idle map + `opens`/`reuses` counters).
  - `src/raft/network.rs:148` `PeerPool::round_trip` — take-warm-or-connect, with the
    stale-retry contract: a reused connection that fails on the first frame is
    discarded and the call retries ONCE on a fresh connection, so openraft never sees
    a spurious failure. Wire is strict request→response on one stream (no correlation
    id), so a connection is loaned out exclusively for one round-trip and only
    returned to the idle set if it SUCCEEDED.
  - `src/raft/network.rs:220` `GroupNetworkFactory` carries `pool: Arc<PeerPool>`;
    `src/raft/network.rs:264` `GroupNetworkClient::round_trip` now calls
    `self.pool.round_trip(...)` instead of `TcpStream::connect` per call.
  - `src/raft/multi.rs:179` `MultiRaft.pool` field, created at `multi.rs:223`
    (`network::PeerPool::new()`), threaded into every group's factory at
    `multi.rs:328`; exposed via `MultiRaft::pool()` (`multi.rs:263`).
- **Proven by:**
  - `src/raft/tests.rs:600` `peer_pool_reuses_warm_connection` — three sequential RPCs
    to one echo peer pay exactly ONE TCP connect (`opens()==1`, `reuses()>=2`).
  - `src/raft/tests.rs:642` `peer_pool_retries_on_stale_connection` — an echo server
    that closes after one frame forces a reconnect; the second round-trip still
    succeeds (`opens()==2`, no hard error).

### 2. Group-per-tenant-range routing ring — `CONCEPT:AU-KG.ingest.mirror-inbound`
- **What:** `GroupRouter::group_of` now resolves `override → tenant-range ring →
  DEFAULT_GROUP`. The ring is a sorted, de-duplicated set of group ids that un-pinned
  graphs hash-distribute across via a **stable FNV-1a** hash (identical on every node,
  never persisted — NOT `RandomState`). With no ring configured (the default) every
  un-pinned graph maps to `DEFAULT_GROUP` — byte-for-byte the single-group scaffold.
- **Where:**
  - `src/raft/multi.rs:62` `GroupRouter.ring` field + `fnv1a` helper.
  - `src/raft/multi.rs:86` `group_of`, `multi.rs:101` `set_group_ring`,
    `multi.rs:110` `group_ring`.
  - `src/raft/multi.rs` `MultiRaft::configure_group_ring(n, peers, bootstrap)` —
    brings up groups `0..n` with the complete configured peer set and installs the
    ring; `n<=1` is a no-op (single-group). Every non-default group therefore has the
    same quorum replication and failover contract as the default group.
- **Proven by:** `src/raft/tests.rs:678` `group_router_distributes_tenants_across_ring`
  — empty ring ⇒ all to `DEFAULT_GROUP`; a 4-group ring spreads 200 tenants across
  ≥2 groups; deterministic on repeat; `assign` override beats the ring;
  `is_cross_shard` works across real ranges; empty ring collapses back to default.

### 3. Per-group snapshot scoping — `CONCEPT:AU-KG.ingest.staged`
- **What:** a group's snapshot dump is SCOPED to the graphs whose tenant range resolves
  to THIS group, so a large tenant in one group never bloats another group's snapshot.
  Snapshot membership is enumerated from the complete graph catalog, not the resident
  cache, so cold/evicted graphs remain recoverable. In Raft snapshot schema 4, each graph
  is serialized once as its complete raw durable row set; decoded node/edge/semantic copies are forbidden
  because they duplicate memory/wire cost, expose plaintext beside encrypted rows, and can
  disagree with durable authority. Install validates the durable `graph_meta` identity,
  rejects cross-group rows, reconstructs and atomically publishes the serving projection
  with its exact durable incarnation, and removes stale same-group graphs absent from the snapshot.
  A store opened WITHOUT a router (the direct/single-store path) dumps the whole
  registry, preserving the scaffold behavior.
- **Where:**
  - `src/raft/mod.rs` `AppCtx.router: Option<Arc<multi::GroupRouter>>` — threaded
    through every `AppCtx` construction (node/harnesses/tests = `None`;
    `MultiRaft::create_group` builds a `store_ctx` carrying `Some(self.router)` at
    `src/raft/multi.rs:311`).
  - `src/raft/store.rs` `dump_graphs` filters the full `registry.list()` catalog by
    `router.group_of(name) == self.group_id` when a router is present.
  - `src/raft/store.rs` `scoped_snapshot_graph_names` (`#[cfg(test)]`) — the
    test seam.
- **Proven by:** `src/raft/tests.rs` `group_snapshot_is_scoped_to_its_tenant_range`
  — two graphs pinned to non-default groups 3 and 5; group 3's snapshot carries ONLY
  resident `graphA`, group 5's ONLY catalog-only/evicted `graphB`, and the DEFAULT group
  (0) carries ONLY the un-pinned bootstrap `__commons__` (no bleed or cold-graph loss);
  a router-less store dumps the WHOLE catalog (`__commons__` + both graphs).

**Build:** `cargo build --release --features "full,cluster" -j8` → clean (exit 0).
**Tests:** `cargo test --release --features "full,cluster" --lib raft` → see REPORT
(all single-group raft tests stay green; the 4 new M2 tests pass).

---

## DONE (R1/R2/R3) — branch `feat/m2-r1r3-completion` (off the base above)

> All three remaining M2 items are now **implemented + lib-tested** on top of the base.
> Single-group default behavior is unchanged — the full pre-existing `--lib raft` suite
> (incl. the 3-node failover test + the nemesis gauntlets) stays green.
> **Concept-ID note:** the original plan reserved EG-KG.storage.kg-kg-2 (R1) and KG-2.269 (R2). By the
> time this branch landed, **KG-2.269 had been claimed by another session** (`shard-commons`,
> "Ingestion graph routing"), so the heartbeat work uses a fresh id. Final mapping:
> **R3 = EG-KG.storage.kg-kg-2**, **R1 = EG-KG.sharding.multi-raft**, **R2 = EG-KG.storage.concept-2** (all reserved in the ledger).

### R3. Per-group multi-NODE membership join — `CONCEPT:EG-KG.storage.kg-kg-2`
- **What:** a group can now be grown from a single-node bootstrap to span multiple
  NODES via the openraft add-learner → change-membership lifecycle, per group, over the
  shared listener.
- **Where (`src/raft/multi.rs`):**
  - `join_group(gid, peers)` — stand a group up on a node as an EMPTY, non-bootstrapping
    member (never `initialize`s) ready to receive replication.
  - `add_group_member(gid, new_node, addr)` — leader-side: `add_learner(.., blocking)`
    (blocks until the joiner is up-to-date) then `change_membership(voters ∪ {new})`.
  - `remove_group_member(gid, node)` — leader-side `change_membership` to a reduced voter
    set; idempotent; refuses to remove the LAST voter.
  - `group_membership(gid)` — the current sorted VOTER set from the replicated metrics.
- **Proven by:** `src/raft/tests.rs` `multi_node_group_join_then_leader_rebalance` —
  node 1 single-member-bootstraps group 7, nodes 2/3 `join_group` it empty, the leader
  `add_group_member`s both → membership becomes `[1,2,3]`, and a write through the leader
  replicates to the two freshly-joined voters.

### R1. Leader balancing across groups — `CONCEPT:EG-KG.sharding.multi-raft`
- **What:** `MultiRaft::rebalance_leaders()` spreads leadership so one node isn't leader
  for every group. EVERY node runs the pass (like a real cluster); per group it computes
  the deterministic round-robin target (`desired_leader(gid, sorted_voters)` — identical
  on every node, no coordination) and either **claims** a group it should lead (triggers
  an election, rate-limited per group by `ELECT_COOLDOWN` = 10s so it never flaps) or
  **yields** a group it leads but shouldn't (disables its heartbeat so the lease expires
  and the target can take over). No-op for single-voter groups + already-balanced groups.
  Returns a `RebalanceReport { targets, elected, yielded, errors }`.
- **openraft-0.9 reality (important):** openraft **0.9.24 has NO `transfer_leader`** (it
  arrives in 0.10 as `trigger_transfer_leader`), and its anti-disruption / leader-
  stickiness makes followers REJECT a challenger's vote while they still hear from a
  healthy leader. So a triggered election ALONE cannot move leadership off a healthy
  incumbent — the incumbent must cooperatively **yield** (stop heartbeating). That is the
  yield half above; with both halves the cluster converges to the round-robin spread
  within a couple of election timeouts.
- **Where (`src/raft/multi.rs`):** `desired_leader` (pure fn), `RebalanceReport`,
  `MultiRaft::rebalance_leaders` + `may_trigger_elect` (cooldown), `last_elect` field.
- **Proven by:** `src/raft/tests.rs` `desired_leader_round_robin_spreads_across_voters`
  (unit: the selection spreads across all voters, deterministic) **and** the integration
  half of `multi_node_group_join_then_leader_rebalance` — over the 3-voter group 7 the
  target is node 2 (`7 % 3`), the first pass shows node 1 `yielded` + node 2 `elected`,
  periodic passes converge leadership to node 2, and a further pass is a no-op (idempotent).

### R2. Heartbeat coalescing — `CONCEPT:EG-KG.storage.concept-2`
- **What:** same-destination Raft HEARTBEATS (empty-entries `AppendEntries`) across N
  groups coalesce into ONE batched frame to that peer instead of N frames per heartbeat
  interval; the batch rides the AU-KG.ontology.manage-arbitrary `PeerPool` (one connect-amortized round-trip).
  Only heartbeats coalesce — log-bearing appends / votes / snapshots stay individual
  (latency/ordering-sensitive).
- **Where (`src/raft/network.rs`):**
  - `RaftFrame { One(GroupRpc) | Batch(Vec<GroupRpc>) }` + `RaftFrameReply` — the
    top-level wire envelope; a single-RPC frame is `One`, so the existing per-group path
    is behavior-identical (and the 3-node + 2-group tests exercise it).
  - `HeartbeatCoalescer` — buffers heartbeats per peer (`offer` gates on `is_heartbeat`),
    `drain_batches()` builds one ordered batch per peer, `send_batch()` ships it over the
    pool and returns the ordered per-RPC replies; `coalesced`/`flushes` counters.
  - `src/raft/multi.rs` `serve_conn` demuxes a `Batch` by dispatching each tagged sub-RPC
    to its group and replying in the SAME order.
- **Proven by:** `src/raft/tests.rs` `heartbeat_coalescer_batches_per_peer` (unit:
  bucket-by-peer, non-heartbeats refused, counters) and
  `coalesced_batch_round_trips_on_one_connection` (a 3-heartbeat batch demuxes to 3
  ordered replies over EXACTLY ONE TCP connect on a live listener).

**Build:** `cargo build --features "full,cluster" --lib` → clean. **Tests:**
`cargo test --features "full,cluster" --lib raft` → **23 passed, 0 failed**. My three
changed files are `cargo fmt` + `cargo clippy` clean (the remaining workspace clippy
warnings pre-date this branch in M1 `redb_backend.rs` / query handlers / `eg-query`).

---

## DONE — openraft 0.9 → 0.10 migration + native handoff (CONCEPT:AU-KG.backend.authority-has-already-acked)

> Branch `feat/openraft-010-cluster`. Bumped openraft to **0.10** (pinned
> `=0.10.0-alpha.26`, the line that carries `transfer_leader`) and migrated the whole
> `src/raft/` API surface to the **v2 split-storage** model. The cooperative
> yield-then-claim leader balancing (EG-KG.sharding.multi-raft) is **replaced** by the native graceful
> handoff. See `docs/architecture/cluster_deployment.md` for the multi-node deploy.

- **Storage = v2 split traits.** openraft 0.10 removed the combined `RaftStorage` + the
  `Adaptor`. `EgStore` now implements **`RaftLogStorage`** (+ super-trait
  `RaftLogReader`) AND **`RaftStateMachine`** (+ `RaftSnapshotBuilder`) on
  `Arc<EgStore>`; `create_group` passes the SAME `Arc` as both (no adaptor). Every
  storage method returns `std::io::Error` (the 0.9 `StorageError`/`StorageIOError`
  constructors are gone); types use the `…Of<C>` aliases. `append` now signals
  durability via an `IOFlushed` callback (fired right after our group-commit fsync);
  `apply` consumes a `Stream` of `(entry, responder)` and `send`s each response;
  `delete_conflict_logs_since`/`purge_logs_upto` became `truncate_after`/`purge`; the
  chunked `install_snapshot` became full-snapshot transfer.
- **Network = `RaftNetworkV2`.** The deprecated v1 `RaftNetwork` was deleted; the client
  implements `RaftNetworkV2` (blanket-deriving the `Net*` sub-traits the factory needs).
  The snapshot RPC is now one tagged `full_snapshot` frame (vote + meta + body) → the
  follower's `install_full_snapshot`. `RPCError<C>` is single-generic. A new
  `transfer_leader` RPC forwards the handoff notification to the target.
- **R1 native instant handoff (DONE).** `MultiRaft::rebalance_leaders` no longer yields
  heartbeats — for each group THIS node leads whose round-robin target is elsewhere, it
  issues the native **`raft.trigger().transfer_leader(target)`** (graceful, near-instant;
  openraft hands a fresh term + the leader vote to the target and notifies it to campaign
  at once). `RebalanceReport.{elected,yielded}` collapse to `transferred`. Rate-limited
  per group by `TRANSFER_COOLDOWN` so it never spams transfers.
- **Tests green.** `cargo test --features "full,cluster" --lib raft` — the full pre-existing
  suite (incl. the 3-node failover + join-then-rebalance, now asserting the native
  transfer) passes against the 0.10 API. Validate with `scripts/validate-raft-cluster.sh`.

## STILL REMAINING — needs real multi-node hardware (not in-process testable)
- **Both:** true cross-host soak (leadership actually rebalancing across machines,
  heartbeat-frame reduction under load) needs multi-node hardware; the in-process
  harness proves correctness, not the distributed performance win.

## NE-171 source closure — cadence, durable group authority, bounded balance

The source seams described above are now wired in the current tree; root-owned
validation remains the release gate:

- `HeartbeatCoalescer` is installed on every live `GroupNetworkFactory`. Empty-entry
  `append_entries` calls enqueue per destination, wait through a bounded 5 ms window,
  and receive ordered replies from one `RaftFrame::Batch`; log-bearing appends, votes,
  snapshots, and transfer notifications retain the single-RPC path. The queue and
  frame remain bounded by the existing peer/RPC caps, and shutdown fails pending
  waiters before groups are drained.
- Existing on-disk redb shard count is reconciled into `RaftClusterConfig.groups`
  before `DEFAULT_GROUP` or the tenant ring starts. A configured group count remains
  the fresh-store request; an existing layout wins until an offline shard migration.
- A 15 s manager-owned scheduler invokes the existing native `transfer_leader` path.
  It observes current leaders, requires a one-slot load margin (equal loads are
  hysteresis), permits one automatic transfer per pass, enforces the existing 10 s
  per-group cooldown, and fails closed when current/target failure domains are equal
  or unknown. `EPISTEMIC_GRAPH_RAFT_FAILURE_DOMAINS` can provide an explicit complete
  node→domain map; absent that map, the advertised endpoint host is the safety domain.
- Source fixtures cover the batch frame contract, durable-count reconciliation,
  deterministic domain parsing/safety, and existing single-node/cluster term and
  no-churn paths. A cross-host soak and measured performance benefit are deliberately
  not claimed here.

---

**See also:** [Capabilities matrix](../capabilities.md) · [Engine Scaling Program](scaling_program.md) · [Catalog-driven Resharding](m3_resharding.md) · [Cluster Deployment](cluster_deployment.md) · [Distribution / Robotics / GPU](distribution_robotics_gpu.md).
