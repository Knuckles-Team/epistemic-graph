# M2 Raft Hardening — Status & Handoff

> Branch `feat/m2-raft-hardening` (off `main`, which carries the full M1 stack).
> Scope: the multi-Raft follow-ups documented on `CONCEPT:KG-2.205` —
> pooled per-peer connections, group-per-tenant-range routing, per-group snapshot
> scoping, leader balancing across groups, heartbeat coalescing.
>
> This branch is **NOT pushed, NOT deployed**. Single-group behavior is byte-for-byte
> unchanged; every addition activates only under a multi-group / multi-node config.

All file:line references are against this branch's tree.

---

## DONE — implemented + lib-tested

### 1. Pooled per-peer Raft connections — `CONCEPT:KG-2.265`
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

### 2. Group-per-tenant-range routing ring — `CONCEPT:KG-2.266`
- **What:** `GroupRouter::group_of` now resolves `override → tenant-range ring →
  DEFAULT_GROUP`. The ring is a sorted, de-duplicated set of group ids that un-pinned
  graphs hash-distribute across via a **stable FNV-1a** hash (identical on every node,
  never persisted — NOT `RandomState`). With no ring configured (the default) every
  un-pinned graph maps to `DEFAULT_GROUP` — byte-for-byte the single-group scaffold.
- **Where:**
  - `src/raft/multi.rs:62` `GroupRouter.ring` field + `fnv1a` helper.
  - `src/raft/multi.rs:86` `group_of`, `multi.rs:101` `set_group_ring`,
    `multi.rs:110` `group_ring`.
  - `src/raft/multi.rs:273` `MultiRaft::configure_group_ring(n)` — brings up groups
    `0..n` on this node and installs the ring; `n<=1` is a no-op (single-group).
- **Proven by:** `src/raft/tests.rs:678` `group_router_distributes_tenants_across_ring`
  — empty ring ⇒ all to `DEFAULT_GROUP`; a 4-group ring spreads 200 tenants across
  ≥2 groups; deterministic on repeat; `assign` override beats the ring;
  `is_cross_shard` works across real ranges; empty ring collapses back to default.

### 3. Per-group snapshot scoping — `CONCEPT:KG-2.267`
- **What:** a group's snapshot dump is SCOPED to the graphs whose tenant range resolves
  to THIS group, so a large tenant in one group never bloats another group's snapshot.
  A store opened WITHOUT a router (the direct/single-store path) dumps the whole
  registry, preserving the scaffold behavior.
- **Where:**
  - `src/raft/mod.rs` `AppCtx.router: Option<Arc<multi::GroupRouter>>` — threaded
    through every `AppCtx` construction (node/harnesses/tests = `None`;
    `MultiRaft::create_group` builds a `store_ctx` carrying `Some(self.router)` at
    `src/raft/multi.rs:311`).
  - `src/raft/store.rs:224` `dump_graphs` filters `all_entries()` by
    `router.group_of(name) == self.group_id` when a router is present.
  - `src/raft/store.rs:247` `scoped_snapshot_graph_names` (`#[cfg(test)]`) — the
    test seam.
- **Proven by:** `src/raft/tests.rs:725` `group_snapshot_is_scoped_to_its_tenant_range`
  — two graphs pinned to non-default groups 3 and 5; group 3's snapshot carries ONLY
  `graphA`, group 5's ONLY `graphB`, and the DEFAULT group (0) carries ONLY the
  un-pinned bootstrap `__commons__` (no bleed across any of them); a router-less store
  dumps the WHOLE registry (`__commons__` + both graphs), proving the unscoped path is
  intact.

**Build:** `cargo build --release --features "full,cluster" -j8` → clean (exit 0).
**Tests:** `cargo test --release --features "full,cluster" --lib raft` → see REPORT
(all single-group raft tests stay green; the 4 new M2 tests pass).

---

## REMAINING — independent pick-up tasks

### R1. Leader balancing across groups — `CONCEPT:KG-2.268` (RESERVE) — UNTOUCHED
- **Status:** not started; no code on this branch. The prior agent did not scaffold it.
- **Module/files:** `src/raft/multi.rs` (new method on `MultiRaft`, e.g.
  `rebalance_leaders()`); read-side helper already exists — the harness
  `src/raft/harness/cluster.rs:250` `leadership_view()` returns `(node_id, term,
  is_leader)` per node, and `MultiRaft::current_leader` is at `multi.rs:163`.
- **What it needs:** with N groups over M nodes, leaders tend to cluster on the
  bootstrap node (lowest-id member initializes each group at `multi.rs:340`). Add a
  balancer that, per group, asks each group's `Raft` for its current leader, computes
  a target distribution (round-robin group→node, or least-loaded), and calls openraft's
  `raft.trigger().transfer_leader(target)` (openraft 0.9.24) on the over-loaded leaders.
  Make it idempotent + rate-limited (no flapping), and a no-op when only one node is a
  voter in a group.
- **Dependencies / ordering:** depends on **per-group multi-NODE membership join**
  (see R3) actually being exercised — today `configure_group_ring` brings each group up
  as a single-member bootstrap on ONE node (`multi.rs:270` doc-comment), so there is
  nothing to balance until groups span nodes. Sequence R3 (or a membership-join test
  harness) before R1 is testable.
- **Parallel-safe?** Touches `src/raft/multi.rs` — **must-sequence** with R2 and any
  other `multi.rs` edit. Disjoint from `network.rs`/`store.rs`.
- **Hardware:** needs a **real (or in-process) multi-node cluster** to verify leadership
  actually moves. The `src/raft/harness/` in-process multi-node harness is the cheapest
  vehicle; true cross-host verification needs multi-node hardware.

### R2. Heartbeat coalescing — `CONCEPT:KG-2.269` (RESERVE) — UNTOUCHED
- **Status:** not started; no code on this branch.
- **Module/files:** `src/raft/network.rs` (+ small touch in `src/raft/multi.rs` if a
  coalescing dispatcher is owned by the manager).
- **What it needs:** today each group's `Raft` sends its own append/heartbeat RPC per
  peer per tick; with N groups to the same peer that is N independent frames per
  heartbeat interval. Coalesce same-destination heartbeats across groups into one
  batched frame (the inbound listener already demuxes by `gid`, so a multi-group frame
  envelope `Vec<GroupRpc>` is a natural extension of `GroupRpc`/`GroupRpcReply` in
  `network.rs`). Must stay strict request→response and preserve per-group ordering;
  openraft's network trait is per-group, so coalescing happens BELOW it (buffer +
  flush on a short timer, à la Nagle) rather than by changing what openraft calls.
- **Dependencies / ordering:** independent of R1's leadership logic, but **edits the
  same files** (`network.rs`, possibly `multi.rs`). Builds naturally on the KG-2.265
  `PeerPool` (the coalesced frame still rides a pooled connection).
- **Parallel-safe?** **Must-sequence** with R1 (shared `multi.rs`) and with KG-2.265
  follow-ups (shared `network.rs`). Do R2 on its own off this branch, then merge.
- **Hardware:** logic is unit-testable in-process (assert N group heartbeats to one
  peer collapse into one frame via the `PeerPool` `opens`/`reuses`-style counters);
  end-to-end latency/throughput benefit needs a multi-node cluster.

### R3. Per-group multi-NODE membership join — enabler, partially-scaffolded
- **Status:** single-NODE path done (`configure_group_ring` bootstraps each group on
  one node, `multi.rs:273`); the cross-NODE join per group is explicitly called out as
  a follow-up in that method's doc-comment (`multi.rs:270`). `create_group`
  (`multi.rs:300`) already accepts a `peers: BTreeMap<NodeId, BasicNode>` and the
  lowest-id member bootstraps + `initialize(peers)` (`multi.rs:340`), so the wire +
  lifecycle exist; what's missing is a driver that joins the SAME group across multiple
  nodes (add-learner → change-membership) and a harness test proving it.
- **Module/files:** `src/raft/multi.rs` (a `join_group`/`add_group_member` using
  `raft.add_learner` + `raft.change_membership`), plus a multi-node test under
  `src/raft/harness/` or `src/raft/tests.rs`.
- **Dependencies / ordering:** **prerequisite for R1** (and for any real-cluster test of
  R2). Do this FIRST of the three.
- **Parallel-safe?** Touches `src/raft/multi.rs` — **must-sequence** with R1/R2.
- **Hardware:** in-process multi-node harness for correctness; multi-node hardware for a
  true distributed soak.

---

## Suggested ordering for a fresh agent
1. **R3** (membership join) — unblocks everything cluster-shaped. Own `multi.rs`.
2. **R1** (leader balancing) — needs R3. Own `multi.rs` (sequence after R3).
3. **R2** (heartbeat coalescing) — independent logic but shares `network.rs`/`multi.rs`;
   can be developed on its own branch in parallel with R1's design, but **merge
   sequentially** to avoid `multi.rs`/`network.rs` conflicts.

Concept IDs KG-2.268 (leader balancing) and KG-2.269 (heartbeat coalescing) are
**reserved here but not yet implemented** — claim them in the workspace concept ledger
before writing code.
