# Multi-node Raft cluster — deployment & data migration (CONCEPT:AU-KG.backend.authority-has-already-acked)

> How to run the epistemic-graph engine as a highly-available **Raft cluster** across
> the fleet's 4 nodes, and how to convert the **live single-node R510 engine** (which
> already holds the authoritative redb data) into the SEED of that cluster **without
> data loss**. Built on openraft **0.10** (the v2 split-storage API + native graceful
> `trigger().transfer_leader()` — see `m2_raft_status.md`).
>
> **This document is a runbook. It does NOT perform the live cutover.** The operator
> runs the steps below by hand, with a verified backup in place.

---

## 0. Topology

| Raft node | Host  | IP          | Role                                   |
|-----------|-------|-------------|----------------------------------------|
| 1         | R510  | 10.0.0.10   | **SEED** — already holds authoritative data; lowest id ⇒ bootstrap |
| 2         | R710  | 10.0.0.11   | learner → voter                        |
| 3         | RW710 | 10.0.0.12   | learner → voter                        |
| 4         | R820  | 10.0.0.13   | learner → voter (Swarm manager)        |

* **Raft RPC** binds `:9100` on every node (the `EPISTEMIC_GRAPH_RAFT_PEERS` port).
  Peers dial each other at `10.0.0.x:9100`.
* The engine's **client TCP** must therefore move OFF `:9100` (collision) — the
  `cluster.env` flavor sets `ENGINE_TCP_ADDR=0.0.0.0:9101`; a **co-located graph-os**
  should prefer the **local UDS**. Remote clients dial a member's `:9101` and follow
  the `ForwardToLeader` redirect.
* `EPISTEMIC_GRAPH_RAFT_BIND_ADDR=0.0.0.0:9100` lets a containerized member bind all
  interfaces while still **advertising** its routable host IP (from `PEERS`) to peers.

### Writer model — K=1 under an active Raft node

With Raft active, **all durable writes for a graph route through that graph's group
LEADER** before they are acked (consensus is the replication barrier). Today every
graph maps to the single `DEFAULT_GROUP`, so the cluster is **HA, not write-scaling**:
the writer is **K=1** (one serialized write path). Splitting the keyspace into many
write groups (multi-Raft sharding, `GroupRouter` ring — EG-KG.sharding.raft-resharding/2.266) is a SEPARATE
effort and is **off** in this deployment. Read that as: a 4-node cluster buys you
*survivability of a node loss*, not 4× write throughput.

---

## 1. Pre-flight (do these BEFORE touching anything)

1. **Back up R510's authoritative data.** Stop writers if you can; snapshot the redb
   persist dir (`ENGINE_PERSIST`, e.g. `/home/genius/epistemic-graph/graph_snapshots`)
   — a filesystem copy of the closed `*.redb` files, or a borg/zfs snapshot. **Verify
   the backup restores** before proceeding. This is the rollback anchor.
2. **Build the cluster-capable engine.** The cluster image/binary must be built with
   `--features "full,cluster"` (it links openraft; a default build does NOT). Stage it
   on every node (`ENGINE_BIN` for the dev/source-mounted flavor, or bake it into the
   registry image for prod).
3. **Open `:9100` between the four hosts** (Raft RPC) on the trusted LAN.
4. **Confirm clocks are sane** (Raft leases tolerate drift but don't abuse it; run NTP).
5. **Decide the client path.** If graph-os is co-located on each node, it uses the local
   UDS and needs no change. If clients dial the engine over TCP, repoint them at a
   member's `:9101` (see `GRAPH_SERVICE_TCP_ADDR` in `cluster.env`).

---

## 2. Convert R510 (live single-node) into the SEED — no data loss

The key idea: **R510 keeps its redb data and becomes node 1.** The other nodes start
**empty** and replicate the authoritative state FROM R510 (via Raft log + snapshot).
You never wipe R510, and you never let an empty node bootstrap the cluster.

### 2a. Restart R510 as a single-member Raft cluster (still authoritative, now HA-ready)

R510 boots with `EPISTEMIC_GRAPH_RAFT_NODE_ID=1` and a `PEERS` list. Because node 1 is
the **lowest id**, it is the bootstrap candidate; it `initialize`s the cluster as a
single voter `{1}` over **its existing redb** (the Raft log lives in the SAME
`graph.redb`, keyed by group id — the authoritative graph data is untouched). At this
point the cluster is `{1}` and every committed write still lands on R510's disk exactly
as before.

```bash
# On the Swarm manager (R820):
set -a; source services/epistemic-graph/flavors/cluster.env; set +a
export EPISTEMIC_GRAPH_RAFT_NODE_ID=1
export SERVER=R510
export ENGINE_PERSIST=/home/genius/epistemic-graph/graph_snapshots   # R510's EXISTING data
docker stack deploy -c services/epistemic-graph/compose.dev.yml epistemic-graph-1
```

**Verify** before going further: the engine is up, it is the leader of the single-member
cluster, and its data is intact (node/edge counts match the pre-cutover baseline). Only
then proceed.

> Why this is safe: `RaftClusterConfig` only lets the lowest-id node bootstrap, and the
> store recovers its applied pointers + graph data from the existing redb. R510 is the
> single source of truth throughout.

### 2b. Bring up nodes 2, 3, 4 EMPTY (they will replicate from R510)

Each joins on a **fresh, empty** persist dir. They do NOT bootstrap (only node 1 can):
they stand up, then wait to be added by the leader.

```bash
# Node 2 (R710):
set -a; source services/epistemic-graph/flavors/cluster.env; set +a
export EPISTEMIC_GRAPH_RAFT_NODE_ID=2
export SERVER=R710
export ENGINE_PERSIST=/home/genius/epistemic-graph/graph_snapshots   # MUST be empty on R710
docker stack deploy -c services/epistemic-graph/compose.dev.yml epistemic-graph-2
# Repeat for node 3 (RW710, id=3) and node 4 (R820, id=4), each with its own empty dir.
```

### 2c. Add nodes 2→4 as learners, then promote to voters (from the LEADER, R510)

Membership changes are leader-only. The engine exposes this through the `MultiRaft`
membership lifecycle (`add_group_member` = `add_learner` blocking-until-caught-up →
`change_membership`). The leader replicates the authoritative state to each learner
(log tail, or a full snapshot if the log was purged) BEFORE promoting it, so a node is
only made a voter once it holds the data. Add them **one at a time**, verifying
catch-up between each, so quorum is never at risk:

1. Add node 2 as a learner; wait until its applied index matches the leader; promote to
   voter. Membership becomes `{1,2}`.
2. Add node 3 the same way → `{1,2,3}` (now a fault-tolerant majority of 3).
3. Add node 4 → `{1,2,3,4}`.

> Operationally this is driven by the engine's admin surface for membership (the
> `MultiRaft::add_group_member` path the lib test `multi_node_group_join_then_leader_rebalance`
> exercises). Wire it to your ops entrypoint, or drive it via a one-shot admin call on
> the leader. Each step is idempotent and refuses to remove the last voter.

### 2d. Verify replication, then (optionally) rebalance leadership

* Read a known key back **on a follower** (e.g. node 3) — it must match R510.
* Write a new key through the leader — it must appear on every follower within a
  heartbeat.
* Leadership starts entirely on node 1 (it bootstrapped). If you want it spread, the
  leader balancer issues the **native** `trigger().transfer_leader(target)` per group
  (graceful, near-instant) toward the deterministic round-robin target. With one group
  this is a no-op worth skipping; it matters once multi-group sharding is enabled.

The cluster is now 4-node HA with R510's data fully replicated. **No data was lost and
no node was wiped.**

---

## 3. Rollback

At any point before you trust the cluster, you can return to the single-node engine:

1. **Tear down nodes 2–4** (`docker stack rm epistemic-graph-2/3/4`). With them gone the
   cluster loses quorum — that is fine for rollback because R510 still holds the
   authoritative data on disk.
2. **Restart R510 WITHOUT the Raft env** (unset `EPISTEMIC_GRAPH_RAFT_NODE_ID`/`PEERS`):
   redeploy the single-node default `compose.dev.yml`. The engine runs single-node over
   the SAME redb exactly as before the migration (the Raft log rows are inert when Raft
   is off).
3. If R510's data is ever in doubt, **restore the §1 backup** into `ENGINE_PERSIST` and
   restart single-node.

Because R510 is never wiped and the Raft log shares its redb, rollback is "stop the new
nodes, drop the Raft env" — no data reconstruction needed.

---

## 4. Operational notes

* **Quorum:** 4 voters tolerate **1** failure (majority = 3). For 2-failure tolerance
  you'd want 5 voters; with 4, prefer keeping all 4 healthy and treat a node loss as a
  page, not a shrug. (A 3-voter set tolerating 1 failure is the classic sweet spot; node
  4 here adds a replica + a manager-co-located voter, not extra fault tolerance.)
* **A restarted node self-heals from its own redb** (durable log tail) and catches the
  rest from the leader — it does not need a wipe.
* **Snapshots are per-group and tenant-scoped**, so a large tenant never bloats another
  group's snapshot transfer.
* **Security:** Raft RPC on `:9100` is unauthenticated on the trusted LAN (matching the
  homelab `--allow-insecure` posture). To require auth, set `GRAPH_SERVICE_AUTH_SECRET`
  identically on every node and drop `--allow-insecure`.

---

## 5. Integration follow-ups (NOT done here — flagged for the dispatch owner)

The write path that routes a durable mutation through `RaftHandle::client_write` on the
leader (and the follower `ForwardToLeader` redirect) lives in `src/server/dispatch.rs`,
which is owned by a sibling change — **out of scope for this branch**. Two items to
confirm there before declaring the cluster production-live:

1. **Leader-routing on the dispatch hot path:** when `ServerState.raft` is `Some`, a
   durable write must go through `client_write` (consensus barrier) and a follower must
   surface the leader hint so the client retries against the leader (or the co-located
   graph-os transparently forwards). Verify this is wired for the 0.10 `client_write`
   return/`ForwardToLeader` shape.
2. **Membership admin entrypoint:** expose `MultiRaft::add_group_member` /
   `remove_group_member` on an ops surface so §2c can be driven without a bespoke
   binary. The methods exist (`src/raft/multi.rs`); they just need a caller.

---

**See also:** [Capabilities matrix](../capabilities.md) · [Engine Scaling Program](scaling_program.md) · [Multi-Raft Cluster Status](m2_raft_status.md) · [Deployment (database)](../deployment.md) · [Operations Runbook](../operations/runbook.md).
