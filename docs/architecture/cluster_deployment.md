# Multi-node Raft cluster — deployment & data migration (CONCEPT:AU-KG.backend.authority-has-already-acked)

> How to run the epistemic-graph engine as a highly-available **Raft cluster** across
> four runtime-selected nodes, and how to convert the **current authoritative node** (which
> already holds the authoritative redb data) into the SEED of that cluster **without
> data loss**. Built on openraft **0.10** (the v2 split-storage API + native graceful
> `trigger().transfer_leader()` — see `m2_raft_status.md`).
>
> **This document is a runbook. It does NOT perform the live cutover.** The operator
> runs the steps below by hand, with a verified backup in place.

---

## 0. Topology

| Raft node | Runtime host variable | Runtime address variable | Role |
|-----------|-----------------------|--------------------------|------|
| 1         | `NODE_1_HOST`         | `NODE_1_ADDR`            | **SEED** — already holds authoritative data; lowest id ⇒ bootstrap |
| 2         | `NODE_2_HOST`         | `NODE_2_ADDR`            | learner → voter |
| 3         | `NODE_3_HOST`         | `NODE_3_ADDR`            | learner → voter |
| 4         | `NODE_4_HOST`         | `NODE_4_ADDR`            | learner → voter (deployment manager when applicable) |

* **Raft RPC** binds `:9100` on every node (the `EPISTEMIC_GRAPH_RAFT_PEERS` port).
  Peers dial the runtime-injected `NODE_<N>_ADDR` values on `:9100`.
* The engine's **client TCP** must therefore move OFF `:9100` (collision) — the
  `cluster.env` flavor sets `ENGINE_TCP_ADDR=0.0.0.0:9101`; a **co-located graph-os**
  should prefer the **local UDS**. Remote clients dial a member's `:9101` and follow
  the `ForwardToLeader` redirect.
* `EPISTEMIC_GRAPH_RAFT_BIND_ADDR=0.0.0.0:9100` lets a containerized member bind all
  interfaces while still **advertising** its runtime-injected routable address (from
  `PEERS`) to peers.
* Every member receives the same runtime-mounted key through
  `EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE`. The Raft transport authenticates both peer
  ids with a fresh nonce exchange, derives a per-connection key, encrypts frames with
  XChaCha20-Poly1305, and rejects replayed/out-of-order sequence numbers. A routable
  or multi-member cluster refuses to start without key material; plaintext is limited
  to one-member loopback development.

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

1. **Back up node 1's authoritative data.** Stop writers if you can; snapshot the redb
   persist dir (`ENGINE_PERSIST`, configured outside the repository)
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

## 2. Convert the authoritative node into the SEED — no data loss

The key idea: **the authoritative host keeps its redb data and becomes node 1.** The
other nodes start **empty** and replicate the authoritative state from node 1 (via
Raft log + snapshot). You never wipe node 1, and you never let an empty node bootstrap
the cluster.

### 2a. Restart node 1 as a single-member Raft cluster (still authoritative, now HA-ready)

Node 1 boots with `EPISTEMIC_GRAPH_RAFT_NODE_ID=1` and a `PEERS` list. Because node 1 is
the **lowest id**, it is the bootstrap candidate; it `initialize`s the cluster as a
single voter `{1}` over **its existing redb** (the Raft log lives in the SAME
the authoritative shard, keyed by group id — the graph data is untouched). At this
point the cluster is `{1}` and every committed write still lands on node 1's disk exactly
as before.

```bash
# On the runtime-selected deployment manager:
set -a; source services/epistemic-graph/flavors/cluster.env; set +a
export EPISTEMIC_GRAPH_RAFT_NODE_ID=1
export EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE="${RAFT_AUTH_SECRET_FILE:?set a runtime secret-file reference}"
# CONCEPT:EG-KG.sharding.cluster-topology (ADR-1 / W1.1): required once Raft peers are
# configured -- the address this node self-reports so ClusterMembers/
# PlacementRoute.endpoints can hand it to a discovering client, replacing
# the static GRAPH_RAFT_GROUP_ENDPOINTS map. Fails closed (refuses to
# start) without it once EPISTEMIC_GRAPH_RAFT_NODE_ID/_PEERS are set.
export EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR="${NODE_1_CLIENT_ADDR:?e.g. tcp://10.0.0.10:8765}"
export SERVER="${NODE_1_HOST:?set the node 1 host alias at runtime}"
export ENGINE_PERSIST="${ENGINE_PERSIST:?set to node 1's existing data directory}"
docker stack deploy -c services/epistemic-graph/compose.dev.yml epistemic-graph-1
```

**Verify** before going further: the engine is up, it is the leader of the single-member
cluster, and its data is intact (node/edge counts match the pre-cutover baseline). Only
then proceed.

> Why this is safe: `RaftClusterConfig` only lets the lowest-id node bootstrap, and the
> store recovers its applied pointers + graph data from the existing redb. Node 1 is the
> single source of truth throughout.

### 2b. Bring up nodes 2, 3, 4 EMPTY (they will replicate from node 1)

Each joins on a **fresh, empty** persist dir. They do NOT bootstrap (only node 1 can):
they stand up, then wait to be added by the leader.

```bash
# Node 2:
set -a; source services/epistemic-graph/flavors/cluster.env; set +a
export EPISTEMIC_GRAPH_RAFT_NODE_ID=2
export EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE="${RAFT_AUTH_SECRET_FILE:?set the shared runtime secret-file reference}"
export SERVER="${NODE_2_HOST:?set the node 2 host alias at runtime}"
export ENGINE_PERSIST="${ENGINE_PERSIST:?set to node 2's empty data directory}"
docker stack deploy -c services/epistemic-graph/compose.dev.yml epistemic-graph-2
# Repeat for node 3 and node 4, using `NODE_3_HOST` and `NODE_4_HOST`; each gets its own
# empty directory.
```

### 2c. Add nodes 2→4 as learners, then promote to voters (from the node 1 LEADER)

Membership changes are leader-only. The engine exposes this through the `MultiRaft`
membership lifecycle — `add_group_learner` (`add_learner`, blocking-until-caught-up)
then `change_group_voters` (`change_membership`), reachable at runtime as
`Method::RaftAddLearner` / `Method::RaftChangeMembership` (see §5 item 2 below). The
leader replicates the authoritative state to each learner (log tail, or a full
snapshot if the log was purged) BEFORE promoting it, so a node is only made a voter
once it holds the data. Add them **one at a time**, verifying catch-up between each,
so quorum is never at risk:

1. Add node 2 as a learner (`client.raft_admin.add_learner(node_id=2, addr=...)`);
   wait until its applied index matches the leader; promote to voter
   (`client.raft_admin.change_membership(voters=[1, 2])`). Membership becomes
   `{1,2}`.
2. Add node 3 the same way → `{1,2,3}` (now a fault-tolerant majority of 3).
3. Add node 4 → `{1,2,3,4}`.

> Operationally this is driven by the engine's admin surface for membership — the
> `MultiRaft::add_group_learner`/`change_group_voters` path the lib tests
> `multi_node_group_join_then_leader_rebalance` (voter promotion) and
> `raft_add_learner_wire_method_attaches_non_voting_learner` (wire method, non-voting)
> exercise — over `epistemic_graph.client`'s `raft_admin` namespace (§5 item 2). Each
> step is idempotent and refuses to produce an empty voter set.

### 2d. Verify replication, then (optionally) rebalance leadership

* Read a known key back **on a follower** (e.g. node 3) — it must match node 1.
* Write a new key through the leader — it must appear on every follower within a
  heartbeat.
* Leadership starts entirely on node 1 (it bootstrapped). If you want it spread, the
  leader balancer issues the **native** `trigger().transfer_leader(target)` per group
  (graceful, near-instant) toward the deterministic round-robin target. With one group
  this is a no-op worth skipping; it matters once multi-group sharding is enabled.

The cluster is now 4-node HA with node 1's data fully replicated. **No data was lost and
no node was wiped.**

---

## 3. Rollback

At any point before you trust the cluster, you can return to the single-node engine:

1. **Tear down nodes 2–4** (`docker stack rm epistemic-graph-2/3/4`). With them gone the
   cluster loses quorum — that is fine for rollback because node 1 still holds the
   authoritative data on disk.
2. **Restart node 1 WITHOUT the Raft env** (unset `EPISTEMIC_GRAPH_RAFT_NODE_ID`/`PEERS`):
   redeploy the single-node default `compose.dev.yml`. The engine runs single-node over
   the SAME redb exactly as before the migration (the Raft log rows are inert when Raft
   is off).
3. If node 1's data is ever in doubt, **restore the §1 backup** into `ENGINE_PERSIST` and
   restart single-node.

Because node 1 is never wiped and the Raft log shares its redb, rollback is "stop the new
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
* **Security:** Raft and served RPC never run unauthenticated. Every node requires
  the same runtime-provisioned request secret, audience/tenant/policy revision,
  trusted signer registry, and Raft transport key. Routable native client traffic
  uses TLS/mTLS; auxiliary listeners remain loopback-only.

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
2. **Membership admin entrypoint — DONE.** `MultiRaft::add_group_member` was split
   into [`add_group_learner`](../../src/raft/multi.rs) (add-learner only, no
   promotion) + `change_group_voters` (the promotion/rebalance half), both now
   reachable at runtime as `Method::RaftAddLearner` /
   `Method::RaftChangeMembership` (`src/server/handlers/raft_admin.rs`), gated
   `admin:cluster` and leader-only — a follower answers `OPERATION_REDIRECTED`
   naming the current leader, exactly like `PlacementRoute`'s stale-route
   redirect. `remove_group_member` still has no wire Method (out of scope here;
   `RaftChangeMembership` can shrink the voter set as a workaround by omitting a
   node from the requested `voters`, but that is NOT the same safety envelope as
   a dedicated remove entrypoint — flag for a follow-up).

   §2c is now drivable end-to-end without a bespoke binary: bring node 2 up on
   the SAME `EPISTEMIC_GRAPH_RAFT_NODE_ID`/`EPISTEMIC_GRAPH_RAFT_PEERS`/
   `EPISTEMIC_GRAPH_RAFT_AUTH_SECRET_FILE` boot env this doc already documents in
   §2b (its `is_bootstrap` resolves to `false` because its id isn't the lowest in
   `EPISTEMIC_GRAPH_RAFT_PEERS`, so it opens the group WITHOUT `initialize` — i.e.
   it boots straight into "empty, non-bootstrapping, learner-ready" — no new env
   var needed), then from the LEADER:

   ```python
   await client.raft_admin.add_learner(node_id=2, addr=NODE_2_PEER_ADDR)
   # ... verify replication (§2d), then promote:
   await client.raft_admin.change_membership(voters=[1, 2])
   ```

   Repeat per node, one at a time, exactly as §2c already prescribed.

---

**See also:** [Capabilities matrix](../capabilities.md) · [Engine Scaling Program](scaling_program.md) · [Multi-Raft Cluster Status](m2_raft_status.md) · [Deployment (database)](../deployment.md) · [Operations Runbook](../operations/runbook.md).
