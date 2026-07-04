# Correctness + Load Harness (CONCEPT:AU-KG.ontology.emits-database-ontology-entities)

The standing **proof-engine** that gates every distributed / durability claim in the
Waves-4-8 program: multi-Raft (lane A), M2 redb durability, distributed transactions
(lane N), PITR / replication (lane E). The authoritative-flip migration deadlock
proved *correctness-tests-passing ≠ scale/fault-proven*; this harness exists so later
distributed work must survive injected faults, not just pass happy-path unit tests.

It is **test/dev-only** — gated behind the `harness` cargo feature (which pulls
`raft`). A production build (`cargo build`, `--features full`, `--features cluster`) links
**nothing** from it, and the one production-code touch (a partition gate in the Raft
network) is `#[cfg(any(test, feature = "harness"))]`, so the production network path
is byte-for-byte unchanged.

## Where it lives

```
src/raft/harness/
├── mod.rs            # run_gauntlet() — elect → load → fault → heal → assert
├── cluster.rs        # Cluster: managed N-node in-process redb-authoritative raft
│                     #   cluster with kill / restart / partition / read controls
├── nemesis.rs        # Nemesis: the 4 fault primitives, schedulable + seeded
├── loadgen.rs        # concurrent writers → recorded History
├── history.rs        # the (op, ack/err, timestamp) op log
├── checker.rs        # the invariant model over history + final state
└── gauntlet_test.rs  # `cargo test --features harness raft::harness` — the gauntlet
src/bin/nemesis.rs    # `cargo run --features harness --bin nemesis` — the soak runner
src/raft/network.rs   # partition::{partition,isolate,heal,reachable} gate (cfg-gated)
```

## The nemesis (4 fault primitives)

| Fault | What it does | Mechanism |
|-------|--------------|-----------|
| **Kill** (`KillLeader`/`KillNode`) | `kill -9` a node mid-write | `Cluster::kill` drops the node's Raft + redb backend; the on-disk redb is exactly what a crashed process leaves (every acked write was fsynced before the ack). `restart` re-opens over the same files → local log replay (EG-KG.storage.one-fsync-covers-raft) + catch-up. |
| **Partition** | Drop the group-tagged TCP frames between subsets | A process-global `partition` "islands" controller the Raft network's per-RPC `round_trip` consults; a partitioned pair gets a `ConnectionReset` (the same thing a real dropped frame surfaces) → openraft `Unreachable` → backoff. |
| **Skew / Pause** (`Isolate`) | A clock-skewed / GC-paused node stops heartbeating | Modeled as a network isolation window: peers see missed heartbeats → election. (Real monotonic-clock skew of one node is deferred — see below.) |
| **Saturate** | Flood the write channel | The load gen's `rate_per_sec = 0` burst contends for the leader's group-commit + in-flight semaphore; `SaturateWindow` marks the window. |

All randomized faults keep a **quorum** (minority partition / one-node kill), so the
no-loss bar stays meaningful (writes can still commit during the fault).

## The load generator

`loadgen::run` spawns `writers` tasks issuing durable `AddNode` mutations through the
cluster's **current** leader (re-resolved each op, so a failover just reroutes the next
write) at a configurable rate. Each op gets a globally-unique `seq` and is recorded as
an `Op { seq, writer, invoked, completed, outcome: Acked | Err(_) }` in a shared
`History`.

## The checker — invariants asserted

Over the recorded `History` + the cluster's final state (after heal + a stable leader):

1. **Acked-write-survives / no-loss.** Every `seq` the client observed `Acked` is
   present on the final authority (the healed leader). An *acked-then-lost* write is the
   headline durability bug → FAIL loudly.
2. **Single-leader-per-term (no split-brain).** A background sampler records every
   node's `(id, term, is_leader)`; no two distinct nodes may be leader of the *same*
   term.
3. **Monotonic commit / no-phantom.** `acked ⊆ applied ⊆ issued` — the authority holds
   no entry that was never issued, and its applied count never exceeds issued writes.

> Full Knossos-style linearizability (per-key register history + a real-time order
> check) is a **later increment**; this asserts the weaker but high-value set above.

## Running it

```bash
# Bounded, seeded, CI-able gauntlet (partition → heal → kill-leader → assert):
cargo test --features harness --lib raft::harness

# Longer seeded soak (random quorum-preserving faults, N rounds, fail-fast):
cargo run --features harness --bin nemesis -- --nodes 3 --rounds 20 --seed 42
```

## How a future distributed lane plugs in

* **A new invariant** (lane N cross-shard atomicity; lane E "PITR recovers to T"):
  add a check arm in `checker::check` reading the relevant final state, plus a run-time
  accumulator like `checker::LeadershipLog` if it needs samples. The load gen already
  records a full timestamped history to assert against.
* **A new nemesis** (lane resharding: split a group mid-write; lane E: kill a
  replication follower): add a `nemesis::Fault` variant + an `inject` arm. The
  partition gate and the kill/restart cluster controls are reusable primitives.
* **A new gauntlet**: compose `Cluster::start` → `loadgen::run` → `Nemesis::inject` →
  `checker::check` exactly as `run_gauntlet` does, with the new fault schedule.

## Deliberately deferred (documented)

* **Full linearizability** (Knossos-style real-time-order model checker).
* **Real monotonic-clock skew** of one node (modeled today as network isolation —
  the observable effect on peers is identical: missed heartbeats).
* **Multi-process** (not in-process) nodes — a true `kill -9` of a separate OS process.
  The in-process kill drops the node's Raft + redb backend, leaving the on-disk redb a
  crash would leave (durability is the property under test, and that holds).

## Production-isolation guarantee

`cargo tree -e features -i openraft` (default build) etc. and the harness feature being
absent from the main/`cluster` builds keep the harness out of shipped builds. The one
production-source touch — `GroupNetworkClient::round_trip`'s partition check — is
`#[cfg(any(test, feature = "harness"))]`; in a production build it is not compiled, so
the Raft network path is unchanged.
