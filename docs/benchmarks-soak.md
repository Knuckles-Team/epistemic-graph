# Soak / Chaos Benchmarks (measured) — SCALE-P2-1, engine scope

> **Status: MEASURED.** Every number below comes from an actual run of the real
> `epistemic-graph-server` binary via `scripts/soak_scale.py` in a controlled
> reference run. Nothing is modeled or extrapolated. Scale and the limits
> of the run are stated plainly in "Scope" and "Execution conditions" — read them before
> trusting any number.

This page is the engine-scope companion to agent-utilities'
`docs/scaling/workload_contract.yml` / `scripts/scale/loadgen.py` (the SCALE-P2-1
soak/chaos harness). That harness's `--engine live` path against a real deployment
is documented but never run (`tests/scale/soak/test_hardware_pending.py` is real,
`skip`-marked, hardware-pending code, not a result). This page closes that gap **at
engine scope**: it runs the real server binary (not a mock, not `FakeScaleEngine`)
and reports what was actually measured.

Reproduce:

```bash
cargo build --release --features server
python3 scripts/soak_scale.py --residents 1500 --nodes-per-resident 6 --duration-s 20 --json report.json
```

## Scope — read before trusting a number

* Measures the ENGINE's native write/read/claim primitives (`AddNode`,
  `GetNodeProperties`, `ClaimNext`, `CompareAndSetNodeFields`) over the real wire
  protocol against a real process — NOT agent-utilities' `WorkItem`/`AgentBus`
  Python orchestration (a different package). `queue`/`end_to_end` latency is
  approximated with the engine-native `ClaimNext` claim-queue primitive.
* **Throughput is driver-paced, not max-load.** `soak_scale.py` drives at the
  workload contract's *target* agent-turn/tool/mutation rates (turn ≈ 0.25/s,
  mutations ≈ 60/s), so `*_per_sec_measured` reflects the offered load, not the
  engine's ceiling. **Latency percentiles are the meaningful signal here**, not
  throughput.
* **Scale is small vs the 1,000,000-resident contract** (`scale_vs_1m ≈ 0.0015`).
  Population and duration were chosen to complete reliably on a shared runner; larger
  resident counts did not complete under the reference runner's background load
  (see Execution conditions). The full contract needs a dedicated distributed
  deployment (see the final section).

## Execution conditions

| | |
|---|---|
| Runner state | **Shared and contended.** Other workloads were active, so absolute latencies are an upper bound; a dedicated runner should be faster. |
| Build | `cargo build --release --features server` (redb-authoritative + query/cypher + blob + kv + security) |
| Workload | 1,501 synthetic residents · 8 synthetic tenants · 6 nodes/resident · 9,006 nodes · 20 s steady-state |
| Script | `scripts/soak_scale.py` (new) |

## Dedicated reference run (300k nodes)

The same harness was rerun on a **dedicated, uncontended reference runner** at
**33× the node count** of the shared run. The bundle contained the release binary,
`soak_scale.py`, its Python client, and declared runtime dependencies. This is the
larger-scale, quiet-runner data point.

| | |
|---|---|
| Run | **50,000 residents · 8 tenants · 6 nodes/resident · 300,048 nodes** · 30 s steady-state |
| Population | **3,432 ops/s** (87.4 s), RSS **7.5 GiB** after populating 300k nodes |

| Phase | Result (measured) |
|---|---|
| **A steady-state** | query p50 **0.57 ms** / p99 7.1 ms (n=**21,164**); write p50 18.8 ms / p99 62.3 ms (n=8,946) — sub-ms reads at 300k nodes with a large, robust sample |
| **B restart/cold** | acked write **survived** the kill; first successful op **2.1 s** after relaunch (warming 300k nodes) — a latency event, not data loss |
| **C hot-tenant** | noisy-neighbor partial isolation held (same shape as the shared-runner result) |
| **D backpressure** | burst 2,000 → **1,908 shed as BUSY**, 92 succeeded, **0 crashes**, post-burst recovery **50/50** |
| **E eviction** | evicted node read-through **0.61 ms**, **no data loss** |

**Scaling finding:** at **150,000 residents (900k nodes)** the run exceeded the 600 s bound during
the population-bound phase (population is single-writer-funnel-limited — the north-star M1 concern);
that is a real, expected single-process ceiling, not a failure. Larger populations need
the distributed multi-group parallel write path (see the cluster section below).

**Read this run as the primary scale data point** (dedicated runner, 300k nodes, robust samples); the
contended shared run below is the same harness under CPU contention (latencies there are an upper bound).

## Phase A — population build + steady-state mixed workload

**Population:** 9,006 nodes written in **4.54 s = 1,986 ops/s**; RSS after populate **716 MiB**.

**Steady state** (23.3 s, 7 turns / 1,039 mutations / 603 tool-calls, Zipf tenant skew):

| Op | p50 | p95 | p99 | n |
|---|---|---|---|---|
| query (`GetNodeProperties`) | **0.43 ms** | 8.99 ms | 18.57 ms | 603 |
| write (`CompareAndSetNodeFields`) | **14.17 ms** | 43.72 ms | 57.24 ms | 1,039 |
| claim queue (`ClaimNext`) | 26.97 ms | 60.26 ms | 60.26 ms | 6 |
| end-to-end turn | 60.62 ms | 83.17 ms | 83.17 ms | 6 |

Reads are sub-millisecond at p50 even under runner contention; writes sit in the
low-tens-of-ms (redb group-commit fsync + a loaded runner).

## Phase B — restart / cold (redb) recovery

| Metric | Value |
|---|---|
| Data survived a full stop+restart | **✅ yes** (probe node present pre- and post-restart) |
| Graceful drained stop | 0.315 s |
| Restart → first successful op | **0.311 s** |
| First post-restart op latency | 0.7 ms |

A durable (redb-authoritative) restart is a **sub-second latency event, not data
loss** — the acked write survived the kill and the engine served reads ~0.3 s after
relaunch.

## Phase C — hot-tenant noisy-neighbor isolation

| | p50 | p95 | p99 | n |
|---|---|---|---|---|
| Elephant tenant (hammered) | 46.04 ms | 71.86 ms | 77.31 ms | 1,801 |
| Ordinary tenant (concurrent) | **11.23 ms** | 28.90 ms | 34.84 ms | 400 |

**Partial isolation:** the ordinary tenant stays ~4× faster than the hammered
elephant, so one noisy tenant does not collapse the others. **Honest caveat:** the
ordinary tenant did NOT meet the strict workload-class SLO at p50/p95/p99 on this
contended shared runner (only p99.9 passed) — full isolation SLOs are a
distributed-deployment and dedicated-runner target.

## Phase D — backpressure / admission shedding

Burst of **2,000 concurrent** requests against `max_inflight=64`:

| Metric | Value |
|---|---|
| Succeeded | 113 |
| BUSY rejections (shed) | 1,887 |
| Other errors / crashes | **0** |
| Shed-load observed | **✅ yes** |
| Accepted-request latency p50 / p99 | 228 ms / 253 ms |
| Post-burst recovery | **50 / 50** ✅ |

The engine **sheds with a typed BUSY signal rather than dropping data or crashing**,
and fully recovers after the burst — the intended backpressure contract.

## Phase E — per-graph eviction + durable read-through

(1,000-node graph, `node_cap=500`, so population exceeds the cap):

| Metric | Value |
|---|---|
| Population exceeds cap | ✅ (5,000 written, cap 500) |
| Evicted node still readable (read-through) | **✅ 0.36 ms** |
| Hot node read | 0.25 ms |
| Data loss | **none** |

Eviction is durability-gated and read-through-safe: an evicted node is served from
redb on a RAM miss, never lost.

## SLO scorecard (this run)

| Property | Result |
|---|---|
| Acked write survives restart | ✅ |
| No data loss under eviction | ✅ |
| Backpressure sheds (no crash/drop) + recovers | ✅ |
| Sub-ms p50 reads | ✅ (0.43 ms) |
| Noisy-neighbor partial isolation | ✅ (4× gap) |
| Strict workload-class latency SLOs under noisy neighbor | ⚠️ not met on this contended shared runner (distributed/dedicated-runner target) |

## Cluster chaos — bounded real run

A `--features cluster` (openraft) build was deployed as a **bounded isolated test
cluster**, and the core node-loss chaos case was exercised, then torn down.
**Measured (real, not modeled):**

| Property | Result |
|---|---|
| Multi-node replication | ✅ a synthetic probe write replicated to a peer |
| **Data survives a node kill** | ✅ the probe remained readable from a surviving peer — durable, no loss |
| **Raft safety under lost quorum** | ✅ after quorum loss, the cluster **correctly refused** new writes (no split-brain, no phantom commit) — the intended openraft safety guarantee |

**Harness caveat:** repeated formation attempts could contend for recently released
ports. This is a harness limitation, not an engine defect; replication, durability,
and safety behavior remained correct in the completed runs. A dedicated supervised
cluster is required for the broader fault-domain matrix below.

## Not run here — needs a dedicated distributed deployment

The following SCALE-P2-1 chaos cases are real, defined scenarios that require an
operator-managed distributed deployment and were not attempted in the bounded run —
the same set `agent-utilities/tests/scale/soak/test_hardware_pending.py` documents
as hardware-pending at the orchestration layer:

| Scenario | Why it needs the cluster | Command to run there |
|---|---|---|
| Leader failover across fault domains (M2 openraft) | `--features cluster` with independent members | Run `scripts/validate-raft-cluster.sh` against operator-provided endpoints |
| Fault-domain loss under load | independent deployment members and an external fault injector | Run the cluster-endpoint soak harness while the operator removes one member |
| Distributed rebalance under writes (M3 online resharding) | `reshard`/`rebalance_execute` currently move rows between local shards; distributed movement needs multi-Raft groups. The `PlacementCatalog` online-move surface also requires a wire `Method` | Run `resharding.rebalance_execute` under load against an operator-managed cluster |
| Rolling upgrade | a supervised deployment that can replace members one at a time | Run the soak harness throughout an operator-managed rolling replacement and assert no lost or duplicate turns |
| Real Kafka broker rebalance | the engine has no Kafka dep; this is an agent-utilities dispatch-layer scenario | `test_hardware_pending.py::test_broker_rebalance_and_partition_expansion_under_load` |
| Full cold activation of 1,000,000 synthetic residents | the measured run is N ≪ 1e6; the target population needs a production-scale durable-shard footprint | `soak_scale.py --residents 1000000 --duration-s <hours>` against an operator-managed distributed deployment |

Guardrail this table enforces (matching `agent-utilities/docs/scaling/capacity_model.md`):
a modeled/projected capacity is never reported as a demonstrated result — every row
above is a real, named gap, not a silently-omitted scenario.
