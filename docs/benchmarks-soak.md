# Soak / Chaos Benchmarks (measured) — SCALE-P2-1, engine scope

> **Status: MEASURED.** Every number below comes from an actual run of the real
> `epistemic-graph-server` binary via `scripts/soak_scale.py` in this session
> (2026-07-11). Nothing is modeled or extrapolated. Scale and the honest limits
> of *this* run are stated plainly in "Scope" and "Environment" — read them before
> trusting any number.

This page is the honest, single-box companion to agent-utilities'
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
  Population/duration were chosen to complete reliably on ONE shared box; larger
  resident counts did not complete under this box's background load (see
  Environment). The full contract needs the 4-node cluster (see last section).

## Environment

| | |
|---|---|
| Host | 24-core x86-64, 247 GiB RAM — **single box, NOT the 4-node cluster** |
| Box state | **SHARED + contended** during the run: load average ~12–17 (the live graph-os fleet, a concurrent k8s-repair session, and other engine processes were resident). Absolute latencies are therefore an *upper bound* — a quiet box would be faster. |
| Build | `cargo build --release --features server` (redb-authoritative + query/cypher + blob + kv + security) |
| Run | 2026-07-11T19:47–19:49Z · 1,501 residents · 8 tenants · 6 nodes/resident · 9,006 nodes · 20 s steady-state |
| Script | `scripts/soak_scale.py` (new) |

## Phase A — population build + steady-state mixed workload

**Population:** 9,006 nodes written in **4.54 s = 1,986 ops/s**; RSS after populate **716 MiB**.

**Steady state** (23.3 s, 7 turns / 1,039 mutations / 603 tool-calls, Zipf tenant skew):

| Op | p50 | p95 | p99 | n |
|---|---|---|---|---|
| query (`GetNodeProperties`) | **0.43 ms** | 8.99 ms | 18.57 ms | 603 |
| write (`CompareAndSetNodeFields`) | **14.17 ms** | 43.72 ms | 57.24 ms | 1,039 |
| claim queue (`ClaimNext`) | 26.97 ms | 60.26 ms | 60.26 ms | 6 |
| end-to-end turn | 60.62 ms | 83.17 ms | 83.17 ms | 6 |

Reads are sub-millisecond at p50 even under box contention; writes sit in the
low-tens-of-ms (redb group-commit fsync + a loaded box).

## Phase B — restart / cold (redb) recovery

| Metric | Value |
|---|---|
| Data survived a full stop+restart | **✅ yes** (probe node present pre- and post-restart) |
| Graceful checkpointed stop | 0.315 s |
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
ordinary tenant did NOT meet the strict per-tier SLO at p50/p95/p99 on this
contended single box (only p99.9 passed) — full isolation SLOs are a
cluster + quiet-box target.

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
| Strict per-tier latency SLOs under noisy neighbor | ⚠️ not met on this contended single box (cluster/quiet-box target) |

## NOT run here — needs the 4-node cluster

The following SCALE-P2-1 chaos cases are real, defined scenarios that **require
multi-node/multi-zone hardware** and were **not** attempted on this single box —
the same set `agent-utilities/tests/scale/soak/test_hardware_pending.py` documents
as hardware-pending at the orchestration layer:

| Scenario | Why it needs the cluster | Command to run there |
|---|---|---|
| Leader failover across hosts (M2 openraft) | `--features cluster` + ≥3 real nodes; a single box has no node to fail over to | `scripts/validate-raft-cluster.sh` on the 4-node cluster |
| Zone/node loss under load | real independent hosts on separate power/network to kill | `soak_scale.py` (cluster-endpoint variant, TBD) vs the cluster while k8s/`container-manager-mcp` kills a node |
| Cross-host rebalance under writes (M3 online resharding across nodes) | `reshard`/`rebalance_execute` move rows between shards on ONE node today; cross-**node** needs multi-Raft groups on real hosts. **Note:** the multi-node `PlacementCatalog` online-move (`plan_start_move`/`fence_cutover`) also has **no wire `Method` yet** — a real engine-side gap surfaced by Seam 4 | bring up `--features cluster` on ≥3 hosts, drive `resharding.rebalance_execute` under load |
| Rolling upgrade across real hosts | real multi-host rolling-deploy (old+new binary coexisting) | k8s rolling-update of `epistemic-graph-server` across the 4 nodes, `soak_scale.py` throughout, assert no lost/dup turns |
| Real Kafka broker rebalance | the engine has no Kafka dep; this is an agent-utilities dispatch-layer scenario | `test_hardware_pending.py::test_broker_rebalance_and_partition_expansion_under_load` |
| Full cold activation of the REAL 1,000,000 residents | this box's run is N ≪ 1e6; the real population needs the real durable-shard footprint | `soak_scale.py --residents 1000000 --duration-s <hours>` vs the 4-node cluster with real durable shards |

Guardrail this table enforces (matching `agent-utilities/docs/scaling/capacity_model.md`):
a modeled/projected capacity is never reported as a demonstrated result — every row
above is a real, named gap, not a silently-omitted scenario.
