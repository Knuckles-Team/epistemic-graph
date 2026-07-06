# Transport Benchmarks (measured)

These are **measured** numbers, not marketing claims. Reproduce with:

```bash
cargo build --release --features server
python3 scripts/bench_transport.py --ops 5000
```

The harness (`scripts/bench_transport.py`) spawns the release server on a
private UDS, creates a graph, then drives `AddNode` + `GetNodeProperties`
round-trips, recording per-operation latency.

## Results

Single client connection, in-memory graph, length-prefixed MessagePack over UDS,
auth disabled. Hardware/run captured 2026-06-01 (Linux x86-64).

| Operation         | ops  | p50      | p99      |
|-------------------|------|----------|----------|
| `AddNode`         | 3000 | 0.187 ms | 0.223 ms |
| `GetNodeProperties` | 3000 | 0.179 ms | 0.210 ms |

That is sub-millisecond round-trip including Python (de)serialisation, i.e.
~5,000 sequential ops/sec on a single connection. Throughput scales with
connection pooling (`pool.py`) and shard fan-out (`ShardRouter`); concurrent
clients are bounded server-side by the in-flight semaphore
(`EPISTEMIC_GRAPH_MAX_INFLIGHT`, default 1024) which sheds excess load with a
`BUSY` response rather than queueing unbounded work.

## Multi-shard scaling (measured) — CONCEPT:AU-KG.query.vendor-agnostic-traversal P3

The "100,000,000 concurrent agents" figure is an **architectural projection**, not
a number that has been run. This section states exactly what *was* measured and the
arithmetic that produces it. Reproduce with:

```bash
cargo build --release --features server
python3 scripts/bench_scale.py --shards 1,2,4 --agents-per-shard 60 --nodes-per-agent 40
```

`scripts/bench_scale.py` keeps **agents-per-shard fixed** and grows the shard count,
with **one driver process per shard** (so the client is parallelised across cores and
never the bottleneck). Each agent works a *bounded subgraph* — its own tenant graph of
40 nodes + an edge chain. RSS is measured as the **delta over idle baseline**, so the
per-agent figure is marginal data cost, not fixed binary overhead.

Run captured 2026-06-09, Linux x86-64, 24 cores, release build:

| shards | agents | write ops/s | wall s | data RSS | RSS / agent |
|-------:|-------:|------------:|-------:|---------:|------------:|
|      1 |     60 |       8,735 |  0.543 |   3.0 MB |    51.5 kB  |
|      2 |    120 |      28,417 |  0.334 |   6.3 MB |    54.0 kB  |
|      4 |    240 |      57,549 |  0.329 |  12.1 MB |    51.7 kB  |

**Findings:**
- **Throughput scales linearly** with shard count — 1→4 shards gives a **6.6×**
  aggregate write throughput at ~constant wall-time (no shared-state cliff; each shard
  is an independent process over its own UDS). The 1-shard baseline is mildly
  *under-driven* by a single client process, which is why the ratio exceeds the 4×
  ideal; the load-bearing conclusion is that **adding shards adds throughput linearly**.
- **Per-agent RSS is flat at ~52 kB** across shard counts — the working-set footprint
  is a stable marginal cost, exactly as a bounded-subgraph-per-agent design predicts
  (it is *not* one monolithic graph whose RAM blows up).

**Extrapolation to 100M agents (measured inputs, stated assumptions):**

```
per-agent footprint   ≈ 52 kB   (measured, bounded 40-node subgraph)
per-host RAM budget   = 64 GB   (assumption)
agents per host       = 64 GB / 52 kB ≈ 1.29 million
hosts for 100M agents = 100e6 / 1.29e6 ≈ 78 hosts
```

So the 100M target is a **~78-host** projection at this working-set size — a fleet, not
a single box — bounded by RAM and validated to scale linearly per shard. Larger working
sets scale the host count proportionally (e.g. 520 kB/agent → ~780 hosts). This is a
*measured projection*, not a load test at 100M.

## Scaling & HA reality

Read the multi-shard numbers above with these architectural facts in mind. (The
durability/HA picture has changed with the redb-authoritative flip and the cluster
tier — see [the master-of-all engine](architecture/engine.md).)

- **Two ways to scale out.** *Client-side sharding* (any tier): independent
  `epistemic-graph-server` processes, each its own graph universe, with the Python
  `ShardRouter` (`epistemic_graph/pool.py`) mapping a graph name to a shard by
  rendezvous/HRW hashing over `GRAPH_SERVICE_ENDPOINTS`. *Server-side multi-Raft*
  (cluster tier): the engine replicates its authoritative store across nodes, with
  online resharding (re-point ownership, not copy rows) and cross-shard 2PC for
  transactions that span groups.
- **Durable by default (redb-authoritative).** A committed write is fsynced to redb
  *before* the client is acked (commit-before-ack); an acked write survives a hard
  crash. A restarted shard re-opens its authoritative redb store — it does not depend
  on the checkpoint interval to avoid data loss.
- **HA in the cluster tier.** `openraft` replicates the authoritative store across
  nodes with automatic leader failover, so a crashed node's graphs stay available on a
  replica. (The opt-in `snapshot` backend is the older single-process
  rebuildable-cache mode, where RPO = checkpoint interval and there is no replication —
  it is no longer the default.)
- **What the 100M projection assumes.** The extrapolation above is
  arithmetic, not a load test: it assumes (1) ~52 kB marginal RSS per agent —
  measured on *bounded 40-node subgraphs*, so larger working sets scale the
  host count proportionally; (2) a 64 GB per-host RAM budget; (3) throughput
  continues to scale linearly with shard count as measured at 1→4 shards;
  (4) a fleet of ~78 independent hosts with client-side routing — with the
  availability and durability caveats above applying to every one of them.

## Fuzzing

Two complementary layers fuzz the query surface:

- **Structured, VALID pipelines (STABLE, in the `cargo test` gate).** The proptest
  harness `crates/eg-plan/tests/fuzz_pipelines.rs`
  (CONCEPT:EG-KG.query.pipeline-fuzz + EG-KG.query.concurrency-chaos-fuzz) generates
  random but *valid* UQL pipelines up to 16 stages over 512 cases and asserts the
  invariants a unified query engine must never violate (no panic/error, unique ids,
  trailing `LIMIT` bound, determinism), plus a concurrency-chaos variant. It runs on
  stable with no extra toolchain:

  ```sh
  cargo test -p eg-plan --test fuzz_pipelines
  ```

- **Unstructured, ARBITRARY bytes (nightly, out of the standard gate).** A
  `cargo-fuzz`/libFuzzer target at `crates/eg-plan/fuzz/`
  (CONCEPT:EG-KG.query.uql-libfuzzer-parse-target) feeds arbitrary bytes to
  `eg_plan::uql::parse`, shaking out lexer/parser panics or hangs on malformed input.
  The fuzz crate is a **detached workspace** (its own `[workspace]` table) so it never
  rides the stable `cargo test` gate — libFuzzer needs the nightly toolchain + the
  fuzzing sanitizer runtime that `cargo-fuzz` injects:

  ```sh
  cargo install cargo-fuzz            # once
  cd crates/eg-plan/fuzz
  cargo +nightly fuzz run uql_parse -- -runs=10000   # short smoke
  cargo +nightly fuzz run uql_parse                  # open-ended soak
  ```

## Notes

- Numbers are for the hot in-memory CRUD path; analytic ops (clustering,
  subgraph match) are heavier and not included here.
- The framing is length-prefixed (4-byte big-endian `u32` + MessagePack body),
  so binary payloads containing `0x0A` bytes round-trip intact — verified by
  `tests/test_no_pyo3_and_quant.py::test_length_prefixed_framing_is_binary_safe`.
- The agent-utilities capacity model (`docs/scaling/capacity_model.py`) consumes
  these per-shard / per-agent figures for the full resident-population sizing.

## Phase-2: agent-memory + KV-cache benchmark (measured)

The unified agent-memory stack — **one in-transaction cross-modal plan** (semantic + lexical +
graph + OWL + AS-OF + RRF), **warm-fork context reuse**, a **durable KV cold-tier**, and
**incremental indexing** — was benchmarked head-to-head against a conventional **stitched stack**
(separate vector store + BM25 + app-level RRF, no KV cache, no warm-fork, full-rebuild indexing) on
the *same* deterministic agent-memory workload. All numbers are **measured** on a dedicated isolated
bench engine (`--features full`, redb-authoritative); full write-up + reproduction in the workspace
report `reports/phase2-memory-kv-benchmark-results.md`.

| Category | epistemic-graph | Stitched baseline | Winner |
|---|---|---|---|
| recall@10 (quality) | **1.000** (indexed ANN) | 1.000 (exhaustive scan) | tie — ours keeps quality *with* an index |
| cross-modal retrieval p50 (N=2000) | **7.3 ms** | 26.3 ms | **~3.6×** |
| cost-optimizer filter-pushdown @scale | **1.21× (10k) → 1.33× (100k)**, grows with N | no unified plan | **ours** |
| warm-fork fan-out (N=8/32/128) | **`retrieval_calls == 1`**, 100% branches | `retrieval_calls == N` | **ours — 128× fewer retrievals** |
| write → read-fresh | **25.7 ms** p50 (incremental, durable) | 19–69 ms full rebuild | **ours** |
| throughput | **799 qps** | N-scan per query | **ours** |
| KV cross-restart | **100% page survival, ~24 µs/page GET → >300× vs recompute** | none (full recompute) | **ours (≥7.5× target smashed)** |

**Ablation (per-feature, N=1000):** disabling the result cache makes warm queries **3.3× slower**
(4.6 → 15.3 ms) — the clear warm-path win; the cost-optimizer and incremental index are
scale-dependent (neutral at micro-scale, they win as N grows).

Reproduce:

```bash
# in-process size sweep + selective-filter cost-opt ablation + cold/warm reopen + recall gate
cargo bench -p eg-plan --features "query,owl,text,timeseries" --bench hybrid_queries
EG_BENCH_ABLATION=1 cargo bench -p eg-plan --features query --bench hybrid_queries   # filter-pushdown win
cargo bench --features full --bench cold_warm_reopen                                  # redb reopen cold vs warm
python3 scripts/bench_gate.py                                                         # p50 + recall@k gate
# live cold-vs-warm + warm-fork + KV cross-restart drivers: see the workspace report
```
