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

## Multi-shard scaling (measured) — CONCEPT:KG-2.7 P3

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

## Notes

- Numbers are for the hot in-memory CRUD path; analytic ops (clustering,
  subgraph match) are heavier and not included here.
- The framing is length-prefixed (4-byte big-endian `u32` + MessagePack body),
  so binary payloads containing `0x0A` bytes round-trip intact — verified by
  `tests/test_no_pyo3_and_quant.py::test_length_prefixed_framing_is_binary_safe`.
- The agent-utilities capacity model (`docs/scaling/capacity_model.py`) consumes
  these per-shard / per-agent figures for the full resident-population sizing.
