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

## Notes

- Numbers are for the hot in-memory CRUD path; analytic ops (clustering,
  subgraph match) are heavier and not included here.
- The framing is length-prefixed (4-byte big-endian `u32` + MessagePack body),
  so binary payloads containing `0x0A` bytes round-trip intact — verified by
  `tests/test_no_pyo3_and_quant.py::test_length_prefixed_framing_is_binary_safe`.
- For multi-shard / multi-host scaling targets and the capacity model, see the
  agent-utilities scaling docs (Plan 07).
