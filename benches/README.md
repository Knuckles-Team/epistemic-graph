# Benchmarks

Criterion micro-benchmark harnesses for epistemic-graph. Every bench here is **dev-only** —
`criterion` is a `[dev-dependencies]` entry, never linked into `epistemic-graph-server`, so
these have **zero** release-build impact. Each is registered `harness = false` in the root
`Cargo.toml`.

## Harnesses

| Bench | Concept | Feature gate | Measures |
|-------|---------|--------------|----------|
| `eg096_massive_scale_bench` | **EG-KG.compute.massive-scale-benchmark** | default (feature-light) | in-process node/edge write throughput, graph query latency, exact flat top-k, IVF-PQ and native HNSW kNN latency |
| `write_coalescer_bench` | EG-KG.txn.write-path-benchmarks | `--features server` | `__commons__` write-lock contention curve vs the batch window |
| `redb_group_commit_bench` | EG-024 | `--features full` | durable group-commit ops-per-fsync vs the linger knob |

## EG-KG.compute.massive-scale-benchmark — massive-scale core harness

`eg096_massive_scale_bench.rs` is the standing "how fast is the core?" harness. It runs
entirely **in-process** against the pure `eg-core` `GraphCore` and the pure `eg-ann`
flat/IVF-PQ/HNSW indexes — **no socket, no Tokio, no durable/redb tier** — so it isolates raw compute cost from
transport/fsync noise. It is deliberately **feature-light**: it builds under default features,
so a plain `cargo bench --no-run` compiles it while the server/redb benches above skip.

Seven criterion groups:

| Group | What it does | Metric |
|-------|--------------|--------|
| `eg096_node_write` | Build a fresh graph and `add_node` N times (N ∈ {10k, 50k}) | elements/sec |
| `eg096_edge_write` | Over a 20k-node graph, `add_edge` E times (E ∈ {20k, 100k}); graph setup is un-timed (`iter_batched`) | elements/sec |
| `eg096_query` | Steady-state read latency: O(1) `edge_count`, single-hop `get_neighbors`, 100-node induced subgraph extraction, label lookup, and a warm unlabeled keyset page over a 50k-node / 200k-edge graph | ns/op |
| `eg096_flat_topk` | Exact flat-vector scan (dim 128, N 20k), partial-select top-k swept over `k ∈ {1,10,100}`, plus repeated 1,024-candidate rerank through the warm id directory | elements/sec + ns/query |
| `eg096_ann_knn` | Train an IVF-PQ index (dim 128, N 20k), sweep `nprobe ∈ {8,16,32}`, then sweep `k ∈ {1,10,100}` through ADC-only and SQ8-refined selection | ns/query |
| `eg096_hnsw_topk` | Build the native exact-distance HNSW index (dim 128, N 10k), then sweep `(k,ef) ∈ {(1,32),(10,64),(100,256)}` | ns/query |
| `eg096_broker_route` | Match exact, trailing-`#`, and adversarial ambiguous topic patterns; de-duplicate 10k fanout bindings onto 1k queues | words/sec or bindings/sec + ns/route |

### Methodology notes

- **Deterministic, dep-free inputs.** A tiny in-file `splitmix64` PRNG generates node ids,
  edge endpoints and clustered vectors, so runs are reproducible and no `rand` dev-dep is
  pulled into the facade crate. ANN vectors are drawn from gaussian **clusters** (the regime
  PQ is designed for) — uniform-random vectors understate achievable recall/latency realism.
- **Setup excluded from the timed region.** The edge and query groups build their graph once
  (or per batch) outside the measured closure; the ANN group trains + populates the index
  before the timing loop. You are measuring the operation, not the fixture.
- **Warm state.** The query group warms the lazy label and sorted node-id directories;
  the flat group warms its id-to-row directory. Their timed cases therefore measure
  steady-state label lookup, keyset seek and candidate rerank, not one-time construction.
- **Scale.** Sizes are chosen so a full run finishes in minutes on a laptop while still
  crossing the interesting thresholds (label-index build, ANN probe fan-out). Bump the `N`/`E`
  arrays in the source for a heavier soak; criterion's throughput reporting scales with them.

### Run + interpret

```bash
# compile only (CI sanity — must be 0 errors)
cargo bench --no-run --bench eg096_massive_scale_bench

# full run
cargo bench --bench eg096_massive_scale_bench

# one group / one parameter
cargo bench --bench eg096_massive_scale_bench eg096_ann_knn
cargo bench --bench eg096_massive_scale_bench eg096_hnsw_topk
cargo bench --bench eg096_massive_scale_bench eg096_flat_topk
cargo bench --bench eg096_massive_scale_bench eg096_broker_route
cargo bench --bench eg096_massive_scale_bench eg096_node_write/50000
```

Criterion prints per-group throughput (`elements/sec` for the write groups) and time/iter
(`ns`) with a confidence interval, and writes HTML reports under
`target/criterion/`. Read the write groups as **throughput** (higher = better) and the
query/ANN groups as **latency** (lower = better). To compare two commits, run on the baseline,
`git checkout` your change, and run again — criterion auto-diffs against the stored baseline and
flags a regression/improvement per benchmark.

Flamegraph any group (needs `cargo-flamegraph`):

```bash
cargo flamegraph --bench eg096_massive_scale_bench -- --bench eg096_ann_knn
```

---
*CONCEPT:EG-KG.compute.massive-scale-benchmark — massive-scale benchmark harness. Sibling harnesses: EG-KG.txn.write-path-benchmarks (write coalescer),
EG-024 (redb group-commit).*
