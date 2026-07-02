# Benchmarks

Criterion micro-benchmark harnesses for epistemic-graph. Every bench here is **dev-only** —
`criterion` is a `[dev-dependencies]` entry, never linked into `epistemic-graph-server`, so
these have **zero** release-build impact. Each is registered `harness = false` in the root
`Cargo.toml`.

## Harnesses

| Bench | Concept | Feature gate | Measures |
|-------|---------|--------------|----------|
| `eg096_massive_scale_bench` | **EG-096** | default (feature-light) | in-process node/edge write throughput, query latency, ANN kNN latency |
| `write_coalescer_bench` | EG-012 | `--features server` | `__commons__` write-lock contention curve vs the batch window |
| `redb_group_commit_bench` | EG-024 | `--features full` | durable group-commit ops-per-fsync vs the linger knob |

## EG-096 — massive-scale core harness

`eg096_massive_scale_bench.rs` is the standing "how fast is the core?" harness. It runs
entirely **in-process** against the pure `eg-core` `GraphCore` and the pure `eg-ann` IVF-PQ
index — **no socket, no Tokio, no durable/redb tier** — so it isolates raw compute cost from
transport/fsync noise. It is deliberately **feature-light**: it builds under default features,
so a plain `cargo bench --no-run` compiles it while the server/redb benches above skip.

Four criterion groups:

| Group | What it does | Metric |
|-------|--------------|--------|
| `eg096_node_write` | Build a fresh graph and `add_node` N times (N ∈ {10k, 50k}) | elements/sec |
| `eg096_edge_write` | Over a 20k-node graph, `add_edge` E times (E ∈ {20k, 100k}); graph setup is un-timed (`iter_batched`) | elements/sec |
| `eg096_query` | Steady-state read latency: single-hop `get_neighbors` and a `get_nodes_by_label` index lookup (limit 100) over a 50k-node / 200k-edge graph | ns/op |
| `eg096_ann_knn` | Train an IVF-PQ index (dim 128, N 20k), then `search(k=10)` swept over `nprobe ∈ {8,16,32}` | ns/query |

### Methodology notes

- **Deterministic, dep-free inputs.** A tiny in-file `splitmix64` PRNG generates node ids,
  edge endpoints and clustered vectors, so runs are reproducible and no `rand` dev-dep is
  pulled into the facade crate. ANN vectors are drawn from gaussian **clusters** (the regime
  PQ is designed for) — uniform-random vectors understate achievable recall/latency realism.
- **Setup excluded from the timed region.** The edge and query groups build their graph once
  (or per batch) outside the measured closure; the ANN group trains + populates the index
  before the timing loop. You are measuring the operation, not the fixture.
- **Warm state.** The query group warms the lazy label index once before timing so
  `get_nodes_by_label` reflects the steady-state O(1) index hit, not the first cold build.
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
*CONCEPT:EG-096 — massive-scale benchmark harness. Sibling harnesses: EG-012 (write coalescer),
EG-024 (redb group-commit).*
