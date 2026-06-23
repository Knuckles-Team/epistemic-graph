# epistemic-graph

<p align="center">
  <b>Unified Rust-Native Compute Engine for AI Agent Infrastructure</b><br>
  <sub>Consolidates graph operations, quantitative finance, data science, AST analysis, and OWL reasoning into a single high-performance binary.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.32.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/language-Rust%20%7C%20Python-orange" alt="Language">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> **Documentation** — The architecture, service-mode operations, Rust compute
> reference, measured transport benchmarks, and concept registry for the engine are
> maintained in the [official documentation](https://knuckles-team.github.io/epistemic-graph/).

> **This is the compute engine for
> [`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities)** — a
> standalone Rust service reached out-of-process over MessagePack/UDS (no PyO3).
> You can use it on its own (binary + pure-Python client), or let `agent-utilities`
> drive it. Contributing? See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Architecture

The `epistemic-graph` crate is the **singular computation engine** for the agent-utilities ecosystem. All high-performance operations route through this crate, exposed to Python **out-of-process** over a long-running Tokio service speaking length-prefixed **MessagePack over Unix Domain Sockets (default) or TCP**, authenticated with HMAC-SHA256. There is **no PyO3 / in-process FFI** — the engine runs as a separate process (`maturin` ships it as `bindings = "bin"`), so callers cross a network boundary, not a function call. This is enforced by `scripts/check_no_pyo3.sh`.

```
┌──────────────────────────────────────────────────────────┐
│                    epistemic-graph                        │
│                                                          │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌───────────┐  │
│  │  Graph   │  │ Finance  │  │  Data   │  │ Reasoning │  │
│  │  Core    │  │ Engine   │  │ Science │  │  Engine   │  │
│  │(petgraph)│  │          │  │         │  │ (Datalog) │  │
│  └─────────┘  └──────────┘  └─────────┘  └───────────┘  │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐                 │
│  │   AST   │  │ Semantic │  │  Algo   │                 │
│  │ Parser  │  │  Store   │  │ Library │                 │
│  └─────────┘  └──────────┘  └─────────┘                 │
│                                                          │
│  ╔════════════════════════════════════════════════════╗   │
│  ║  Tokio Server (UDS/TCP + HMAC-SHA256 auth)        ║   │
│  ╚════════════════════════════════════════════════════╝   │
│           ↕ length-prefixed MessagePack (UDS / TCP)      │
│  ╔════════════════════════════════════════════════════╗   │
│  ║  Python: EpistemicGraph class                     ║   │
│  ╚════════════════════════════════════════════════════╝   │
└──────────────────────────────────────────────────────────┘
```

## Features

### Core Graph Engine (CONCEPT:KG-2.2)
- **petgraph-backed**: Native-compiled Rust graph structures
- **Temporal Knowledge Graph (TKG)**: Ebbinghaus Forgetting Curve and fact decay natively integrated
- **Topological Sort**: Sub-millisecond DAG resolving
- **DFS Cycle Detection**: Returns precise cycle paths
- **Shortest Path**: Efficient unweighted BFS traversal
- **Blast Radius**: Transitive impact analysis to configurable depth
- **PageRank & PPR**: Centrality computation
- **Community Detection**: Louvain-style graph clustering
- **VF2 Subgraph Isomorphism**: Pattern matching queries
- **Reactive State Ledger**: Transaction log with replay for backend persistence

### Finance Engine (CONCEPT:QF-1.0)
- **Portfolio Optimization**: Mean-variance (MVO), min-variance, risk-parity, efficient frontier
- **Risk Metrics**: VaR (historical + Monte Carlo), CVaR, Sortino, Calmar, max drawdown
- **Regime Detection**: Hidden Markov Model (Baum-Welch + Viterbi)
- **Signal Generation**: Rolling Z-score, EWMA, momentum, alpha combination, information coefficient
- **Execution Algorithms**: TWAP/VWAP scheduling, market impact estimation, LOB matching
- **Pairs Trading**: Spread signal generation and regime-aware position sizing

### Data Science Engine (CONCEPT:DS-1.0)
- **OLS Regression**: Gradient descent with configurable learning rate and epochs
- **K-Means Clustering**: Parallel centroid computation
- **PCA**: Eigenvalue decomposition via power iteration
- **Dataset Statistics**: Mean, std, min, max, correlation matrix
- **Estimators**: ridge / lasso / elasticnet / decisiontree / randomforest / gradientboosting / adaboost / svr (replaces sklearn on the hot path)
- **Training loss / optimizer kernels (CONCEPT:KG-2.22)**: `softmax` / `log_softmax`, `cross_entropy` (+grad), `dpo_loss` (Bradley-Terry, +grads), `grpo_surrogate` (PPO clip, +grad), `kl_divergence` (Schulman k3), `adam_step` / `sgd_step` — the pure-Rust performance path for the in-house training substrate, mirroring `data-science-mcp trainers/objectives.py`. `client.datascience.{...}`.

### Reasoning Engine (CONCEPT:KG-2.23)
- **Transitive/Symmetric Inference**: Compiled Datalog closures
- **Domain/Range Rules**: OWL-style type inference
- **Property Chain Composition**: Multi-hop rule chaining

### AST Parser
- **Multi-Language**: 9 languages via tree-sitter — Python, Rust, TypeScript, JavaScript, Go, Java, C, C++, C# (`src/parser/tree_sitter.rs::lang_for_path`)
- **Full Granularity**: Functions, classes, methods, imports stored as `Symbol` nodes
- **Repository Ingestion**: Directory walker with automatic graph population

---

## Cargo Feature Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `ast` | Tree-sitter AST parser | `tree-sitter`, language grammars |
| `finance` | Quantitative finance engine | (pure Rust) |
| `datascience` | ML primitives | (pure Rust) |
| `reasoning` | OWL/Datalog reasoning | (pure Rust) |
| `compute` | All compute features | `finance` + `datascience` + `reasoning` |
| `server` | Tokio UDS/TCP server | `tokio`, `hmac`, `rmp-serde` |
| `full` | Everything | `compute` + `server` + `ast` |
| `all` | Alias for `full` | (same as `full`) |

### Build examples

```bash
# Library only (default, no server)
cargo build

# With compute modules
cargo build --features compute

# Full build including server binary
cargo build --features full

# Run all tests
cargo test --lib --features compute
```

---

## Quickstart

### 1. Installation

```bash
uv pip install -e .
# or
pip install -e .
```

### 2. Python Usage

The client speaks to the out-of-process engine and exposes capabilities through
typed namespaces (`g.nodes`, `g.edges`, `g.graph`, `g.finance`, `g.datascience`,
`g.reasoning`, ...). Use `SyncEpistemicGraphClient` for blocking code or
`EpistemicGraphClient` for async.

```python
from epistemic_graph import SyncEpistemicGraphClient

# Connect to the running engine (starts/attaches to the UDS service)
g = SyncEpistemicGraphClient()

# Graph operations
g.nodes.add("AgentA", {"type": "coordinator"})
g.nodes.add("AgentB", {"type": "worker"})
g.edges.add("AgentA", "AgentB", {"weight": 1.5})

print("Order:", g.graph.topological_sort())
print("Cycle:", g.graph.find_cycle())

# Finance — portfolio optimization
weights = g.finance.optimize_portfolio([0.1, 0.15, 0.08], [[0.04, 0.01, 0.005], ...], 0.02)

# Finance — risk metrics
metrics = g.finance.risk_metrics([0.01, -0.02, 0.03, -0.005, 0.02])

# Data Science — regression
coeffs = g.datascience.linear_regression([[1.0, 2.0], [3.0, 4.0]], [3.0, 7.0])

# Reasoning — OWL/RDFS forward chaining (CONCEPT:KG-2.17)
# Materialises inferred edges/types in-graph and returns the inferred triples.
result = g.reasoning.reason(
    subclass_relations=[("Dog", "Animal")],
    transitive_properties=["ancestor"],
)
print("Inferred:", result["inferred_count"], "triples")
```

---

## Security: Authentication & Tenant Isolation

- **Auth is mandatory.** Every request carries an `HMAC-SHA256(secret, request_id)`
  token. The server **refuses to start with an empty secret** — set
  `GRAPH_SERVICE_AUTH_SECRET` (or `--auth-secret`). To intentionally run
  unauthenticated (development only) pass `--allow-insecure` or set
  `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`; the server then starts with a prominent
  `SECURITY:` warning naming the bind addresses.
- **ACLs are enforced in dispatch.** Once any identity is registered
  (`client.consensus.register_identity`), every graph-targeted operation is
  checked by the isolation layer (`src/isolation.rs`): peer agent graphs are
  denied, managers reach subordinate graphs, team graphs are member-read /
  manager-write, `global:` graphs are read-only, and `__commons__` stays open to
  all authenticated agents. Callers identify themselves with the optional
  `agent_id` field (`EpistemicGraphClient.connect(..., agent_id="worker1")`).
  With **zero registered identities nothing is checked** — single-tenant
  deployments are unchanged. Violations return `ACCESS_DENIED: ...` errors.
- **TCP has no TLS.** The optional TCP listener is plaintext; keep it on
  loopback or behind a TLS-terminating proxy / WireGuard / SSH tunnel.

See [docs/service_mode.md](docs/service_mode.md) for the full protocol,
policy table, and examples.

---

## Engine Internals

Capabilities living in the binary beyond the headline features:

- **HNSW semantic store** (`src/compute/semantic.rs`) — per-graph embedding
  store with an `hnsw_rs` approximate-nearest-neighbor index for O(log n)
  cosine search, falling back to brute force below 32 embeddings. Served over
  the protocol as `AddEmbedding` / `SemanticSearch`; search results are
  re-weighted by temporal confidence decay (Ebbinghaus) before ranking.
- **Lock-free heavy compute** (CONCEPT:KG-2.51) — CPU-heavy read-only
  operations (semantic search, PageRank, betweenness, community detection,
  MST, similarity edges, VF2, lifecycle metrics) never run while holding the
  per-graph lock: dispatch takes a cheap structural snapshot
  (`GraphCore::topology_snapshot` / `analysis_snapshot`) under the read lock
  and computes on the tokio blocking pool, so a large analytics request no
  longer stalls writers on that graph. Single-pass O(V+E) ops stay under-lock
  (a snapshot would cost as much as the computation).
- **Prometheus metrics** (`src/metrics.rs`, cargo feature `metrics`, on by
  default; CONCEPT:KG-2.51) — per-op request counters + latency histograms,
  in-flight / admission-permit gauges, BUSY-rejection counter, per-graph op
  counters and node/edge gauges (bounded label cardinality), checkpoint
  duration/timestamp, and auth-failure / ACL-denial counters. Exposed by a
  dependency-free HTTP listener on `--metrics-addr` /
  `GRAPH_SERVICE_METRICS_ADDR` (disabled when unset; e.g. `127.0.0.1:9101`),
  entirely separate from the MessagePack RPC transports.
- **Parser symbol metadata** (`src/parser/tree_sitter.rs`) — beyond raw
  symbols, each parse extracts per-symbol metadata (name, kind, line,
  docstring, argument list) plus import edges, and stamps every symbol with a
  stable language label so the graph can answer per-language queries
  ("all Java symbols") and compute per-language metrics.
- **Spectral clustering** (`src/compute/spectral.rs`) — a normalized-Laplacian
  spectral cluster navigator. **Source-only today:** the module is not
  compiled into the crate (excluded from `compute/mod.rs`) and the
  `SpectralCluster` protocol method returns a deprecation error pointing at
  the `datascience` primitives (`kmeans`/`pca`).
- **Hypergraph interaction encoding** (`src/compute/hypergraph.rs`) — a seeded
  2-layer MLP positional-interaction encoder with an encoder cache, used to
  embed (position, position) interactions into fixed-width vectors. Compiled,
  but its `HypergraphEncodeInteraction` protocol method is deprecated in favor
  of the `datascience` primitives.
- **Execution orchestrator** (`src/execution/orchestrator.rs`) — scaffold for
  executing compiled task graphs (topological scheduling of `TaskGraphSpec`).
  **Not wired into the crate** (no `execution` module in `lib.rs`); orchestration
  currently lives in agent-utilities, which compiles task graphs client-side.

---

## Scaling & HA Reality

What the architecture does and does not give you (measured numbers in
[docs/benchmarks.md](docs/benchmarks.md)):

- **Sharding is client-side.** Shards are independent server processes, one
  graph universe each; the Python `ShardRouter` (`epistemic_graph/pool.py`)
  picks a shard per graph name with rendezvous/HRW hashing over
  `GRAPH_SERVICE_ENDPOINTS`. There is no server-side coordination, rebalancing,
  or cross-shard query.
- **No replication, no HA.** Each graph lives in exactly one process's memory.
  If a shard dies, its graphs are unavailable until the process restarts and
  reloads its last snapshot.
- **RPO = checkpoint interval.** Durability is periodic snapshotting
  (`--persist-dir`, default every 300 s) plus checkpoint-on-shutdown; a crash
  loses writes since the last checkpoint. There is no WAL.
- **The 100M-agent figure is a projection**, not a load test: it assumes
  ~52 kB resident per agent (measured on bounded 40-node subgraphs), 64 GB RAM
  per host, and linear shard scaling — arithmetic that yields ~78 hosts.

---

## Development & Test

### Run Unit Tests
```bash
# Rust tests (29 compute + graph tests)
cargo test --lib --features compute

# Python tests
uv run pytest
```

### Format and Lint
```bash
pre-commit run --all-files
```

---

## Documentation

- [Technical Overview](docs/overview.md) — Rust-side structures and graph algorithm layouts.
- [Concept Registry](docs/concepts.md) — Registered `CONCEPT` bridges.
- [AI Agent Handbook](AGENTS.md) — Quick command sheet for coding assistants.
- [Changelog](CHANGELOG.md) — Progression of updates and releases.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for inter-process authentication (**required** — the server refuses to start without it unless the insecure opt-out is set) |
| `EPISTEMIC_GRAPH_ALLOW_INSECURE` | `1`/`true`: explicit opt-out allowing an empty auth secret (development only; logs a prominent warning) |
| `GRAPH_SERVICE_SOCKET` | Path to Unix Domain Socket for UDS communication |
| `GRAPH_SERVICE_ENDPOINTS` | Comma-separated shard endpoints consumed by the Python `ShardRouter` |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` | Server backpressure cap (default 1024); excess load is shed with `BUSY` |
| `GRAPH_SERVICE_METRICS_ADDR` | Prometheus `/metrics` HTTP listener address (e.g. `127.0.0.1:9101`). Disabled when unset |
| `XDG_RUNTIME_DIR` | Directory for UDS socket placement |

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
