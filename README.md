# epistemic-graph

<p align="center">
  <b>Unified Rust-Native Compute Engine for AI Agent Infrastructure</b><br>
  <sub>Consolidates graph operations, quantitative finance, data science, AST analysis, and OWL reasoning into a single high-performance binary.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.27.0-blue" alt="Version">
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
- **Multi-Language**: Python, Rust, TypeScript, JavaScript, Go (via tree-sitter)
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

```python
import epistemic_graph

# Initialize
g = epistemic_graph.EpistemicGraph()

# Graph operations
g.add_node("AgentA", '{"type": "coordinator"}')
g.add_node("AgentB", '{"type": "worker"}')
g.add_edge("AgentA", "AgentB", '{"weight": 1.5}')

print("Order:", g.topological_sort())
print("Cycle:", g.find_cycle())

# Finance — portfolio optimization
weights = g.optimize_portfolio([0.1, 0.15, 0.08], [[0.04, 0.01, 0.005], ...], 0.02)

# Finance — risk metrics
metrics = g.risk_metrics([0.01, -0.02, 0.03, -0.005, 0.02])

# Data Science — regression
coeffs = g.linear_regression([[1.0, 2.0], [3.0, 4.0]], [3.0, 7.0])

# Reasoning — transitive inference
inferred = g.infer_transitive(["part_of", "depends_on"], ["related_concept"])
```

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
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for inter-process authentication |
| `GRAPH_SERVICE_SOCKET` | Path to Unix Domain Socket for UDS communication |
| `XDG_RUNTIME_DIR` | Directory for UDS socket placement |

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
