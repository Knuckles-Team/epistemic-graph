# AGENTS.md — Epistemic Graph Compute Engine

> **Project Name**: `epistemic-graph`
> **Ecosystem Prefix**: `EG` / `EPG`
> **Key Concepts**: `CONCEPT:KG-2.2` (High-Performance Graph Compute Engine), `CONCEPT:KG-2.19` (Tokio Service Layer), `CONCEPT:QF-1.0` (Quantitative Finance Engine), `CONCEPT:DS-1.0` (Data Science Primitives), `CONCEPT:KG-2.23` (Rust-Accelerated Reasoning)

---

## Overview

Unified Rust-native computation engine for the agent-packages ecosystem. Consolidates graph operations, quantitative finance, data science, AST analysis, and OWL reasoning into a single high-performance binary. Exposed to Python via PyO3 FFI and remotely via Tokio UDS/TCP server.

---

## Commands for AI Agents

| Objective | Command |
|-----------|---------|
| **Install in editable mode** | `uv pip install -e .` or `pip install -e .` |
| **Run python tests** | `uv run pytest` or `pytest` |
| **Run Rust tests (all)** | `cargo test --lib --features compute` |
| **Run Rust tests (finance)** | `cargo test --lib --features finance` |
| **Run Rust tests (ML)** | `cargo test --lib --features datascience` |
| **Build full binary** | `cargo build --features full` |
| **Check compilation** | `cargo check --all-targets --features full` |
| **Pre-commit checks** | `pre-commit run --all-files` |
| **Validate workspace** | `uv run repository-manager validate --repositories epistemic-graph` |

---

## Cargo Feature Flags

```toml
# Cargo.toml features
[features]
default = []
ast       = ["tree-sitter", "tree-sitter-python", ...]  # Multi-language AST parser
finance   = []                                           # Pure-Rust quant finance engine
datascience = []                                         # Pure-Rust ML primitives
reasoning = []                                           # OWL/Datalog inference
compute   = ["finance", "datascience", "reasoning"]      # All compute modules
server    = ["tokio/full", "hmac", ...]                  # Tokio UDS/TCP server
full      = ["compute", "server", "ast"]                 # Everything
all       = ["full"]                                     # Alias
```

---

## Python API Reference

### Graph Operations

```python
import epistemic_graph

graph = epistemic_graph.EpistemicGraph()

# Nodes & edges
graph.add_node("node_id", '{"type": "Agent"}')
graph.add_edge("source", "target", '{"weight": 1.0}')
graph.has_node("node_id") -> bool
graph.has_edge("source", "target") -> bool
graph.get_nodes() -> list[tuple[str, str]]
graph.get_edges() -> list[tuple[str, str, str]]
graph.remove_node("node_id")
graph.remove_edge("source", "target")

# Algorithms
graph.topological_sort() -> list[str]
graph.find_cycle() -> list[str] | None
graph.get_shortest_path("source", "target") -> list[str] | None
graph.get_blast_radius("node_id", max_depth) -> list[str]
graph.pagerank(damping, iterations) -> dict[str, float]
graph.community_detection(resolution) -> dict[str, int]
```

### Finance Engine (feature: `finance`)

```python
# Portfolio optimization
graph.optimize_portfolio(expected_returns, cov_matrix, risk_free_rate) -> dict
graph.min_variance_portfolio(cov_matrix) -> dict
graph.risk_parity_portfolio(cov_matrix) -> dict
graph.efficient_frontier(expected_returns, cov_matrix, n_points) -> list[dict]

# Risk metrics
graph.risk_metrics(returns) -> dict  # VaR, CVaR, Sharpe, Sortino, max_drawdown
graph.historical_var(returns, confidence) -> float
graph.monte_carlo_var(returns, confidence, n_simulations) -> float

# Regime detection
graph.detect_regimes(observations, n_states, n_iterations) -> dict

# Signal generation
graph.rolling_zscore(values, window) -> list[float]
graph.ewma(values, span) -> list[float]
graph.combine_alphas(signals, weights) -> list[float]
graph.momentum(values, lookback) -> list[float]
graph.information_coefficient(predicted, actual) -> float

# Execution algorithms
graph.twap_schedule(total_qty, n_slices) -> list[float]
graph.vwap_schedule(total_qty, volume_profile) -> list[float]
graph.estimate_market_impact(qty, avg_daily_volume, volatility) -> float
```

### Data Science (feature: `datascience`)

```python
graph.linear_regression(x, y, lr, epochs) -> dict
graph.kmeans(data, k, max_iter) -> dict
graph.pca(data, n_components) -> dict
graph.dataset_stats(data) -> dict
```

### Reasoning Engine (feature: `reasoning`)

```python
graph.infer_transitive(transitive_props, symmetric_props) -> list[dict]
graph.infer_domain_range(domain_rules, range_rules) -> list[dict]
graph.infer_property_chains(chains) -> list[dict]
```

---

## Module Structure

```
src/
├── lib.rs           # PyO3 FFI surface + module registration
├── graph.rs         # GraphCore: petgraph-backed graph with ledger
├── algorithms.rs    # Graph algorithms (PageRank, centrality, BFS, DFS)
├── finance/
│   ├── mod.rs       # Finance module root
│   ├── optimizer.rs # MVO, min-variance, risk-parity, efficient frontier
│   ├── risk.rs      # VaR, CVaR, drawdown, Sharpe, Sortino, Calmar
│   ├── regime.rs    # HMM regime detection (Baum-Welch + Viterbi)
│   ├── signals.rs   # Z-score, EWMA, momentum, alpha combination
│   └── exchange.rs  # TWAP/VWAP, market impact, order matching
├── datascience/
│   ├── mod.rs
│   └── primitives.rs # OLS, K-means, PCA, dataset stats
├── reasoning.rs     # Datalog inference: transitive, symmetric, domain/range, chains
├── ast/
│   ├── mod.rs
│   ├── symbol.rs    # Symbol struct for KG nodes
│   └── parser.rs    # Multi-language tree-sitter parser
├── compute/
│   ├── mod.rs       # Legacy compute (deprecated)
│   └── semantic.rs  # SemanticStore (Vec<f32> cosine similarity)
├── server.rs        # Tokio UDS/TCP server with HMAC auth
├── protocol.rs      # MsgPack request/response protocol
├── registry.rs      # Multi-tenant graph registry
├── channels.rs      # Agent communication channels
└── isolation.rs     # Zero-trust agent isolation
```

---

## ⛔ No Scratch or Temporary Files in Repository

**NEVER write any of the following to this repository:**
- Temporary test scripts (`test_*.py`, `debug_*.py` outside of `tests/`)
- Scratch scripts or experimental one-off files
- Log files (`.log`, `.txt` command output)
- Random text files with command output or debug dumps
- Any file that is NOT production source code, tests in `tests/`, or documentation

**Where to put scratch work instead:**
- Use `~/workspace/scratch/` for temporary scripts and experiments
- Use `~/workspace/reports/` for command output and reports
- Keep test scripts in the `tests/` directory following proper pytest conventions

### Environment Variables
| Variable | Description |
|----------|-------------|
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for Tokio service authentication |
| `GRAPH_SERVICE_SOCKET` | Path to Unix Domain Socket |
| `XDG_RUNTIME_DIR` | Directory for UDS socket placement |
