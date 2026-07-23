# Rust Compute Guide

> Architecture and usage reference for the `epistemic-graph` Rust compute engine.

## Overview

`epistemic-graph` is a Unix Sockets-based Rust crate that provides the **sole high-performance compute backend** for `agent-utilities`. It replaces all previous graph computation libraries (including `epistemic-graph`) with a single, purpose-built engine.

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│ Python (agent-utilities)                                    │
│                                                            │
│  GraphComputeEngine ──────▶ EpistemicGraph (Unix Sockets)          │
│  graph_primitives.py        │                              │
│                             ▼                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Rust Core (lib.rs)                                    │  │
│  │                                                       │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │ graph.rs     │  │ algorithms.rs│  │ reasoning.rs│  │ finance.rs   │  │  │
│  │  │ (GraphCore,  │  │ (PageRank,   │  │ (Datalog,   │  │ (MVO, RP,    │  │  │
│  │  │  petgraph)   │  │  BFS, DFS,   │  │  Prolog)    │  │  Kelly,      │  │  │
│  │  │              │  │  community,  │  │             │  │  Optimization│  │  │
│  │  │              │  │  coloring,   │  │             │  │  via faer)   │  │  │
│  │  │              │  │  similarity) │  │             │  │              │  │  │
│  │  └─────────────┘  └──────────────┘  └─────────────┘  └─────────────┘  │  │
│  │                                                       │  │
│  │  ┌─────────────┐                                      │  │
│  │  │ types.rs     │  NodeData, EdgeData, LifecycleState  │  │
│  │  │              │  GraphMetrics, PruneStats, ContextView│ │
│  │  └─────────────┘                                      │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

## Modules

### `lib.rs` — Unix Sockets Surface
- Single `#[pyclass]`: `EpistemicGraph`
- Every `#[pymethod]` returns `PyResult<T>` — no panics on user-facing paths
- Delegates all logic to `graph`, `algorithms`, `reasoning`, and `finance`

### `graph.rs` — GraphCore
- `petgraph::StableDiGraph`-backed directed graph
- Node map: `HashMap<String, NodeIndex>` for O(1) ID→index lookup
- Mutation ledger for replay/undo tracking
- JSON serialization for graph persistence

### `algorithms.rs` — Graph Algorithms
| Algorithm | Function | Complexity |
|---|---|---|
| Topological Sort | `topological_sort()` | O(V+E) |
| Cycle Detection | `find_cycle()` | O(V+E) |
| BFS Shortest Path | `get_shortest_path()` | O(V+E) |
| Blast Radius | `get_blast_radius()` | O(V+E) |
| Degree Centrality | `degree_centrality_all()` | O(V+E) |
| Betweenness Centrality | `betweenness_centrality()` | O(V·(V+E)) |
| PageRank | `pagerank()` | O(iterations·E) |
| Personalized PageRank | `personalized_pagerank()` | O(iterations·E) |
| Connected Components | `connected_components()` | O(V+E) |
| Community Detection | `community_detection()` | O(iterations·E) |
| Graph Coloring | `graph_coloring()` | O(V+E) |
| Similarity Edges | `compute_similarity_edges()` | O(V²·d) |
| Lifecycle Pruning | `prune_by_lifecycle()` | O(V) |
| Context View | `get_context_view()` | O(V+E) |
| Batch Update | `batch_update()` | O(ops) |
| Metrics | `compute_metrics()` | O(V) |

### `types.rs` — Typed Data Model
- `NodeData`: id, type, embedding, lifecycle state, timestamps, metadata
- `EdgeData`: canonical `relationship`, weight, provenance, metadata
- `LifecycleState`: Active → Compacted → Archived → PendingDeletion
- `GraphMetrics`: node/edge counts, mutation tracking, lifecycle breakdowns
- `PruneStats`: pruning operation results
- `ContextView`: budget-aware agent context windows

### `reasoning.rs` — Logic & Inference
- Datalog-style forward chaining
- Prolog-style backward chaining with unification

### `finance/` — Quantitative Finance
Replaces Python `scipy`/`numpy` quant code in the execution layer. Served via
`client.finance.*`:
- **Optimization** (`optimizer.rs`): mean-variance, min-variance, risk-parity,
  efficient-frontier, Black-Litterman (views/τ/risk-aversion).
- **Risk** (`risk.rs`): `var`, `cvar`, `max_drawdown`, `drawdown_series`,
  `downside_deviation`, `risk_metrics` (VaR/CVaR/Sortino/Calmar/vol),
  `monte_carlo_var`, `stress_test`.
- **Regime** (`regime.rs`): `detect_regimes` — Gaussian HMM (Baum-Welch + Viterbi).
- **Signals** (`signals.rs`): rolling z-score, EWMA, signal decay, alpha combine,
  cross-sectional rank, momentum, mean-reversion, information coefficient.
- **Execution** (`exchange.rs`): TWAP/VWAP schedules, market-impact, pairs-trading
  spread, limit-order-book matching.

### `datascience/` — ML Primitives & Estimators
Replaces `scikit-learn` on the hot path (parity-validated). Served via
`client.datascience.*`:
- **Primitives** (`primitives.rs`): `linear_regression` (OLS), `kmeans`, `pca`,
  `compute_stats`, `train_test_split` (seeded shuffle).
- **Estimators** (`estimators.rs`): stateless `fit_estimator(name, x, y, params)`
  → serializable model blob, `predict_estimator(model, x)` → predictions.
  Names: `ridge`, `lasso`, `elasticnet`, `decisiontree`, `randomforest`,
  `gradientboosting`, `adaboost`, `svr` (RBF/linear via SMO). `rayon`-parallel
  forests; no external ML crates.
- **Training kernels** (`training.rs`, CONCEPT:EG-KG.compute.rust-native-training-loss): the pure-Rust loss /
  optimizer performance path for the in-house training substrate — `softmax`,
  `log_softmax`, `cross_entropy` (+ analytic grad), `dpo_loss` (Bradley-Terry,
  + chosen/rejected grads), `grpo_surrogate` (PPO/GRPO clipped, + grad with a
  zero-grad clip region), `kl_divergence` (Schulman k3), `adam_step`, `sgd_step`.
  Mirrors the torch kernels in `data-science-mcp trainers/objectives.py` so a
  trainer can batch a step over the wire in one round-trip. No candle/ndarray —
  same hand-vectorized style as the rest of `datascience`. Each is a forward (and,
  where it drives backprop, an analytic-gradient) function; unit-tested on toy
  tensors. New kernel? add a fn here + `Method::Ds*` (protocol.rs) + dispatch
  (server.rs) + `client.datascience.*` (client.py) + a round-trip test.

> Downstream packages (e.g. `data-science-mcp`, `emerald-exchange`,
> `agent-utilities/domains/finance`) should call these over the client rather
> than re-implementing the math in Python. sklearn-parity for the estimators is
> validated out-of-tree against scikit-learn on identical splits.

## Usage from Python

```python
import epistemic_graph

# Create a graph
g = epistemic_graph.EpistemicGraph()

# Add nodes with typed properties
g.add_node("agent:planner", '{"type": "Agent", "lifecycle_state": "active"}')
g.add_node("tool:git", '{"type": "Tool"}')

# Add edges
g.add_edge("agent:planner", "tool:git", '{"relationship": "USES"}')

# Run algorithms
ranks = g.pagerank(damping=0.85, iterations=100)
components = g.connected_components()
colors = g.graph_coloring()

# Lifecycle operations
import json
stats_json = g.prune_by_lifecycle(max_age_secs=86400, min_score=0.1)
stats = json.loads(stats_json)

# Batch operations (single epistemic-graph crossing)
ops = json.dumps([
    {"op": "add_node", "id": "concept:auth", "properties": {}},
    {"op": "add_edge", "source": "agent:planner", "target": "concept:auth", "properties": {}},
])
result = g.batch_update(ops)

# Metrics
metrics = json.loads(g.metrics())
print(f"Nodes: {metrics['node_count']}, Edges: {metrics['edge_count']}")
```

## Error Handling

All methods return `PyResult<T>` to Python. Errors surface as:
- `PyValueError` — invalid input (missing node, cycle in topo sort)
- `PyRuntimeError` — serialization failures, internal errors

No `unwrap()` calls on user-facing code paths. All errors are structured and descriptive.

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `petgraph` | 0.6.4 | Core graph data structure |
| `serde` | 1.0 | JSON serialization |
| `serde_json` | 1.0 | JSON parsing |
| `rmp-serde` | 1.3 | MessagePack (wire transport — no PyO3) |
| `rayon` | 1.10 | Data parallelism |
| `tokio` | 1.x | Async runtime (service layer) |
| `clap` | 4.x | CLI argument parsing |
| `tracing` | 0.1 | Structured logging |
| `hmac` / `sha2` | 0.12 / 0.10 | HMAC-SHA256 authentication |
| `hex` | 0.4 | Hex encoding for auth tokens |
| `faer` | 0.19 | High-performance linear algebra for finance |
| `ndarray` | 0.15 | N-dimensional arrays for convex optimization |

## Service Mode (Tokio-First)

The service layer is the primary interface for graph operations. It runs as a
long-lived Tokio process with multi-tenant named graphs, dynamic P2P/group
communication channels, and ACL-based isolation.

### New Modules

| Module | Purpose |
|---|---|
| `protocol.rs` | JSON wire protocol (Request/Response/Method enums) |
| `registry.rs` | Multi-tenant graph registry with `__bus__` auto-created |
| `isolation.rs` | ACL isolation (peer deny, manager access, team scoping) |
| `channels.rs` | Ephemeral P2P and group channels with KG imprint on close |
| `server.rs` | Tokio UDS/TCP server with HMAC auth |
| `main.rs` | Binary entry point (`epistemic-graph-server`) |

See [`docs/service_mode.md`](service_mode.md) for full reference.

## Building

```bash
cd epistemic-graph

# Build the Unix Sockets library (for embedded fallback)
maturin develop --release

# Build the server binary
cargo build --release
```

## Testing

```bash
# Rust unit tests (protocol, registry, isolation, channels)
cargo test

# Python tests
pytest tests/ -v

# Run both (as pre-commit does)
cargo test && pytest tests/ -v
```
