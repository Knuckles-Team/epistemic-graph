# epistemic-graph

<p align="center">
  <b>High-Performance Rust-compiled Epistemic Graph Compute Engine for Python</b><br>
  <sub>Bridges sub-millisecond local-first speed with petgraph algorithms and PyO3 native bindings.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.6.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/language-Rust%20%7C%20Python-orange" alt="Language">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

---

## Features

- **Blazing Fast**: Native-compiled Rust graph structures utilizing `petgraph` stable graph under the hood.
- **Robust Algorithms**:
  - **Topological Sorting**: Sub-millisecond DAG resolving.
  - **DFS Cycle Detection**: Returns precise cycle paths for immediate debugging of dependency loops.
  - **Shortest Path Finder**: Efficient unweighted BFS traversal.
  - **Blast Radius Calculator**: Transitive impact analysis up to target depth.
- **Advanced Capabilities 🔬**:
  - **Native AST Parser**: High-performance Python directory crawler and AST parser to dynamically map classes, functions, and files into the graph.
  - **VF2 Subgraph Isomorphism**: Highly optimized node-matching algorithm to query and match graph structural patterns.
  - **Reactive State Ledger**: Sequential transaction logs with JSON serialization and replay capabilities to keep states synchronized with zero-overhead.
- **Zero-overhead FFI**: Fully typed PyO3 bindings mapped cleanly to Python.

---

## Quickstart

### 1. Installation

To compile and install the extension locally in editable mode, you must have Rust and `maturin` installed:

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

# Add nodes and edges with JSON properties
g.add_node("AgentA", '{"type": "coordinator"}')
g.add_node("AgentB", '{"type": "worker"}')
g.add_edge("AgentA", "AgentB", '{"weight": 1.5}')

# Check structure
assert g.has_node("AgentA")
assert g.has_edge("AgentA", "AgentB")

# Topological Sort
print("Workflow Order:", g.topological_sort())  # -> ["AgentA", "AgentB"]

# Cycle Check
print("Cycle:", g.find_cycle())  # -> None
```

---

## Development & Test

We use standard ecosystem tools to ensure quality and compliance.

### Run Unit Tests
Ensure the module is built and run `pytest`:
```bash
uv run pytest
```

### Format and Lint
Run styling checks before submitting changes:
```bash
pre-commit run --all-files
```

---

## Documentation

For deep technical details, refer to the `docs` folder:
- [Technical Overview](docs/overview.md) — Rust-side structures and graph algorithm layouts.
- [Concept Registry](docs/concepts.md) — Registered `CONCEPT` bridges.
- [AI Agent Handbook](AGENTS.md) — Quick command sheet for coding assistants.
- [Changelog](CHANGELOG.md) — Progression of updates and releases.

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
