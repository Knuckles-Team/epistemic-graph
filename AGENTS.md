# AGENTS.md — Epistemic Graph Compute Engine

> **Project Name**: `epistemic-graph`
> **Ecosystem Prefix**: `EG` / `EPG`
> **Key Concepts**: `CONCEPT:KG-2.16` (High-Performance Graph Compute Engine), `CONCEPT:ORCH-1.29` (Compiled Orchestration Kernel)

---

## Overview

This repository houses the compiled Rust core of the Epistemic Graph Engine for the agent-packages ecosystem. It compiles as a PyO3 Rust extension module (`epistemic_graph`) to yield extreme local-first speed for dependency resolution, topological sorting, and cycle detection across agent workflows.

---

## Commands for AI Agents

These are the exact commands you should use to build, test, and audit this repository:

| Objective | Command |
|-----------|---------|
| **Install in editable mode** | `uv pip install -e .` or `pip install -e .` |
| **Run python tests** | `uv run pytest` or `pytest` |
| **Run pre-commit checks** | `pre-commit run --all-files` |
| **Run drift standardizer** | `uv run repository-manager validate --repositories epistemic-graph` |
| **Clean target & build** | `cargo clean` |

---

## Python API Reference

```python
import epistemic_graph

# 1. Instantiate the graph
graph = epistemic_graph.EpistemicGraph()

# 2. Add nodes (properties_json must be a JSON string)
graph.add_node("node_id", '{"type": "Agent", "status": "active"}')

# 3. Add edges (returns None or raises PyValueError if nodes don't exist)
graph.add_edge("source_id", "target_id", '{"weight": 1.0}')

# 4. Query nodes & edges
graph.has_node("node_id") -> bool
graph.has_edge("source_id", "target_id") -> bool
graph.get_nodes() -> list[tuple[str, str]]  # list of (node_id, properties_json)
graph.get_edges() -> list[tuple[str, str, str]]  # list of (source_id, target_id, properties_json)

# 5. Remove elements
graph.remove_edge("source_id", "target_id")
graph.remove_node("node_id")

# 6. Topological Sort & Dependency Checks
# Raises ValueError ("Graph contains cycles") if a cycle is present.
graph.topological_sort() -> list[str]

# 7. Cycle Detection
# Returns a list of node_ids forming a cycle, or None if acyclic.
graph.find_cycle() -> list[str] | None

# 8. Shortest Path (BFS)
# Returns list of node_ids representing the shortest path, or None if unreachable.
graph.get_shortest_path("source_id", "target_id") -> list[str] | None

# 9. Blast Radius Dependencies
# Returns a list of downstream node_ids up to max_depth
graph.get_blast_radius("node_id", max_depth) -> list[str]
```

---

## Implementation Details

The underlying algorithms are written in Rust using the `petgraph` library:
- **`toposort`**: Utilizes petgraph's standard topological sorting algorithm.
- **`find_cycle`**: Runs a custom depth-first search (DFS) coloring algorithm (0 = unvisited, 1 = visiting, 2 = visited) to detect cycles and reconstruct the cycle path cleanly.
- **`get_shortest_path`**: Breadth-First Search (BFS) starting at the source node to track predecessors and locate the target.
- **`get_blast_radius`**: Uses a BFS queue tracking depth up to `max_depth` to gather all reachable descendants.
