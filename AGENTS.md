# AGENTS.md — Epistemic Graph Compute Engine

> Claude Code loads this file via `CLAUDE.md` (`@AGENTS.md` import) — the two stay
> in sync. Edit **this** file, not `CLAUDE.md`.

> **Project Name**: `epistemic-graph`
> **Ecosystem Prefix**: `EG` / `EPG`
> **Key Concepts**: `CONCEPT:KG-2.16` (High-Performance Graph Compute Engine), `CONCEPT:KG-2.19` (Tokio Service Layer), `CONCEPT:KG-2.20` (Rust-Native Finance), `CONCEPT:KG-2.22` (Data Science Primitives), `CONCEPT:KG-2.23` (Rust-Accelerated Reasoning)

---

## Overview

Unified Rust-native computation engine for the agent-packages ecosystem.
Consolidates graph operations, quantitative finance, data science, AST analysis,
and OWL reasoning into a single high-performance binary.

**Transport (important — this changed):** the engine is exposed to Python
**out-of-process** via a long-running Tokio service speaking **length-prefixed
MessagePack over Unix Domain Sockets (default) or TCP**, authenticated with
**HMAC-SHA256**. There is **NO PyO3 / in-process extension** — that coupling was
removed to stay GIL-free and horizontally scalable. The shipped wheel contains
the `epistemic-graph-server` binary plus a pure-Python client; `maturin` is
configured `bindings = "bin"`. This is enforced by `scripts/check_no_pyo3.sh`
(fails on PyO3 in source **or** a compiled `_epistemic_graph*.so`).

The framing is a 4-byte big-endian `u32` length prefix + a MessagePack body, so
binary payloads containing `0x0A` survive intact (newline framing would not).

**Why this drives architecture decisions (read before designing a caller).**
Because the engine is **tokio + MessagePack over a socket — NOT PyO3, NOT an FFI /
in-process call** — every invocation costs *serialize → socket round-trip →
deserialize* against a separate process. A call is **not** a cheap function call.
Two rules follow, and they shape every integration:

- **Batch, never per-element.** Ship work to the engine in **one** round-trip over
  data already resident in the graph (e.g. `compute_similarity_edges(threshold)` for
  all-pairs similarity), instead of calling per pair/row in a Python loop. *N*
  elements in a loop = *N* round-trips = catastrophic; the same work as one batch op
  = one round-trip. If a batch op you need doesn't exist yet, add it engine-side
  rather than looping client-side.
- **Keep tight per-element math in-process.** A single cosine of two vectors is
  cheaper in local numpy than marshalled over the wire — push to the engine only
  when a *batch* amortizes the round-trip. (Reference: agent-utilities `KG-2.3`
  similarity collapse routes the all-pairs batch here but keeps pairwise cosine local.)

There is no GIL coupling and no shared address space — design for a network boundary,
because that is exactly what it is.

---

## Commands for AI Agents

| Objective | Command |
|-----------|---------|
| **Build the server binary** | `cargo build --release --features server` → `target/release/epistemic-graph-server` |
| **Run Rust lib tests** | `cargo test --features server --lib` |
| **Run finance tests** | `cargo test --features server --lib finance::optimizer` |
| **Python client tests** | `pytest tests/` |
| **No-PyO3 gate** | `bash scripts/check_no_pyo3.sh` |
| **Measured transport benchmark** | `python3 scripts/bench_transport.py --ops 5000` |
| **Launch N shards** | `EPISTEMIC_GRAPH_SECRET=… scripts/run_shards.sh [N]` |
| **Check compilation** | `cargo check --features server` |
| **Pre-commit checks** | `pre-commit run --all-files` |

Measured baseline (see `docs/benchmarks.md`): `AddNode` p50 ≈ 0.187 ms, p99 ≈
0.223 ms over UDS, single connection.

---

## Python API (out-of-process client — NOT in-process)

```python
from epistemic_graph.client import EpistemicGraphClient, SyncEpistemicGraphClient

# Async
client = await EpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock",
                                             graph_name="agent:planner")
await client.tenants.create("agent:planner")           # CreateGraph
await client.nodes.add("n1", {"type": "Agent"})         # AddNode
props = await client.nodes.properties("n1")             # GetNodeProperties
await client.edges.add("n1", "n2", {"weight": 1.0})
order = await client.graph.topological_sort()
res  = await client.finance.optimize_portfolio(returns, cov, risk_free_rate=0.02)
await client.close()

# Sync wrapper (used by agent-utilities domains/finance/*)
sc = SyncEpistemicGraphClient.connect(...)
```

Sub-clients on the connection: `.nodes`, `.edges`, `.graph` (algorithms),
`.analytics`, `.finance`, `.lifecycle`, `.ledger`, `.channels`, `.tenants`,
`.consensus`. **Connection pooling + shard routing** live in
`epistemic_graph/pool.py` (`ConnectionPool`, `ShardRouter` using rendezvous/HRW
hashing over `GRAPH_SERVICE_ENDPOINTS`). `epistemic_graph/quant.py` provides
pure-Python rolling-stats/order-matching helpers (no compiled extension).

Engine capabilities served over the protocol: **finance** (mean-variance,
min-variance, risk-parity, efficient-frontier, **black_litterman** consuming
views/τ/risk-aversion, VaR/CVaR, HMM regime detection, signals, TWAP/VWAP),
**data science** (OLS, K-means, PCA, stats), **reasoning** (transitive/symmetric/
inverse closure, domain/range, property chains).

---

## Cargo Feature Flags

```toml
[features]
default     = []
ast         = [ ... ]          # Multi-language tree-sitter AST parser
finance     = []               # Pure-Rust quant finance engine
datascience = []               # Pure-Rust ML primitives
reasoning   = []               # OWL/Datalog inference
compute     = ["finance", "datascience", "reasoning"]
server      = ["tokio/full", "hmac", ... ]   # Tokio UDS/TCP service (build with this)
full        = ["compute", "server", "ast"]
```

`Cargo.toml` declares `crate-type = ["rlib"]` (no `cdylib`/pyo3).

---

## Module Structure

```
src/
├── lib.rs           # CONCEPT:KG-2.19 — Tokio service layer (MessagePack RPC); delegates to modules
├── main.rs          # epistemic-graph-server entrypoint; builds ServerState (incl. backpressure semaphore)
├── server.rs        # Tokio UDS/TCP server: HMAC auth, length-prefixed framing,
│                    #   global in-flight Semaphore -> BUSY (EPISTEMIC_GRAPH_MAX_INFLIGHT), Health/Shutdown/Ping
├── protocol.rs      # Length-prefixed MessagePack: Request/Response/Method + ResultPayload enum
├── graph.rs         # GraphCore: petgraph-backed graph with ledger
├── algorithms.rs    # PageRank, centrality, BFS/DFS, components, MST
├── finance/         # optimizer.rs (incl. black_litterman via nalgebra), risk.rs, regime.rs, signals.rs, exchange.rs
├── datascience/     # primitives.rs (OLS, K-means, PCA)
├── reasoning.rs     # Datalog closure: transitive, symmetric, inverse, domain/range, chains
├── ast/             # tree-sitter multi-language parser -> KG symbols
├── compute/semantic.rs   # SemanticStore (Vec<f32> cosine + HNSW)
├── registry.rs      # Multi-tenant graph registry
├── channels.rs      # Agent communication channels
└── isolation.rs     # Zero-trust agent isolation

epistemic_graph/     # pure-Python client package
├── client.py        # EpistemicGraphClient / SyncEpistemicGraphClient (framed MessagePack + HMAC)
├── pool.py          # ConnectionPool + ShardRouter (HRW)
└── quant.py         # pure-Python rolling stats / order matching
scripts/             # check_no_pyo3.sh, run_shards.sh, bench_transport.py
docs/benchmarks.md   # measured p50/p99 latency
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for the Tokio service (alias: `EPISTEMIC_GRAPH_SECRET` via `run_shards.sh`) |
| `GRAPH_SERVICE_SOCKET` | Path to the UDS socket |
| `GRAPH_SERVICE_ENDPOINTS` | Comma-separated shard endpoints for the Python `ShardRouter` |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` | Server backpressure cap (default 1024); excess → `BUSY` |
| `XDG_RUNTIME_DIR` | Directory for UDS socket placement |

---

## ⛔ No Scratch or Temporary Files in Repository

**NEVER** commit temporary scripts (`test_*.py`/`debug_*.py` outside `tests/`),
scratch files, `.log`/`.txt` command dumps, patch leftovers (`*.orig`, `*.rej`),
or tracked binaries. Put scratch work in `~/workspace/scratch/` and reports in
`~/workspace/reports/`. Keep tests in `tests/` (pytest) / `#[cfg(test)]` (Rust).


## ⛔ Keep the Repository Root Pristine

The repository root must contain only canonical project files. The only hidden
directories allowed at root are `.git/`, `.github/`, `.specify/` (plus a local,
git-ignored `.venv/`). NEVER write scratch/debug/migration files to the repo —
especially the root: no `fix_*.py`/`migrate_*.py`/`refactor_*.py`/root `test_*.py`,
no `*.db`/`*.log`/scratch `*.txt`/`*.orig`/`*.rej`/`*.bak`, no build artifacts
(`*.tsbuildinfo`), and no AI scratch dirs (`.agent/`, `.agents/`, `.agent_data/`,
`.tmp/`, `.hypothesis/`). Put experiments in `~/workspace/scratch/`, tests in
`tests/`. Run `git status` before finishing and confirm no stray root files.
