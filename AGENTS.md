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
`.analytics`, `.finance`, `.datascience`, `.lifecycle`, `.ledger`, `.channels`,
`.tenants`, `.consensus`. **Connection pooling + shard routing** live in
`epistemic_graph/pool.py` (`ConnectionPool`, `ShardRouter` using rendezvous/HRW
hashing over `GRAPH_SERVICE_ENDPOINTS`). `epistemic_graph/quant.py` provides
pure-Python rolling-stats/order-matching helpers (no compiled extension).

Engine capabilities served over the protocol:

- **finance** (`client.finance.*`): portfolio optimization (mean-variance,
  min-variance, risk-parity, efficient-frontier, **black_litterman** consuming
  views/τ/risk-aversion); risk metrics (`var`, `cvar`, `max_drawdown`,
  `drawdown_series`, `downside_deviation`, `risk_metrics` → VaR/CVaR/Sortino/
  Calmar/vol, `monte_carlo_var`, `stress_test`); **HMM regime detection**
  (`detect_regimes` — Baum-Welch + Viterbi); signals (`rolling_zscore`, `ewma`,
  `signal_decay`, `combine_alphas`, `cross_sectional_rank`, `momentum`,
  `mean_reversion`, `information_coefficient`); execution/microstructure
  (`twap`, `vwap`, `market_impact`, `pairs_trading`, `match_orders`);
  **market-making / HFT** (`CONCEPT:KG-2.20f`): `avellaneda_stoikov`,
  `glt_quotes`, `logit_quotes` (bounded prediction-market prices + boundary
  inventory cap), `glosten_milgrom_spread`, `expected_pnl_rate`,
  `breakeven_alpha`, `ofi_series`, `microprice_series`, `vpin_pm`, `hawkes_mle`
  (+ `hardiman_bouchaud`); **sizing** `kelly_fraction`, `bayesian_kelly`,
  `posterior_credible_interval`; **backtest validation** `purged_cpcv`,
  `deflated_sharpe`, `probability_backtest_overfit`, `diebold_mariano`;
  **forensic accounting** (`CONCEPT:KG-2.20g`) `forensic_report` — Beneish M /
  Altman Z / Piotroski F / Sloan accruals over two fiscal years;
  **state-space / stat-arb** (`CONCEPT:KG-2.20h`): `kalman_filter_1d`,
  `kalman_beta` (dynamic time-varying beta), `kalman_volatility` (log-variance
  state), `adf_test` (cointegration; finite-sample-interpolated MacKinnon 1/5/10% criticals
  + approximate p-value), `ou_calibrate` + `ou_optimal_thresholds`
  (Ornstein-Uhlenbeck mean reversion with numerical-MFPT-optimal entry/exit),
  `markov_transition_matrix`; **signal combination / sizing / calibration**
  (`CONCEPT:KG-2.20i`): `order_book_imbalance`, `information_ratio` (IC·√N),
  `effective_independent_n` (eigenvalue participation ratio), `alpha_combination_engine`,
  `brier_score`, `convergence_gate`, `empirical_kelly` (uncertainty-adjusted);
  **derivatives** (`CONCEPT:KG-2.20j`): `sabr_implied_vol`, `sabr_smile`, `sabr_calibrate`
  (Hagan 2002 SABR stochastic-vol surface for smile/skew vol-arb).
- **data science** (`client.datascience.*`): primitives (`linear_regression`
  OLS, `kmeans`, `pca`, `compute_stats`, `train_test_split` with seeded shuffle);
  and a stateless estimator API — `fit_estimator(name, x, y, params)` returns a
  serializable model blob, `predict_estimator(model, x)` predicts. Estimators:
  `ridge`, `lasso`, `elasticnet`, `decisiontree`, `randomforest`,
  `gradientboosting`, `adaboost`, `svr` (RBF/linear via SMO). **These replace
  scikit-learn** on the hot path (parity-validated vs sklearn).
- **reasoning**: transitive/symmetric/inverse closure, domain/range, chains.

**Adding a capability:** implement it in the relevant Rust module
(`src/finance/*`, `src/datascience/*`), then expose it across three layers —
a `Method` variant in `src/protocol.rs`, a dispatch arm in `src/server.rs`, and
a client method in `epistemic_graph/client.py` — and add a round-trip test in
`tests/`. Compute already resident in the graph should be a **batch** op (one
round-trip), never a per-row loop. See `docs/RUST_COMPUTE_GUIDE.md`.

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
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for the Tokio service (alias: `EPISTEMIC_GRAPH_SECRET` via `run_shards.sh`). **Required** — the server refuses to start with an empty secret |
| `EPISTEMIC_GRAPH_ALLOW_INSECURE` | `1`/`true`: explicit opt-out allowing an empty auth secret (development only; prominent warning at startup) |
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

## Working Discipline — think, simplify, stay surgical, verify

These four habits cut the most common LLM coding mistakes. For trivial tasks, use
judgment; the bias here is correctness over speed.

- **Think before coding.** State your assumptions explicitly. If a request has more than
  one reasonable reading, surface the options instead of silently picking one. If a
  simpler approach exists, say so and push back when warranted. When something is
  genuinely unclear, stop and name what's confusing — ask, don't guess.
- **Simplicity first.** Write the minimum code that solves the stated problem — no
  speculative features, no abstraction for single-use code, no configurability that
  wasn't requested, no error handling for impossible states. If you wrote 200 lines and
  it could be 50, rewrite it. (Name code from its purpose, never `wave0`/`phase2`/`v2`.)
- **Stay surgical.** Every changed line should trace directly to the task. Don't refactor,
  reformat, or "improve" working code adjacent to your change; match the existing style
  even where you'd do it differently. Remove only the imports/symbols your own change
  orphaned; if you spot unrelated dead code, mention it rather than deleting it inline.
  *Exception — the Quality Bar below:* lint/format/type errors the pre-commit gate flags
  get fixed regardless of who introduced them. In short: **surgical on behavior, clean on
  lint.**
- **Verify against a goal.** Turn the task into a checkable outcome before you start:
  "fix the bug" → "write a failing test that reproduces it, then make it pass"; "add
  validation" → "tests for the invalid inputs pass". For multi-step work, state the short
  plan and the check for each step, then loop until the checks pass.

## Quality Bar — Leave the Codebase Clean (REQUIRED)

After completing any code change, run the project's pre-commit suite and drive it
**fully green** before committing:

```bash
pre-commit run --all-files
```

Resolve **every** issue it reports — failures, lint errors, type errors, and
warnings — **including problems that pre-date your change and were not caused by
your edits**. The standing goal is a clean, working codebase with **no errors and
no warnings**. Do not silence checks (`# noqa`, `# type: ignore`, `SKIP=`,
`--no-verify`) to force green unless the exception is already documented in this
file as a known, unavoidable limitation. Only commit once `pre-commit run
--all-files` passes cleanly; if a check legitimately cannot pass, stop and explain
why rather than bypassing it.

## Working with Git Worktrees (multi-session)

Multiple agents/sessions work the `agent-packages/*` repos concurrently. **Do not
edit the canonical checkout** (`/home/apps/workspace/agent-packages/<repo>`) — a
background `repository-manager` sync can reset its working tree and discard
uncommitted edits. Take your own git worktree on your own branch instead:

```bash
# preferred — repository-manager MCP:
rm_worktree add <repo> <your-branch>      # -> /home/apps/worktrees/<repo>/<your-branch>

# raw-git fallback:
git -C agent-packages/<repo> checkout main
git -C agent-packages/<repo> worktree add /home/apps/worktrees/<repo>/<branch> -b <branch>
```

Work in the worktree and **commit often** (commits survive a working-tree reset).
Each session must use a **distinct branch** — git allows a branch in only one
worktree, which is what keeps concurrent sessions from colliding. Worktrees live
under `/home/apps/worktrees/` (outside the workspace scan, so the sync leaves them
alone).

**Finishing work in a worktree** — run this sequence before calling it done:
1. **Pre-commit green** — `pre-commit run --all-files`; resolve every issue per the
   Quality Bar above (including pre-existing), no `--no-verify`.
2. **Commit** in the worktree.
3. **Merge to main locally** — `rm_worktree merge <repo> <branch> --into main`
   (or `git merge --no-ff`). Push only when the user asks.
4. **Clean up** — remove the worktree and delete the merged branch:
   `rm_worktree remove <repo> <branch> --delete-branch`; `rm_worktree prune` clears
   stale entries. (Raw-git: `git worktree remove <path> && git branch -d <branch>`.)
