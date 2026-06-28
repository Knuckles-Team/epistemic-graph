# AGENTS.md — Epistemic Graph Compute Engine

> Claude Code loads this file via `CLAUDE.md` (`@AGENTS.md` import) — the two stay
> in sync. Edit **this** file, not `CLAUDE.md`.

> **Project Name**: `epistemic-graph`
> **Ecosystem Prefix**: `EG` / `EPG`
> **Key Concepts**: `CONCEPT:KG-2.16` (High-Performance Graph Compute Engine), `CONCEPT:KG-2.19` (Tokio Service Layer), `CONCEPT:KG-2.20` (Rust-Native Finance), `CONCEPT:KG-2.22` (Data Science Primitives), `CONCEPT:KG-2.23` (Rust-Accelerated Reasoning), `CONCEPT:KG-2.51` (Lock-Free Compute + Engine Observability)

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

## Durability model — redb-authoritative is the DEFAULT (CONCEPT:KG-2.195 — THE FLIP)

**The engine is now a durable SOURCE OF TRUTH out of the box.** When built with the
`redb` feature — which `full`/`node`/`cluster`/`pi` all include — the persist
backend defaults to `redb` and runs in **authoritative mode** whenever a persist
dir is configured. A stock deployment is therefore durable-by-default: an acked
write survives a `kill -9` (proven by `tests/test_redb_authoritative_crash.py::test_stock_default_build_is_durable_by_default`,
which sets NO durability env — only a persist dir). The one-time `.mp`/`.wal` →
redb migration runs automatically on first authoritative boot (old files left as a
backstop).

**Resolution rules** (read once at startup, `src/main.rs`):

- **Backend** = `EPISTEMIC_GRAPH_PERSIST_BACKEND`, default **`redb`**. Force the old
  rebuildable-cache path with `EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot`.
- **Authoritative** = `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` when set, else defaults
  **ON exactly when the redb backend is active** (the `redb` feature is compiled AND
  selected). A `snapshot` build, or a build without the `redb` feature (bare
  `default` / `server`-only — where the `redb` default name silently falls back to
  snapshot), is **not** authoritative and boots clean with **no warning**.
- The mismatch warning fires ONLY when an operator EXPLICITLY sets
  `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE=1` against a build where the redb backend is
  not active. Likewise the "backend not available, falling back to snapshot"
  warning fires ONLY when the operator EXPLICITLY named `redb` in a non-redb build —
  not for the new implicit default.
- **No persist dir** + authoritative ⇒ the engine runs **in-memory only** (every
  durable-record/eviction path short-circuits on the absent backend — no panic, no
  files written) and logs a loud warning that writes are not durable. Set a persist
  dir to make it a source of truth.

The three durability rules that make "authoritative" actually safe (CONCEPT:KG-2.187/
KG-2.191), read once at startup into `ServerState.redb_authoritative`:

- **Commit-before-ack.** A durable mutation is COMMITTED to redb (group-commit
  fsync) BEFORE its Response is acked. Dispatch awaits `record_durable`; a commit
  failure becomes an ERROR response — an acked write is *always* on disk. Many
  concurrent awaiting writers still coalesce into ONE group-commit fsync.
- **Eviction is read-through-safe** (CONCEPT:KG-2.191). The per-graph node cap
  resumes ENFORCING under authoritative mode so memory stays bounded — but WITHOUT
  data loss. eg-core defines a `ReadThrough` seam (`crates/eg-core/src/read_through.rs`);
  the facade implements it over the redb backend's point-read and injects one per
  graph at startup, so `GraphCore::get_node_properties` serves an EVICTED node's
  stored blob from redb on a RAM miss. Eviction is durability-gated: a node is
  dropped from RAM ONLY after a redb read CONFIRMS it is on disk (commit-before-ack
  makes that the common case); a node whose durability can't be confirmed is left
  resident. So an evicted node is still readable and never lost. (Topology/edge
  reconstruction of an evicted node still needs a full `load_all` — the read-through
  seam is node-property granularity, matching what redb `read_node` durably holds.)
- **Backpressure, not drop.** The redb writer's bounded channel BLOCKS for capacity
  (off-reactor) instead of shedding a mutation. A durable write is never silently
  discarded.

**Sharded K-way durable writer (CONCEPT:EG-026).** redb is single-writer-PER-FILE, so
ONE `graph.redb` serialized every tenant's commits onto ONE core. The durable writer
now shards by graph into **K independent redb files** (`graph-<n>.redb`), each with its
OWN writer thread / bounded channel / `Pending` — so the EG-024 micro-linger, the
EG-025 O(1) audit-tail cache, commit-before-ack, group-commit and backpressure all hold
**per shard**, and K cores commit in parallel. A graph ALWAYS routes to the same shard
(`FNV-1a(sanitized_name) % K`), keeping its data + audit chain + group-commit co-located
and single-writer-correct; a transaction stays within one graph (group = txn boundary)
so per-shard atomicity is preserved (no durable commit spans graphs/shards). K =
`clamp(cpu/2, 1, 8)` (env `EPISTEMIC_GRAPH_REDB_SHARDS` overrides). **K=1 is byte-for-byte
the old single-`graph.redb` path** (the Pi degrades here, and existing deployments + the
`.mp`/`.wal`→redb migration are untouched). K is fixed per persist-dir once created —
the on-disk layout is detected and honored at open. Under an active Raft node, K is
forced to 1 (the Raft store wraps the single writer; multi-Raft sharding is M2).

**Snapshot reads off the writer (CONCEPT:EG-027).** redb 4.1 is MVCC: a
`Database::begin_read()` opens a consistent read snapshot that runs CONCURRENTLY with
the single writer (no writer involvement, no commit). The point-read / read-through
path (`read_node`, only hit on a RAM miss — the KG-2.191 eviction read-through) now
serves the evicted node DIRECTLY off a `begin_read()` snapshot on the TARGET SHARD's
shared `Database` (routed by the SAME EG-026 `shard_for`), so a read NEVER routes
through the writer thread's channel and NEVER forces a group-commit. Previously a
read-through was sent as `Cmd::ReadNode` over the writer's blocking channel and
forced a `Durability::Immediate` commit first — coupling reads to the write
bottleneck (worst on a Pi, where eviction/read-through is common, and now worse
cross-shard). **Consistency model: reads see the latest COMMITTED state per shard.**
Commit-before-ack (KG-2.187) guarantees any ACKED write is already committed, so a
`begin_read()` opened after that ack sees it; writes still buffered in the writer's
`Pending` are not yet acked (no happens-before to any reader), so dropping the old
forced commit changes no observable read result. Eviction stays durability-gated (a
node leaves RAM only after redb confirms it on disk), so an evicted node is always
served. The writer thread owns the SOLE strong `Arc<Database>` per shard; the read
path holds a `Weak` and `upgrade()`s it per read (a second handle on the same file
would hit redb's exclusive per-process file lock — so reads share the handle, they do
not re-open). Holding a `Weak` (not a strong clone) keeps the file-lock lifetime
identical to before EG-027: the lock releases exactly when the writer thread exits on
`shutdown`, so an in-process reopen of the same persist dir still succeeds. The
`Cmd::ReadNode` variant + handler are retained but no longer constructed (the writer
loop is left byte-for-byte).

### Opt-in: the rebuildable-cache model (`EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot`)

When the backend is `snapshot` (or the `redb` feature isn't compiled), the engine
is the **fast in-memory cache + compute layer** over an external durable
system-of-record (the **abstracted backend** agent-utilities writes through —
Postgres/pggraph, neo4j, falkordb, or ladybug). Behavior is byte-for-byte the
pre-flip model:

- The local RDB snapshot (`.mp`) + WAL exist purely for **fast warm restart** —
  they bound how much a restart has to recompute, not whether data survives.
- A crashed shard is a **latency event, not data loss**: it re-hydrates from the
  durable backend (or replays its snapshot + WAL). Run the engine under a
  supervisor (systemd / the Python host daemon) that auto-restarts it; restart is
  sub-second for typical shards, so the RTO is small.
- There is **no in-engine replication or consensus** in this mode — that would
  duplicate the durable backend's job. Horizontal scale is client-side HRW
  sharding over independent single-process shards, and per-graph memory is bounded
  by `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH` (LRU eviction back to the durable tier)
  so a shard **degrades instead of OOM-killing every tenant** on it. (The
  cluster-tier opt-in below pairs in-engine Raft with the redb-authoritative store
  for the HA case where the engine itself IS the system-of-record.)
- Durable mutations use fire-and-forget `record()` (write-behind); eviction drops
  the LRU on saturation.

### Opt-in: in-engine Raft replication (CONCEPT:KG-2.188, `raft` feature, cluster tier)

The DEFAULT remains single-node + a rebuildable cache. The `raft` cargo feature
(cluster tier only — `cluster = ["node", "raft"]`; NOT in `full`/`all`, so a
default / `pi` / `full` build links **no openraft** and the Pi contract holds: no
DataFusion AND no openraft) runs the engine as a multi-node, highly-available
cluster that replicates its **authoritative** state via [`openraft`] 0.9. It
**activates only** when built `--features raft` AND configured at runtime:

- `EPISTEMIC_GRAPH_RAFT_NODE_ID` — this node's integer id (absent ⇒ single-node,
  unchanged).
- `EPISTEMIC_GRAPH_RAFT_PEERS` — `id@host:port,…` cluster members (must include
  self). Requires `GRAPH_SERVICE_PERSIST_DIR` (Raft replicates the redb store).

When active, a durable mutation is routed through Raft consensus (the leader's
`client_write`) BEFORE it is applied+acked — the replication barrier. A committed
log entry is applied on **every** node by the SAME `wal::apply` → `GraphCore` +
`record_durable` path a replayed WAL record / a single-node write uses (pairs with
redb-authoritative M2). Followers redirect writes to the leader; leader failover is
automatic. When the feature is off, the dispatch write path is **byte-for-byte** the
single-node path.

**Durable redb Raft log (CONCEPT:KG-2.204).** The Raft log — and the vote + applied
state — live in the SAME `graph.redb` Database as the M2 graph data, keyed by
`(group_id, index)` / `(group_id, key)`. Because the log shares M2's off-reactor
group-commit writer, a log append and its graph mutation **coalesce into ONE
`WriteTransaction` / one fsync**. A restarted node recovers its log tail **locally**
from redb (it no longer needs the leader to refill an un-snapshotted tail). The
separate `raft.redb` sidecar is gone — one shared DB serves M2 + every group's log.

**Multi-Raft scaffold (CONCEPT:KG-2.205).** A `MultiRaft` manager holds N openraft
groups keyed by `GroupId`, each its own state machine + `GraphCore`, **sharing ONE
TCP listener per node** (RPC frames tagged + demuxed by group id) and **ONE shared
`graph.redb`** (composite-key log/meta — not a file per group, the spike's FD-ceiling
fix). A `GroupRouter` maps `graph_name → GroupId`. This increment runs **one group**
(`DEFAULT_GROUP`) so behavior matches the single-group path, but the manager,
routing, group create/open/close lifecycle, and multi-group isolation are exercised
by tests. **Group = transaction boundary:** one graph belongs to one group and a txn
stays inside a group — **no cross-group transactions yet** (documented follow-up
CONCEPT:KG-2.207).

**M2 hardening (CONCEPT:KG-2.265/266/267).** Pooled per-peer Raft connections
(`PeerPool` reuses warm `TcpStream`s across RPCs + groups, KG-2.265),
group-per-tenant-range routing (`GroupRouter` hash ring + `configure_group_ring`,
KG-2.266), and per-group snapshot scoping (`dump_graphs` filtered by the router on
`AppCtx`, KG-2.267) are **done + lib-tested**. The final follow-ups are also done +
lib-tested: **multi-node membership join** (`join_group`/`add_group_member`/
`remove_group_member` via openraft add-learner→change-membership, KG-2.268),
**leader balancing across groups** (`MultiRaft::rebalance_leaders` — deterministic
round-robin `desired_leader` + cooperative claim/yield, KG-2.270; openraft 0.9 has no
graceful `transfer_leader`, so an over-loaded leader yields by disabling its heartbeat),
and **heartbeat coalescing** (`RaftFrame::Batch` + `HeartbeatCoalescer` fold same-peer
heartbeats into one pooled round-trip, KG-2.271). What still needs **real multi-node
hardware**: a native instant leader handoff (openraft 0.10 `trigger_transfer_leader`) and
wiring the coalescer under openraft's live heartbeat cadence. See
`docs/architecture/m2-raft-status.md`.

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
  (+ `hardiman_bouchaud`); **Kyle insider/stealth surveillance**
  (`CONCEPT:KG-2.20k`, distils arXiv:2605.27684): `kyle_lambda` (empirical price
  impact) + `surveillance_risk` (informed-flow share / detection hazard /
  cumulative suspicion / stealth ratio / legal-risk score — defensive
  adverse-selection protection); **sizing** `kelly_fraction`, `bayesian_kelly`,
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

The facade's features are **real** — each passes through to the crate that owns
the code + its heavy deps, so a slim build genuinely drops them (verified by
`cargo tree`: a `--features server` build links neither nalgebra nor tree-sitter).

```toml
# facade epistemic-graph/Cargo.toml
[features]
default     = ["graph", "algorithms", "metrics"]
graph       = []                                      # no-op marker (graph core is always linked via eg-core)
algorithms  = []                                      # no-op marker (always linked via eg-compute)
metrics     = ["dep:prometheus", "eg-types/metrics"]  # Prometheus /metrics listener; on by default
finance     = ["eg-compute/finance", "eg-types/finance"]   # → nalgebra; wire DTOs in eg-types
datascience = ["eg-compute/datascience", "eg-types/datascience"]  # → rand_chacha
reasoning   = ["eg-compute/reasoning"]                # OWL/Datalog inference
ast         = ["eg-compute/ast"]                      # → tree-sitter grammars
# Native eg-ann IVF-PQ+OPQ+SQ8-refine vector index (CONCEPT:KG-2.207) as the
# SemanticStore backend, replacing rebuild-on-load HNSW. Pure-Rust CPU serving —
# NO GPU/faiss/native-ML — so it is folded into pi/node/cluster/full. A persisted
# index reopens WITHOUT rebuilding from raw vectors. `ann-redb` also stores the
# codes in the redb durable tier.
ann         = ["eg-core/ann"]
ann-redb    = ["ann", "redb", "eg-core/ann-redb"]
compute     = ["finance", "ast", "datascience", "reasoning"]
# Tokio service (UDS/TCP). tokio is pinned to the minimal feature set actually
# used (rt-multi-thread, net, io-util, sync, time) — NOT "full".
server      = ["dep:tokio", "dep:clap", "dep:tracing-subscriber"]
# Durable redb store (CONCEPT:KG-2.177). Folded into full/node/cluster/pi so the
# standard build is redb-AUTHORITATIVE by default (CONCEPT:KG-2.195 — THE FLIP).
redb        = ["server", "dep:redb"]
# Engine-level security (CONCEPT:KG-2.231): per-agent Row-Level Security (the
# read/plan-path GraphView filter), encryption-at-rest for the redb value blobs, and a
# hash-chained tamper-evident audit log over the ledger. PURE-RUST: RLS + audit chain
# link only sha2/hmac (RustCrypto, already deps); encryption pulls chacha20poly1305
# (RustCrypto AEAD — NO ring/openssl/C). Implies `redb`. Folded into node/cluster/full;
# OUT of bare default + the lean `pi` tier.
security    = ["redb", "dep:chacha20poly1305", "eg-types/security", "eg-core/security"]
# `full` now pulls `redb` (+ `query`/`cypher`/`security`) so a stock full build is a
# durable, RLS/audit/encryption-capable source of truth. It stays SINGLE-NODE (no raft).
full        = ["compute", "server", "query", "cypher", "redb", "ann", "security"]
```

`eg-compute` is a non-optional dep (its `algorithms` is used by the always-on
graph-op handlers); only its heavy domains + their deps are feature-gated.
The facade declares `crate-type = ["rlib"]` (no `cdylib`/pyo3; maturin
`bindings = "bin"`).

---

## Module Structure — a Cargo workspace along the dependency DAG

The engine is a **Cargo workspace**; member crates map 1:1 to the acyclic
dependency DAG `eg-types → eg-ann → eg-core → eg-compute → epistemic-graph`
(`eg-ann` is a leaf used by `eg-core` under the `ann` feature; `eg-query` is the
optional SQL/Cypher surface depending on `eg-core`). A crate may only `use`
crates to its left; a cycle won't compile, which is the enforcement.

```
crates/
├── eg-types/        # lib eg_types — BOTTOM of the DAG; deps = serde family only
│   ├── protocol.rs  #   Length-prefixed MessagePack: Request/Response/Method + ResultPayload
│   ├── types.rs     #   Typed node/edge data model (lifecycle, embeddings, metadata)
│   ├── wire.rs      #   Pure-data DTOs the protocol embeds: Order/YearData (finance),
│   │                #     EstimatorParams/FittedModel/DecisionTree/TreeNode (datascience) — feature-gated
│   └── acl.rs       #   AgentRole/AgentIdentity (RegisterIdentity carries them over the wire)
├── eg-ann/          # lib eg_ann — native IVF-PQ + OPQ + SQ8-refine vector index
│   │                #     (CONCEPT:KG-2.207). Leaf crate (serde/memmap2/rayon/rand; redb opt).
│   ├── ivfpq.rs     #   IVF-PQ core: OPQ rotation, PQ codes (ADC), SQ8 refine tier
│   ├── kmeans.rs · linalg.rs   #   k-means++ + dependency-free Jacobi-SVD (OPQ R update)
│   ├── persist.rs   #   mmap format — no-rebuild reopen + compaction (VACUUM)
│   └── redb_store.rs   #   optional redb-durable codes (feature `redb`)
├── eg-core/         # lib eg_core — graph engine core; depends on eg-types (+ eg-ann under `ann`)
│   ├── graph.rs     #   GraphCore: petgraph-backed graph + ledger; topology/analysis snapshots
│   │                #     (heavy read-only compute runs off the graph lock)
│   ├── registry.rs  #   Multi-tenant graph registry
│   ├── isolation.rs #   Zero-trust agent isolation / ACL
│   └── compute/semantic.rs  # SemanticStore: brute-force + HNSW (default) | eg-ann IVF-PQ (feature `ann`)
├── eg-compute/      # lib eg_compute — compute domains; depends on eg-types + eg-core
│   ├── algorithms.rs     # PageRank, centrality, BFS/DFS, components, MST (ALWAYS compiled)
│   ├── ast/ + parser/    # tree-sitter multi-language parser → KG symbols (feature `ast`)
│   ├── finance/          # optimizer (black_litterman via nalgebra), risk, regime, signals, exchange (feature `finance`)
│   ├── datascience/      # estimators + primitives: OLS/Ridge/Lasso/trees/SVR (feature `datascience`)
│   └── reasoning.rs      # Datalog closure: transitive/symmetric/inverse/domain-range/chains (feature `reasoning`)
└── (workspace root = the facade)

src/                 # the `epistemic-graph` FACADE crate (lib epistemic_graph) — TOP of the DAG:
├── lib.rs           #   re-exports eg-{types,core,compute} under the historical crate:: paths,
│                    #     then declares the server-side modules below (server feature)
├── main.rs          #   epistemic-graph-server entrypoint (the maturin bindings="bin" wheel target)
├── server/          #   Tokio UDS/TCP server, DECOMPOSED (see dispatch conventions below):
│                    #     dispatch.rs (thin routing table) + handlers/{graph_ops,finance,datascience}.rs
│                    #     + state/auth/access/compute/transport.rs
├── metrics.rs       #   Prometheus metrics + /metrics listener (feature `metrics`)
├── channels.rs      #   Agent communication channels
├── persist.rs · persist_lock.rs · wal.rs · wal_service.rs   # durable WAL + checkpoints + single-writer lock

epistemic_graph/     # pure-Python client package
├── client.py        # EpistemicGraphClient / SyncEpistemicGraphClient (framed MessagePack + HMAC)
├── pool.py          # ConnectionPool + ShardRouter (HRW)
└── quant.py         # pure-Python rolling stats / order matching
scripts/             # check_no_pyo3.sh, run_shards.sh, bench_transport.py
docs/benchmarks.md   # measured p50/p99 latency
```

> The server lib + the bin are 1:1 (the bin only exists with the `server`
> feature), so they deliberately share the facade crate rather than splitting an
> `eg-server` crate — a boundary that would carry no consumer and only entangle
> the `server`/`metrics` features across crates. `[profile.release]` lives on the
> **workspace root** `Cargo.toml` (it is only honored there).

---

## Workspace & server dispatch conventions

These are the rules that keep the engine from re-forming into a monolith as it
grows. Each is tied to a mechanical CI gate (a rule without a gate is a comment).

1. **Crates mirror the DAG** `eg-types → eg-core → eg-compute → epistemic-graph`.
   A new shared/wire type → `eg-types`; a new graph-core capability → `eg-core`; a
   new compute domain → `eg-compute`. Imports point left only — never make a lower
   crate depend on a higher one. *Gate:* the workspace graph (a cycle won't build).

2. **Dispatch is a thin routing table.** `src/server/dispatch.rs` is a labeled
   `'dispatch` block that routes each `Method` to a `handlers::<domain>::try_handle`
   (`Ok(resp)` = handled, `Err(method)` = not mine, fall through). The post-match
   write side-effects (in-flight gauge, `mark_dirty`, WAL enqueue) stay
   **centralized in the shell** so every write handler gets durability for free and
   it cannot drift per-domain. **No business logic in a routing arm** — logic lives
   in `handlers::<domain>`. *Gate:* clippy + the handler tests.

3. **One handler module per domain**, 1:1 with the `// ── <domain> ──` sections in
   `protocol.rs`. A new domain ⇒ a new `handlers/<domain>.rs`, not another arm in an
   existing file.

4. **Feature-gating gates three sites** (the `ast` precedent): the crate/feature
   wiring (`eg-compute/<domain>` + `eg-types/<domain>` if it has wire DTOs), the
   handler `mod` (`#[cfg(feature=…)] pub(crate) mod <domain>;`), and the dispatch
   routing. A gated-out method's variant stays in the enum and **must** fall to the
   explicit "not available in this server build" catch-all — never a panic, never a
   silent mis-route. *Gate:* `test_gated_out_method_returns_not_built` (slim-server row).

5. **Wire DTOs live in `eg-types`, behavior lives upstream.** When the protocol
   enum must embed a compute type, the pure-data struct/enum goes in
   `eg-types::wire` (feature-gated) and the domain module re-exports it
   (`pub use eg_types::wire::Order;`) — the data sits at the bottom of the DAG, the
   algorithm stays in `eg-compute`. Do **not** pull a heavy dep (nalgebra,
   tree-sitter) into a default build to satisfy a type: gate it.

6. **Adding a capability (5 steps):** (1) implement it in the `eg-compute`/`eg-core`
   domain module; (2) add the `Method` variant in the matching `protocol.rs` section
   (eg-types), cfg-gated if the domain is feature-gated; (3) add the handler fn in
   `handlers/<domain>.rs`; (4) add the **one-liner** routing arm in `dispatch.rs`
   (with the not-built fallback if gated); (5) add the `epistemic_graph/client.py`
   method + a `tests/` round-trip and a co-located `#[cfg(test)]` dispatch test.

7. **The protocol enum stays flat + section-commented.** Nesting into
   `Method::Finance(FinanceMethod)` is wire-breaking (the Python client mirrors flat
   method-name strings) — defer it behind a hard trigger (protocol.rs > ~2000 lines
   AND a protocol-v2 client cutover already in scope).

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC-SHA256 secret for the Tokio service (alias: `EPISTEMIC_GRAPH_SECRET` via `run_shards.sh`). **Required** — the server refuses to start with an empty secret |
| `EPISTEMIC_GRAPH_ALLOW_INSECURE` | `1`/`true`: explicit opt-out allowing an empty auth secret (development only; prominent warning at startup) |
| `GRAPH_SERVICE_SOCKET` | Path to the UDS socket |
| `GRAPH_SERVICE_PERSIST_DIR` | Persist dir (alias `--persist-dir`). When set with a redb-bearing build, the engine is a durable source of truth; absent ⇒ in-memory only |
| `EPISTEMIC_GRAPH_PERSIST_BACKEND` | `redb` (**default**, CONCEPT:KG-2.195) = durable authoritative store; `snapshot` = opt-in rebuildable-cache (snapshot RDB + WAL). A `redb` request in a build without the `redb` feature silently falls back to snapshot |
| `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` | Override authoritative mode. Unset ⇒ defaults ON when the redb backend is active (full/node/cluster/pi). Set `1` against a non-redb build ⇒ warns + ignored |
| `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` | Adaptive group-commit micro-linger (CONCEPT:EG-024), microseconds (default `1000` = 1ms; `0` disables = legacy commit-on-drain). When the `eg-redb-writer` is about to commit a SHALLOW barrier batch it spends ONE bounded `recv_timeout(linger)` letting concurrent in-flight authoritative writers fold into the SAME fsync (the profiled write ceiling was ~1 op/fsync from serial awaits). Durability is unchanged — writes still commit `Durability::Immediate` before their ack; only the batch widens |
| `EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW` | Shallow-batch op threshold for the EG-024 micro-linger (default `32`, clamped 1..4096). The writer lingers only while `pending.ops.len()` is below this; a deeper batch already coalesces, so it commits immediately (adaptive — no added latency on a deep queue) |
| `EPISTEMIC_GRAPH_REDB_SHARDS` | **Sharded K-way durable writer (CONCEPT:EG-026).** Number of independent redb files / writer threads K, overriding the auto-size `clamp(cpu/2, 1, 8)` (clamped 1..64). Each graph routes to a fixed shard by `FNV-1a(sanitized_name) % K`, so a 64-core box commits on K cores in parallel instead of one. **K=1 is byte-for-byte the pre-EG-026 path: ONE file `graph.redb`**; K>1 uses `graph-<n>.redb`. K is FIXED per persist-dir once created — the on-disk layout is detected + honored at open (changing K needs a migration; never strands data). Forced to **1 under an active Raft node** (multi-Raft sharding is M2) |
| `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` | **Auto-sized early-flush op threshold (CONCEPT:EG-028b)**, per shard — replaces the old hardcoded `4096`. The writer flushes a `Pending` batch early once it holds this many ops (bounds writer RAM before the bounded channel saturates). Default ≈ half the WAL queue depth, clamped 256..16384 (with the legacy default queue 8192 this is exactly `4096` — unchanged; small on a Pi, large on a big box). Override clamped 64..1_048_576 |
| `GRAPH_SERVICE_ENDPOINTS` | Comma-separated shard endpoints for the Python `ShardRouter` |
| `EPISTEMIC_GRAPH_PGWIRE_ADDR` | When set (build `--features pgwire`), the pg-wire listener binds this address (documented loopback `127.0.0.1:5433`). Unset ⇒ no listener. A connecting driver/ORM introspects a SYNTHETIC read-only catalog (CONCEPT:KG-2.201: DataFusion `information_schema` + a supplemented `pg_catalog` `pg_namespace`/`pg_class`/`pg_attribute`/`pg_type` + `version()`/`current_schema()`/`current_database()`) then runs SQL over `nodes`/`edges` |
| `EPISTEMIC_GRAPH_PGWIRE_GRAPH` | Default graph a fresh pg-wire connection runs against when the libpq `database` param is unset. Defaults to `__commons__` |
| `EPISTEMIC_GRAPH_PGWIRE_AUTH` | pg-wire auth mode (CONCEPT:KG-2.202): `scram` (SCRAM-SHA-256, what modern drivers negotiate) or `trust` (no auth, dev). DEFAULT = `scram` when `GRAPH_SERVICE_AUTH_SECRET` is set, else `trust`. SCRAM maps the pg `user` → an engine `agent_id`; the password is `hex(HMAC-SHA256(secret, "pgwire:"+user))`; a successful login sets the connection's ACL actor so queries run under that `AgentIdentity` (`IsolationLayer::check_access`) |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` | Server backpressure cap (default 1024); excess → `BUSY` |
| `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES` | Overload backstop for the unbounded `GetNodes` full-graph dump (CONCEPT:KG-2.264, default 50_000, read once at startup). When a graph exceeds this cap, `GetNodes` returns a typed `RESULT_TOO_LARGE` error (client raises `ResultTooLargeError`) telling the caller to use a bounded query (`get_nodes_by_label(label, limit)` / pagination) INSTEAD of serializing a gigabyte-scale frame that resets the connection. `0` disables the guard (legacy unbounded behavior). Bounded reads (`GetNodesByLabel`, per-id) are unaffected |
| `EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS` | Reference-counted idle shutdown (CONCEPT:KG-2.223, alias `--idle-shutdown-secs`). `N>0` ⇒ the engine self-terminates (checkpointing cleanly) after N seconds with ZERO active connections — the shared tiny-daemon mode agent-utilities' EngineResolver autostarts. **Absent or `0` (default) ⇒ NEVER self-terminate on idle: long-living/persistent, runs forever like a normal server.** SIGTERM/SIGINT graceful checkpointed shutdown works in BOTH modes |
| `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS` | Cold-tenant idle offload sweep (CONCEPT:EG-040, R6, feature `redb`). `N>0` ⇒ a periodic task (every N seconds, reusing the engine's existing interval-task cadence — no new daemon) hibernates every graph idle longer than N seconds (access recency is `touch`ed on the dispatch read/write path), bounding RAM across many tenants. Durability-gated + read-through-safe (KG-2.191) so an offloaded graph is never lost, only evicted; `__commons__` is never offloaded. **Absent or `0` (default) ⇒ disabled** (no proactive idle offload; the KG-2.234 budget enforcer still runs) |
| `EPISTEMIC_GRAPH_TENANT_CATALOG` | Tenant-catalog auto-attach gate (CONCEPT:EG-033, R5, feature `redb`). `1`/`true`/`yes`/`on` ⇒ `RedbBackend::open` attaches the durable `catalog.redb` tenant→shard override map to the live routing seam (also auto-attached when a populated `catalog.redb` already exists). **Default OFF** ⇒ pure EG-026 FNV-1a routing. An attached-but-empty catalog routes identically, so enabling it is a no-op until the M3 admin RPC (CONCEPT:EG-038) assigns a placement / runs an online reshard |
| `GRAPH_SERVICE_METRICS_ADDR` | Prometheus `/metrics` HTTP listener address (alias of `--metrics-addr`, e.g. `127.0.0.1:9101`). Disabled when unset; requires the `metrics` cargo feature (on by default) |
| `EPISTEMIC_GRAPH_ENCRYPTION_KEY` | Encryption-at-rest key material (CONCEPT:KG-2.231, feature `security`). When set, the redb durable **value** blobs (node/edge property + semantic store) are sealed with a pure-Rust ChaCha20-Poly1305 AEAD (RustCrypto — NO ring/openssl) — raw `.redb` bytes hold no plaintext properties. Keys stay plaintext so range scans work. **Default OFF / opt-in** (changes the on-disk format). `ValueCipher::from_env` is the KMS hook seam (swap it for a data-key fetch). A wrong key fails the read (never silent plaintext) |
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

## No Legacy — no back-compat, update every consumer, delete the old path

**We own every consumer.** Everything that calls this engine lives under
`agent-packages/*` (the `epistemic_graph` Python client, agent-utilities,
data-science-mcp, the `agents/*` connectors). There is no external caller pinned
to an old version, so we **do not carry backward compatibility**: no deprecated
protocol methods, no client-API aliases, no `*_legacy`/`*_compat` symbols, no
fallback branches, no "kept for existing deployments" code.

When you change a shared contract — a `Method`/`ResultPayload` wire shape, the
client API, an env/flag name, a feature set — make it **atomic across the
ecosystem**: grep every consumer under `agent-packages/`, update them in the same
change, and **delete the old path** (a left-behind legacy reference is a bug, not
compatibility). No deprecation window — this is the aggressive form of
strangler-then-delete: skip the strangle, migrate-and-delete in one commit.

**The one exception is persisted on-disk state** (snapshots `.mp`, the WAL,
`manifest.json`): a format change may need a **one-time data migration**
(read-old → write-new), after which the old-format reader is removed. That is data
migration, not API back-compat — never a permanent dual-format reader.

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
