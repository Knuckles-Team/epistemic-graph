# AGENTS.md — Epistemic Graph Compute Engine

> Claude Code loads this file via `CLAUDE.md` (`@AGENTS.md` import) — the two stay
> in sync. Edit **this** file, not `CLAUDE.md`.

> **Project Name**: `epistemic-graph`
> **Ecosystem Prefix**: `EG` / `EPG`
> **Key Concepts**: `CONCEPT:EG-KG.compute.graph-compute-engine` (High-Performance Graph Compute Engine), `CONCEPT:EG-KG.query.wire-protocol` (Tokio Service Layer), `CONCEPT:AU-KG.memory.mementified-context` (Rust-Native Finance), `CONCEPT:EG-KG.compute.rust-native-training-loss` (Data Science Primitives), `CONCEPT:AU-KG.domains.legal-automation` (Rust-Accelerated Reasoning), `CONCEPT:EG-KG.txn.per-graph-write-isolation` (Lock-Free Compute + Engine Observability)

---

## Overview

Unified Rust-native computation engine for the agent-packages ecosystem.
Consolidates graph operations, quantitative finance, data science, AST analysis,
and OWL reasoning into a single high-performance binary.

**Transport (important — this changed):** the engine is exposed to Python
**out-of-process** via a long-running Tokio service speaking **length-prefixed
MessagePack over Unix Domain Sockets (default) or TCP**, authenticated with
**HMAC-SHA256**. `eg2.` is the sole request envelope: every served request carries
a verified principal, tenant, audience, effective agent, policy version, scopes,
timestamp, nonce, and idempotency key. The server requires the `security` feature,
a non-empty signing secret, deployment policy values, a durable replay ledger,
and a signer-key registry before it opens a listener (see
`docs/service_mode.md#authentication-protocol`). There is **NO PyO3 / in-process extension** — that coupling was
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

## Durability model — redb-authoritative is the DEFAULT (CONCEPT:AU-KG.backend.backend-modes — THE FLIP)

**The engine is now a durable SOURCE OF TRUTH out of the box.** When built with the
`redb` feature — which the main build (`default`/`full`) and the `cluster` layer all
include — the persist
backend defaults to `redb` and runs in **authoritative mode** whenever a persist
dir is configured. A stock deployment is therefore durable-by-default: an acked
write survives a `kill -9` (proven by the authoritative-store crash suite,
which sets no durability override — only a persist dir).

**Resolution rules** (read once at startup, `src/main.rs`):

- Every served build includes `redb` and uses it as the authoritative backend.
- `GRAPH_SERVICE_PERSIST_DIR` is mandatory before a listener opens; it contains
  both graph durability and the request replay ledger.
- A served mutation always follows commit-before-ack. There is no write-behind,
  in-memory-only, or alternate snapshot persistence mode.

The three durability rules that make "authoritative" actually safe (CONCEPT:EG-KG.backend.authoritative-dispatch/
EG-KG.storage.read-through-seam-exercised), attached once at startup as the authoritative store:

- **Commit-before-ack.** A durable mutation is COMMITTED to redb (group-commit
  fsync) BEFORE its Response is acked. Dispatch awaits `record_durable`; a commit
  failure becomes an ERROR response — an acked write is *always* on disk. Many
  concurrent awaiting writers still coalesce into ONE group-commit fsync.
- **Eviction is read-through-safe** (CONCEPT:EG-KG.storage.read-through-seam-exercised). The per-graph node cap
  resumes ENFORCING under authoritative mode so memory stays bounded — but WITHOUT
  data loss. eg-core defines a `ReadThrough` seam (`crates/eg-core/src/read_through.rs`);
  the facade implements it over the redb backend's point-read and injects one per
  graph at startup, so `GraphCore::get_node_properties` serves an EVICTED node's
  stored blob from redb on a RAM miss. Eviction is durability-gated: a node is
  dropped from RAM ONLY after a redb read CONFIRMS it is on disk (commit-before-ack
  makes that the common case); a node whose durability can't be confirmed is left
  resident. So an evicted node is still readable and never lost. (Topology/edge
  reconstruction and whole-graph scans use the bounded, incarnation-fenced lazy-page
  materializer; they never require an unbounded request-path `load_all`.)
- **Backpressure, not drop.** The redb writer's bounded channel BLOCKS for capacity
  (off-reactor) instead of shedding a mutation. A durable write is never silently
  discarded.

**Sharded K-way durable writer (CONCEPT:EG-KG.backend.sharded-k-way-durable).** redb is single-writer-PER-FILE, so
ONE redb file serialized every tenant's commits onto ONE core. The durable writer
now shards by graph into **K independent redb files** (`graph-<n>.redb`), each with its
OWN writer thread / bounded channel / `Pending` — so the EG-024 micro-linger, the
EG-KG.storage.embedded-store O(1) audit-tail cache, commit-before-ack, group-commit and backpressure all hold
**per shard**, and K cores commit in parallel. A graph ALWAYS routes to the same shard
(`FNV-1a(sanitized_name) % K`), keeping its data + audit chain + group-commit co-located
and single-writer-correct; a transaction stays within one graph (group = txn boundary)
so per-shard atomicity is preserved (no durable commit spans graphs/shards). K =
`clamp(cpu/2, 1, 8)` (env `EPISTEMIC_GRAPH_REDB_SHARDS` overrides). **K=1 is the
single-shard layout** for constrained hosts. K is fixed per persist-dir once created —
the on-disk layout is detected and honored at open. **Under an active Raft node K == N
Raft groups (ADR-2 / W1.2 — `reports/wave1/ADR-scale-trio.md` §ADR-2): raft group *g*
owns redb shard *g***, so HA and write-scaling coexist (N groups = N parallel durable
writers per node). K then follows `EPISTEMIC_GRAPH_RAFT_GROUPS` (default cores-derived up
to `MAX_SHARD_COUNT`=64); `EPISTEMIC_GRAPH_REDB_SHARDS` does NOT apply under raft. An
existing K=1 raft store stays K=1 (all groups on shard 0, exactly the pre-ADR-2 behavior)
until the offline `migrate-shards` tool rewrites its layout.

**Snapshot reads off the writer (CONCEPT:EG-KG.storage.snapshot-read-off-writer).** redb 4.1 is MVCC: a
`Database::begin_read()` opens a consistent read snapshot that runs CONCURRENTLY with
the single writer (no writer involvement, no commit). The point-read / read-through
path (`read_node`, only hit on a RAM miss — the EG-KG.storage.read-through-seam-exercised eviction read-through) now
serves the evicted node DIRECTLY off a `begin_read()` snapshot on the TARGET SHARD's
shared `Database` (routed by the SAME EG-KG.backend.sharded-k-way-durable `shard_for`), so a read NEVER routes
through the writer thread's channel and NEVER forces a group-commit.
**Consistency model: reads see the latest COMMITTED state per shard.**
Commit-before-ack (KG-2.187) guarantees any ACKED write is already committed, so a
`begin_read()` opened after that ack sees it; writes still buffered in the writer's
`Pending` are not yet acked (no happens-before to any reader). Eviction stays durability-gated (a
node leaves RAM only after redb confirms it on disk), so an evicted node is always
served. The writer thread owns the SOLE strong `Arc<Database>` per shard; the read
path holds a `Weak` and `upgrade()`s it per read (a second handle on the same file
would hit redb's exclusive per-process file lock — so reads share the handle, they do
not re-open). Holding a `Weak` (not a strong clone) keeps the file-lock lifetime
identical across clean shutdown/reopen: the lock releases exactly when the writer
thread exits on `shutdown`, so an in-process reopen of the same persist dir succeeds.

### Opt-in: in-engine Raft replication (CONCEPT:AU-KG.ingest.source-sync-canonical, `raft` feature, `cluster` layer)

The default is a single-node authoritative database. The `raft` cargo feature
(the opt-in `cluster` build layer only — `cluster = ["full", "raft", …]`; NOT in
`default`/`full`/`all`, so the main build links **no openraft**, asserted by
`cargo tree`) runs the engine as a multi-node, highly-available
cluster that replicates its **authoritative** state via [`openraft`] **0.10** (the v2
split-storage API + native graceful leader transfer — CONCEPT:AU-KG.backend.authority-has-already-acked). It
**activates only** when built `--features raft` AND configured at runtime:

- `EPISTEMIC_GRAPH_RAFT_NODE_ID` — this node's integer id (absent ⇒ single-node).
- `EPISTEMIC_GRAPH_RAFT_PEERS` — `id@host:port,…` cluster members (must include
  self). Requires `GRAPH_SERVICE_PERSIST_DIR` (Raft replicates the redb store).
- `EPISTEMIC_GRAPH_RAFT_BIND_ADDR` (optional) — bind the Raft listener here (e.g.
  `0.0.0.0:9100`) while still ADVERTISING the host IP from `PEERS`; needed for a
  containerized member that cannot bind its host's external IP directly.

Multi-node deploy (the 4-node fleet cluster + the live single-node→cluster data
migration) is `services/epistemic-graph/flavors/cluster.env` +
`docs/architecture/cluster-deployment.md`. **Under an active Raft node K == N Raft
groups (ADR-2 / W1.2, EG-KG.sharding.raft-resharding): raft group *g* owns redb shard
*g*, so N groups run N parallel apply loops + N durable shard writers per node** — HA and
write-scaling coexist, with `MultiRaft::rebalance_leaders` spreading leaders across nodes.
K follows `EPISTEMIC_GRAPH_RAFT_GROUPS` (default cores-derived up to `MAX_SHARD_COUNT`=64).
The per-shard cost is one open file descriptor + one writer thread per group, so a very
high N trades RAM/FDs for write parallelism.

When active, a durable mutation is routed through Raft consensus (the leader's
`client_write`) BEFORE it is applied+acked — the replication barrier. A committed
log entry is applied on **every** node by the same canonical mutation applier and
committed through the authoritative redb path used by a single-node write. Followers
redirect writes to the leader; leader failover is
automatic. When the feature is off, the dispatch write path is **byte-for-byte** the
single-node path.

**Durable redb Raft log (CONCEPT:EG-KG.storage.one-fsync-covers-raft).** The Raft log — and the vote + applied
state — live in the SAME authoritative shard Database as the M2 graph data, keyed by
`(group_id, index)` / `(group_id, key)`. Because the log shares M2's off-reactor
group-commit writer, a log append and its graph mutation **coalesce into ONE
`WriteTransaction` / one fsync**. A restarted node recovers its log tail **locally**
from redb (it no longer needs the leader to refill an un-snapshotted tail). The
separate `raft.redb` sidecar is gone — one shared DB serves M2 + every group's log.

**Multi-Raft groups (CONCEPT:EG-KG.sharding.raft-resharding, ADR-2 / W1.2).** A `MultiRaft` manager holds N
openraft groups keyed by `GroupId`, each its own state machine + `GraphCore`, **sharing
ONE TCP listener per node** (RPC frames tagged + demuxed by group id) and each owning its
own durable shard (`RedbBackend::shard_for_group(g) = shard[g % K]`, composite-key
log/meta — group *g* owns shard *g*). A `GroupRouter` maps `graph_name → GroupId` via
`FNV-1a(sanitize(graph_name)) % N`, the SAME hash `shard_index` uses, so a graph's
consensus group and its durable shard are the same — the ADR-2 alignment that keeps a
group's log co-located with its data (one shard = one writer). The default sizes N to the
cores-derived group count (like the non-raft shard auto-size), collapsing to **one group**
(`DEFAULT_GROUP`, shard 0) on a constrained host; `EPISTEMIC_GRAPH_RAFT_GROUPS` overrides,
creating every configured group with the complete peer set, so all groups have quorum
replication and failover. One graph-local transaction belongs to one group; a cross-group
request is coordinated by the dedicated cross-shard transaction protocol rather than
smuggled through a graph-local transaction.

Cross-group reads use the same authenticated, multiplexed `PeerPool` as consensus:
each leg is a bounded durable keyset page preceded by that group's ReadIndex and
fenced by the catalog `(group, epoch)`. Results state per-group linearizability
explicitly; independent group barriers are never described as a global snapshot.
Online partition moves are driven only by the placement leader and persist an
integrity-checked, monotonic move journal before every side effect. Pre-fence aborts
restore the source; an abort racing a committed epoch fence reconciles forward.

**M2 hardening (CONCEPT:AU-KG.ontology.manage-arbitrary/266/267).** Pooled per-peer Raft connections
(`PeerPool` reuses warm `TcpStream`s across RPCs + groups, AU-KG.ontology.manage-arbitrary),
group-per-tenant-range routing (`GroupRouter` hash ring + peer-aware `configure_group_ring`,
KG-2.266), and per-group snapshot scoping (`dump_graphs` filtered by the router on
`AppCtx`, AU-KG.ingest.staged) are **done + lib-tested**. The final follow-ups are also done +
lib-tested: **multi-node membership join** (`join_group`/`add_group_member`/
`remove_group_member` via openraft add-learner→change-membership, EG-KG.storage.kg-kg-2),
**leader balancing across groups** (`MultiRaft::rebalance_leaders` — deterministic
round-robin `desired_leader` + the **native graceful `trigger().transfer_leader(target)`**
handoff, AU-KG.backend.authority-has-already-acked; replaces the old 0.9 cooperative heartbeat-yield), and **heartbeat
coalescing** (`RaftFrame::Batch` + `HeartbeatCoalescer` fold same-peer heartbeats into
one pooled round-trip, EG-KG.storage.concept-2). The openraft **0.9→0.10 migration** (AU-KG.backend.authority-has-already-acked) moved
`src/raft/` to the v2 split storage (`RaftLogStorage` + `RaftStateMachine` on
`Arc<EgStore>`, no `Adaptor`; `io::Error` returns; stream-based `apply`; full-snapshot
transfer) and `RaftNetworkV2`. Validate the cluster mechanism (formation / replication /
failover / **native transfer** / durable log) on throwaway loopback nodes with
`scripts/validate-raft-cluster.sh`. What still needs **real multi-node hardware**: wiring
the coalescer under openraft's live heartbeat cadence + a cross-host soak. See
`docs/architecture/m2-raft-status.md` and `docs/architecture/cluster-deployment.md`.

---

## Epistemic OS surfaces in the main build

The default build is `full`. It carries the governed epistemic, evidence, analytics,
program-optimization, KnowledgeBatch, and document/media serving surfaces described
below. Cargo features remain useful as compile-time ownership boundaries, but these
features are not operator opt-ins in the main artifact. The machine-generated method
ledger in `docs/capabilities.generated.md` and the feature-contract check are the
authoritative inventory.

- **Epistemic reasoning (`eg-epistemic`, features `epistemic`/`epistemic-tms`/
  `epistemic-redaction`).** Claim/Evidence/BeliefState + confidence propagation and the
  `EVIDENCE FOR`/`BELIEF AS OF`/… UQL ops (`epistemic`, shipped 2.16.0); paraconsistent
  truth-maintenance + Dung argumentation, with a durable per-graph incremental
  projection that marks dependents `Stale` from committed
  `RemoveNode`/`RemoveEdge`/`CompareAndSetNodeFields` events, plus the bitemporal
  `Method::EpistemicStatus`/`Method::WhatChanged` capstone (`epistemic-tms`); the
  grounded/preferred/stable argumentation semantics themselves are ALSO reachable
  standalone (not just composed inside `EpistemicStatus`) via `Method::ResolveConflict`
  (EPI-P3-7, gap-fill — `client.query.resolve_conflict(node_ids, semantics=...)`);
  policy-aware proof redaction via `Method::ExplainBelief`'s `disclosure_level`, reusing the same
  per-agent RLS check every read path enforces (`epistemic-redaction`; Python-client-bound via
  `client.query.explain_belief(node_id, disclosure_level=...)`). **Calibrated causal
  reasoning** (`eg-epistemic::{causal,ranking}` — genuine Pearl do-calculus: `observe`/
  `intervene`/`counterfactual`, plus provenance-aware retrieval ranking) is facade-reachable
  through `Method::CausalEstimate`'s `mode`
  (EPI-P3-6 — `Intervene`, the `#[serde(default)]`, or `Observe`), `Method::CausalCounterfactual`
  (Pearl's point-counterfactual, a deterministic point value per variable rather than a
  calibrated distribution), and `Method::RankByProvenance` — all three Python-client-bound
  (`client.query.causal_estimate(..., mode=...)`/`causal_counterfactual`/`rank_by_provenance`).
  `epistemic`, `epistemic-redaction`, `epistemic-tms`, `epistemic-causal`, and
  `evidence-graph` are all in `full`.
- **Multimodal evidence graph (X-1).** `eg_modality::EvidenceLocus` carries 11
  governed located-evidence address kinds spanning document/table/image/audio/
  video/metric/row/code/trace sources and is reachable under the `epistemic`
  feature. Its citation resolver
  (`eg-epistemic::evidence`) is facade-reachable via
  `Method::ExplainEvidence` (resolves a Claim's evidence to its exact governed
  loci). The `alignment` feature's
  `CasEvidenceResolver`
  (`src/server/blob/cas_resolver.rs`) returns an actual UTF-8 excerpt for text/table
  addresses and a CAS-digest reference for every other locus kind. Native document,
  image, audio, and video decoding and extraction live in their served modality
  runtimes; evidence resolution remains a provenance lookup and does not duplicate
  those decoders.
- **Document and media serving (`eg-document`/`eg-image`/`eg-audio`/`eg-video`/
  `eg-alignment`, EG-P1-3).** The `modality-serving` feature is in `full` and exposes
  governed ingest, typed native query, delete, cold/restore, event, and capability
  operations through `ServedModality`. Document, image, audio, and video runtimes
  perform bounded native decoding and extraction, build derived lexeme/spatial/
  perceptual/temporal indexes, use stable Artifact/Occurrence/Rendition/Segment/
  Feature/EvidenceLocus identities, and commit through the authoritative mutation
  boundary. Durable modality content addresses are SHA-256; document lexemes are
  authority-keyed opaque values and source bodies remain request-local.
- **Mandatory modality TCK (EG-P1-1).** Production modality runtimes must report 12/12
  PASS, zero N/A results, and a passing native production probe covering codec,
  normalization, secondary indexing, typed query, and malformed/resource-bound
  rejection. The served capability operation refuses to advertise an incomplete
  runtime, and `scripts/check_p2_modality_architecture.py` prevents an exemption or
  no-op implementation from entering the main artifact.
- **Distributed planes (Phase 2, `raft`/`cluster` unless noted).** `PlacementCatalog`
  (`src/raft/placement.rs`) is the one epoch'd placement authority for online split/merge/
  move; `EPISTEMIC_GRAPH_RAFT_GROUPS` stands up real multi-group production clusters with
  cross-shard read fan-out (`src/raft/xread.rs`); **engine-authoritative cluster topology
  discovery** (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1, `server::persistence::node_info_store`)
  replaces the static `GRAPH_RAFT_GROUP_ENDPOINTS` client map — each node self-reports
  `{node_id, raft_addr, advertised_client_addr, tls_server_name}` at startup
  (`Method::NodeInfoUpsert`, a `CatalogAssign`-shaped `ClusterAdmin`-domain command, NOT
  graph nodes) and `Method::ClusterMembers` (gated `cluster:topology-read`, answered from
  ANY node) + `PlacementRoute.endpoints` (leader-first) hand it back to a discovering
  Python client (`client.cluster_topology.members()`, `epistemic_graph/pool.py`'s
  `resolve_cluster_endpoints`); **the fleet server registry** (CONCEPT:EG-KG.sharding.server-registry,
  W2.5, `src/server/registry_reaper.rs`) is the opposite shape — unlike node-info,
  a fleet MCP/agent server's registration IS a real, queryable `:Server` graph node
  in `__commons__` (`MATCH (s:Server)-[:PROVIDES]->(r:CallableResource)`, the same
  shape au's config-sync ingestion writes). `Method::RegisterServer` (idempotent
  push-registration + lease-TTL heartbeat — a repeat call renews the lease) self-
  translates into a plain `Method::AddNode`, so durability/audit/CDC are AddNode's
  own; a periodic stale-lease reaper (the engine's existing interval-task cadence,
  `EPISTEMIC_GRAPH_SERVER_REGISTRY_REAP_SECS`, default 15s) durably removes any
  `:Server` node whose lease has lapsed and emits the resulting `RemoveNode` CDC
  event. `mcp/server_factory.py` (au) wires every fleet server's `create_mcp_server`
  to self-register + heartbeat via its FastMCP `lifespan` for free; au's config-file
  sync is now a reconciler-of-record over the SAME RPC rather than the sole writer.
  Python-client-bound via `client.server_registry.register(name, url, resources=,
  ttl_secs=)`. lazy graph lifecycle is mandatory and
  bounded in served mode; the durable analytics-job plane (`eg-jobs`, feature `jobs`, in
  `full`) gives
  `Method::AnalyticsJob` async submit/status/cancel/resume over an immutable
  input-snapshot handle, Python-client-bound via `client.jobs.{submit,status,cancel,resume}`
  (+ the general `client.cancel_request` for `Method::CancelRequest`); lake materialization
  (feature `lake`, in `full` as of W4.8) now emits real OpenLineage `RunEvent`s, optional
  push via `EPISTEMIC_GRAPH_OPENLINEAGE_URL`, and the Iceberg-REST catalog surface
  (feature `lake-rest`, also in `full`) serves a standard Iceberg REST client
  (PyIceberg/Spark/Trino) `config`/`namespaces`/`tables`/`load-table` when
  `--iceberg-addr`/`EPISTEMIC_GRAPH_ICEBERG_ADDR` is configured.
- **Persistent index pushdown (EG-P1-4).** Text, vector, spatial, temporal, and semantic
  indexes publish source-snapshot/build/completeness manifests. A recovered or lazily
  opened graph cannot advertise an index until its completeness watermark covers the
  query snapshot; bounded fallback remains authoritative while rebuilding.
- **KnowledgeBatch and native program optimization.** `knowledge-batch` is the sole
  bounded Arrow result currency across graph, SQL, RDF, vector, time-series, jobs, and
  cross-modal queries. `program-optimization` supplies the graph-native typed LM-program
  contract and governed optimizer families without a Python DSPy/LiteLLM runtime.

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
`.tenants`, `.consensus`, `.query` (UQL/SQL/epistemic explain surfaces), `.jobs`
(the durable analytics-job plane, feature `jobs`), `.rbac`, `.admin`, `.broker`.
**Connection pooling + shard routing** live in
`epistemic_graph/pool.py` (`ConnectionPool`, `ShardRouter` accepting engine-authoritative
placement routes over `GRAPH_SERVICE_ENDPOINTS`). `epistemic_graph/quant.py` provides
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
  **market-making / HFT** (`CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest`): `avellaneda_stoikov`,
  `glt_quotes`, `logit_quotes` (bounded prediction-market prices + boundary
  inventory cap), `glosten_milgrom_spread`, `expected_pnl_rate`,
  `breakeven_alpha`, `ofi_series`, `microprice_series`, `vpin_pm`, `hawkes_mle`
  (+ `hardiman_bouchaud`); **Kyle insider/stealth surveillance**
  (`CONCEPT:EG-KG.domains.concept-2`, distils arXiv:2605.27684): `kyle_lambda` (empirical price
  impact) + `surveillance_risk` (informed-flow share / detection hazard /
  cumulative suspicion / stealth ratio / legal-risk score — defensive
  adverse-selection protection); **sizing** `kelly_fraction`, `bayesian_kelly`,
  `posterior_credible_interval`; **backtest validation** `purged_cpcv`,
  `deflated_sharpe`, `probability_backtest_overfit`, `diebold_mariano`;
  **forensic accounting** (`CONCEPT:EG-KG.domains.forensic-accounting-kernels`) `forensic_report` — Beneish M /
  Altman Z / Piotroski F / Sloan accruals over two fiscal years;
  **state-space / stat-arb** (`CONCEPT:EG-KG.domains.state-space-statistical-arbitrage`): `kalman_filter_1d`,
  `kalman_beta` (dynamic time-varying beta), `kalman_volatility` (log-variance
  state), `adf_test` (cointegration; finite-sample-interpolated MacKinnon 1/5/10% criticals
  + approximate p-value), `ou_calibrate` + `ou_optimal_thresholds`
  (Ornstein-Uhlenbeck mean reversion with numerical-MFPT-optimal entry/exit),
  `markov_transition_matrix`; **signal combination / sizing / calibration**
  (`CONCEPT:EG-KG.domains.quant-finance`): `order_book_imbalance`, `information_ratio` (IC·√N),
  `effective_independent_n` (eigenvalue participation ratio), `alpha_combination_engine`,
  `brier_score`, `convergence_gate`, `empirical_kelly` (uncertainty-adjusted);
  **derivatives** (`CONCEPT:AU-KG.domains.derivatives`): `sabr_implied_vol`, `sabr_smile`, `sabr_calibrate`
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
# Native eg-ann IVF-PQ+OPQ+SQ8-refine vector index (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed) as the
# SemanticStore backend, replacing rebuild-on-load HNSW. Pure-Rust CPU serving —
# NO GPU/faiss/native-ML — so it is in the main build. A persisted index reopens
# WITHOUT rebuilding from raw vectors. `ann-redb` also stores the codes in the redb
# durable tier (also in the main build).
ann         = ["eg-core/ann"]
ann-redb    = ["ann", "redb", "eg-core/ann-redb"]
compute     = ["finance", "ast", "datascience", "reasoning"]
# Tokio service (UDS/TCP). tokio is pinned to the minimal feature set actually
# used (rt-multi-thread, net, io-util, sync, time) — NOT "full".
server      = ["dep:tokio", "dep:clap", "dep:tracing-subscriber"]
# Durable redb store (CONCEPT:EG-KG.storage.kg-kg). In the main build so the standard build is
# redb-AUTHORITATIVE by default (CONCEPT:AU-KG.backend.backend-modes — THE FLIP).
redb        = ["server", "dep:redb"]
# Engine-level security (CONCEPT:EG-KG.sharding.row-level-security): per-agent Row-Level Security (the
# read/plan-path GraphView filter), encryption-at-rest for the redb value blobs, and a
# hash-chained tamper-evident audit log over the ledger. PURE-RUST: RLS + audit chain
# link only sha2/hmac (RustCrypto, already deps); encryption pulls chacha20poly1305
# (RustCrypto AEAD — NO ring/openssl/C). Implies `redb`. In the main build.
security    = ["redb", "dep:chacha20poly1305", "eg-types/security", "eg-core/security"]
# ONE MAIN BUILD (CONCEPT:EG-KG.sharding.deployment-tiers): `default == full`. `full` pulls every MAIN feature
# that compiles without an external GPU/robotics toolchain — compute, server, SQL
# (query/DataFusion), cypher, redb, ann, security, the whole wire family, obs, … — so
# `cargo build` IS the full-featured, durable, RLS/audit/encryption-capable source of
# truth. It stays SINGLE-NODE (no raft; that is the opt-in `cluster` layer). The list
# below is illustrative — see Cargo.toml for the exhaustive set.
default     = ["graph", "algorithms", "metrics", "full"]
full        = ["compute", "server", "query", "cypher", "redb", "ann", "security", "pgwire", "mysql-wire", "…"]
```

`eg-compute` is a non-optional dep (its `algorithms` is used by the always-on
graph-op handlers); only its heavy domains + their deps are feature-gated.
The facade declares `crate-type = ["rlib"]` (no `cdylib`/pyo3; maturin
`bindings = "bin"`).

**Opt-in build layer — `full-extras` (main build + GPU/robotics).** An umbrella
(`full-extras = ["full", "gpu-cuda", "ros2-bridge", "ros2-dds", "ros2-rmw"]`) for heavy
legs that need an external toolchain/GPU/robotics stack to actually *run* but still build
clean everywhere: `gpu-cuda` (real CUDA via `dynamic-loading` cudarc, EG-KG.compute.gpu-distance-seam/327),
`ros2-bridge` (rosbridge-WebSocket ROS2 leg, EG-KG.domains.robotics-gpu-distribution — pure-Rust `tokio-tungstenite`),
`ros2-dds` (**native DDS/RTPS ROS2 leg, EG-KG.ingest.dds-transport** — pure-Rust `rustdds`, NO
CycloneDDS/rmw/C toolchain, so it CI-builds), and `ros2-rmw` (**CycloneDDS-C-backed `rmw`
ROS2 leg, S5 / EG-KG.ingest.rmw-cyclonedds-leg** — the safe `cyclonedds` Rust crate over
vendored, cmake-built CycloneDDS C sources: `cyclonedds-src` ships the C sources IN the
crate tarball, `cyclonedds-rust-sys`'s build.rs builds them with `cmake` + ships prebuilt
bindgen output, so it needs a C toolchain (`cc`/`cmake`) but NOT libclang/network at build
time; this is genuine zero-config live-`ros2` interop, a real `ros2` node discovers/pubs/
subs with no bridge). The `DdsTransport` trait in `src/server/dds.rs` unifies the WS +
BOTH native DDS legs behind one interface, sharing the SAME `mangle_topic_name`/
`mangle_type_name` rmw mangling. NOT in the main build — a `default`/`full` build links
no cudarc/rustdds/cyclonedds (asserted by `cargo tree`). Robotics config: `ros2-dds`/
`ros2-rmw` both read the DDS domain from `EPISTEMIC_GRAPH_ROS_DDS_DOMAIN` (default `0`).

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
│   │                #     (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed). Leaf crate (serde/memmap2/rayon/rand; redb opt).
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
├── lib.rs           #   re-exports eg-{types,core,compute} through the current crate:: paths,
│                    #     then declares the server-side modules below (server feature)
├── main.rs          #   epistemic-graph-server entrypoint (the maturin bindings="bin" wheel target)
├── server/          #   Tokio UDS/TCP server, DECOMPOSED (see dispatch conventions below):
│                    #     dispatch.rs (thin routing table) + handlers/{graph_ops,finance,datascience}.rs
│                    #     + state/auth/access/compute/transport.rs
├── metrics.rs       #   Prometheus metrics + /metrics listener (feature `metrics`)
├── channels.rs      #   Agent communication channels
├── redb_store.rs · persist_lock.rs · server/persistence/redb_backend.rs
│                    # authoritative redb persistence + single-writer lock

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
   write side-effects (in-flight gauge, authoritative redb commit, CDC emit) stay
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
   (with a typed feature-required error if gated); (5) add the `epistemic_graph/client.py`
   method + a `tests/` round-trip and a co-located `#[cfg(test)]` dispatch test.

7. **The protocol enum stays flat + section-commented.** The current client and server
   share that exact contract. Any structural change is an atomic ecosystem-wide
   replacement: update every generated/client consumer and delete the superseded
   shape in the same change.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GRAPH_SERVICE_AUTH_SECRET` | Non-empty HMAC-SHA256 secret for the sole `eg2.` request envelope. **Required**; source it from a runtime secret provider. |
| `EPISTEMIC_GRAPH_AUDIENCE` | Expected non-empty request audience. **Required** and matched exactly before dispatch. |
| `EPISTEMIC_GRAPH_TENANT` | Expected non-empty tenant. **Required** and matched exactly before dispatch. |
| `EPISTEMIC_GRAPH_POLICY_VERSION` | Expected non-empty authorization-policy revision. **Required** and matched exactly before dispatch. |
| `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` | Runtime secret map of trusted operation signer ids to non-empty HMAC keys. **Required**; never commit the values. |
| `EPISTEMIC_GRAPH_ENVELOPE_SKEW_SECS` (EG-P0-5) | Clock-skew window in seconds for `eg2.` timestamps and durable replay retention. Default `300`. |
| `EPISTEMIC_GRAPH_NODE_ID` (ADR-3 / W1.9, `server::auth::node_identity`) | This node's stable identity for `eg2.` envelope node-binding, used when the `raft` feature/`EPISTEMIC_GRAPH_RAFT_NODE_ID` are not configured (single-node deployments, or a build without `raft`). **Default `single`** when unset — so a single-node deployment's node claim, if a client ever mints one, always matches with zero operator configuration. Clustered deployments use `EPISTEMIC_GRAPH_RAFT_NODE_ID` instead (the same integer identifying this node in `EPISTEMIC_GRAPH_RAFT_PEERS`); this var is ignored when that one is set. |
| `EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING` (ADR-3 / W1.9, `server::auth::validate_context_claims`) | Tri-state rollout posture for the `eg2.` envelope's optional `node` claim (replay protection under replication — a captured envelope replayed against a DIFFERENT cluster node is rejected `NODE_MISMATCH`, checked before the nonce/replay ledger; same-node replay is still caught by the existing ledger). `off` = an absent claim is accepted silently. `warn` = an absent claim is accepted but logged once per principal (find not-yet-migrated clients before enforcing). `on` = an absent claim is rejected (fail closed) — the W5.2 cluster-cutover posture. **Unset or unrecognized ⇒ `warn`** (the shipped default). A PRESENT claim is exact-matched against this node's identity in EVERY posture — only an absent claim's handling varies. |
| `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` (feature `oidc`) | Primary-protocol OIDC identity binding (`src/server/oidc.rs`, `server::auth::bind_verified_identity`): when set, every `eg2.` envelope must additionally carry an `oidc_token` that independently RSA/JWKS-verifies against this issuer, and the envelope's principal/tenant/roles/scopes must match the verified token's claims (reject on mismatch) — extends the same RSA-JWKS verifier the KV-cache HTTP surface already used. Falls back to the shared `OIDC_ISSUER` when unset. **Absent ⇒ today's HMAC-only `eg2.` behavior is unchanged** (unauthenticated local/dev deployments still work); once set, identity is enforced. Independent of `EPISTEMIC_GRAPH_KVCACHE_JWT_ISSUER` — the two surfaces may point at different realms/audiences. |
| `EPISTEMIC_GRAPH_OIDC_JWT_AUDIENCE` (feature `oidc`) | The OIDC client audience `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER`'s tokens must carry. Falls back to the shared `OIDC_AUDIENCE`. Mandatory once an issuer is configured. |
| `EPISTEMIC_GRAPH_OIDC_JWKS_URL` (feature `oidc`) | JWKS endpoint the primary protocol fetches signing keys from. No generic fallback (discovery/vendor URL construction belongs at the deployment boundary). Mandatory once an issuer is configured. |
| `EPISTEMIC_GRAPH_RAFT_GROUPS` (DIST-P2-2 / ADR-2 W1.2, `raft`/`cluster` feature) | Number of Raft groups this node stands up at boot **and** the durable shard count K (ADR-2: K == N, raft group *g* owns redb shard *g*). Default = the cores-derived auto-size `clamp(cpu/2, 1, MAX_SHARD_COUNT=64)` (the same write-sharding the non-raft path uses — turning on raft no longer collapses to one writer); set explicitly to size the pool, clamped `1..=64`. Un-pinned graphs spread across the `0..N` tenant-range ring (`FNV-1a(sanitize(name)) % N`) while `PlacementCatalog` remains authoritative for explicit placements. **Per-shard cost:** each group opens one redb file descriptor + one group-commit writer thread, so a high N trades RAM/FDs for N-way parallel durable writes. An existing K=1 raft store keeps K=1 (all groups on shard 0) until `migrate-shards` rewrites its layout. |
| `EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR` (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1, `raft::config`) | This node's client-reachable address, self-reported into the durable cluster-topology store (`Method::NodeInfoUpsert`) and handed back by `Method::ClusterMembers`/`PlacementRoute.endpoints` — the engine-authoritative discovery that replaces the static hand-maintained `GRAPH_RAFT_GROUP_ENDPOINTS` client map. **Required whenever Raft peers are configured** (`EPISTEMIC_GRAPH_RAFT_NODE_ID`/`_PEERS` set) — config-contract style, like the transport secret: a clustered node refuses to start without it, since a discovering client would otherwise have no address to learn for this node beyond its own seed contact. Not read at all when Raft is not configured (single-node). |
| `EPISTEMIC_GRAPH_ADVERTISED_TLS_SERVER_NAME` (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1, `raft::config`) | Optional TLS server name (SNI / certificate hostname) a client should verify when connecting to `EPISTEMIC_GRAPH_ADVERTISED_CLIENT_ADDR` over `tls://`, self-reported alongside it into the cluster-topology store. **Unset ⇒ `None`** — the client verifies against the address's own host (the TLS default); zero friction for a deployment that doesn't need SNI override. |
| `EPISTEMIC_GRAPH_SERVER_REGISTRY_REAP_SECS` (CONCEPT:EG-KG.sharding.server-registry, W2.5, `server::registry_reaper`) | Positive interval, seconds, for the fleet server-registry stale-lease reaper: how often `__commons__` is swept for `:Server` nodes (written by `Method::RegisterServer`) whose `lease_expires_at_ms` has lapsed. A lapsed row is durably removed and a `RemoveNode` CDC event is emitted (feeds the incident brain). **Default `15`** (unset or non-positive) — short enough that even the minimum allowed `RegisterServer.ttl_secs` lease (1s) is reaped promptly; always armed (unlike cold-offload's opt-in memory policy, an unreaped dead registration is a staleness/correctness concern, not a resource-usage opt-in). |
| `EPISTEMIC_GRAPH_LAZY_STARTUP` (DIST-P2-3) | Catalog-first recovery is mandatory for served mode: a graph hydrates on first access behind its incarnation/version fence. |
| `EPISTEMIC_GRAPH_MAX_RESIDENT_GRAPHS` (DIST-P2-3) | Positive cap on simultaneously resident `GraphCore`s; default `1024`. The coldest eligible graph is durability-gated and evicted before admission; `__commons__` is never evicted. |
| `EPISTEMIC_GRAPH_LAZY_OPEN_PAGE_SIZE` (CONCEPT:EG-KG.sharding.paged-lazy-open, `redb` feature) | Positive bound for each source-side lazy-open scan; default `4096`. Every page is fenced by graph incarnation and durable source version under a per-graph lifecycle lock. Graph operations return typed `PARTIAL_MATERIALIZATION` until the final page rebuilds and publishes every maintained index. |
| `EPISTEMIC_GRAPH_OPENLINEAGE_URL` (INT-P2-3, feature `lake`, in `full`) | When set, every `LakeManager` materialize/compact/delete run pushes its OpenLineage `RunEvent` (job/run/input-dataset/output-dataset facets) to `<url>/api/v1/lineage` over HTTP. **Unset ⇒ silent no-op** — lineage export is best-effort and never blocks or fails a materialization run |
| `EPISTEMIC_GRAPH_LAKE_MATERIALIZE_INTERVAL_SECS` (INT-P2-3, feature `lake`, in `full`) | Positive interval, in seconds, for the periodic WAL/series→lakehouse materialization sweep: each tick lists every tsdb series and incrementally drains any new points into its Iceberg/Delta lake table. **`0`/unset ⇒ disabled** (the sweep never runs; a caller can still drive `LakeManager` directly, e.g. `drain_series`). NOTE: the sweep currently also requires an authenticated carrier (`server::unauthenticated_carrier_denied`); see `EPISTEMIC_GRAPH_ICEBERG_ADDR` below and `reports/issue-register.md` (W4.8) — today it always skips the tick and logs a warning, a pre-existing cross-cutting gap shared by every `serve_with_security`-wired auxiliary HTTP surface, not specific to lake. |
| `EPISTEMIC_GRAPH_ICEBERG_ADDR` (INT-P2-3, feature `lake-rest`, in `full`, alias `--iceberg-addr`) | When set, a hand-rolled HTTP/1.1 listener (the SAME dependency-free idiom as `--metrics-addr`/`--obs-addr` — NO axum/hyper) binds this address (documented loopback `127.0.0.1:8181`) and serves the `iceberg.apache.org/rest-catalog-spec` surface — `GET /v1/config`, `GET /v1/namespaces[/…]`, `GET /v1/namespaces/{ns}/tables[/…]` (ListTables/LoadTable/HEAD-exists), `POST /v1/namespaces/{ns}/tables/{table}` (CommitTable, bridged to the engine's own compaction pass) — over the tables the `lake` materialization tier writes, so a standard Iceberg REST client (PyIceberg/Spark/Trino) can list + load them. **Unset, or a build without `lake-rest`, ⇒ no listener.** NOTE: every request currently fails closed via `server::unauthenticated_carrier_denied` (a pre-existing, cross-cutting gap shared by `obs`/`s3-api`/`sparql-http`/`federation-search`/`kvcache-server`, not specific to lake — see `reports/issue-register.md`, W4.8); the endpoint set and response shapes are exercised through the feature's own `serve()` (non-security) test suite in `src/server/lake/rest.rs`. |
| `GRAPH_SERVICE_SOCKET` | Path to the UDS socket |
| `GRAPH_SERVICE_PERSIST_DIR` | Mandatory served-mode redb directory (alias `--persist-dir`) containing authoritative graph state and the durable request replay ledger. |
| `EPISTEMIC_GRAPH_REDB_COMMIT_POLICY` | Authoritative redb commit policy: `each`, `interval`, or a positive millisecond value. Invalid values and zero fail startup; there is no durability-off mode. |
| `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` | Positive adaptive group-commit micro-linger in microseconds (default `1000`). A shallow barrier batch waits once for concurrent writers to join the same fsync; durability remains commit-before-ack. |
| `EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW` | Shallow-batch op threshold for the EG-024 micro-linger (default `32`, clamped 1..4096). The writer lingers only while `pending.ops.len()` is below this; a deeper batch already coalesces, so it commits immediately (adaptive — no added latency on a deep queue) |
| `EPISTEMIC_GRAPH_REDB_SHARDS` | **Sharded K-way durable writer (CONCEPT:EG-KG.backend.sharded-k-way-durable).** Number of independent `graph-<n>.redb` files/writer threads, overriding auto-size `clamp(cpu/2, 1, 8)` (clamped 1..64). Each graph routes to a fixed shard by `FNV-1a(sanitized_name) % K`; `K=1` uses canonical `graph-0.redb`. The value is fixed per durable store. **Ignored under an active Raft node** — there K == N and follows `EPISTEMIC_GRAPH_RAFT_GROUPS` instead (ADR-2 / W1.2, group *g* owns shard *g*), logged as a warning if set. Normal startup rejects the retired unindexed `graph.redb`; convert it offline with `migrate-shards --shards 1`. |
| `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` | Per-shard auto-sized early-flush threshold. The writer flushes a `Pending` batch once it reaches the threshold, bounding RAM before the channel saturates. Default is half the durable-writer queue depth, clamped `256..16384`; overrides are clamped `64..1_048_576`. |
| `GRAPH_SERVICE_ENDPOINTS` | Comma-separated shard endpoints for the Python `ShardRouter` |
| `EPISTEMIC_GRAPH_PGWIRE_ADDR` | When set (build `--features pgwire`), the pg-wire listener binds this address (documented loopback `127.0.0.1:5433`). Unset ⇒ no listener. A connecting driver/ORM introspects a SYNTHETIC read-only catalog (CONCEPT:EG-KG.query.datafusion: DataFusion `information_schema` + a supplemented `pg_catalog` `pg_namespace`/`pg_class`/`pg_attribute`/`pg_type` + `version()`/`current_schema()`/`current_database()`) then runs SQL over `nodes`/`edges` |
| `EPISTEMIC_GRAPH_PGWIRE_GRAPH` | Default graph a fresh pg-wire connection runs against when the libpq `database` param is unset. Defaults to `__commons__` |
| `EPISTEMIC_GRAPH_PGWIRE_AUTH` | pg-wire auth mode (CONCEPT:EG-KG.query.concept-13): only `scram` is accepted. A non-empty `GRAPH_SERVICE_AUTH_SECRET` is mandatory; missing key material and every other mode fail startup. SCRAM maps the pg `user` → an engine `agent_id`; the password is `hex(HMAC-SHA256(secret, "pgwire:"+user))`. Only after proof succeeds does the loopback adapter bind a server-owned tenant+actor `CarrierAuthority`; user-table catalogs resolve to opaque owner files below `GRAPH_SERVICE_PERSIST_DIR/sql-catalog/`. |
| `EPISTEMIC_GRAPH_MYSQL_ADDR` | When set (build `--features mysql-wire`), the hand-rolled MySQL/MariaDB wire listener (CONCEPT:EG-KG.query.kg-2) binds this address (documented loopback `127.0.0.1:3306`). Unset ⇒ no listener. A MySQL driver/ORM/`mysql` CLI connects (Handshake v10 + `mysql_native_password`) and runs SQL over `nodes`/`edges` via the SAME EG-KG.compute.subsystems-reference `WireSession` execute→classify→exec core as pgwire. Prepared-statement binary protocol is unsupported |
| `EPISTEMIC_GRAPH_MYSQL_GRAPH` | Default graph a fresh MySQL-wire connection runs against when the connect `database` (schema) is unset. Defaults to `__commons__` |
| `EPISTEMIC_GRAPH_MYSQL_AUTH` | MySQL-wire auth mode (CONCEPT:EG-KG.query.kg-2 / EG-KG.query.concept-13): only `native` (`mysql_native_password`) is accepted. A non-empty `GRAPH_SERVICE_AUTH_SECRET` is mandatory; missing key material and every other mode fail startup. NATIVE maps the MySQL `user` → an engine `agent_id`; the password is `hex(HMAC-SHA256(secret, "mysql:"+user))`. Only a successful proof binds the server-owned tenant+actor `CarrierAuthority` used for graph ACL and owner-scoped SQL catalog access. |
| `EPISTEMIC_GRAPH_BOLT_ADDR` | Loopback-only Bolt v4.4 listener. HELLO/LOGON must use scheme `epistemic` with a fresh hex-MessagePack signed `Health` request as credentials. The current `eg2.` verifier binds graph, tenant, audience, policy, actor, and scopes into the session; unsigned database switching, actor-only credentials, and auth-mode downgrade do not exist. Writes use the staged authoritative MutationBatch gateway; explicit rollback discards detached state. |
| `EPISTEMIC_GRAPH_MSSQL_ADDR` | When set (build `--features mssql-wire`), the MSSQL **TDS** wire listener (CONCEPT:EG-KG.query.hand-rolled-tds-server) binds authenticated loopback only (`127.0.0.1:1433`). A decoded `SQLBatch` runs through the shared `WireSession`; TDS encryption is answered `ENCRYPT_NOT_SUP`, so remote access requires a TLS/mTLS identity-binding gateway into loopback. RPC/prepared statements and MARS are unsupported |
| `EPISTEMIC_GRAPH_MSSQL_GRAPH` | Default graph when LOGIN7 `Database` is unset (`__commons__`). A non-empty `GRAPH_SERVICE_AUTH_SECRET` is mandatory. LOGIN7 password is `hex(HMAC-SHA256(secret, "mssql:"+user))`; only a verified login binds the server-owned tenant+actor `CarrierAuthority` used for graph ACL and owner-scoped SQL catalog access. Missing key material fails startup. |
| `EPISTEMIC_GRAPH_AMQP_ADDR` | Authenticated loopback-only AMQP 0.9.1 listener. SASL PLAIN password is `hex(HMAC-SHA256(secret, "amqp:"+principal))`; the verified principal becomes a secret-keyed pseudonymous actor reference before broker dispatch. Missing `GRAPH_SERVICE_AUTH_SECRET` fails startup |
| `EPISTEMIC_GRAPH_MQTT_ADDR` | Authenticated loopback-only MQTT 3.1.1/5.0 listener. CONNECT username and password are mandatory; password is `hex(HMAC-SHA256(secret, "mqtt:"+principal))`. The verified username becomes a secret-keyed pseudonymous actor reference before broker dispatch |
| `EPISTEMIC_GRAPH_STOMP_ADDR` | Authenticated loopback-only STOMP 1.2 listener. CONNECT `login` and `passcode` are mandatory; passcode is `hex(HMAC-SHA256(secret, "stomp:"+principal))`. The verified login becomes a secret-keyed pseudonymous actor reference before broker dispatch |
| `EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE` (W4.10/M6, `server::cep`, feature `broker`) | **CEP → broker push bridge.** When set to a non-empty exchange name, every match a live CEP standing query (`CepSubscribe`) detects is ALSO published — topic-routed, routing key = the subscription id — onto that exchange in `__commons__`'s broker, so an already-connected AMQP/MQTT/STOMP consumer (the three wire adapters' own poll-driven push pumps, `EPISTEMIC_GRAPH_AMQP_ADDR`/`_MQTT_ADDR`/`_STOMP_ADDR` above) is pushed the match with no further client action, and any RPC client can equally `BrokerConsume` it. Exists alongside `CepPoll` (long-poll), never replacing it. **Default unset ⇒ disabled** — CEP delivery stays `CepPoll`-only, byte-for-byte the pre-existing behavior; the bridge is reachable only from `CepSubscribe`'s registration path, never from the CDC write-path hook (`CdcHub::emit`), so the per-write cost is zero whether or not this is armed. |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` | Server backpressure cap (default 1024); excess → `BUSY` |
| `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES` | Positive overload bound for `GetNodes` (default `50_000`). Larger graphs return typed `RESULT_TOO_LARGE`; callers must use bounded label queries or pagination. The guard cannot be disabled. |
| `EPISTEMIC_GRAPH_MAX_RESPONSE_EDGES` | Positive overload bound for `GetEdges` (default `50_000`) — the edge-count sibling of `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`. Larger graphs return typed `RESULT_TOO_LARGE`; callers must use `GetEdgesPage` (keyset-paginated, `(source, target, ordinal)` cursor) instead. The guard cannot be disabled. |
| `EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS` | Reference-counted idle shutdown (CONCEPT:EG-KG.backend.tiny-shared, alias `--idle-shutdown-secs`). `N>0` ⇒ the engine self-terminates cleanly after N seconds with ZERO active connections — the shared tiny-daemon mode agent-utilities' EngineResolver autostarts. **Absent or `0` (default) ⇒ NEVER self-terminate on idle: long-living/persistent, runs forever like a normal server.** SIGTERM/SIGINT drains cleanly in BOTH modes; commit-before-ack means no shutdown checkpoint is required. |
| `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS` | Cold-tenant idle offload sweep (CONCEPT:EG-KG.backend.r6-feature, R6, feature `redb`). `N>0` ⇒ a periodic task (every N seconds, reusing the engine's existing interval-task cadence — no new daemon) hibernates every graph idle longer than N seconds (access recency is `touch`ed on the dispatch read/write path), bounding RAM across many tenants. Durability-gated + read-through-safe (EG-KG.storage.read-through-seam-exercised) so an offloaded graph is never lost, only evicted; `__commons__` is never offloaded. **Absent or `0` (default) ⇒ disabled** (no proactive idle offload; the EG-KG.compute.lane-v budget enforcer still runs) |
| `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS` | Provenance-anchoring sweep (CONCEPT:EG-KG.sharding.row-level-security, feature `security`). `N>0` ⇒ a periodic task (every N seconds, reusing the engine's existing interval-task cadence — no new daemon) Merkle-hashes each resident graph's `:ToolCall`/`:RunTrace` provenance-node window and, only when the root changed since that graph's last anchor, appends a `PROVENANCE_ANCHOR|...` entry to the SAME hash-chained audit log the `security` feature already maintains (`src/audit.rs`). `Method::AuditProveInclusion` then proves/verifies one node's inclusion against an anchor; a node whose durable content changed after anchoring fails verification. The window-size-dependent read/hash work runs off the writer thread and an unchanged window commits nothing, so overhead is bounded independent of `N` or window size (measured in `benches/provenance_anchor_bench.rs`). **Absent or `0` (default) ⇒ disabled** (no anchoring; `AuditVerify`'s ordinary chain-sequence tamper-evidence is unaffected). |
| `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_MAX_NODES` | Per-tick cap on `:ToolCall`/`:RunTrace` candidate ids considered per label per graph by the provenance-anchoring sweep above (default `50_000`). Bounds both the off-writer-thread hashing pass and the `PROVENANCE_ANCHOR_MEMBERS` row. A graph whose provenance-node count exceeds the cap anchors only its lexicographically-smallest `N` ids every tick (deterministic, but NOT a rotating/incremental window) — raise the cap for a graph with a larger provenance corpus than the default. |
| `REASON_ON_WRITE` (W3.6/E16, feature `owl`) | **Reasoning auto-cascade opt-in** — comma-separated exact graph names. For each listed graph, a committed axiom/edge write DEBOUNCES a re-materialization of that graph's OWL/RL closure: the existing `eg_rdf::owl::Reasoner` incremental delta re-seed (`add_axioms`, CONCEPT:EG-KG.ontology.incremental-materialization) re-runs over only the NEW TBox triples since the last refresh, on a periodic sweep task (`server::reasoning_cascade`), never inline on the write path. A burst of writes to the same graph inside the debounce window coalesces into ONE refresh. **Absent or empty (default) ⇒ disabled** — NEVER default-on (closure materialization is real, ontology-size-dependent CPU cost): no cascade is installed on the CDC hub and no background task is spawned; a non-listed graph's write pays exactly one hashset-miss lookup, zero lock/allocation. |
| `EPISTEMIC_GRAPH_REASON_ON_WRITE_DEBOUNCE_MS` (W3.6/E16, feature `owl`) | Debounce window (milliseconds) for `REASON_ON_WRITE`'s closure refresh — how long a graph must go quiet after its last write before the cascade re-materializes it. **Absent, `0`, or invalid ⇒ default `500`ms.** Only read when `REASON_ON_WRITE` is non-empty. |
| `EPISTEMIC_GRAPH_TENANT_CATALOG` | Tenant-catalog auto-attach gate (CONCEPT:EG-KG.sharding.r5-feature, R5, feature `redb`). `1`/`true`/`yes`/`on` ⇒ `RedbBackend::open` attaches the durable `catalog.redb` tenant→shard override map to the live routing seam (also auto-attached when a populated `catalog.redb` already exists). **Default OFF** ⇒ pure EG-KG.backend.sharded-k-way-durable FNV-1a routing. An attached-but-empty catalog routes identically, so enabling it is a no-op until the M3 admin RPC (CONCEPT:EG-KG.backend.m3-admin-dispatch) assigns a placement / runs an online reshard |
| `GRAPH_SERVICE_METRICS_ADDR` | Prometheus `/metrics` HTTP listener address (alias of `--metrics-addr`, e.g. `127.0.0.1:9101`). Disabled when unset; requires the `metrics` cargo feature (on by default) |
| `EPISTEMIC_GRAPH_OBS_ADDR` | **Observability log-ingestion HTTP listener (CONCEPT:AU-KG.ingest.self-ingest/161, feature `obs`, alias `--obs-addr`).** When set (build `--features obs`), a hand-rolled HTTP listener (the SAME dep-free idiom as `--metrics-addr`/`--sparql-addr` — NO axum/hyper) binds this address (documented loopback `127.0.0.1:5080`, O2's log-ingest port). It accepts log records over `POST /v1/logs` (OTLP/HTTP JSON), `POST /_bulk` + `POST /<stream>/_doc` (Elasticsearch-compatible), and `POST /` (JSON-lines), normalizes them to a common record and lands each in an eg-tsdb series (time range + retention) + a per-stream eg-text Tantivy full-text index (schema-on-read), then rolls Parquet columnar segments into the blob CAS (S3 when `blob-s3` is on) with a per-segment manifest (stream, time range, row count, schema) for prune-before-scan. Multi-tenant by **stream**. **Unset, or a build without `obs`, ⇒ no listener** (a warning is logged if the addr is set but the feature is off). Flush window via `EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS` (default 1024 records per segment). Separate from the RPC/SPARQL/metrics transports |
| `EPISTEMIC_GRAPH_OTLP_ENDPOINT` | **OpenTelemetry OTLP span export (CONCEPT:EG-OS.observability.slow-query-descriptor, feature `otel`).** When set, the binary must include `otel` and the endpoint must initialize successfully; invalid configured observability fails startup rather than silently changing trace delivery. |
| `EPISTEMIC_GRAPH_SLOW_QUERY_MS` | **Slow-query log threshold, milliseconds (CONCEPT:EG-OS.observability.slow-query-descriptor).** When set to a positive integer, any query (SQL / Cypher / SPARQL / UnifiedQuery over the RPC path, and pgwire SQL) whose end-to-end execution meets/exceeds this many ms is emitted as a structured `tracing::warn!` (target `epistemic_graph::slow_query`) with the query kind, truncated query text, elapsed ms, and — where available — the plan op count. Also increments the `epistemic_graph_slow_query_total` Prometheus counter (feature `metrics`). **Absent / `0` / invalid ⇒ disabled** (the only hot-path cost is a single cached-threshold compare) |
| `EPISTEMIC_GRAPH_CYPHER_ENGINE` (W1.3 / ADR-cypher-lowering, feature `cypher-plan` — in `full`; a lean `cypher`-only build is legacy-only) | Cypher execution engine selector: `legacy` \| `plan` \| `shadow`. **Unset/unrecognized ⇒ `legacy`** (the shipped default). `plan` = cost-based start/order selection (label-index cardinality; reverses a linear MATCH toward its cheapest end) over the same semantics-exact walk — result-identical by construction. `shadow` = execute BOTH, serve legacy, diff row-sets (order-insensitive without ORDER BY) and `tracing::warn!` each divergence with the query text (target `epistemic_graph::cypher_shadow`) — the zero-divergence soak gate that must hold before the default flips to `plan` in a later release. |
| `EPISTEMIC_GRAPH_ENCRYPTION_KEY` | Encryption-at-rest key material (CONCEPT:EG-KG.sharding.row-level-security, feature `security`). When set, the redb durable **value** blobs (node/edge property + semantic store) are sealed with a pure-Rust ChaCha20-Poly1305 AEAD (RustCrypto — NO ring/openssl) — raw `.redb` bytes hold no plaintext properties. Keys stay plaintext so range scans work. **Default OFF / opt-in** (changes the on-disk format). `ValueCipher::from_env` is the KMS hook seam (swap it for a data-key fetch). A wrong key fails the read (never silent plaintext) |
| `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW` | **SPARQL SERVICE federation allowlist (CONCEPT:EG-KG.query.sparql-service-federation-client, feature `sparql-service`).** Comma-separated set of allowed endpoint hosts / `scheme://host:port` origins the `/sparql` endpoint may delegate a `SERVICE <ep> { … }` clause to. **Default empty/unset ⇒ SERVICE DISABLED (fail-closed)**: no remote client is bound, so a non-SILENT SERVICE errors (a `SERVICE SILENT` yields the empty solution). A host that resolves to a loopback/link-local/RFC-1918 (or unique-local IPv6) address is refused unless the allowlist names that exact IP literal (SSRF guard). The client rides the SAME pure-Rust rustls `ureq` stack `federation` links (no new dep; both are in the main build), with bounded connect/read timeouts + a response-size cap |
| `EPISTEMIC_GRAPH_KVCACHE_BACKEND` (CONCEPT:EG-KG.backend.networked-shared-kv, feature `kvcache-server`) | Selects the KV-cache HTTP surface's backend. `durable` (aliases `shared`/`store`) ⇒ the **networked, mutation-store-backed, FLEET-SHARED** [`SharedKvStoreBackend`] over the engine's live `kv.redb` (`src/server/kvcache_http/shared_store.rs`) — the SAME durable store the `KvGet`/`KvPut` wire methods use, so a prefix block one vLLM/LMCache serving instance PUTs is a cache HIT for every other instance hitting this engine, and the shared cache **survives an engine restart**. Requires the `kv` feature + a persist dir (`GRAPH_SERVICE_PERSIST_DIR`); a clear startup error if either is missing. **Unset / anything else ⇒ the in-process ephemeral, single-process `SharedKvIndex`** (the unchanged default). Data-version invalidation is fleet-wide (the epoch is persisted to `kv.redb`, gated lazily on read); reclaim stale derived-context blocks out of band via the bounded `retire_stale` sweep. |
| `EPISTEMIC_GRAPH_KVCACHE_INSTANCE_ID` (CONCEPT:EG-KG.backend.networked-shared-kv, feature `kvcache-server`) | Labels THIS serving instance in the durable KV-cache backend's metrics/logs (the `instance` field on every hit/miss `tracing` line + the periodic `epistemic_graph::kvcache` hit-rate metric), so cross-instance prefix reuse is attributable (e.g. instance B logging a HIT on a hash instance A stored). **Unset ⇒ `EPISTEMIC_GRAPH_NODE_ID`, else `"kvcache"`.** Only read by the `durable` backend. |
| `EPISTEMIC_GRAPH_ICV_NATIVE_WRITES` (W4.13, feature `shacl`) | Extends the SHACL/ICV write guard (CONCEPT:EG-KG.ontology.rdf-update-guard) from RDF writes to native property-graph writes — Cypher `CREATE`/`SET`/`DELETE`, `CompareAndSetNodeFields`, and any other mutation on the gateway's staged/diffable commit path (`server::mutation::commit_mutation_inner` and its async twin `commit_conditional_mutation_async_inner`, which is what `CypherQuery` actually routes through). A **two-level opt-in**: this process-wide gate (`1`/`true`/`yes`/`on`, trimmed, enable it) AND the target graph's own registered integrity policy (`IcvConfigure`) — a graph that never registered a policy is unaffected either way, even with the gate on. On an opted-in graph, a native write that would introduce a shape violation is rejected with the same witness-bearing `GuardRejection` detail the RDF write path returns; a write with no observable RDF-projected change is a fast no-op (no shapes evaluation). **Default/absent ⇒ disabled** (byte-for-byte today's native-write behavior). Does not cover the mutation gateway's row-local fast path (plain `AddNode`/`RemoveNode`/`RemoveEdge`, a simple `AddEdge` between existing nodes, `ClearGraph`, `AddEmbedding`, `BatchUpdate`) — a deliberate, logged scope boundary (`reports/issue-register.md`, W4.13) |
| `XDG_RUNTIME_DIR` | Directory for UDS socket placement |

---

## ⛔ No Scratch or Temporary Files in Repository

**NEVER** commit temporary scripts (`test_*.py`/`debug_*.py` outside `tests/`),
scratch files, `.log`/`.txt` command dumps, patch leftovers (`*.orig`, `*.rej`),
or tracked binaries. Put scratch work and reports in deployment-configured
directories outside the repository. Keep tests in `tests/` (pytest) /
`#[cfg(test)]` (Rust).


## ⛔ Keep the Repository Root Pristine

The repository root must contain only canonical project files. The only hidden
directories allowed at root are `.git/`, `.github/`, `.specify/` (plus a local,
git-ignored `.venv/`). NEVER write scratch/debug/migration files to the repo —
especially the root: no `fix_*.py`/`migrate_*.py`/`refactor_*.py`/root `test_*.py`,
no `*.db`/`*.log`/scratch `*.txt`/`*.orig`/`*.rej`/`*.bak`, no build artifacts
(`*.tsbuildinfo`), and no AI scratch dirs (`.agent/`, `.agents/`, `.agent_data/`,
`.tmp/`, `.hypothesis/`). Put experiments in a configured external scratch
directory and tests in `tests/`. Run `git status` before finishing and confirm
no stray root files.

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

**The one exception is persisted on-disk state** in the current durable redb
store: a format change may need a **one-time offline data migration**
(read-old → write-new), after which the old-format reader is removed. That is data
migration, not API back-compat — never a permanent dual-format reader.

The served security contract follows this rule literally. `eg2.` is the only
request-envelope format; there is no authentication bypass, profile downgrade,
or permissive RLS switch. The server build must include `security`, routable native
TCP must use TLS, and every auxiliary listener is loopback-only. A fresh empty
durable RBAC store admits exactly one bootstrap action: signer-backed `eg2.`
self-registration in `__commons__` as `System`, with no teams or roles and the
single exact scope `security:bootstrap`. All later operations use normal durable
identity/RBAC policy.

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
edit the repository-manager-owned canonical checkout** — its background sync can
reset the working tree and discard
uncommitted edits. Take your own git worktree on your own branch instead:

```bash
# preferred — repository-manager MCP:
rm_worktree add <repo> <your-branch>      # returns the runtime worktree location

# raw-git fallback:
git -C agent-packages/<repo> checkout main
: "${WORKTREE_ROOT:?set a runtime worktree root}"
git -C agent-packages/<repo> worktree add "$WORKTREE_ROOT/<repo>/<branch>" -b <branch>
```

Work in the worktree and **commit often** (commits survive a working-tree reset).
Each session must use a **distinct branch** — git allows a branch in only one
worktree, which is what keeps concurrent sessions from colliding. Keep
`WORKTREE_ROOT` outside the canonical workspace scan so the sync leaves worktrees
alone.

**Finishing work in a worktree** — run this sequence before calling it done:
1. **Pre-commit green** — `pre-commit run --all-files`; resolve every issue per the
   Quality Bar above (including pre-existing), no `--no-verify`.
2. **Commit** in the worktree.
3. **Merge to main locally** — `rm_worktree merge <repo> <branch> --into main`
   (or `git merge --no-ff`). Push only when the user asks.
4. **Clean up** — remove the worktree and delete the merged branch:
   `rm_worktree remove <repo> <branch> --delete-branch`; `rm_worktree prune` clears
   stale entries. (Raw-git: `git worktree remove <path> && git branch -d <branch>`.)

## Version & lockfile drift edict (keep the version mirrors AND the lock in sync)

The two most common release-breakers in this fleet are **version drift** (the version in
`pyproject.toml`/`.bumpversion.cfg` advancing while `README.md`, `docker/Dockerfile`, and the
module `__version__`s lag) and a **stale `uv.lock`** (shipping known-vulnerable transitive deps).
A version mismatch makes the next `bump-my-version` throw `VersionNotFoundException`; a stale lock
is what Dependabot flags. Rules:

1. **Never hand-edit a version string.** Change the version ONLY via
   `bump-my-version bump {patch|minor|major}` (a.k.a. `bump2version`), which rewrites every file
   registered in `.bumpversion.cfg` in one atomic, tagged commit. If you edited the version in
   `pyproject.toml` by hand, you created drift — revert and use the bumper.
2. **Every version-bearing file must be registered in `.bumpversion.cfg`** — at minimum
   `pyproject.toml` AND `README.md`, plus `docker/Dockerfile` and any module `__version__`. Never
   add a file that embeds the version without a `[bumpversion:file:...]` entry for it.
3. **Re-lock on every dependency change.** After editing `pyproject.toml` deps/extras, run
   `uv lock` and commit `uv.lock` in the SAME change. The `uv-lock` pre-commit hook runs with
   `--locked` and fails on drift — never bypass it. The committed `uv.lock` is the
   Dependabot/security surface.
4. **Patch CVEs with a version floor at the source, then re-lock.** `uv` resolves one version
   graph-wide, so a lower-bound in the extra that pulls a dependency raises it for the whole lock.
