# Concept Registry — epistemic-graph

> **Prefix**: `CONCEPT:EG-*` / `CONCEPT:EPG-*`
> **Bridge**: `CONCEPT:ECO-4.0` (Unified Toolkit Ingestion)

## Project-Specific Concepts

These concepts are actively realized by the compiled Rust/Python Epistemic Graph backend in this repository.

| Concept ID | Name | Description |
|------------|------|-------------|
| `CONCEPT:KG-2.16` | High-Performance Graph Compute Engine | Optimized native-compiled memory model and search traversal (DFS/BFS) for the Knowledge Graph. |
| `CONCEPT:ORCH-1.29` | Compiled Orchestration Kernel | A fast, deterministic core designed to resolve multi-agent dependency loops and order pipeline executions. |
| `CONCEPT:KG-2.17` | Compiled Semantic Reasoner | Ultra-fast native-compiled Datalog OWL forward chaining reasoning engine. |
| `CONCEPT:KG-2.18` | High-Performance Quant Epistemic-Graph Engine | Native-compiled quantitative metrics computation, portfolio optimization, regime detection, and order matching simulation engine (replacing Python `numpy`/`scipy`). |
| `CONCEPT:KG-2.19` | Tokio Service Layer | High-performance Tokio async service exposing RPC endpoints over UDS/TCP for inter-agent communication. |
| `CONCEPT:KG-2.51` | Lock-Free Compute + Engine Observability | Heavy read-only algorithms compute on structural snapshots via the blocking pool (writers never starved by analytics), plus Prometheus metrics (per-op rate/latency, admission, per-graph size, checkpoint, auth/ACL counters) on a `--metrics-addr` HTTP listener. |
| `CONCEPT:EG-010` | Ontology Lexical Classification Gate | Embedding-free aho-corasick match of a query against capability-node names+synonyms (Tool/Skill/MCPServer/…), cached per node-count. The "free" (~µs) tier of chat-turn classification: a turn naming a real fleet capability escalates to the full graph without a vector search. `Method::MatchOntologyTerms` (read-only). |
| `CONCEPT:KG-2.182` | Per-Graph Write Coalescer | Concurrent single-op writes to ONE hot graph (the `__commons__` ingestion firehose) batch onto a lazily-created per-graph writer (`src/write_coalescer.rs`) and apply under ONE `topo.write()` per batch — collapsing N lock acquisitions into ⌈N/batch⌉. Writers are keyed by graph name in a `DashMap` (auto per new graph/connector, no hardcoded list); `dirty`/WAL/gauge side-effects stay in the dispatch shell so durability and checkpoint contracts are unchanged. Default ON, batch auto-sized from cpu count; opt out with `EPISTEMIC_GRAPH_WRITE_COALESCE=0`. See [`write-coalescer.md`](architecture/write-coalescer.md). |
| `CONCEPT:KG-2.180` | Multi-op OCC ACID Transactions | Optimistic, snapshot-isolation, server-staged transactions (`src/server/txn.rs` + `handlers/txn.rs`). `BeginTxn` returns a server-issued `txn_id`; `Txn{AddNode,RemoveNode,AddEdge,RemoveEdge,Cas}` STAGE durable mutations into a server-held write-set (nothing touches the graph or persistence until commit). `Commit` takes `topo.write()` ONCE — the serialization point — validates the OCC read-set (per-`GraphCore` `AtomicU64` version + per-node fingerprints), applies the staged write-set atomically through ONE `GraphTxn`, bumps the version, and records each staged method through the configured `PersistenceBackend`; it returns `Bool(false)` on conflict (a true rollback — nothing applied). `Rollback` discards the staged state. A long-open txn never holds `topo.write()`; an idle-TTL sweep auto-rolls-back abandoned txns, and per-graph/per-agent open-txn caps bound memory. Staged ops bypass the write coalescer (no deadlock). Single-op CAS auto-commit is unchanged. |
| `CONCEPT:KG-2.179` | Dep-free Cypher query surface | Read-only `MATCH (a:Label)-[:REL]->(b:Label2) WHERE a.prop = 'x' RETURN a, b LIMIT k` over ONE graph, behind the facade `cypher` feature. A hand-written recursive-descent parser (`crates/eg-query/src/cypher/`) compiles the subset to the engine's OWN primitives — label predicates → the eg-core label index, fixed-shape multi-hops → a synthesized pattern `GraphView` fed to `vf2_match_views`, variable-length paths `*m..n` → petgraph BFS — with NO DataFusion, so it ships in the lean Pi tier (`pi` feature). Routed through `handlers/query.rs` (`Method::CypherQuery`), returning the same `QueryResult` carrier as `Sql`. |
| `CONCEPT:KG-2.183` | Transaction isolation levels | `BeginTxn` carries an isolation level (M6b); the OCC commit path validates the read-set under the requested level so a txn opts into the consistency it needs. |
| `CONCEPT:KG-2.185` | AT-SPI accessibility view | Exposes AT-SPI accessible elements as a structural view the engine can ingest/query. |
| `CONCEPT:KG-2.187` | Commit-before-ack durability | A durable mutation is committed to redb (group-commit fsync) BEFORE its Response is acked; a commit failure becomes an ERROR response, so an acked write is always on disk. Read once at startup into `ServerState.redb_authoritative`. |
| `CONCEPT:KG-2.188` | In-engine Raft replication | Cluster-tier `raft` feature: the engine runs as a multi-node HA cluster replicating its authoritative redb state via `openraft` 0.9; durable mutations route through Raft consensus before apply+ack, with automatic leader failover. Off ⇒ the write path is byte-for-byte the single-node path. |
| `CONCEPT:KG-2.189` | Postgres wire-protocol shim | A pg-wire front-end (`pgwire` facade feature) that lets standard Postgres clients speak to the engine's SQL surface over the wire. |
| `CONCEPT:KG-2.191` | Read-through-safe eviction | Under authoritative mode the per-graph node cap resumes enforcing (bounded memory) WITHOUT data loss: a `ReadThrough` seam (`crates/eg-core/src/read_through.rs`) serves an evicted node's stored blob from redb on a RAM miss, and a node is dropped from RAM only after a redb read confirms it is on disk. |
| `CONCEPT:KG-2.195` | redb-authoritative default (THE FLIP) | Built with the `redb` feature (folded into `full`/`node`/`cluster`/`pi`), the persist backend defaults to `redb` in authoritative mode whenever a persist dir is configured — the engine is a durable SOURCE OF TRUTH out of the box, with a one-time `.mp`/`.wal` → redb migration on first authoritative boot. |
| `CONCEPT:KG-2.197` | pg-wire extended/prepared protocol | The Describe step of the Postgres extended-query (prepared-statement) protocol over the pg-wire shim, where the shim can't statically know a result shape ahead of execution. |
| `CONCEPT:KG-2.198` | pg-wire SQL DML completeness | Completes INSERT/UPDATE/DELETE DML coverage over the pg-wire shim's SQL surface. |
| `CONCEPT:KG-2.200` | Quiet common-restart recovery | The common small restart path recovers without noisy warnings — only genuine anomalies log loudly. |
| `CONCEPT:KG-2.202` | pg-wire identity bridge | Bridges pg-wire (SCRAM) authentication to the engine's `AgentIdentity`, so the post-login pg user drives `IsolationLayer` ACL checks. |
| `CONCEPT:KG-2.204` | Durable redb Raft log | The Raft log, vote, and applied state live in the SAME `graph.redb` Database as M2 graph data, keyed by `(group_id, index)`/`(group_id, key)`; a log append and its graph mutation coalesce into ONE `WriteTransaction`/one fsync, and a restarted node recovers its log tail locally from redb. The separate `raft.redb` sidecar is gone. |
| `CONCEPT:KG-2.205` | Multi-Raft scaffold | A `MultiRaft` manager holds N openraft groups keyed by `GroupId`, sharing ONE TCP listener per node (frames tagged/demuxed by group id) and ONE shared `graph.redb`; a `GroupRouter` maps `graph_name → GroupId`. Group = transaction boundary (no cross-group txns yet). Runs one `DEFAULT_GROUP` while exercising routing/lifecycle/isolation in tests. |
| `CONCEPT:KG-2.206` | Content-addressed BLOB substrate | A streamed, content-addressed BLOB store (`blob` feature) for large binary payloads, with begin/chunk/commit/fetch/ref/unref/gc methods served over the protocol. |
| `CONCEPT:KG-2.207` | Native eg-ann vector index | Pure-Rust CPU IVF-PQ + OPQ + SQ8-refine vector index (`ann` feature, folded into `pi`/`node`/`cluster`/`full`) as the `SemanticStore` backend, replacing rebuild-on-load HNSW; a persisted index reopens WITHOUT rebuilding from raw vectors (`ann-redb` also stores the codes in the redb durable tier). |
| `CONCEPT:KG-2.208` | Cross-modal query plan | ONE cross-modal plan composing a DataFusion filter with vector/graph stages into a single execution pipeline. |
| `CONCEPT:KG-2.209` | Cost-based plan reorder | The same cross-modal plan is reordered by cost (a selective vs. a broad predicate) so the most selective stage runs first. |
| `CONCEPT:KG-2.210` | Native time-series store | A native time-series store (`tsdb` feature) with store + query primitives, present when the feature is compiled. |
| `CONCEPT:KG-2.211` | Unified Ebbinghaus decay curve | The ONE temporal-decay curve (`eg_core::decay`): the same Ebbinghaus function powers semantic-memory confidence decay and the tsdb time-series decay queries, so decay is defined once and shared. |
| `CONCEPT:KG-2.212` | Tenant-delete durable purge | `DeleteGraph` now durably PURGES the graph's redb rows (nodes/edges/ledger/semantic + the `graph_meta` identity), awaited commit-before-ack under authoritative mode, so a recreate of the SAME tenant name starts from a clean durable slate. Without it a deleted incarnation's rows survived (same sanitized key) and leaked into the recreated tenant via the read-through-on-RAM-miss path and via `load_all` — silently dropping/corrupting the new tenant's writes (tenant churn / resharding / hibernation rehydration). Default no-op for non-authoritative / non-redb backends. |

## Cross-Project References (from agent-utilities)

| Concept ID | Name | Origin |
|------------|------|--------|
| `CONCEPT:ECO-4.0` | Unified Toolkit Ingestion | agent-utilities |
| `CONCEPT:KG-2.0` | Knowledge Graph Core Core Architecture | agent-utilities |
| `CONCEPT:ORCH-1.0` | Multi-Agent Orchestration Abstraction | agent-utilities |
| `CONCEPT:KG-2.7` | Batch Materialization / Local SPARQL Fast Path | agent-utilities |
| `CONCEPT:KG-2.8` | Code/Test Enrichment & Interlinking (incl. `2.8r` cross-file call/import resolution) | agent-utilities |
| `CONCEPT:KG-2.171` | Cross-graph union reads (point/label/neighbor reads unioned across a graph set, deduped by id) | epistemic-graph |
| `CONCEPT:KG-2.178` | Internal SQL query surface (read-only `SELECT … FROM nodes` over one graph via DataFusion, behind the `query` feature) | epistemic-graph |
| `CONCEPT:KG-2.176` | Lazy secondary label index (`label → node ids`) for O(1) label lookup, invalidated on write | epistemic-graph |
| `CONCEPT:KG-2.177` | Pluggable `PersistenceBackend` + durable redb write-through tier (behind the `redb` feature) | epistemic-graph |
\n## CONCEPT:KG-2.20\nRust-Native Finance Compute Suite
\n## CONCEPT:KG-2.22\nGraph Network Protocols
\n## CONCEPT:KG-2.22\nData Science Primitives
\n## CONCEPT:KG-2.21\nAST Ingestion Pipeline
