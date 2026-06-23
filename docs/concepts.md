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
