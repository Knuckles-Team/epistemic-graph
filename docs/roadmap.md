# Universal-DB parity roadmap

epistemic-graph converges nine modalities under one durable engine and one planner. This page tracks the
**remaining gaps** to full drop-in parity, with an honest status. It is the companion to the
[capability matrix](capabilities.md). Status: **🔶 in-progress** · **🗺 designed, not started**.

> **Recently shipped** (these were previously listed here as roadmap and are now ✅ — see the capability
> matrix for evidence): SQL DDL + arbitrary user tables + `COPY`; SPARQL `ASK`/`CONSTRUCT`/`DESCRIBE`,
> `UPDATE`, and the W3C `/sparql` HTTP endpoint; true named-graph quad dataset; Cypher writes
> (`CREATE`/`MERGE`/`SET`/`DELETE`); GraphQL mutations; the generic KV surface; **multi-Raft groups +
> `GroupRouter` + N-group ring + online resharding**; and **N-participant cross-shard 2PC** with crash
> recovery. The "Universal-DB Program" below closes everything that remains so this page can reach empty.

## SQL — toward full Postgres + multi-wire parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Compound/complex WHERE on DML, `INSERT … SELECT` into `nodes`, `UPDATE…FROM`/`DELETE…USING` | 🔶 | EG-045/046/047 — `RowPredicate` decoder + DataFusion id-resolution + serializable CAS gates |
| `ON CONFLICT` upsert + `RETURNING` on user tables | 🔶 | EG-048 |
| Transactions over the wire (`BEGIN`/`COMMIT`, mixed graph + user-table) | 🔶 | EG-049 — buffered node ops + RYOW + `TransactionStatus` (T/E/I) |
| `CREATE VIEW` / `DROP VIEW` | 🔶 | EG-072 — durable view catalog + expansion in `build_ctx` |

## SPARQL — toward full Stardog/GraphDB parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Content negotiation (results XML/CSV/TSV, Turtle) | 🔶 | EG-050 — Accept-aware serializers on `/sparql` |
| Rich FILTER (regex, arithmetic, `IN`, `STR`/`LANG`/`DATATYPE`/string/type builtins) | 🔶 | EG-053 — extend the expression evaluator |
| `FROM` / `FROM NAMED` dataset clauses | 🔶 | EG-054 — honor the parsed dataset spec |
| Sub-SELECT · SERVICE federation · MINUS · negated property set `!p` | 🔶 | EG-051/052/055/056 — additional algebra arms + a federation seam |
| SPO/POS triple index + selectivity join-ordering | 🔶 | EG-057 — replace full-scan-per-pattern |

## OWL — toward full DL-reasoner parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| `rdfs:range` enforcement in EL completion | 🔶 | EG-058 — lift range into the EL closure (RL/instance use already ships) |
| OWL-DL (tableau: cardinality, `complementOf`, nominals, negation) | 🔶 | EG-059 — a tableau reasoner behind `owl-dl`, alongside the EL⁺∪RL core |
| SWRL / RuleML rules + built-in library | 🔶 | EG-060 — extend the existing Horn-rule DSL |

## Graph — toward full Neo4j parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Cypher `REMOVE` | 🔶 | EG-061 — `WriteOp::Remove` + parser branch (fix the mis-route) |
| Cypher `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/`DISTINCT` | 🔶 | EG-062 — grammar + executor extensions |
| Var-length combined with fixed hops + path binding | 🔶 | EG-063 — generalize the BFS executor |

## GraphQL

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Subscriptions (live queries over CDC) | 🔶 | EG-064 — `broadcast` change-stream off `mark_dirty` + WS/SSE carrier |
| Fragments / variables / directives | 🔶 | EG-065 — parser + resolver extensions |
| Relay pagination (`edges`/`pageInfo`/cursor/`after`) | 🔶 | EG-066 |

## Time-series · Vector · Blob

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Time-ops as unified planner ops (`Op::Window` execution) | 🔶 | EG-067 — wire eg-tsdb behind `Op::Window` (the eg-plan→eg-tsdb edge) |
| Per-point retention trim | 🔶 | EG-068 — trim straddling buckets |
| Cross-shard kNN merge | 🔶 | EG-069 — scatter-gather + top-k merge |
| Hybrid metadata pre-filtering | 🔶 | EG-070 — push predicates into `ivfpq::search` |
| Content-defined chunking | 🔶 | EG-071 — FastCDC/Gear rolling hash |

## Multi-wire (one `WireProtocol` trait → many databases)

| Gap | Status | What lands it |
|-----|:------:|---------------|
| `WireProtocol` trait extracting the wire-agnostic core | 🔶 | EG-074 — Postgres `pgwire` refactored behind it |
| SQLite surface (`.db` file export + served) | 🔶 | EG-075 |
| MySQL / MariaDB wire | 🔶 | EG-076 |
| MSSQL TDS wire | 🔶 | EG-077 |

## Unified planner / UQL

| Gap | Status | What lands it |
|-----|:------:|---------------|
| `Op::Foreign` name → `ForeignSourceSpec` resolution | 🔶 | EG-073 — server-side `foreign_sources` registry (`ForeignScan` already executes) |
| Natural-language → query (dual-mode) | 🔶 | EG-078/079/080 — `NlPlanner` seam; agent-utilities LLM (opt-out) or standalone `config.json` |

## Distribution

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Parallel-commit + read-only-participant fast path | 🔶 | EG-081 — 2PC optimizations |
| Non-blocking commit (3PC / Paxos-Commit / Calvin deterministic ordering) | 🔶 | EG-082 — behind a feature, with a correctness harness |

---

**Reading this as a contributor?** Every row above is being actively closed under the Universal-DB Program
(EG-045..082). Each lands with tests + a `docs/concepts.md` entry, and its row flips to ✅ in the
[capability matrix](capabilities.md) as it merges. The goal is for this page to reach empty.
