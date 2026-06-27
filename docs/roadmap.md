# Universal-DB parity roadmap

epistemic-graph already converges seven modalities under one durable engine and one planner. This page
tracks the **remaining gaps** to full drop-in parity with the systems it replaces, with an honest
status for each. It is the companion to the [capability matrix](capabilities.md).

Status: **🔶 in-progress** (actively being built) · **🗺 roadmap** (designed, not started).

## SQL — toward full Postgres/SQLite parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Complex/compound WHERE on DML, `INSERT … SELECT` | 🔶 | KG-2.198 follow-up — generalise the DML WHERE decoder beyond single `col = literal` |
| Arbitrary user tables + DDL (`CREATE`/`ALTER`/`DROP TABLE`) | 🔶 | a user-table catalog over redb + DDL statement handling in `classify.rs`; **being added now** |
| Views, transactions over the wire (`BEGIN`/`COMMIT`) | 🗺 | after user tables |

## SPARQL — toward full Stardog/GraphDB parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| `ASK` / `CONSTRUCT` / `DESCRIBE` query forms | 🔶 | match the remaining `spargebra::Query` variants in `sparql.rs`; **being added now** |
| `/sparql` HTTP endpoint (SPARQL Protocol) | 🔶 | HTTP route in front of `Method::Sparql`; **being added now** |
| SPARQL `UPDATE` (`INSERT/DELETE DATA`, `INSERT/DELETE WHERE`) | 🔶 | `spargebra::Update` parse → graph mutations; **being added now** |
| True named graphs (multi-graph quad querying) | 🔶 | promote the request-graph binding to a real quad dataset |
| SPO/POS triple index + selectivity join-ordering | 🔶 | replace naive full-scan-per-pattern (W2) |
| Rich FILTER (regex, arithmetic, `IN`, `STR`/`LANG`/…) | 🔶 | extend the FILTER evaluator |
| Sub-SELECT / SERVICE / MINUS / negated property sets | 🗺 | additional algebra arms |

## OWL — toward full DL-reasoner parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| OWL-DL (tableau: cardinality, `allValuesFrom`, nominals, negation) | 🗺 | a DL reasoner alongside the EL+RL core |
| SWRL user rules | 🗺 | rule-atom parsing + safe rule evaluation |
| `rdfs:range` enforcement in completion | 🗺 | currently parsed but unused |

## Graph — toward full Neo4j parity

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Cypher writes (`CREATE`/`MERGE`/`SET`/`DELETE`) | 🗺 | a write planner over the graph mutation methods |
| Cypher `ORDER BY` / `SKIP` / `WITH` / `OPTIONAL MATCH` / `OR` / aggregation | 🗺 | grammar + executor extensions |
| Multi-hop var-length beyond a single hop | 🗺 | generalise the BFS executor |

## GraphQL

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Mutations | 🗺 | a write resolver over graph mutation methods |
| Subscriptions (live queries over CDC) | 🗺 | bind to the streaming/CDC layer |
| Fragments / variables / directives / relay pagination | 🗺 | parser + resolver extensions |

## Time-series

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Time-ops as unified planner ops (`Op::Window` execution) | 🔶 | wire the eg-tsdb functions behind `Op::Window` in `eg-plan/exec.rs` (the "D-bind" increment) |
| Per-point retention trim | 🗺 | finer-grained eviction than whole-bucket drop |

## Vector

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Cross-shard kNN merge | 🗺 | scatter-gather over Raft groups + top-k merge |
| Hybrid metadata pre-filtering | 🗺 | push graph/SQL predicates into the ANN scan |

## Blob / KV

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Content-defined chunking | 🗺 | replace fixed 2 MiB chunks for better dedup |
| Generic `get`/`put` KV surface | 🗺 | a KV view over redb for RocksDB-style use |
| SQLite-compatible wire / file surface | 🗺 | optional embedded SQL surface |

## Unified planner / UQL

| Gap | Status | What lands it |
|-----|:------:|---------------|
| `Op::Window` / `Op::Foreign` execution (currently pass-through) | 🔶 | bind to eg-tsdb / federation respectively |
| Natural-language → query (NL → UQL/Plan) | 🗺 | an LLM front-end emitting `wire::Plan`; today only a reserved, rejected seam |

## Distribution

| Gap | Status | What lands it |
|-----|:------:|---------------|
| Multi-Raft groups (N-group resharding, online ownership move) | 🔶 | promote the `GroupRouter` scaffold from single `DEFAULT_GROUP` to N groups |
| Non-blocking commit / 3PC / Calvin / >2-group transactions | 🗺 | beyond classic presumed-abort 2PC |

---

**Reading this as a contributor?** Pick a 🔶 row — those are the active edges. The SQL DDL/user-tables
and SPARQL ASK/CONSTRUCT/UPDATE/endpoint work are in flight right now; coordinate before starting to
avoid colliding with the Rust changes landing in `crates/eg-query` and `crates/eg-rdf`.
</content>
