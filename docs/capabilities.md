# Capabilities & parity matrix

This page is the **operation-by-operation truth table** for epistemic-graph as a universal database. It
is verified against the source, not against intent. Legend:

- **✅ supported** — implemented and covered by tests.
- **🔶 in-progress** — partially built or actively being added; the unsupported part errors honestly.
- **🗺 roadmap** — designed, not yet built.

The **Feature** column is the Cargo feature that gates the surface; the
[tiers page](architecture/tiers.md) shows which prebuilt binary carries it.

## SQL (`eg-query/sql` + pgwire)

| Operation | Status | Feature | Evidence |
|-----------|:------:|---------|----------|
| `SELECT` — joins, aggregates, GROUP BY/HAVING, window, CTE, subquery, UNION | ✅ | `query` | DataFusion 43; `classify.rs` routes `Query` → `Read`, executed in `exec.rs` |
| Predicate pushdown into the `nodes` scan | ✅ | `query` | `NodesTableProvider` returns `Inexact`, narrows rows via a per-column equality index (`providers.rs`) |
| `INSERT` / `UPDATE` / `DELETE` on the `nodes` table | ✅ | `query` | `classify_insert`/`classify_update`/`classify_delete` (KG-2.198); maps to `add_node` / `compare_and_set_fields` / `remove_node` |
| `RETURNING` on DML | ✅ | `query` | pgwire write path returns affected rows |
| Complex/compound WHERE, `INSERT … SELECT`, JOIN/`FROM`/`USING` in DML | 🔶 | `query` | restricted to single `col = literal`; errors `… supports only a single \`<column> = <literal>\` WHERE` |
| DML on arbitrary user tables (`INSERT`/`UPDATE`/`DELETE`, `INSERT … SELECT`, `COPY`) | ✅ | `query` | durable redb `TableStore` (EG-018/EG-020); `run_insert_table`/`run_update_table`/`run_delete_table` |
| `CREATE` / `ALTER ADD COLUMN` / `DROP TABLE`, arbitrary user tables, DDL | ✅ | `query` | `crates/eg-query/src/tables/` durable catalog (EG-018); JOINable to the graph; `ALTER` beyond ADD COLUMN 🔶 |

### Postgres wire (`pgwire`, also pulled by `cluster`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| TCP listener, gated on `EPISTEMIC_GRAPH_PGWIRE_ADDR` | ✅ | `src/server/pgwire/mod.rs` `serve`/`serve_with_auth` |
| Simple query protocol | ✅ | `SimpleQueryHandler` |
| Extended / prepared protocol (Parse/Bind/Describe/Execute, `$N` params) | ✅ | `ExtendedQueryHandler`, `substitute_params` |
| SCRAM-SHA-256 auth (pg user → engine ACL actor) | ✅ | `auth.rs` `PgWireAuthMode::Scram` (KG-2.202) |
| Trust auth (dev) | ✅ | `auth.rs` `PgWireAuthMode::Trust` |
| `pg_catalog` / `information_schema` introspection | ✅ | `register_pg_catalog` + DataFusion `with_information_schema` (KG-2.201) |
| `SET graph = '<name>'` connection switch | ✅ | `mod.rs` |

## SPARQL (`eg-rdf`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `SELECT` query form | ✅ | `sparql.rs` `Query::Select` arm |
| Algebra: BGP, property paths, FILTER subset, OPTIONAL, UNION, GROUP/aggregate, BIND, DISTINCT, REDUCED, SLICE | ✅ | `eval_pattern` match in `sparql.rs` |
| Aggregates COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/SAMPLE | ✅ | `sparql.rs` aggregate evaluator |
| `ASK` / `CONSTRUCT` / `DESCRIBE` | ✅ | `sparql.rs` `Query::{Ask,Construct,Describe}` arms; `construct_graph`/`describe_resources` (gated by `rdf`, implied by `sparql`) |
| `UPDATE` (`INSERT/DELETE DATA`, `DELETE/INSERT WHERE`, `CLEAR`, `CREATE`/`DROP GRAPH`) | ✅ | `eg-rdf/src/update.rs` over a `GraphStore`; `LOAD` intentionally deferred (no HTTP fetch in write path) |
| `/sparql` HTTP endpoint (W3C SPARQL 1.1 Protocol) | ✅ | `src/server/sparql_http.rs` (EG-017), feature `sparql-http`; GET + POST query/update |
| True named graphs (multi-graph quad querying) | ✅ | `Dataset` over every registry graph; `GRAPH ?g`/constant-IRI scoping (`FROM`/`FROM NAMED` 🔶) |
| SPO/POS index + selectivity join-ordering | 🔶 | naive full-scan-per-pattern today (W2 follow-on) |
| Rich FILTER (regex, arithmetic, `IN`, builtins like `STR`/`LANG`) | 🔶 | subset evaluator; unknown ops are silently false |
| Sub-SELECT / SERVICE / MINUS / negated property set | 🗺 | error `unsupported algebra node` / `negated property set not supported` |

## OWL reasoning (`eg-rdf/owl`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| OWL 2 EL⁺ completion (CR-sub/conj/some/chain/subrole/bot/disjoint) | ✅ | `owl.rs` `saturate()` |
| OWL 2 RL property rules (transitive/symmetric/inverse/chains/domain) | ✅ | `parse_ontology` + completion |
| Classification, consistency (unsat → inconsistent) | ✅ | `owl.rs` `snapshot()` |
| Forward-chaining materialization + incremental `add_axioms` | ✅ | monotone fixpoint, resumed in place |
| Confidence-weighting (per-axiom `eg:confidence`, noisy-OR) | ✅ | `classify_weighted()` (KG-2.236) |
| Ebbinghaus time-decay of facts | ✅ | `eg_core::decay::ebbinghaus_weight`; `GRAPH_SERVICE_DECAY_HALF_LIFE` |
| Distributed / cross-shard reasoning (union TBox+ABox, one closure) | ✅ | `reason_distributed_weighted` |
| Query-time `Op::Reason` (reasoner seeds a RowSet) | ✅ | `wire.rs` `Op::Reason` under `owl-plan`; executor seeds class members |
| OWL-DL (tableau, cardinality, `complementOf`, nominals) | 🔶 | EL approximations of `allValuesFrom`/`hasValue` present; full tableau being added behind `owl-dl` |
| SWRL user rules | 🔶 | `rules.rs` Horn-rule DSL (`body → head @conf`) with range-safety; SWRL/RuleML atoms + built-in library being added |

## Cypher (`eg-query/cypher`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| `MATCH … WHERE … RETURN … LIMIT` (read-only, one snapshot) | ✅ | `exec_cypher(&GraphView, …)` |
| Patterns: labels, typed edges, both directions, var-length single hop | ✅ | `parser.rs` / VF2 + petgraph BFS |
| WHERE: `=, <>, !=, <, <=, >, >=`, AND-joined | ✅ | `parse_predicates` |
| Writes (`CREATE`/`MERGE`/`SET`/`DELETE`+`DETACH`) | ✅ | `exec_cypher_write` → `apply_create`/`apply_merge`/`apply_set`/`apply_delete` over eg-core mutations |
| `REMOVE` | 🔶 | not yet in the grammar (being added) |
| `ORDER BY` / `SKIP` / `WITH` / `OPTIONAL MATCH` / `OR` / aggregation / `DISTINCT` | 🔶 | being added to parser + executor |

## GraphQL (`eg-graphql`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Read queries: root label fields, `first`/`limit`, property filters, aliases, nested edge selections | ✅ | `resolver.rs` scan + BFS; schema introspected from the live graph |
| Mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`) | ✅ | `mutation.rs` `execute` over eg-core mutations; OCC bumped once per batch |
| Subscriptions | 🔶 | poll-only stub (`subscription.rs`); broadcast/CDC + WS/SSE being added |
| Fragments / variables / directives / relay pagination | 🔶 | rejected at parse today; being added |

## Vector / ANN (`eg-ann` + `eg-core`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| IVF-PQ index (coarse quantizer + 8-bit PQ codes) | ✅ | `ivfpq.rs` `train` |
| OPQ learned rotation (polar/SVD update) | ✅ | `ivfpq.rs` rotation update |
| SQ8 refine re-rank tier | ✅ | `ivfpq.rs` over-fetch + SQ8 re-rank (recall ≥ 0.95 in tests) |
| Persistent index — reopen WITHOUT rebuild (mmap codes, O(N) posting rebuild) | ✅ | `persist.rs` `open` |
| Parallel + SIMD brute-force fallback (rayon, contiguous arena, cached L2 norms) | ✅ | `semantic_store_ann.rs` |
| Warm-on-start (index built off the query path) | ✅ | `warm()` / `ensure_index` |
| Cross-shard kNN merge, hybrid metadata pre-filter | 🗺 | single-shard today |

## Time-series (`eg-tsdb`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb columnar store, group-amortized `append_batch`, range scan, retention | ✅ | `store.rs` |
| `time_bucket` (avg/min/max/sum/count/first/last) | ✅ | `query.rs` |
| ASOF backward join (the primitive DataFusion lacks) | ✅ | `query.rs` `asof_join_backward` |
| gap-fill LOCF, OHLC bars, downsample/rollup, decay-weighted mean, EWMA, rolling z-score | ✅ | `query.rs` |
| Time-ops as unified planner ops (`Op::Window` execution) | 🔶 | `Op::Window` is a pass-through seam in `eg-plan/exec.rs` today |
| Per-point retention trim (vs whole-bucket drop) | 🗺 | deferred |

## Blob / CAS (`src/server/blob`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Content-addressed (sha256) streaming store, manifest of chunk digests | ✅ | `store.rs` `ChunkStore` |
| redb-native backend, group-commit, refcount mark-and-sweep GC | ✅ | `RedbChunkStore` |
| S3 / MinIO backend behind the same `ChunkStore` trait | ✅ | `s3.rs` `S3ChunkStore` (`blob-s3`) |
| Content-defined chunking | 🗺 | fixed 2 MiB chunks today |

## Key-value / embedded (`redb`, `embedded`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| Embedded in-process engine over redb rows (no Tokio/socket/HMAC, commit-before-return) | ✅ | `src/embedded.rs` `EmbeddedEngine::open` (KG-2.216) |
| Durable graph-shaped tables (`nodes`/`edges`/`ledger`/`semantic_store`/`graph_meta`) | ✅ | `src/redb_store.rs` |
| Generic namespaced `get`/`put`/`delete`/`scan`/`cas` KV surface over redb | ✅ | `src/server/kv.rs` `KvStore` (EG-022); `Method::Kv*`; durable, commit-before-ack; not graph-scoped |
| Multi-wire SQL (SQLite / MySQL-MariaDB / MSSQL behind one `WireProtocol` trait) | 🔶 | Postgres ✅ via `pgwire`; SQLite/MySQL/MSSQL wires being added |

## Unified planner & UQL (`eg-plan`)

| Operation | Status | Evidence |
|-----------|:------:|----------|
| RowSet ops: `Scan, Filter, Traverse, Rank, RankNodeDistance, RankMentions, RankMmr, AsOf, Limit` | ✅ | `wire.rs` `Op`; `exec.rs` arms |
| `RankText`, `FuseRrf` | ✅ | `text` feature |
| `Reason`, `SparqlBgp` | ✅ | `owl-plan` feature |
| `Udf` (sandboxed WASM) | ✅ | `wasm-udf` feature |
| `ForeignScan` (remote engine / HTTP-JSON / external SQL) | ✅ | `federation` / `federation-sql` |
| `Window`, `Foreign` execution | 🔶 | pass-through seams in `exec.rs` today |
| UQL text DSL → `wire::Plan` (dependency-free parser) | ✅ | `eg-plan/src/uql` (KG-2.214) |
| Natural-language → query | 🗺 | only a reserved, rejected seam |

## Durability & distribution

| Operation | Status | Evidence |
|-----------|:------:|----------|
| redb-authoritative, commit-before-ack (`kill -9`-safe) | ✅ | `redb_backend.rs` `record_durable` |
| Cross-modal ACID (graph + vector + blob in one WriteTransaction) | ✅ | shared redb transaction |
| openraft replication + automatic failover (`raft`/`cluster`) | ✅ | `src/raft/mod.rs` |
| Cross-shard 2PC (presumed-abort, crash-recoverable) | ✅ | `src/raft/cross_shard_txn.rs` |
| Multi-Raft groups (N-group ring, online reshard, hibernate/rehydrate) | ✅ | `src/raft/multi.rs` `MultiRaft`/`GroupRouter` (KG-2.266/267/268); `reshard.rs` online ownership move |
| Non-blocking commit / 3PC / Calvin / parallel-commit | 🔶 | 2PC works; parallel-commit + read-only-participant + Calvin/3PC being added |
| Federation (remote/HTTP/external SQL) | ✅ | `federation`(`-sql`), OFF by default, never in `pi` |

See the [parity roadmap](roadmap.md) for the order in which the 🔶 / 🗺 items are being closed.
</content>
