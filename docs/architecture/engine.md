# The master-of-all engine

This is the deep architectural reference for `epistemic-graph` — the one durable engine that unifies
graph, vector, SQL, RDF/SPARQL, OWL-2, time-series, content-addressed BLOB, full-text, and reasoning
behind a single cross-modal planner, distributed and replicated from a Raspberry Pi to an HA cluster.

For the entry-level map see [the overview](../overview.md); for build composition see
[One build, opt-in layers](tiers.md); for the protocol see [Service Mode](../service_mode.md).

---

## System context (C4 level 1)

<div class="admonition architecture" markdown>
<p class="admonition-title">System context (C4 level 1)</p>

`epistemic-graph` is a unified data, compute, messaging, observability, and
lakehouse engine (graph + vector + SQL + RDF/OWL + TSDB + BLOB + text + GIS
+ tensor + stream + broker + KV-cache + LTAP). Inbound callers:

| Caller | Protocol into the engine |
|---|---|
| AI agent fleet (agent-utilities, graph-os, MCP) | MessagePack / UDS / TCP, HMAC |
| psql / BI tools / ORMs | Postgres wire, SCRAM |
| Neo4j / Redis / MySQL / MSSQL / SQLite drivers | Bolt · RESP · MySQL · TDS wire |
| AMQP / MQTT / STOMP pub-sub clients | broker wire protocols (exactly-once) |
| Log/metric/trace agents (OTLP, Elastic `_bulk`, Prometheus, Grafana) | OTLP/HTTP · PromQL · federated `_search` |
| S3 clients (aws-cli / boto) | S3 REST, SigV4-lite, multipart |
| vLLM / LMCache KV-block clients | KV-block GET/PUT by token-hash |

Outbound, the engine reaches: lakehouse engines (Databricks/Spark/Trino/
DuckDB) via Parquet + Delta + Iceberg-REST, zero ETL
(EG-KG.storage.lsn-as-snapshot-returns); an external OTel collector /
Prometheus via OTLP export + remote-write (EG-316); peer
`epistemic-graph` engines bidirectionally via Raft replication + cross-shard
2PC, and also for federated/super-cluster reads; and external Postgres/
MySQL/HTTP-JSON sources via `ForeignScan` federation.

</div>

## Container view (C4 level 2)

<div class="admonition architecture" markdown>
<p class="admonition-title">Container view (C4 level 2) — one Rust process</p>

**Wire adapters** (EG-KG.compute.subsystems-reference `WireProtocol`/
`WireSession` — one exec path): native MessagePack (UDS/TCP, HMAC) enters
through transport + admission control (framed, HMAC, BUSY shedding);
`pgwire`/sqlite/mysql/mssql/bolt(Neo4j)/redis(RESP)/s3-REST all enter
dispatch directly; `amqp`/`mqtt`/`stomp` enter the message broker directly;
the obs listener (OTLP/`_bulk`/PromQL/traces/`_search`) enters observability
directly.

Native traffic flows transport → the QoS/SLO scheduler (EG-320: per-tenant/
priority admission, deadline, backpressure) → the security layer (RLS
GraphView filter, audit chain, AEAD-at-rest, durable RBAC) → dispatch, which
routes either through the unified RowSet planner (`eg-plan`) or directly to
`GraphCore`.

**`GraphCore`** (`eg-core`: petgraph + ledger + result cache + index
manager) is the hub for the storage/compute core: vector ANN
(IVF-PQ + HNSW, `eg-ann`), SQL+Cypher (`eg-query`/DataFusion), RDF/SPARQL/
OWL/SHACL/ShEx (`eg-rdf`/`eg-shacl`/`eg-shex`), time-series+VRL
(`eg-tsdb`), full-text (`eg-text`), BLOB CAS, WASM UDF (`eg-wasm`), GIS
(`eg-geo`), tensor (`eg-tensor`), and event/CEP (`eg-stream`).

Dispatch also routes to the cross-cutting subsystems: message broker
(`eg-core/broker`: exchanges, queues, streams, DLQ, TTL, exactly-once),
observability, agent-memory (summary/consolidation/decay/scene/trajectory,
wire-Op surface), KV-cache tiering (hot/warm-zstd/cold), and the LTAP
lakehouse (`eg-lake`). Of these: the broker and agent-memory both feed back
into `GraphCore`; observability feeds time-series and full-text; KV-cache
tiering feeds redb; the lakehouse feeds SQL+Cypher and BLOB CAS.

**Durability and distribution:** `GraphCore` writes through the write
coalescer (group commit) into the redb authoritative store (the canonical
mutation applier), which replicates via multi-Raft groups + cross-shard
2PC. `GraphCore` also feeds the CDC hub (streaming/subscriptions/triggers),
which feeds the event/CEP engine.

</div>

---

## Durability: redb-authoritative (the default)

Built with the `redb` feature — in the one main build (and the `cluster` layer) — the persist
dir is the **authoritative source of truth** in authoritative mode (default whenever a persist dir is
set). Three rules make "authoritative" safe:

- **Commit-before-ack.** A durable mutation is fsynced to redb (group-commit) *before* its Response is
  acked. A commit failure becomes an ERROR response, so an acked write is always on disk. Many awaiting
  writers coalesce into one group-commit fsync.
- **Read-through-safe eviction.** The per-graph node cap stays enforced (bounded RAM) without data
  loss: a `ReadThrough` seam serves an evicted node's blob from redb on a RAM miss, and a node is
  dropped from RAM only after a redb read confirms it is on disk.
- **Backpressure, not drop.** The redb writer's bounded channel blocks for capacity off-reactor instead
  of shedding a mutation.

Served mode requires this authoritative redb contract and a durable directory;
there is no alternate write-behind persistence mode.

Portable and isolated `GraphCore` images use one strict MessagePack
`GraphSnapshot` schema. Every image carries the mandatory current schema version,
rejects unknown or missing fields, and is structurally bounded before deserialization.
Restore never interprets an older or partial shape as current; format conversion is an
explicit offline operation.

---

## Cross-modal ACID write

A single durable `WriteTransaction` lands a graph mutation **and** a vector upsert **and** a blob
reference atomically across modalities — either all commit in the one redb transaction or none do (a
true rollback, no torn cross-modal write).

<div class="admonition architecture" markdown>
<p class="admonition-title">Cross-modal write sequence</p>

The client sends `BeginTxn` + `TxnAddNode` + `TxnAddEmbedding` +
`TxnBlobRef`; dispatch stages the write-set on `GraphCore` — nothing is
applied yet. On `Commit`, dispatch takes `topo.write` once (the
serialization point) and `GraphCore` opens **one** redb `WriteTransaction`,
putting the node rows, vector codes, and blob ref together. If all puts
succeed, redb group-commit fsyncs, `GraphCore` bumps the version and applies
it, and dispatch acks durably to the client. If any modality fails, redb
drops the whole transaction (nothing lands), `GraphCore` rolls back, and
dispatch returns an error — no partial write.

</div>

The content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace) is the bytes tier under multimodal
`:Media`/`:Blob` nodes: `begin / chunk / commit / fetch / ref / unref / gc` stream large binaries over
the same transport (a chunk-get returns a raw MessagePack bin). The native CAS lives in `blob.redb`;
an explicit `blob-s3` build routes chunks to S3/MinIO behind the same `ChunkStore` trait — the lean
tiers link no object-store SDK.

---

## RDF / SPARQL / OWL over the property graph

The engine does not bolt on a separate triple-store: an RDF dataset is **projected onto the same
property graph** the rest of the engine uses, and serialized back out (Turtle / N-Triples via
oxrdf/oxttl). Multi-valued literals live in a reserved typed property inside the same authoritative
node image and therefore share its transaction, ownership, backup, and recovery boundary.

<div class="admonition architecture" markdown>
<p class="admonition-title">RDF/OWL projected onto the property graph</p>

A triple's subject becomes a node; when its object is an IRI, the triple
becomes an edge (object-property predicate); when its object is a literal,
it becomes a property, with multi-valued literals living in a `quads` side
table. OWL axioms (TBox) feed the OWL 2 EL⁺/RL reasoner, which produces
classification, consistency, and justifications for both query surfaces —
SPARQL 1.1 (SELECT/ASK/CONSTRUCT/DESCRIBE + UPDATE + `/sparql`, `spargebra`
to `GraphView` scans, fed by nodes and edges) and the reasoner itself.

</div>

The OWL 2 reasoner (CONCEPT:EG-KG.ontology.incremental-materialization/2.236) is pure-Rust — EL⁺ completion (the ELK/CEL core) unioned
with OWL 2 RL property rules — and reaches entailments the RL-only reasoner cannot (e.g.
`HumanHeart ⊑ HumanComponent` through `∃partOf.Body`). It is **confidence-weighted and time-decayed**:
each entailment carries a `[0,1]` confidence (axiom annotations × per-node confidence × Ebbinghaus
decay), and `OwlReason` accepts a `min_confidence` threshold. Both SPARQL and OWL plug into the unified
planner as the `SparqlBgp` and `Reason` source ops.

---

## Security & the RLS request path

Three pure-Rust security primitives (the `security` feature, in the one main build) make
the engine multi-tenant-safe: **per-agent Row-Level Security**, **encryption-at-rest**, and a
**hash-chained audit log**. The critical property: RLS filters the `GraphView` *before* any query
surface sees it, so **no query language can exfiltrate a forbidden row**.

<div class="admonition architecture" markdown>
<p class="admonition-title">RLS request path</p>

A request (eg2 authority, query) is first checked: eg2 + deployment policy
+ replay validity. On failure it is rejected as an auth failure. On success,
the engine takes `analysis_snapshot_versioned()` under the topo read lock,
then `IsolationLayer.filter_view(caller)` keeps only owner/grant/manager/
System rows. The filtered view is checked against the result cache (keyed
by query-hash, version, and `rls_cache_hash`): a hit returns the filtered
result directly; a miss runs the query surface (SQL/Cypher/SPARQL/GraphQL/
UnifiedQuery), appends to the hash-chained audit log, and then returns the
filtered result.

</div>

An empty durable identity store grants no graph access. Its only admitted mutation
is the exact signer-backed `security:bootstrap` self-registration that creates the
first `System` identity; normal durable RBAC applies immediately afterward. RLS is
always default-deny, and the result cache key folds in the caller's complete RLS
context (`rls_cache_hash`), so one authority's filtered result is never served to
another. Encryption-at-rest seals redb durable **value** blobs with
ChaCha20-Poly1305 (keys stay plaintext so range scans work); a wrong key fails the
read rather than silently returning ciphertext.

---

## Streaming / CDC / the reactive substrate

Every durable mutation the dispatch shell records also emits an ordered, cursor-addressable `CdcEvent`
into a per-graph in-memory feed (a bounded ring + a Tokio `Notify`); `streaming` is
included in the main build. From that one feed the engine drives CDC reads, incremental continuous queries, and
LISTEN/NOTIFY-style watches + triggers, all over the **same one-Response-per-Request transport** (no
side-channel socket).

<div class="admonition architecture" markdown>
<p class="admonition-title">CDC fan-out</p>

Every durable mutation (a dispatch write side-effect) becomes a per-graph
change record in the ledger, which feeds the `CdcHub` (an ordered ring +
`Notify`). The hub fans out to five consumers: `CdcRead{from_seq}` (tail by
cursor), `ContinuousQuery` (incremental aggregate), `Watch{label,timeout}`
(long-poll, wakes on write), `Trigger{label,op,action}` (fired log), and
cross-replica cache invalidation.

</div>

A continuous query is seeded from the graph's current state at registration and updated by delta on
each change, so it equals a full re-run. A `Watch` returns matching changes since the cursor or awaits
the per-graph `Notify` up to `timeout_ms`, then returns a `WatchBatch{events, next_seq}` the client
resumes from. The same CDC feed drives distributed cache-coherence: a write on replica A retires
replica B's cached result for that graph.

---

## Query federation (including external SQL)

A federated `UnifiedQuery` reads an **external** source as a RowSet and composes it with the local
graph/vector/SQL ops in **one** plan — no Python round-trip. The `Op::ForeignScan` source op is the
resolved executor; the UQL `FOREIGN "<name>"` clause is the lighter name marker resolved against the
server-side `foreign_sources` registry.

<div class="admonition architecture" markdown>
<p class="admonition-title">Query federation plan</p>

Within one `UnifiedQuery` plan, `ForeignScan{source}` resolves against one
of three foreign source kinds (`ForeignSourceSpec`): a remote
epistemic-graph engine (same transport, HMAC), an HTTP/JSON API (rustls
`ureq`), or an external Postgres/MySQL source (`sqlx`,
`runtime-tokio-rustls`). The foreign scan's rows join on id with the
local Scan/Traverse/Rank result, and the joined rows flow through `Limit`.

</div>

The HTTP/SQL clients are pure-Rust rustls stacks (no openssl) and are **in the one main build** —
a minimal server build links no ureq/rustls/sqlx. Federation is in the one main build.

---

## Distribution: multi-Raft, cross-shard 2PC, resharding, hibernation

The `cluster` feature runs the engine as a multi-node HA cluster. A `MultiRaft` manager holds N openraft
groups keyed by `GroupId`, sharing **one** TCP listener per node (frames tagged + demuxed by group id)
and **one** shared authoritative shard (the Raft log shares M2's group-commit writer, so a log append + its
graph mutation coalesce into one fsync). A `GroupRouter` maps `graph_name -> GroupId`; a group is the
transaction boundary.

### Cross-shard 2PC (a transaction spanning groups)

<div class="admonition architecture" markdown>
<p class="admonition-title">Cross-shard 2PC sequence</p>

The `CrossShardCoordinator` sends `PREPARE` (a staged slice) to each
participant group; each writes an `xshard_prepare` row to durable redb and
votes YES (or NO, or times out). If every participant voted YES, the
coordinator writes `DECISION = commit` (presumed-abort) and sends `COMMIT`
to each, and each applies and clears its prepare row. If any vote was NO or
timed out, the coordinator writes `DECISION = abort` and sends `ABORT` to
each instead.

</div>

In-doubt transactions survive a coordinator or participant crash and are resolved deterministically
from the durable prepare/decision rows on boot (`recover_in_doubt`, run before serving). A single-group
txn stays the byte-for-byte single-node fast path. Distributed Pregel/GAS compute (`compute-dist`) runs
PageRank / connected-components / BFS across graphs whose vertices span multiple groups, and persists
named results as redb-backed materialized views reloaded on boot.

### Tenant lifecycle (create / hibernate / reshard / delete + purge)

Because one shared registry + one shared authoritative shard is keyed by graph name, a "move" is re-pointing
ownership of future writes, not copying rows — so resharding is zero-downtime.

<div class="admonition architecture" markdown>
<p class="admonition-title">Tenant lifecycle states</p>

A graph starts **Resident** on `CreateGraph` (records the owner). From
Resident, `hibernate()` drops in-RAM topology/props/vectors, moving to
**Hibernated**; `rehydrate_graph()` reads back the durable redb dump to
return to Resident. Resident can also self-transition via
`reshard_graph(A to B)` (quiesce, barrier, re-point router, resume). From
either Resident or Hibernated, `DeleteGraph` durably purges the redb rows,
moving to **Purged** (terminal). Tenant-delete durable purge
(EG-KG.backend.tenant-delete-recreate-same) removes nodes/edges/ledger/
semantic/identity rows under commit-before-ack, so recreating the same
tenant name starts from a clean slate.

</div>

Cold-tenant hibernation drops the in-RAM state while the durable redb rows + read-through seam stay
intact (extended by the cold-tier object-store seam for whole-graph offload). The per-tenant memory
budget (CONCEPT:EG-KG.compute.lane-v) drives this automatically: a tenant over its byte budget has its coldest
graphs evicted (durability-gated LRU) then hibernated, with a global ceiling + fair per-tenant caps so
one hot tenant cannot starve others. See [the cost model](../cost_model.md).

---

## WASM-sandboxed UDFs

An agent can push a custom compute function as a WebAssembly module the engine runs **sandboxed** over
a RowSet: wasmtime with fuel-metering (an infinite loop is fuel-killed, never a hang), a hard memory
cap, and **no host capabilities** (a module importing fs/net is rejected). `RegisterUdf{id, wasm}`
compiles + caches it; `RunUdf{id, input}` runs it off-reactor; and the `Op::Udf{id}` plan op runs a
registered UDF as a `RowSet -> RowSet` transform inside a unified query. The wasmtime/cranelift runtime
is heavy, but pure-Rust, so it ships in the one main build.

---

## Related references

- [Subsystems (C4 container level)](subsystems.md) — the broker, observability, GIS, tensor, stream, KV-cache, agent-memory, LTAP lakehouse, and multi-wire subsystems and how they compose on the one substrate.
- [Lakehouse LTAP interop (EG-KG.storage.lsn-as-snapshot-returns)](lakehouse_ltap.md) — the eg-lake Parquet/Delta/Iceberg egress tier that makes the engine Databricks-interoperable with zero ETL.
- [Tiers & binaries](tiers.md) — which features ship in which binary, and the prebuilt sizes.
- [Engine modes](../engine_modes.md) — remote / shared-local / autostart resolution + the auto-bundle.
- [Deployment](../deployment.md) — Docker / wheel / single-node / HA recipes.
- [Write coalescer](write_coalescer.md) · [Index manager](index_manager.md) · [Correctness harness](correctness_harness.md).
