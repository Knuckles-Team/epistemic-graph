# The master-of-all engine

This is the deep architectural reference for `epistemic-graph` — the one durable engine that unifies
graph, vector, SQL, RDF/SPARQL, OWL-2, time-series, content-addressed BLOB, full-text, and reasoning
behind a single cross-modal planner, distributed and replicated from a Raspberry Pi to an HA cluster.

For the entry-level map see [the overview](../overview.md); for build tiers see
[Tiers & binaries](tiers.md); for the protocol see [Service Mode](../service_mode.md).

---

## System context (C4 level 1)

```mermaid
flowchart TB
    AGENT["AI agent fleet (agent-utilities, graph-os, MCP)"]
    PSQL["psql / BI tools / ORMs"]
    DBCLI["Neo4j / Redis / MySQL / MSSQL / SQLite drivers"]
    MSG["AMQP / MQTT / STOMP pub-sub clients"]
    OBSAG["Log / metric / trace agents (OTLP · Elastic _bulk · Prometheus · Grafana)"]
    S3CLI["S3 clients (aws-cli / boto)"]
    LLM["vLLM / LMCache KV-block clients"]
    LAKE["Lakehouse engines (Databricks · Spark · Trino · DuckDB)"]
    OTELC["External OTel collector / Prometheus (remote-write)"]
    PEER["Peer epistemic-graph engines (federation / Raft / super-cluster)"]
    EXT["External Postgres / MySQL / HTTP-JSON sources"]

    ENGINE["epistemic-graph<br/>unified data · compute · messaging · observability · lakehouse engine<br/>(graph + vector + SQL + RDF/OWL + TSDB + BLOB + text + GIS + tensor + stream + broker + KV-cache + LTAP)"]

    AGENT -->|"MessagePack / UDS / TCP, HMAC"| ENGINE
    PSQL -->|"Postgres wire, SCRAM"| ENGINE
    DBCLI -->|"Bolt · RESP · MySQL · TDS wire"| ENGINE
    MSG -->|"broker wire protocols (exactly-once)"| ENGINE
    OBSAG -->|"OTLP/HTTP · PromQL · federated _search"| ENGINE
    S3CLI -->|"S3 REST, SigV4-lite, multipart"| ENGINE
    LLM -->|"KV-block GET/PUT by token-hash"| ENGINE
    ENGINE -->|"Parquet + Delta + Iceberg-REST, zero ETL (EG-KG.storage.lsn-as-snapshot-returns)"| LAKE
    ENGINE -->|"OTLP export + Prometheus remote-write (EG-316)"| OTELC
    ENGINE <-->|"Raft replication + cross-shard 2PC"| PEER
    ENGINE -->|"ForeignScan federation"| EXT
    ENGINE -->|"federated / super-cluster read"| PEER
```

## Container view (C4 level 2)

```mermaid
flowchart TB
    subgraph Process["epistemic-graph-server (one Rust process)"]
        subgraph Wire["Wire adapters (EG-KG.compute.subsystems-reference WireProtocol / WireSession — one exec path)"]
            NATIVE["native MessagePack (UDS/TCP, HMAC)"]
            PGW["pgwire"]
            SQLITEW["sqlite"]
            MYSQLW["mysql"]
            MSSQLW["mssql"]
            BOLTW["bolt (Neo4j)"]
            REDISW["redis (RESP)"]
            S3W["s3 REST"]
            BROKERW["amqp · mqtt · stomp"]
            OBSW["obs listener: OTLP · _bulk · PromQL · traces · _search"]
        end
        TRANSPORT["Transport + admission control<br/>(framed MessagePack, HMAC, BUSY shedding)"]
        QOS["QoS/SLO scheduler (EG-320):<br/>per-tenant/priority admission · deadline · backpressure"]
        SECURITY["Security layer<br/>(RLS GraphView filter, audit chain, AEAD-at-rest, durable RBAC)"]
        DISPATCH["Dispatch + per-domain handlers"]
        PLANNER["Unified RowSet planner (eg-plan)"]

        subgraph Cores["Storage and compute core"]
            GRAPHCORE["GraphCore (eg-core): petgraph + ledger + result cache + index manager"]
            ANN["Vector ANN: IVF-PQ + HNSW + exact/recall + cross-shard scatter (eg-ann)"]
            QUERY["SQL + Cypher (eg-query / DataFusion)"]
            RDFOWL["RDF / SPARQL / OWL / SHACL / ShEx (eg-rdf / eg-shacl / eg-shex)"]
            TSDB["Time-series + VRL (eg-tsdb)"]
            TEXT["Full-text (eg-text)"]
            BLOBC["BLOB CAS (blob / blob-s3)"]
            WASM["WASM UDF (eg-wasm)"]
            GEO["GIS (eg-geo)"]
            TENSOR["Tensor (eg-tensor)"]
            STREAM["Event/CEP (eg-stream)"]
        end

        subgraph Subsys["New cross-cutting subsystems"]
            BROKER["Message broker (eg-core/broker):<br/>exchanges · queues · streams · DLQ · TTL · exactly-once"]
            OBS["Observability: logs · PromQL (extended) metrics · traces · federated search · OTel/remote-write egress"]
            MEM["Agent-memory: summary · consolidation · decay · scene · trajectory (wire-Op surface, EG-KG.memory.eg-batch-decay-caller)"]
            KVC["KV-cache tiering (eg-kvcache): hot/warm(zstd)/cold + shared backend"]
            LAKE["LTAP lakehouse (eg-lake): Parquet · Delta · Iceberg-REST · LSN as-of"]
        end

        subgraph Durable["Durability and distribution"]
            REDB[("redb authoritative store + WAL apply")]
            COAL["write coalescer (group commit)"]
            RAFT["Multi-Raft groups + cross-shard 2PC"]
            CDC["CDC hub: streaming / subscriptions / triggers"]
        end
    end

    NATIVE --> TRANSPORT
    PGW & SQLITEW & MYSQLW & MSSQLW & BOLTW & REDISW & S3W --> DISPATCH
    BROKERW --> BROKER
    OBSW --> OBS
    TRANSPORT --> QOS --> SECURITY --> DISPATCH
    DISPATCH --> PLANNER --> GRAPHCORE
    DISPATCH --> GRAPHCORE
    GRAPHCORE --> ANN & QUERY & RDFOWL & TSDB & TEXT & BLOBC & WASM & GEO & TENSOR & STREAM
    DISPATCH --> BROKER & OBS & MEM & KVC & LAKE
    BROKER --> GRAPHCORE
    MEM --> GRAPHCORE
    OBS --> TSDB
    OBS --> TEXT
    KVC --> REDB
    LAKE --> QUERY
    LAKE --> BLOBC
    GRAPHCORE --> COAL --> REDB
    REDB <--> RAFT
    GRAPHCORE --> CDC
    CDC --> STREAM
```

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

The opt-in `EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot` mode reverts to the older rebuildable-cache
behavior (an in-memory cache + compute layer over an external system-of-record), where the `.mp`
snapshot + WAL exist only for fast warm restart. This is no longer the default.

---

## Cross-modal ACID write

A single durable `WriteTransaction` lands a graph mutation **and** a vector upsert **and** a blob
reference atomically across modalities — either all commit in the one redb transaction or none do (a
true rollback, no torn cross-modal write).

```mermaid
sequenceDiagram
    participant C as Client
    participant D as Dispatch
    participant G as GraphCore
    participant R as redb WriteTransaction

    C->>D: BeginTxn + TxnAddNode + TxnAddEmbedding + TxnBlobRef
    D->>G: stage write-set (nothing applied yet)
    C->>D: Commit
    D->>G: take topo.write once (serialization point)
    G->>R: open ONE WriteTransaction
    R->>R: put node rows + vector codes + blob ref
    alt all puts succeed
        R-->>G: group-commit fsync OK
        G-->>D: version bumped, applied
        D-->>C: ack (durable)
    else any modality fails
        R-->>G: drop transaction (nothing landed)
        G-->>D: rollback
        D-->>C: error (no partial write)
    end
```

The content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace) is the bytes tier under multimodal
`:Media`/`:Blob` nodes: `begin / chunk / commit / fetch / ref / unref / gc` stream large binaries over
the same transport (a chunk-get returns a raw MessagePack bin). The native CAS lives in `blob.redb`;
an explicit `blob-s3` build routes chunks to S3/MinIO behind the same `ChunkStore` trait — the lean
tiers link no object-store SDK.

---

## RDF / SPARQL / OWL over the property graph

The engine does not bolt on a separate triple-store: an RDF dataset is **projected onto the same
property graph** the rest of the engine uses, and serialized back out (Turtle / N-Triples via
oxrdf/oxttl). Multi-valued-literal predicates the property blob cannot hold live in an opt-in lossless
redb `quads` table (`rdf-redb`) and are unioned back on read.

```mermaid
flowchart LR
    subgraph RDFworld["RDF / OWL world"]
        TRIPLE["Triple: subject predicate object"]
        AXIOM["OWL axioms (TBox)"]
    end
    subgraph PG["Property graph"]
        NODE["Node (subject IRI)"]
        EDGE["Edge (object-property predicate)"]
        PROP["Property (literal predicate)"]
        QUADS[("quads table — multi-valued literals")]
    end
    subgraph Surfaces["Query surfaces"]
        SPARQLS["SPARQL 1.1 SELECT/ASK/CONSTRUCT/DESCRIBE + UPDATE + /sparql endpoint (spargebra to GraphView scans)"]
        OWLR["OWL 2 EL+ / RL reasoner"]
    end

    TRIPLE -->|"object is IRI"| EDGE
    TRIPLE -->|"object is literal"| PROP
    TRIPLE -->|"subject"| NODE
    PROP -.->|"multi-valued"| QUADS
    AXIOM --> OWLR
    NODE --> SPARQLS
    EDGE --> SPARQLS
    OWLR -->|"classification, consistency, justifications"| Surfaces
```

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

```mermaid
flowchart TB
    REQ["Request (agent_id, query)"]
    AUTH{"HMAC valid?"}
    IDENT{"any identity registered?"}
    SNAP["analysis_snapshot_versioned() under topo read lock"]
    RLS["IsolationLayer.filter_view(caller): keep owner / grant / manager / System rows"]
    CACHE{"result cache hit?<br/>key = (query-hash, version, rls_cache_hash)"}
    SURF["Query surface: SQL / Cypher / SPARQL / GraphQL / UnifiedQuery"]
    AUDIT["append to hash-chained audit log"]
    RESP["filtered result"]

    REQ --> AUTH
    AUTH -->|no| DENY["reject: auth failure"]
    AUTH -->|yes| IDENT
    IDENT -->|"no — single tenant"| SNAP
    IDENT -->|yes| SNAP
    SNAP --> RLS --> CACHE
    CACHE -->|hit| RESP
    CACHE -->|miss| SURF --> AUDIT --> RESP
```

With zero registered identities nothing is filtered (single-tenant back-compat). The result cache key
folds in the caller's RLS context (`rls_cache_hash`, agent-id-salted when rules exist), so agent A's
filtered cached result is never served to agent B for the same query text. Encryption-at-rest seals the
redb durable **value** blobs with ChaCha20-Poly1305 (keys stay plaintext so range scans work); a wrong
key fails the read rather than silently returning ciphertext.

---

## Streaming / CDC / the reactive substrate

Every durable mutation the dispatch shell records also emits an ordered, cursor-addressable `CdcEvent`
into a per-graph in-memory feed (a bounded ring + a Tokio `Notify`) — pure-Rust, so `streaming` folds
into every tier. From that one feed the engine drives CDC reads, incremental continuous queries, and
LISTEN/NOTIFY-style watches + triggers, all over the **same one-Response-per-Request transport** (no
side-channel socket).

```mermaid
flowchart LR
    WRITE["Durable mutation (dispatch write side-effect)"]
    LEDGER["per-graph change record (ledger)"]
    HUB["CdcHub: ordered ring + Notify"]
    CDC["CdcRead{from_seq} — tail by cursor"]
    CQ["ContinuousQuery — incremental aggregate"]
    WATCH["Watch{label,timeout} — long-poll, wakes on write"]
    TRIG["Trigger{label,op,action} — fired log"]
    COHERE["cross-replica cache invalidation"]

    WRITE --> LEDGER --> HUB
    HUB --> CDC
    HUB --> CQ
    HUB --> WATCH
    HUB --> TRIG
    HUB --> COHERE
```

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

```mermaid
flowchart LR
    subgraph Plan["One UnifiedQuery plan"]
        FS["ForeignScan{source}"]
        JOIN["join on id (foreign ∩ local)"]
        LOCAL["local Scan / Traverse / Rank"]
        LIM["Limit"]
    end
    subgraph Foreign["Foreign source kinds (ForeignSourceSpec)"]
        REMOTE["Remote epistemic-graph engine (same transport, HMAC)"]
        HTTP["HTTP / JSON API (rustls ureq)"]
        SQLSRC["External Postgres / MySQL (sqlx, runtime-tokio-rustls)"]
    end

    FS --> REMOTE
    FS --> HTTP
    FS --> SQLSRC
    FS --> JOIN
    LOCAL --> JOIN --> LIM
```

The HTTP/SQL clients are pure-Rust rustls stacks (no openssl) and are **in the one main build** —
a minimal server build links no ureq/rustls/sqlx. Federation is in the one main build.

---

## Distribution: multi-Raft, cross-shard 2PC, resharding, hibernation

The cluster tier runs the engine as a multi-node HA cluster. A `MultiRaft` manager holds N openraft
groups keyed by `GroupId`, sharing **one** TCP listener per node (frames tagged + demuxed by group id)
and **one** shared `graph.redb` (the Raft log shares M2's group-commit writer, so a log append + its
graph mutation coalesce into one fsync). A `GroupRouter` maps `graph_name -> GroupId`; a group is the
transaction boundary.

### Cross-shard 2PC (a transaction spanning groups)

```mermaid
sequenceDiagram
    participant CO as CrossShardCoordinator
    participant PA as Participant A (group 1)
    participant PB as Participant B (group 2)
    participant DB as durable redb (prepare / decision rows)

    CO->>PA: PREPARE (staged slice)
    PA->>DB: write xshard_prepare row
    PA-->>CO: vote YES
    CO->>PB: PREPARE (staged slice)
    PB->>DB: write xshard_prepare row
    PB-->>CO: vote YES
    alt all voted YES
        CO->>DB: write DECISION = commit (presumed-abort)
        CO->>PA: COMMIT
        CO->>PB: COMMIT
        PA->>DB: apply + clear prepare
        PB->>DB: apply + clear prepare
    else any vote NO or timeout
        CO->>DB: write DECISION = abort
        CO->>PA: ABORT
        CO->>PB: ABORT
    end
```

In-doubt transactions survive a coordinator or participant crash and are resolved deterministically
from the durable prepare/decision rows on boot (`recover_in_doubt`, run before serving). A single-group
txn stays the byte-for-byte single-node fast path. Distributed Pregel/GAS compute (`compute-dist`) runs
PageRank / connected-components / BFS across graphs whose vertices span multiple groups, and persists
named results as redb-backed materialized views reloaded on boot.

### Tenant lifecycle (create / hibernate / reshard / delete + purge)

Because one shared registry + one shared `graph.redb` is keyed by graph name, a "move" is re-pointing
ownership of future writes, not copying rows — so resharding is zero-downtime.

```mermaid
stateDiagram-v2
    [*] --> Resident: CreateGraph (records owner)
    Resident --> Hibernated: hibernate() drops RAM topology/props/vectors
    Hibernated --> Resident: rehydrate_graph() from durable redb dump
    Resident --> Resident: reshard_graph(A to B) quiesce, barrier, re-point router, resume
    Resident --> Purged: DeleteGraph durably purges redb rows
    Hibernated --> Purged: DeleteGraph durably purges redb rows
    Purged --> [*]
    note right of Purged
        Tenant-delete durable purge (EG-KG.backend.tenant-delete-recreate-same):
        nodes / edges / ledger / semantic / identity rows
        removed under commit-before-ack, so a recreate of
        the SAME tenant name starts from a clean slate.
    end note
```

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
