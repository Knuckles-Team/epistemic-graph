# epistemic-graph

<p align="center">
  <b>The unified, Rust-native "master-of-all" data & compute engine for AI agent infrastructure</b><br>
  <sub>Graph · vector · SQL · RDF/SPARQL · OWL-2 · time-series · content-addressed BLOB · full-text · reasoning —
  one durable engine, one unified planner, from a Raspberry Pi to a replicated cluster.</sub>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-0.34.0-blue" alt="Version">
  <img src="https://img.shields.io/badge/language-Rust%20%7C%20Python-orange" alt="Language">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> **Documentation** — The full architecture, the tier/binary map, deployment recipes for every
> scale, the engine-resolution modes, the service-mode protocol, the Rust compute reference, and the
> concept registry live in the [official documentation](https://knuckles-team.github.io/epistemic-graph/).

> **This is the compute & storage engine for
> [`agent-utilities`](https://github.com/Knuckles-Team/agent-utilities)** — a standalone Rust service
> reached out-of-process over MessagePack/UDS (no PyO3), or embedded in-process. You can run it on its
> own (binary + pure-Python client), or let `agent-utilities` drive it. Contributing? See
> [CONTRIBUTING.md](CONTRIBUTING.md).

---

## What it is

`epistemic-graph` collapses what is normally a rack of separate systems — a graph database, a vector
index, a SQL warehouse, a triple-store + reasoner, a time-series DB, a blob store, a search index — into
**one durable engine with one unified query planner**. Every modality is a first-class citizen of the
same `RowSet` algebra, so a single plan can seed candidates from an OWL inference or a SPARQL pattern,
filter them with SQL, traverse the graph, re-rank by vector similarity *and* BM25 text, fuse the two,
and run a sandboxed WASM UDF — without ever leaving the engine or marshalling data back to Python.

It is **durable by default**: built with the `redb` feature (folded into every deployment tier), the
persist dir is the **authoritative source of truth** and an acked write survives `kill -9`
(commit-before-ack). It scales by configuration alone — the *same binary family* runs as an embedded
in-process library on a Pi, a single durable server, or a multi-node Raft cluster with cross-shard
transactions.

### The unified substrate (one engine, every modality)

| Modality | What it gives you | Surfaced as |
|----------|-------------------|-------------|
| **Property graph** | petgraph `StableDiGraph` core, traversal, PageRank, centrality, community, VF2, blast-radius | always-on |
| **Vector / ANN** | native pure-Rust IVF-PQ + OPQ + SQ8-refine index; reopens without rebuilding | `ann` |
| **SQL** | DataFusion `SELECT` over `nodes`/`edges`, predicate pushdown, cross-modal plans | `query` |
| **Postgres wire** | psql / BI tools / ORMs speak SQL over pg-wire (SCRAM auth maps to agent identity) | `pgwire` |
| **RDF / SPARQL** | RDF dataset mapped onto the property graph; SPARQL 1.1 SELECT (oxrdf/spargebra) | `rdf` / `sparql` |
| **OWL 2 reasoning** | EL⁺ completion + RL rules, classification/consistency, confidence-weighted + time-decayed | `owl` |
| **Time-series** | native redb-backed TS store: ASOF, gap-fill, time_bucket, OHLC, Ebbinghaus decay | `tsdb` |
| **Content-addressed BLOB** | streamed CAS bytes tier under multimodal `:Media`/`:Blob` (redb-native or S3) | `blob` |
| **Full-text** | Tantivy BM25 inverted index, lexical `RankText` + reciprocal-rank fusion | `text` |
| **GraphQL** | pure-Rust GraphQL read surface compiled to scans + BFS | `graphql` |
| **Finance / data-science** | portfolio/risk/regime/HFT, OLS/trees/SVR estimators (replaces numpy/sklearn on the hot path) | `finance` / `datascience` |

All of it sits behind **one `RowSet` planner** whose ops compose freely —
`Scan · Filter · Traverse · Rank · RankText · FuseRrf · Reason · SparqlBgp · Udf · ForeignScan · AsOf · Window · Foreign · Limit`.
See **[docs/overview.md](docs/overview.md)** for the pipeline and **[docs/architecture/engine.md](docs/architecture/engine.md)**
for the full architecture.

---

## Architecture at a glance

```mermaid
flowchart TB
    subgraph Clients["Clients"]
        AU["agent-utilities / graph-os"]
        PY["epistemic_graph Python client"]
        PG["psql / BI / ORM"]
        EMB["Embedded in-process caller (Pi/edge)"]
    end

    subgraph Engine["epistemic-graph-server (one Rust process)"]
        T["Transport: length-prefixed MessagePack over UDS / TCP, HMAC-SHA256"]
        SEC["Security: per-agent RLS + audit chain + encryption-at-rest"]
        PLAN["Unified RowSet planner (cost-reordered, cross-modal)"]
        CORE["GraphCore: petgraph + ledger + result cache"]

        subgraph Modalities["Modalities (feature-gated, one core)"]
            VEC["Vector ANN (eg-ann)"]
            SQL["SQL (eg-query / DataFusion)"]
            RDF["RDF / SPARQL / OWL (eg-rdf)"]
            TS["Time-series (eg-tsdb)"]
            TXT["Full-text (eg-text)"]
            BLOB["BLOB CAS (blob)"]
            WASM["WASM UDF (eg-wasm)"]
        end

        subgraph Durability["Durability and distribution"]
            REDB[("redb authoritative store")]
            RAFT["Multi-Raft replication + cross-shard 2PC (cluster)"]
            CDC["CDC / streaming / subscriptions"]
        end
    end

    AU --> T
    PY --> T
    PG --> SQL
    EMB --> CORE
    T --> SEC --> PLAN --> CORE
    CORE --> Modalities
    CORE --> REDB
    REDB <--> RAFT
    CORE --> CDC
```

---

## Deployment tiers and the four prebuilt binaries

The same engine ships as a small family of prebuilt, size-optimized binaries (`release-tiny` profile).
A Pi **pulls a prebuilt wheel and never compiles**. Full build/wheel recipes are in
**[docs/deployment.md](docs/deployment.md)**; the feature-composition map is in
**[docs/architecture/tiers.md](docs/architecture/tiers.md)**.

| Binary | Approx. size | Adds over the tier below | For |
|--------|--------------|--------------------------|-----|
| **pi** | ~6.46 MB | redb-authoritative + cypher + ann + rdf/sparql/owl + streaming + result-cache + cost (NO DataFusion / Tantivy / Raft) | Raspberry Pi / edge, ultra-lean |
| **pi-max** | ~6.96 MB | + tsdb + blob + security — **all pure-Rust, still no C toolchain / no DataFusion** | Pi-3 "everything without a C compiler" |
| **node** | (single-node) | + DataFusion SQL + GraphQL + Tantivy text + wasm-udf + federation (incl. external SQL) + finance/datascience/ast | single durable server |
| **cluster** | (HA) | + Raft replication + pgwire + distributed Pregel compute + cross-shard 2PC | multi-node HA / SQL clients |
| **full** | ~58.67 MB | every single-node feature, size-optimized (the "contains-all smallest"; no raft/pgwire) | workstation / one binary, every feature |

Every tier is **redb-authoritative** — the persist dir is a durable source of truth at every scale.
The stale "tiny = SQLite/LadybugDB" and "L0/L1/L2/L3 tier" vocabulary is gone: **tiny is just the
auto-started `pi`-tier engine binary** — the engine is the one store at every scale.

---

## Three engine modes + the auto-bundle

`agent-utilities` reaches an engine through **one resolver** (`EngineResolver`, CONCEPT:OS-5.63) by a
single precedence — no per-entrypoint code:

```
remote  ->  shared-local  ->  autostart
```

A configured remote (Docker on another host) is used as-is and never autostarts; a co-located engine
already serving is reused; otherwise a **detached, supervised** engine is autostarted under a
first-one-wins lock and **reference-counted idle-shuts-down** after its last client disconnects (or
runs forever in the persistent lifecycle). Details + the decision flow: **[docs/engine-modes.md](docs/engine-modes.md)**.

For the embedded/edge story, the `embedded` feature gives a SQLite/DuckDB-style in-process handle
(`EmbeddedEngine`) over the *same* GraphCore + redb durable rows — no Tokio, no socket, no HMAC — the
"100M agents, a local engine each" path.

---

## Distribution & durability

The architecture is durable and highly-available — what was once a single in-memory process is now a
replicated source of truth:

- **redb-authoritative by default.** A committed write is fsynced to redb *before* the client is acked
  (commit-before-ack); an acked write survives a hard crash. Eviction is read-through-safe (an evicted
  node is served back from redb, never lost), and the writer applies backpressure rather than dropping.
- **In-engine Raft replication (cluster tier).** `openraft` replicates the authoritative redb store
  across nodes; the Raft log shares the one `graph.redb` (a log append + its graph mutation coalesce
  into one fsync). Leader failover is automatic. Off ⇒ the write path is byte-for-byte single-node.
- **Cross-shard 2PC + online resharding + tenant hibernation.** A transaction spanning multiple Raft
  groups commits atomically via presumed-abort two-phase commit, surviving coordinator/participant
  crashes. Tenants reshard with zero downtime (re-point ownership, not copy rows) and cold tenants
  hibernate to the durable tier, rehydrating on access.
- **Cross-modal ACID.** A graph mutation + a vector upsert + a blob reference land in **one** redb
  `WriteTransaction` — all modalities commit together or none do.

> Earlier docs described "no replication, no HA, RPO = checkpoint interval, no WAL". That described the
> opt-in rebuildable-cache mode (`EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot`) and is no longer the
> default — the stock engine is a durable, replicable source of truth.

---

## Security & isolation

- **Auth is mandatory.** Every RPC carries `HMAC-SHA256(secret, request_id)`; the server refuses to
  start with an empty secret (`--allow-insecure` opts out, dev only).
- **Per-agent Row-Level Security.** Once any identity is registered, the read/plan-path `GraphView` is
  filtered to the rows the caller may see *before* any query surface (SQL/Cypher/SPARQL/GraphQL/unified)
  touches it — **no cross-agent leak on any query language**. The result cache keys on the caller's RLS
  context, so one agent's filtered result is never served to another.
- **Encryption-at-rest** (`security`): redb durable value blobs are ChaCha20-Poly1305 AEAD-sealed
  (pure-Rust RustCrypto, no ring/openssl); keys stay plaintext so range scans work.
- **Hash-chained tamper-evident audit log** over every durable mutation; `AuditVerify` walks it.

See **[docs/service_mode.md](docs/service_mode.md)** for the protocol, auth, and isolation policy.

---

## Quickstart

### Out-of-process (the standard path)

```python
from epistemic_graph import SyncEpistemicGraphClient

g = SyncEpistemicGraphClient()                    # connects/attaches to the UDS engine

g.nodes.add("AgentA", {"type": "coordinator"})
g.nodes.add("AgentB", {"type": "worker"})
g.edges.add("AgentA", "AgentB", {"weight": 1.5})
print("Order:", g.graph.topological_sort())

# Finance / data-science compute, all one round-trip each
metrics = g.finance.risk_metrics([0.01, -0.02, 0.03, -0.005, 0.02])
coeffs = g.datascience.linear_regression([[1.0, 2.0], [3.0, 4.0]], [3.0, 7.0])

# OWL/RDFS forward chaining — materialises inferred edges/types in-graph
result = g.reasoning.reason(subclass_relations=[("Dog", "Animal")],
                            transitive_properties=["ancestor"])
print("Inferred:", result["inferred_count"], "triples")
```

### Embedded in-process (Pi / edge, `embedded` feature)

The `EmbeddedEngine` handle drives the same GraphCore + redb durable rows with no server, socket, or
HMAC — open a persist dir and call core ops as plain methods (SQLite/DuckDB-style).

> **Batch, never per-element.** Every out-of-process call is a serialize -> socket -> deserialize round
> trip, not a function call. Ship work as one batch op over data already in the graph; keep tight
> per-element math in-process. See [AGENTS.md](AGENTS.md) and [docs/RUST_COMPUTE_GUIDE.md](docs/RUST_COMPUTE_GUIDE.md).

---

## Documentation

- [Technical Overview](docs/overview.md) — the crate DAG, the unified RowSet planner pipeline, the modalities.
- [Master-of-all engine](docs/architecture/engine.md) — the deep architecture: planner, distribution, security, streaming, federation, multimodal.
- [Tiers & binaries](docs/architecture/tiers.md) — the feature-composition map and the four prebuilt binaries.
- [Engine modes](docs/engine-modes.md) — remote -> shared-local -> autostart + the auto-bundle.
- [Deployment (database)](docs/deployment.md) — Docker / wheels / single-node / HA cluster recipes for every scale.
- [Service Mode](docs/service_mode.md) — protocol, auth, isolation, metrics.
- [Cost model & capacity](docs/cost-model.md) — per-tenant memory budget + autoscale signals.
- [Rust Compute Guide](docs/RUST_COMPUTE_GUIDE.md) · [Transport Benchmarks](docs/benchmarks.md) · [Concept Registry](docs/concepts.md).

## License

MIT — see [LICENSE](LICENSE).
