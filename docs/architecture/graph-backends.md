# Graph Backend Architecture

**The epistemic-graph engine is the ONE database — the authority and system of
record.** A single Rust engine does compute, in-memory cache, semantic/ontology
reasoning, AND durable persistence. All reads are served by the engine; all
writes commit to the engine and then fan out — losslessly and asynchronously
(durable outbox, replay-on-reconnect) — to optional **mirrors** for interop, BI,
external query, and disaster recovery. The mirror backends (PostgreSQL/pg-age,
Neo4j, FalkorDB, LadybugDB) each implement the same unified `GraphBackend`
abstract interface (Cypher query execution, vector search, node/edge CRUD, and
optional SPARQL support), so a mutation runs natively on every store.

The **default** is the engine alone — zero external services; there is no
engine-mode selector left to configure. Turn on mirroring by setting
`GRAPH_MIRROR_TARGETS` to a mirror set; the engine stays the read authority and
each durable mirror receives the replicated stream. PostgreSQL/pg-age, Neo4j, and
FalkorDB are first-class mirror targets (drivers install as optional extras under
`backends/contrib/`). There is **no tier vocabulary** — it is the engine authority
plus mirrors.

> **Verified parity (KG-2.7).** Node properties (declared / ad-hoc / nested),
> edge existence, **edge properties**, and vector search round-trip on **every**
> backend; the full cross-backend matrix and how to run it live are in
> [backend-parity-and-profile-testing](https://knuckles-team.github.io/agent-utilities/guides/backend-parity-and-profile-testing/).
> SPARQL is served **locally over any backend** via the OWL/RDF layer (see
> [owl_rdf_layer](https://knuckles-team.github.io/agent-utilities/architecture/owl_rdf_layer/)).

> **PostgreSQL runs Apache AGE (`GRAPH_PG_AGE=1` / `backend_type=age`).** This
> executes **real openCypher** via AGE's `cypher()` function — `count(r)`,
> `RETURN … AS alias`, multi-hop and variable-length traversal all work natively —
> retiring the bounded regex Cypher→SQL transpiler (still the default when AGE is
> off). pgvector continues to back embeddings. Image: `docker/pg-age.compose.yml`.

## Architecture Overview

<div class="admonition architecture" markdown>
<p class="admonition-title">Backend authority and mirrors</p>

`IntelligenceGraphEngine`'s methods — `query_cypher()`, `add_node()`,
`link_nodes()` (all via `engine.execute()`), `search_hybrid()` (via
`engine.semantic_search()`), `iter_nodes_by_types()`/`get_node_data()` (via
the Rust `GraphComputeEngine`'s compute + cache), and the `QueryRouter` —
all ultimately call into the epistemic-graph engine: the Rust-native
`EpistemicGraph` (compute + in-mem cache + semantic + durable persistence),
**the** authority and system of record, serving all reads and acking all
writes.

When `GRAPH_MIRROR_TARGETS` is set, the engine also writes through a
durable, async, lossless per-mirror outbox (replay-on-reconnect) to any of
five optional mirrors (interop/BI/external query/DR):
PostgreSQL/pg-age (openCypher + pgvector + ParadeDB BM25), Neo4j (Bolt),
FalkorDB (Redis), LadybugDB (SQLite + HNSW), and Stardog (SPARQL,
`urn:source:*`). Separately, the engine also serves a W3C SPARQL endpoint
(`core/owl_bridge`/`pyjena_fuseki`) via its OWL/RDF layer, locally, over
any data.

</div>

## Durability — the engine is redb-authoritative BY DEFAULT (CONCEPT:AU-KG.backend.backend-modes)

**The engine is a durable source of truth out of the box — not a rebuildable
cache.** As of "THE FLIP" (CONCEPT:AU-KG.backend.backend-modes), a stock `epistemic-graph-server`
built with the standard `--features full` includes the **`redb`** store, so its
persist backend (`EPISTEMIC_GRAPH_PERSIST_BACKEND`) defaults to `redb` and runs in
**authoritative mode** whenever a persist dir (`GRAPH_SERVICE_PERSIST_DIR`) is
configured. An acked write survives a `kill -9`. The three guarantees that make
"authoritative" safe (engine-side, CONCEPT:EG-KG.backend.authoritative-dispatch/EG-KG.storage.read-through-seam-exercised):

- **Commit-before-ack** — a durable mutation is fsync-committed to redb *before*
  its response is acked, so an acked write is always on disk.
- **Read-through-safe eviction** — the per-graph node cap stays enforced (memory
  bounded) but a node is dropped from RAM only after redb confirms it on disk;
  evicted nodes are served back from redb on a RAM miss. No data loss.
- **Backpressure, not drop** — the redb writer blocks for capacity rather than
  shedding a write.

> **One-time migration on first authoritative boot.** When the engine first boots
> authoritative and finds legacy snapshot (`.mp`) / WAL files but an empty redb
> store, it runs a **one-time, idempotent, crash-safe `.mp`→redb migration**
> (read-old → write-new) before it binds its socket; the old files are left in
> place as a backstop. On a large KG (thousands of graphs / a multi-hundred-MB
> commons) this can take **minutes** — see the engine's
> [binary-promotion runbook](https://github.com/knuckles-team/epistemic-graph/blob/main/docs/deploy/binary_promotion.md)
> for the deploy-time health-start-period handling.

To opt back into the pre-flip **rebuildable-cache** model — the engine as a fast
in-memory cache over an external durable system-of-record (a `fanout` mirror) —
set `EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot` on the engine; the local `.mp`
snapshot + WAL then exist only for fast warm restart. (A build without the `redb`
feature is non-authoritative and boots clean with no warning.) This is an
**engine-side** setting (`epistemic-graph-server` env), independent of the
agent-utilities-side `GRAPH_MIRROR_TARGETS` mirror selection below.

## Derived stores route to the engine, NOT a local DB (engine-only, CONCEPT:AU-KG.backend.cache-lives-as–2.248)

Auxiliary stores that used to keep their own local SQLite/JSON file *next to* the
one engine authority now route through the **engine unconditionally** — there is
**no SQLite/JSON/file fallback**. Each resolves the engine-authority backend (the
OS-5.63 resolver auto-starts the pi-tier engine in prod; the AU-KG.memory.provides-real-ephemeral-one test fixture
provides a real ephemeral one) and raises a clear error if the engine is genuinely
unreachable. They share the engine-only helpers in
`knowledge_graph/backends/base.py` — `is_engine_authority_backend` /
`require_engine_authority_backend` — and persist via the engine node API
(`add_node` / `get_node_properties` / `nodes_by_label`, deterministic node ids), so
the reads/writes work against the real engine (the in-process `execute()` Cypher
subset can't do `WHERE … IN $list` / unscoped `MATCH`).

| Store | Engine surface (the only store) | Node id / scan | Concept |
|---|---|---|---|
| LLM card cache (`CardStore`) | `:CardCache` nodes | `cardcache:<ast_hash>` (keyed) | AU-KG.backend.cache-lives-as |
| Registry graph (`IntelligencePipeline`) | engine graph nodes/edges via the active backend | `persist_to_ladybug=False` | AU-KG.compute.graph-builder |
| Time-series memory | engine `client.timeseries.*` (eg-tsdb, `series.redb`) | series ids | AU-KG.memory.time-series-lives-one |
| Write-back proposals (`ProposalQueue`) | `:WritebackProposal` nodes | `wbp:<target>:<seq>` + label scan | AU-KG.enrichment.proposals-live-as |
| Code-health baselines | `:CodeHealthBaseline` nodes | `codehealthbaseline:<repo>` (keyed) | AU-KG.maintenance.only-no-file-cache |

> The **ingestion delta manifest** (`DeltaManifest`, `:IngestManifest` /
> `kg_ingest_manifest.db`, CONCEPT:AU-KG.ingest.enterprise-source-extractor) is a SEPARATE, pre-existing store that
> *keeps* its zero-infra SQLite fallback via `is_durable_backend` — it is NOT part
> of this engine-only consolidation.

These enforce the platform's "one authority" discipline: auxiliary state is
queryable beside the graph, never scattered across host-local files, and never a
SQLite straggler. (Intentional non-store local files stay local by design: the
`STATE_DB_URI` state-store default, the mirror outbox, and RAM-only caches.)

## Engine authority vs. mirror comparison

The first column is **the engine** — the one authority that serves reads and acks
writes. The rest are **mirror targets**: durable, full-openCypher (or SPARQL)
stores that receive the replicated write stream for interop/BI/external-query/DR.

| Capability | epistemic-graph (THE authority) | LadybugDB (mirror) | PostgreSQL/pg-age (mirror) | Neo4j (mirror) | FalkorDB (mirror) |
|---|:---:|:---:|:---:|:---:|:---:|
| **Role** | **authority · system of record** | mirror (extra) | mirror (extra) | mirror (extra) | mirror (extra) |
| Cypher Support | subset (id-anchored)¹ | Native (Kuzu) | **Native (AGE)** / transpiled | Native | Native |
| Node props (declared/ad-hoc/nested) | ✅ | ✅ (ad-hoc in `metadata`) | ✅ | ✅ | ✅ |
| **Edge properties** | ✅ | ✅ (JSON `r.properties`) | ✅ | ✅ | ✅ |
| Vector Search | ✅ | ✅ | ✅ pgvector | ✅ (`:Embeddable`) | ⚠️ AVX2 host² |
| SPARQL (via OWL/RDF layer) | ✅ local | ✅ local | ✅ local | ✅ local | ✅ local |
| Graph Traversal (multi-hop) | ✅ native compute¹ | ✅ | ✅ (AGE) | ✅ | ✅ |
| Connection Pooling | UDS client | File Lock | ✅ psycopg_pool | ✅ | — |
| Persistence | **durable (built-in, redb-authoritative)** | File | Server | Server | Redis |
| Zero Config | ✅ | ✅ | — | — | — |

¹ The engine is the authority and serves multi-hop traversal natively from its
own compute/cache over its durable store; `engine.execute` interprets an
operational id-anchored Cypher subset on the write path. ² FalkorDB vector search
is code-correct (Cypher `CREATE VECTOR INDEX` + `db.idx.vector.queryNodes`) but
the `falkordb` image SIGILLs on 768-dim vector ops on non-AVX2 host CPUs.

## PostgreSQL Mirror Deep Dive

PostgreSQL is the richest **mirror** target: it combines three PostgreSQL
extensions into a unified graph + vector + search store, so the engine's
replicated write stream lands in a queryable, BI-friendly, openCypher-capable
durable copy. (It is a mirror, not the authority — the engine remains the read
source of truth.)

### Internal extension architecture

<div class="admonition architecture" markdown>
<p class="admonition-title">Postgres/AGE mirror layout</p>

Relational storage (Node Tables — Agent, Tool, Memory, … — plus the
`kg_edges` table) feeds the AGE graph index (traversal, shortest-path,
search). The Node Tables also feed two search paths: pgvector HNSW
(cosine similarity) and ParadeDB BM25 (lexical ranking).

</div>

### Cypher Transpilation

The engine speaks Cypher; PostgreSQL speaks SQL. The `transpile()` function in
`backends/cypher_transpiler.py` handles the translation for all patterns the
engine generates:

| Engine Cypher | PostgreSQL SQL |
|---|---|
| `MATCH (n:Agent) WHERE n.id = $id RETURN n` | `SELECT * FROM "Agent" WHERE id = $1` |
| `CREATE (n:Tool {id: $id, name: $name})` | `INSERT INTO "Tool" (id, name) VALUES ($1, $2)` |
| `MATCH (s)-[r:PROVIDES]->(t) MERGE ...` | `INSERT INTO kg_edges ... ON CONFLICT DO UPDATE` |
| `MATCH (n) WHERE toLower(n.name) CONTAINS $q` | `SELECT * FROM ... WHERE LOWER(name) LIKE '%$1%'` |
| Path traversal `(n)-[*1..3]-(t)` | `graph.traverse(seed, max_depth:=3)` |

### Extension Dependencies

**Postgres is the richest mirror target here** (`backend: "age"`), and
to be a first-class graph + vector + search mirror it needs **three** extensions
installed — and, for AGE and pg_search, preloaded:

| Extension | Required | `shared_preload_libraries` | Purpose |
|---|:---:|:---:|---|
| **Apache AGE** (`age`) | **Yes** (for `backend: "age"`) | **yes** | **Native openCypher** — runs the query the engine emits unchanged, so a Postgres connection has `cypher_support = "full"` (parity with Neo4j/FalkorDB). AGE **must** be in `shared_preload_libraries` or it won't load. |
| **pgvector** (`vector`) | **Yes** | no | Embedding storage + HNSW cosine `semantic_search` |
| **ParadeDB** (`pg_search`) | **Yes** | yes | BM25 full-text scoring |
| pg_trgm | Optional | no | Trigram fuzzy text matching |
| pg-age | Optional (legacy) | no | CSR traversal for the regex-transpiler `backend: "postgresql"` path |

> **Why AGE, not just the transpiler.** Without AGE, a Postgres connection falls
> back to the bounded regex transpiler (`backend: "postgresql"`, `cypher_support
> = "subset"`) — fine as a single store, but it cannot serve the full query
> surface a fan-out mirror set shares (CONCEPT:AU-KG.backend.mirror-health-repair). With AGE, Postgres is a
> peer of Neo4j/FalkorDB and the **richest mirror target** (full openCypher).

**The curated image bundles all three.** `services/pg-age/` builds
`your-registry.example/pg-age` **FROM `paradedb/paradedb:latest` (PostgreSQL 18)** — which
already ships `vector` + `pg_search` — and adds **Apache AGE 1.7.0** (the release
that introduced PG18 support). The stack `command` sets
`shared_preload_libraries=pg_search,pg_cron,pg_stat_statements,age` and
`init-extensions.sql` runs `CREATE EXTENSION` for `vector`, `age`, and
`pg_search`. The **stock `paradedb/paradedb` image does NOT include AGE** — using
it directly leaves a Postgres connection on the `subset` transpiler path.

The mirror **gracefully degrades** when extensions are missing — CRUD and basic
search work with plain PostgreSQL; native Cypher requires AGE; vector search
requires pgvector; BM25 requires pg_search.

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GRAPH_MIRROR_TARGETS` | unset | JSON/list of mirror connection names (declared in `KG_CONNECTIONS`) that receive the fanned-out write stream. There is no engine-mode selector or read-authority selector to configure alongside it: the epistemic-graph engine is unconditionally the read authority, and setting this list is the only thing that turns mirroring on |
| `GRAPH_DB_PATH` | `knowledge_graph.db` | File path for EpistemicGraph (`file` mode) / LadybugDB |
| `GRAPH_DB_URI` | — | Connection URI for Neo4j or PostgreSQL |
| `GRAPH_DB_HOST` | `localhost` | Host for FalkorDB |
| `GRAPH_DB_PORT` | `6379`/`7687` | Port for FalkorDB/Neo4j |
| `GRAPH_DB_USER` | `neo4j` | Username for Neo4j/PostgreSQL |
| `GRAPH_DB_PASSWORD` | `password` | Password for Neo4j/PostgreSQL |
| `GRAPH_DB_NAME` | `agent_graph` | Database/graph name |
| `GRAPH_POOL_MIN` | `2` | PostgreSQL pool minimum connections |
| `GRAPH_POOL_MAX` | `10` | PostgreSQL pool maximum connections |
| `GRAPH_PGGRAPH_SCHEMA` | `public` | Schema for pg-age table registration |
| `GRAPH_FUSEKI_URL` | `http://localhost:3030` | Jena/Apache Fuseki server URL |
| `GRAPH_FUSEKI_DATASET` | `agent_kg` | Fuseki dataset name |
| `GRAPH_FUSEKI_USER` / `GRAPH_FUSEKI_PASSWORD` | — | Optional Fuseki credentials |
| `KG_CONNECTIONS` | — | JSON list of named connections for the multi-connection registry (CONCEPT:AU-KG.backend.multi-connection-registry). See below. |

These are the **agent-utilities-side** (mirror-selection) variables. Engine
durability is configured on the **`epistemic-graph-server`** process itself
(CONCEPT:AU-KG.backend.backend-modes) and is independent of `GRAPH_MIRROR_TARGETS`:

| Variable (engine-side) | Default | Description |
|---|---|---|
| `GRAPH_SERVICE_PERSIST_DIR` | unset | Engine persist dir. Set ⇒ the engine is a durable source of truth; absent ⇒ in-memory only |
| `EPISTEMIC_GRAPH_PERSIST_BACKEND` | `redb` | `redb` = durable authoritative store (default — THE FLIP); `snapshot` = opt-in rebuildable-cache (`.mp` + WAL) |
| `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` | (auto) | Defaults ON when the redb backend is active; rarely set by hand |

## Multiple Connections at Once (CONCEPT:AU-KG.backend.multi-connection-registry)

The engine has always been vendor-agnostic, but historically only **one** backend
was live per process. The **named multi-connection registry** lets a deployment
keep several live connections side by side and run the *same* graph tools against
any one — or fan out to all — with the backend choice fully abstracted behind a
`target` parameter. No code is forked into a separate server: every existing
`graph_*` MCP tool and its REST twin gains this for free.

### Register connections

Declaratively, via `KG_CONNECTIONS` (each entry is `create_backend` kwargs plus a
`name`):

```bash
export KG_CONNECTIONS='[
  {"name": "prod-neo4j", "backend": "neo4j", "uri": "bolt://neo4j:7687", "user": "neo4j", "password": "..."},
  {"name": "team-falkor", "backend": "falkordb", "host": "falkor", "port": 6379},
  {"name": "pg-main", "backend": "age", "uri": "postgresql://agent:agent@pg:5432/agent_kg"}
]'
```

…or at runtime via `graph_configure`:

```
graph_configure(action="add_connection", config_key="pg-main",
                config_value='{"backend":"age","uri":"postgresql://agent:agent@pg:5432/agent_kg"}')
graph_configure(action="list_connections")          # per-connection health
graph_configure(action="remove_connection", config_key="pg-main")
```

### Target a connection

Every `graph_query` / `graph_search` / `graph_write` (and the heavier
`graph_ingest`/`graph_analyze`/`graph_orchestrate`) accepts an optional `target`:

| `target` | Behaviour |
|---|---|
| omitted / `""` / `"default"` | the primary engine — **identical to legacy behaviour** (backward compatible) |
| `"pg-main"` | a single named connection; result shape unchanged |
| `"all"` or `"a,b"` or `["a","b"]` | **fan-out** — per-connection labeled results (`{"targets": {...}, "errors": {...}}`), partial success: one backend failing never aborts the others |

**Writes** only fan out on an *explicit* multi-target value (`"all"`/list) — the
default and a single named target stay single-write, so you never accidentally
triple-write.

### Portability: one query, every backend

For the same Cypher to run unchanged everywhere, the backend must speak native
openCypher. Each backend advertises a `cypher_support` tier:

| Backend | `cypher_support` | Notes |
|---|---|---|
| neo4j, falkordb | `full` | native Cypher |
| **Postgres via Apache AGE** (`backend: "age"`) | `full` | native openCypher (`count(r)`, aliases, multi-hop, `-[*1..2]->`, edge props) + pgvector |
| Postgres regex transpiler (`backend: "postgresql"`) | `subset` | only the bounded operational subset the engine emits; fallback when the AGE extension is absent |
| epistemic_graph (in-memory) | `subset` | AU-P0-2: label/property `MATCH` + a real `WHERE` predicate + aggregates/`DISTINCT` route to the engine's OWN native Cypher executor (`GraphComputeEngine.query_cypher`, its `eg-query` parser — still a bounded grammar, e.g. no comma-separated disjoint `MATCH` patterns or arbitrary function calls) instead of a client-side regex scan-and-eval; a rejected/unsupported shape raises (`CypherEngineError`/`NotImplementedError`) rather than silently returning `[]`. Two AU-specific shapes (the virtual `id` node-identity accessor; relationship-type traversal/merge, keyed by `rel_type` not the engine's `relationship`/`type`) stay on typed engine methods because native routing would give silently-wrong results. |

**Register Postgres connections as `age`** (not `postgresql`) when you want one
query to run unchanged across neo4j + falkordb + postgres. `list_connections`
reports each connection's `cypher_support` so fan-out callers can tell which
backends can serve a full query.

## Connection roles + live config (CONCEPT:AU-KG.backend.connection-registry)

Every registered connection carries a **role**, so you can safely attach an existing
third-party graph as a data source — not just for mirroring:

| role | meaning | `target=` writes |
|---|---|---|
| `read` (default) | external **data source** — query/profile/imprint only | rejected |
| `read_write` | full query + write target | allowed |
| `mirror` | receives fan-out replication of *our* KG | rejected (written only via the outbox) |

```jsonc
// KG_CONNECTIONS entry — role + a secret reference (kept out of config.json)
{"name":"prod-neo4j","backend":"neo4j","uri":"bolt://neo4j.example.internal:7687",
 "user":"neo4j","password":"vault://agents/kg/neo4j#password","role":"read"}
```

- **Credentials** may be literals *or* `vault://…` / `env://VAR` refs, resolved at
  connect; `list_connections` redacts them either way.
- **The mirror set is derived from `role=mirror`** connections (`GRAPH_MIRROR_TARGETS`
  stays an optional override) — one registry drives both query targets and mirrors.
- **Durable + live:** `graph_configure add_connection/remove_connection` persists the
  list to `config.json` (survives restart). `profile_connection`
  introspects a foreign graph's schema and writes a self-describing
  `ExternalGraphReference` catalog node, mapping its labels onto our ontology.
- **Generic live config:** legacy `graph_configure get_config|list_config` remains
  read-only; updates use `graph_config action=set`, the sole governed mutation path.
  It validates against `AgentConfig`, passes the ActionPolicy approval gate, persists
  to config.json, records provenance, and reconciles the served catalog; engine-rebuild
  settings come back with `restart_required: true`. The doctor's `graph_connections`
  check reports each connection's role + flags stalled mirrors. Both surfaces have MCP
  and API gateway parity: legacy reads use `POST /graph/configure`; governed generic
  config administration uses `POST /graph/config`.

## Mirror every write to N stores at once (CONCEPT:AU-KG.backend.mirror-health-repair)

Where KG-2.63 lets you *target* several connections per call, **fan-out** makes
mirroring available for every write: the epistemic-graph engine is
unconditionally the **authority** store that serves reads and acks writes, and
each mutation is replicated — losslessly and asynchronously — to any set of
durable backends named in `GRAPH_MIRROR_TARGETS`; the zero-infra default is
unchanged (it is only built when you configure a mirror set). There is no
separate fan-out mode switch and no authority selector — naming a mirror set is
the only thing that turns fan-out on, and the read authority is always the
engine.

```bash
# Authority = epistemic-graph engine (fast in-mem reads + durable); mirror to Postgres-AGE,
# Neo4j and FalkorDB. The mirror set names entries declared in KG_CONNECTIONS.
export GRAPH_MIRROR_TARGETS='["pg-age","prod-neo4j","team-falkor"]'
export KG_CONNECTIONS='[
  {"name":"pg-age","backend":"age","uri":"postgresql://u:p@pg.example.internal:5432/agent_kg"},
  {"name":"prod-neo4j","backend":"neo4j","uri":"bolt://neo4j.example.internal:7687","user":"neo4j","password":"…"},
  {"name":"team-falkor","backend":"falkordb","host":"falkordb.example.internal","port":6379}
]'
```

### Where a mirror writes — the mirror target graph (CONCEPT:AU-KG.backend.mirror-target-graph)

A mirror never has to take over the store it lands in. Each backend has a native
**isolation unit**, and one `mirror_target` declaration on the connection picks
which one mirroring uses — resolved in a single place
(`backends/mirror_target.py`, applied by `create_backend`), never four times:

| Backend | Native isolation unit | Dedicated target becomes |
|---|---|---|
| Neo4j | a **database** (`session(database=…)`) | that database (`CREATE DATABASE … IF NOT EXISTS`, Enterprise) |
| Stardog | a **database** *and* a SPARQL **named graph** | `urn:mirror:<name>` — a named graph inside the configured database (default level), or a whole database with `"level":"database"` |
| PostgreSQL/AGE | an **AGE graph** | that graph (`ag_catalog.create_graph`) |
| FalkorDB | a **graph key** | that key (`select_graph`; created on first write) |

```jsonc
// kg_connections — mirror into a graph dedicated to mirroring
{"name":"prod-neo4j","backend":"neo4j","role":"mirror","mirror_target":"dedicated"}
// ...under a name you choose
{"name":"pg-age","backend":"age","role":"mirror",
 "mirror_target":{"mode":"dedicated","name":"kg_mirror"}}
// Stardog: dedicate the whole database instead of a named graph
{"name":"stardog","backend":"stardog","role":"mirror",
 "mirror_target":{"mode":"dedicated","name":"kg_mirror","level":"database"}}
```

Three modes, all explicit:

| `mirror_target` | Where writes land | Guarded? |
|---|---|---|
| *(omitted, connection names `database`/`db_name`/`graph_name`)* | exactly that unit — **unchanged from before** | no |
| `"dedicated"` / `{"mode":"dedicated","name":…}` | a graph reserved for mirroring, created if absent | no |
| `"default"` | the instance default | **yes** |
| `"default-overwrite"` | the instance default, guard waived | no |

On a single-level store (AGE, FalkorDB) `"dedicated"` **supersedes** the
connection's own graph name — those connection profiles require one, so
dedicating could otherwise never be expressed. It is logged, never silent.

### The non-empty-default guard (CONCEPT:AU-KG.backend.mirror-nonempty-default-guard)

Before a mirror is attached to the fan-out, `preflight_mirror_target` creates a
dedicated target if absent and then, when the resolved target **is** the instance
default, proves it empty. If it already holds data the mirror is **refused**:
nothing is written, the refusal is logged at ERROR with both fixes, and
`graph_configure(action="mirror_status")` reports it with `refused: true`. The
authority and every other mirror are unaffected. The probe **fails closed** — a
target whose emptiness cannot be proven is refused too.

> **The one behaviour change.** A deployment that named its database/graph
> explicitly is untouched. A deployment that relied on an *implicit* default —
> a Neo4j mirror with no `database`, or a Stardog mirror with no `STARDOG_DATABASE`
> (including `continuous_stardog_mirror`) — now **refuses to start that mirror**
> if the instance already holds data. That is a refusal, never a silent redirect
> and never a silent overwrite. Fix it by declaring `"mirror_target":"dedicated"`
> (recommended) or `"mirror_target":"default-overwrite"` to keep the old behaviour.

### How it stays lossless

* **Authority commits synchronously; mirrors apply async.** A write returns once
  the authority commits **and** the mutation is durably appended to each mirror's
  outbox. Reads (served from the authority) are always consistent; mirrors are
  eventually-consistent (seconds of lag).
* **Durable per-mirror outbox.** `backends/outbox.py` is a sqlite/WAL append log
  (zero external infra). A per-mirror drainer thread applies entries in order and
  advances a persisted cursor, so a mirror that is **offline or slow keeps its
  unapplied tail and replays from its cursor on reconnect / restart** — a
  transient outage never drops a write. (Unlike an in-memory write-behind queue,
  the outbox survives a crash.)
* **One drainer per mirror = natural single-writer.** A file-locked store
  (LadybugDB/Kuzu) is serialised for free — it is simply the slowest mirror.
* **Reconcile backstop.** `graph_configure(action="reconcile")` runs a full
  authority→mirror re-sync and reports exact remaining drift — the repair for the
  only un-mirrorable window (a crash between the authority commit and the outbox
  append) and for a mirror whose outbox tail was lost.

### Operate it

| Surface | Call | What it does |
|---|---|---|
| MCP | `graph_configure(action="mirror_status")` | Per-mirror replication health: `lag`, `writes`, `failures`, `stalled`, `last_error`, plus `outbox_depth`. Alarm on `lag`/`stalled`. |
| MCP | `graph_configure(action="reconcile", config_key="<mirror>")` | Full drift-repair for one mirror (empty `config_key` = all). |
| REST | `POST /graph/configure {"action":"mirror_status"}` | Same core, REST twin. |

**Portability:** use full-openCypher mirrors (Postgres-AGE / Neo4j / FalkorDB) so
the same mutation runs unchanged on every store — see the `cypher_support` table
above. **LadybugDB** can be a 4th local-write mirror — config only: add a
`kg_connections` entry `{"name":"local-ladybug","backend":"ladybug","db_path":"<data_dir>/mirror_ladybug.db"}`
and list it in `GRAPH_MIRROR_TARGETS`. Its single-writer file lock is serialised
by its one drainer thread; ad-hoc props fold into the `metadata` JSON column and
edge props into the `properties` JSON column (durably stored, conformance-verified).

### Native cross-backend migration (CONCEPT:AU-KG.backend.mirror-health-repair)

`knowledge_graph/migration.py` `copy_graph(source, target)` copies every node +
edge (+ embeddings) from any source (the engine authority, or a full-cypher mirror
read via `MATCH`) into any target backend. The **write** goes through the
engine's proven, dialect-aware MERGE upserts (`IntelligenceGraphEngine._upsert_node`
/ `_upsert_edge`) — so each backend gets a *correct native write*, never one
backend's raw cypher forwarded to all. It is the right layer for interchangeable
storage:

* **MERGE-on-`{id}`** → idempotent + resumable (re-running converges, never dupes);
* per-backend dialect handled once: nested values JSON-encoded for map-rejecting
  drivers (Neo4j/FalkorDB), ad-hoc keys → `metadata` JSON and edge props →
  `properties` JSON for strict-schema Kuzu/Ladybug, transpiler-safe for plain
  Postgres;
* returns exact post-condition drift (`nodes_missing` / `edges_missing`).

This is what **backfills a freshly-added mirror** (`graph_configure(action="reconcile")`
delegates to `copy_graph`) and what migrates
data between any two backends (e.g. Neo4j → FalkorDB). It replaced the old reconcile
that reconstructed `CREATE (n:Label {`k`: $k})` cypher — fragile on native-cypher
backends (double-escaped reserved keys) and edge-lossy. Parity is proven by
`tests/integration/backends/test_cross_backend_parity.py` (`pytest -m live`): the same
source graph copied into Postgres + Neo4j + FalkorDB + LadybugDB lands as identical
counts.

### Quick Start: PostgreSQL mirror

```bash
# 1. Start the database
docker compose -f docker/pg-age.compose.yml up -d

# 2. Mirror the engine authority to Postgres-AGE
export GRAPH_MIRROR_TARGETS='["pg-age"]'
export KG_CONNECTIONS='[{"name":"pg-age","backend":"age","uri":"postgresql://agent:agent@localhost:5433/agent_kg"}]'

# 3. Run the graph-os MCP server — engine serves reads, Postgres mirrors writes
graph-os
```

## Engine stability backstops — surviving the `__commons__` firehose

The engine is one process behind a MessagePack socket holding a multi-tenant
graph (`__commons__` carries 166K+ nodes, each with a 1024-dim embedding). Three
failure modes were hardened after a stress run wedged the engine; each backstop
fixes a specific one.

<div class="admonition architecture" markdown>
<p class="admonition-title">Client/engine backstop boundary</p>

On the client (`agent-utilities`): `iter_nodes_by_types()`/`get_node_data()`
(`core/bounded_read.py`) deliberately avoids an unbounded `nodes(data=True)`
scan (which would produce a 166K × 1024-dim, 1GB frame and a connection
reset) by doing a per-label fetch — O(#type), not O(graph) — through the
engine's `GetNodesByLabel` (bounded, not capped). Separately,
`BreakerClientProxy._guard()` (`core/engine_breaker.py`) treats
ConnectionReset/BrokenPipe as transient (retries ×2, 0.25s backoff, rides
`client._reconnect`, breaker not tripped) and an RPC timeout as an elapsed
budget (fails once, counts a breaker failure, never replays onto a
contended engine).

On the engine (Rust): the `GetNodes` handler's `oversize_dump_error`
(`handlers/graph_ops.rs`) checks the node count against
`max_response_nodes()` (`EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`, default
50000, 0 = off); exceeding the cap returns `RESULT_TOO_LARGE` →
`ResultTooLargeError`. Separately, `write_coalescer.apply_batch()` folds N
writes into one `core.txn()` under the per-graph topology write lock.

</div>

### The three backstops

- **Bounded reads (AU-KG.ingest.never-scan-whole-graph, `core/bounded_read.py`).** A whole-graph
  `graph.nodes(data=True)` materializes every node into one MessagePack frame — a
  gigabyte payload that overloads and resets the connection. `iter_nodes_by_types`
  iterates by **type** through the engine-side bounded label fetch
  (`get_nodes_by_label`, KG-2.51), and **trusts** an empty bounded result rather
  than falling back to a full scan; `get_node_data` fetches one node by id. A
  type-filtered reader becomes O(#type), not O(graph).
- **Adaptive transient retry (AU-KG.compute.single-dropped-connection, `core/engine_breaker.py`).** A single
  dropped connection mid-op is transient — the client transparently reconnects on
  its next call. `_guard` now retries the op a bounded `_MAX_TRANSIENT_RETRIES`
  (=2) with exponential backoff and records a `"retry"` outcome **without**
  counting it against the circuit breaker, so a brief blip self-heals instead of
  cascading N callers into a tripped breaker. An elapsed RPC `TimeoutError` is
  different: the engine may still be finishing that work, so it is counted once
  and returned immediately rather than replayed and multiplied across the retry
  budget.
- **Engine response guard (EG-KG.ingest.resets-socket-so-assimilation, epistemic-graph).** The engine itself caps
  the unbounded full-graph dump: `oversize_dump_error` checks the cheap
  `node_count()` against `max_response_nodes()` (env
  `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`, default **50000**, `0` disables) **before**
  materializing, and returns `RESULT_TOO_LARGE:` (surfaced client-side as
  `ResultTooLargeError`). Only the `GetNodes` dump is capped — `GetNodesByLabel`
  and per-id reads stay bounded and uncapped.

### The H1 finding — write-lock starvation (EG-011)

A profiling/stress run confirmed **H1**: under sustained ingestion every write
funnels through the **`__commons__` per-graph topology write lock**, so it
serializes and starves reads. Measured: a `semantic_search` that is **~0.02s
idle** climbs to **~14s** under the ingestion firehose (and a single `nodes.add`
to `__commons__` took ~13s), with the engine pinning roughly one core. Two
histograms (CONCEPT:EG-KG.compute.parse-resolve-span, `epistemic_graph/src/metrics.rs`) make the gap
observable:

| metric (labelled `{graph}`) | measures |
|---|---|
| `epistemic_graph_write_lock_wait_seconds` | enqueue to lock acquired (the **starvation** cost) |
| `epistemic_graph_write_lock_hold_seconds` | how long each coalesced batch holds the lock (the **contention** cost) |

<div class="admonition architecture" markdown>
<p class="admonition-title">Write-lock contention and its fixes</p>

Both the ingestion firehose (writes to `__commons__`) and `semantic_search`
reads (0.02s idle to ~14s under load) contend on the same per-graph write
lock, recorded as EG-011 wait-time (starvation) and hold-time (contention)
histograms. Two fixes: the write-coalescer folds N writes into one
transaction per batch (~57× fewer lock acquisitions), and splitting the
control plane onto its own `__control__` engine graph gives it a distinct
`GraphCore` — and therefore a distinct lock — so ingestion on `__commons__`
can never starve it.

</div>

The fixes attack both axes: the **write-coalescer (EG-KG.sharding.per-graph-write-coalescer)** turns many
concurrent single-op writes into one lock acquisition per batch (~57× fewer
acquisitions in the engine benchmark), and routing the scheduler/control plane
onto a separate **`__control__`** engine graph gives it its own lock so ingestion
on `__commons__` can never starve it (distinct graphs already isolate — each
`GraphCore` has its own lock).

## Implementing a New Backend

1. Inherit from `GraphBackend` in `backends/base.py`
2. Implement all abstract methods: `execute()`, `execute_batch()`, `create_schema()`,
   `add_embedding()`, `semantic_search()`, `prune()`, `close()`
3. Optionally override `supports_sparql` and `execute_sparql()` for SPARQL support
4. Register in the `create_backend()` factory in `backends/__init__.py`
5. Add optional dependency group to `pyproject.toml`
6. Add integration tests

## Compute/read surfaces that are NOT `GraphBackend` mirrors (CA-27)

`TrinoQueryBackend` (`backends/trino_backend.py`) and `spark_jobs.py`
(`backends/spark_jobs.py`) read the Iceberg lakehouse (`services/trino`,
`services/spark`) as the calling principal. They deliberately do **not**
implement `GraphBackend` — Trino/Spark return tabular result pages, not
Cypher-shaped rows, and per the Company Architecture program's invariant I3
("Provenance on every hop") a compute result is not a KG fact until it
carries a lineage fence (run id, input snapshot id(s), code version,
confidence). `TrinoQueryBackend` implements the narrower `QueryBackend` port
owned by `core/tabular_query_service.py` (`query()` -> paged
`KnowledgeBatch`), while retaining its validated `as_of()` adapter API.
`ChangeEnvelopeBuilder` (in `trino_backend.py`, reused by `spark_jobs.py`)
is the one place that fence is assembled before a result may reach the
existing `ApplyChangeEnvelope` door (`knowledge_graph/ingestion/
change_envelope.py`). eg redb remains the only authoritative store (I1);
Trino/Spark are read/compute surfaces over the Iceberg projection, never a
competing writer — both modules refuse any write/DDL statement (grep
`trino_backend.py`/`spark_jobs.py` for `iceberg`: read/catalog-list
mentions only, zero write/commit calls). See
`plans/company-architecture/lanes/CA-27-trino-spark-adapters.md` for the
full design note.

The public tabular path is deliberately recomposed around one lower application
service. `tabular_query(sql=...)` is the purpose-specific transport operation;
`TabularQueryService` owns selector validation, page materialization,
and provenance consistency. The Trino adapter receives required token and
principal providers at the MCP composition root and imports neither MCP nor
runtime configuration. There is no anonymous, static-credential, or cleartext
fallback: the configured endpoint must be TLS and the delegated bearer is
resolved on every dispatch. A token rotation replaces only that principal's
pooled engine; the LRU cap bounds total warm principals.

<div class="admonition architecture" markdown>
<p class="admonition-title">Tabular query path</p>

Both `MCP tabular_query` and `REST /query/tabular` go through a thin SQL
route into `TabularQueryService` (request + provenance contract), which
calls the `QueryBackend` port. `TrinoQueryBackend` (TLS-only, read-only,
bounded pools) implements that port — configured with typed config +
delegated identity from graph-os's composition — and reads the
Trino/Iceberg projection.

</div>

## Related

- [KG as Bidirectional ETL Hub](https://knuckles-team.github.io/agent-connector-sdk/architecture/etl-hub/) — how these backends serve as ETL load
  targets (Stardog SPARQL data backend, fan-out mirrors) under the unified `graph_etl`
  interface, plus the one ingestion contract and data lineage.
- [OWL/RDF Layer](https://knuckles-team.github.io/agent-utilities/architecture/owl_rdf_layer/) — local SPARQL + reasoning over any backend.
