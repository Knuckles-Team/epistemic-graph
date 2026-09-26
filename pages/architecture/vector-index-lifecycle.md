# HNSW Vector Index Lifecycle

This document describes how the Knowledge Graph manages HNSW (Hierarchical
Navigable Small World) vector indexes for sub-second semantic search across
121K+ nodes.

## Overview

The KG stores embedding vectors (768-dim, `FLOAT[768]`) on 15 node tables.
The exact set is derived dynamically from `schema_definition.py` at runtime (every
node whose `embedding` column is a `FLOAT` array), so it stays in sync as the schema
evolves rather than being hardcoded.
Vector search is served by the **engine's** native ANN (IVF-PQ/HNSW) — **O(log N)**
— either inside a unified cross-modal plan or via the `semantic_search` primitive
(see *Retrieval is ONE engine plan* below). The retriever keeps **no** Python
O(N) cosine path. (The Ladybug/Kuzu HNSW lifecycle below applies to the demoted
`contrib` Kuzu mirror, not the engine authority.)

### The Kuzu Constraint

LadybugDB (Kuzu) **does not support `SET` on properties that are part of a
vector index**. This means:

- ❌ Cannot write new embeddings while HNSW indexes exist on that table
- ✅ Can read/search using HNSW indexes at any time
- ✅ `CREATE_VECTOR_INDEX` skips tables that already have indexes (`IF NOT EXISTS`)

This constraint drives the entire lifecycle design.

## Lifecycle: Drop → Ingest → Build

Drop the indexes for the affected tables only, which lets the ingest step
(`SET emb`) succeed (no index is blocking the write), then build the
indexes for those same tables.

### Phase 1: Targeted Drop (Pre-Ingestion)

When `submit_task()` is called, it drops HNSW indexes **only for the tables
affected by this task type**:

| Task Type      | Tables Dropped | Tables Left Intact |
|----------------|----------------|--------------------|
| `codebase`     | `Code`         | Article, Concept, Agent, etc. |
| `document`     | `Article`      | Code, Concept, Agent, etc. |
| `conversation` | `Message`      | Code, Article, Concept, etc. |

This means if you're ingesting a PDF, the `Code` table's HNSW index stays
active and searches against code remain O(log N).

The drop is **idempotent** — dropping a non-existent index is silently skipped.
Each table is only dropped once per ingestion batch (tracked via `_dropped_tables` set).

### Phase 2: Ingestion

Standard ingestion proceeds — embeddings are written via `SET n.embedding = $emb`.
This succeeds because the affected table's index was dropped in Phase 1.

### Phase 3: Auto-Build (Post-Ingestion)

After the last worker completes and the task queue is empty:

1. `_maybe_build_vector_indexes()` fires automatically
2. Only rebuilds indexes for tables that were dropped (`_dropped_tables`)
3. Runs in a **background thread** (`KG-IndexBuilder`) — non-blocking
4. Resets all flags so the next ingestion batch re-triggers the cycle

## Embedding Tables (15 total)

These are the node tables with embedding columns, derived from `schema_definition.py`:

| Table | Primary Content | Typical Node Count |
|-------|----------------|-------------------|
| `Code` | Source code (Code/Test/Feature nodes) | ~78K |
| `Symbol` | Code symbols | varies |
| `Article` | Research papers, documents | ~2.6K |
| `Message` | Conversation messages | ~4.6K |
| `Concept` | Extracted concepts | ~79 |
| `Agent` | Registered agents | varies |
| `AgentTemplate` | Agent templates | varies |
| `Tool` | MCP tools | varies |
| `Skill` | Universal skills | varies |
| `DiffEntry` | Git diff entries | varies |
| `CallableResource` | Callable resources | varies |
| `SystemPrompt` | System prompts | varies |
| `KBConcept` | Knowledge base concepts | varies |
| `KBFact` | Knowledge base facts | varies |
| `KnowledgeBaseTopic` | KB topics | varies |

## Retrieval is ONE engine plan — no Python vector path

> The decision, its rejected alternative (the hand-orchestrated Python
> pipeline of siloed round-trips) and its cost are in
> `.specify/design/kg-engine-native-compute/design.md`.

`HybridRetriever.retrieve_hybrid`'s vector arm is **collapsed onto the engine**.
The vector neighbourhood is **always** computed by the engine's native ANN — never
by Python. There are exactly two engine-native vector paths, in preference order:

1. **Unified cross-modal plan** (`graph.query_unified` → `client.query.unified`).
   ONE costed round-trip the engine sequences over a single
   off-lock snapshot, composing the vector `Rank` leg with optional `Filter`
   (DataFusion) / `Traverse` (petgraph BFS) / `FuseRrf` (native reciprocal-rank
   fusion of a vector + lexical leg, CONCEPT:AU-KG.query.text-spatial-time). The
   mandatory full engine artifact includes this `query` feature.
2. **Native ANN primitive** (`graph.semantic_search`, the engine's IVF-PQ/HNSW).
   The same engine vector index, reached directly as a bounded operational fallback
   if unified planning is unavailable. The unified call's "unknown variant / not
   available" error is the trigger to use it.

The returned `{id, score}` rows are hydrated to full node dicts in **one batched
property fetch** (`nodes.properties_batch`, CONCEPT:EG-KG.compute.graph-compute-engine). Corpus / target-path
restriction is an id-set intersection / substring match over the **bounded ranked
candidate pool the engine returned** — never a full-graph scan.

| Tier | When Active | Performance | Method |
|------|------------|-------------|--------|
| **Unified plan** | `query`-feature engine | O(log N) + costed compose | `client.query.unified` (Scan/Filter/Traverse/`Rank`/FuseRrf/Limit) |
| **Native ANN** | unified planning unavailable | O(log N) | `graph.semantic_search` (IVF-PQ/HNSW) |
| **Keyword** | no engine embeddings | bounded | `engine._search_keyword` (degrade, not a vector path) |
| **Unbounded O(N) Python cosine** | **Never (deleted)** | 80K+ comparisons | ~~`_vector_search_native` label-scoped scan + `cosine_similarity`~~ |

There is **no SQLite-style fallback and no O(N) Python cosine scan**: if the engine
has no embeddings the vector arm is empty and retrieval degrades to keyword search.
The old label-scoped Cypher fallback (`MATCH (n:Label) WHERE n.embedding IS NOT
NULL` + a Python `cosine_similarity` loop) and `_vector_search_native` are removed.

## Manual Operations

### Build Indexes (via MCP)

```
graph_ingest(action="rebuild_indexes")
```

Starts HNSW index building in a **background thread** — returns immediately.
Run this after any bulk ingestion completes or when you want to optimize search.

#### Expected Build Time

| Node Count | Example | Build Time |
|-----------|---------|------------|
| ~80K | Full workspace (78K Code + 2.6K Article) | ~3 minutes |
| ~160K | Double workspace | ~6 minutes |
| ~10K | Small project | ~30 seconds |

Build time scales roughly linearly with total embedded nodes. The `Code` table
(78K nodes) dominates the build time in a typical workspace.

### Check if Search is Using HNSW

If search returns results in sub-second time, HNSW is active. If it takes
2-10 seconds, the label-scoped fallback is being used (still functional,
just not optimal).

## Implementation Files

| File | Component |
|------|-----------|
| `knowledge_graph/backends/contrib/ladybug_backend.py` | `build_vector_indices()`, `drop_vector_indices()` (Ladybug/Kuzu is a demoted `contrib` mirror; the authority is `epistemic_graph_backend.py`, with `postgresql_backend.py` as an optional mirror) |
| `knowledge_graph/retrieval/hybrid_retriever.py` | `_engine_vector_search()` (ONE engine unified plan / native ANN), `_batch_node_properties()` |
| `knowledge_graph/core/graph_compute.py` | `query_unified()` (→ `client.query.unified`), `semantic_search()` |
| `knowledge_graph/core/engine_tasks.py` | `submit_task()` (pre-drop), `_maybe_build_vector_indexes()` (post-build) |
| `mcp/kg_server.py` | `graph_ingest(action="rebuild_indexes")` |

## Key Design Decisions

1. **Targeted drop/build**: Only affects tables written to during ingestion,
   preserving HNSW on unaffected tables for continued fast search.

2. **No startup index build**: Building HNSW on 78K+ Code vectors is CPU/memory
   intensive (~minutes, GBs of RAM). This would block MCP server startup.

3. **Background thread for rebuilds**: Post-ingestion index building runs in a
   daemon thread so worker threads aren't blocked.

4. **One engine vector index for retrieval**: `retrieve_hybrid` no longer keeps a
   Python vector path. The mandatory full engine artifact's ANN is the single
   source of vector ranking (unified plan, or the native `semantic_search`
   primitive when unified planning is unavailable).

## The capability designation index stays in-RAM — and why

`retrieval/capability_index.py` (`CapabilityIndex`) keeps its **own** small
HNSW/native index over *callable* nodes (tools / skills / agents) for
`designate()`. It is **deliberately NOT retired** to the engine's ANN, because it
is a different primitive with stateful semantics the engine vector search does not
provide:

- **Capability pre-filtering** — a `capability -> set[id]` inverted index gives an
  O(1) set-intersection over `providesCapability` *before* ranking. HNSW (and the
  engine ANN) cannot pre-filter a kNN query by an id set; the only correct way to
  rank a capability-restricted candidate subset is a bounded scan over **that
  subset** (`_rank`'s native bounded-cosine branch — O(|filtered candidates|),
  not O(N) over the graph). This is why the native path stays: it ranks the
  filtered candidates without scanning every node.
- **Reward-EMA blending** — `record_outcome()` trains a per-entity reward EMA that
  blends into the designation score, mutated live by the feedback / step-credit /
  reasoner / outcome-router / variant-pool loops. No engine-side equivalent.
- **Ontology-type prior + `swappableWith` alternatives** — re-projection toward the
  modal ontology type and the swappable adjacency surfaced in provenance.

The index is tiny (the callable subset), dependency-discipline-clean (hnswlib is
an optional dep with a native numeric fallback), and stateful — so it is a
genuinely distinct in-RAM structure, not a redundant second copy of the engine's
retrieval ANN. The *general retrieval* vector index is, and remains, the engine's.
