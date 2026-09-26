# Unified IndexManager (CONCEPT:AU-KG.retrieval.architecture-report)

One registry/seam over the engine's secondary indexes, so a planner consults **one
place** instead of knowing each index individually.

## Why

The engine's secondary indexes grew ad-hoc, each bolted onto `GraphCore`:

| Index | Concept | Where it lives | Shape |
|-------|---------|----------------|-------|
| Lazy LABEL index (`label → ids`) | `CONCEPT:EG-KG.compute.consult-lazy` | `eg-core/src/graph.rs` (`label_index` cell) | equality lookup |
| Bounded PROPERTY equality index (`key → value → ids`) | `CONCEPT:EG-KG.query.concept-12` | `eg-core/src/graph.rs` (`property_index` cell) | equality lookup |
| aho-corasick ontology term index | `CONCEPT:EG-ORCH.routing.lexical-capability-escalation` | `eg-core/src/graph.rs` (`ontology_index` cell) | lexical scan |
| Vector index (HNSW / eg-ann IVF-PQ) | `CONCEPT:EG-KG.sharding.semantic-embedding-store-backed` | `eg-core/src/compute/semantic.rs` (`SemanticStore`) | kNN |

All are lazy + `version()`/`mark_dirty`-invalidated, but there was **no single
registry**: eg-query's pushdown and eg-plan's Filter leg each had to know the
label/property indexes individually.

## The seam — `eg-core/src/index.rs`

```text
SecondaryIndex (trait)
  kind() -> IndexKind
  descriptor() -> IndexDescriptor          // discoverability metadata
  covers(&Predicate) -> bool               // "could I resolve this?"
  lookup(&GraphCore, &Predicate) -> Option<Vec<String>>   // resolve, or None -> full-scan

IndexManager  (owned by GraphCore, reachable via `core.indexes()`)
  index_for(&Predicate) -> Option<&dyn SecondaryIndex>    // the pushdown registry
  lookup(&GraphCore, &Predicate) -> Option<Vec<String>>
  descriptors() -> Vec<IndexDescriptor>
  descriptors_for_column(&str) -> Vec<IndexDescriptor>    // "what covers column X?"
  invalidate_all(&GraphCore)               // single invalidation hook
```

### Behavior preserved — the caches did not move

The LABEL and PROPERTY indexes keep their **existing cache cells on `GraphCore`**
(`label_index` / `property_index`) and their exact lazy-build + `mark_dirty`
invalidation. The `LabelIndex` / `PropertyEqIndex` trait impls are **thin
descriptors** that route to the same `GraphCore` methods
(`get_nodes_by_label` / `nodes_by_property`). The manager is the registry/routing
seam — it does **not** relocate cache storage, so label/property performance and
their tests are untouched. The vector + ontology indexes are registered as
**discoverable-only** (`serves_lookup = false`): they answer kNN / lexical scans
through their own surfaces, but a planner can now enumerate them through the one
registry.

### The relational sibling — `eg-query`'s `PushdownRegistry`

eg-query's `nodes` SQL provider works on row positions over a materialized Arrow
batch (a different data shape than node ids), so it has its own registry —
`PushdownRegistry` in `crates/eg-query/src/sql/providers.rs` — mirroring the same
seam at the relational boundary. `NodesTableProvider::supports_filters_pushdown`
and `::scan` consult **one** `PushdownRegistry` (`indexable_eq` /  `lookup`)
instead of bespoke per-column checks scattered across provider methods. The
bounded + demand-driven equality policy (`CONCEPT:EG-KG.query.concept-12`,
`EPISTEMIC_GRAPH_INDEXED_PROPERTIES` / `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES`) is
preserved exactly, and the predicates stay `Inexact` so DataFusion re-applies them
— results are byte-identical to the full-scan path.

eg-plan's Filter leg goes through `eg_query::exec_sql` over the same `nodes`
provider, so it consults the same one registry transitively.

## Extension point — adding a new index TYPE (text / spatial / time)

Adding a new index kind is a **closed, three-step change that does not touch the
manager core**:

1. **Add a variant** to `IndexKind` (and, if it answers a new predicate shape, a
   variant to `Predicate` — e.g. `Predicate::TextMatch { column, query }` or
   `Predicate::TimeBetween { column, lo, hi }`).
2. **Implement `SecondaryIndex`** for the new index: its own cache (lazy-built,
   invalidated off `mark_dirty`/`version()` like the others), its `descriptor()`
   (`columns` + `serves_lookup`), `covers()`, and `lookup()`.
3. **Register** an instance in `IndexManager::with_default_indexes()`.

`index_for`, `descriptors`, `descriptors_for_column`, and `invalidate_all` iterate
the registry generically, so they pick the new index up with no edit. A future
stateful index whose cache lives in its own struct (not a `GraphCore` cell) clears
it from `IndexManager::invalidate_all`, keeping invalidation a single seam.

## Server-layer indexes — the write side (Track D3)

A core-owned index (label / property / vector) is registered once at construction in
`with_default_indexes()` and read lock-free. But the **text** (`eg-text` Tantivy),
**temporal** (`eg-tsdb` `SeriesStore`), and **derived-OWL** (`eg-rdf` reasoner) indexes
live in the SERVER layer, ABOVE `eg-core` in the crate DAG — so `eg-core` cannot name
their concrete types, and they can't be in the fixed set. They implement
`SecondaryIndex` (defined in `eg-core`), and are registered as boxed trait objects
AFTER construction:

```text
IndexManager
  indexes:        Vec<Box<dyn SecondaryIndex>>          // core set (label/property/ontology/vector), lock-free read path
  server_indexes: RwLock<Vec<Box<dyn SecondaryIndex>>>  // text/temporal/derived-OWL, registered post-construction
  content_capture: AtomicBool                           // "some server index derives from node content"

  register_server_index(&self, idx)     // &self interior mutability, flips content_capture on needs_content()
  wants_change_content() -> bool         // one relaxed atomic load on the write hot path
```

`SecondaryIndexFactory::for_graph(name) -> Vec<Box<dyn SecondaryIndex>>` mirrors
`ReadThroughFactory`: `GraphRegistry::set_secondary_index_factory` registers the
per-graph server indexes onto every existing graph and every future `create_graph`,
via `GraphCore::register_index`. `commit_batch`/`rebuild_all` iterate `server_indexes`
in addition to the core set, so the committed write batch (`GraphCore::maintain_indexes`)
drives their `apply_delta` under the SAME topology lock as the vector store — never a
torn hybrid read.

**Content capture + deadlock discipline.** A content-derived index overrides
`needs_content() -> true`; the coalescer's `apply_batch` then captures each added/
updated node's property blob into `ChangeSet.NodeChange.properties_msgpack` (zero-clone
when no such index is registered). `apply_delta` reads content ONLY from that captured
blob — NEVER by calling back into `core` under the held topology lock — so it can't
deadlock. `full_rebuild`, which does re-read every live node, is available only as
an explicit out-of-lock operator repair action; served writes always maintain deltas.
The adapters live in `src/server/secondary_indexes.rs`.

The text (`CONCEPT:EG-KG.storage.incremental-text`), temporal
(`CONCEPT:EG-KG.storage.incremental-temporal`), and derived-OWL
(`CONCEPT:EG-KG.storage.incremental-derived-owl`) indexes are now **wired**; the derived-OWL
adapter is a documented no-op because the reasoner already differentially materializes
on-demand. Spatial (`CONCEPT:AU-KG.query.text-spatial-time`) remains a future index behind
the same seam.
