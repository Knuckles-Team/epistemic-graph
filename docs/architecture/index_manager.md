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

This increment leaves the text (`CONCEPT:AU-KG.query.text-spatial-time`), spatial, and time
(`CONCEPT:AU-KG.retrieval.god-nodes-communities`) indexes **unbuilt** — it only opens the seam.
