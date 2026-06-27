# Cypher interface

The `cypher` feature gives a **read-only** Cypher surface over a single graph snapshot. It is the
familiar `MATCH … WHERE … RETURN … LIMIT` shape, executed against the engine's native graph primitives
(label index, VF2 subgraph matching, petgraph BFS) — not DataFusion.

> Status snapshot: read traversal is supported; Cypher **writes are roadmap**. See the
> [capability matrix](../capabilities.md#cypher-eg-querycypher).

## Supported grammar

```cypher
MATCH (a:Agent)-[:SUPERVISES*1..3]->(b:Agent)
WHERE a.region = 'us' AND b.active = true
RETURN a.id, b.id
LIMIT 100
```

- **Patterns**: linear `node (edge node)*`. Nodes `(var:Label)` — both the variable and the label are
  optional. Edges `-[:REL]->`, `<-[:REL]-`, and variable-length `-[:REL*m..n]->` (single hop today).
- **WHERE**: conjunctive (`AND`-only) predicates `var.prop <op> literal` with `= <> != < <= > >=`.
- **RETURN**: `var` or `var.prop`, comma-separated.
- **LIMIT**: integer (an implicit cap of 50,000 rows protects the engine).

## Not supported (roadmap)

- Writes: `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE` (not in the grammar).
- `ORDER BY`, `SKIP`, `WITH`, `OPTIONAL MATCH`, `OR`/`NOT` in WHERE, aggregation, `DISTINCT`,
  comma-separated disjoint patterns.

These land via a write planner and grammar extensions —
see the [roadmap](../roadmap.md#graph-toward-full-neo4j-parity).

## Relationship to the other surfaces

The Cypher executor shares its edge-matching and snapshot model with the
[GraphQL](graphql.md) resolver — the two produce byte-equal results for equivalent reads. For writes
today, use the native client or SQL DML on the `nodes` table; for cross-modal queries that mix graph
traversal with vector/text/SQL, use [UQL](../uql.md).
</content>
