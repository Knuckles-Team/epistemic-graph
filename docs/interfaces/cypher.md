# Cypher interface

The `cypher` feature gives a Cypher surface over the engine's native graph primitives (label index, VF2
subgraph matching, petgraph BFS) — not DataFusion. Reads run over a snapshot; writes mutate the graph.

> Status snapshot: `MATCH … WHERE … RETURN … LIMIT` reads and writes (`CREATE`/`MERGE`/`SET`/`DELETE`
> +`DETACH`) are supported. `REMOVE` and the `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/
> `DISTINCT` clauses are 🔶 in-progress. See the [capability matrix](../capabilities.md#cypher-eg-querycypher).

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

## Writes

```cypher
CREATE (a:Agent {id: 'AgentC', region: 'us'})
MERGE (b:Agent {id: 'AgentD'})
SET a.active = true
DELETE a            // DETACH DELETE to also drop incident edges; edge-var DELETE supported
```

`CREATE`/`MERGE`/`SET`/`DELETE` (+`DETACH`) map to native eg-core mutations (`add_node`/`add_edge`/
`compare_and_set_fields`/`remove_node`/`remove_edge`). MERGE is idempotent (create-if-absent via the label index).

## Not yet (🔶 in-progress)

- `REMOVE` (property/label removal).
- `ORDER BY`, `SKIP`, `WITH`, `OPTIONAL MATCH`, `OR`/`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL` in WHERE,
  aggregation (`count`/`collect`/…), `DISTINCT`, comma-separated disjoint patterns.
- Var-length combined with surrounding fixed hops + path-variable binding.

These land via grammar + executor extensions — see the [roadmap](../roadmap.md#graph-toward-full-neo4j-parity).

## Relationship to the other surfaces

The Cypher executor shares its edge-matching and snapshot model with the
[GraphQL](graphql.md) resolver — the two produce byte-equal results for equivalent reads. For writes
today, use the native client or SQL DML on the `nodes` table; for cross-modal queries that mix graph
traversal with vector/text/SQL, use [UQL](../uql.md).
</content>
