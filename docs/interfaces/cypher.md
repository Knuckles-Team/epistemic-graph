# Cypher interface

The `cypher` feature gives a Cypher surface over the engine's native graph primitives (label index, VF2
subgraph matching, petgraph BFS) — not DataFusion. Reads run over a snapshot; writes mutate the graph.

> Status snapshot: `MATCH … WHERE … RETURN … LIMIT` reads and writes (`CREATE`/`MERGE`/`SET`/`DELETE`
> +`DETACH`/`REMOVE`) are supported, along with `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`UNWIND`, the
> richer WHERE (`OR`/`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL`), aggregation, `DISTINCT`, `CALL {subquery}` /
> `CALL proc() YIELD`, and the `gds.*` graph-data-science procedures (EG-061/062/063/141/142/143/144). A
> **Bolt v4.4** wire (EG-159) lets Neo4j drivers connect directly. See the
> [capability matrix](../capabilities.md#cypher-eg-querycypher).

## Supported grammar

```cypher
MATCH (a:Agent)-[:SUPERVISES*1..3]->(b:Agent)
WHERE a.region = 'us' AND b.active = true
RETURN a.id, b.id
LIMIT 100
```

- **Patterns**: linear `node (edge node)*`. Nodes `(var:Label)` — both the variable and the label are
  optional. Edges `-[:REL]->`, `<-[:REL]-`, and variable-length `-[:REL*m..n]->` (now combinable with
  surrounding fixed hops + path-variable binding, EG-063).
- **WHERE** (EG-062): `AND`/`OR`, `var.prop <op> literal` with `= <> != < <= > >=`, plus `IN`,
  `STARTS WITH`, `CONTAINS`, `IS NULL`.
- **RETURN**: `var`, `var.prop`, `*`, `DISTINCT`, comma-separated; aggregation
  (`count`/`collect`/`sum`/`avg`/`min`/`max`).
- **Pipeline**: `WITH`, `ORDER BY`, `SKIP`, `OPTIONAL MATCH`, and `UNWIND expr AS var` (EG-141) compose as
  chained stages.
- **LIMIT**: integer (an implicit cap of 50,000 rows protects the engine).

## Writes

```cypher
CREATE (a:Agent {id: 'AgentC', region: 'us'})
MERGE (b:Agent {id: 'AgentD'})
SET a.active = true
DELETE a            // DETACH DELETE to also drop incident edges; edge-var DELETE supported
```

`CREATE`/`MERGE`/`SET`/`DELETE` (+`DETACH`) and `REMOVE` (property/label removal, EG-061) map to native
eg-core mutations (`add_node`/`add_edge`/`compare_and_set_fields`/`remove_node`/`remove_edge`). MERGE is
idempotent (create-if-absent via the label index).

## Procedures — `CALL` + GDS (EG-142/143/144)

`CALL { subquery }` and `CALL proc(args) YIELD …` invoke a procedure registry that dispatches to native
(or WASM) procedures — the Neo4j-parity keystone. The registry ships an APOC-equivalent library plus the
**graph-data-science** algorithms (a pure-Rust Neo4j-GDS-parity library in eg-compute, EG-144):

```cypher
CALL gds.pageRank.stream('social') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 10;
```

Available `gds.*`: PageRank, weakly/strongly-connected components, Louvain community detection,
betweenness + degree centrality, single-source weighted shortest path (Dijkstra), and node similarity
(Jaccard/cosine over neighborhoods).

## Remote drivers — Bolt v4.4 (EG-159, feature `bolt-wire`)

A native Bolt v4.4 listener (`src/server/bolt_wire/`, PackStream v2 chunked framing,
HELLO/LOGON/RUN/PULL/DISCARD/BEGIN/COMMIT/ROLLBACK) lets Neo4j drivers (neo4j-python/js/go, `cypher-shell`)
connect directly — `RUN`'s Cypher goes straight to this engine, no SQL layer. Set
`EPISTEMIC_GRAPH_BOLT_ADDR` (default `127.0.0.1:7687`); see [connecting](connecting.md#neo4j--cypher-shell--bolt-driver-bolt-wire).

## Relationship to the other surfaces

The Cypher executor shares its edge-matching and snapshot model with the
[GraphQL](graphql.md) resolver — the two produce byte-equal results for equivalent reads. For cross-modal
queries that mix graph traversal with vector/text/SQL, use [UQL](../uql.md).
</content>

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](sql.md) · [SPARQL & RDF](sparql.md) · [GraphQL](graphql.md) · [Vector / ANN](vector.md) · [Connecting (per-wire guide)](connecting.md).
