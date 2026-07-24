# Cypher interface

The `cypher` feature gives a Cypher surface over the engine's native graph primitives (label index, VF2
subgraph matching, petgraph BFS) — not DataFusion. Reads run over an authority-filtered snapshot. Writes
run against a detached graph and publish only after the authoritative MutationBatch commit succeeds.
Native clients use separate `query.cypher_read(...)` and `query.cypher_write(...)` methods. Each request
carries a required mode, and the engine's complete Cypher parser rejects a declared-mode mismatch before
execution. Authorization never depends on a client-side keyword scan.

> Status snapshot: `MATCH … WHERE … RETURN … LIMIT` reads and writes (`CREATE`/`MERGE`/`SET`/`DELETE`
> +`DETACH`/`REMOVE`) are supported, along with `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`UNWIND`, the
> richer WHERE (`OR`/`IN`/`STARTS WITH`/`CONTAINS`/`IS NULL`), aggregation, `DISTINCT`, `CALL {subquery}` /
> `CALL proc() YIELD`, and the `gds.*` graph-data-science procedures (EG-KG.query.cypher-execution/062/063/141/142/143/144). A
> **Bolt v4.4** wire (EG-KG.query.bolt-wire-protocol) lets Neo4j drivers connect directly. See the
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
  surrounding fixed hops + path-variable binding, EG-KG.query.concept-2).
- **Quantified path patterns (Cypher 25, EG-KG.query.quantified-path-pattern)**: `((a)-[:REL]->(b)){m,n}`
  repeats a WHOLE inner sub-pattern — not just one relationship — `m..n` times, e.g.
  `MATCH (x)((a)-[r:LIKES]->()-[:KNOWS]->(b)){1,3}(y) RETURN a, b, type(r), y`.
  A single-hop group generalizes `-[:REL*m..n]->`; multi-hop and recursively nested inner patterns use
  the same walker. Variables declared inside the group are projected as ordered per-repetition lists
  (node variables, relationship variables, property access, and `type(r)`); a valid `{0}` match exposes
  empty lists. Distinct paths ending at the same node remain distinct when their group bindings differ.
  `WITH` and `RETURN *` preserve those group variables. Expansion fails at the same 50,000-row bound as
  ordinary Cypher results rather than exhausting memory.
- **QPP in `CREATE`**: the group is materialized natively, including multi-hop/nested groups and returned
  per-iteration variable lists. An exact `{n}` creates `n` repetitions. A bounded range `{m,n}`
  deterministically creates its inclusive upper bound `n`; the open upper form uses the parser's finite
  bound of 16. Descending bounds are rejected.
- **WHERE** (EG-KG.query.eg-extend-read-side): `AND`/`OR`, `var.prop <op> literal` with `= <> != < <= > >=`, plus `IN`,
  `STARTS WITH`, `CONTAINS`, `IS NULL`.
- **RETURN**: `var`, `var.prop`, `*`, `DISTINCT`, comma-separated; aggregation
  (`count`/`collect`/`sum`/`avg`/`min`/`max`). A bare node variable materializes as
  a property map with authoritative virtual `id` and canonical `node_type`; it is
  never returned as an implementation-level node-id string. `var.id` reads the
  authoritative graph key even if an input payload attempted to shadow `id`.
- **Pipeline**: `WITH`, `ORDER BY`, `SKIP`, `OPTIONAL MATCH`, and `UNWIND expr AS var` (EG-KG.query.param-list-drives-unwind) compose as
  chained stages.
- **LIMIT**: integer (an implicit cap of 50,000 rows protects the engine).

## Writes

```cypher
CREATE (a:Agent {id: 'AgentC', region: 'us'})
MERGE (b:Agent {id: 'AgentD'})
SET a.active = true
DELETE a            // DETACH DELETE to also drop incident edges; edge-var DELETE supported
```

`CREATE`/`MERGE`/`SET`/`DELETE` (+`DETACH`) and `REMOVE` (property/label removal, EG-KG.query.cypher-execution) map to native
eg-core mutations (`add_node`/`add_edge`/`compare_and_set_fields`/`remove_node`/`remove_edge`). MERGE is
idempotent (create-if-absent via the label index).

Cypher has one primary-label field: `node_type`. `CREATE (n:Agent ...)` persists
`node_type: "Agent"`; `MATCH (n:Agent)`, `n.node_type`, `REMOVE n:Agent`, and
`db.labels()` read that same field (plus the explicit secondary `labels` array where
applicable). Ordinary payload properties named `type` or `label` are not structural
aliases. A conflicting explicit `node_type` on a labelled `CREATE` is rejected.

## Procedures — `CALL` + GDS (EG-KG.query.cypher-planning/143/144)

`CALL { subquery }` and `CALL proc(args) YIELD …` invoke a procedure registry that dispatches to native
(or WASM) procedures — the Neo4j-parity keystone. The registry ships an APOC-equivalent library plus the
**graph-data-science** algorithms (a pure-Rust Neo4j-GDS-parity library in eg-compute, EG-144):

```cypher
CALL gds.pageRank.stream('social') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 10;
```

Available `gds.*`: PageRank, weakly/strongly-connected components, Louvain community detection, Label
Propagation, betweenness + degree centrality, single-source weighted shortest path (Dijkstra), node
similarity (Jaccard/cosine over neighborhoods, all-pairs `gds.nodeSimilarity` + per-node top-`k`
`gds.knn` — `mode: 'exact'` [default, full `O(V²·d̄)` sweep] or `mode: 'approximate'` [seeded
NN-descent sampling: `sampleRate`/`maxIterations`/`deltaThreshold`/`randomSeed`, sub-quadratic at
large V]), density clustering (`gds.dbscan`, feature `cypher-mining`), and link prediction
(`gds.linkPrediction`, a KAN classifier over structural pair features, feature `cypher-graphlearn`) —
CONCEPT:EG-KG.query.gds-procedure-routing.

**W4.1 GDS-parity expansion** (12→26 procedures, all always-on — no new feature): **community** —
`gds.leiden` (a refinement phase makes every returned community's induced subgraph CONNECTED by
construction, the defect Traag/Waltman/van Eck 2019 prove plain Louvain does not avoid),
`gds.triangleCount`/`gds.localClusteringCoefficient`, `gds.kcore` (degeneracy/coreness), `gds.k1coloring`
(Welsh–Powell greedy proper coloring); **centrality** — `gds.eigenvector` (power iteration),
`gds.articleRank` (a PageRank variant discounting low-out-degree sources), `gds.closeness` (optional
Wasserman–Faust `useWassermanFaust` correction), `gds.harmonic`; **paths** — `gds.shortestPath.astar`
(caller-supplied heuristic, e.g. haversine over a lat/lon property pair), `gds.shortestPath.yens` (the
`k` shortest loopless paths), `gds.steinerTree` (the classical Kou–Markowsky–Berman MST-based
2-approximation), `gds.randomWalk` (weighted, with restart probability, seeded and deterministic given
the seed).

## Remote drivers — Bolt v4.4 (EG-KG.query.bolt-wire-protocol, feature `bolt-wire`)

A native Bolt v4.4 listener (`src/server/bolt_wire/`, PackStream v2 chunked framing,
HELLO/LOGON/RUN/PULL/DISCARD/BEGIN/COMMIT/ROLLBACK) lets Neo4j drivers with custom auth-token support
connect directly — `RUN`'s Cypher goes straight to this engine, no SQL layer. Set
`EPISTEMIC_GRAPH_BOLT_ADDR` (default `127.0.0.1:7687`); see [connecting](connecting.md#neo4j-bolt-drivers).

The direct listener is plaintext and unconditionally loopback-only. HELLO/LOGON accepts only the
`epistemic` scheme. Its credentials are a fresh hex-MessagePack `Health` request carrying the current
`eg2.` envelope. The shared verifier durably consumes the nonce, verifies tenant/audience/policy and
derives `CarrierAuthority` plus row authority; the request's signed graph becomes immutable for that
connection. A `db` value on BEGIN/RUN is accepted only when it equals that signed graph. The display
`principal` is ignored as authority.

Every RUN rechecks `query:cypher` scope and graph ACL. Reads use the common RLS projection. Auto-commit
writes and explicit COMMIT use the same detached Cypher → row delta → authoritative MutationBatch
barrier as native Cypher. An explicit transaction captures a versioned snapshot, replays buffered writes
under the same RLS projection, rejects a version conflict, and publishes all writes once; ROLLBACK,
RESET, disconnect, and failed execution discard the detached state. Durable receipts store an opaque
transaction digest rather than query text or bound values.

There is no Bolt auth-mode or default-graph environment variable and no basic-password fallback.
Clients must mint a fresh token for each physical connection; the Python native client exposes
`fresh_bolt_auth_token(graph=...)` for auth-manager callbacks. A basic-only `cypher-shell` cannot use
this current contract. Remote clients require an authenticated TLS/mTLS gateway into the loopback
backend and still must complete signed-session verification.

## Relationship to the other surfaces

The Cypher executor shares its edge-matching and snapshot model with the
[GraphQL](graphql.md) resolver — the two produce byte-equal results for equivalent reads. For cross-modal
queries that mix graph traversal with vector/text/SQL, use [UQL](../uql.md).
</content>

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](sql.md) · [SPARQL & RDF](sparql.md) · [GraphQL](graphql.md) · [Vector / ANN](vector.md) · [Connecting (per-wire guide)](connecting.md).
