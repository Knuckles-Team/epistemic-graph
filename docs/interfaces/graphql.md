# GraphQL interface

The `graphql` feature gives a native, pure-Rust GraphQL surface (no `async-graphql`). The schema is
introspected from the live graph — root fields are node labels, scalar fields are properties, and object
fields are typed edges. Queries compile to label scans + BFS over the same `GraphView` the Cypher path
uses, and produce byte-equal results; mutations map to native graph write methods.

> Status snapshot: read queries and mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`)
> are supported, along with CDC subscriptions (EG-064), fragments / variables / directives (EG-065), relay
> pagination (EG-066), **Apollo Federation v2** subgraph support (EG-295), and production hardening
> (APQ + depth/complexity limits, EG-296). See the [capability matrix](../capabilities.md#graphql-eg-graphql).

## Supported queries

```graphql
{
  Agent(first: 10, region: "us") {
    id
    role
    supervises {        # an edge typed "supervises" → target nodes
      id
    }
  }
}
```

- Root fields are node **labels**; unknown labels error
  `GraphQL: no node type … (root fields must be node labels)`.
- Arguments: `first` / `limit` (int) and property-equality filters.
- Aliases (`alias: field`), nested selection sets, scalar property fields, and object/edge fields.
- `query { … }`, `query Name { … }`, and bare `{ … }` are all accepted.

## Mutations

```graphql
mutation {
  createNode(label: "Agent", props: {id: "AgentC", region: "us"}) { id }
  addEdge(src: "AgentC", tgt: "AgentD", type: "supervises") { ok }
}
```

`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge` map to native eg-core mutations and bump the
OCC version once per batch; returned objects are shaped by the same resolver the query path uses.

## Subscriptions — CDC (EG-064)

A `tokio::sync::broadcast` change-stream fed by `GraphCore` `mark_dirty` backs live subscriptions over a
WS/SSE carrier (`EPISTEMIC_GRAPH_GRAPHQL_ADDR` starts the SSE carrier — a `text/event-stream` frame per
graph change). This replaces the earlier poll-only stub.

## Fragments, variables, directives & pagination (EG-065/066)

- **Fragments** (EG-065): named fragment-spreads and inline fragments (`... on Type`).
- **Variables** (EG-065): `$var` definitions/refs, resolved from the request variables.
- **Directives** (EG-065): `@skip(if:)` / `@include(if:)`.
- **Relay pagination** (EG-066): `first`/`after`/`before`/cursor args with an
  `edges`/`node`/`cursor`/`pageInfo` envelope over a deterministic sort.

## Apollo Federation subgraph (EG-295)

The engine is a **federated subgraph** in an Apollo supergraph: it serves `_service { sdl }` and
`_entities(representations: [_Any!]!)` resolvers, and parses `@key`/`@shareable`/`@external` directives in
the emitted SDL.

```graphql
{ _service { sdl } }
```

## Production hardening — APQ + limits (EG-296)

For production the surface adds: **automatic persisted queries** (APQ — a sha256 hash registry so clients
send hash-only requests), query **depth + complexity/cost** analysis with configurable limits (over-budget
queries are rejected before execution), field/node count caps, and an **introspection on/off** toggle.
These protect the federated subgraph.
</content>
