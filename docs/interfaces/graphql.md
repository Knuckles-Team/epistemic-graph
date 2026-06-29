# GraphQL interface

The `graphql` feature gives a native, pure-Rust GraphQL surface (no `async-graphql`). The schema is
introspected from the live graph — root fields are node labels, scalar fields are properties, and object
fields are typed edges. Queries compile to label scans + BFS over the same `GraphView` the Cypher path
uses, and produce byte-equal results; mutations map to native graph write methods.

> Status snapshot: read queries and mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`)
> are supported. Subscriptions (poll-only stub today), fragments, variables, directives, and relay
> pagination are 🔶 in-progress. See the [capability matrix](../capabilities.md#graphql-eg-graphql).

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

## Not yet (🔶 in-progress)

- Subscriptions: a poll-only stub today; a `broadcast` change-stream over `mark_dirty` + a WS/SSE carrier
  is being added.
- Fragments, variables, and directives (`@skip`/`@include`) are rejected at parse today.

Interfaces/unions and relay connection pagination are also being added. Subscriptions (over the streaming/CDC layer) are on the
[roadmap](../roadmap.md#graphql).
</content>
