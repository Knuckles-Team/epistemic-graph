# GraphQL interface

The `graphql` feature gives a native, pure-Rust GraphQL **read** surface (no `async-graphql`). The
schema is introspected from the live graph — root fields are node labels, scalar fields are properties,
and object fields are typed edges. Queries compile to label scans + BFS over the same `GraphView` the
Cypher path uses, and produce byte-equal results.

> Status snapshot: read queries are supported; mutations and subscriptions are roadmap. See the
> [capability matrix](../capabilities.md#graphql-eg-graphql).

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

## Not supported (roadmap)

Rejected at parse with clear messages:

- `GraphQL mutations are not supported (read-only surface)`
- `GraphQL subscriptions are not supported`
- `GraphQL fragments are not supported`

Variables, directives, interfaces/unions, and relay connection pagination are also deferred. Mutations
(over the graph write methods) and subscriptions (over the streaming/CDC layer) are on the
[roadmap](../roadmap.md#graphql).
</content>
