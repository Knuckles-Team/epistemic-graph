# GraphQL interface

The `graphql` feature gives a native, pure-Rust GraphQL surface (no `async-graphql`). The schema is
introspected from the live graph — root fields are node labels, scalar fields are properties, and object
fields are typed edges. Queries compile to label scans + BFS over the same `GraphView` the Cypher path
uses, and produce byte-equal results; mutations map to native graph write methods.

Read, mutation, and Federation operations use the authenticated native
`Method::GraphQl` dispatch. The optional HTTP listener documented below is a
subscription-only SSE carrier; it is not a second query/mutation endpoint.

> Status snapshot: read queries and mutations (`createNode`/`updateNode`/`deleteNode`/`addEdge`/`removeEdge`)
> are supported, along with CDC subscriptions (EG-064), fragments / variables / directives (EG-KG.query.fragments-variables-directives), relay
> pagination (EG-KG.query.graphql-cursors), **Apollo Federation v2** subgraph support (EG-295), and production hardening
> (APQ + depth/complexity limits, EG-KG.domains.graphql-enterprise-hardening). See the [capability matrix](../capabilities.md#graphql-eg-graphql).

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

## Subscriptions — authenticated SSE (EG-064) {#subscriptions-authenticated-sse}

`EPISTEMIC_GRAPH_GRAPHQL_ADDR` starts the loopback-only SSE listener. It accepts one
current request contract:

```http
GET /graphql/subscribe?graph=<percent-encoded-graph>&query=<percent-encoded-subscription> HTTP/1.1
Host: <configured-listener>
Authorization: Bearer eg2.<verified-envelope>
X-Epistemic-Request-Id: <positive-u64>
```

Construct the `eg2.` envelope over the exact native request below, then place that
token in the `Authorization` header. The request id, graph, and subscription text must
be byte-identical to the HTTP values after form decoding:

```text
Request {
  id: <X-Epistemic-Request-Id>,
  graph: <graph>,
  agent_id: None,
  method: GraphQl { query: <subscription>, variables: None }
}
```

The signed context must carry `query:graphql`, `query:*`, `*`, or an applicable
`kg:read`/`kg:write`/`kg:admin` scope. The engine then enforces the graph's current
read ACL and applies default-deny RLS before every initial or updated frame. ACL/RLS
are re-evaluated on both graph changes and keepalive ticks. A change affecting only
hidden rows produces no data frame, preventing hidden-row change timing leaks.
The carrier applies the locked-down depth/complexity/field policy and requires
an explicit `first`/`limit` of at most `100` on every subscription root (nested
fan-out is also complexity-priced). Query text is capped at 32 KiB,
syntax nesting at 64 delimiters, and each serialized SSE data frame at 8 MiB.
Slow readers are disconnected by bounded writes.

Example after obtaining a newly signed token from the configured authority issuer:

```bash
curl --no-buffer --get "http://127.0.0.1:7879/graphql/subscribe" \
  -H "Authorization: Bearer ${EG2_TOKEN:?}" \
  -H "X-Epistemic-Request-Id: ${REQUEST_ID:?}" \
  --data-urlencode "graph=${GRAPH:?}" \
  --data-urlencode 'query=subscription { Document(first: 5) { id title } }'
```

Bearer tokens are never accepted in the URL. There is no unsigned mode, default
graph, permissive CORS/`OPTIONS` path, or compatibility listener. Each reconnect must
use a fresh request id, nonce, and `eg2.` envelope because replay nonces are consumed
before streaming begins. The carrier does not log or persist bearer tokens, graph
names, or subscription documents; it drops the signed request after verification.
Sessions are deliberately bounded and must reauthenticate.

| Setting | Default | Meaning |
|---------|---------|---------|
| `EPISTEMIC_GRAPH_GRAPHQL_ADDR` / `--graphql-addr` | disabled; `on` resolves to `127.0.0.1:7879` | Loopback SSE listener. Invalid or non-loopback addresses fail startup. |
| `EPISTEMIC_GRAPH_GRAPHQL_MAX_CONNECTIONS` / `--graphql-max-connections` | `128` | Process-wide cap across in-flight handshakes and SSE sessions; valid range `1..=10000`. |
| `EPISTEMIC_GRAPH_GRAPHQL_MAX_SESSION_SECS` / `--graphql-max-session-secs` | `300` | Maximum session lifetime before fresh authentication is required; valid range `1..=3600`. |

The listener does not terminate TLS. For remote clients, place a TLS reverse proxy on
the same host and forward the `Authorization` and `X-Epistemic-Request-Id` headers to
the loopback listener.

## Fragments, variables, directives & pagination (EG-KG.query.fragments-variables-directives/066)

- **Fragments** (EG-KG.query.fragments-variables-directives): named fragment-spreads and inline fragments (`... on Type`).
- **Variables** (EG-KG.query.fragments-variables-directives): `$var` definitions/refs, resolved from the request variables.
- **Directives** (EG-KG.query.fragments-variables-directives): `@skip(if:)` / `@include(if:)`.
- **Relay pagination** (EG-KG.query.graphql-cursors): `first`/`after`/`before`/cursor args with an
  `edges`/`node`/`cursor`/`pageInfo` envelope over a deterministic sort.

## Apollo Federation subgraph (EG-295)

The engine is a **federated subgraph** in an Apollo supergraph: it serves `_service { sdl }` and
`_entities(representations: [_Any!]!)` resolvers, and parses `@key`/`@shareable`/`@external` directives in
the emitted SDL.

```graphql
{ _service { sdl } }
```

## Production hardening — APQ + limits (EG-KG.domains.graphql-enterprise-hardening)

For production the surface adds: **automatic persisted queries** (APQ — a sha256 hash registry so clients
send hash-only requests), query **depth + complexity/cost** analysis with configurable limits (over-budget
queries are rejected before execution), field/node count caps, and an **introspection on/off** toggle.
These protect the federated subgraph.

---

**See also:** [Capabilities matrix](../capabilities.md) · [Cypher & Bolt](cypher.md) · [SPARQL & RDF](sparql.md) · [SQL & pgwire](sql.md) · [Connecting (per-wire guide)](connecting.md).
