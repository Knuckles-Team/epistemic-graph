# epistemic-graph — thin JS/Node client (CONCEPT:EG-KG.ingest.broker-streams-namespaces)

A **deliberately thin** Node client for the Program-B engine `Method`s that had no
client surface. It is **not a full SDK** — the canonical, full-featured client is the
Python one in [`epistemic_graph/client.py`](../../epistemic_graph/client.py). This
binding covers only the new wire ops from B1.7:

| Domain | Methods | Engine concept |
|--------|---------|----------------|
| Broker admin | `declareExchange` `deleteExchange` `declareQueue` `bindQueue` `unbindQueue` | EG-275/276/277/278 |
| Broker publish | `publish` `publishEx` `publishConfirmed` `publishIdempotent` | EG-275/279/284/314 |
| Broker consume | `brokerConsume` `brokerAck` `brokerReject` `brokerAckTag` `brokerNackTag` `brokerRenewTag` `sweepExpired` | EG-KG.compute.groups-qos-prefetch-honoring/276/284 |
| Streams | `streamDeclare` `streamPublish` `streamRead` `streamTrim` `streamCommitOffset` `streamCommittedOffset` | EG-283 |
| RBAC admin | `rbacAddRole` `rbacRemoveRole` `rbacAddGrant` `rbacRemoveGrant` `rbacList` | EG-KG.compute.feature |
| Identity security | `registerIdentity` `bootstrapSystemIdentity` `applyMultisigMutation` | Signed current-context operations |
| Ops | `backup(destination, label)` `restore(source, targetShards)` | EG-090 |
| NL→query | `nlQuery` | EG-080 |

> **Generated-from-the-Method-list.** Every method maps 1:1 to a Rust `Method`
> variant in [`crates/eg-types/src/protocol.rs`](../../crates/eg-types/src/protocol.rs),
> using the exact serde field names that enum destructures. The full graph / vector /
> RDF / SQL API is intentionally omitted — use the Python client for that.

## Wire contract

Same framed-MessagePack transport as the Python client (the ONE contract — there is no
PyO3/FFI, so the wire is the API):

- **Framing:** 4-byte big-endian length prefix + a msgpack request
  `{ id, graph, auth_token, method, params }`.
- **Correlation:** responses arrive out of order, demuxed by `id`.
- **Auth:** every request carries an `eg2.` signed request-context envelope. The
  MAC binds the request id, graph, canonical method body hash, explicit authority
  claims, timestamp, nonce, and idempotency key. Construction rejects an empty
  secret, missing claims, unknown fields, empty or duplicate list entries, and a
  malformed delegation chain.
- **Compact results:** a top-level msgpack `bin` result is a second `Raw` layer and is
  decoded once more (matching the Python client).

## Pi-contract

Thin by design: one pure-JS dependency (`@msgpack/msgpack`) for framing; Node built-ins
`net` (UDS/TCP) + `crypto` (HMAC). No native addon, no heavy SDK. Nothing here belongs
in the `pi` engine build — it is a client.

## Usage

```js
import { EpistemicGraphThinClient } from "@epistemic-graph/thin-client";

const c = new EpistemicGraphThinClient({
  socketPath: process.env.GRAPH_SERVICE_SOCKET,
  authSecret: process.env.GRAPH_SERVICE_AUTH_SECRET,
  graph: "agent:planner",
  verifiedContext: {
    principal: "service:graph-client",
    tenant: "tenant:default",
    audience: "epistemic-graph",
    agent_id: "service:graph-client",
    roles: ["graph-client"],
    scopes: ["graph:read", "graph:write"],
    policy_version: "policy:current",
    delegation: [],
  },
});
await c.connect();

// Broker
await c.declareExchange("events", "topic");
await c.declareQueue("orders", { dlExchange: "dlx", maxDeliveryCount: 3 });
await c.bindQueue("events", "orders", "user.*");
const delivered = await c.publish("events", "user.signup", new Uint8Array([1, 2, 3]));

const now = Date.now();
const msg = await c.brokerConsume("orders", { group: "g", consumer: "c1", nowMs: now });
if (msg) {
  const [nodeId] = msg;
  await c.brokerAck("orders", nodeId);
}

// Streams (replay by offset)
await c.streamDeclare("audit", { maxMessages: 1000 });
const off = await c.streamPublish("audit", new Uint8Array([9]), now);
const back = await c.streamRead("audit", { fromOffset: 0, max: 10 });

// RBAC
await c.rbacAddRole("reader");
await c.rbacAddGrant("reader", { Graph: "agent:planner" }, "Read", "Allow");
const policy = await c.rbacList();

// Ops / NL
const report = await c.backup("scheduled-001", "scheduled"); // logical name under configured backup root
const rows = await c.nlQuery("all agents that cite paper X", "agent:planner");

c.close();
```

Fresh stores use the same strict transport. Construct the client with a context
whose only scope is `security:bootstrap`, empty roles/delegation, and explicitly
matching principal, agent, target agent, and signer id; then call
`bootstrapSystemIdentity({ agentId, signerId, signerKey })`. Load `signerKey` from
the deployment secret provider. It is used only for the call and is never sent or
retained. `registerIdentity` and `applyMultisigMutation` use the same detached
canonical-operation signature.

Requires a server built with the matching features: `broker` (broker + streams),
`security` (RBAC), redb (backup/restore), `nl-query` + a configured planner (`nlQuery`).
A build without one returns a clear "not available in this build" error.

## Status

Static context, signing, and secret-leakage tests use Node's built-in test runner.
The repository does not yet provide a live Node engine harness for transport-level
integration tests.
