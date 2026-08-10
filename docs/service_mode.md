# Epistemic Graph: Service Mode Reference

## Quick Start

```bash
# 1. Build the standard server; `full` includes required `server` + `security`.
cargo build --release

# 2. Load secrets/policy from deployment configuration and start durably.
: "${GRAPH_SERVICE_AUTH_SECRET:?required}"
: "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
: "${GRAPH_SERVICE_PERSIST_DIR:?required}"
: "${GRAPH_SERVICE_SOCKET:?required}"
export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
export EPISTEMIC_GRAPH_TENANT=tenant:default
export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial
epistemic-graph-server

# 3. Use from Python
python -c "
from epistemic_graph import EpistemicGraphClient
import asyncio

async def main():
    context = {
        'principal': 'service:client',
        'tenant': 'tenant:default',
        'audience': 'epistemic-graph',
        'agent_id': 'service:client',
        'roles': ['graph-client'],
        'scopes': ['kg:read', 'kg:write'],
        'policy_version': 'policy:initial',
        'delegation': [],
    }
    client = await EpistemicGraphClient.connect(verified_context=context)
    await client.nodes.add('node:example', {'type': 'Entity'})
    print(await client.nodes.has('node:example'))
    await client.close()

asyncio.run(main())
"
```

## CLI Reference

```bash
# Start the service (background daemon)
epistemic-graph-service start [--socket-path PATH] [--tcp-addr HOST:PORT] [--auth-secret SECRET]

# Stop the service (SIGTERM to PID)
epistemic-graph-service stop

# Check if service is running
epistemic-graph-service status

# Send a ping
epistemic-graph-service ping

# List registered graphs
epistemic-graph-service graphs list
```

## Server Binary Arguments

| Argument | Env Var | Default | Description |
|---|---|---|---|
| `--socket-path` | `GRAPH_SERVICE_SOCKET` | platform runtime socket | UDS socket path |
| `--socket-mode` | `GRAPH_SERVICE_SOCKET_MODE` | `0600` | Octal mode applied to the UDS socket after bind; refused at startup if malformed or if it would grant world ("other") access |
| `--tcp-addr` | `GRAPH_SERVICE_TCP_ADDR` | None | Optional native TCP listener; a routable address requires TLS |
| `--tcp-tls-cert` / `--tcp-tls-key` | `GRAPH_SERVICE_TLS_CERT` / `GRAPH_SERVICE_TLS_KEY` | — | PEM identity required together for routable native TCP |
| `--tcp-tls-client-ca` | `GRAPH_SERVICE_TLS_CLIENT_CA` | — | Optional CA bundle enabling required client certificates |
| `--auth-secret` | `GRAPH_SERVICE_AUTH_SECRET` | — (**required**) | Non-empty HMAC-SHA256 secret for `eg2.` envelopes |
| — | `EPISTEMIC_GRAPH_REQUIRE_OIDC` | unset ⇒ **required** (secure by default since 2026-07-22) | OIDC identity binding is mandatory unless explicitly opted out (`false`/`0`/`no`/`off`); see [deployment.md § Migrating to OIDC-required](deployment.md#migrating-to-oidc-required) |
| — | `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` / `EPISTEMIC_GRAPH_OIDC_JWT_AUDIENCE` / `EPISTEMIC_GRAPH_OIDC_JWKS_URL` | — (required unless opted out) | Keycloak realm issuer / audience / JWKS URL |
| `--persist-dir` | `GRAPH_SERVICE_PERSIST_DIR` | — (**required for served mode**) | Durable store and replay-ledger directory |
| `--metrics-addr` | `GRAPH_SERVICE_METRICS_ADDR` | None (disabled) | Prometheus `/metrics` HTTP listener (e.g. `127.0.0.1:9101`) |

Every auxiliary listener is loopback-only. A bare enable token or port resolves
to loopback, and a non-loopback auxiliary address is rejected at startup.

## Prometheus metrics

With the `metrics` cargo feature (on by default) and `--metrics-addr` /
`GRAPH_SERVICE_METRICS_ADDR` set, the server exposes Prometheus text-format
metrics over a minimal HTTP listener (`src/metrics.rs`, CONCEPT:EG-KG.txn.per-graph-write-isolation). The
listener is completely separate from the MessagePack RPC transports, serves
the full registry on any path, and stays disabled unless an address is
configured — so multiple shards never collide on a default port. Recommended
bind: `127.0.0.1:9101` (one port per shard).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `epistemic_graph_requests_total` | counter | `op` | Requests dispatched, per protocol operation |
| `epistemic_graph_request_duration_seconds` | histogram | `op` | Dispatch latency (100 µs – 30 s buckets) |
| `epistemic_graph_in_flight_requests` | gauge | — | Requests currently holding an admission permit |
| `epistemic_graph_inflight_permits_available` | gauge | — | Admission-semaphore permits remaining. Admission is try-acquire — nothing queues; excess is shed as `BUSY` (so there is no "waiting" series) |
| `epistemic_graph_busy_rejections_total` | counter | — | Requests shed with `BUSY` |
| `epistemic_graph_graph_ops_total` | counter | `graph` | Graph-targeted ops admitted past the ACL |
| `epistemic_graph_graph_nodes` / `_edges` | gauge | `graph` | Per-graph size, refreshed on mutation (O(1) counts) |
| `epistemic_graph_auth_failures_total` | counter | — | HMAC authentication rejections |
| `epistemic_graph_access_denied_total` | counter | — | Isolation-ACL denials |

The `graph` label is capped at 128 distinct names; graphs beyond the cap
aggregate under `__overflow__` (deleting a graph frees its slot), so an
unbounded tenant namespace cannot explode time-series cardinality.

## Durable persistence

Served mode requires `GRAPH_SERVICE_PERSIST_DIR`. The redb store is authoritative:
each accepted mutation is committed before acknowledgement and restart recovery reads
that same store. There is no in-memory-only served profile, alternate snapshot format,
checkpoint RPC, or write-behind durability mode.

## Wire Protocol

Communication uses **length-prefixed MessagePack framing** (see
`src/server.rs::handle_connection` and `src/protocol.rs`), *not* JSON or
newline delimiting. Each message — in both directions — is:

```
┌────────────────────────┬──────────────────────────────┐
│ 4-byte big-endian u32  │ MessagePack-encoded body     │
│ (body length in bytes) │ (a map; see shapes below)    │
└────────────────────────┴──────────────────────────────┘
```

Because the frame length is explicit, binary payloads containing `0x0A`
(newline) bytes round-trip intact — newline framing would corrupt them.

### Request Shape

The body is a MessagePack map with this structure (shown as Python, which is
exactly what `epistemic_graph/client.py::_send` packs):

```python
import msgpack

request = {
    "id": 1,                                  # u64 correlation id
    "graph": "graph:example",                 # target graph name
    "auth_token": "eg2.<claims>.<hmac>",       # see Authentication Protocol
    "agent_id": "service:client",              # must match verified authority
    "method": "AddNode",
    "params": {
        "node_id": "n1",
        # node/edge properties travel as nested MessagePack bytes
        "properties_msgpack": msgpack.packb({"type": "Agent"}),
    },
}
body = msgpack.packb(request)
frame = len(body).to_bytes(4, byteorder="big") + body
```

`agent_id` is an authenticated assertion, never authority by itself. The server
derives the effective actor from the verified envelope and rejects a conflict.

### Response Shape

A MessagePack map with `id` plus exactly one of `result` / `error`:

```python
{"id": 1, "result": "ok"}                          # success
{"id": 1, "error": "Graph 'unknown' not found"}    # failure
{"id": 7, "error": "ACCESS_DENIED: agent 'worker2' lacks Write access to graph 'agent:worker1'"}
```

## Postgres wire transactions (CONCEPT:EG-KG.compute.kg-transaction-is-pinned)

When built `--features pgwire` and bound via `EPISTEMIC_GRAPH_PGWIRE_ADDR`, the
Postgres wire shim supports `BEGIN` / `COMMIT` / `ROLLBACK` over a **mixed-store**
transaction that buffers BOTH graph-node DML (`INSERT`/`UPDATE`/`DELETE` over
`nodes`, including the `… SELECT` / `… FROM` join forms) and user-table DDL/DML,
applying them at `COMMIT`.

- **Read-your-own-writes** — a `SELECT` inside an open transaction sees the
  transaction's own uncommitted node writes (the read runs over the live snapshot
  overlaid with the buffered ops).
- **Aborted transactions** — after any statement inside the block errors, every
  later statement except `COMMIT`/`ROLLBACK` is rejected with SQLSTATE `25P02`
  ("current transaction is aborted…"); `COMMIT` while aborted behaves as `ROLLBACK`.
- **One graph per transaction** — the transaction is pinned to the graph selected
  at `BEGIN`; `SET graph` is rejected while a transaction is open (a transaction
  stays within one redb shard, CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
- **`ReadyForQuery` status** — `BEGIN`/`COMMIT`/`ROLLBACK` drive the driver-visible
  `T` (in-transaction) / `E` (failed) / `I` (idle) status correctly.

### COMMIT durability — sequenced, NOT two-phase (2PC)

`COMMIT` is **best-effort ordered across stores**. It (a) replays the buffered
graph-node ops as ONE atomic in-memory batch (a single topology write guard) and
records them as ONE durable group in a single redb `WriteTransaction`, THEN (b) commits the
user-table transaction. **Each store commits atomically within itself, but the two
are sequenced** — there is a narrow partial-failure window: if the graph group
commits and the user-table commit then fails, the graph writes are durable while
the table writes are not (the `COMMIT` returns an error and the block ends). This is
an intentional trade-off (no distributed two-phase commit across the two engines).
A transaction confined to a single store (only node ops, or only table ops) has no
cross-store window and is fully atomic.

## Authentication Protocol

`eg2.` is the only request envelope accepted by `src/server/auth.rs`. It binds
the request id, graph, canonical method/body digest, timestamp, nonce,
idempotency key, authenticated principal, tenant, audience, effective agent,
roles, scopes, policy version, and delegation path under HMAC-SHA256. Tokens are
compared in constant time. Unknown prefixes and malformed, stale, replayed, or
policy-mismatched envelopes fail before dispatch.

The server refuses to open a listener unless all of these are true:

- the binary includes the `security` feature;
- `GRAPH_SERVICE_AUTH_SECRET` is non-empty;
- `EPISTEMIC_GRAPH_AUDIENCE`, `EPISTEMIC_GRAPH_TENANT`, and
  `EPISTEMIC_GRAPH_POLICY_VERSION` are non-empty;
- `GRAPH_SERVICE_PERSIST_DIR` provides the durable replay ledger;
- `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` contains non-empty trusted signer ids and
  keys;
- **since 2026-07-22**, `EPISTEMIC_GRAPH_REQUIRE_OIDC` is not explicitly opted
  out (`false`/`0`/`no`/`off`) AND `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` (plus its
  audience and JWKS URL) is configured — OIDC identity binding is required by
  default; see [deployment.md § Migrating to OIDC-required](deployment.md#migrating-to-oidc-required).

`EPISTEMIC_GRAPH_ENVELOPE_SKEW_SECS` controls the accepted timestamp window and
replay-retention horizon. Nonce acceptance is committed durably before dispatch,
so a process restart cannot make a captured envelope reusable.

### Fresh-store identity bootstrap

An empty durable identity/RBAC store does not grant ordinary graph or admin
access. It permits exactly one bootstrap mutation in `__commons__`: a trusted
signer-backed `RegisterIdentity` request that registers the envelope's own
principal/effective agent as `System`, with empty teams and roles, no delegation,
and the single exact scope `security:bootstrap`. The detached operation signature
must verify against `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` and its signer id must equal
the verified principal. Once the first rule exists, this bootstrap path closes;
all operations, including later identity administration, require normal durable
RBAC and capability checks.

Remote native TCP also requires TLS; the request envelope authenticates and
authorizes a request but does not provide transport confidentiality. Native
federation signs the same `eg2.` contract and fails before dialing when its
claims, secret, or TLS boundary are incomplete.

## Multi-Graph Management

```python
# Create a new agent-scoped graph
await client.tenants.create("agent:researcher", "Agent")

# List all graphs
graphs = await client.tenants.list()
# [{"name": "__bus__", "type": "Bus"}, {"name": "agent:researcher", "type": "Agent"}]

# Target a specific graph for operations
researcher = await EpistemicGraphClient.connect(graph_name="agent:researcher")
await researcher.nodes.add("finding:1", {"content": "..."})
```

## Dynamic Communication Channels

### 1:1 Peer-to-Peer

```python
await client.channels.create(
    channel_id="channel:p2p:planner:researcher",
    channel_type="PeerToPeer",
    creator="agent:planner",
    initial_members=["agent:researcher"],
)
await client.channels.send_message("channel:p2p:planner:researcher", "agent:planner", "Need data on X")
msgs = await client.channels.get_messages("channel:p2p:planner:researcher")
```

### Many-to-Many Group

```python
await client.channels.create(
    channel_id="channel:group:brainstorm-1",
    channel_type="Group",
    creator="agent:planner",
    initial_members=["agent:researcher"],
)
# Other agents can join dynamically
await client.channels.join("channel:group:brainstorm-1", "agent:writer")

# When done, close creates a KG imprint
imprint = await client.channels.close(
    "channel:group:brainstorm-1",
    topic_metadata="Project brainstorm session #1",
)
```

### Channel Lifecycle

```
Create → Join/Leave (dynamic) → Close → KG Imprint
```

On close, the channel records:
- All participant agent IDs (preserved as graph edges)
- Message count and timestamps
- Optional conversation summary embedding
- Topic metadata for future retrieval

## Isolation Policy

ACLs are **enforced in dispatch** (`src/server.rs::check_graph_access` calling
`src/isolation.rs::check_access`) for every graph-targeted operation.
Violations return an `ACCESS_DENIED: ...` error response.

Enforcement is unconditional:

- Every caller comes from verified `eg2.` authority. The unsigned `agent_id`
  request field cannot establish or change identity.
- Native wire session objects may exist before authentication, but they cannot
  execute, enter QoS/admission accounting, evaluate ACLs, or access state until
  a non-empty verified identity and opaque principal scope are bound. There is
  no anonymous, graph-name, or empty-string identity bucket.
- An empty durable identity/RBAC store grants no graph access. It admits only
  the exact signer-backed `security:bootstrap` self-registration described
  above.
- After that first `System` rule commits, normal durable graph ACL, admin RBAC,
  and row-level policy govern every request.
- Unowned, undecodable, and untagged rows are denied unless explicitly public
  or authorized by owner/grant policy.
- `CreateGraph` records the caller as the graph **owner** — ownership is what
  peer-deny and manager-access resolve against. `DeleteGraph` requires Write
  access to the target graph.
- Cross-graph reads (`DiffAgainst`, `Vf2SubgraphMatch`) also check Read access
  on the secondary graph.

| Requester | Target Graph Type | Read | Write |
|---|---|---|---|
| Any agent | `__bus__` | ✅ | ✅ |
| Owner | `agent:<self>` | ✅ | ✅ |
| Peer | `agent:<other>` | ❌ | ❌ |
| Manager | `agent:<subordinate>` | ✅ | ✅ |
| Team member | `team:<name>` | ✅ | ❌ |
| Team manager | `team:<name>` | ✅ | ✅ |
| Any agent | `global:<name>` | ✅ | ❌ |
| Unbound transport (before verified identity) | Any graph | Cannot execute | Cannot execute |

## Building & Running

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with tracing
RUST_LOG=info ./target/release/epistemic-graph-server --socket-path "${GRAPH_SERVICE_SOCKET:?}"
```
