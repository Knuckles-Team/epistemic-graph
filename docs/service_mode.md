# Epistemic Graph: Service Mode Reference

## Quick Start

```bash
# 1. Build the server binary (the binary requires the `server` feature)
cd epistemic-graph
cargo build --release --features server

# 2. Start the service (an auth secret is REQUIRED — see Authentication Protocol)
./target/release/epistemic-graph-server \
  --socket-path /tmp/epistemic-graph.sock \
  --auth-secret my-secret

# 3. Use from Python
export GRAPH_SERVICE_SOCKET=/tmp/epistemic-graph.sock
export GRAPH_SERVICE_AUTH_SECRET=my-secret
python -c "
from epistemic_graph import EpistemicGraphClient
import asyncio

async def main():
    client = await EpistemicGraphClient.connect()
    await client.nodes.add('agent:planner', {'type': 'Agent'})
    print(await client.nodes.has('agent:planner'))  # True
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
| `--socket-path` | `GRAPH_SERVICE_SOCKET` | `/tmp/epistemic-graph.sock` | UDS socket path |
| `--tcp-addr` | — | None | Optional TCP listener (e.g., `0.0.0.0:9100`) |
| `--auth-secret` | `GRAPH_SERVICE_AUTH_SECRET` | — (**required**) | HMAC-SHA256 secret; an empty secret refuses to start unless the insecure opt-out is set |
| `--allow-insecure` | `EPISTEMIC_GRAPH_ALLOW_INSECURE` | off | Explicit opt-out: start with an empty secret (unauthenticated, development only) |
| `--persist-dir` | `GRAPH_SERVICE_PERSIST_DIR` | None | Checkpoint directory |
| `--checkpoint-interval` | — | `300` | Auto-checkpoint interval (seconds) |
| `--persist-on-shutdown` | — | `true` | Serialize on SIGTERM |

## Snapshot persistence

When `--persist-dir` is set the service keeps a fast, RDB-style on-disk snapshot of
every in-memory graph so state survives a restart without an external database
(`src/persist.rs`, server-feature-gated):

- **Format.** Each graph is serialized with `GraphCore::to_msgpack` to
  `{persist_dir}/{sanitized-name}.mp` (compact MessagePack — small on disk, fast to
  write), alongside a `manifest.json` listing the graphs and their files.
- **Atomicity.** Each snapshot is written to a temp file and `rename`d into place, so
  a crash mid-write never corrupts the previous good snapshot.
- **Triggers.** `checkpoint_all(state)` runs (1) on an interval timer every
  `--checkpoint-interval` seconds, (2) on the `Checkpoint` RPC (returns
  `checkpoint_complete:{n}`), and (3) on graceful shutdown when
  `--persist-on-shutdown` is true.
- **Recovery.** On startup `load_all(state)` reads the manifest and rehydrates every
  graph before the listener accepts connections, so clients reconnect to a warm graph.

This is the durable-backup path for the singleton host daemon: cheap enough to run on
a short interval, and bounded by the live graph size (no unbounded WAL growth).

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
    "graph": "agent:planner",                 # target graph name
    "auth_token": "hex-encoded-hmac-sha256",  # see Authentication Protocol
    "agent_id": "planner",                    # OPTIONAL caller identity (ACLs)
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

`agent_id` is optional and backward-compatible: clients that omit it are
treated as anonymous for ACL purposes (see Isolation Policy below).

### Response Shape

A MessagePack map with `id` plus exactly one of `result` / `error`:

```python
{"id": 1, "result": "ok"}                          # success
{"id": 1, "error": "Graph 'unknown' not found"}    # failure
{"id": 7, "error": "ACCESS_DENIED: agent 'worker2' lacks Write access to graph 'agent:worker1'"}
```

## Authentication Protocol

1. Client and server share a secret (`--auth-secret` / `GRAPH_SERVICE_AUTH_SECRET`)
2. For each request, client computes: `HMAC-SHA256(secret, str(request_id))`
3. Token is sent as `auth_token` field in the request
4. Server recomputes and compares; rejects on mismatch (`"Authentication failed"`)
5. **A secret is mandatory.** With an empty secret the server **refuses to
   start** (exit code 2). To intentionally run unauthenticated — development
   only — pass `--allow-insecure` or set `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`;
   the server then starts but logs a prominent `SECURITY:` warning naming the
   bind addresses. This applies to UDS and TCP alike; never combine the
   insecure opt-out with a non-loopback `--tcp-addr`. Note the TCP transport
   is also unencrypted (no TLS) — for cross-host deployments put it behind a
   TLS-terminating or WireGuard/SSH tunnel.

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

How it activates:

- **No identities registered → no rules → nothing is checked.** A
  single-tenant deployment that never calls `RegisterIdentity` behaves exactly
  as before (full back-compat).
- The first `RegisterIdentity` (Python: `client.consensus.register_identity`)
  switches the server to enforcing mode.
- The caller is identified by the optional `agent_id` request field (Python:
  `EpistemicGraphClient.connect(..., agent_id="worker1")`; also accepted by
  `ConnectionPool` / `ShardRouter`). Requests without it are anonymous:
  in enforcing mode they can still use `__bus__` and read `global:` graphs,
  but are denied on `agent:`/`team:` graphs.
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
| Anonymous (no `agent_id`) | `agent:` / `team:` | ❌ | ❌ |

## Building & Running

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with tracing
RUST_LOG=info ./target/release/epistemic-graph-server --socket-path /tmp/eg.sock
```
