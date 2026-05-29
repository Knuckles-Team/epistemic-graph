# Epistemic Graph: Service Mode Reference

## Quick Start

```bash
# 1. Build the server binary
cd epistemic-graph
cargo build --release

# 2. Start the service
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
    await client.add_node('agent:planner', {'type': 'Agent'})
    print(await client.has_node('agent:planner'))  # True
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
| `--socket-path` | — | `/tmp/epistemic-graph.sock` | UDS socket path |
| `--tcp-addr` | — | None | Optional TCP listener (e.g., `0.0.0.0:9100`) |
| `--auth-secret` | `GRAPH_SERVICE_AUTH_SECRET` | `""` (disabled) | HMAC-SHA256 secret |
| `--persist-dir` | `GRAPH_SERVICE_PERSIST_DIR` | None | Checkpoint directory |
| `--checkpoint-interval` | — | `300` | Auto-checkpoint interval (seconds) |
| `--persist-on-shutdown` | — | `true` | Serialize on SIGTERM |

## Wire Protocol

Communication uses JSON-over-newline framing. Each line is a complete JSON object.

### Request Format

```json
{
  "id": 1,
  "graph": "agent:planner",
  "auth_token": "hex-encoded-hmac-sha256",
  "method": "AddNode",
  "params": {
    "node_id": "n1",
    "properties_json": "{\"type\": \"Agent\"}"
  }
}
```

### Response Format

```json
{
  "id": 1,
  "result": "ok"
}
```

Error response:

```json
{
  "id": 1,
  "error": "Graph 'unknown' not found"
}
```

## Authentication Protocol

1. Client and server share a secret (`--auth-secret` / `GRAPH_SERVICE_AUTH_SECRET`)
2. For each request, client computes: `HMAC-SHA256(secret, str(request_id))`
3. Token is sent as `auth_token` field in the request
4. Server recomputes and compares; rejects on mismatch
5. Empty secret disables authentication (UDS-only development)

## Multi-Graph Management

```python
# Create a new agent-scoped graph
await client.create_graph("agent:researcher", "Agent")

# List all graphs
graphs = await client.list_graphs()
# [{"name": "__bus__", "type": "Bus"}, {"name": "agent:researcher", "type": "Agent"}]

# Target a specific graph for operations
researcher = await EpistemicGraphClient.connect(graph_name="agent:researcher")
await researcher.add_node("finding:1", {"content": "..."})
```

## Dynamic Communication Channels

### 1:1 Peer-to-Peer

```python
await client.create_channel(
    channel_id="channel:p2p:planner:researcher",
    channel_type="PeerToPeer",
    creator="agent:planner",
    initial_members=["agent:researcher"],
)
await client.send_message("channel:p2p:planner:researcher", "agent:planner", "Need data on X")
msgs = await client.get_channel_messages("channel:p2p:planner:researcher")
```

### Many-to-Many Group

```python
await client.create_channel(
    channel_id="channel:group:brainstorm-1",
    channel_type="Group",
    creator="agent:planner",
    initial_members=["agent:researcher"],
)
# Other agents can join dynamically
await client.join_channel("channel:group:brainstorm-1", "agent:writer")

# When done, close creates a KG imprint
imprint = await client.close_channel(
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

| Requester | Target Graph Type | Read | Write |
|---|---|---|---|
| Any agent | `__bus__` | ✅ | ✅ |
| Owner | `agent:<self>` | ✅ | ✅ |
| Peer | `agent:<other>` | ❌ | ❌ |
| Manager | `agent:<subordinate>` | ✅ | ✅ |
| Team member | `team:<name>` | ✅ | ❌ |
| Team manager | `team:<name>` | ✅ | ✅ |
| Any agent | `global:<name>` | ✅ | ❌ |

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
