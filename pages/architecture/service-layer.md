# Epistemic Graph Service Layer Architecture

> CONCEPT:AU-KG.query.vendor-agnostic-traversal — Tokio-first graph service

## Overview

The epistemic-graph service layer is a long-running Tokio process that holds multiple named graphs in memory and serves requests over Unix Domain Socket (UDS) or TCP. It replaces the previous PyO3 in-process FFI approach as the primary compute backend.

The Python `agent-utilities` API gateway's `GraphComputeEngine` (an async
UDS/TCP client) calls the `epistemic-graph-server` (Tokio, long-running),
which holds the `GraphRegistry` (named graphs), `ChannelManager` (P2P, 1:1,
many:many, bus), `IsolationLayer` (ACL enforcement), and
Checkpoint/Persistence.

## Graph Topology

### Graph Types

| Type | Naming Convention | Access | Purpose |
|---|---|---|---|
| **Bus** | `__bus__` | All agents R/W | Global event broadcast, inter-agent messaging |
| **Agent** | `agent:<id>` | Owner: full, Manager: full, Peers: denied | Private agent knowledge, episode memory |
| **Team** | `team:<name>` | Members: read, Manager: R/W | Shared team context, project knowledge |
| **Global** | `global:<name>` | All: read-only | System ontology, tool registry |

### Isolation Rules

1. **Peer isolation**: Agent graphs are invisible to peer agents
2. **Hierarchical access**: Manager agents have full access to subordinate graphs
3. **Bus is public**: `__bus__` readable/writable by all authenticated agents
4. **Team scoping**: Team graphs are read-only for members, read-write for manager
5. **Global read-only**: Global graphs are system-managed, agent-readable

## Dynamic Communication Channels

Agents can create ephemeral channels for P2P or group communication:

- **1:1 channels**: `channel:p2p:<agent_a>:<agent_b>` — direct messaging
- **Many:many channels**: `channel:group:<uuid>` — group created by any agent
- **Lifecycle**: Create → Join → Leave → Close
- **KG Imprint**: On close, the channel creates a permanent KG record with:
  - Vectorized embedding of the conversation summary
  - Participant edges preserved permanently
  - Topic metadata and timestamps

## Configuration

All settings are available in the XDG `config.json`:

| Field | Env Var | Default | Description |
|---|---|---|---|
| `graph_service_endpoints` | `GRAPH_SERVICE_ENDPOINTS` | `None` | Ordered connect-only coordinator contacts; absence selects the platform-default packaged engine transport |
| `graph_service_auth_secret` | `GRAPH_SERVICE_AUTH_SECRET` | `None` | HMAC-SHA256 shared secret |
| `graph_service_persist_on_shutdown` | `GRAPH_SERVICE_PERSIST_ON_SHUTDOWN` | `true` | Serialize on shutdown |

## Authentication

All connections require HMAC-SHA256 authentication:
- Client computes `HMAC-SHA256(secret, request_id)` and sends it as `auth_token`
- Server verifies the token before processing any request
- For UDS-only deployments, Unix file permissions provide additional isolation
- TCP deployments **require** authentication

## API Gateway Integration

The service lifecycle is tied to the agent-utilities API gateway:
- **Startup**: Gateway sends `Reconcile` to push authoritative state from the backend
- **Shutdown**: Gateway sends `Checkpoint` to persist all graphs
- The service process is the `epistemic-graph-server` Rust daemon (run via
  `cargo run -p epistemic-graph`). With `GRAPH_SERVICE_ENDPOINTS` unset,
  `GraphComputeEngine` shares or provisions the packaged local daemon. Any
  configured coordinator topology requires an existing daemon and is
  connect-only.

## Migration from PyO3

The PyO3 in-process FFI path has been **fully removed**. `GraphComputeEngine`
talks to the `epistemic-graph-server` Tokio daemon **exclusively** over the
out-of-process MessagePack/UDS (or TCP) client (`epistemic_graph.client`); there
is no in-process embedded fallback. Omit `GRAPH_SERVICE_ENDPOINTS` for the
packaged local lifecycle; configure it to connect to an existing authority,
which never starts a local stand-in.

## Sharding (Stage 2)

`GraphComputeEngine` connects to a configured coordinator contact. Under an
authenticated `GraphSession`, each graph request resolves the engine's complete
placement route and validates its epoch and group fence. Multiple contacts are
not a hash ring; ambiguous or unreachable placement fails closed. Autostart applies
only to a sole local endpoint. See
[engine sharding](https://knuckles-team.github.io/graph-os/architecture/engine-sharding/)
(graph-os).
