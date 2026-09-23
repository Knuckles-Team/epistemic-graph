# Engine modes & the auto-bundle

`epistemic-graph` is reachable three ways, resolved by **one** precedence so every entrypoint provisions
an engine identically with no per-entrypoint code. In `agent-utilities` this is the `EngineResolver`
(CONCEPT:AU-OS.deployment.engine-resolver-auto-provision); every entrypoint — the graph-os MCP server, the gateway/host daemon, the facade, the
tenant engine pool, messaging, agent/serving — funnels through it.

```
remote  ->  shared-local  ->  autostart
```

For the on-disk durability story see [the engine architecture](architecture/engine.md); for the
service protocol see [Service Mode](service_mode.md).

---

## The resolution decision flow

<div class="admonition architecture" markdown>
<p class="admonition-title">Engine-mode resolution</p>

When a process needs an engine, it first checks whether Agent Utilities'
`GRAPH_SERVICE_ENDPOINTS` configures an endpoint. If so and it's reachable,
mode is **remote**: connect to it, never autostart. If configured but
unreachable, fail loud rather than silently spawning a divergent local
engine. If nothing is configured, probe whether a local endpoint is already
serving (a cheap connect probe, or a verified spawn-lock holder); if so,
mode is **shared**: reuse it, spawn nothing. If not, acquire the per-socket
`engine_spawn_guard` (first-one-wins `flock`) and double-check whether a
peer just started one — if so, share it; if not, mode is **autostart**:
spawn a detached, supervised engine. Every path ends in connect.

</div>

- **remote** — an endpoint is configured (e.g. the engine runs in Docker on another host). The resolver
  returns it and **never autostarts**; an unreachable configured remote stays fail-loud rather than
  silently spawning a divergent local engine.
- **shared-local** — the default/local endpoint is already serving (a cheap connect probe succeeds, or a
  recorded spawn-lock holder is verified by a probe). Reuse it. This is how co-located entrypoints on
  one host share the **one** engine.
- **autostart** — nothing reachable. Under a per-socket first-one-wins `flock`, a double-checked probe
  re-shares a peer's just-started engine; otherwise spawn a **detached, supervised** engine. Detached =
  it survives the spawning process, so other entrypoints share it. Supervised = reference-counted idle
  shutdown.

---

## The auto-bundle: a supervised, idle-shutting engine

Autostart launches the packaged main binary against the configured durable store.
It is the same engine used for an explicitly managed single-node service. Two
lifecycle behaviours make sharing safe:

- **Reference-counted graceful shutdown** (CONCEPT:EG-KG.backend.tiny-shared). The accept loop selects the next
  connection against a `ShutdownCoordinator` (an active-connection refcount + a `Notify`). With
  `--idle-shutdown-secs N` (`EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS`), the engine self-terminates cleanly
  once the refcount has been zero for `N` seconds. So the auto-bundled daemon
  vanishes after its last client disconnects (robust to client crashes).
- **Persistent lifecycle.** Absent or `0` ⇒ the engine never idle-terminates: it runs forever like a
  normal service. SIGTERM/SIGINT drains cleanly in **both** modes. Commit-before-ack means a stop never
  drops an acknowledged write and requires no final checkpoint.

<div class="admonition architecture" markdown>
<p class="admonition-title">Autostart lifecycle states</p>

An autostarted engine begins **Starting** (under the spawn-guard), moves to
**Serving** once it binds the socket and accepts connections, and stays
there as clients connect/disconnect (refcount changes). If
`idle-shutdown-secs > 0` and the refcount reaches 0, it moves to
**IdleWatch**: a reconnect returns it to Serving, or it stays idle for N
seconds and moves to **Draining**. Serving also moves directly to Draining
on SIGTERM/SIGINT. Draining ends in a clean exit. With a persistent
lifecycle (`idle-shutdown-secs` = 0 or unset), the engine never enters
IdleWatch at all — it runs forever like a normal service.

</div>

---

## Embedded in-process (the edge path)

For a Pi or a single-process deployment that wants **no** Tokio server, socket, or HMAC at all, the
`embedded` feature gives an `EmbeddedEngine` handle (CONCEPT:EG-KG.backend.engine-modes) that owns a `GraphRegistry` + the
redb durable store directly and exposes core ops as plain method calls — SQLite/DuckDB-style: open a
persist dir, call ops. It drives the **same** `GraphCore` + redb-authoritative durable rows the socket
dispatch does (via the canonical mutation applier + authoritative `redb_store`) — one core, two transports. This
is the "100M agents, a local engine each" path: `--features "embedded redb"` builds with no Tokio
runtime. Gated query/tsdb/rdf surfaces light up when those features are also compiled.

The release wheel also contains `epistemic_graph.engine`, the Python in-process
binding. Its explicit `persist_dir=":memory:"` mode exposes graph creation and
node operations without starting a service:

```python
import msgpack
from epistemic_graph.engine import Engine

engine = Engine(persist_dir=":memory:")
engine.create_graph("demo")
engine.add_node("demo", "node:a", msgpack.packb({"kind": "Example"}, use_bin_type=True))
print(engine.node_count("demo"))
```

This Python mode is ephemeral and process-local. Durable Python applications
use the authenticated client against the served engine; the
[standalone deployment guide](standalone_deployment.md) carries that complete
configuration.

---

## Which mode am I in?

| Symptom | Mode |
|---------|------|
| Agent Utilities `GRAPH_SERVICE_ENDPOINTS` contains a reachable endpoint | **remote** (or shared, if local) |
| Several processes on one host, one `epistemic-graph-server` PID | **shared-local** |
| First process on a host, an engine appears under the socket | **autostart** (detached, supervised) |
| No socket, calls go straight to the library | **embedded** |

`GRAPH_SERVICE_ENDPOINTS` is the sole Agent Utilities client-topology selector.
`GRAPH_SERVICE_TCP_ADDR` and `GRAPH_SERVICE_SOCKET` configure Epistemic Graph
server listeners (and may be passed explicitly to the native client); they do
not select an Agent Utilities resolver mode. See [Service Mode](service_mode.md)
for server variables and [Deployment](deployment.md) for the standalone-container
path the `remote` mode connects to.
