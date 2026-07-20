---
name: epistemic-graph-troubleshooting
skill_type: skill
description: >
  Diagnose and recover a live epistemic-graph engine (the AI-native database):
  engine down / socket refused, host-daemon crash-loop, circuit-breaker open,
  backend-type errors, zombie containers, duplicate Swarm tasks, slow first boot.
  Use when the KG is unreachable, ingestion stalls, go__ tools error, or a deploy
  left the engine unhealthy.
domain: operations
license: MIT
tags: [epistemic-graph, database, operations, troubleshooting, swarm, docker]
metadata:
  author: Genius
  version: '0.1.0'
---

# epistemic-graph troubleshooting

A runbook for an AI agent operating the engine natively. The engine is a Rust service
on a Unix socket, run as a Docker Swarm service; Python `graph-os` (MCP) + `graph-os-host`
(daemon) + `messaging` are **clients** of it. Diagnose top-down: engine → socket → clients.

## First, triage

```bash
docker ps -a --format '{{.Names}}\t{{.Status}}' | grep -E 'epistemic-graph|graph-os|messaging'
ls -la /run/epistemic-graph/epistemic-graph.sock          # socket present?
```

A healthy system = one `epistemic-graph_epistemic-graph.* (healthy)`, one
`graph-os_graph-os.* (healthy)`, one `graph-os_graph-os-host.* (healthy)`. More than one of
any (in the same `.N` slot) is a duplicate — see below.

## Symptom → cause → fix

### Engine container down / restart-looping
```bash
cid=$(docker ps -aq -f name=epistemic-graph_epistemic-graph | head -1)
docker logs "$cid" 2>&1 | tail -30
```
- `Listening on UDS` not yet printed but lock acquired → **slow first boot**: the
  snapshot / `.mp`→redb migration binds the socket only when done (can take minutes on a
  large KG). Wait; do **not** kill it. (Deploys: use `promote_engine.sh --migrate`.)
- `docker restart` fails with **"PID … is zombie and can not be killed"** → the old process
  didn't reap children. Use `docker rm -f <cid>`; Swarm reschedules a fresh task.
- Exit 139 (SIGSEGV) / 137 (OOM-kill) repeatedly → a bad binary or memory pressure.
  Roll back the binary (see `epistemic-graph-migrations`) and check host memory.

### `ConnectionRefusedError` / `[Errno 111]` from a client
The socket file may exist but nothing is listening (engine mid-startup) **or** the engine
restarted and the client hasn't reconnected. Confirm the engine printed `Listening on UDS`,
then let the client's circuit breaker re-probe (below). The socket lives at
`/run/epistemic-graph/epistemic-graph.sock` (shared bind mount).

### Host daemon crash-loops with "Unknown graph backend type: '…'" / "A persistent graph backend is required"
The deployment's `GRAPH_BACKEND` env names a removed/unknown backend. `tiered` is
auto-migrated forward to `epistemic_graph` (a warning, not a crash) as of the
engine-authority consolidation; any other unknown value still fails. Set the consumer
service env to `epistemic_graph` (self-contained authority) or `fanout` (+ mirrors) on the
**manager** (`docker service update --env-add GRAPH_BACKEND=fanout <svc>`). `config.json`
**cannot** override a baked container env (injection is `setdefault`).

### Circuit breaker open (ingestion stalls, "engine breaker HALF-OPEN/OPEN")
The client tripped its breaker after engine failures (CONCEPT:AU-OS.observability.no-op-without-metrics). Once the engine is
healthy it auto-probes `open → half_open → closed`. Confirm in the host log:
```bash
docker logs <host-daemon> 2>&1 | grep -iE 'breaker|Connected to epistemic' | tail
```
If it stays open, the engine isn't actually serving — go back to the engine container.

### Duplicate tasks / zombie after an out-of-band restart
`docker restart`/`rm -f` of a Swarm task on a **worker** makes the manager think the task
died and reschedule → briefly two tasks in the slot. Single-socket services: the second
fails to bind and is reaped. The host daemon: two instances contend on the host-lock until
one wins. Resolve to exactly one healthy task:
```bash
docker ps -aq -f name=graph-os_graph-os-host -f status=exited | xargs -r docker rm -f
docker ps -f name=epistemic-graph_epistemic-graph -f health=healthy   # want exactly 1
```
Prefer cycling via the **manager** (`docker service update --force`) — no duplicate churn.

## Confirm recovery (live UQL smoke)
```python
from epistemic_graph.client import EpistemicGraphClient
context = {
    "principal": "ops:troubleshoot", "tenant": "__commons__", "audience": "epistemic-graph",
    "agent_id": "ops:troubleshoot", "roles": ["graph-client"], "scopes": ["kg:read"],
    "policy_version": "policy:initial", "delegation": [],
}
c = await EpistemicGraphClient.connect(
    socket_path="/run/epistemic-graph/epistemic-graph.sock", graph_name="__commons__",
    verified_context=context)
print(await c.query.uql("MATCH (:Concept) |> LIMIT 1"))   # engine serves queries
```
For the full surface smoke (temporal / rerank), see `scripts/promote_engine.sh --verify`
and [UQL](../../../docs/uql.md).

## Escalate to
- `epistemic-graph-deploy` — to (re)promote a binary or restart services cleanly.
- `epistemic-graph-migrations` — for binary flips, snapshot/redb migrations, rollback.
- `docs/deploy/binary-promotion.md` — the full deploy runbook + rationale.
