# Engine + graph-os transition runbook (clean, self-verifying deploy)

> How to cleanly transition the epistemic-graph engine to a new build **and** restart
> graph-os/messaging without the roughness that bit us on 2026-07-05. The one-command
> path is `scripts/transition_deploy.sh --build`. This doc explains what it does, the
> config hardening that stops graph-os from silently talking to the wrong engine, and
> rollback/troubleshooting.

## TL;DR

```bash
# From the build host (must share glibc with the engine node — the script checks):
scripts/transition_deploy.sh --build          # discover → build → copy → restart → VERIFY → rollback-on-fail
scripts/transition_deploy.sh --build --dry-run # see exactly what it would do, mutate nothing
```

Then confirm the **served** path (not just /health):
```bash
# a real engine verb through graph-os must return data, not "Connection refused" / "auto-start":
go__graph_query cypher='MATCH (n) RETURN count(n)'      # or the graph_query MCP tool
```

## The topology (why the old script was rough)

The live deployment is the **split-storage** flavor (`services/epistemic-graph/flavors/split-storage.env`):

| Piece | Where | Detail |
|-------|-------|--------|
| Engine | node **R510** (10.0.0.10) | pinned `node.labels.name == R510`; binary bind-mounted from `/home/genius/epistemic-graph-server-eg024`; persist `/home/genius/epistemic-graph/graph_snapshots` (redb K=4); serves TCP `:9100` |
| graph-os (MCP) | node **RW710** (10.0.0.12) | dials the engine at `GRAPH_SERVICE_TCP_ADDR=10.0.0.10:9100` |
| messaging / host daemon | RW710 | same — TCP clients of the R510 engine |
| swarm manager | **R820** (10.0.0.13) | all `docker service …` run here; other nodes reached via a manager SSH hop |

`promote_engine.sh` assumed the engine is bind-mounted on the **local build host** and only
force-restarted by service name. In split-storage that means it installs a binary the R510
task never mmaps (**silent no-op**) and never checks the served path. `transition_deploy.sh`
fixes both: it **discovers** the real node + bind path from `docker service inspect`, copies to
the right node, and **gates on a real engine query** before declaring success.

## What `transition_deploy.sh` does (and the guarantees)

1. **Discover** engine placement (node label → IP, binary bind-mount source, TCP addr) from the
   manager. Nothing hardcoded — relocating the engine needs no script edit.
2. **Build** `--features full`; guard the **finance symbols** (a server-only build silently breaks
   finance/quant); verify the binary's **max required GLIBC ≤ the target node's glibc** (a binary
   built on a newer-glibc box will not run on an older node — fail fast, don't ship a crash-loop).
3. **Copy** to the correct node atomically: stream → `sha256` verify → timestamped `.bak` → `mv -f`.
4. **Restart engine** stop-first with `--health-start-period` (survives a first-boot `.mp→redb`
   migration), watching the target node's logs for the socket bind.
5. **Restart consumers**, wait for each to be Running/healthy.
6. **Served gate + rollback**: run a real `MATCH (n) |> LIMIT 1` against the engine end-to-end.
   On failure it **rolls the binary back to the `.bak`** and restarts — so a bad transition
   self-heals instead of leaving a dead KG.

## ⚠️ The shadowing gotcha — client nodes must be TCP-only

**Symptom:** after a graph-os restart, real engine verbs (`graph_query`, `graph_memory`,
`engine_lifecycle`) return `[Errno 111] Connection refused` or
`Cannot connect … after auto-start: [Errno 2] No such file or directory`, while `/health`
is 200 and a *direct* TCP client to the engine works fine.

**Root cause:** the split-storage flavor adds `GRAPH_SERVICE_TCP_ADDR` for clients but leaves the
**co-located-UDS defaults** in place — graph-os still bind-mounts `/run/epistemic-graph` and the
node-local `config.json` still sets `GRAPH_SERVICE_SOCKET=/run/epistemic-graph/epistemic-graph.sock`.
On the client node that socket path is served (when present) by a **local autostarted "tiny-daemon"
engine** (`--idle-shutdown-secs 60`, separate near-empty store `~/.local/share/agent-utilities/graph_snapshots`).
So a client can resolve/​autostart that **ephemeral empty local engine instead of the real R510 KG**,
and when the tiny-daemon is in its idle-shutdown window you get Connection refused. `graph_kvcache`
hides it (it degrades transport errors to an all-zeros miss).

**Resolution precedence (already correct in code)** — `resolve_endpoints()` ranks
`ENGINE_ENDPOINT` > `GRAPH_SERVICE_ENDPOINTS` > `tcp://GRAPH_SERVICE_TCP_ADDR` > `unix://GRAPH_SERVICE_SOCKET`,
and any `tcp://` endpoint is classified **remote → autostart disabled**. So the fix is purely
**configuration**: make the client resolve TCP unambiguously and forbid the local stand-in.

### Client TCP-only hardening (apply on the client node / in the flavor)

Add to the **client** section of `services/epistemic-graph/flavors/split-storage.env` (threaded
into the graph-os / messaging / host-daemon service env):

```sh
# --- client nodes: TCP-ONLY, never resolve or autostart a local engine ---
ENGINE_MODE=remote                       # forces the remote leg; disables autostart
ENGINE_ENDPOINT=tcp://10.0.0.10:9100     # highest-precedence endpoint (beats any stray socket)
EPISTEMIC_GRAPH_AUTOSTART=0              # belt-and-suspenders: the tiny-daemon can never spawn
```

And on the client services (`services/graph-os/compose*.yml`, messaging, host-daemon):
- **Drop the `/run/epistemic-graph:/run/epistemic-graph` bind mount** — a client node has no local
  engine socket to share; mounting it only creates the stale-socket surface.
- **Remove `GRAPH_SERVICE_SOCKET`** from the node-local `~/.config/agent-utilities/config.json` on
  client nodes (or leave it — `ENGINE_ENDPOINT` overrides it, but removing it is cleaner).

### nofile (the EMFILE fix)

Swarm ignores the compose `ulimits:` key, so the engine raises fds inline (`ulimit -Sn 524288`
before `exec`). The client services set **no** fd limit → the host daemon hit
`[Errno 24] Too many open files`. Wrap each client command the same way, e.g.:

```yaml
command: ["sh","-c","ulimit -Sn 524288; exec python -m agent_utilities.mcp.kg_server"]
```

## Applying the hardening live (needs an operator; these MUTATE production)

The env hardening is reversible (`--env-rm`). On the manager:

```bash
# graph-os (repeat for the host-daemon + messaging services):
docker service update \
  --env-add ENGINE_MODE=remote \
  --env-add ENGINE_ENDPOINT=tcp://10.0.0.10:9100 \
  --env-add EPISTEMIC_GRAPH_AUTOSTART=0 \
  graph-os_graph-os
# then verify the served path answers a real verb (not just /health), and confirm no
# `/home/apps/workspace/.venv/bin/epistemic-graph-server --idle-shutdown-secs` tiny-daemon
# respawns on the client node.
```

## Rollback

- **Binary:** every transition leaves `<bind-path>.bak-<ts>` on the engine node; the script
  auto-rolls-back on a failed served gate. Manual: `mv -f <bind-path>.bak-<ts> <bind-path>` on the
  engine node, then `docker service update --force <engine-service>` on the manager.
- **Client env:** `docker service update --env-rm ENGINE_ENDPOINT --env-rm ENGINE_MODE --env-rm EPISTEMIC_GRAPH_AUTOSTART <svc>`.

## Troubleshooting quick table

| Symptom | Cause | Fix |
|---------|-------|-----|
| deploy "succeeds" but engine unchanged | old script installed to the wrong node/path | use `transition_deploy.sh` (placement-aware) |
| `Connection refused` / `after auto-start: No such file` on real verbs, `/health` 200 | client resolving/​autostarting a local tiny-daemon | apply client TCP-only hardening above |
| `[Errno 24] Too many open files` | no `nofile` on client services | add `ulimit -Sn 524288` inline |
| multiple `graph-os_graph-os.1.*` containers, intermittent breaker-open | orphan accumulation from `docker restart` of a swarm task | prefer `docker service update --force`; `docker rm -f` all but the newest |
| engine restart hangs binding socket on a large store | first-boot `.mp→redb` migration | already redb here (no migration); otherwise raise `--health-start-period` |

## See also

- `scripts/transition_deploy.sh` — the implementation.
- `scripts/promote_engine.sh` — the older single-node script (kept for the co-located UDS topology).
- `services/epistemic-graph/flavors/split-storage.env` — the live flavor.
- Engine resolution: agent-utilities `knowledge_graph/core/{shard_topology,engine_resolver}.py`.
</content>
