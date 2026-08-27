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
| Engine | node **ENGINE-NODE** (192.0.2.10) | pinned `node.labels.name == ENGINE-NODE`; binary bind-mounted from `/home/app/epistemic-graph-server-eg024`; persist `/home/app/epistemic-graph/graph_snapshots` (redb K=4); serves TCP `:9100` |
| graph-os (MCP) | node **MCP-NODE** (192.0.2.12) | dials the engine at `GRAPH_SERVICE_TCP_ADDR=192.0.2.10:9100` |
| messaging / host daemon | MCP-NODE | same — TCP clients of the ENGINE-NODE engine |
| swarm manager | **MANAGER-NODE** (192.0.2.13) | all `docker service …` run here; other nodes reached via a manager SSH hop |

`promote_engine.sh` assumed the engine is bind-mounted on the **local build host** and only
force-restarted by service name. In split-storage that means it installs a binary the ENGINE-NODE
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
So a client can resolve/​autostart that **ephemeral empty local engine instead of the real ENGINE-NODE KG**,
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
ENGINE_ENDPOINT=tcp://192.0.2.10:9100     # highest-precedence endpoint (beats any stray socket)
EPISTEMIC_GRAPH_AUTOSTART=0              # belt-and-suspenders: the tiny-daemon can never spawn
```

And on the client services (`services/graph-os/compose*.yml`, messaging, host-daemon):
- **Drop the `/run/epistemic-graph:/run/epistemic-graph` bind mount** — a client node has no local
  engine socket to share; mounting it only creates the stale-socket surface.
- **Remove `GRAPH_SERVICE_SOCKET`** from the node-local `~/.config/agent-utilities/config.json` on
  client nodes (or leave it — `ENGINE_ENDPOINT` overrides it, but removing it is cleaner).

### The REAL root cause — node-local `config.json` on a client node

The `split-storage` flavor injects `GRAPH_SERVICE_TCP_ADDR` into the swarm **service env**, but the
**node-local `~/.config/agent-utilities/config.json`** (read by BOTH the swarm containers via the
ro-mount AND any locally-spawned graph-os, e.g. a Claude session's `mcpServers.graph-os`) was left
in the co-located-UDS default:

```jsonc
// WRONG for a client node:
"GRAPH_SERVICE_SOCKET": "/run/epistemic-graph/epistemic-graph.sock",
"EPISTEMIC_GRAPH_AUTOSTART": "1",         // explicitly autostart a local engine
// (no GRAPH_SERVICE_TCP_ADDR / engine endpoint at all)
```

So every process that reads only the config.json (not the service env) resolves a **local engine and
autostarts the flapping tiny-daemon**. The swarm containers escaped it only because their service env
added the TCP addr. **Fix the node config.json on every CLIENT node** (this is the durable, one-place fix):

```jsonc
"GRAPH_SERVICE_TCP_ADDR": "192.0.2.10:9100",
"ENGINE_MODE": "remote",
"ENGINE_ENDPOINT": "tcp://192.0.2.10:9100",
"EPISTEMIC_GRAPH_AUTOSTART": "0"
// remove GRAPH_SERVICE_SOCKET
```

A locally-spawned graph-os (e.g. an agent session) must be **restarted / MCP-reconnected** to pick up
the change — a long-running process caches the resolution at start.

### Editable installs MUST include the compiled numeric kernel (the crash-on-restart trap)

The containers run the **latest source** via `PYTHONPATH=/au:/eg` layered over **old, non-editable**
pip wheels (`epistemic-graph 0.31.0`, `agent-utilities 0.51.0`). `/au` main now hard-requires the
compiled **`eg-numeric` kernel** (`agent_utilities.numeric`, no numpy fallback), which ships as
`epistemic_graph/numeric.abi3.so` **inside the `epistemic-graph[numeric]` wheel** — NOT in the `/eg`
source tree. Because `/eg` source shadows `epistemic_graph.*`, the compiled submodule can never
resolve, so `gateway/daemon.py` (host-daemon) crash-loops on `ImportError: epistemic-graph kernel
required` — but only on **restart** (a long-running process imported once, before the cutover).

**Provision the kernel into the editable tree** (idempotent; `*.so` is gitignored):

```bash
pip download --no-deps 'epistemic-graph==<engine-version>'
python3 -c "import zipfile,glob; z=zipfile.ZipFile(glob.glob('epistemic_graph-*.whl')[0]); z.extract('epistemic_graph/numeric.abi3.so', '<repo>/epistemic-graph')"
# → <repo>/epistemic-graph/epistemic_graph/numeric.abi3.so  (visible to every container mounting /eg)
python3 -c "import epistemic_graph.numeric as n; assert n.__kernel__=='eg-numeric'"
```

The clean long-term fix is to make these **true editable installs with extras** — `pip install -e
/au` and either `pip install epistemic-graph[numeric]` (kernel-bearing wheel, no `/eg` shadow) or a
maturin build of `/eg` that lands the `.so` — so "latest editable" actually includes the built
artifacts. Until then, `transition_deploy.sh --preflight` verifies the kernel is importable before it
restarts anything.

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
  --env-add ENGINE_ENDPOINT=tcp://192.0.2.10:9100 \
  --env-add EPISTEMIC_GRAPH_AUTOSTART=0 \
  graph-os_graph-os
# then verify the served path answers a real verb (not just /health), and confirm no
# `/home/app/workspace/.venv/bin/epistemic-graph-server --idle-shutdown-secs` tiny-daemon
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
| `Connection refused` / `after auto-start: No such file` on real verbs, `/health` 200 | client resolving/​autostarting a local tiny-daemon | fix node `config.json` (remote TCP + `AUTOSTART=0`) AND client service env; restart the client |
| a locally-spawned graph-os (agent session) still fails after the fix | long-running process cached the old resolution | MCP-reconnect / restart that process — config is read at start |
| host-daemon crash-loops `ImportError: epistemic-graph kernel required` on restart | compiled `numeric.abi3.so` absent from the source-mounted `/eg`; `/au` hard-requires it | drop the wheel's `epistemic_graph/numeric.abi3.so` into `/eg` (see above); `--preflight` catches it |
| `[Errno 24] Too many open files` | no `nofile` on client services | add `ulimit -Sn 524288` inline |
| multiple `graph-os_graph-os.1.*` containers, intermittent breaker-open | orphan accumulation from `docker restart` of a swarm task | prefer `docker service update --force`; `docker rm -f` all but the newest |
| engine restart hangs binding socket on a large store | first-boot `.mp→redb` migration | already redb here (no migration); otherwise raise `--health-start-period` |

## See also

- `scripts/transition_deploy.sh` — the implementation.
- `scripts/promote_engine.sh` — the older single-node script (kept for the co-located UDS topology).
- `services/epistemic-graph/flavors/split-storage.env` — the live flavor.
- Engine resolution: agent-utilities `knowledge_graph/core/{shard_topology,engine_resolver}.py`.
</content>
