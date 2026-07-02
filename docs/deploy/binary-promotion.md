# Engine binary promotion (homelab)

How to ship a new `epistemic-graph-server` binary into the live homelab fleet, and
why each step is what it is. Automated by [`scripts/promote_engine.sh`](https://github.com/Knuckles-Team/epistemic-graph/blob/main/scripts/promote_engine.sh).

> TL;DR: `scripts/promote_engine.sh --build --restart-consumers`

## The deploy model

The engine runs as a Docker **Swarm service** (`epistemic-graph_epistemic-graph`).
Its binary is **bind-mounted from the host**, not baked into the image:

```
/home/apps/workspace/.venv/bin/epistemic-graph-server   (host)
        → /usr/local/bin/epistemic-graph-server          (container, ubuntu:26.04)
```

So promotion is just: **replace the host binary → restart the service** (the new
container re-execs the bind-mounted binary). The KG lives in a separate snapshot
volume (`/data/graph_snapshots`), so it survives the restart.

The Python `epistemic_graph` client and `agent-utilities` are installed **editable**
from their canonical checkouts under `/home/apps/workspace/agent-packages/*`, and the
`graph-os` / `messaging` containers bind-mount that checkout at `/au`. So Python-side
changes ship by **merging to `main` + restarting those services** — no rebuild.

## Steps

1. **Build with `--features full`.** Production needs the finance / quant / datascience /
   reasoning method surface. A `--features server` build *links and runs* but is missing
   those methods, so emerald/quant callers fail at runtime with "Method not available in
   this server build". The script guards this by checking for a finance symbol. **`full`
   now also pulls `redb`** (CONCEPT:KG-2.195), so a stock build is **redb-authoritative
   by default** — a durable source of truth, not a rebuildable cache. The *first* boot of
   a flipped binary runs a one-time `.mp`→redb migration that needs special restart
   handling — see [Authoritative migration (first boot)](#authoritative-migration-first-boot)
   below before promoting the FLIP binary.
   ```
   cargo build --release --features full
   ```
2. **Atomically install the binary.** The running engine has the file open/executing, so a
   direct overwrite risks `ETXTBSY`. Copy to a same-dir temp then `mv` (atomic rename): the
   live process keeps its old inode; the next container start picks up the new one. Keep a
   timestamped `.bak-*` for rollback.
3. **Restart the engine service with `--update-order stop-first`.** The engine binds a single
   UDS socket, so the swarm default **`start-first` FAILS** — the new task can't bind the
   socket the old one still holds and exits non-zero (the rollout then pauses). `stop-first`
   releases the socket first. This node (`RW710`) is a swarm **worker**, so the update runs on
   the **manager** (`R820`):
   ```
   ssh R820 docker service update --update-order stop-first --force epistemic-graph_epistemic-graph
   ```
   There is a brief (~seconds) engine outage; consumers reconnect, the KG reloads from the
   snapshot volume.
4. **Restart consumers** (`--restart-consumers`; required after the FLIP, optional for plain
   Python-side changes). `graph-os` + `messaging` reconnect cleanly to the restarted engine
   and pick up merged `agent-utilities`. **They must run `GRAPH_BACKEND=fanout`** (engine
   authority + optional mirrors). The old **`tiered` value was removed** — a consumer still
   set to it fails bootstrap with *"A persistent graph backend is required"* /
   *"Unknown graph backend type 'tiered'"*. (The compose files
   `services/graph-os/compose*.yml` and `services/agent-utilities-messaging/compose.dev.yml`
   are already `fanout`.)
   ```
   ssh R820 docker service update --update-order stop-first --force graph-os_graph-os
   ssh R820 docker service update --update-order stop-first --force agent-utilities-messaging_agent-utilities-messaging
   ```
5. **Verify the method is live** (not just that the container is healthy):
   ```python
   from epistemic_graph.client import SyncEpistemicGraphClient as C
   c = C.connect(socket_path="/run/epistemic-graph/epistemic-graph.sock")
   print(c.graph.match_ontology_terms("portainer"))   # → [{'term': 'portainer', ...}]
   ```

## Authoritative migration (first boot)

The first time a **redb-authoritative** binary boots against a persist dir that still
holds the old `.mp` snapshot + WAL (but an empty redb store), it runs a **one-time,
idempotent, crash-safe `.mp`→redb migration** (read-old → write-new) **before it binds
its UDS socket**. The old files are left in place as a rollback backstop. This is the
normal, expected path the very first time you promote the FLIP binary.

**The trap: the migration can take minutes, and the healthcheck will kill it.** On a
large KG (thousands of tenant graphs + a multi-hundred-MB `__commons__`) the migration
runs for **minutes**, and the engine does **not** bind its socket until it finishes. The
swarm healthcheck is `test -S <socket>` with `start-period 20s` — so it marks the task
**unhealthy** while the migration is still running and **kills + restarts** it mid-migration.
The migration is crash-safe (it re-detects "redb empty + legacy present" and re-runs), so
this becomes a **restart loop** that never completes.

**Mitigation — restart with an extended `--health-start-period` covering the migration,
then restore it.** Give the healthcheck a grace window long enough for the migration to
finish before it starts probing. `scripts/promote_engine.sh` now does this for you
(CONCEPT:OS-5.62) — prefer the script flags over the manual dance:

```bash
# Migration-aware promotion of the FLIP binary: extend the start-period to cover the
# migration, watch the progress log until the socket binds, then restore the normal
# start-period, then restart consumers.
scripts/promote_engine.sh --build --migrate --restore-health-period --restart-consumers
```

What `--migrate` does, step by step:

1. **Restart the engine with a generous start-period.** `--migrate` implies
   `--health-start-period 600` (override with an explicit `--health-start-period <secs>`,
   or the `MIGRATE_DEFAULT_START_PERIOD` env). The emitted restart is:
   ```
   docker service update --update-order stop-first --health-start-period 600s --force epistemic-graph_epistemic-graph
   ```
2. **Watch it migrate.** The script tails the new engine container's logs **on this node**
   (the service is node-pinned to the bind-mount host) and surfaces the progress lines until
   the socket binds or `MIGRATE_WATCH_TIMEOUT` (default 1800s) elapses:
   ```
   Snapshot load progress: 2000/8123 graph(s) processed (1998 restored)
   redb authoritative migration: imported 8123 graph(s) from legacy snapshot/WAL into redb in 214.3s …
   Listening on UDS: /run/epistemic-graph/epistemic-graph.sock
   ```
   If the engine task is **not** on this node, the tail is skipped with a note telling you
   how to watch it on the bind-mount host (and the start-period is left extended).
3. **Restore the normal start-period (opt-in).** With `--restore-health-period`, once the
   `Listening on UDS` line is seen the script runs a second `service update` restoring
   `--health-start-period 20s` (override via `NORMAL_HEALTH_START_PERIOD`). The second
   restart on the now-populated redb is fast. If the bind couldn't be confirmed (engine not
   on this node, or a timeout), the restore is **skipped** so you don't shrink the window
   mid-migration — restore it manually after confirming the bind.

Progress is logged so you can tell *slow-but-working* from *hung* (CONCEPT:KG-2.200):
`Snapshot load progress: N/total graph(s) processed` during the legacy load, then
`redb authoritative migration: imported N graph(s) … in Xs` when it commits to redb. Then
the script (with `--restart-consumers`) **restarts the consumers** with `GRAPH_BACKEND=fanout`
so they reconnect to the now-authoritative engine.

**Doing it by hand** (e.g. when promoting from a node that isn't the bind-mount host) — the
same two-`service update` dance the flags automate:

```bash
# 1. Restart the engine with a generous start-period (e.g. 600s) for the migration.
ssh R820 docker service update --update-order stop-first \
    --health-start-period 600s --force epistemic-graph_epistemic-graph

# 2. Watch it migrate (it logs progress; the socket binds only when done):
ssh R820 'docker service logs -f epistemic-graph_epistemic-graph 2>&1' | grep -i 'snapshot load\|migration\|imported'

# 3. Once the socket is bound and the migration line printed, restore the normal start-period.
ssh R820 docker service update --health-start-period 20s --force epistemic-graph_epistemic-graph
```

**Rollback during/after migration.** The `.mp`/`.wal` files are left untouched, so rollback
is unchanged: restore the timestamped `.bak-*` binary and restart — the old binary reads the
still-present snapshot + WAL backstop (and is non-authoritative, so it ignores the redb
store entirely).

## Gotchas

- **`--features full`, always.** `server`-only is a silent runtime breakage for finance/quant;
  it also drops `redb`, so the engine is **not** authoritative (a `default`/`server` build
  silently falls back to the snapshot cache).
- **First FLIP boot ⇒ extend `--health-start-period`.** The one-time `.mp`→redb migration runs
  before the socket binds and can take minutes; the 20s healthcheck start-period will kill it
  into a restart loop. Use `promote_engine.sh --migrate [--restore-health-period]` (CONCEPT:OS-5.62),
  or pass `--health-start-period <secs>` directly. See
  [Authoritative migration (first boot)](#authoritative-migration-first-boot).
- **Consumers should be `GRAPH_BACKEND=fanout` (or `epistemic_graph`), not `tiered`.** The `tiered`
  backend was removed. A stale `tiered` value is now **migrated forward to `epistemic_graph`
  at boot with a warning** (so a consumer no longer hard-crashes on it), but the warning fires
  every start — fix the deployment env to `fanout`/`epistemic_graph` on the manager to silence it.
- **`stop-first`, never `start-first`.** Single-socket service; start-first can't bind twice.
- **Worker vs manager.** `RW710` is a worker — `docker service ...` must run on `R820`
  (`docker node ls` shows the Leader). `docker ps`/`docker inspect` work locally.
- **Node-pinned bind mounts.** The host binary + source mounts only exist on `RW710`, so these
  services are pinned there; a reschedule must stay on `RW710` or it gets the wrong/old files.
- **Editable Python.** `epistemic_graph` and `agent-utilities` are editable installs from the
  canonical checkouts — merge to `main` is enough; no wheel rebuild. (Confirm with
  `python -c "import epistemic_graph; print(epistemic_graph.__file__)"`.)
- **Rollback:** `mv .venv/bin/epistemic-graph-server.bak-<ts> .venv/bin/epistemic-graph-server`
  then restart the engine service.

## Verify the new surface actually serves (`--verify`)

A bound socket only proves the engine *started* — not that it serves the capabilities you
just shipped. `scripts/promote_engine.sh --verify` (or run it standalone after a restart)
runs a **live UQL smoke** against the engine socket and asserts the new query surface
executes and that the parser validates:

```bash
scripts/promote_engine.sh --build --migrate --restore-health-period --verify
```

It checks `AS OF @t` (valid-time), `AS OF TX @t` (transaction-time), `RERANK MENTIONS`,
`RERANK MMR`, and that a bogus `RERANK` keyword is **rejected** (proof the parser is live,
not a stale binary). Connect-only failure is reported, not fatal; a real surface mismatch
exits non-zero. Override `ENGINE_SOCKET` / `VERIFY_GRAPH` if needed. See
[UQL](../uql.md) for the query surface this exercises.

## When you can't reach the manager (worker-only fallback)

The clean path restarts via `ssh R820 docker service update` (the manager). If you are on the
worker (`RW710`) **without** manager SSH and must still cycle the engine:

1. **Stage the binary** with `--no-restart` (atomic + backup, no service touch).
2. **Cycle the local task:** `docker rm -f <engine-container-on-this-node>` — Swarm reschedules
   a fresh task that re-execs the new bind-mounted binary. (`docker restart` can fail with a
   **zombie PID** if the old process didn't reap children; `rm -f` is the reliable cycle.)
3. **Expect transient churn / duplicates.** An out-of-band cycle makes Swarm think the task
   died and may briefly run **two** tasks in the slot; for single-socket services the second
   fails to bind and is reaped. For the `graph-os-host` daemon, two instances contend on the
   host-lock until one wins. Wait for exactly one `healthy` task
   (`docker ps -f name=<svc> -f health=healthy`) and `docker rm -f` any stray.
4. **First boot is slow** (snapshot/`.mp`→redb load binds the socket only when done) — wait for
   `Listening on UDS` in the engine logs before `--verify`.

This is a fallback. Prefer the manager `docker service update` path (clean reschedule, no
duplicate churn) whenever you can reach `R820`.
