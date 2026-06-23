# Engine binary promotion (homelab)

How to ship a new `epistemic-graph-server` binary into the live homelab fleet, and
why each step is what it is. Automated by [`scripts/promote_engine.sh`](../../scripts/promote_engine.sh).

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
finish before it starts probing:

```bash
# 1. Restart the engine with a generous start-period (e.g. 600s) for the migration.
ssh R820 docker service update --update-order stop-first \
    --health-start-period 600s --force epistemic-graph_epistemic-graph

# 2. Watch it migrate (it logs progress; the socket binds only when done):
ssh R820 'docker service logs -f epistemic-graph_epistemic-graph 2>&1' | grep -i 'snapshot load\|migration\|imported'
#   Snapshot load progress: 2000/8123 graph(s) processed (1998 restored)
#   redb authoritative migration: imported 8123 graph(s) from legacy snapshot/WAL into redb in 214.3s …

# 3. Once the socket is bound and the migration line printed, restore the normal start-period.
ssh R820 docker service update --health-start-period 20s --force epistemic-graph_epistemic-graph
```

Progress is logged so you can tell *slow-but-working* from *hung* (CONCEPT:KG-2.200):
`Snapshot load progress: N/total graph(s) processed` during the legacy load, then
`redb authoritative migration: imported N graph(s) … in Xs` when it commits to redb. After
that completes, run step 4 (**restart the consumers** with `GRAPH_BACKEND=fanout`) so they
reconnect to the now-authoritative engine.

> **Recommended follow-up (not yet implemented):** teach `scripts/promote_engine.sh` a
> `--health-start-period <dur>` flag (or a `--migrate` mode that auto-extends the
> start-period for the engine restart, tails the migration progress log, and restores the
> default once the socket is bound). Today this is a manual two-`service update` dance.

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
  into a restart loop. See [Authoritative migration (first boot)](#authoritative-migration-first-boot).
- **Consumers must be `GRAPH_BACKEND=fanout`, never `tiered`.** The `tiered` backend was removed;
  a stale consumer fails bootstrap with "A persistent graph backend is required" / "Unknown
  graph backend type 'tiered'".
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
