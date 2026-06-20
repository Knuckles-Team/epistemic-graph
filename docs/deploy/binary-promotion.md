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
   this server build". The script guards this by checking for a finance symbol.
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
4. **Restart consumers (optional, for Python-side changes).** `graph-os` + `messaging` pick up
   merged `agent-utilities`:
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

## Gotchas

- **`--features full`, always.** `server`-only is a silent runtime breakage for finance/quant.
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
