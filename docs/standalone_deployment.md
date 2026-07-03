# Standalone deployment — just the engine, nothing else

`epistemic-graph` is a **self-contained durable database**. You do **not** need
`agent-utilities`, a KG hub, or any other service to run it — the shipped
`epistemic-graph-server` binary *is* the whole product. This page is the copy-paste
recipe to stand up a running database two ways:

1. **[Pure binary](#pure-binary)** — `pip install` the wheel, run one command.
2. **[Docker Compose](#docker-compose)** — one container, one durable volume.

Once it is up, point **any Postgres client** at the wire port and run
`CREATE TABLE` / `INSERT` / `SELECT` — see the
**[DBeaver / psql quickstart](dbeaver_quickstart.md)**. For where this sits relative
to the optional orchestrator and UIs, see the
**[deployment topology](deployment_topology.md)**.

> **Nothing here depends on `agent-utilities`.** The engine is a stand-alone Rust
> service. `agent-utilities` is an *optional* consumer that enhances it (see the
> [topology doc](deployment_topology.md)); it is never required to run, deploy, or
> query the database.

---

## What you get

The published default wheel is the **`node` tier** — a *complete single-node DB*:
durable redb-authoritative store + Cypher + DataFusion **SQL** + vector ANN +
time-series + full-text + the **Postgres wire listener** (`pgwire`), with no Raft
(single node). That is everything a SQL/graph client needs. (For in-engine-Raft HA,
build the `cluster` tier from source — see [deployment.md](deployment.md).)

---

## Pure binary

### 1. Install

The wheel ships the prebuilt `epistemic-graph-server` binary (maturin
`bindings = "bin"`) — installing it just downloads and unpacks the per-platform
binary; nothing compiles.

```bash
# with pip
pip install epistemic-graph

# …or with uv (the fleet-standard fast installer)
uv pip install --prerelease=allow epistemic-graph

# confirm the binary is on PATH
epistemic-graph-server --help
```

### 2. Run — durable, authenticated, with the Postgres wire port

Two rules the binary enforces, worth knowing up front:

- **An auth secret is mandatory.** The RPC transport refuses to start without
  `GRAPH_SERVICE_AUTH_SECRET` (or `--auth-secret`). For a throwaway dev instance
  only, `--allow-insecure` (or `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`) starts it
  unauthenticated.
- **Every listener is opt-in and loopback-by-default.** The Postgres wire listener
  starts **only** when `EPISTEMIC_GRAPH_PGWIRE_ADDR` is set. Unset ⇒ no open port.

```bash
# a stable secret for this instance (reuse it for every client)
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"

# a durable data directory (the redb-authoritative source of truth)
mkdir -p ~/eg-data

# turn on the Postgres wire listener
export EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433

epistemic-graph-server \
  --persist-dir ~/eg-data \
  --metrics-addr 127.0.0.1:9101
```

That is a running database. It also opens a MessagePack RPC transport — a local
**Unix Domain Socket** by default (`$XDG_RUNTIME_DIR/epistemic-graph.sock`, else
`/tmp/epistemic-graph.sock`), and a TCP listener if you add `--tcp-addr`. Neither is
needed to use the Postgres wire; they are for the native `epistemic_graph` client.

Minimal env-only variant (everything via environment, no flags):

```bash
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"
export GRAPH_SERVICE_PERSIST_DIR=~/eg-data
export GRAPH_SERVICE_METRICS_ADDR=127.0.0.1:9101
export EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433
epistemic-graph-server
```

### 3. Connect

```bash
# password = hex(HMAC-SHA256($GRAPH_SERVICE_AUTH_SECRET, "pgwire:agent"))
# (see the DBeaver/psql quickstart for how to compute it, or use trust auth for dev)
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

### Server flags & env (verified against `--help` / `src/main.rs`)

| Flag | Env var | Default | Purpose |
|------|---------|---------|---------|
| `--socket-path` | `GRAPH_SERVICE_SOCKET` | `$XDG_RUNTIME_DIR/epistemic-graph.sock` → `/tmp/epistemic-graph.sock` | Local UDS for the native client |
| `--tcp-addr` | — | unset (no TCP) | TCP RPC listener, e.g. `0.0.0.0:9100` (remote native clients) |
| `--auth-secret` | `GRAPH_SERVICE_AUTH_SECRET` | — (**required**) | HMAC-SHA256 shared secret; empty refuses to start |
| `--allow-insecure` | `EPISTEMIC_GRAPH_ALLOW_INSECURE` | off | Dev-only: start with no auth |
| `--persist-dir` | `GRAPH_SERVICE_PERSIST_DIR` | in-memory if unset | Durable redb-authoritative store dir |
| `--metrics-addr` | `GRAPH_SERVICE_METRICS_ADDR` | unset (no metrics) | Prometheus `/metrics` listener |
| `--checkpoint-interval` | — | `300` | Auto-checkpoint interval (seconds) |
| — | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | unset (no listener) | **Turn on the Postgres wire**; e.g. `127.0.0.1:5433` |
| — | `EPISTEMIC_GRAPH_PGWIRE_AUTH` | `scram` if secret set, else `trust` | pgwire auth mode: `scram` \| `trust` |
| — | `EPISTEMIC_GRAPH_PGWIRE_GRAPH` | `__commons__` | Default graph a fresh pgwire connection binds |

> Leaving `--persist-dir` unset runs the engine **in-memory only** (data is lost on
> exit). Always set it for a real deployment.

---

## Docker Compose

The repo ships a self-contained **[`docker/compose.standalone.yml`](https://github.com/Knuckles-Team/epistemic-graph/blob/main/docker/compose.standalone.yml)**
that runs **only** epistemic-graph — a durable volume, the Postgres wire, RPC, and
metrics ports. No other services.

```bash
# 1. an auth secret is mandatory
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"

# 2. up (builds the node-tier image from docker/Dockerfile the first time)
docker compose -f docker/compose.standalone.yml up -d

# 3. it's a database — connect with any Postgres client
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

Published ports (all bound to `127.0.0.1`):

| Port | Purpose |
|------|---------|
| `5433` | Postgres wire — psql / DBeaver / BI / ORMs |
| `9100` | MessagePack RPC — native `epistemic_graph` client (optional) |
| `9101` | Prometheus `/metrics` |

The durable store is the named volume `eg-standalone-data`; it survives
`docker compose down` and is removed only by `down -v`.

To pull a prebuilt multi-arch image instead of building locally:

```bash
export EG_IMAGE=<registry>/epistemic-graph:<tag>
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"
docker compose -f docker/compose.standalone.yml up -d   # `build:` is ignored when EG_IMAGE is set to a pullable ref
```

Or the same thing as a bare `docker run` (no compose):

```bash
docker volume create eg-data
docker run -d --name epistemic-graph \
  -e GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)" \
  -e EPISTEMIC_GRAPH_PGWIRE_ADDR=0.0.0.0:5433 \
  -p 127.0.0.1:5433:5433 -p 127.0.0.1:9100:9100 -p 127.0.0.1:9101:9101 \
  -v eg-data:/var/lib/epistemic-graph/data \
  <registry>/epistemic-graph:<tag>
```

---

## Next steps

- **[DBeaver / psql quickstart](dbeaver_quickstart.md)** — connect a SQL client and
  run real `CREATE TABLE` / `INSERT` / `SELECT`.
- **[Connecting (per-wire guide)](interfaces/connecting.md)** — MySQL, MSSQL, Bolt,
  Redis, SPARQL, GraphQL and the other wire protocols.
- **[SQL & pgwire](interfaces/sql.md)** — the full SQL surface (DDL, DML, `nodes`/
  `edges`, user tables, functions, vectors).
- **[Deployment topology](deployment_topology.md)** — run just the engine, or add
  the optional orchestrator (`agent-utilities`) and UIs.
- **[Operations runbook](operations/runbook.md)** — backups, TLS at the edge,
  metrics, tuning.
