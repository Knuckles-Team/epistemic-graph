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

The published wheel is the **one main build** — a *complete single-node DB*:
durable redb-authoritative store + Cypher + DataFusion **SQL** + vector ANN +
time-series + full-text + the **Postgres wire listener** (`pgwire`), with no Raft
(single node). That is everything a SQL/graph client needs. (For in-engine-Raft HA,
build with the `cluster` Cargo feature — see [deployment.md](deployment.md).)

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

The wheel's Rust binary is already built with Cargo `full`, and the bare Python
package includes the OWL helpers, LMCache client acceleration, and numeric
interoperability dependencies. Python extras do not change its compiled feature set:
`epistemic-graph`, `[full]`, and `[all]` are equivalent production installs. Use
`[test]` only for its explicit validation suite. The `[lake-parity]` compatibility
marker is empty because uv resolves all workspace extras together; install
`tests/lake-parity-requirements.txt` in an isolated environment when validating
against the external PyIceberg/DeltaLake reference readers.

### 2. Run — durable, authenticated, with the Postgres wire port

Two rules the binary enforces, worth knowing up front:

- **The complete authority configuration is mandatory.** The served binary
  requires `security`, a non-empty `GRAPH_SERVICE_AUTH_SECRET`, audience, tenant,
  policy revision, durable replay store, and trusted signer registry.
- **Every auxiliary listener is opt-in and loopback-only.** The Postgres wire
  listener starts only when `EPISTEMIC_GRAPH_PGWIRE_ADDR` is set and rejects a
  non-loopback address. Routable native TCP requires TLS.

```bash
# Load secret and deployment-specific values from runtime configuration.
: "${GRAPH_SERVICE_AUTH_SECRET:?required}"
: "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
: "${GRAPH_SERVICE_PERSIST_DIR:?required}"
export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
export EPISTEMIC_GRAPH_TENANT=tenant:default
export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial

# turn on the Postgres wire listener
export EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433

epistemic-graph-server \
  --metrics-addr 127.0.0.1:9101
```

That is a running database. It also opens a MessagePack RPC transport — a local
**Unix Domain Socket** in the platform runtime directory, and a TCP listener if
you add `--tcp-addr`. Neither is
needed to use the Postgres wire; they are for the native `epistemic_graph` client.

Minimal env-only variant (everything via environment, no flags):

```bash
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"
: "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
: "${GRAPH_SERVICE_PERSIST_DIR:?required}"
export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
export EPISTEMIC_GRAPH_TENANT=tenant:default
export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial
export GRAPH_SERVICE_METRICS_ADDR=127.0.0.1:9101
export EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433
epistemic-graph-server
```

### 3. Connect

```bash
# password = hex(HMAC-SHA256($GRAPH_SERVICE_AUTH_SECRET, "pgwire:agent"))
# (see the DBeaver/psql quickstart for how to compute it)
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

### Server flags & env (verified against `--help` / `src/main.rs`)

| Flag | Env var | Default | Purpose |
|------|---------|---------|---------|
| `--socket-path` | `GRAPH_SERVICE_SOCKET` | platform runtime socket | Local UDS for the native client |
| `--socket-mode` | `GRAPH_SERVICE_SOCKET_MODE` | `0600` | Octal mode applied to the UDS socket after bind; refused at startup if malformed or world-accessible |
| `--tcp-addr` | `GRAPH_SERVICE_TCP_ADDR` | unset (no TCP) | Native TCP RPC listener; a routable address requires TLS |
| `--tcp-tls-cert` / `--tcp-tls-key` | `GRAPH_SERVICE_TLS_CERT` / `GRAPH_SERVICE_TLS_KEY` | — | PEM identity required together for routable native TCP |
| `--auth-secret` | `GRAPH_SERVICE_AUTH_SECRET` | — (**required**) | Non-empty HMAC-SHA256 secret for `eg2.` |
| — | `EPISTEMIC_GRAPH_REQUIRE_OIDC` | unset ⇒ **required** (secure by default since 2026-07-22) | OIDC identity binding is mandatory unless explicitly opted out with `false`/`0`/`no`/`off`; see [deployment.md § Migrating to OIDC-required](deployment.md#migrating-to-oidc-required) |
| — | `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` / `EPISTEMIC_GRAPH_OIDC_JWT_AUDIENCE` / `EPISTEMIC_GRAPH_OIDC_JWKS_URL` | — (required unless opted out) | Keycloak realm issuer / audience / JWKS URL |
| `--persist-dir` | `GRAPH_SERVICE_PERSIST_DIR` | — (**required for served mode**) | Durable redb-authoritative store and replay-ledger dir |
| — | `EPISTEMIC_GRAPH_REDB_COMMIT_POLICY` | engine default | `each`, `interval`, or positive milliseconds; invalid/zero values fail startup |
| `--metrics-addr` | `GRAPH_SERVICE_METRICS_ADDR` | unset (no metrics) | Prometheus `/metrics` listener |
| — | `EPISTEMIC_GRAPH_AUDIENCE` / `EPISTEMIC_GRAPH_TENANT` / `EPISTEMIC_GRAPH_POLICY_VERSION` | — (**required**) | Exact request policy values |
| — | `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` | — (**required**) | Runtime secret map of trusted operation signer ids to HMAC keys |
| — | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | unset (no listener) | **Turn on the Postgres wire**; e.g. `127.0.0.1:5433` |
| — | `EPISTEMIC_GRAPH_PGWIRE_AUTH` | `scram` | Mandatory pgwire SCRAM auth; any other value or missing key material fails startup |
| — | `EPISTEMIC_GRAPH_PGWIRE_GRAPH` | `__commons__` | Default graph a fresh pgwire connection binds |

Served mode rejects a missing persist directory; the authoritative redb store and
durable replay protection cannot be disabled.
The current shard layout is `graph-<n>.redb` for every K (`graph-0.redb` for K=1).
Any other layout must be converted offline with `migrate-shards` before startup.

---

## Docker Compose

The repo ships a self-contained **[`docker/compose.standalone.yml`](https://github.com/Knuckles-Team/epistemic-graph/blob/main/docker/compose.standalone.yml)**
that runs **only** epistemic-graph with a durable volume and TLS native RPC. No
other service is required unless an operator chooses to add an authenticated
gateway for a loopback-only auxiliary protocol.

```bash
# 1. load secrets, policy, and TLS material from deployment configuration
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"
: "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
: "${TLS_CERT_FILE:?required}"
: "${TLS_KEY_FILE:?required}"
export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
export EPISTEMIC_GRAPH_TENANT=tenant:default
export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial

# 2. up (builds the main image from docker/Dockerfile the first time)
docker compose -f docker/compose.standalone.yml up -d
```

The native RPC port is the only routable engine listener and uses TLS. Database
protocol, metrics, and other auxiliary listeners remain inside the deployment's
loopback namespace; a co-located authenticated TLS gateway can publish them.

| Port | Purpose |
|------|---------|
| `9100` | TLS MessagePack RPC — native `epistemic_graph` client |

The durable store is the named volume `eg-standalone-data`; it survives
`docker compose down` and is removed only by `down -v`.

To pull a prebuilt multi-arch image instead of building locally:

```bash
export EG_IMAGE=<registry>/epistemic-graph:<tag>
export GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)"
: "${EPISTEMIC_GRAPH_SIGNER_KEYS_JSON:?required}"
: "${TLS_CERT_FILE:?required}"
: "${TLS_KEY_FILE:?required}"
export EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph
export EPISTEMIC_GRAPH_TENANT=tenant:default
export EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial
docker compose -f docker/compose.standalone.yml up -d   # `build:` is ignored when EG_IMAGE is set to a pullable ref
```

Or the same thing as a bare `docker run` (no compose):

```bash
: "${CONTAINER_DATA_DIR:?set to the image data directory}"
docker volume create eg-data
docker run -d --name epistemic-graph \
  -e GRAPH_SERVICE_AUTH_SECRET \
  -e EPISTEMIC_GRAPH_AUDIENCE=epistemic-graph \
  -e EPISTEMIC_GRAPH_TENANT=tenant:default \
  -e EPISTEMIC_GRAPH_POLICY_VERSION=policy:initial \
  -e EPISTEMIC_GRAPH_SIGNER_KEYS_JSON \
  -e GRAPH_SERVICE_PERSIST_DIR="${CONTAINER_DATA_DIR}" \
  -e GRAPH_SERVICE_TCP_ADDR=0.0.0.0:9100 \
  -e GRAPH_SERVICE_TLS_CERT=/run/secrets/server.crt \
  -e GRAPH_SERVICE_TLS_KEY=/run/secrets/server.key \
  -p 9100:9100 \
  --mount type=bind,src="${TLS_CERT_FILE}",dst=/run/secrets/server.crt,readonly \
  --mount type=bind,src="${TLS_KEY_FILE}",dst=/run/secrets/server.key,readonly \
  -v eg-data:"${CONTAINER_DATA_DIR}" \
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
