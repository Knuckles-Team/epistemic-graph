# Connecting — per-wire connection guide

epistemic-graph is a **drop-in substrate**: existing clients connect to it as if it were the
database they already speak. This page is the single connect-and-query recipe per surface —
the Cargo feature to build with, the env var (or CLI flag) that sets the listen address, and a
minimal working example.

Two universal rules hold for **every** listener:

1. **Opt-in.** A listener starts only when the binary is built with its feature **and** its
   `EPISTEMIC_GRAPH_*_ADDR` env var (or CLI flag) is set. Unset ⇒ no listener, no open port.
2. **Loopback by default.** A bare enable token (`1`/`on`) or a bare port binds
   `127.0.0.1` — never `0.0.0.0`. Bind a routable address explicitly and put TLS/mTLS at the
   edge (the wire listeners terminate plaintext); see the [runbook](../operations/runbook.md).

The prebuilt `full` binary carries the single-node surfaces; `cluster` adds `pgwire`. Wires the
orchestrator folds per-tier (MySQL/MSSQL/SQLite/Bolt/AMQP) build cleanly with `--features
"<wire> query server"` (or `bolt-wire` / `amqp-wire`). See [tiers](../architecture/tiers.md).

## Address & feature reference

| Surface | Client | Feature | Listen-addr env (CLI flag) | Default when enabled |
|---------|--------|---------|----------------------------|----------------------|
| Postgres wire | `psql`, BI, ORMs | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | `127.0.0.1:5433` |
| MySQL / MariaDB wire | `mysql`, drivers | `mysql-wire` | `EPISTEMIC_GRAPH_MYSQL_ADDR` | `127.0.0.1:3306` |
| MSSQL TDS wire | `sqlcmd`, drivers | `mssql-wire` | `EPISTEMIC_GRAPH_MSSQL_ADDR` | `127.0.0.1:1433` |
| SQLite-dialect NDJSON | any TCP client | `sqlite-wire` | `EPISTEMIC_GRAPH_SQLITE_ADDR` | `127.0.0.1:<your port>` |
| Neo4j Bolt wire | `cypher-shell`, drivers | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` | `127.0.0.1:7687` |
| AMQP 0.9.1 broker | `pika`, AMQP clients | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` |
| SPARQL 1.1 HTTP | `curl`, `rdflib`, Jena | `sparql-http` | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` |
| GraphQL SSE carrier | GraphQL clients | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` |
| PromQL / Prometheus API | Grafana, `curl` | `promql` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| OTLP traces | OTel exporters, `curl` | `traces` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| Prometheus `/metrics` | Prometheus scrape | `metrics` (default) | `GRAPH_SERVICE_METRICS_ADDR` (`--metrics-addr`) | `127.0.0.1:9101` |

> The default ports above are the **documented conventions** each listener binds when given a
> bare enable token. You can always pass a full `host:port`. `EPISTEMIC_GRAPH_PGWIRE_ADDR=5433`
> avoids clashing with a real Postgres on `5432` on the same host.

---

## Postgres — `psql` / BI / ORM (`pgwire`)

```bash
# build + start (cluster carries pgwire; or: --features "pgwire query server")
EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir /var/lib/eg

# connect with any Postgres client
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

```sql
SET graph = 'my_graph';          -- select the session graph
SELECT id, properties FROM nodes LIMIT 10;
INSERT INTO nodes (id, properties) VALUES ('n1', '{"label":"Doc"}');
```

- **Auth**: SCRAM-SHA-256 when `GRAPH_SERVICE_AUTH_SECRET` is set, else trust (dev). The pg
  `user` becomes the engine ACL actor, so Row-Level Security applies.
- **Protocols**: simple **and** extended/prepared (`$N` params); `pg_catalog` /
  `information_schema` introspection is served.

## MySQL / MariaDB (`mysql-wire`)

```bash
EPISTEMIC_GRAPH_MYSQL_ADDR=127.0.0.1:3306 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "mysql-wire query server"

mysql --host 127.0.0.1 --port 3306 --user agent
```

```sql
SELECT id, properties FROM nodes LIMIT 10;
```

- Hand-rolled **Handshake v10** + `mysql_native_password` auth (`MYSQL_AUTH_ENV` selects
  `Trust` for dev). Text-protocol result sets. Same wire-neutral `WireSession` as pgwire, so
  SQL semantics are identical across wires (CONCEPT:EG-074).

## MSSQL / SQL Server (`mssql-wire`)

```bash
EPISTEMIC_GRAPH_MSSQL_ADDR=127.0.0.1:1433 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "mssql-wire query server"

sqlcmd -S 127.0.0.1,1433 -U agent -Q "SELECT id FROM nodes"
```

- Hand-rolled **TDS** server (no `tiberius`/`tds` server crate). Routes through the shared wire
  core — no SQL reimplemented per wire.

## SQLite dialect — NDJSON over TCP (`sqlite-wire`)

SQLite has no client/server wire protocol (it is an embedded library), so this surface is a
tiny **dependency-free NDJSON-over-TCP** endpoint: one JSON object per line in, one JSON line
back, over a persistent connection (so `SET graph = …` and `BEGIN`/`COMMIT` are
connection-scoped). SQLite-isms (`AUTOINCREMENT`, `INTEGER PRIMARY KEY`, `PRAGMA`) are
rewritten, then run through the shared wire core.

```bash
EPISTEMIC_GRAPH_SQLITE_ADDR=127.0.0.1:8770 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "sqlite-wire query server"
```

```bash
# request → response, newline-delimited JSON
printf '{"sql":"SELECT id FROM nodes LIMIT 3"}\n' | nc 127.0.0.1 8770
# → {"columns":[{"name":"id","type":"TEXT"}],"rows":[["n1"],["n2"],["n3"]]}
```

Response shapes: rows `{"columns":[…],"rows":[…]}`, command `{"tag":"INSERT","rows_affected":1}`,
txn `{"tag":"BEGIN"|"COMMIT"|"ROLLBACK"}`, pragma `{"tag":"PRAGMA"}`, error
`{"error":{"code":"…","message":"…"}}`. `.db` file export/import is a documented pure-Rust
follow-up (no C `rusqlite` — the Pi/no-native-dep contract).

## Neo4j — `cypher-shell` / Bolt driver (`bolt-wire`)

A native **Bolt v4.4** server (PackStream v2 codec, chunked framing) — Neo4j drivers connect
directly and `RUN` Cypher against the engine's native Cypher surface.

```bash
EPISTEMIC_GRAPH_BOLT_ADDR=127.0.0.1:7687 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "bolt-wire cypher server"

cypher-shell -a bolt://127.0.0.1:7687 -u agent -p "$SECRET"
```

```cypher
MATCH (n:Doc)-[:MENTIONS]->(m) RETURN n, m LIMIT 10;
```

```python
from neo4j import GraphDatabase
drv = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("agent", SECRET))
with drv.session() as s:
    for rec in s.run("MATCH (n) RETURN n LIMIT 5"):
        print(rec)
```

- Bolt speaks **Cypher, not SQL**, so it does not use the SQL `WireSession` core — `RUN`'s
  Cypher goes straight to the eg-query cypher engine.

## RabbitMQ — AMQP 0.9.1 client (`amqp-wire` / `broker`)

A hand-rolled **AMQP 0.9.1** server (no heavy AMQP crate) mapping
connection/channel/exchange/queue/`basic.*` frames onto the engine's RabbitMQ-class broker
primitives (exchanges, bindings, topic routing) over the KG-2.303 work-queue.

```bash
EPISTEMIC_GRAPH_AMQP_ADDR=127.0.0.1:5672 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "amqp-wire server"
```

```python
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 5672))
ch = conn.channel()
ch.queue_declare(queue="tasks")
ch.basic_publish(exchange="", routing_key="tasks", body="hello")
```

---

## HTTP surfaces

All HTTP listeners are minimal **hand-rolled HTTP/1.1** (no axum/hyper — the Pi contract).

### SPARQL 1.1 Protocol (`sparql-http`)

```bash
EPISTEMIC_GRAPH_SPARQL_ADDR=127.0.0.1:7878 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "sparql-http"

# query (GET or POST) — existing rdflib / Jena / Stardog clients work unchanged
curl -s 'http://127.0.0.1:7878/sparql' \
  --data-urlencode 'query=SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5'

# update
curl -s 'http://127.0.0.1:7878/sparql' \
  --data-urlencode 'update=INSERT DATA { <urn:a> <urn:knows> <urn:b> }'
```

The same listener serves **`POST /nl`** (natural-language query) when built with `nl-query` and
an OpenAI-compatible endpoint is configured; without configuration the surface is inert and
returns a clear "not configured" error (never a panic).

### GraphQL (`graphql`)

The GraphQL **read/mutation** surface is reachable via the native `Method::GraphQl` dispatch;
`EPISTEMIC_GRAPH_GRAPHQL_ADDR` starts the **SSE subscription carrier** (poll-only broadcast
today). Queries are compiled to graph scans + BFS (schema introspected from the live graph).

```graphql
{ Doc(first: 5) { id title mentions { id } } }
```

### PromQL / Prometheus HTTP query API (`promql`, on the `obs` listener)

```bash
EPISTEMIC_GRAPH_OBS_ADDR=127.0.0.1:5080 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "promql"

curl -s 'http://127.0.0.1:5080/api/v1/query?query=up'
curl -s 'http://127.0.0.1:5080/api/v1/query_range?query=rate(http_requests[5m])&start=…&end=…&step=15s'
curl -s 'http://127.0.0.1:5080/api/v1/labels'
```

Point a Grafana **Prometheus** data source at `http://<host>:5080` — the durable eg-tsdb series
answer PromQL directly.

### OTLP traces (`traces`, on the `obs` listener)

```bash
# OTLP/HTTP JSON span ingest (point your OTel exporter here)
curl -s -XPOST 'http://127.0.0.1:5080/v1/traces' \
  -H 'content-type: application/json' --data-binary @spans.json

# trace search + single-trace assembly + service-dependency graph
curl -s 'http://127.0.0.1:5080/api/traces?service=my-svc'
```

### Prometheus `/metrics` (the engine's own telemetry, `metrics`, default-on)

```bash
epistemic-graph-server --metrics-addr 127.0.0.1:9101 --persist-dir /var/lib/eg
curl -s http://127.0.0.1:9101/metrics
```

---

## Embedded (no wire at all) — `embedded`

For the edge / "a local engine per agent" story, skip the socket entirely: open a persist dir
and call core ops as plain methods (SQLite/DuckDB-style, no Tokio / no socket / no HMAC).

```rust
use epistemic_graph::embedded::EmbeddedEngine;
let eng = EmbeddedEngine::open("/var/lib/eg")?;   // built --features "embedded redb"
eng.add_node("my_graph", "n1", br#"{"label":"Doc"}"#.to_vec())?;
```

---

See the [capability matrix](../capabilities.md) for the operation-by-operation truth per
surface and the [operations runbook](../operations/runbook.md) for the full env-var catalog,
tiers, backup/PITR, and RBAC.

---
*CONCEPT:EG-095 — comprehensive interface + operations documentation.*
