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
| Redis RESP wire | `redis-cli`, clients | `redis-wire` | `EPISTEMIC_GRAPH_REDIS_ADDR` | `127.0.0.1:6379` |
| S3 REST API | `aws s3`, MinIO SDKs | `s3-api` | `EPISTEMIC_GRAPH_S3_ADDR` | `127.0.0.1:9000` |
| AMQP 0.9.1 broker | `pika`, AMQP clients | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` |
| MQTT 3.1.1/5.0 broker | `mosquitto_pub`, IoT | `mqtt-wire` | `EPISTEMIC_GRAPH_MQTT_ADDR` | `127.0.0.1:1883` |
| STOMP 1.2 broker | STOMP clients | `stomp-wire` | `EPISTEMIC_GRAPH_STOMP_ADDR` | `127.0.0.1:61613` |
| KV-cache (vLLM/LMCache) | vLLM/LMCache connector | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` | `127.0.0.1:9130` |
| SPARQL 1.1 HTTP + `/nl` | `curl`, `rdflib`, Jena | `sparql-http` | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` |
| GraphQL SSE carrier | GraphQL clients | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` |
| Federated search (`/federated`) | `curl`, apps | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (`--federated-addr`) | `127.0.0.1:7900` |
| PromQL / Prometheus API | Grafana, `curl` | `promql` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| OTLP traces | OTel exporters, `curl` | `traces` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| Obs logs (OTLP/`_bulk`/syslog) | log shippers | `obs` | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
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

## Redis — `redis-cli` / clients (`redis-wire`)

A hand-rolled **RESP2/RESP3** listener (`src/server/redis_wire/`, CONCEPT:EG-174) serving the
core Redis command set over the engine's namespace-scoped KV surface (feature `kv`): strings
(`GET`/`SET`/`DEL`/`EXPIRE`/`INCR`), hashes (`HSET`/`HGET`), lists (`LPUSH`/`LRANGE`), sets
(`SADD`/`SMEMBERS`), sorted sets (`ZADD`/`ZRANGE`), and keyspace `SCAN`.

```bash
EPISTEMIC_GRAPH_REDIS_ADDR=127.0.0.1:6379 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "redis-wire server"

redis-cli -h 127.0.0.1 -p 6379 SET agent:1 online
redis-cli -h 127.0.0.1 -p 6379 GET agent:1        # → "online"
```

- **Auth**: `EPISTEMIC_GRAPH_REDIS_PASSWORD` enables `AUTH` (`redis-cli -a …`); unset ⇒ no auth
  (dev). The command set is a documented **subset** of Redis, backed by the durable KV store.

## S3 — `aws s3` / MinIO SDKs (`s3-api`)

An **S3-compatible REST API** (`src/server/s3/`, CONCEPT:EG-176) over the blob CAS: bucket +
object CRUD (`PUT`/`GET`/`DELETE`/`HEAD`/List) with **SigV4-lite** auth, so S3 clients read/write
blobs as objects.

```bash
EPISTEMIC_GRAPH_S3_ADDR=127.0.0.1:9000 \
EPISTEMIC_GRAPH_S3_ACCESS_KEY=agent \
EPISTEMIC_GRAPH_S3_SECRET_KEY=$SECRET \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "s3-api server"

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://docs
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./report.pdf s3://docs/report.pdf
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://docs
```

- **Auth**: SigV4-lite keyed by `EPISTEMIC_GRAPH_S3_ACCESS_KEY` / `…_S3_SECRET_KEY`.
- Objects are stored content-addressed in the same dedup'd, refcount-GC'd blob store as the
  native blob surface (see [kv-blob](kv_blob.md)).

## RabbitMQ — AMQP 0.9.1 client (`amqp-wire` / `broker`)

A hand-rolled **AMQP 0.9.1** server (no heavy AMQP crate) mapping
connection/channel/exchange/queue/`basic.*` frames onto the engine's RabbitMQ-class broker
primitives (exchanges, bindings, topic routing) over the KG-2.303 work-queue. See
[messaging](messaging.md) for the broker semantics (DLQ/TTL/priority/streams/confirms).

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

- The broker graph is `EPISTEMIC_GRAPH_AMQP_GRAPH` (default `__commons__`). All three broker
  wires (AMQP/MQTT/STOMP) share the **one** broker — a message published over AMQP can be
  consumed over MQTT/STOMP by topic.

## MQTT — `mosquitto_pub` / IoT (`mqtt-wire`)

An **MQTT 3.1.1 / 5.0** listener (`src/server/mqtt_wire/`, CONCEPT:EG-281) mapping
CONNECT/PUBLISH/SUBSCRIBE/PINGREQ/DISCONNECT onto the EG-275 broker (topic exchange + bindings,
QoS 0/1), so MQTT/IoT clients pub/sub over the native broker.

```bash
EPISTEMIC_GRAPH_MQTT_ADDR=127.0.0.1:1883 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "mqtt-wire server"

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/#' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/room1' -m '21.5'
```

- Broker graph: `EPISTEMIC_GRAPH_MQTT_GRAPH` (default `__commons__`).

## STOMP — text-frame clients (`stomp-wire`)

A **STOMP 1.2** text-frame listener (`src/server/stomp_wire/`, CONCEPT:EG-282) mapping
CONNECT/SEND/SUBSCRIBE/ACK/DISCONNECT onto the EG-275 broker.

```bash
EPISTEMIC_GRAPH_STOMP_ADDR=127.0.0.1:61613 \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "stomp-wire server"
```

```text
CONNECT
accept-version:1.2

^@
SEND
destination:/queue/tasks

hello^@
```

- Broker graph: `EPISTEMIC_GRAPH_STOMP_GRAPH` (default `__commons__`).

## KV-cache — vLLM / LMCache shared blocks (`kvcache-server`)

A gated HTTP surface over the tiered shared KV-cache (`src/server/kvcache_http/`,
CONCEPT:EG-185/186/187), so parallel-deployed vLLM/LMCache instances share LLM KV blocks by
token-hash (dedup + OOM-offload). See [kvcache](kvcache.md) for the tier model and the LMCache
connector contract.

```bash
EPISTEMIC_GRAPH_KVCACHE_ADDR=127.0.0.1:9130 \
EPISTEMIC_GRAPH_KVCACHE_TOKEN=$SECRET \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "kvcache-server server"

curl -s -XPUT --data-binary @block.bin http://127.0.0.1:9130/kv/<token-hash>   # store
curl -s http://127.0.0.1:9130/kv/<token-hash>                                  # fetch (404 if absent)
curl -s http://127.0.0.1:9130/kv/<token-hash>/exists                           # {"hash":…,"exists":bool}
curl -s http://127.0.0.1:9130/kv/stats                                         # occupancy + dedup stats
```

- **Auth**: bearer `EPISTEMIC_GRAPH_KVCACHE_TOKEN` when set (`Authorization: Bearer …`).

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

See [observability](observability.md) for the full logs + PromQL + traces + VRL-pipeline surface.

### Super-cluster federated search (`federation-search`)

`/federated` fans a read query across the peer registry
(`EPISTEMIC_GRAPH_FEDERATION_PEERS`, an SSRF allowlist in `EPISTEMIC_GRAPH_FEDERATION_ALLOW`)
**and** the local store, then unions/de-dups + RRF-re-ranks the partials (a slow/dead peer
degrades to `partial: true`, never fails — CONCEPT:EG-243). This is a **separate** listener from
`/sparql`, on its own `EPISTEMIC_GRAPH_FEDERATED_ADDR`.

```bash
EPISTEMIC_GRAPH_FEDERATED_ADDR=127.0.0.1:7900 \
EPISTEMIC_GRAPH_FEDERATION_PEERS='http://peer-b:7900,http://peer-c:7900' \
EPISTEMIC_GRAPH_FEDERATION_ALLOW='peer-b,peer-c' \
  epistemic-graph-server --persist-dir /var/lib/eg   # built --features "federation-search server"

curl -s -XPOST 'http://127.0.0.1:7900/federated' \
  -H 'content-type: application/json' \
  --data '{"query":"SELECT id FROM nodes LIMIT 10","lang":"sql"}'
```

Peers answer each other over `/federated?local=1` (run-locally-only, no re-fan). Distinct from
the in-plan `Op::ForeignScan` federation (KG-2.232) — that composes a single foreign source into
one query plan; `/federated` scatter-gathers a whole query across peer engines.

### Natural-language query (`/nl`, `nl-query`)

`POST /nl` on the **SPARQL** listener turns a natural-language string into a plan and executes it
through the deterministic pipeline (CONCEPT:EG-078/080). It needs `nl-query` **and** an
OpenAI-compatible endpoint (`EPISTEMIC_GRAPH_NL_ENDPOINT` / `…_NL_MODEL` / `…_NL_API_KEY_ENV`);
unconfigured it returns a clear "not configured" error, never a panic.

```bash
curl -s -XPOST 'http://127.0.0.1:7878/nl' \
  -H 'content-type: application/json' \
  --data '{"text":"how many Doc nodes mention Alice?","graph":"my_graph"}'
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

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](sql.md) · [SPARQL & RDF](sparql.md) · [Cypher & Bolt](cypher.md) · [Messaging & Broker](messaging.md) · [Key-value & Blob](kv_blob.md) · [Client Drivers](clients.md).
