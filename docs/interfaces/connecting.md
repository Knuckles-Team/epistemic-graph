# Connecting — per-wire connection guide

epistemic-graph is a **drop-in substrate**: existing clients connect to it as if it were the
database they already speak. This page is the single connect-and-query recipe per surface —
the Cargo feature to build with, the env var (or CLI flag) that sets the listen address, and a
minimal working example.

Two universal rules hold for **every** listener:

1. **Opt-in.** A listener starts only when the binary is built with its feature **and** its
   `EPISTEMIC_GRAPH_*_ADDR` env var (or CLI flag) is set. Unset ⇒ no listener, no open port.
2. **Authenticated loopback for direct plaintext protocol wires.** A bare enable
   token (`1`/`on`) or a bare port binds `127.0.0.1` — never `0.0.0.0`. PGWire, MySQL,
   MSSQL TDS, Bolt, AMQP, MQTT and STOMP reject every non-loopback bind and reject
   missing key material. Expose one only through an authenticated TLS/mTLS sidecar or
   gateway that connects to the loopback backend; the protocol login still verifies
   and binds the actor. See the
   [runbook](../operations/runbook.md).
3. **Current authority is mandatory.** Every served build includes `security` and
   authoritative redb. Before starting any example below, load
   `GRAPH_SERVICE_AUTH_SECRET`, `GRAPH_SERVICE_PERSIST_DIR`,
   `EPISTEMIC_GRAPH_AUDIENCE`, `EPISTEMIC_GRAPH_TENANT`,
   `EPISTEMIC_GRAPH_POLICY_VERSION`, and `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` from
   deployment configuration. There is no anonymous or in-memory served mode.

The prebuilt `full` binary carries every single-node surface. `cluster` adds Raft
replication, not a different security or wire contract. See
[tiers](../architecture/tiers.md).

## Address & feature reference

| Surface | Client | Feature | Listen-addr env (CLI flag) | Default when enabled |
|---------|--------|---------|----------------------------|----------------------|
| Postgres wire | `psql`, BI, ORMs | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | `127.0.0.1:5433` |
| MySQL / MariaDB wire | `mysql`, drivers | `mysql-wire` | `EPISTEMIC_GRAPH_MYSQL_ADDR` | `127.0.0.1:3306` |
| MSSQL TDS wire | `sqlcmd`, drivers | `mssql-wire` | `EPISTEMIC_GRAPH_MSSQL_ADDR` | `127.0.0.1:1433` |
| SQLite-dialect NDJSON | any TCP client | `sqlite-wire` | `EPISTEMIC_GRAPH_SQLITE_ADDR` | `127.0.0.1:<your port>` |
| Neo4j Bolt wire | custom-auth drivers | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` | `127.0.0.1:7687` |
| Redis RESP wire | `redis-cli`, clients | `redis-wire` | `EPISTEMIC_GRAPH_REDIS_ADDR` | `127.0.0.1:6379` |
| S3 REST API | `aws s3`, MinIO SDKs | `s3-api` | `EPISTEMIC_GRAPH_S3_ADDR` | `127.0.0.1:9000` |
| AMQP 0.9.1 broker | `pika`, AMQP clients | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` |
| MQTT 3.1.1/5.0 broker | `mosquitto_pub`, IoT | `mqtt-wire` | `EPISTEMIC_GRAPH_MQTT_ADDR` | `127.0.0.1:1883` |
| STOMP 1.2 broker | STOMP clients | `stomp-wire` | `EPISTEMIC_GRAPH_STOMP_ADDR` | `127.0.0.1:61613` |
| KV-cache (vLLM/LMCache) | vLLM/LMCache connector | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` | `127.0.0.1:9130` |
| SPARQL 1.1 HTTP + `/nl` | `curl`, `rdflib`, Jena | `sparql-http` | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` |
| GraphQL authenticated SSE | HTTP/SSE clients with an `eg2.` signer | `graphql` (implies `security`) | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` |
| Federated search (`/federated`) | `curl`, apps | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (`--federated-addr`) | `127.0.0.1:7900` |
| PromQL / Prometheus API | Grafana, `curl` | `promql` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| OTLP traces | OTel exporters, `curl` | `traces` (impl `obs`) | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| Obs logs (OTLP/`_bulk`/syslog) | log shippers | `obs` | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` |
| Prometheus `/metrics` | Prometheus scrape | `metrics` (default) | `GRAPH_SERVICE_METRICS_ADDR` (`--metrics-addr`) | `127.0.0.1:9101` |

> The default ports above are the **documented conventions** each listener binds when given a
> bare enable token. You may pass a full **loopback** `host:port`; a routable
> auxiliary address is rejected. `EPISTEMIC_GRAPH_PGWIRE_ADDR=5433`
> avoids clashing with a real Postgres on `5432` on the same host.

---

## Postgres — `psql` / BI / ORM (`pgwire`)

```bash
# build + start (cluster carries pgwire; or: --features "pgwire query server")
EPISTEMIC_GRAPH_PGWIRE_ADDR=127.0.0.1:5433 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"

# connect with any Postgres client
psql "host=127.0.0.1 port=5433 user=agent dbname=__commons__"
```

```sql
SET graph = 'my_graph';          -- select the session graph
SELECT id, properties FROM nodes LIMIT 10;
INSERT INTO nodes (id, properties) VALUES ('n1', '{"label":"Doc"}');
```

- **Auth**: SCRAM-SHA-256 with mandatory non-empty `GRAPH_SERVICE_AUTH_SECRET`.
  Only a successful SCRAM proof maps the pg `user` to the engine ACL actor, so
  Row-Level Security applies. Authentication cannot be disabled.
- **Protocols**: simple **and** extended/prepared (`$N` params); `pg_catalog` /
  `information_schema` introspection is served.

## MySQL / MariaDB (`mysql-wire`)

```bash
EPISTEMIC_GRAPH_MYSQL_ADDR=127.0.0.1:3306 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "mysql-wire query server"

mysql --host 127.0.0.1 --port 3306 --user agent
```

```sql
SELECT id, properties FROM nodes LIMIT 10;
```

- Hand-rolled **Handshake v10** + mandatory `mysql_native_password` auth. Only a
  verified native-password proof maps the user to an ACL actor; missing key material
  and every non-`native` mode fail startup. Text-protocol result sets use the same wire-neutral `WireSession`
  as pgwire, so SQL semantics are identical across wires
  (CONCEPT:EG-KG.compute.subsystems-reference).

## MSSQL / SQL Server (`mssql-wire`)

```bash
EPISTEMIC_GRAPH_MSSQL_ADDR=127.0.0.1:1433 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "mssql-wire query server"

sqlcmd -S 127.0.0.1,1433 -U agent -P "$MSSQL_PASSWORD" -Q "SELECT id FROM nodes"
```

- Hand-rolled **TDS** server (no `tiberius`/`tds` server crate). Routes through the shared wire
  core — no SQL reimplemented per wire.
- **Auth**: LOGIN7 password is `hex(HMAC-SHA256(secret, "mssql:" + user))`.
  Missing key material fails startup; a verified user becomes the ACL actor. TDS
  encryption is not implemented, so remote clients require a TLS/mTLS gateway into
  this authenticated loopback listener.

## SQLite dialect — NDJSON over TCP (`sqlite-wire`)

SQLite has no client/server wire protocol (it is an embedded library), so this surface is a
tiny **dependency-free NDJSON-over-TCP** endpoint: one JSON object per line in, one JSON line
back, over a persistent connection (so `SET graph = …` and `BEGIN`/`COMMIT` are
connection-scoped). SQLite-isms (`AUTOINCREMENT`, `INTEGER PRIMARY KEY`, `PRAGMA`) are
rewritten, then run through the shared wire core.

```bash
EPISTEMIC_GRAPH_SQLITE_ADDR=127.0.0.1:8770 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "sqlite-wire query server"
```

```bash
# request → response, newline-delimited JSON
printf '{"sql":"SELECT id FROM nodes LIMIT 3"}\n' | nc 127.0.0.1 8770
# → {"columns":[{"name":"id","type":"TEXT"}],"rows":[["n1"],["n2"],["n3"]]}
```

Response shapes: rows `{"columns":[…],"rows":[…]}`, command `{"tag":"INSERT","rows_affected":1}`,
txn `{"tag":"BEGIN"|"COMMIT"|"ROLLBACK"}`, pragma `{"tag":"PRAGMA"}`, error
`{"error":{"code":"…","message":"…"}}`. Governed `.db` file export/import is
available through `ImportSqliteFile` and `ExportSqliteFile`; the bundled SQLite
component is confined to the `sqlite-file` feature.

## Neo4j — Bolt drivers (`bolt-wire`) {#neo4j-bolt-drivers}

A native **Bolt v4.4** server (PackStream v2 codec, chunked framing) — Neo4j drivers connect
directly and `RUN` Cypher against the engine's native Cypher surface.

```bash
EPISTEMIC_GRAPH_BOLT_ADDR=127.0.0.1:7687 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "bolt-wire cypher server"
```

```cypher
MATCH (n:Doc)-[:MENTIONS]->(m) RETURN n, m LIMIT 10;
```

Create the auth token immediately before each physical connection. The native client helper returns
the exact custom auth-token map a Neo4j driver auth-manager callback must send:

```python
token = native_client.fresh_bolt_auth_token(graph="agent:planner")
# token == {"scheme": "epistemic", "principal": <opaque digest>,
#           "credentials": <hex MessagePack signed Health request>}
```

Do not cache or reuse `token`: its signed nonce is durably consumed by the first connection. Configure
the Neo4j driver's custom/dynamic auth-token provider to call the helper for every new socket. The
driver session database, when supplied, must equal the graph passed above.

- Bolt speaks **Cypher, not SQL**. Reads use the shared graph ACL/RLS authority; writes use the
  authoritative staged MutationBatch commit path.
- **Auth**: only the `epistemic` scheme exists. The current `eg2.` request verifier binds graph,
  tenant, audience, policy revision, actor, and scopes into the session. The Bolt `principal` field
  is never authority, and there is no basic-password/auth-mode fallback.
- Explicit transactions buffer detached writes. COMMIT publishes the whole write set once after an
  optimistic version check; ROLLBACK/RESET/disconnect discard it. Acknowledged writes are durable
  before they become visible.
- Basic-only `cypher-shell` authentication is intentionally unsupported by the current-only contract.

## Redis — `redis-cli` / clients (`redis-wire`)

A hand-rolled **RESP2/RESP3** listener (`src/server/redis_wire/`, CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) serving the
core Redis command set over the engine's namespace-scoped KV surface (feature `kv`): strings
(`GET`/`SET`/`DEL`/`EXPIRE`/`INCR`), hashes (`HSET`/`HGET`), lists (`LPUSH`/`LRANGE`), sets
(`SADD`/`SMEMBERS`), sorted sets (`ZADD`/`ZRANGE`), and keyspace `SCAN`.

```bash
EPISTEMIC_GRAPH_REDIS_ADDR=127.0.0.1:6379 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "redis-wire server"

redis-cli -h 127.0.0.1 -p 6379 --user "$REDIS_PRINCIPAL" --pass "$REDIS_CREDENTIAL" SET agent:1 online
redis-cli -h 127.0.0.1 -p 6379 --user "$REDIS_PRINCIPAL" --pass "$REDIS_CREDENTIAL" GET agent:1
```

- **Auth and isolation are mandatory**: `REDIS_CREDENTIAL` is
  `hex(HMAC-SHA256(GRAPH_SERVICE_AUTH_SECRET, "redis:" + REDIS_PRINCIPAL))`.
  The raw deployment secret is resolved by the server and is never a client credential.
  The verified principal is converted to a secret-keyed pseudonym; its durable keys and
  pub/sub channels are isolated from every other principal. Direct Redis binds loopback
  only; remote access terminates TLS/mTLS at an identity-binding gateway. The command set
  is a documented **subset** of Redis, backed by the durable KV store.

## S3 — `aws s3` / MinIO SDKs (`s3-api`)

An **S3-compatible REST API** (`src/server/s3/`, CONCEPT:EG-KG.ontology.object-put-get-head) over the blob CAS: bucket +
object CRUD (`PUT`/`GET`/`DELETE`/`HEAD`/List) with **SigV4-lite** auth, so S3 clients read/write
blobs as objects.

```bash
EPISTEMIC_GRAPH_S3_ADDR=127.0.0.1:9000 \
EPISTEMIC_GRAPH_S3_ACCESS_KEY=agent \
EPISTEMIC_GRAPH_S3_SECRET_KEY=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "s3-api server"

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://docs
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./report.pdf s3://docs/report.pdf
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://docs
```

- **Auth**: SigV4-lite keyed by `EPISTEMIC_GRAPH_S3_ACCESS_KEY` / `…_S3_SECRET_KEY`.
- Objects are stored content-addressed in the same dedup'd, refcount-GC'd blob store as the
  native blob surface (see [kv-blob](kv_blob.md)).

## RabbitMQ — AMQP 0.9.1 client (`amqp-wire` / `broker`) {#rabbitmq-amqp-client}

A hand-rolled **AMQP 0.9.1** server (no heavy AMQP crate) mapping
connection/channel/exchange/queue/`basic.*` frames onto the engine's RabbitMQ-class broker
primitives (exchanges, bindings, topic routing) over the EG-KG.compute.atomically-claim-oldest-pending work-queue. See
[messaging](messaging.md) for the broker semantics (DLQ/TTL/priority/streams/confirms).

```bash
EPISTEMIC_GRAPH_AMQP_ADDR=127.0.0.1:5672 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "amqp-wire server"
```

```python
import pika
credentials = pika.PlainCredentials("publisher", AMQP_PASSWORD)
conn = pika.BlockingConnection(
    pika.ConnectionParameters("127.0.0.1", 5672, credentials=credentials)
)
ch = conn.channel()
ch.queue_declare(queue="tasks")
ch.basic_publish(exchange="", routing_key="tasks", body="hello")
```

- The broker graph is `EPISTEMIC_GRAPH_AMQP_GRAPH` (default `__commons__`). All three broker
  wires (AMQP/MQTT/STOMP) share the **one** broker — a message published over AMQP can be
  consumed over MQTT/STOMP by topic.
- SASL PLAIN is mandatory. `AMQP_PASSWORD` is
  `hex(HMAC-SHA256(secret, "amqp:" + principal))`; the verified principal becomes a
  secret-keyed pseudonymous ACL actor reference before dispatch.

## MQTT — `mosquitto_pub` / IoT (`mqtt-wire`)

An **MQTT 3.1.1 / 5.0** listener (`src/server/mqtt_wire/`, CONCEPT:EG-KG.query.mqtt-packet-codec) mapping
CONNECT/PUBLISH/SUBSCRIBE/PINGREQ/DISCONNECT onto the EG-275 broker (topic exchange + bindings,
QoS 0/1), so MQTT/IoT clients pub/sub over the native broker.

```bash
EPISTEMIC_GRAPH_MQTT_ADDR=127.0.0.1:1883 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "mqtt-wire server"

mosquitto_sub -h 127.0.0.1 -p 1883 -u subscriber -P "$MQTT_SUB_PASSWORD" -t 'sensors/#' &
mosquitto_pub -h 127.0.0.1 -p 1883 -u publisher -P "$MQTT_PUB_PASSWORD" -t 'sensors/room1' -m '21.5'
```

- Broker graph: `EPISTEMIC_GRAPH_MQTT_GRAPH` (default `__commons__`).
- CONNECT username/password are mandatory. Each password is
  `hex(HMAC-SHA256(secret, "mqtt:" + username))`; the verified username becomes a
  secret-keyed pseudonymous ACL actor reference before dispatch.

## STOMP — text-frame clients (`stomp-wire`)

A **STOMP 1.2** text-frame listener (`src/server/stomp_wire/`, CONCEPT:EG-KG.ontology.stomp-frame-codec-unit) mapping
CONNECT/SEND/SUBSCRIBE/ACK/DISCONNECT onto the EG-275 broker.

```bash
EPISTEMIC_GRAPH_STOMP_ADDR=127.0.0.1:61613 \
GRAPH_SERVICE_AUTH_SECRET=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "stomp-wire server"
```

```text
CONNECT
accept-version:1.2
login:publisher
passcode:<hex(HMAC-SHA256(secret, "stomp:publisher"))>

^@
SEND
destination:/queue/tasks

hello^@
```

- Broker graph: `EPISTEMIC_GRAPH_STOMP_GRAPH` (default `__commons__`).
- CONNECT `login`/`passcode` are mandatory; the verified login becomes a secret-keyed
  pseudonymous ACL actor reference before broker dispatch.

## KV-cache — vLLM / LMCache shared blocks (`kvcache-server`)

A gated HTTP surface over the tiered shared KV-cache (`src/server/kvcache_http/`,
CONCEPT:EG-KG.memory.byte-bounded-tiers/186/187), so parallel-deployed vLLM/LMCache instances share LLM KV blocks by
token-hash (dedup + OOM-offload). See [kvcache](kvcache.md) for the tier model and the LMCache
connector contract.

```bash
EPISTEMIC_GRAPH_KVCACHE_ADDR=127.0.0.1:9130 \
EPISTEMIC_GRAPH_KVCACHE_TOKEN=$SECRET \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "kvcache-server server"

auth_header="Authorization: Bearer ${EPISTEMIC_GRAPH_KVCACHE_TOKEN:?}"
curl -s -H "$auth_header" -XPUT --data-binary @block.bin http://127.0.0.1:9130/kv/<token-hash>   # store
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/<token-hash>                                  # fetch
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/<token-hash>/exists                           # exists
curl -s -H "$auth_header" http://127.0.0.1:9130/kv/stats                                         # stats
```

- **Auth**: mandatory verified JWT or runtime-injected bearer
  `EPISTEMIC_GRAPH_KVCACHE_TOKEN` (`Authorization: Bearer …`).
- **Remote transport**: the connector accepts plain HTTP only for explicit
  loopback hosts. Non-loopback endpoints require HTTPS and use standard
  `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, or `SSL_CERT_DIR` trust configuration.

---

## HTTP surfaces

All HTTP listeners are minimal **hand-rolled HTTP/1.1** with no axum/hyper dependency.

### SPARQL 1.1 Protocol (`sparql-http`)

```bash
EPISTEMIC_GRAPH_SPARQL_ADDR=127.0.0.1:7878 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "sparql-http"

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

The GraphQL **read/mutation** surface is reachable via native `Method::GraphQl` dispatch.
`EPISTEMIC_GRAPH_GRAPHQL_ADDR` starts the loopback-only authenticated **SSE subscription
carrier**. It accepts only `GET /graphql/subscribe` with explicit percent-encoded `graph`
and `query` parameters, `Authorization: Bearer eg2.…`, and a positive
`X-Epistemic-Request-Id`. The envelope must be signed over the identical
`Method::GraphQl { query, variables: None }` native request. Graph ACL and default-deny
RLS are rechecked throughout the bounded session; unsigned requests and URL tokens are
rejected. The locked-down subscription policy requires each root to carry an explicit
`first`/`limit` no greater than `100` and applies
depth/complexity/field, syntax-nesting, query-size, frame-size, and write-time limits.
See the [GraphQL guide](graphql.md#subscriptions-authenticated-sse).

```graphql
{ Doc(first: 5) { id title mentions { id } } }
```

The listener has no CORS compatibility path and does not terminate TLS. Remote access
uses a same-host TLS reverse proxy that forwards the two authentication headers to the
loopback address. `EPISTEMIC_GRAPH_GRAPHQL_MAX_CONNECTIONS` (default `128`) and
`EPISTEMIC_GRAPH_GRAPHQL_MAX_SESSION_SECS` (default `300`) bound resource use and force
periodic reauthentication.

### PromQL / Prometheus HTTP query API (`promql`, on the `obs` listener)

```bash
EPISTEMIC_GRAPH_OBS_ADDR=127.0.0.1:5080 \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "promql"

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
degrades to `partial: true`, never fails — CONCEPT:EG-KG.ontology.federation-client). This is a **separate** listener from
`/sparql`, on its own `EPISTEMIC_GRAPH_FEDERATED_ADDR`.

```bash
EPISTEMIC_GRAPH_FEDERATED_ADDR=127.0.0.1:7900 \
EPISTEMIC_GRAPH_FEDERATION_PEERS='http://peer-b:7900,http://peer-c:7900' \
EPISTEMIC_GRAPH_FEDERATION_ALLOW='peer-b,peer-c' \
  epistemic-graph-server --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"   # built --features "federation-search server"

curl -s -XPOST 'http://127.0.0.1:7900/federated' \
  -H 'content-type: application/json' \
  --data '{"query":"SELECT id FROM nodes LIMIT 10","lang":"sql"}'
```

Peers answer each other over `/federated?local=1` (run-locally-only, no re-fan). Distinct from
the in-plan `Op::ForeignScan` federation (EG-KG.query.query-federation) — that composes a single foreign source into
one query plan; `/federated` scatter-gathers a whole query across peer engines.

### Natural-language query (`/nl`, `nl-query`)

`POST /nl` on the **SPARQL** listener turns a natural-language string into a plan and executes it
through the deterministic pipeline (CONCEPT:EG-KG.query.core-query-input/080). It needs `nl-query` **and** an
OpenAI-compatible endpoint (`EPISTEMIC_GRAPH_NL_ENDPOINT` / `…_NL_MODEL` / `…_NL_API_KEY_ENV`);
unconfigured it returns a clear "not configured" error, never a panic.

```bash
curl -s -XPOST 'http://127.0.0.1:7878/nl' \
  -H 'content-type: application/json' \
  --data '{"text":"how many Doc nodes mention Alice?","graph":"my_graph"}'
```

### Prometheus `/metrics` (the engine's own telemetry, `metrics`, default-on)

```bash
epistemic-graph-server --metrics-addr 127.0.0.1:9101 --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}"
curl -s http://127.0.0.1:9101/metrics
```

---

## Embedded (no wire at all) — `embedded`

For the edge / "a local engine per agent" story, skip the socket entirely: open a persist dir
and call core ops as plain methods (SQLite/DuckDB-style, no Tokio / no socket / no HMAC).

```rust
use epistemic_graph::embedded::EmbeddedEngine;
let persist_dir = std::env::var("GRAPH_SERVICE_PERSIST_DIR")?;
let eng = EmbeddedEngine::open(&persist_dir)?;   // built --features "embedded redb"
eng.add_node("my_graph", "n1", br#"{"label":"Doc"}"#.to_vec())?;
```

---

See the [capability matrix](../capabilities.md) for the operation-by-operation truth per
surface and the [operations runbook](../operations/runbook.md) for the full env-var catalog,
tiers, backup/PITR, and RBAC.

---
*CONCEPT:EG-KG.ontology.comprehensive-interface-operations-documentation — comprehensive interface + operations documentation.*

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](sql.md) · [SPARQL & RDF](sparql.md) · [Cypher & Bolt](cypher.md) · [Messaging & Broker](messaging.md) · [Key-value & Blob](kv_blob.md) · [Client Drivers](clients.md).
