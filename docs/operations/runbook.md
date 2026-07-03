# Operations runbook

Everything an operator needs to run epistemic-graph as a durable database: the deployment
tiers, the full env-var catalog for every listener and knob, backup / point-in-time recovery,
RBAC, encryption-at-rest, and how to turn each modality on. For the per-client connect recipes
see [`interfaces/connecting.md`](../interfaces/connecting.md); for the storage internals see
the [architecture](../architecture/engine.md) pages.

## 1. One build + opt-in layers (feature bundles)

The engine is **one build** (CONCEPT:EG-371): `cargo build` (== `--features full`) is the whole
full-featured engine — every MAIN feature that compiles without an external GPU/robotics toolchain.
Two opt-in layers stack on top. **The invariants still hold** (asserted by `cargo tree`): the main
build links **no openraft** (that is the `cluster` layer) and **no cudarc/rustdds** (that is the
`full-extras` layer); it links DataFusion (SQL is a main feature now). It targets Raspberry Pi 4+.

| Build | What it is | Typical target |
|-------|-----------|----------------|
| **main** (`default` = `full` = `all`) | The whole single-node DB: redb-authoritative + Cypher + SQL/DataFusion + GraphQL + ANN + RDF/SPARQL/OWL + TSDB + blob + KV + full-text + GeoSPARQL + federation + spatial/tensor/stream + WASM UDF + security + obs + the whole wire family (`pgwire`/`mysql`/`mssql`/`sqlite`/`bolt`/`redis`/`amqp`/`mqtt`/`stomp`) + `numeric` + `kvcache-server` + `broker` | anything single-node, Raspberry Pi 4+ → workstation |
| **+ `cluster`** | main + in-engine Raft replication (EG/KG-2.188) + distributed compute + cross-shard 2PC | multi-node HA |
| **+ `full-extras`** | main + `gpu-cuda` + `ros2-bridge`/`ros2-dds` | GPU / robotics |

```bash
cargo build --release                        # the main build (== --features full)
cargo build --release --no-default-features --features cluster,ast-extended      # HA layer
cargo build --release --no-default-features --features full-extras,ast-extended  # GPU/ROS2 layer
# a la carte: pick exactly the surfaces you want
cargo build --release --no-default-features --features "server pgwire bolt-wire promql traces security"
```

The whole wire family — `pgwire`, `mysql-wire`, `mssql-wire`, `sqlite-wire`, `bolt-wire`,
`redis-wire`, `amqp-wire`, `mqtt-wire`, `stomp-wire` — plus `broker`, `obs`, `otel`, `otel-export`,
`shacl`, `shex`, `nl-query`, `geosparql`, `federation-search`, `kvcache-server` are all part of the
one main build now. Only `raft`/`compute-dist` (the `cluster` layer) and `gpu-cuda`/`ros2-*` (the
`full-extras` layer) are opt-in. See [one build, opt-in layers](../architecture/tiers.md).

## 2. Core process configuration

| Var / flag | Purpose | Default |
|------------|---------|---------|
| `--persist-dir` / `GRAPH_SERVICE_PERSIST_DIR` | Durable redb store directory. **Unset ⇒ IN-MEMORY ONLY** (data lost on exit). | (none) |
| `--socket-path` / `GRAPH_SERVICE_SOCKET` | Native UDS listener path (the primary transport). | per-platform default |
| `--tcp-addr` / `GRAPH_SERVICE_TCP_FALLBACK_ADDR` | Native TCP fallback for the client protocol. | `127.0.0.1:8765` |
| `GRAPH_SERVICE_AUTH_SECRET` | HMAC secret for the native protocol + SCRAM for the SQL wires. **Set this in production.** | (empty) |
| `EPISTEMIC_GRAPH_ALLOW_INSECURE` | Opt out of the empty-secret guard (dev only). | off |
| `EPISTEMIC_GRAPH_PERSIST_BACKEND` / `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` | Select / confirm the redb-authoritative durable path (KG-2.195). | authoritative |
| `EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS` | Auto-shutdown after idle (edge/serverless). | off |

> **Loopback default everywhere.** A bare enable token or bare port binds `127.0.0.1`, never
> `0.0.0.0`. Terminate TLS/mTLS at the edge (Caddy/ingress) — the wire listeners speak
> plaintext on the wire they emulate.

## 3. Listener / env-var catalog

Every listener is opt-in (feature **and** address must be set). Full connect examples in
[`connecting.md`](../interfaces/connecting.md).

| Surface | Feature | Listen-addr env (flag) | Default | Aux env |
|---------|---------|------------------------|---------|---------|
| Native client (UDS/TCP) | (core) | `GRAPH_SERVICE_SOCKET` / `…_TCP_FALLBACK_ADDR` | UDS; `127.0.0.1:8765` | `GRAPH_SERVICE_AUTH_SECRET` |
| Postgres wire | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | `127.0.0.1:5433` | `EPISTEMIC_GRAPH_PGWIRE_AUTH`, `…_PGWIRE_GRAPH` |
| MySQL wire | `mysql-wire` | `EPISTEMIC_GRAPH_MYSQL_ADDR` | `127.0.0.1:3306` | `EPISTEMIC_GRAPH_MYSQL_AUTH`, `…_MYSQL_GRAPH` |
| MSSQL wire | `mssql-wire` | `EPISTEMIC_GRAPH_MSSQL_ADDR` | `127.0.0.1:1433` | `EPISTEMIC_GRAPH_MSSQL_GRAPH` |
| SQLite NDJSON | `sqlite-wire` | `EPISTEMIC_GRAPH_SQLITE_ADDR` | (your port) | `EPISTEMIC_GRAPH_SQLITE_GRAPH` |
| Bolt wire (Neo4j) `EG-159` | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` | `127.0.0.1:7687` | `EPISTEMIC_GRAPH_BOLT_GRAPH` |
| Redis RESP wire `EG-174` | `redis-wire` | `EPISTEMIC_GRAPH_REDIS_ADDR` | `127.0.0.1:6379` | `EPISTEMIC_GRAPH_REDIS_PASSWORD` |
| AMQP broker `EG-275` | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` | `EPISTEMIC_GRAPH_AMQP_GRAPH` |
| MQTT broker `EG-281` | `mqtt-wire` | `EPISTEMIC_GRAPH_MQTT_ADDR` | `127.0.0.1:1883` | `EPISTEMIC_GRAPH_MQTT_GRAPH`, `…_MQTT_EXCHANGE` |
| STOMP broker `EG-282` | `stomp-wire` | `EPISTEMIC_GRAPH_STOMP_ADDR` | `127.0.0.1:61613` | `EPISTEMIC_GRAPH_STOMP_GRAPH`, `…_STOMP_EXCHANGE` |
| S3 REST (object store) `EG-176` | `s3-api` | `EPISTEMIC_GRAPH_S3_ADDR` | `127.0.0.1:9000` | `EPISTEMIC_GRAPH_S3_ACCESS_KEY`, `…_S3_SECRET_KEY` |
| Shared KV-cache HTTP `EG-187` | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` | `127.0.0.1:9130` | `EPISTEMIC_GRAPH_KVCACHE_TOKEN` |
| SPARQL HTTP + `/nl` | `sparql-http` (+ `nl-query`) | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` | `EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH`, `…_SPARQL_SERVICE_ALLOW`; NL: `…_NL_ENDPOINT`, `…_NL_MODEL`, `…_NL_API_KEY_ENV` |
| GraphQL SSE | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` | — |
| Obs: logs + PromQL + traces + VRL | `obs`/`promql`/`traces` | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` | `EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS` |
| Federated search `EG-243` | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (`--federated-addr`) | `127.0.0.1:7900` | `EPISTEMIC_GRAPH_FEDERATION_PEERS`, `…_FEDERATION_ALLOW` |
| Prometheus `/metrics` | `metrics` (default) | `GRAPH_SERVICE_METRICS_ADDR` (`--metrics-addr`) | `127.0.0.1:9101` | — |
| OTLP span **export** | `otel` | `EPISTEMIC_GRAPH_OTLP_ENDPOINT` | (off) | — |

### Operational tuning knobs

| Var | Effect |
|-----|--------|
| `EPISTEMIC_GRAPH_WAL_FSYNC` | Durability/latency: `off` \| `each` \| `<ms>` \| `interval` (default 100ms). `each` = fsync-per-commit. |
| `EPISTEMIC_GRAPH_WAL_QUEUE` | WAL apply queue depth. |
| `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` / `…_REDB_GROUP_SHALLOW` | Group-commit micro-linger window / shallow batch (EG-024). |
| `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` / `…_REDB_SHARDS` | redb flush threshold / shard count. |
| `EPISTEMIC_GRAPH_WRITE_COALESCE` | Per-graph write-coalescer window (EG-012). |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` / `…_MAX_INFLIGHT_PER_GRAPH` | Admission control / back-pressure. |
| `EPISTEMIC_GRAPH_READ_RESERVED` | Reserved read-admission lane (EG-044) — keep reads fast under a write firehose. |
| `EPISTEMIC_GRAPH_RESULT_CACHE_CAP` | Version-keyed result-cache capacity (KG-2.233). |
| `EPISTEMIC_GRAPH_SLOW_QUERY_MS` | Slow-query log threshold. |
| `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` / `…_INDEXED_JSON_PATHS` | Secondary index hints (+ `…_MAX_*` caps). |
| `EPISTEMIC_GRAPH_MEMORY_BUDGET` / `…_TENANT_BUDGET` / `…_MEMCAP_INTERVAL` / `…_BUDGET_INTERVAL` | Per-tenant memory budget + autoscale signals (KG-2.234, `cost`). |
| `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS` | Cold-tier offload idle threshold (KG-2.233 `cold-tier`). |
| `EPISTEMIC_GRAPH_TXN_TTL_SECS` / `…_TXN_MAX_PER_AGENT` / `…_TXN_MAX_PER_GRAPH` | Interactive-transaction TTL / caps. |
| `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH` / `…_MAX_RESPONSE_NODES` | Per-graph node ceiling / response cap. |
| `EPISTEMIC_GRAPH_TENANT_CATALOG` | Multi-tenant catalog path. |
| `GRAPH_SERVICE_DECAY_HALF_LIFE` / `…_DECAY_FLOOR` / `…_DECAY_INTERVAL` | Ebbinghaus fact-decay (agent memory). |

## 4. Backup, restore & PITR (CONCEPT:EG-090)

The redb-authoritative store supports **online** consistent backups (no stop-the-world; the
bundle is a crash-consistent point-in-time image). A bundle is a portable shard set +
`MANIFEST.json` (format version, engine version, shard count K, per-shard digests).

**Online backup** — over the native protocol, while serving:

```jsonc
// Method::Backup  → returns a BackupReport JSON
{ "Backup": { "destination": "/backups/eg-2026-07-02", "label": "nightly" } }
```

**Restore** — two paths:

```jsonc
// Method::Restore stages a rebuilt copy in a sibling dir (swap in after stopping the engine)
{ "Restore": { "source": "/backups/eg-2026-07-02" } }
```

```bash
# Offline in-place restore CLI (engine stopped). Re-shard-on-restore: pass --shards K.
restore --bundle /backups/eg-2026-07-02 --persist-dir /var/lib/epistemic-graph
restore --bundle /backups/eg-2026-07-02 --persist-dir /var/lib/eg-k8 --shards 8
```

Restoring at the bundle's own K is a 1:1 verbatim row import; a different K re-shards on
restore (EG-030 routing). **PITR**: the backup is the base image; replay the durable change
ledger / WAL forward to a target instant on top of a restored bundle. Both halves (backup +
restore) ship here; the forward-replay driver rides the same ledger.

Non-redb builds return a clean "not available" error for `Backup`/`Restore` (no panic).

## 5. RBAC (CONCEPT:EG-092, feature `security`)

RBAC-at-scale is a pure-Rust policy evaluator: an agent carries a set of **role** names; roles
form an inheritance hierarchy (a role gets every grant of its transitively-reachable parents);
`Grant`s bind a role to a `(resource, action, effect)` triple. `RbacPolicy::evaluate` returns
the winning effect (deny-overrides, most-specific-wins).

- An agent presents its roles on the request (`roles`, `#[serde(default)]` ⇒ pre-RBAC clients
  stay wire-compatible with an empty set).
- Administer the policy with `Method::RbacAdmin { op }` (add/remove role, grant/revoke).
- The identity flows through the SQL wires too: a pgwire `user` (SCRAM) becomes the engine ACL
  actor, so **Row-Level Security** (below) applies to every wire query.

## 6. Security-at-rest, RLS & audit (CONCEPT:KG-2.231, feature `security`)

| Capability | Enable | Notes |
|------------|--------|-------|
| Row-Level Security (per-agent read/plan-path view filter) | `security` | pure-Rust; applies across every wire |
| Encryption-at-rest (redb value blobs) | `security` + `EPISTEMIC_GRAPH_ENCRYPTION_KEY` | ChaCha20-Poly1305 (RustCrypto — no ring/openssl); rides the redb tier |
| Hash-chained tamper-evident audit log | `security` | over the durable ledger (sha2/hmac) |

`security` is part of the one main build (and therefore the `cluster` layer).

## 7. Enabling each modality (feature ⇒ what lights up)

| Want | Build with | Then set |
|------|-----------|----------|
| SQL (in-engine) | `query` | — (native `Method::Sql`) |
| Postgres clients (+ pg_catalog / AGE / pgvector / Timescale / ParadeDB `EG-103`/`114`/`116`/`117`/`119`) | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` |
| MySQL / MSSQL / SQLite clients | `mysql-wire` / `mssql-wire` / `sqlite-wire` | the matching `*_ADDR` |
| Bolt (Neo4j drivers) `EG-159` | `bolt-wire` (impl `cypher`) | `EPISTEMIC_GRAPH_BOLT_ADDR` |
| Redis clients (RESP2/3) `EG-174` | `redis-wire` (impl `kv`) | `EPISTEMIC_GRAPH_REDIS_ADDR` |
| Cypher | `cypher` (in `pi`) | — / Bolt for remote drivers |
| GraphQL (+ Apollo Federation / hardening `EG-295`/`296`) | `graphql` | — / `EPISTEMIC_GRAPH_GRAPHQL_ADDR` for SSE |
| SPARQL / RDF / OWL (+ JSON-LD/TriG/RDF-XML/ShEx/ICV/GSP `EG-133`..`137`/`146`) | `sparql` (in `pi`) / `owl` / `shex` | `EPISTEMIC_GRAPH_SPARQL_ADDR` for HTTP |
| GeoSPARQL / spatial (+ RCC8/Egenhofer `EG-155`/`261`) | `geosparql` / `geo` | — |
| GIS / logistics (CRS/R-tree/tiles/routing/map-tasks `EG-255`..`267`) | `geo` | — |
| Vector ANN (+ exact/flat recall harness `EG-297`) | `ann` (in `pi`) | — |
| Time-series | `tsdb` | — |
| Blob / CAS | `blob` (+ `blob-s3` for S3/MinIO) | `EPISTEMIC_GRAPH_BLOB_*` |
| S3-compatible object store (serve) `EG-176` | `s3-api` (impl `blob`+`kv`) | `EPISTEMIC_GRAPH_S3_ADDR` |
| Key-value | `kv` | — |
| Full-text (+ ParadeDB BM25 `EG-119`) | `text` | — |
| Message broker (queues/exchanges/streams/DLQ/TTL `EG-275`..`284`) | `broker` | `Method::*` broker ops |
| Message broker wires (AMQP / MQTT / STOMP) `EG-275`/`281`/`282` | `amqp-wire` / `mqtt-wire` / `stomp-wire` (impl `broker`) | `EPISTEMIC_GRAPH_{AMQP,MQTT,STOMP}_ADDR` |
| Observability (logs / PromQL / traces / VRL `EG-163`/`165`/`172`) | `obs` / `promql` / `traces` | `EPISTEMIC_GRAPH_OBS_ADDR` |
| Natural-language query `EG-078`/`080` | `nl-query` | `EPISTEMIC_GRAPH_NL_ENDPOINT` / `…_NL_MODEL` / `…_NL_API_KEY_ENV` (served on `/nl`) |
| Agent memory (summary tier / consolidation / LeanRAG `EG-195`/`220`..`222`) | (core / `ann`) | `Method::*` memory ops |
| LLM KV-cache server (vLLM/LMCache share) `EG-185`..`187` | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` (+ `…_KVCACHE_TOKEN`) |
| WASM UDF | `wasm-udf` | — (`RegisterUdf`/`RunUdf`) |
| Federation (remote / HTTP / external SQL) | `federation` / `federation-sql` | per-source spec |
| Super-cluster federated search `EG-243` | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (+ `…_FEDERATION_PEERS` / `…_FEDERATION_ALLOW`) |
| Clustering / HA | `raft` (in `cluster`) | `EPISTEMIC_GRAPH_RAFT_NODE_ID` + `…_RAFT_PEERS` (+ `…_RAFT_BIND_ADDR`) |

## 8. Observability of the engine itself

- **Metrics**: `--metrics-addr` exposes Prometheus counters/gauges (request/latency/in-flight,
  per-graph gauges). Scrape it.
- **Tracing/spans**: `otel` + `EPISTEMIC_GRAPH_OTLP_ENDPOINT` exports the engine's existing
  `tracing` spans over OTLP (additive; installs only when both are set).
- **Slow queries**: `EPISTEMIC_GRAPH_SLOW_QUERY_MS`.

!!! note "Deferred — admin console UI (🗺)"
    Tenant / shard / RBAC / backup-PITR administration is fully driveable over the **APIs + CLI** today
    (`Method::Backup`/`Restore`, the RBAC admin surface, `GroupRouter` resharding). A browser **admin
    console** over those APIs is designed but not built. See the [forward roadmap](../roadmap.md).

---

**See also:** [Capabilities matrix](../capabilities.md) · [Deployment (database)](../deployment.md) ·
[Cost Model & Capacity](../cost_model.md) · [Cluster Deployment](../architecture/cluster_deployment.md) ·
[Observability](../interfaces/observability.md).

---
*CONCEPT:EG-095 — comprehensive interface + operations documentation.*
