# Operations runbook

Everything an operator needs to run epistemic-graph as a durable database: the deployment
tiers, the full env-var catalog for every listener and knob, backup / point-in-time recovery,
RBAC, encryption-at-rest, and how to turn each modality on. For the per-client connect recipes
see [`interfaces/connecting.md`](../interfaces/connecting.md); for the storage internals see
the [architecture](../architecture/engine.md) pages.

## 1. One build + opt-in layers (feature bundles)

The engine is **one build** (CONCEPT:EG-KG.sharding.deployment-tiers): `cargo build` (== `--features full`) is the whole
full-featured engine — every MAIN feature that compiles without an external GPU/robotics toolchain.
Two opt-in layers stack on top. **The invariants still hold** (asserted by `cargo tree`): the main
build links **no openraft** (that is the `cluster` layer) and **no cudarc/rustdds** (that is the
`full-extras` layer); it links DataFusion (SQL is a main feature now). It targets Raspberry Pi 4+.

| Build | What it is | Typical target |
|-------|-----------|----------------|
| **main** (`default` = `full` = `all`) | The whole single-node DB: redb-authoritative + Cypher + SQL/DataFusion + GraphQL + ANN + RDF/SPARQL/OWL + TSDB + blob + KV + full-text + GeoSPARQL + federation + spatial/tensor/stream + WASM UDF + security + obs + the whole wire family (`pgwire`/`mysql`/`mssql`/`sqlite`/`bolt`/`redis`/`amqp`/`mqtt`/`stomp`) + `numeric` + `kvcache-server` + `broker` | anything single-node, Raspberry Pi 4+ → workstation |
| **+ `cluster`** | main + in-engine Raft replication (EG/AU-KG.ingest.source-sync-canonical) + distributed compute + cross-shard 2PC | multi-node HA |
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
| `--persist-dir` / `GRAPH_SERVICE_PERSIST_DIR` | Durable redb store and request-replay ledger directory. **Required for served mode.** | (none) |
| `--socket-path` / `GRAPH_SERVICE_SOCKET` | Native UDS listener path (the primary transport). | per-platform default |
| `--tcp-addr` / `GRAPH_SERVICE_TCP_ADDR` | Native TCP listener; a routable bind requires TLS. | disabled |
| `GRAPH_SERVICE_TLS_CERT` / `GRAPH_SERVICE_TLS_KEY` | PEM server identity for native TCP TLS. Both are required together. | (none) |
| `GRAPH_SERVICE_TLS_CLIENT_CA` | Optional client CA bundle; setting it requires mTLS. | (none) |
| `GRAPH_SERVICE_AUTH_SECRET` | Non-empty HMAC secret for `eg2.` plus protocol-specific credential derivation. **Required.** | (none) |
| `EPISTEMIC_GRAPH_ENCRYPTION_KEY` | Encryption-at-rest key material for redb value blobs (feature `security`). Keep it in the deployment secret/KMS boundary; it is never persisted or logged. | (none) |
| `EPISTEMIC_GRAPH_ENCRYPTION_KEY_ID` | Stable, non-secret KMS object identifier pinned into each encrypted shard and backup manifest. Existing single-secret deployments default to `legacy`; changing it is an explicit offline rotation boundary. | `legacy` |
| `EPISTEMIC_GRAPH_ENCRYPTION_KEY_VERSION` | Stable, non-secret key version pinned beside the encryption canary. Existing single-secret deployments default to `1`; changing it is an explicit offline rotation boundary. | `1` |
| `EPISTEMIC_GRAPH_ENCRYPTION_REQUIRED` | Missing-key posture: `off` / `warn` / `on`. Unset/unrecognized ⇒ `warn`; `on` refuses startup before writer/listener admission. | `warn` |
| `EPISTEMIC_GRAPH_REQUIRE_OIDC` | MANDATORY-OIDC posture (since 2026-07-22). Unset/unrecognized ⇒ **required**: refuses to start without a configured OIDC verifier. `false`/`0`/`no`/`off` is the explicit, deliberate local/dev opt-out. See [deployment.md § Migrating to OIDC-required](../deployment.md#migrating-to-oidc-required). | **required** |
| `EPISTEMIC_GRAPH_OIDC_JWT_ISSUER` / `_AUDIENCE` / `EPISTEMIC_GRAPH_OIDC_JWKS_URL` | Keycloak realm issuer / audience / JWKS URL. **Required unless `EPISTEMIC_GRAPH_REQUIRE_OIDC` is explicitly opted out.** | (none) |
| `EPISTEMIC_GRAPH_AUDIENCE` | Exact non-empty request audience. **Required.** | (none) |
| `EPISTEMIC_GRAPH_TENANT` | Exact non-empty request tenant. **Required.** | (none) |
| `EPISTEMIC_GRAPH_POLICY_VERSION` | Exact non-empty authorization-policy revision. **Required.** | (none) |
| `EPISTEMIC_GRAPH_SIGNER_KEYS_JSON` | Runtime secret map of trusted operation signer ids to HMAC keys. **Required.** | (none) |
| `EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS` | Auto-shutdown after idle (edge/serverless). | off |

> **Strict listener boundary.** A bare auxiliary enable token or port binds
> `127.0.0.1`, never `0.0.0.0`, and every non-loopback auxiliary bind is rejected.
> Routable native MessagePack TCP requires TLS/mTLS. Place a co-located
> authenticated TLS gateway in front of an auxiliary protocol when remote access
> is required.

## 3. Listener / env-var catalog

Every listener is opt-in (feature **and** address must be set). Full connect examples in
[`connecting.md`](../interfaces/connecting.md).

| Surface | Feature | Listen-addr env (flag) | Default | Aux env |
|---------|---------|------------------------|---------|---------|
| Native client (UDS/TCP/TLS) | (core; TLS in `server-tls`, included by `full`) | `GRAPH_SERVICE_SOCKET` / `GRAPH_SERVICE_TCP_ADDR` | platform runtime UDS; TCP disabled | `GRAPH_SERVICE_AUTH_SECRET`, `GRAPH_SERVICE_TLS_*` |
| Postgres wire | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` | `127.0.0.1:5433` | `EPISTEMIC_GRAPH_PGWIRE_AUTH`, `…_PGWIRE_GRAPH` |
| MySQL wire | `mysql-wire` | `EPISTEMIC_GRAPH_MYSQL_ADDR` | `127.0.0.1:3306` | `EPISTEMIC_GRAPH_MYSQL_AUTH`, `…_MYSQL_GRAPH` |
| MSSQL wire | `mssql-wire` | `EPISTEMIC_GRAPH_MSSQL_ADDR` | `127.0.0.1:1433` | `EPISTEMIC_GRAPH_MSSQL_GRAPH` |
| SQLite NDJSON | `sqlite-wire` | `EPISTEMIC_GRAPH_SQLITE_ADDR` | (your port) | `EPISTEMIC_GRAPH_SQLITE_GRAPH` |
| Bolt wire (Neo4j) `EG-KG.query.bolt-wire-protocol` | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` | `127.0.0.1:7687` | signed `eg2.` session request binds the graph; no graph/auth-mode env |
| Redis RESP wire `EG-KG.ontology.resp2-resp3-codec-round` | `redis-wire` | `EPISTEMIC_GRAPH_REDIS_ADDR` | `127.0.0.1:6379` | `AUTH <principal> <hex(HMAC-SHA256(GRAPH_SERVICE_AUTH_SECRET, "redis:" + principal))>`; isolated pseudonymous keyspace |
| AMQP broker `EG-275` | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` | `EPISTEMIC_GRAPH_AMQP_GRAPH` |
| MQTT broker `EG-281` | `mqtt-wire` | `EPISTEMIC_GRAPH_MQTT_ADDR` | `127.0.0.1:1883` | `EPISTEMIC_GRAPH_MQTT_GRAPH`, `…_MQTT_EXCHANGE` |
| STOMP broker `EG-KG.ontology.stomp-frame-codec-unit` | `stomp-wire` | `EPISTEMIC_GRAPH_STOMP_ADDR` | `127.0.0.1:61613` | `EPISTEMIC_GRAPH_STOMP_GRAPH`, `…_STOMP_EXCHANGE` |
| S3 REST (object store) `EG-KG.ontology.object-put-get-head` | `s3-api` | `EPISTEMIC_GRAPH_S3_ADDR` | `127.0.0.1:9000` | `EPISTEMIC_GRAPH_S3_ACCESS_KEY`, `…_S3_SECRET_KEY` |
| Shared KV-cache HTTP `EG-KG.backend.is-configured-so-co` | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` | `127.0.0.1:9130` | `EPISTEMIC_GRAPH_KVCACHE_TOKEN` |
| SPARQL HTTP + `/nl` | `sparql-http` (+ `nl-query`) | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` | `EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH`, `…_SPARQL_SERVICE_ALLOW`; NL: `…_NL_ENDPOINT`, `…_NL_MODEL`, `…_NL_API_KEY_ENV` |
| GraphQL SSE | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` | `EPISTEMIC_GRAPH_GRAPHQL_MAX_CONNECTIONS`, `EPISTEMIC_GRAPH_GRAPHQL_MAX_SESSION_SECS` |
| Obs: logs + PromQL + traces + VRL | `obs`/`promql`/`traces` | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` | `EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS` |
| Federated search `EG-KG.ontology.federation-client` | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (`--federated-addr`) | `127.0.0.1:7900` | `EPISTEMIC_GRAPH_FEDERATION_PEERS`, `…_FEDERATION_ALLOW` |
| Prometheus `/metrics` | `metrics` (default) | `GRAPH_SERVICE_METRICS_ADDR` (`--metrics-addr`) | `127.0.0.1:9101` | — |
| OTLP span **export** | `otel` | `EPISTEMIC_GRAPH_OTLP_ENDPOINT` | (off) | — |

### Operational tuning knobs

| Var | Effect |
|-----|--------|
| `EPISTEMIC_GRAPH_REDB_COMMIT_POLICY` | Commit policy: `each`, `interval`, or positive milliseconds. Invalid values and zero fail startup; durability cannot be disabled. |
| `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` / `…_REDB_GROUP_SHALLOW` | Group-commit micro-linger window / shallow batch (EG-024). |
| `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` / `…_REDB_SHARDS` | redb flush threshold / shard count. Every K uses `graph-<n>.redb`; K=1 is `graph-0.redb`. |
| `EPISTEMIC_GRAPH_MAX_INFLIGHT` / `…_MAX_INFLIGHT_PER_GRAPH` | Admission control / back-pressure. |
| `EPISTEMIC_GRAPH_READ_RESERVED` | Reserved read-admission lane (EG-KG.coordination.reserved-read-lane) — keep reads fast under a write firehose. |
| `EPISTEMIC_GRAPH_RESULT_CACHE_CAP` | Version-keyed result-cache capacity (KG-2.233). |
| `EPISTEMIC_GRAPH_CYPHER_PLAN_CACHE` | Process-wide Cypher AST/plan-cache capacity, keyed on query text (`0` disables). Schema-independent — never invalidated by a write. |
| `EPISTEMIC_GRAPH_SLOW_QUERY_MS` | Slow-query log threshold. |
| `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` / `…_INDEXED_JSON_PATHS` | Secondary index hints (+ `…_MAX_*` caps). |
| `EPISTEMIC_GRAPH_MEMORY_BUDGET` / `…_TENANT_BUDGET` / `…_MEMCAP_INTERVAL` / `…_BUDGET_INTERVAL` | Per-tenant memory budget + autoscale signals (EG-KG.compute.lane-v, `cost`). |
| `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS` | Cold-tier offload idle threshold (KG-2.233 `cold-tier`). |
| `EPISTEMIC_GRAPH_TXN_TTL_SECS` / `…_TXN_MAX_PER_AGENT` / `…_TXN_MAX_PER_GRAPH` | Interactive-transaction TTL / caps. |
| `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH` / `…_MAX_RESPONSE_NODES` | Per-graph node ceiling / response cap. |
| `EPISTEMIC_GRAPH_MAX_REQUEST_BYTES` / `…_MAX_RESPONSE_BYTES` | Native protocol frame limits (bounded by a hard engine ceiling). |
| `EPISTEMIC_GRAPH_CONNECTION_IO_TIMEOUT_SECS` / `…_TLS_HANDSHAKE_TIMEOUT_SECS` | Native connection I/O and TLS-handshake deadlines. |
| `EPISTEMIC_GRAPH_GRAPHQL_MAX_CONNECTIONS` | GraphQL SSE process-wide cap across handshakes and sessions; `1..=10000`, default `128`, outside the range fails startup. |
| `EPISTEMIC_GRAPH_GRAPHQL_MAX_SESSION_SECS` | Maximum GraphQL SSE session lifetime before a newly signed eg2 request is required; `1..=3600` seconds, default `300`, outside the range fails startup. |
| `EPISTEMIC_GRAPH_TENANT_CATALOG` | Multi-tenant catalog path. |
| `GRAPH_SERVICE_DECAY_HALF_LIFE` / `…_DECAY_FLOOR` / `…_DECAY_INTERVAL` | Ebbinghaus fact-decay (agent memory). |

## 4. Backup, restore & PITR (CONCEPT:EG-KG.sharding.reshard-on-restore)

The redb-authoritative store supports **online** consistent format-v4 backups without
a global stop-the-world. Each attempt brackets the per-shard MVCC copies with
cryptographic change tokens for the admin saga ledger and cross-shard
prepare/decision state. A transition during the copy leaves the attempt unpublished
and the caller retries. A complete bundle contains the portable graph shard set,
`admin-mutations.redb`, the non-shard durable stores a restore is incomplete without
(`rbac.redb`, `kv.redb`, `node_info.redb`, `catalog.redb`), and `MANIFEST.json` with
aggregate graph, receipt, encrypted recovery-plan and cross-shard-decision counts plus
exact portable-file digests. The manifest is file-synced and atomically published only
after those stores validate.

**Read the manifest's scope before trusting a restore.** `bundled_stores` lists every
non-shard durable store the bundle carries, with its copied row count; `excluded_stores`
names every durable store the bundle deliberately does NOT carry, with the reason. The
notable exclusion is `blob.redb` — content-addressed blob bytes are unbounded in size
and are not copied into a bundle, so a restore leaves blob references dangling until an
operator copies `blob.redb` alongside the bundle. A bundle whose `bundled_stores` map is
**empty** was written before this scope was declared: restoring it brings back graph
shards and coordinator receipts only, and the engine comes up with NO RBAC/identity
state — no roles, no grants, no registered identities, and a `Consumed` bootstrap that
cannot be reopened. Treat such a bundle as a graph-only recovery point and re-establish
identity out of band.

Normal startup accepts only a contiguous canonical `graph-<n>.redb` set and fails on
the retired unindexed `graph.redb`. With the engine stopped, convert that one-time
persisted layout in place (leaving a timestamped backup) with:

```bash
migrate-shards --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}" --shards 1
```

Restore validates all aggregate counts against the copied stores. Coordinator
receipt/idempotency/child links must remain internally bound, and private prepared
plans must remain authenticated ciphertext. Backup and restore command output exposes
only aggregate counts and opaque digests; never store locations, endpoints, graph
content or identity values in operational evidence.

**Online backup** — over the native protocol, while serving:

Provision a dedicated private directory at deployment time and expose its path only
through `EPISTEMIC_GRAPH_BACKUP_ROOT` (mode `0700` on Unix). The wire request carries a
logical bundle name, never a host path. Without the root, both remote DR methods fail
closed. Backup publication is staged and renamed atomically inside that root.

```jsonc
// Method::Backup  → returns a BackupReport JSON
{ "Backup": { "destination": "scheduled-001", "label": "scheduled" } }
```

**Restore** — two paths:

```jsonc
// Method::Restore requires the intended current shard layout and returns an opaque stage_ref.
{ "Restore": { "source": "scheduled-001", "target_shards": 1 } }
```

```bash
# Offline in-place restore CLI (engine stopped). The target K is always explicit.
restore --bundle "${BACKUP_BUNDLE:?}" --persist-dir "${GRAPH_SERVICE_PERSIST_DIR:?}" --shards 1
restore --bundle "${BACKUP_BUNDLE:?}" --persist-dir "${TARGET_PERSIST_DIR:?}" --shards 8
```

The wire request always states `target_shards`; restoring at the bundle's own K is a
1:1 verbatim row import, while a different K re-shards on
restore (EG-030 routing). **PITR**: the backup is the base image; replay the durable change
ledger forward to a target instant on top of a restored bundle. Both halves (backup +
restore) ship here; the forward-replay driver rides that ledger.

Non-redb builds return a clean "not available" error for `Backup`/`Restore` (no panic).

## 5. RBAC (CONCEPT:EG-KG.compute.feature, feature `security`)

RBAC-at-scale is a pure-Rust policy evaluator: an agent carries a set of **role** names; roles
form an inheritance hierarchy (a role gets every grant of its transitively-reachable parents);
`Grant`s bind a role to a `(resource, action, effect)` triple. `RbacPolicy::evaluate` returns
the winning effect (deny-overrides, most-specific-wins).

- Roles and scopes come from the verified `eg2.` authority envelope; unsigned
  request fields never grant access.
- Administer the policy with `Method::RbacAdmin { op }` (add/remove role, grant/revoke).
- The identity flows through the SQL wires too: a pgwire `user` (SCRAM) becomes the engine ACL
  actor, so **Row-Level Security** (below) applies to every wire query.

A fresh durable policy store denies every ordinary graph and admin action. Its
only admitted mutation is a trusted-signer-backed `RegisterIdentity` in
`__commons__` that registers the verified principal/effective agent itself as
`System`, with no teams, roles, or delegation and exactly the
`security:bootstrap` scope. After that first rule commits, all identity and RBAC
administration requires the normal durable admin policy.

## 6. Security-at-rest, RLS & audit (CONCEPT:EG-KG.sharding.row-level-security, feature `security`)

| Capability | Enable | Notes |
|------------|--------|-------|
| Row-Level Security (per-agent read/plan-path view filter) | always in served mode | strict/default-deny across every wire; no runtime toggle |
| Encryption-at-rest (redb value blobs) | `security` + `EPISTEMIC_GRAPH_ENCRYPTION_KEY` | ChaCha20-Poly1305 (RustCrypto — no ring/openssl); rides the redb tier |
| Hash-chained tamper-evident audit log | `security` | over the durable ledger (sha2/hmac) |

### Encryption key lifecycle (NE-028 / BUG-248)

Each encrypted shard stores an AEAD-sealed canary whose plaintext binds the configured
key identity and version, plus a non-secret copy of that reference.  Startup verifies
both before the redb writer thread or any listener is admitted.  The old
single-secret configuration remains compatible as `legacy@1`; new deployments should
set `EPISTEMIC_GRAPH_ENCRYPTION_KEY_ID` and
`EPISTEMIC_GRAPH_ENCRYPTION_KEY_VERSION` from the KMS record.

The key reference is a pin, not a secret.  A changed ID/version is deliberately a
rotation boundary: the engine refuses to open with a bounded diagnostic and never
auto-rekeys a live store.  This prevents a crash halfway through a rewrite from
leaving a mixture that is unreadable after restart.  Do not delete or overwrite the
old key until the replacement store has been independently opened, read-verified,
backed up, and the rollback window has expired.

Safe rotation ceremony:

1. Keep the engine on the current key reference and take an online backup.  Retain
   the immutable bundle and its manifest; the manifest records only the key ID and
   version, never key material.
2. Stop writes and build a fresh destination persist directory.  Run the approved
   offline re-encryption/export procedure for the deployment, reading the source
   with the old key and writing every durable value through the normal authoritative
   commit path under a new key and incremented version.  Raw `migrate-shards` and
   `restore` are **verbatim** operations and therefore do not rotate ciphertext.
3. Open the destination with the new `EPISTEMIC_GRAPH_ENCRYPTION_KEY`, matching ID,
   and matching version.  Verify representative graph reads, audit verification,
   mutation replay/outbox state, and a fresh backup before switching the service
   path.  Keep the old source and key available until this verification passes.
4. Atomically switch the deployment to the verified destination.  If any startup
   or read check fails, stop the destination and roll back to the old path/key;
   never change the canary by hand and never suppress the mismatch error.

DR rules:

- Online backups copy encrypted values and key-binding metadata byte-for-byte, so
  they can be captured without the key but can only be opened with the matching
  key reference and material.
- Restore with a configured key refuses a manifest whose key ID/version differs or
  is absent.  Restoring without a key is allowed only as a staged offline copy; the
  engine must still receive the original key before serving encrypted data.
- A different shard count preserves the same binding on every destination shard;
  a shard migration is not a re-encryption operation.  PITR replays the ledger on
  top of a restored bundle only after the key reference check succeeds.
- Diagnostics expose environment variable names and bounded key IDs/versions only;
  raw secrets, ciphertext, graph content, and host paths are never logged or put in
  manifests.

`security` is part of the one main build (and therefore the `cluster` layer) and
is mandatory for any served binary. Startup fails if it is absent.

## 7. Enabling each modality (feature ⇒ what lights up)

| Want | Build with | Then set |
|------|-----------|----------|
| SQL (in-engine) | `query` | — (native `Method::Sql`) |
| Postgres clients (+ pg_catalog / AGE / pgvector / Timescale / ParadeDB `EG-KG.query.route-create-view-create`/`114`/`116`/`117`/`119`) | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` |
| MySQL / MSSQL / SQLite clients | `mysql-wire` / `mssql-wire` / `sqlite-wire` | the matching `*_ADDR` |
| Bolt (Neo4j drivers) `EG-KG.query.bolt-wire-protocol` | `bolt-wire` (impl `cypher`) | `EPISTEMIC_GRAPH_BOLT_ADDR` |
| Redis clients (RESP2/3) `EG-KG.ontology.resp2-resp3-codec-round` | `redis-wire` (impl `kv`) | `EPISTEMIC_GRAPH_REDIS_ADDR` |
| Cypher | `cypher` (in the main build) | — / Bolt for remote drivers |
| GraphQL (+ Apollo Federation / hardening `EG-295`/`296`) | `graphql` | — / loopback `EPISTEMIC_GRAPH_GRAPHQL_ADDR` for authenticated SSE; current eg2 header + request id required, remote access through a same-host TLS proxy only |
| SPARQL / RDF / OWL (+ JSON-LD/TriG/RDF-XML/ShEx/ICV/GSP `EG-KG.compute.concept-2`..`137`/`146`) | `sparql` / `owl` / `shex` (in the main build) | `EPISTEMIC_GRAPH_SPARQL_ADDR` for HTTP |
| GeoSPARQL / spatial (+ RCC8/Egenhofer `EG-KG.ontology.concept-7`/`261`) | `geosparql` / `geo` | — |
| GIS / logistics (CRS/R-tree/tiles/routing/map-tasks `EG-KG.domains.coordinate-reference-system`..`267`) | `geo` | — |
| Vector ANN (+ exact/flat recall harness `EG-KG.query.concept-5`) | `ann` (in the main build) | — |
| Time-series | `tsdb` | — |
| Blob / CAS | `blob` (+ `blob-s3` for S3/MinIO) | `EPISTEMIC_GRAPH_BLOB_*` |
| S3-compatible object store (serve) `EG-KG.ontology.object-put-get-head` | `s3-api` (impl `blob`+`kv`) | `EPISTEMIC_GRAPH_S3_ADDR` |
| Key-value | `kv` | — |
| Full-text (+ ParadeDB BM25 `EG-KG.query.paradedb-bm25`) | `text` | — |
| Message broker (queues/exchanges/streams/DLQ/TTL `EG-275`..`284`) | `broker` | `Method::*` broker ops |
| Message broker wires (AMQP / MQTT / STOMP) `EG-275`/`281`/`282` | `amqp-wire` / `mqtt-wire` / `stomp-wire` (impl `broker`) | `EPISTEMIC_GRAPH_{AMQP,MQTT,STOMP}_ADDR` |
| Observability (logs / PromQL / traces / VRL `EG-163`/`165`/`172`) | `obs` / `promql` / `traces` | `EPISTEMIC_GRAPH_OBS_ADDR` |
| Natural-language query `EG-078`/`080` | `nl-query` | `EPISTEMIC_GRAPH_NL_ENDPOINT` / `…_NL_MODEL` / `…_NL_API_KEY_ENV` (served on `/nl`) |
| Agent memory (summary tier / consolidation / LeanRAG `EG-195`/`220`..`222`) | (core / `ann`) | `Method::*` memory ops |
| LLM KV-cache server (vLLM/LMCache share) `EG-185`..`187` | `kvcache-server` | `EPISTEMIC_GRAPH_KVCACHE_ADDR` (+ `…_KVCACHE_TOKEN`) |
| WASM UDF | `wasm-udf` | — (`RegisterUdf`/`RunUdf`) |
| Federation (remote / HTTP / external SQL) | `federation` / `federation-sql` | per-source spec |
| Super-cluster federated search `EG-KG.ontology.federation-client` | `federation-search` | `EPISTEMIC_GRAPH_FEDERATED_ADDR` (+ `…_FEDERATION_PEERS` / `…_FEDERATION_ALLOW`) |
| Clustering / HA | `raft` (in `cluster`) | `EPISTEMIC_GRAPH_RAFT_NODE_ID` + `…_RAFT_PEERS` + `…_RAFT_AUTH_SECRET_FILE` (+ `…_RAFT_BIND_ADDR`) |

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
*CONCEPT:EG-KG.ontology.comprehensive-interface-operations-documentation — comprehensive interface + operations documentation.*
