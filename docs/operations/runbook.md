# Operations runbook

Everything an operator needs to run epistemic-graph as a durable database: the deployment
tiers, the full env-var catalog for every listener and knob, backup / point-in-time recovery,
RBAC, encryption-at-rest, and how to turn each modality on. For the per-client connect recipes
see [`interfaces/connecting.md`](../interfaces/connecting.md); for the storage internals see
the [architecture](../architecture/engine.md) pages.

## 1. Deployment tiers (feature bundles)

The engine is one codebase built at a chosen tier. A tier is a Cargo feature bundle spanning
the Pi↔cluster spectrum. **The Pi contract is sacred**: a `pi` build links **no DataFusion, no
openraft, no Tantivy, no wasmtime, no native/C dep** (asserted by `cargo tree`).

| Tier | What it is | Typical target |
|------|-----------|----------------|
| `pi` | Lean durable in-memory + redb, dep-free Cypher, native ANN, RDF/SPARQL/OWL (all pure-Rust), streaming, result-cache, cold-tier, cost | Raspberry-Pi / edge, "a local engine per agent" |
| `pi-max` | `pi` + `tsdb` + `blob` + `security` — every pure-Rust, no-DataFusion feature that still fits a Pi | maximal edge node without a C toolchain |
| `node` | `pi` + SQL/DataFusion, GraphQL, compute domains, TSDB, blob, KV, full-text, GeoSPARQL, federation, spatial/tensor/stream, WASM UDF, security, obs | single workstation / server, "one binary, most features" |
| `full` (= `all`) | Every **single-node** feature, size-optimized. Deliberately **no** raft/pgwire | one binary, every feature, no clustering |
| `cluster` | `node` + in-engine Raft replication (EG/KG-2.188) + `pgwire` + distributed compute + cross-shard 2PC + the SQLite/MySQL/MSSQL wires | multi-node HA, SQL clients |

```bash
cargo build --release --features full        # or: pi / pi-max / node / cluster
# a la carte: pick exactly the surfaces you want
cargo build --release --features "pgwire bolt-wire promql traces security"
```

Individual wire/broker features (`mysql-wire`, `mssql-wire`, `sqlite-wire`, `bolt-wire`,
`amqp-wire`, `promql`, `traces`) are folded per-tier by the orchestrator; you can always add
them explicitly. See [tiers](../architecture/tiers.md) for the prebuilt-binary mapping.

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
| Bolt wire (Neo4j) | `bolt-wire` | `EPISTEMIC_GRAPH_BOLT_ADDR` | `127.0.0.1:7687` | `EPISTEMIC_GRAPH_BOLT_GRAPH` |
| AMQP broker | `amqp-wire` | `EPISTEMIC_GRAPH_AMQP_ADDR` | `127.0.0.1:5672` | `EPISTEMIC_GRAPH_AMQP_GRAPH` |
| SPARQL HTTP + `/nl` | `sparql-http` | `EPISTEMIC_GRAPH_SPARQL_ADDR` (`--sparql-addr`) | `127.0.0.1:7878` | `EPISTEMIC_GRAPH_SPARQL_DEFAULT_GRAPH`, `…_SPARQL_SERVICE_ALLOW` |
| GraphQL SSE | `graphql` | `EPISTEMIC_GRAPH_GRAPHQL_ADDR` (`--graphql-addr`) | `127.0.0.1:7879` | — |
| Obs: logs + PromQL + traces | `obs`/`promql`/`traces` | `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`) | `127.0.0.1:5080` | `EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS` |
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

`security` is folded into `node`/`cluster`/`full` (and `pi-max`); it is out of the lean `pi`
tier by default.

## 7. Enabling each modality (feature ⇒ what lights up)

| Want | Build with | Then set |
|------|-----------|----------|
| SQL (in-engine) | `query` | — (native `Method::Sql`) |
| Postgres clients | `pgwire` | `EPISTEMIC_GRAPH_PGWIRE_ADDR` |
| MySQL / MSSQL / SQLite / Bolt clients | `mysql-wire` / `mssql-wire` / `sqlite-wire` / `bolt-wire` | the matching `*_ADDR` |
| Cypher | `cypher` (in `pi`) | — / Bolt for remote drivers |
| GraphQL | `graphql` | — / `EPISTEMIC_GRAPH_GRAPHQL_ADDR` for SSE |
| SPARQL / RDF / OWL | `sparql` (in `pi`) / `owl` | `EPISTEMIC_GRAPH_SPARQL_ADDR` for HTTP |
| GeoSPARQL / spatial | `geosparql` / `geo` | — |
| Vector ANN | `ann` (in `pi`) | — |
| Time-series | `tsdb` | — |
| Blob / CAS | `blob` (+ `blob-s3` for S3/MinIO) | `EPISTEMIC_GRAPH_BLOB_*` |
| Key-value | `kv` | — |
| Full-text | `text` | — |
| Message broker (AMQP) | `amqp-wire` (impl `broker`) | `EPISTEMIC_GRAPH_AMQP_ADDR` |
| Observability (logs/PromQL/traces) | `obs` / `promql` / `traces` | `EPISTEMIC_GRAPH_OBS_ADDR` |
| Natural-language query | `nl-query` | `EPISTEMIC_GRAPH_NL_ENDPOINT` / `…_NL_MODEL` / `…_NL_API_KEY_ENV` |
| WASM UDF | `wasm-udf` | — (`RegisterUdf`/`RunUdf`) |
| Federation (remote / HTTP / external SQL) | `federation` / `federation-sql` | per-source spec |
| Clustering / HA | `raft` (in `cluster`) | `EPISTEMIC_GRAPH_RAFT_NODE_ID` + `…_RAFT_PEERS` (+ `…_RAFT_BIND_ADDR`) |

## 8. Observability of the engine itself

- **Metrics**: `--metrics-addr` exposes Prometheus counters/gauges (request/latency/in-flight,
  per-graph gauges). Scrape it.
- **Tracing/spans**: `otel` + `EPISTEMIC_GRAPH_OTLP_ENDPOINT` exports the engine's existing
  `tracing` spans over OTLP (additive; installs only when both are set).
- **Slow queries**: `EPISTEMIC_GRAPH_SLOW_QUERY_MS`.

---
*CONCEPT:EG-095 — comprehensive interface + operations documentation.*
