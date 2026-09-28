# Design and implementation plan

**State:** PROPOSED. [Requirements](spec.md) · [tests](test-spec.md) · [tasks](tasks.md) · [cross-repository contracts](cross-repo.md).

## Authority and reuse

Use the existing EG foreign catalog and `eg-plan::ForeignSourceSpec`, the server OBDA rewriter and R2RML support, `CdcHub`/`ChangeEnvelope`, `eg-lake`, DataFusion providers, `KvStore`/GraphCore, DepClock cache, pgwire/MySQL/MSSQL/RESP listeners, and EG's native backup. Extend the actual owners in place; retire superseded registration paths after migration. Reuse the SDK connector contract and Graph OS query surface, with no duplicate engine-side connector catalog. EH-508 moves fan-out mirror behavior into EG before the registry folds it in. Credentials stay as references to the deployment's secret provider.

## Module ownership and request path

`src/server/foreign_catalog.rs` is the server-side source-record boundary. `crates/eg-plan` owns planning of bound foreign sources. `src/server/handlers/rdf/obda.rs` and `crates/eg-rdf/src/obda.rs` own virtual graph rewriting. `src/server/cdc.rs` and `src/server/cdc_sink/` own change envelopes and sinks; `crates/eg-lake` owns accelerated columnar copies. `crates/eg-query/src/tables/` owns native user-table storage. Wire listeners authenticate and classify requests before planning. Keep one registry, one source ID and one policy path across these modules.

The request envelope carries tenant, actor, source grant, consistency mode, last committed source position (if any), timeout and trace ID. The server checks these before selecting a path. An attached table plan carries a bound source ID and catalog version, never a raw caller DSN. EXPLAIN can show route, pushdown fragments, source/copy position and fallback reason, with credentials and sensitive predicate values redacted. A prepared plan is invalidated on schema-version or grant change.

### Source record and adapter interface

The durable record must contain `{source_id, tenant_id, owner_id, endpoint_binding, secret_ref, dialect, dialect_version, capabilities, allowed_catalog_scope, capture_config, current_catalog_version, status}`. Optional policies name acceleration, quotas, write-back and mirror targets. `endpoint_binding` is verified at registration and revalidated when a connection is created; DNS rebinding and private-address bypass are denied by outbound policy. Authorization is checked at registration, query planning and actual connection. Secrets are resolved only by the connection owner and never copied into a plan or trace.

An adapter provides `connect`, `render`, `read_catalog`, `decode_types`, and `capture` or an explicit no-capture result. Capability flags are versioned and testable: projection, predicate, limit, join, aggregate, order, transaction snapshot, and capture mode. The renderer quotes identifiers and binds values using the source dialect; it never concatenates user values into SQL. The catalog reader records stable object IDs, schema version, keys, FKs, indexes, views, enums, JSON shape and source provenance. Unknown types remain opaque typed values rather than silent text casts.

## Contracts and data flow

1. **Source registry.** A source record binds stable ID, tenant/owner, verified endpoint, secret reference, dialect and version, read/write/mirror capabilities, quota, mapping versions and capture position. Provisioning is an authenticated governed mutation. One registry identity is passed to OBDA, SQL federation, CDC and mirror sinks. Do not resolve arbitrary request-supplied DSNs. Revocation blocks new plans and drains active readers under an explicit policy.
2. **Adapter seam.** Each adapter implements connection, dialect SQL rendering, catalog extraction, type conversion, and capture or an explicit `unsupported` capability. Versioned capabilities drive planning and conformance. Postgres uses `pg_catalog` and `pgoutput`; MySQL and MariaDB use separate binlog/GTID fixtures; SQLite uses lock-safe read-only file access; MSSQL uses CDC/Change Tracking; ClickHouse and warehouses are federation-only unless a later approved contract adds capture; Oracle/Db2 use Debezium. Do not fabricate CDC from a periodic full scan.
3. **Schema intelligence.** Hash a typed catalog snapshot; collect bounded, policy-gated column and JSON shape statistics; infer unary/n-ary inclusion dependencies, functional dependencies and candidate keys with support counts, then use fixed-seed Leiden grouping. View definitions and captured joins can provide co-query edges. Deterministic name/type/value/structure matchers propose ontology mapping first; optional model suggestions remain proposals and cannot approve or activate a mapping. Approved mappings become immutable versioned R2RML named graphs. DDL drift creates a new catalog version and suspends affected mappings pending review. `schema_context` reads the same graph and explains owner, joins, class and impact. Catalog artifacts without an OBDA or context consumer are not admitted.
4. **Queries.** The planner uses typed source values and capability-aware pushdown, including same-source joins/aggregates/order/limit. Fallback is explicit in EXPLAIN and retains tenant filters. The router compares freshness requirement, source position, copy position and latency policy before selecting native/live/accelerated. A bounded wait for the caller's last source commit is required for read-your-writes; timeout is a typed failure or policy-authorized live retry, never a silent stale result.
5. **Change path.** Adapters emit a source-keyed `ChangeEnvelope` carrying source position, transaction/order boundary, table/object ID, operation, before/after shape when available, schema version and event ID. The durable subscription cursor and idempotency key are committed with its consumer effects, or recovered by replay. Existing `CdcHub` fans into approved graph deltas, search/vector indexes, CEP/outbox and optional `eg-lake` copies. Keyless updates, DDL changes, WAL/binlog retention and lag are explicit states with alerts. A snapshot/catch-up handshake prevents the gap between initial read and streaming. Mirrors are sink direction on the same registry and keep per-target cursor, replay and reconciliation; they never answer reads.
6. **Write path.** External mutation requires EH-658 reservation, checked actor and target policy, idempotency and an approval receipt. SDK/app API is used when application rules require it. Return an indeterminate outcome if the external effect may have succeeded but its acknowledgement/audit completion failed; reconcile before retrying.
7. **Native path.** EH-290 latency repair precedes OLTP admission. Classify point reads, KV and data-structure operations before planner allocation, preserving authz/audit. Keep redb/canonical mutation applier as durable authority; RAM structures are reconstructible for durable classes. `sync` acknowledges after commit, `async` publishes bounded measured loss (target ≤100 ms), `ephemeral` publishes TTL and restart loss. Use `eg-lake` as cold tier under a single union provider; tombstones and compaction must not resurrect records. Evaluate a different hot store by ADR on captured workloads. The pgrx companion is a separate spike and cannot become a second database authority by default.

## Delivery sequence

The dependency triggers in [tasks](tasks.md) control implementation. Begin with EH-660/661 and EH-508/692; build the conformance harness before T1 adapters; then catalog/mapping/query/capture and three app admissions. Add acceleration, router and write-back after durable positions and audit reservation. T2/T3 dialects follow T1 evidence. Gramps P0–P5 is isolated from production; P6 is a separate operator cutover. The bursty lane starts after EH-290 and gates claims through EH-722. Keep feature changes partitioned by actual owner files, not ledger IDs.

### Dialect implementation matrix

| Tier | Dialect | Driver, catalog and capture contract |
|---|---|---|
| 1 | Postgres | `sqlx`/`tokio-postgres`, `pg_catalog`, `pgoutput`; preserve `jsonb`, arrays, enum and vector types. |
| 1 | MySQL and MariaDB | `sqlx mysql`, `information_schema`, row binlog plus GTID; separate conformance for both engines. |
| 2 | SQLite | Extend `eg-sqlite-format` for `WITHOUT ROWID`, lock-safe read-only attach; WAL-frame tail only where safe, otherwise declared watermark polling. |
| 2 | SQL Server | `tiberius`, `sys.*` catalog, T-SQL `TOP` and `OFFSET/FETCH`, CDC or Change Tracking positions. |
| 2 | ClickHouse | HTTP/native query, `system.*` catalog, federation and acceleration; declare no native CDC. |
| 3 | Oracle and Db2 | Driver/ODBC query and catalog; Debezium bridge for change events. |
| 3 | MongoDB/DocumentDB | Native driver, bounded inferred document shape as catalog, change streams where supported. |
| 3 | Snowflake, BigQuery, DuckDB, Iceberg | Vendor query or Arrow Flight SQL where offered; federation-only, with Iceberg extending existing federation. |

### Native transaction and storage decisions

An ADR must choose one-shard atomic redb transaction, recoverable intent log or true two-phase commit for mixed graph/table writes. Table-only traffic must never enter the mixed-store path. Crash fault injection at each commit boundary and parallel writer tests decide acceptance. For external app data, the source database retains transactions and EG's CDC is a derived read path; EG cannot claim a distributed transaction from cursor progress alone.

After EH-290 removes the measured durable-write bottleneck, primary-key reads and writes bypass DataFusion while retaining the same policy/audit path. A union provider reads hot redb rows and cold `eg-lake` Parquet by key/version; tombstones and compaction preserve latest-visible semantics. A storage-engine ADR compares redb, fjall and RocksDB on captured app/finance workloads, including recovery and write amplification. A separate pgrx spike tests a Postgres companion for SPARQL, vector, one time-series kernel and wait-for-position; it is admitted only by ADR and cannot become a parallel authority.

The bursty lane classifies native, RESP, pgwire, MySQL and MSSQL requests before planner allocation. `sync` is commit-before-ack; `async` is measured group commit with a target loss window ≤100 ms; `ephemeral` is RAM+TTL and explicitly lost on restart. Namespace/table durability is declared and visible. RAM hash, sorted set, list and set working state for durable classes is reconstructed from the canonical log. Atomic `INCRBY`+expiry, `SET NX EX`, compare-and-set leases, token bucket, bounded counter and work claim are native operations. RESP pipelines and pgwire extended batches use one admission and group commit with N ordered replies. Repeated SQL plans use statement digest plus schema version, sharing DepClock invalidation with the existing result cache.

## Design and quality constraints

- CCCC complexity, KISS file/symbol limits, jscpd and Dupehound duplicate gates, formatting, lint and hosted CI must be green on the exact proposed merge head. Record measured deltas and deleted/retired paths; do not add ratchets, suppressions, version-suffixed parallel implementations or a second source of truth.
- SQL semantics and types need differential tests against the real engine; planner pushdown must not bypass tenant filters. Secrets and private app data must not appear in EXPLAIN, logs, snapshots or fixtures.
- Preserve durable format and wire compatibility under the repository policy. New storage decisions require migration/rollback and crash recovery evidence. No benchmark claim follows merely from a structural simplification.
- Cross-repository changes follow [cross-repo.md](cross-repo.md); no owner repo should duplicate another's runtime logic.
