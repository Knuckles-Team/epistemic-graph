# Design and implementation plan

**State:** PROPOSED. [Requirements](spec.md) · [tests](test-spec.md) · [tasks](tasks.md) · [cross-repository contracts](cross-repo.md).

## Authority and reuse

Use the existing EG foreign catalog and `eg-plan::ForeignSourceSpec`, the server OBDA rewriter and R2RML support, `CdcHub`/`ChangeEnvelope`, `eg-lake`, DataFusion providers, `KvStore`/GraphCore, DepClock cache, pgwire/MySQL/MSSQL/RESP listeners, and EG's native backup. Extend the actual owners in place; retire superseded registration paths after migration. Reuse the SDK connector contract and Graph OS query surface, with no duplicate engine-side connector catalog. EG-DURABLE-KERNEL-R024 moves fan-out mirror behavior into EG before the registry folds it in. Credentials stay as references to the deployment's secret provider.

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
6. **Write path.** External mutation requires EG-DURABLE-KERNEL-R031 reservation, checked actor and target policy, idempotency and an approval receipt. SDK/app API is used when application rules require it. Return an indeterminate outcome if the external effect may have succeeded but its acknowledgement/audit completion failed; reconcile before retrying.
7. **Native path dependency.** Before app admission, consume the `durable-graph-kernel` proof for point/KV routing, authz/audit parity, durability classes, hot/cold union, crash recovery and store choice. The pgrx companion remains a separate spike admitted only by ADR; this data-plane plan does not implement a second native store.

8. **Virtual-graph metadata.** Add `core:virtual-graph@1` and its shapes to the core ontology corpus. Extend the catalog reader so every source kind writes a hashed `MetadataContract`. Reuse the source registry identity and the R2RML mapping approval path. Add the acceleration policy fields to the source record. Copy no entity row outside that policy. (EG-UNIFIED-DATA-PLANE-R037, EG-UNIFIED-DATA-PLANE-R038)

## Delivery sequence

The dependency triggers in [tasks](tasks.md) control implementation. Begin with EG-UNIFIED-DATA-PLANE-R001 and the kernel-owned EG-DURABLE-KERNEL-R036 mirror interface; build the conformance harness before T1 adapters; then catalog/mapping/query/capture and three app admissions. Add acceleration, router and write-back after durable positions and audit reservation. T2/T3 dialects follow T1 evidence. Gramps P0–P5 is isolated from production; P6 is a separate operator cutover. The kernel's bursty lane begins after EG-DURABLE-KERNEL-R016 and supplies EG-DURABLE-KERNEL-R042 benchmark evidence before this spec makes a comparative claim. Keep feature changes partitioned by actual owner files, not requirement IDs.

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

### Native transaction and storage dependency

`durable-graph-kernel` owns the mixed graph/table atomicity ADR, hot/cold union, storage-engine evaluation, native and RESP/SQL wire routing, durability classes, batch admission, plan cache and benchmark receipt. This spec consumes their exact-head crash, isolation, restore and performance results before admitting an application. External app data remains under its source database transaction; an EG CDC cursor is a derived read position, never a distributed-commit claim. The separate pgrx spike cannot become a parallel database authority by default.

## Design and quality constraints

- CCCC complexity, KISS file/symbol limits, jscpd and Dupehound duplicate gates, formatting, lint and hosted CI must be green on the exact proposed merge head. Record measured deltas and deleted/retired paths; do not add ratchets, suppressions, version-suffixed parallel implementations or a second source of truth.
- SQL semantics and types need differential tests against the real engine; planner pushdown must not bypass tenant filters. Secrets and private app data must not appear in EXPLAIN, logs, snapshots or fixtures.
- Preserve durable format and wire compatibility under the repository policy. New storage decisions require migration/rollback and crash recovery evidence. No benchmark claim follows merely from a structural simplification.
- Cross-repository changes follow [cross-repo.md](cross-repo.md); no owner repo should duplicate another's runtime logic.
