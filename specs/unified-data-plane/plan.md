# Design and implementation plan

**State:** PROPOSED. [Requirements](spec.md) · [tests](test-spec.md) · [tasks](tasks.md) · [cross-repository contracts](plan.md).

## Authority and reuse

Use the existing EG foreign catalog and `eg-plan::ForeignSourceSpec`, the server OBDA rewriter and R2RML support, `CdcHub`/`ChangeEnvelope`, `eg-lake`, DataFusion providers, `KvStore`/GraphCore, DepClock cache, pgwire/MySQL/MSSQL/RESP listeners, and EG's native backup. Extend the actual owners in place; retire superseded registration paths after migration. Reuse the SDK connector contract and Graph OS query surface, with no duplicate engine-side connector catalog. EG-DURABLE-KERNEL-R024 moves fan-out mirror behavior into EG before the registry folds it in. Credentials stay as references to the deployment's secret provider.

## Module ownership and request path

`src/server/foreign_catalog.rs` is the server-side source-record boundary. `crates/eg-plan` owns planning of bound foreign sources. `src/server/handlers/rdf/obda.rs` and `crates/eg-rdf/src/obda.rs` own virtual graph rewriting. `src/server/cdc.rs` and `src/server/cdc_sink/` own change envelopes and sinks; `crates/eg-lake` owns accelerated columnar copies. `crates/eg-query/src/tables/` owns native user-table storage. Wire listeners authenticate and classify requests before planning. Keep one registry, one source ID and one policy path across these modules.

The request envelope carries tenant, actor, source grant, consistency mode, last committed source position (if any), timeout and trace ID. The server checks these before selecting a path. An attached table plan carries a bound source ID and catalog version, never a raw caller DSN. EXPLAIN can show route, pushdown fragments, source/copy position and fallback reason, with credentials and sensitive predicate values redacted. A prepared plan is invalidated on schema-version or grant change.

### Source record and adapter interface

The durable record must contain `{source_id, tenant_id, owner_id, endpoint_binding, secret_ref, dialect, dialect_version, capabilities, allowed_catalog_scope, capture_config, current_catalog_version, status}`. Optional policies name acceleration, quotas, write-back and mirror targets. `endpoint_binding` is verified at registration and revalidated when a connection is created; DNS rebinding and private-address bypass are denied by outbound policy. Authorization is checked at registration, query planning and actual connection. Secrets are resolved only by the connection owner and never copied into a plan or trace.

An adapter provides `connect`, `render`, `read_catalog`, `decode_types`, and `capture` or an explicit no-capture result. Capability flags are versioned and testable: projection, predicate, limit, join, aggregate, order, transaction snapshot, and capture mode. The renderer quotes identifiers and binds values using the source dialect; it never concatenates user values into SQL. The catalog reader records stable object IDs, schema version, keys, FKs, indexes, views, enums, JSON shape and source provenance. Unknown types remain opaque typed values rather than silent text casts.

### SQL destination fixture contract

The SQL registration fixtures for EG-UNIFIED-DATA-PLANE-R002.1 and the OBDA
constructor fixtures for EG-UNIFIED-DATA-PLANE-R002.2 explicitly allow
`127.0.0.1:5432` for positive cases and install an empty allowlist for refusals.
Loopback is not implicitly trusted. A restore-on-drop test guard holds the shared
crypto environment write lock, or a local lock when crypto is not compiled, for
both synchronous and asynchronous callers. The foreign-column owner/mapping
fixture uses this admitted numeric address so DNS failure cannot mask its refusal
assertions; those assertions run before any SQL connection.

OBDA constructor checks exercise destination validation only. They do not prove
R002.2's mixed allowed/disallowed `register_table_sources` batch acceptance;
that registry-level test remains required.

## Contracts and data flow

1. **Source registry.** A source record binds stable ID, tenant/owner, verified endpoint, secret reference, dialect and version, read/write/mirror capabilities, quota, mapping versions and capture position. Provisioning is an authenticated governed mutation. One registry identity is passed to OBDA, SQL federation, CDC and mirror sinks. Do not resolve arbitrary request-supplied DSNs. Revocation blocks new plans and drains active readers under an explicit policy.
2. **Adapter seam.** Each adapter implements connection, dialect SQL rendering, catalog extraction, type conversion, and capture or an explicit `unsupported` capability. Versioned capabilities drive planning and conformance. Postgres uses `pg_catalog` and `pgoutput`. MySQL and MariaDB use separate binlog/GTID fixtures. SQLite uses lock-safe read-only file access. MSSQL uses CDC/Change Tracking. ClickHouse and warehouses are federation-only unless a later approved contract adds capture. Oracle/Db2 use Debezium. Do not fabricate CDC from a periodic full scan.
3. **Schema intelligence.** Hash a typed catalog snapshot. Collect bounded, policy-gated column and JSON shape statistics. Infer unary/n-ary inclusion dependencies, functional dependencies and candidate keys with support counts, then use fixed-seed Leiden grouping. View definitions and captured joins can provide co-query edges. Deterministic name/type/value/structure matchers propose ontology mapping first. Optional model suggestions remain proposals and cannot approve or activate a mapping. Approved mappings become immutable versioned R2RML named graphs. DDL drift creates a new catalog version and suspends affected mappings pending review. `schema_context` reads the same graph and explains owner, joins, class and impact. Catalog artifacts without an OBDA or context consumer are not admitted.
4. **Queries.** The planner uses typed source values and capability-aware pushdown, including same-source joins/aggregates/order/limit. Fallback is explicit in EXPLAIN and retains tenant filters. The router compares freshness requirement, source position, copy position and latency policy before selecting native/live/accelerated. A bounded wait for the caller's last source commit is required for read-your-writes; timeout is a typed failure or policy-authorized live retry, never a silent stale result.
5. **Change path.** Adapters emit a source-keyed `ChangeEnvelope` carrying source position, transaction/order boundary, table/object ID, operation, before/after shape when available, schema version and event ID. The durable subscription cursor and idempotency key are committed with its consumer effects, or recovered by replay. Existing `CdcHub` fans into approved graph deltas, search/vector indexes, CEP/outbox and optional `eg-lake` copies. Keyless updates, DDL changes, WAL/binlog retention and lag are explicit states with alerts. A snapshot/catch-up handshake prevents the gap between initial read and streaming. Mirrors are sink direction on the same registry and keep per-target cursor, replay and reconciliation; they never answer reads.
6. **Write path.** External mutation requires EG-DURABLE-KERNEL-R031 reservation, checked actor and target policy, idempotency and an approval receipt. SDK/app API is used when application rules require it. Return an indeterminate outcome if the external effect may have succeeded but its acknowledgement/audit completion failed; reconcile before retrying.
7. **Native path dependency.** Before app admission, consume the `durable-graph-kernel` proof for point/KV routing, authz/audit parity, durability classes, hot/cold union, crash recovery and store choice. The pgrx companion remains a separate spike admitted only by ADR; this data-plane plan does not implement a second native store.

8. **Virtual-graph metadata.** Add `core:virtual-graph@1` and its shapes to the core ontology corpus. Extend the catalog reader so every source kind writes a hashed `MetadataContract`. Reuse the source registry identity and the R2RML mapping approval path. Add the acceleration policy fields to the source record. Copy no entity row outside that policy. (EG-UNIFIED-DATA-PLANE-R037, EG-UNIFIED-DATA-PLANE-R038)

## Delivery sequence

The dependency triggers in [tasks](tasks.md) control implementation. Begin with EG-UNIFIED-DATA-PLANE-R001 and the kernel-owned EG-DURABLE-KERNEL-R036 mirror interface. Build the conformance harness before T1 adapters. Then catalog/mapping/query/capture and three app admissions. Add acceleration, router and write-back after durable positions and audit reservation. T2/T3 dialects follow T1 evidence. Gramps P0–P5 is isolated from production. P6 is a separate operator cutover. The kernel's bursty lane begins after EG-DURABLE-KERNEL-R016 and supplies EG-DURABLE-KERNEL-R042 benchmark evidence before this spec makes a comparative claim. Keep feature changes partitioned by actual owner files, not requirement IDs.

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
- Cross-repository changes follow [cross-repo.md](plan.md); no owner repo should duplicate another's runtime logic.

## Cross-repository

**State:** PROPOSED. EG owns attached-source identity, federation, mapping, CDC, mirrors, native storage and this specification. Other components implement only the consumer or deployment contract below. A contribution may implement one side at a time, but it cannot claim acceptance until the named counterparty behavior and negative cases pass. The requirement IDs cited below map these contracts to [tasks](tasks.md) and [requirements.md](requirements.md).

### Boundary map

| Component owner | Inputs to EG | Outputs and obligations | Acceptance |
|---|---|---|---|
| Agent utilities | Existing graph-source schema proposal pattern and legacy fan-out/Debezium behavior are migration inputs, not new runtime authorities (EG-DURABLE-KERNEL-R024). | Remove old ingest/mirror execution only after EG source registry, cursor replay, SQL-table mirror restore and catalog parity are live. | No duplicate source or mirror registry remains reachable; differential and restore reports identify old and new behavior. |
| Agent connector SDK | Connector identity, app API client and write-back idempotency key (EG-UNIFIED-DATA-PLANE-R012). | Carry tenant/actor/source ID and approval receipt to EG. Prefer an app API when the app owns validation, triggers or other business rules. | Replayed write returns the prior outcome. Missing audit reservation or approval produces no effect. |
| Graph OS | Authenticated request envelope, `schema_context` query and last committed source position (EG-UNIFIED-DATA-PLANE-R011). | Propagate tenant, actor, source grant, freshness mode, timeout and trace ID unchanged; display typed route/freshness errors. | Wrong tenant, stale copy, missing source and missing audit all fail closed; a valid context answer names owner, joins, class and impact. |
| Immich API connector | Version-pinned OpenAPI schema and per-user authorized API reads (EG-UNIFIED-DATA-PLANE-R032). | MCP tools, incremental asset/album/person ingest and approved Person-link proposals; do not ingest face vectors in the pilot. | Synthetic four-user fixture: second ingest no-op, one new upload appears, unnamed people stay unlinked, cross-user photo/GPS access denied. |
| Gramps integration | Synthetic import/export tree, real-Postgres control corpus and app driver behavior (EG-UNIFIED-DATA-PLANE-R026). | Keep production data unchanged during P0–P5; P6 is separate explicit authorization with replica, independent export and fallback. | P0–P5 digests, differential and restore artifacts pass before any production decision. |
| Finance consumer | Finance-v1 Account/Activity semantics and provenance requirements (EG-FINANCE-PRIMITIVES-R012). | Import Ghostfolio activities one way through attached Postgres; maintain stable external IDs and source timestamps. | Reimport is idempotent, amounts remain exact, and retirement is a separate decision after EG accounting parity. |
| Database deployment | Isolated CloudNativePG groups by major version/extension and a MariaDB group, per-app database and role, EG read/replication roles (EG-UNIFIED-DATA-PLANE-R023). | Logical replication enabled where supported; PITR to object storage, scheduled scratch restore and per-app rollback. An extension-heavy app keeps a compatible isolated group. | At least three apps attached including MariaDB; restore and app rollback pass without cross-app impact. |
| Release/catalog maintainer | New connector package metadata and version pins (EG-UNIFIED-DATA-PLANE-R032). | Validate every required package catalog/registry entry and generated manifest using the existing release gate; preserve one authoritative package identity. | Missing site or version mismatch fails CI; no hand-maintained alternate fleet inventory. |

### Shared wire contract

Every cross-repository call carries `{tenant_id, actor_id, source_id, trace_id, policy_context}`. Query calls add `{consistency, last_source_position, timeout}`. Governed writes add `{idempotency_key, approval_receipt, audit_reservation}`. Source IDs refer to EG's registry; no counterparty accepts or forwards a caller-provided DSN or secret value. Position tokens are opaque to consumers and scoped to the source and subscription. A token from another source or tenant is rejected. Errors distinguish `DENIED`, `SOURCE_UNAVAILABLE`, `STALE_TIMEOUT`, `SCHEMA_DRIFT`, `CAPABILITY_UNSUPPORTED` and `OUTCOME_INDETERMINATE`; clients preserve these rather than returning an empty result.

The deployment layer supplies a secret reference resolver and outbound network policy. EG resolves a reference at connection time and verifies the target address. Application connectors retain API-based business operations; database attachments are read-only until a specific governed write-back policy is approved. All interfaces are versioned by capability and tested at the exact paired commits named by the implementing pull requests.

### Contribution requirements

An integration pull request names the requirement IDs it closes, EG source-record/capability version, counterparty commit, synthetic fixtures, positive and denied request traces, and any old-path removal. An EG-only pull request keeps the row `IMPLEMENTED-UNVALIDATED` if the counterparty is not yet exercised. A deployment-only pull request does not declare the engine adapter accepted. Cross-component status is updated in [tasks](tasks.md) after the combined test and operational evidence are available.
