# Unified data plane

| Field | Value |
|---|---|
| Spec ID | `unified-data-plane` |
| Owner | epistemic-graph |
| Program | refactor train 9 |
| State | PROPOSED; no implementation or acceptance is claimed by this document |
| Scope | EH-660–EH-697, EH-714, EH-717–EH-722 (45 T9 rows) |

**Draft authority:** database program (`plans/database/README.md`), train plan (`plans/refactor/TRAINS-8-9.md`), and ledger (`plans/refactor/LEDGER.md`). Paths to workspace plans are design provenance; owner-native acceptance evidence must be recorded here and in the ledger.

## Outcome and boundaries

EG is the converged engine for EG-owned data: property graph, SQL, RDF/OWL, vector, time series, provenance and reasoning over one durable authority and one query surface. Third-party applications keep their real database. EG attaches to each through a single owner-scoped registry, serves live SQL and named virtual graphs, optionally maintains a change-captured accelerated copy, and exposes a freshness-aware query route. Native hosting is an app-specific admission track, first measured with Gramps. Immich is an API connector and entity-resolution pilot; it is not a native-hosting candidate. Ghostfolio is a one-way finance import from attached Postgres until an explicit retirement decision.

This spec does not authorize a production cutover, direct writes around an application's business logic, automatic `owl:sameAs` assertions, or a performance claim without the stated benchmark gate. P6 Gramps cutover requires the operator's go-ahead after P0–P5 evidence.

## Functional requirements

| ID | Requirement | Ledger | Acceptance intent |
|---|---|---|---|
| DP-01 | Record the attached-source default, native-hosting gate, router consistency and pgrx boundary as ADRs. | EH-660 | Reviewed ADRs DB-7.0, DB-9.3, DB-9.4. |
| DP-02 | Register one tenant/owner-scoped attached source with secrets by reference, verified outbound target, capabilities and mirror direction. Replace the separate `ForeignSourceSpec::Sql`, transient OBDA source and EH-508 mirror registration paths. | EH-661, EH-692 | Same source identity serves SQL, OBDA, CDC and mirror; unauthorized cross-owner reads and unverified addresses fail closed. |
| DP-03 | Read each dialect catalog into a versioned, typed, hashed schema graph; profile under bounded policy; infer dependencies and deterministic Leiden groups. Every artifact has a named OBDA or `schema_context` consumer. | EH-662, EH-663, EH-693 | Stable hash/order on repeated reads; no inert RF-031 style nodes. |
| DP-04 | Compile deterministic-first, operator-approved ontology mappings to versioned R2RML named virtual graphs. Model output is proposal only. Query by name through SPARQL, UQL and REASON. | EH-664 | Unapproved/broken mapping cannot expose data. |
| DP-05 | Push typed projection, filter, same-source join, aggregate, order and limit to the source when supported; use DataFusion providers for attached tables; show every fallback in EXPLAIN. | EH-665, EH-666 | Native-source differential equality across dialects and null/type edges. |
| DP-06 | Normalize change capture to `ChangeEnvelope`, persist source position per subscription, replay idempotently, detect schema drift, update approved graph/search/vector/event consumers and optional `eg-lake` copies. | EH-667–EH-669, EH-680 | Snapshot equals replay after crash/failover; lag and retention guardrails visible. |
| DP-07 | Route native, live or accelerated reads by policy and freshness. A client asking read-your-writes waits for its committed source position within a bounded timeout; no stale success on timeout. | EH-670 | EXPLAIN states route/position; stale route fails or falls back according to declared policy. |
| DP-08 | Govern attached-source write-back with approval, idempotency and EH-658 audit reservation. Prefer the app API when it owns business rules. | EH-671 | Missing audit/approval, replayed key, and policy denial fail safely. |
| DP-09 | Implement five-part adapters (driver, rendering, catalog, change capture where offered, type map) for Postgres, MySQL, MariaDB, SQLite, MSSQL, ClickHouse, Oracle, Db2, MongoDB/DocumentDB, Snowflake, BigQuery, DuckDB and Iceberg; use Debezium bridge where specified. | EH-672–EH-681 | Per engine/version conformance matrix; unsupported capture explicitly declared, never silently simulated. |
| DP-10 | Admit shared CloudNativePG and MariaDB services and apps one at a time, with separate roles, PITR and restore drills; retire an ingest connector only after attached parity. | EH-682, EH-683, EH-714 | Three attached apps including MariaDB; rollback and one-way Ghostfolio import proven. |
| DP-11 | Earn native hosting with the Gramps traffic/differential/restore pilot, only then address observed pgwire gaps, cross-store atomicity, tooling and safety. | EH-684–EH-691, EH-695, EH-697 | P0–P5 evidence, reviewed deviations, crash/restore tests; P6 separately approved. |
| DP-12 | Resolve entities across apps through blocking, similarity and Fellegi–Sunter proposals; deliver the Immich API connector and its incremental, private KG ingest. | EH-694, EH-696 | Approved links only; per-user isolation and incremental no-op replay. |
| DP-13 | Classify point/data-structure workloads before DataFusion, add explicit sync/async/ephemeral durability, RAM hot structures backed by canonical recovery where durable, batch pipelining and a schema-versioned plan cache. | EH-717–EH-721 | Same authz/HMAC keyspace; measured loss window; no second authority. |
| DP-14 | Publish same-hardware R820 YCSB A–F, redis-benchmark, pgbench, HammerDB TPC-C, LDBC and converged-query results with configs and targets. | EH-722 | Only green per-axis rows permit a “surpass” claim. |

## Acceptance states

`PROPOSED` is a design artifact. `IMPLEMENTED-UNVALIDATED` needs a merged owner commit and linked tests. `ACCEPTED` needs passing exact-head conformance, security, quality and operational evidence. `CUTOVER` needs recorded consumer switch and rollback rehearsal. `DELETED` needs proof that old registration/ingest/mirror paths were removed. `DONE` needs the ledger and native spec reconciled to the accepted evidence. A task checkbox means work was planned or performed; it never alone changes state.

## Program exit

All attached-source dialect tiers must pass their applicable conformance rows; three self-hosted apps including a MariaDB app must be attached with ingest connectors retired; the accelerated copy must hold its lag objective across restart and failover; EG mirrors must match AU fan-out semantics and restore SQL tables into real Postgres; Gramps must either meet the native-hosting exit evidence or close with an evidence report if P6 is not approved. Record DB-7.0, DB-9.3 and DB-9.4 ADRs. The [test specification](test-spec.md) defines failures as well as successes, and [tasks](tasks.md) retains every ledger row without marking it built.
