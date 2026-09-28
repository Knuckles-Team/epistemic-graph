# Unified data plane

| Field | Value |
|---|---|
| Spec ID | `unified-data-plane` |
| Owner | epistemic-graph |
| Program | refactor train 9 |
| State | PROPOSED; no implementation or acceptance is claimed by this document |
| Scope | Attached-source, dialect, app-admission and freshness obligations assigned in [status.json](status.json); native storage and finance import are adjacent owner contracts |

This directory is the public build contract for the attached-source and app-admission part of T9. The EH IDs are stable trace identifiers. [status.json](status.json) assigns normative ownership for this spec; related native storage obligations live in `durable-graph-kernel`, and the optional finance import lives in `finance-primitives`. `SPECIFIED` and `NOT_AUDITED` mean the design is ready for contribution, not that its behavior is built or accepted.

## Outcome and boundaries

EG is the converged engine for EG-owned data: property graph, SQL, RDF/OWL, vector, time series, provenance and reasoning over one durable authority and one query surface. Third-party applications keep their real database. EG attaches to each through a single owner-scoped registry, serves live SQL and named virtual graphs, optionally maintains a change-captured accelerated copy, and exposes a freshness-aware query route. Native hosting is an app-specific admission track, first measured with Gramps. Immich is an API connector and entity-resolution pilot; it is not a native-hosting candidate. Ghostfolio is a one-way finance import from attached Postgres until an explicit retirement decision.

This spec does not authorize a production cutover, direct writes around an application's business logic, automatic `owl:sameAs` assertions, or a performance claim without the stated benchmark gate. P6 Gramps cutover requires the operator's go-ahead after P0–P5 evidence.

## Functional requirements

| ID | Requirement | Ledger | Acceptance intent |
|---|---|---|---|
| DP-01 | Record the attached-source default, native-hosting gate, router consistency and pgrx boundary as ADRs. | EH-660 | Reviewed ADRs DB-7.0, DB-9.3, DB-9.4. |
| DP-02 | Register one tenant/owner-scoped attached source with secrets by reference, verified outbound target, capabilities and mirror direction. Replace separate foreign-source and transient OBDA registration paths; consume the kernel-owned mirror cursor/replay contract. | EH-661 | Same source identity serves SQL, OBDA, CDC and mirror; unauthorized cross-owner reads and unverified addresses fail closed. |
| DP-03 | Read each dialect catalog into a versioned, typed, hashed schema graph; profile under bounded policy; infer dependencies and deterministic Leiden groups. Every artifact has a named OBDA or `schema_context` consumer. | EH-662, EH-663, EH-693 | Stable hash/order on repeated reads; no inert RF-031 style nodes. |
| DP-04 | Compile deterministic-first, operator-approved ontology mappings to versioned R2RML named virtual graphs. Model output is proposal only. Query by name through SPARQL, UQL and REASON. | EH-664 | Unapproved/broken mapping cannot expose data. |
| DP-05 | Push typed projection, filter, same-source join, aggregate, order and limit to the source when supported; use DataFusion providers for attached tables; show every fallback in EXPLAIN. | EH-665, EH-666 | Native-source differential equality across dialects and null/type edges. |
| DP-06 | Normalize change capture to `ChangeEnvelope`, persist source position per subscription, replay idempotently, detect schema drift, update approved graph/search/vector/event consumers and optional `eg-lake` copies. | EH-667–EH-669, EH-680 | Snapshot equals replay after crash/failover; lag and retention guardrails visible. |
| DP-07 | Route native, live or accelerated reads by policy and freshness. A client asking read-your-writes waits for its committed source position within a bounded timeout; no stale success on timeout. | EH-670 | EXPLAIN states route/position; stale route fails or falls back according to declared policy. |
| DP-08 | Govern attached-source write-back with approval, idempotency and EH-658 audit reservation. Prefer the app API when it owns business rules. | EH-671 | Missing audit/approval, replayed key, and policy denial fail safely. |
| DP-09 | Implement five-part adapters (driver, rendering, catalog, change capture where offered, type map) for Postgres, MySQL, MariaDB, SQLite, MSSQL, ClickHouse, Oracle, Db2, MongoDB/DocumentDB, Snowflake, BigQuery, DuckDB and Iceberg; use Debezium bridge where specified. | EH-672–EH-681 | Per engine/version conformance matrix; unsupported capture explicitly declared, never silently simulated. |
| DP-10 | Admit shared CloudNativePG and MariaDB services and apps one at a time, with separate roles, PITR and restore drills; retire an ingest connector only after attached parity. The optional Ghostfolio finance import is governed by `finance-primitives`. | EH-682, EH-683 | Three attached apps including MariaDB; rollback and one-way finance import integration proven. |
| DP-11 | Earn native hosting with the Gramps traffic/differential/restore pilot, then admit only observed pgwire gaps and app-specific safety controls. Consume the kernel-owned hot-store, cross-store atomicity and operational tooling contracts. | EH-686–EH-689, EH-695, EH-697 | P0–P5 evidence, reviewed deviations, crash/restore tests; P6 separately approved. |
| DP-12 | Resolve entities across apps through blocking, similarity and Fellegi–Sunter proposals; deliver the Immich API connector and its incremental, private KG ingest. | EH-694, EH-696 | Approved links only; per-user isolation and incremental no-op replay. |

The native fast path, durability classes, RAM hot structures, batch pipelining, plan cache and comparative benchmark gate (EH-684/EH-685/EH-690–EH-692/EH-717–EH-722) are specified by `durable-graph-kernel`. This data-plane spec consumes their tested interfaces and records app-level integration results; it does not assign those IDs a second owner.

## Source and capability contract

An attached source is identified by an immutable source ID and scoped to one tenant and owner. Its record contains endpoint identity, a secret reference, dialect/version, supported query and capture capabilities, table grants, mapping versions, capture subscription and cursor, acceleration policy, quotas, and read/write/mirror direction. Registration verifies the endpoint against outbound policy before persistence. Per-request DSNs and inline passwords are forbidden. Revocation stops new plans and makes active reads follow a documented drain policy. An adapter may report `unsupported` for capture or pushdown; that state is visible to the planner and conformance harness. No capability is inferred solely from a dialect name.

The source database remains the write authority for attached application data. EG's native store is the authority for EG-owned data, approved mappings, catalog artifacts, capture cursors and local accelerated copies. A mirror is a write sink and never a query authority. A change-captured copy is a derived read path, selected only when its version and freshness meet the request's consistency contract.

## Native-hosting qualification

Postgres wire compatibility is a measured gate, not a blanket promise. Capture an unmodified candidate application's driver startup, migrations and real statement corpus. Test the candidate against real Postgres and EG with equal schema/data, comparing rows, column types, ordered results and SQLSTATE. The compatibility inventory includes B-tree indexes, collation, POSIX regex, `bytea`, sequences, savepoints, row/advisory locks, `LISTEN/NOTIFY`, triggers, PL/pgSQL, JSONB operators and indexes, transaction isolation, driver `SET`/`SHOW`, SQLSTATE, schemas/search path, constraints and referential actions, enums/domains/composites, materialized views, large objects, timezone and collation behavior. Implement only gaps proven by captured traffic or a separately approved admission requirement; record each passing or failing case. No app is admitted until its migration tool passes, differential deviations are reviewed, replica lag objective is met, and every backup path has been restored successfully.

The Gramps reference pilot has seven phases. P0 records an export, object counts and content digest. P1 imports that export into a disposable real-Postgres tree, verifies an equal export and captures the full driver statement corpus. P2 replays the corpus against an unmodified EG tenant and records each SQLSTATE; no EG code change is part of measurement. P3 fixes only proven gaps with a failing test first and requires equal import/export content or reviewed deviations. P4 verifies a real-Postgres replica, a dump and an EG backup by independent scratch restores, plus kill-during-import recovery. P5 profiles the catalog and JSON shape, approves R2RML mapping to Person/Family/Event, answers a grandparents query over live tables and exposes `schema_context`. P6 is a separate, explicitly authorized production decision after P0–P5; it requires the replica, independent nightly export, rehearsed fallback and 30 days of observation. A declined P6 closes the native-hosting track with a written evidence report, not a false cutover claim.

The Immich reference pilot is an API connector, not a hosted database. I0 generates a version-pinned client from a checked-in OpenAPI fixture and records its digest. I1 provides read-first domain MCP tools and approval-gated writes. I2 registers the connector in all release catalogs and validates catalog parity. I3 ingests assets, albums and per-user people incrementally, proving that a second run is a no-op and one new upload appears. I4 names representative face clusters through the application UI. I5 emits cross-app Person links as proposals and approves selected links before a cross-app SPARQL question. I6 adds health, lag and upgrade-digest checks. Face vectors remain with the application for this pilot. Per-user media, location and face data never cross user scope through this connector.

## Required outcomes and exclusions

At least three self-hosted apps, including one on MariaDB, must be attached end to end through the registry. Their old ingest connectors retire only after result and change parity. Ghostfolio is an optional one-way import into the finance ontology; idempotent import precedes any separate retirement decision. Every dialect tier gets a green versioned conformance entry for its declared capabilities. EG mirrors must replay and reconcile graph and SQL-table changes and pass a real-Postgres restore drill. The native track closes through the Gramps acceptance evidence or a documented decision not to cut over. No external Redis or graph mirror may be introduced as a read-path authority; no second connector registry, cache product or storage authority may be created.

## Acceptance states

`PROPOSED` is a design artifact. `IMPLEMENTED-UNVALIDATED` needs a merged owner commit and linked tests. `ACCEPTED` needs passing exact-head conformance, security, quality and operational evidence. `CUTOVER` needs recorded consumer switch and rollback rehearsal. `DELETED` needs proof that old registration/ingest/mirror paths were removed. `DONE` needs every applicable task and evidence entry in this spec reconciled. A task checkbox means work was planned or performed; it never alone changes state.

## Program exit

All attached-source dialect tiers must pass their applicable conformance rows; three self-hosted apps including a MariaDB app must be attached with ingest connectors retired; the accelerated copy must hold its lag objective across restart and failover; EG mirrors must match prior fan-out semantics and restore SQL tables into real Postgres; Gramps must either meet the native-hosting exit evidence or close with an evidence report if P6 is not approved. Record DB-7.0, DB-9.3 and DB-9.4 ADRs. The [test specification](test-spec.md) defines failures as well as successes, and [tasks](tasks.md) retains every EH row without marking it built.
