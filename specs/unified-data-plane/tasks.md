# Delivery tasks and traceability

**State:** PROPOSED. All 33 entries are unchecked and waiting to be built. Each entry's requirement ID, as defined in [requirements.md](requirements.md), preserves traceability; this checklist and its linked evidence must be sufficient for a public contributor to understand status. Change a checkbox only with a linked merged commit and test evidence. A checkbox is never acceptance by itself. [Spec](spec.md) · [design](plan.md) · [tests](test-spec.md).

## Foundation and understanding

- [x] **EG-UNIFIED-DATA-PLANE-R001:** Decide attached-source default, consistency, native admission and pgrx boundary by ADR. Recorded in [docs/architecture/unified_data_plane_adr.md](../../docs/architecture/unified_data_plane_adr.md) (five decisions, each quoting the merged spec.md/plan.md text it formalizes); the separate pgrx go/no-go spike decision remains EG-UNIFIED-DATA-PLANE-R025.
- [ ] **EG-UNIFIED-DATA-PLANE-R002:** Merge source registration, owner scope, secret references, verified outbound checks and five-part adapter contract; depends on EG-FEDERATED-QUERY-R013.
- [ ] **EG-UNIFIED-DATA-PLANE-R003:** Build versioned typed catalog extraction with OBDA and `schema_context` consumers; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R004:** Add bounded profiling, JSON shape, dependency inference and fixed-seed Leiden grouping; depends on EG-UNIFIED-DATA-PLANE-R003.
- [ ] **EG-UNIFIED-DATA-PLANE-R005:** Approve and version R2RML named virtual graphs; depends on EG-UNIFIED-DATA-PLANE-R004 and EG-DECISION-ENGINE-R033.
- [ ] **EG-UNIFIED-DATA-PLANE-R029:** Expose `schema_context` through Graph OS; depends on EG-UNIFIED-DATA-PLANE-R004.
- [ ] **EG-UNIFIED-DATA-PLANE-R030:** Add cross-app entity proposals, approval and evidence; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R031:** Define isolation, quotas, compatibility, native admission and rollback; depends on EG-UNIFIED-DATA-PLANE-R002.

## Query, change and write paths

- [ ] **EG-UNIFIED-DATA-PLANE-R006:** Typed OBDA join/aggregate/order/limit pushdown and explicit fallback; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R007:** Attached table DataFusion providers and dialect-aware sub-plan pushdown; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R008:** Durable source-position CDC, replay, lag and drift; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R009:** Idempotent mapped graph, search/vector and event consumers; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R010:** Policy-controlled `eg-lake` accelerated copy; depends on EG-UNIFIED-DATA-PLANE-R008.
- [ ] **EG-UNIFIED-DATA-PLANE-R011:** Freshness-aware native/live/accelerated router and EXPLAIN; depends on EG-UNIFIED-DATA-PLANE-R007.
- [ ] **EG-UNIFIED-DATA-PLANE-R011.1:** Typed `FreshnessRoute`/`ReadYourWritesWait` refusing an unbounded wait; part of EG-UNIFIED-DATA-PLANE-R011 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R012:** Governed idempotent write-back through SDK/app API and EG-DURABLE-KERNEL-R031 audit; depends on EG-UNIFIED-DATA-PLANE-R002.

## Dialects and conformance

- [ ] **EG-UNIFIED-DATA-PLANE-R022:** Build native-source differential and CDC replay conformance harness; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R013:** Postgres catalog/query/pgoutput/type adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R014:** Separate MySQL and MariaDB query/catalog/binlog/type entries; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R015:** SQLite lock-safe file attach, WITHOUT ROWID and safe WAL/watermark capture; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R016:** MSSQL catalog/T-SQL/CDC adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R017:** ClickHouse federation/acceleration adapter with explicit no-CDC capability; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R018:** Oracle and Db2 catalog/query through driver/ODBC and Debezium capture; depends on EG-UNIFIED-DATA-PLANE-R021.
- [ ] **EG-UNIFIED-DATA-PLANE-R019:** MongoDB/DocumentDB document catalog and change streams; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R020:** Snowflake, BigQuery, DuckDB and Iceberg federation; depends on EG-UNIFIED-DATA-PLANE-R007.
- [ ] **EG-UNIFIED-DATA-PLANE-R021:** Debezium Kafka-to-`ChangeEnvelope` bridge; depends on EG-UNIFIED-DATA-PLANE-R008.

## Platform and app admission

- [ ] **EG-UNIFIED-DATA-PLANE-R023:** Build shared CloudNativePG and MariaDB platform with per-app roles, PITR and restore drill.
- [ ] **EG-UNIFIED-DATA-PLANE-R024:** Admit three apps including MariaDB with rollback and retire each ingest connector only after parity; depends on EG-UNIFIED-DATA-PLANE-R023.
- [ ] **EG-UNIFIED-DATA-PLANE-R032 / Immich I0–I6:** Generated API client, MCP tools, registration, incremental per-user ingest, naming, approved links and operations. Implement I0–I6 as defined in [spec.md](spec.md).

## Native hosting and app admission

Native storage and wire work (EG-DURABLE-KERNEL-R032/EG-DURABLE-KERNEL-R033/EG-DURABLE-KERNEL-R034–EG-DURABLE-KERNEL-R036/EG-DURABLE-KERNEL-R037–EG-DURABLE-KERNEL-R042) is implemented and evidenced by `durable-graph-kernel`; the one-way Ghostfolio import EG-FINANCE-PRIMITIVES-R012 is implemented and evidenced by `finance-primitives`. This spec consumes their exact-head test receipts before app admission, without duplicating those work items.

- [ ] **EG-UNIFIED-DATA-PLANE-R025:** Separate pgrx companion spike and go/no-go ADR; depends on EG-UNIFIED-DATA-PLANE-R001.
- [ ] **EG-UNIFIED-DATA-PLANE-R026 / Gramps P0–P5:** Baseline, real-Postgres control, unmodified EG replay, pilot fixes, restores and OBDA understanding; depends on EG-DURABLE-KERNEL-R036. Implement P0–P5 and their exit artifacts as defined in [spec.md](spec.md) and [test-spec.md](test-spec.md).
- [ ] **EG-UNIFIED-DATA-PLANE-R027:** Fix Postgres features only when captured app traffic proves need; include B-tree indexes, collation, regex, bytea, sequences, savepoints, locks, notify, triggers/functions, JSONB, isolation, startup, SQLSTATE, schemas, constraints, types, materialized views, large objects and timezone; depends on EG-UNIFIED-DATA-PLANE-R026.
- [ ] **EG-UNIFIED-DATA-PLANE-R028:** Differential Postgres regression, generated and captured-traffic corpora with reviewed deviations and no ratchet; depends on EG-UNIFIED-DATA-PLANE-R022.
- [ ] **EG-UNIFIED-DATA-PLANE-R033 / Gramps P6:** Separate explicitly authorized production cutover, replica, independent nightly export and 30-day read-only SQLite fallback; depends on EG-UNIFIED-DATA-PLANE-R026 and every P0–P5 acceptance artifact.

## Virtual graphs

- [ ] **EG-UNIFIED-DATA-PLANE-R037:** Ship `core:virtual-graph@1` with SHACL shapes and positive and negative fixtures.
- [ ] **EG-UNIFIED-DATA-PLANE-R038:** Write a hashed `MetadataContract` for every source kind; add the acceleration policy; prove live reads by default and copy only for declared hot subsets.

## Completion record for each row

For each row, add a subentry containing its merged commit, exact-head test run, conformance/quality reports, operational drill where relevant, cross-repository consumer and old-path deletion. Record one of `PROPOSED`, `IMPLEMENTED-UNVALIDATED`, `ACCEPTED`, `CUTOVER`, `DELETED` or `DONE` with dated evidence. Update [status.json](status.json) only from proved evidence. `ACCEPTED` requires all applicable gates in [test-spec.md](test-spec.md); `CUTOVER` and `DONE` additionally require rollout, rollback and consumer evidence. Preserve failed runs and corrections rather than rewriting history. Rows above remain unchecked until that evidence exists.
