# Delivery tasks and traceability

**State:** PROPOSED. All 33 entries are unchecked and waiting to be built. Each entry's requirement ID, as defined in [requirements.md](requirements.md), preserves traceability; this checklist and its linked evidence must be sufficient for a public contributor to understand status. Change a checkbox only with a linked merged commit and test evidence. A checkbox is never acceptance by itself. [Spec](spec.md) · [design](plan.md) · [tests](test-spec.md).

## Foundation and understanding

- [x] **EG-UNIFIED-DATA-PLANE-R001:** Decide attached-source default, consistency, native admission and pgrx boundary by ADR. Recorded in [docs/architecture/unified_data_plane_adr.md](../../docs/architecture/unified_data_plane_adr.md) (five decisions, each quoting the merged spec.md/plan.md text it formalizes); the separate pgrx go/no-go spike decision remains EG-UNIFIED-DATA-PLANE-R025.
- [ ] **EG-UNIFIED-DATA-PLANE-R002:** Merge source registration, owner scope, secret references, verified outbound checks and five-part adapter contract; depends on EG-FEDERATED-QUERY-R013.
- [ ] **EG-UNIFIED-DATA-PLANE-R003:** Build versioned typed catalog extraction with OBDA and `schema_context` consumers; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R003.1:** Typed, hashed `AttachedCatalogGraph` model with reorder-stable hashing and duplicate-table refusal; part of EG-UNIFIED-DATA-PLANE-R003 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R004:** Add bounded profiling, JSON shape, dependency inference and fixed-seed Leiden grouping; depends on EG-UNIFIED-DATA-PLANE-R003.
- [ ] **EG-UNIFIED-DATA-PLANE-R004.1:** Typed, bounded `ColumnSamplingBudget` with zero/over-ceiling refusal; part of EG-UNIFIED-DATA-PLANE-R004 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R005:** Approve and version R2RML named virtual graphs; depends on EG-UNIFIED-DATA-PLANE-R004 and EG-DECISION-ENGINE-R033.
- [ ] **EG-UNIFIED-DATA-PLANE-R005.1:** Typed `MappingApprovalState`/`VirtualGraphMapping` refusing an unapproved query; part of EG-UNIFIED-DATA-PLANE-R005 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R029:** Expose `schema_context` through Graph OS; depends on EG-UNIFIED-DATA-PLANE-R004.
- [ ] **EG-UNIFIED-DATA-PLANE-R030:** Add cross-app entity proposals, approval and evidence; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R031:** Define isolation, quotas, compatibility, native admission and rollback; depends on EG-UNIFIED-DATA-PLANE-R002.

## Query, change and write paths

- [ ] **EG-UNIFIED-DATA-PLANE-R006:** Typed OBDA join/aggregate/order/limit pushdown and explicit fallback; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R006.1:** Typed `PushdownDecision`/`PushdownFallbackReason` refusing a contradictory decision; part of EG-UNIFIED-DATA-PLANE-R006 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R007:** Attached table DataFusion providers and dialect-aware sub-plan pushdown; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R008:** Durable source-position CDC, replay, lag and drift; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R008.1:** Typed `ChangeEnvelope`/`ChangeOp` model with source-position and keyless-table refusal rules; part of EG-UNIFIED-DATA-PLANE-R008 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R009:** Idempotent mapped graph, search/vector and event consumers; depends on EG-UNIFIED-DATA-PLANE-R005.
- [ ] **EG-UNIFIED-DATA-PLANE-R010:** Policy-controlled `eg-lake` accelerated copy; depends on EG-UNIFIED-DATA-PLANE-R008.
- [ ] **EG-UNIFIED-DATA-PLANE-R010.1:** Typed per-table `AccelerationPolicy` model and validation; part of EG-UNIFIED-DATA-PLANE-R010 (parent rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R011:** Freshness-aware native/live/accelerated router and EXPLAIN; depends on EG-UNIFIED-DATA-PLANE-R007.
- [ ] **EG-UNIFIED-DATA-PLANE-R012:** Governed idempotent write-back through SDK/app API and EG-DURABLE-KERNEL-R031 audit; depends on EG-UNIFIED-DATA-PLANE-R002.

## Dialects and conformance

- [ ] **EG-UNIFIED-DATA-PLANE-R022:** Build native-source differential and CDC replay conformance harness; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R022.1:** `DialectConformanceEntry`/`ConformanceReport` typed model plus auto-suppression refusal (`crates/eg-types/src/dialect_conformance.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R022.2:** Containerized matrix runner per adapter/engine version.
- [ ] **EG-UNIFIED-DATA-PLANE-R022.3:** Change-capture replay comparison against a source snapshot.
- [ ] **EG-UNIFIED-DATA-PLANE-R013:** Postgres catalog/query/pgoutput/type adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R014:** Separate MySQL and MariaDB query/catalog/binlog/type entries; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R015:** SQLite lock-safe file attach, WITHOUT ROWID and safe WAL/watermark capture; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R016:** MSSQL catalog/T-SQL/CDC adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R017:** ClickHouse federation/acceleration adapter with explicit no-CDC capability; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R018:** Oracle and Db2 catalog/query through driver/ODBC and Debezium capture; depends on EG-UNIFIED-DATA-PLANE-R021.
- [ ] **EG-UNIFIED-DATA-PLANE-R019:** MongoDB/DocumentDB document catalog and change streams; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R019.1:** `DocumentSourceCatalog` typed inferred-shape model plus field-path refusal (`crates/eg-types/src/document_source_catalog.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R019.2:** Native MongoDB/DocumentDB driver connection and change-stream capture into `ChangeEnvelope`.
- [ ] **EG-UNIFIED-DATA-PLANE-R019.3:** Conformance entry comparing EG query results and captured change stream against native MongoDB/DocumentDB.
- [ ] **EG-UNIFIED-DATA-PLANE-R020:** Snowflake, BigQuery, DuckDB and Iceberg federation; depends on EG-UNIFIED-DATA-PLANE-R007.
- [x] **EG-UNIFIED-DATA-PLANE-R020.1:** `WarehouseSourceKind`/`WarehouseSourceConfig` typed model plus required-field refusal (`crates/eg-types/src/warehouse_federation.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R020.2:** Query pushdown per kind (Arrow Flight SQL where offered), extending `eg-query::sql::iceberg_federation` for Iceberg.
- [ ] **EG-UNIFIED-DATA-PLANE-R020.3:** Conformance entry comparing federated query results against each native warehouse/lake engine.
- [ ] **EG-UNIFIED-DATA-PLANE-R021:** Debezium Kafka-to-`ChangeEnvelope` bridge; depends on EG-UNIFIED-DATA-PLANE-R008.
- [x] **EG-UNIFIED-DATA-PLANE-R021.1:** `DebeziumChangeEvent` typed envelope shape plus op-code and before/after refusal (`crates/eg-types/src/debezium_bridge.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R021.2:** Kafka consumer plus real conversion into `ChangeEnvelope` records.
- [ ] **EG-UNIFIED-DATA-PLANE-R021.3:** Replay test feeding a captured Debezium event stream through the bridge.

## Platform and app admission

- [ ] **EG-UNIFIED-DATA-PLANE-R023:** Build shared CloudNativePG and MariaDB platform with per-app roles, PITR and restore drill.
- [x] **EG-UNIFIED-DATA-PLANE-R023.1:** `SharedPlatformClusterGroup`/`ApplicationRoleSet` typed model plus engine-specific refusal (`crates/eg-types/src/shared_db_platform.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R023.2:** CloudNativePG/MariaDB operator wiring and PITR schedule to object storage.
- [ ] **EG-UNIFIED-DATA-PLANE-R023.3:** Restore-drill automation and post-restore data-match verification.
- [ ] **EG-UNIFIED-DATA-PLANE-R024:** Admit three apps including MariaDB with rollback and retire each ingest connector only after parity; depends on EG-UNIFIED-DATA-PLANE-R023.
- [x] **EG-UNIFIED-DATA-PLANE-R024.1:** `ApplicationAdmission` typed stage machine plus early-retirement refusal (`crates/eg-types/src/platform_admission.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R024.2:** Admission runner and tested rollback path per candidate application.
- [ ] **EG-UNIFIED-DATA-PLANE-R024.3:** Per-application admission test confirming rollback and that the retired connector no longer writes.
- [ ] **EG-UNIFIED-DATA-PLANE-R032 / Immich I0–I6:** Generated API client, MCP tools, registration, incremental per-user ingest, naming, approved links and operations. Implement I0–I6 as defined in [spec.md](spec.md).

## Native hosting and app admission

Native storage and wire work (EG-DURABLE-KERNEL-R032/EG-DURABLE-KERNEL-R033/EG-DURABLE-KERNEL-R034–EG-DURABLE-KERNEL-R036/EG-DURABLE-KERNEL-R037–EG-DURABLE-KERNEL-R042) is implemented and evidenced by `durable-graph-kernel`; the one-way Ghostfolio import EG-FINANCE-PRIMITIVES-R012 is implemented and evidenced by `finance-primitives`. This spec consumes their exact-head test receipts before app admission, without duplicating those work items.

- [ ] **EG-UNIFIED-DATA-PLANE-R025:** Separate pgrx companion spike and go/no-go ADR; depends on EG-UNIFIED-DATA-PLANE-R001.
- [x] **EG-UNIFIED-DATA-PLANE-R025.1:** `PgrxSpikeDecision` typed model plus full-scope-evidence refusal (`crates/eg-types/src/pgrx_spike.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R025.2:** Run the spike across its declared scope areas.
- [ ] **EG-UNIFIED-DATA-PLANE-R025.3:** Review the spike's results and recorded decision; produce the approved ADR.
- [ ] **EG-UNIFIED-DATA-PLANE-R026 / Gramps P0–P5:** Baseline, real-Postgres control, unmodified EG replay, pilot fixes, restores and OBDA understanding; depends on EG-DURABLE-KERNEL-R036. Implement P0–P5 and their exit artifacts as defined in [spec.md](spec.md) and [test-spec.md](test-spec.md).
- [x] **EG-UNIFIED-DATA-PLANE-R026.1:** `GrampsPilotProgress`/`PhaseExitArtifact` typed P0-P5 gate plus wrong/unreviewed/out-of-order refusal (`crates/eg-types/src/gramps_pilot_phase.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R026.2:** Run P0-P2 (baseline digest, Postgres control, unmodified corpus replay).
- [ ] **EG-UNIFIED-DATA-PLANE-R026.3:** Run P3-P5 (proven-gap fixes, restore verification, schema-understanding demonstration).
- [ ] **EG-UNIFIED-DATA-PLANE-R027:** Fix Postgres features only when captured app traffic proves need; include B-tree indexes, collation, regex, bytea, sequences, savepoints, locks, notify, triggers/functions, JSONB, isolation, startup, SQLSTATE, schemas, constraints, types, materialized views, large objects and timezone; depends on EG-UNIFIED-DATA-PLANE-R026.
- [ ] **EG-UNIFIED-DATA-PLANE-R028:** Differential Postgres regression, generated and captured-traffic corpora with reviewed deviations and no ratchet; depends on EG-UNIFIED-DATA-PLANE-R022.
- [ ] **EG-UNIFIED-DATA-PLANE-R033 / Gramps P6:** Separate explicitly authorized production cutover, replica, independent nightly export and 30-day read-only SQLite fallback; depends on EG-UNIFIED-DATA-PLANE-R026 and every P0–P5 acceptance artifact.

## Virtual graphs

- [ ] **EG-UNIFIED-DATA-PLANE-R037:** Ship `core:virtual-graph@1` with SHACL shapes and positive and negative fixtures.
- [ ] **EG-UNIFIED-DATA-PLANE-R038:** Write a hashed `MetadataContract` for every source kind; add the acceleration policy; prove live reads by default and copy only for declared hot subsets.

## Completion record for each row

For each row, add a subentry containing its merged commit, exact-head test run, conformance/quality reports, operational drill where relevant, cross-repository consumer and old-path deletion. Record one of `PROPOSED`, `IMPLEMENTED-UNVALIDATED`, `ACCEPTED`, `CUTOVER`, `DELETED` or `DONE` with dated evidence. Update [status.json](status.json) only from proved evidence. `ACCEPTED` requires all applicable gates in [test-spec.md](test-spec.md); `CUTOVER` and `DONE` additionally require rollout, rollback and consumer evidence. Preserve failed runs and corrections rather than rewriting history. Rows above remain unchecked until that evidence exists.
