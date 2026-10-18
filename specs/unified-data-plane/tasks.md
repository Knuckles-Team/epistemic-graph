# Delivery tasks and traceability

**State:** PROPOSED. All 33 entries are unchecked and waiting to be built. Each entry's requirement ID, as defined in [requirements.md](requirements.md), preserves traceability; this checklist and its linked evidence must be sufficient for a public contributor to understand status. Change a checkbox only with a linked merged commit and test evidence. A checkbox is never acceptance by itself. [Spec](spec.md) · [design](plan.md) · [tests](test-spec.md).

## Foundation and understanding

- [x] **EG-UNIFIED-DATA-PLANE-R001:** Decide attached-source default, consistency, native admission and pgrx boundary by ADR. Recorded in [docs/architecture/unified_data_plane_adr.md](../../docs/architecture/unified_data_plane_adr.md) (five decisions, each quoting the merged spec.md/plan.md text it formalizes); the separate pgrx go/no-go spike decision remains EG-UNIFIED-DATA-PLANE-R025.
- [x] **EG-UNIFIED-DATA-PLANE-R002 (rollup):** Merge source registration, owner scope, secret references, verified outbound checks and five-part adapter contract; depends on EG-FEDERATED-QUERY-R013. Split by code root per SPEC-SIZING-AND-DEPENDENCIES.md (score > 6, 3 code roots): LANDED only when R002.1, R002.2 and R002.3 are all LANDED.
  - [x] **EG-UNIFIED-DATA-PLANE-R002.1:** Verify the outbound destination before persisting a `ForeignSourceSpec::Sql` registration (producer path, `src/server/foreign_catalog.rs` + `crates/eg-plan/src/federation_ssrf.rs`). No dependency.
  - [x] **EG-UNIFIED-DATA-PLANE-R002.2:** Verify the outbound destination before persisting an OBDA `external_sources` registration (consumer path, `src/server/handlers/rdf/obda.rs`); depends on EG-UNIFIED-DATA-PLANE-R002.1.
  - [x] **EG-UNIFIED-DATA-PLANE-R002.3:** Verify the outbound destination before persisting a kernel mirror-target registration (sink path). Depends on EG-UNIFIED-DATA-PLANE-R002.1 and the same-repo EG-DURABLE-KERNEL-R024 (BUILDING; no mirror-target registration code root exists yet).
- [x] **EG-UNIFIED-DATA-PLANE-R003:** Build versioned typed catalog extraction with OBDA and `schema_context` consumers; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R003.1:** Typed, hashed `AttachedCatalogGraph` model with reorder-stable hashing and duplicate-table refusal; part of EG-UNIFIED-DATA-PLANE-R003 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R004:** Add bounded profiling, JSON shape, dependency inference and fixed-seed Leiden grouping; depends on EG-UNIFIED-DATA-PLANE-R003.
- [x] **EG-UNIFIED-DATA-PLANE-R004.1:** Typed, bounded `ColumnSamplingBudget` with zero/over-ceiling refusal; part of EG-UNIFIED-DATA-PLANE-R004 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R005:** Approve and version R2RML named virtual graphs; depends on EG-UNIFIED-DATA-PLANE-R004 and EG-DECISION-ENGINE-R033.
- [x] **EG-UNIFIED-DATA-PLANE-R005.1:** Typed `MappingApprovalState`/`VirtualGraphMapping` refusing an unapproved query; part of EG-UNIFIED-DATA-PLANE-R005 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R029:** Expose `schema_context` through Graph OS; depends on EG-UNIFIED-DATA-PLANE-R004.
- [x] **EG-UNIFIED-DATA-PLANE-R029.1:** `SchemaContextCatalog`/`SchemaContextAnswer` typed model plus unknown-table refusal (`crates/eg-types/src/schema_context.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R029.2:** Wire the real `schema_context` query entry point through Graph OS, following the `code_context` pattern.
- [x] **EG-UNIFIED-DATA-PLANE-R030:** Add cross-app entity proposals, approval and evidence; depends on EG-UNIFIED-DATA-PLANE-R005.
- [x] **EG-UNIFIED-DATA-PLANE-R030.1:** `EntityLinkProposal` typed model plus unapproved/non-Match assertion refusal (`crates/eg-types/src/entity_link_proposal.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R030.2:** Blocking and field-similarity candidate generation across Gramps/Immich/Twenty/Firefly.
- [ ] **EG-UNIFIED-DATA-PLANE-R030.3:** Real Fellegi-Sunter scoring and the approval workflow.
- [ ] **EG-UNIFIED-DATA-PLANE-R031 (rollup):** Define isolation, quotas, compatibility, native admission and rollback; depends on EG-UNIFIED-DATA-PLANE-R002.
  - [x] **EG-UNIFIED-DATA-PLANE-R031.1:** `ApplicationAdmission`/`ConsolidationIsolationRegistry` typed model plus identity/quota refusals (`crates/eg-types/src/consolidation_isolation.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R031.2:** Real fault-isolation enforcement and quota metering.

## Query, change and write paths

- [x] **EG-UNIFIED-DATA-PLANE-R006:** Typed OBDA join/aggregate/order/limit pushdown and explicit fallback; depends on EG-UNIFIED-DATA-PLANE-R005.
- [x] **EG-UNIFIED-DATA-PLANE-R006.1:** Typed `PushdownDecision`/`PushdownFallbackReason` refusing a contradictory decision; part of EG-UNIFIED-DATA-PLANE-R006 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R007:** Attached table DataFusion providers and dialect-aware sub-plan pushdown; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R007.1:** Typed `DialectPushdownCapabilities` refusing an unknown operation name; part of EG-UNIFIED-DATA-PLANE-R007 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R008:** Durable source-position CDC, replay, lag and drift; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R008.1:** Typed `ChangeEnvelope`/`ChangeOp` model with source-position and keyless-table refusal rules; part of EG-UNIFIED-DATA-PLANE-R008 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R009:** Idempotent mapped graph, search/vector and event consumers; depends on EG-UNIFIED-DATA-PLANE-R005.
- [x] **EG-UNIFIED-DATA-PLANE-R009.1:** Typed `ConsumerCheckpoint` with idempotent `apply_if_newer`; part of EG-UNIFIED-DATA-PLANE-R009 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R010:** Policy-controlled `eg-lake` accelerated copy; depends on EG-UNIFIED-DATA-PLANE-R008.
- [x] **EG-UNIFIED-DATA-PLANE-R010.1:** Typed per-table `AccelerationPolicy` model and validation; part of EG-UNIFIED-DATA-PLANE-R010 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R011:** Freshness-aware native/live/accelerated router and EXPLAIN; depends on EG-UNIFIED-DATA-PLANE-R007.
- [x] **EG-UNIFIED-DATA-PLANE-R011.1:** Typed `FreshnessRoute`/`ReadYourWritesWait` refusing an unbounded wait; part of EG-UNIFIED-DATA-PLANE-R011 (parent rollup).
- [x] **EG-UNIFIED-DATA-PLANE-R012:** Governed idempotent write-back through SDK/app API and EG-DURABLE-KERNEL-R031 audit; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R012.1:** Typed `AttachedSourceWriteRequest`/`validate` model in `eg-types::attached_source_governance` with refusal tests for missing approval, audit reservation, idempotency key and business-logic-bypassing direct writes. Depends on EG-UNIFIED-DATA-PLANE-R012.

## Dialects and conformance

- [x] **EG-UNIFIED-DATA-PLANE-R022:** Build native-source differential and CDC replay conformance harness; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R022.1:** `DialectConformanceEntry`/`ConformanceReport` typed model plus auto-suppression refusal (`crates/eg-types/src/dialect_conformance.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R022.2:** Containerized matrix runner per adapter/engine version.
- [ ] **EG-UNIFIED-DATA-PLANE-R022.3 (rollup):** Change-capture replay comparison against a source snapshot.
  - [x] **EG-UNIFIED-DATA-PLANE-R022.3.1:** `compare_replay_to_snapshot` pure comparison and conformance-entry construction (`crates/eg-types/src/replay_conformance.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R022.3.2:** Live comparison against a real replay stream and source snapshot.
- [x] **EG-UNIFIED-DATA-PLANE-R013:** Postgres catalog/query/pgoutput/type adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R014:** Separate MySQL and MariaDB query/catalog/binlog/type entries; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R014.1:** Add the typed `SqlEngineKind`/`SqlEngineCaptureSupport`/`SqlEngineDialectEntry` model (`crates/eg-types/src/attached_source_dialect.rs`) proving MySQL and MariaDB stay separate, explicitly declared dialect entries, with refusal tests for a missing version floor and an unreviewed type-map revision. The driver, rendering, catalog reader and binlog capture parts are `EG-UNIFIED-DATA-PLANE-R014.2`+ (not in scope for this slice; depend on `EG-UNIFIED-DATA-PLANE-R002`). Test: `crates/eg-types/src/attached_source_dialect.rs::tests::mysql_and_mariadb_are_distinct_entries`.
- [ ] **EG-UNIFIED-DATA-PLANE-R014.2 (rollup):** Remaining scope of R014 (driver, rendering, catalog reader, binlog capture). LANDED only once every child is.
  - [x] **EG-UNIFIED-DATA-PLANE-R014.2.1:** Adapter skeleton — `MysqlMariadbConnectionConfig`/`MysqlMariadbDriverConfig` typed mapping plus the no-live-database refusal path (`crates/eg-types/src/mysql_mariadb_connection.rs`), behind the existing `federation-sql` sqlx-mysql feature structure. Test: `crates/eg-types/src/mysql_mariadb_connection.rs::tests`.
  - [ ] **EG-UNIFIED-DATA-PLANE-R014.2.2:** Live row-based binlog/GTID capture, information_schema catalog read, and per-engine (MySQL, MariaDB) conformance entries.
- [x] **EG-UNIFIED-DATA-PLANE-R015 (rollup):** SQLite lock-safe file attach, WITHOUT ROWID and safe WAL/watermark capture; depends on EG-UNIFIED-DATA-PLANE-R002.
  - [x] **EG-UNIFIED-DATA-PLANE-R015.1:** `SqliteSourceCatalog` typed model plus WITHOUT-ROWID capture-safety refusal (`crates/eg-types/src/sqlite_attached_catalog.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R015.2 (rollup):** Lock-safe live read-only attach and real WAL-frame tailing/watermark polling. LANDED only once every child is.
    - [x] **EG-UNIFIED-DATA-PLANE-R015.2.1:** Adapter skeleton — `SqliteAttachConfig`/`SqliteDriverOpenConfig` typed mapping plus the no-live-file refusal path (`crates/eg-types/src/sqlite_live_attach.rs`). Test: `crates/eg-types/src/sqlite_live_attach.rs::tests`.
    - [ ] **EG-UNIFIED-DATA-PLANE-R015.2.2:** Real lock-safe open, WAL-frame tailing, and watermark polling against a live, concurrently-written file.
- [ ] **EG-UNIFIED-DATA-PLANE-R016 (rollup):** MSSQL catalog/T-SQL/CDC adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
  - [x] **EG-UNIFIED-DATA-PLANE-R016.1:** `MssqlSourceCatalog` typed model plus bracket-quoting identifier refusal (`crates/eg-types/src/mssql_attached_catalog.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R016.2 (rollup):** tiberius driver connection, T-SQL TOP/OFFSET-FETCH rendering, and LSN-polled CDC/Change Tracking capture. LANDED only once every child is.
    - [x] **EG-UNIFIED-DATA-PLANE-R016.2.1:** Adapter skeleton — `MssqlConnectionConfig`/`MssqlDriverConfig` typed mapping plus the no-live-server refusal path (`crates/eg-types/src/mssql_driver_connection.rs`). Test: `crates/eg-types/src/mssql_driver_connection.rs::tests`.
    - [ ] **EG-UNIFIED-DATA-PLANE-R016.2.2:** Real tiberius connection, T-SQL TOP/OFFSET-FETCH rendering, and LSN-polled CDC/Change Tracking capture loop.
- [ ] **EG-UNIFIED-DATA-PLANE-R017 (rollup):** ClickHouse federation/acceleration adapter with explicit no-CDC capability; depends on EG-UNIFIED-DATA-PLANE-R002.
  - [x] **EG-UNIFIED-DATA-PLANE-R017.1:** `ClickHouseSourceCatalog` typed model with always-explicit capture capability (`crates/eg-types/src/clickhouse_attached_catalog.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R017.2 (rollup):** HTTP/native client connection and federated query pushdown.
    - [x] **EG-UNIFIED-DATA-PLANE-R017.2.1:** `ClickHouseDriverConfig` typed driver config plus connect/refusal path (`crates/eg-plan/src/clickhouse_connection.rs`).
    - [ ] **EG-UNIFIED-DATA-PLANE-R017.2.2:** Live HTTP/native client federated query pushdown.
- [x] **EG-UNIFIED-DATA-PLANE-R018:** Oracle and Db2 catalog/query through driver/ODBC and Debezium capture; depends on EG-UNIFIED-DATA-PLANE-R021.
- [x] **EG-UNIFIED-DATA-PLANE-R015:** SQLite lock-safe file attach, WITHOUT ROWID and safe WAL/watermark capture; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R016:** MSSQL catalog/T-SQL/CDC adapter; depends on EG-UNIFIED-DATA-PLANE-R002.
- [ ] **EG-UNIFIED-DATA-PLANE-R017:** ClickHouse federation/acceleration adapter with explicit no-CDC capability; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R018 (rollup):** Oracle and Db2 catalog/query through driver/ODBC and Debezium capture; depends on EG-UNIFIED-DATA-PLANE-R021.
  - [x] **EG-UNIFIED-DATA-PLANE-R018.1:** `RelationalSourceCatalog` typed model plus identifier and Debezium-capture refusals (`crates/eg-types/src/relational_attached_catalog.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R018.2 (rollup):** Driver/ODBC connection, catalog read, and Debezium bridge wiring.
    - [ ] **EG-UNIFIED-DATA-PLANE-R018.2.1:** `RelationalDriverConfig` typed driver config plus connect/refusal path (`crates/eg-plan/src/relational_connection.rs`).
    - [ ] **EG-UNIFIED-DATA-PLANE-R018.2.2:** Live driver/ODBC catalog read and Debezium bridge wiring.
- [x] **EG-UNIFIED-DATA-PLANE-R019:** MongoDB/DocumentDB document catalog and change streams; depends on EG-UNIFIED-DATA-PLANE-R002.
- [x] **EG-UNIFIED-DATA-PLANE-R019.1:** `DocumentSourceCatalog` typed inferred-shape model plus field-path refusal (`crates/eg-types/src/document_source_catalog.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R019.2 (rollup):** Native MongoDB/DocumentDB driver connection and change-stream capture into `ChangeEnvelope`.
  - [ ] **EG-UNIFIED-DATA-PLANE-R019.2.1:** `DocumentDriverConfig` typed driver config plus connect/refusal path (`crates/eg-plan/src/document_connection.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R019.2.2:** Live native driver connection and change-stream capture into `ChangeEnvelope`.
- [ ] **EG-UNIFIED-DATA-PLANE-R019.3:** Conformance entry comparing EG query results and captured change stream against native MongoDB/DocumentDB.
- [ ] **EG-UNIFIED-DATA-PLANE-R019.2:** Native MongoDB/DocumentDB driver connection and change-stream capture into `ChangeEnvelope`.
- [ ] **EG-UNIFIED-DATA-PLANE-R019.3 (rollup):** Conformance entry comparing EG query results and captured change stream against native MongoDB/DocumentDB.
  - [x] **EG-UNIFIED-DATA-PLANE-R019.3.1:** `build_document_conformance_entry` typed per-adapter conformance-entry constructor (`crates/eg-types/src/document_conformance.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R019.3.2:** Live comparison against native MongoDB/DocumentDB.
- [x] **EG-UNIFIED-DATA-PLANE-R020:** Snowflake, BigQuery, DuckDB and Iceberg federation; depends on EG-UNIFIED-DATA-PLANE-R007.
- [x] **EG-UNIFIED-DATA-PLANE-R020.1:** `WarehouseSourceKind`/`WarehouseSourceConfig` typed model plus required-field refusal (`crates/eg-types/src/warehouse_federation.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R020.2 (rollup):** Query pushdown per kind (Arrow Flight SQL where offered), extending `eg-query::sql::iceberg_federation` for Iceberg.
  - [x] **EG-UNIFIED-DATA-PLANE-R020.2.1:** `plan_pushdown` typed transport-dispatch plus refusal path (`crates/eg-plan/src/warehouse_pushdown.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R020.2.2:** Live Arrow Flight SQL / native query pushdown per kind.
- [ ] **EG-UNIFIED-DATA-PLANE-R020.3 (rollup):** Conformance entry comparing federated query results against each native warehouse/lake engine.
  - [x] **EG-UNIFIED-DATA-PLANE-R020.3.1:** `build_warehouse_conformance_entry` typed per-kind conformance-entry constructor (`crates/eg-types/src/warehouse_conformance.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R020.3.2:** Live comparison against each native warehouse/lake engine.
- [x] **EG-UNIFIED-DATA-PLANE-R021:** Debezium Kafka-to-`ChangeEnvelope` bridge; depends on EG-UNIFIED-DATA-PLANE-R008.
- [x] **EG-UNIFIED-DATA-PLANE-R021.1:** `DebeziumChangeEvent` typed envelope shape plus op-code and before/after refusal (`crates/eg-types/src/debezium_bridge.rs`).
- [x] **EG-UNIFIED-DATA-PLANE-R021.2:** Kafka consumer plus real conversion into `ChangeEnvelope` records. (pure-function conversion slice: `DebeziumChangeEvent::to_draft` in `crates/eg-types/src/debezium_bridge.rs`; the live Kafka consumer wiring remains open.)
- [x] **EG-UNIFIED-DATA-PLANE-R021.3:** Replay test feeding a captured Debezium event stream through the bridge. (`replay_of_captured_fixture_stream_matches_source_changes` over `crates/eg-types/fixtures/debezium_replay_stream.json`.)

## Platform and app admission

- [x] **EG-UNIFIED-DATA-PLANE-R023:** Build shared CloudNativePG and MariaDB platform with per-app roles, PITR and restore drill.
- [x] **EG-UNIFIED-DATA-PLANE-R023.1:** `SharedPlatformClusterGroup`/`ApplicationRoleSet` typed model plus engine-specific refusal (`crates/eg-types/src/shared_db_platform.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R023.2 (rollup):** CloudNativePG/MariaDB operator wiring and PITR schedule to object storage.
  - [x] **EG-UNIFIED-DATA-PLANE-R023.2.1:** `PitrSchedule` typed schedule contract plus engine/archiving-mode refusal (`crates/eg-types/src/pitr_schedule.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R023.2.2:** Live operator CRD wiring and schedule installation.
- [ ] **EG-UNIFIED-DATA-PLANE-R023.3 (rollup):** Restore-drill automation and post-restore data-match verification.
  - [x] **EG-UNIFIED-DATA-PLANE-R023.3.1:** `RestoreDrillResult` typed drill-result contract plus digest-agreement refusal (`crates/eg-types/src/restore_drill.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R023.3.2:** Live restore-drill automation and digest capture.
- [x] **EG-UNIFIED-DATA-PLANE-R024:** Admit three apps including MariaDB with rollback and retire each ingest connector only after parity; depends on EG-UNIFIED-DATA-PLANE-R023.
- [x] **EG-UNIFIED-DATA-PLANE-R024.1:** `ApplicationAdmission` typed stage machine plus early-retirement refusal (`crates/eg-types/src/platform_admission.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R024.2:** Admission runner and tested rollback path per candidate application.
  - [x] **EG-UNIFIED-DATA-PLANE-R024.2.1:** `AdmissionRunner`/`PriorIngestConnector` runner plus tested rollback path, against a fake connector (`crates/eg-types/src/platform_admission.rs`); no live candidate application needed.
- [ ] **EG-UNIFIED-DATA-PLANE-R024.3:** Per-application admission test confirming rollback and that the retired connector no longer writes.
  - [x] **EG-UNIFIED-DATA-PLANE-R024.3.1:** Fake-connector admission test run for each candidate application (gramps, immich, firefly) (`crates/eg-types/src/platform_admission.rs`); no live candidate application needed.
- [ ] **EG-UNIFIED-DATA-PLANE-R032 (rollup) / Immich I0–I6:** Generated API client, MCP tools, registration, incremental per-user ingest, naming, approved links and operations. Implement I0–I6 as defined in [spec.md](spec.md).
  - [x] **EG-UNIFIED-DATA-PLANE-R032.1:** `ImmichPilotProgress` typed phase-gate model (I0-I6) plus exit-artifact and skip-ahead refusals (`crates/eg-types/src/immich_pilot_phase.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R032.2:** Run phase I0 (version-pinned OpenAPI client generation and digest).

## Native hosting and app admission

Native storage and wire work (EG-DURABLE-KERNEL-R032/EG-DURABLE-KERNEL-R033/EG-DURABLE-KERNEL-R034–EG-DURABLE-KERNEL-R036/EG-DURABLE-KERNEL-R037–EG-DURABLE-KERNEL-R042) is implemented and evidenced by `durable-graph-kernel`. The one-way Ghostfolio import EG-FINANCE-PRIMITIVES-R012 is implemented and evidenced by `finance-primitives`. This spec consumes their exact-head test receipts before app admission, without duplicating those work items.

- [x] **EG-UNIFIED-DATA-PLANE-R025:** Separate pgrx companion spike and go/no-go ADR; depends on EG-UNIFIED-DATA-PLANE-R001.
- [x] **EG-UNIFIED-DATA-PLANE-R025.1:** `PgrxSpikeDecision` typed model plus full-scope-evidence refusal (`crates/eg-types/src/pgrx_spike.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R025.2:** Run the spike across its declared scope areas.
  - [x] **EG-UNIFIED-DATA-PLANE-R025.2.1:** `run_spike`/`PgrxSpikeAreaEvaluator` runner assembling a decision from fake evaluators (`crates/eg-types/src/pgrx_spike.rs`); no live pgrx/Postgres instance needed.
- [ ] **EG-UNIFIED-DATA-PLANE-R025.3:** Review the spike's results and recorded decision; produce the approved ADR.
- [ ] **EG-UNIFIED-DATA-PLANE-R025.3 (rollup):** Review the spike's results and recorded decision; produce the approved ADR. LANDED only once every child is.
  - [x] **EG-UNIFIED-DATA-PLANE-R025.3.1:** Review skeleton — `PgrxSpikeReviewRequest`/`ApprovedPgrxAdr` typed mapping plus the invalid-decision/empty-reviewer/empty-title refusal path (`crates/eg-types/src/pgrx_spike_review.rs`). Test: `crates/eg-types/src/pgrx_spike_review.rs::tests`.
  - [ ] **EG-UNIFIED-DATA-PLANE-R025.3.2:** Filing the approved ADR document against the live spike's actual results.
- [x] **EG-UNIFIED-DATA-PLANE-R026 / Gramps P0–P5:** Baseline, real-Postgres control, unmodified EG replay, pilot fixes, restores and OBDA understanding; depends on EG-DURABLE-KERNEL-R036. Implement P0–P5 and their exit artifacts as defined in [spec.md](spec.md) and [test-spec.md](test-spec.md).
- [x] **EG-UNIFIED-DATA-PLANE-R026.1:** `GrampsPilotProgress`/`PhaseExitArtifact` typed P0-P5 gate plus wrong/unreviewed/out-of-order refusal (`crates/eg-types/src/gramps_pilot_phase.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R026.2 (rollup):** Run P0-P2 (baseline digest, Postgres control, unmodified corpus replay). LANDED only once every child is.
  - [x] **EG-UNIFIED-DATA-PLANE-R026.2.1:** P0 adapter skeleton — `GrampsBaselineDigest` typed mapping to the P0 phase exit plus the zero-count/empty-digest/unreviewed refusal path (`crates/eg-types/src/gramps_baseline_digest.rs`). Test: `crates/eg-types/src/gramps_baseline_digest.rs::tests`.
  - [ ] **EG-UNIFIED-DATA-PLANE-R026.2.2:** Real baseline capture, Postgres control, and unmodified corpus replay (P1-P2) against a live export.
- [ ] **EG-UNIFIED-DATA-PLANE-R026.3 (rollup):** Run P3-P5 (proven-gap fixes, restore verification, schema-understanding demonstration). LANDED only once every child is.
  - [x] **EG-UNIFIED-DATA-PLANE-R026.3.1:** P3 adapter skeleton — `GrampsProvenGapFix` typed mapping to the P3 phase exit plus the empty-field/same-test/unreviewed refusal path (`crates/eg-types/src/gramps_gap_fix.rs`). Test: `crates/eg-types/src/gramps_gap_fix.rs::tests`.
  - [ ] **EG-UNIFIED-DATA-PLANE-R026.3.2:** Real restore verification (P4) and schema-understanding demonstration (P5) against the live pilot.
- [x] **EG-UNIFIED-DATA-PLANE-R027:** Fix Postgres features only when captured app traffic proves need; include B-tree indexes, collation, regex, bytea, sequences, savepoints, locks, notify, triggers/functions, JSONB, isolation, startup, SQLSTATE, schemas, constraints, types, materialized views, large objects and timezone; depends on EG-UNIFIED-DATA-PLANE-R026.
- [x] **EG-UNIFIED-DATA-PLANE-R027.1:** `FeatureFixRecord`/`PgCompatFeature` typed inventory plus incomplete-proof refusal (`crates/eg-types/src/pg_compat_feature.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R027.2:** Capture application traffic per feature and record the proof reference.
  - [x] **EG-UNIFIED-DATA-PLANE-R027.2.1:** `capture_feature_fix`/`TrafficCaptureSource` runner plus unproven/incomplete-capture refusal (`crates/eg-types/src/pg_compat_feature.rs`); no live candidate application needed.
- [ ] **EG-UNIFIED-DATA-PLANE-R027.3:** Implement each proven feature with its failing-then-passing test.
  - [x] **EG-UNIFIED-DATA-PLANE-R027.3.1:** `apply_proven_fix` runner applying a feature's fix only once proven, against a fake fix closure (`crates/eg-types/src/pg_compat_feature.rs`); no live Postgres-compatible target needed.
- [x] **EG-UNIFIED-DATA-PLANE-R028:** Differential Postgres regression, generated and captured-traffic corpora with reviewed deviations and no ratchet; depends on EG-UNIFIED-DATA-PLANE-R022.
- [x] **EG-UNIFIED-DATA-PLANE-R028.1:** `DeviationBaseline`/`PostgresDeviation` typed model plus no-silent-drop refusal (`crates/eg-types/src/pg_diff_baseline.rs`).
- [ ] **EG-UNIFIED-DATA-PLANE-R028.2:** Run the Postgres regression suite and SQLancer-style generated corpora against EG.
  - [x] **EG-UNIFIED-DATA-PLANE-R028.2.1:** `run_differential_harness`/`DifferentialCaseEvaluator` runner assembling a baseline from fake evaluators (`crates/eg-types/src/pg_diff_baseline.rs`); no live Postgres instance needed.
- [ ] **EG-UNIFIED-DATA-PLANE-R028.3:** Run the captured-application-traffic corpus and publish the harness report.
- [x] **EG-UNIFIED-DATA-PLANE-R033 (rollup) / Gramps P6:** Separate explicitly authorized production cutover, replica, independent nightly export and 30-day read-only SQLite fallback. Depends on EG-UNIFIED-DATA-PLANE-R026 and every P0–P5 acceptance artifact.
  - [x] **EG-UNIFIED-DATA-PLANE-R033.1:** `GrampsCutoverGoAhead` typed model plus P5-exit/approver/fallback-window refusals (`crates/eg-types/src/gramps_cutover_gate.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R033.2:** Real production cutover: Postgres replica, nightly independent XML export, and the rehearsed failover test.

## Virtual graphs

- [x] **EG-UNIFIED-DATA-PLANE-R037:** Ship `core:virtual-graph@1` with SHACL shapes and positive and negative fixtures.
- [ ] **EG-UNIFIED-DATA-PLANE-R038 (rollup):** Write a hashed `MetadataContract` for every source kind. Add the acceleration policy. Prove live reads by default and copy only for declared hot subsets.
  - [x] **EG-UNIFIED-DATA-PLANE-R038.1:** `MetadataContract`/`AccelerationPolicy` typed model plus capability refusal and watermark-missing live fallback (`crates/eg-types/src/metadata_acceleration.rs`).
  - [ ] **EG-UNIFIED-DATA-PLANE-R038.2:** Real per-source-kind discovery writing the contract, the copy pipeline, and EXPLAIN's per-fragment live/copy reporting.

## Completion record for each row

For each row, add a subentry containing its merged commit, exact-head test run, conformance/quality reports, operational drill where relevant, cross-repository consumer and old-path deletion. Record one of `PROPOSED`, `IMPLEMENTED-UNVALIDATED`, `ACCEPTED`, `CUTOVER`, `DELETED` or `DONE` with dated evidence. Update [status.json](status.json) only from proved evidence. `ACCEPTED` requires all applicable gates in [test-spec.md](test-spec.md); `CUTOVER` and `DONE` additionally require rollout, rollback and consumer evidence. Preserve failed runs and corrections rather than rewriting history. Rows above remain unchecked until that evidence exists.
- [ ] **EG-UNIFIED-DATA-PLANE-R039 (rollup):** Authenticated, tenant-scoped Iceberg REST catalog (rollup).
  - [ ] **EG-UNIFIED-DATA-PLANE-R039.1:** `CatalogPrincipal` typed model.
  - [ ] **EG-UNIFIED-DATA-PLANE-R039.2:** Catalog authorization refusal.
  - [ ] **EG-UNIFIED-DATA-PLANE-R039.3:** Locate and wire the REST listener.
  - [ ] **EG-UNIFIED-DATA-PLANE-R039.4:** Partition spec in emitted Iceberg metadata.
  - [ ] **EG-UNIFIED-DATA-PLANE-R039.5:** End-to-end external read.
- [ ] **EG-UNIFIED-DATA-PLANE-R040:** Per-modality retrieval gate blocks promotion of a shared embedding space.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.1:** Model. Add `ModalityCheck{modality, probe_set_digest, metric, floor, baseline}` and `PromotionVerdict` in new `crates/eg-types/src/embedding_promotion.rs`. Closed `modality` values are `text`, `image` and `audio`. `EmbeddingVersion` at `crates/eg-ann/src/version.rs:32` names a model and version but holds no check result.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.2:** Behavior. Add `evaluate_promotion(candidate, checks)` in `embedding_promotion.rs`. It computes recall at k per modality with `recall_at_k` at `crates/eg-ann/src/recall.rs:28` on that modality probe set. A drop beyond the tolerance blocks promotion. The default tolerance is 0.02 absolute and is a named replaceable default. A modality with no probe set blocks promotion.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.3:** Wiring. The gate attaches before `EmbeddingVersionStore::tag_batch` at `crates/eg-ann/src/version.rs:129` in the re-embed path. No production caller of `tag_batch` or `tag` exists today. This row creates the call site in the re-embed worker queued by `EG-FEDERATED-QUERY-R077.3`, and it reads the binding at `crates/eg-query/src/tables/embedding_binding.rs:526`.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.4:** Refusal. A blocked promotion must leave the prior version active. The engine must return `PromotionBlocked` naming each regressed modality, its baseline and its measured value. The engine must not tag any vector with the candidate version.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.5:** Evidence. The engine must store each `PromotionVerdict` as a node linked to the candidate model and version. It carries the per-modality metric, floor and probe set digest. The node is readable through a graph query.
- [ ] **EG-UNIFIED-DATA-PLANE-R040.6:** Hot-path proof. The entry point is a served row write on a bound text column. The test replaces the embedding model with a candidate that regresses the image check. The write passes through the re-embed worker and the wired gate. The test observes the blocked promotion and the prior version still active.

<!-- spec-definition-patch:owner-batch-20261012-unified-data-plane -->
## Implementation tasks — owner batch 2026-10-12

- [ ] **EG-UNIFIED-DATA-PLANE-R026.4 — projection boundary:** Route approved, revision-pinned R003/R005 mappings through the R026 projection read.
  - Refuse projected DML/DDL for read-only and mutation-granted principals. Admit writes only through authorized graph `MutationBatch`.
  - Verify that projection reads reflect committed graph mutations.
  - **Hold:** The R026 owner must furnish the target application schema and captured statement corpus before app compatibility is claimed.
  - Keep status SPECIFIED. Source SHA-256: `c55bc629b1bfefc9d0346b75b8bb95e5a231b891e7e34297df17b960d6c9773c`.
<!-- /spec-definition-patch -->

- [ ] **EG-UNIFIED-DATA-PLANE-R038.3 (rollup):** Transactional row stores answer through one consistent snapshot (rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.1 (Model):** Add the typed model.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.2 (Behavior):** Implement the behavior.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.3 (Wiring):** Wire the production call site.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.5 (Evidence):** Emit the evidence.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.3.6 (Hot-path proof):** Add the served hot-path test.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4 (rollup):** Metastore catalogs and ORC and Avro files answer in place (rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.1 (Model):** Add the typed model.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.2 (Behavior):** Implement the behavior.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.3 (Wiring):** Wire the production call site.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.5 (Evidence):** Emit the evidence.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.4.6 (Hot-path proof):** Add the served hot-path test.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5 (rollup):** Search-index sources answer with relevance pushdown (rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.1 (Model):** Add the typed model.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.2 (Behavior):** Implement the behavior.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.3 (Wiring):** Wire the production call site.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.5 (Evidence):** Emit the evidence.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.5.6 (Hot-path proof):** Add the served hot-path test.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6 (rollup):** openCypher and Gremlin answer over relational and lake sources without a copy (rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.1 (Model):** Add the typed model.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.2 (Behavior):** Implement the behavior.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.3 (Wiring):** Wire the production call site.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.5 (Evidence):** Emit the evidence.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.6.6 (Hot-path proof):** Add the served hot-path test.
- [ ] **EG-UNIFIED-DATA-PLANE-R022.4:** Source-category coverage matrix.

- [ ] **EG-UNIFIED-DATA-PLANE-R038.7 (rollup):** Web metasearch sources answer at query time through a cached connector query (rollup).
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.1 (Model):** Add the typed model.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.2 (Behavior):** Implement the behavior.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.3 (Wiring):** Wire the production call site.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.5 (Evidence):** Emit the evidence.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.6 (Hot-path proof):** Add the served hot-path test.

## Web page ingest (R022.4.1, R038.7.7, R651.7)

- [ ] **EG-UNIFIED-DATA-PLANE-R022.4.1:** Add the residency column to the matrix and its test.
- [ ] **EG-UNIFIED-DATA-PLANE-R038.7.7:** Add the candidate set with query provenance.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7 (rollup):** Fetched web pages enrich through the one document path.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.1 (Model):** Add the web document provenance and the `WebPage` job kind.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.2 (Behavior):** Run the shared phases and the digest filter.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.3 (Wiring):** Route the served `SourceIngest` web page to the phase registry.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.4 (Refusal):** Add the typed refusals.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.5 (Evidence):** Emit the ingest receipt counts.
- [ ] **EG-UNIFIED-DATA-PLANE-R651.7.6 (Hot-path proof):** Add the served two-backend test and the benchmark.
