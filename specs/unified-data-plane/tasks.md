# Delivery tasks and traceability

**State:** PROPOSED. All entries are unchecked and retain the ledger's `QUEUED` state as of this draft. This is a scope and dependency map, not implementation evidence. Change a checkbox only with a linked commit and test evidence; change acceptance only in the ledger (`plans/refactor/LEDGER.md`) with exact merged-head proof. [Spec](spec.md) · [design](plan.md) · [tests](test-spec.md).

## Foundation and understanding

- [ ] **EH-660 / DB-7.0:** Decide attached-source default, consistency, native admission and pgrx boundary by ADR.
- [ ] **EH-661 / DB-7.1:** Merge source registration, owner scope, secret references, verified outbound checks and five-part adapter contract; depends on EH-373/378/508/660.
- [ ] **EH-662 / DB-7.2, DB-5.1:** Build versioned typed catalog extraction with OBDA and `schema_context` consumers; depends on EH-661.
- [ ] **EH-663 / DB-5.2–5.4:** Add bounded profiling, JSON shape, dependency inference and fixed-seed Leiden grouping; depends on EH-662.
- [ ] **EH-664 / DB-5.5–5.6, DB-7.3:** Approve and version R2RML named virtual graphs; depends on EH-663 and EH-033.
- [ ] **EH-693 / DB-5.7:** Expose `schema_context` through Graph OS; depends on EH-663/664.
- [ ] **EH-694 / DB-5.8:** Add cross-app entity proposals, approval and evidence; depends on EH-664/696.
- [ ] **EH-695 / DB-6.1–6.4:** Define isolation, quotas, compatibility, native admission and rollback; depends on EH-661.

## Query, change and write paths

- [ ] **EH-665 / DB-7.4:** Typed OBDA join/aggregate/order/limit pushdown and explicit fallback; depends on EH-664.
- [ ] **EH-666 / DB-7.5:** Attached table DataFusion providers and dialect-aware sub-plan pushdown; depends on EH-661.
- [ ] **EH-667 / DB-7.6, DB-4.4, DB-5.9, DB-3.5:** Durable source-position CDC, replay, lag and drift; depends on EH-661/139.
- [ ] **EH-668 / DB-7.7:** Idempotent mapped graph, search/vector and event consumers; depends on EH-664/667.
- [ ] **EH-669 / DB-7.8:** Policy-controlled `eg-lake` accelerated copy; depends on EH-667.
- [ ] **EH-670 / DB-7.9:** Freshness-aware native/live/accelerated router and EXPLAIN; depends on EH-666/669.
- [ ] **EH-671 / DB-7.10:** Governed idempotent write-back through SDK/app API and EH-658 audit; depends on EH-661/658/216.
- [ ] **EH-692 / DB-10.1, DB-4.0–4.1:** Extend EH-508 mirrors with cursor, replay, reconcile and SQL-table Postgres restore; depends on EH-508/661.

## Dialects and conformance

- [ ] **EH-681 / DB-8.10, DB-2.1:** Build native-source differential and CDC replay conformance harness; depends on EH-661.
- [ ] **EH-672 / DB-8.1:** Postgres catalog/query/pgoutput/type adapter; depends on EH-661/681.
- [ ] **EH-673 / DB-8.2:** Separate MySQL and MariaDB query/catalog/binlog/type entries; depends on EH-661/681.
- [ ] **EH-674 / DB-8.3:** SQLite lock-safe file attach, WITHOUT ROWID and safe WAL/watermark capture; depends on EH-661/681.
- [ ] **EH-675 / DB-8.4:** MSSQL catalog/T-SQL/CDC adapter; depends on EH-661/681.
- [ ] **EH-676 / DB-8.5:** ClickHouse federation/acceleration adapter with explicit no-CDC capability; depends on EH-661/681.
- [ ] **EH-677 / DB-8.6:** Oracle and Db2 catalog/query through driver/ODBC and Debezium capture; depends on EH-680.
- [ ] **EH-678 / DB-8.7:** MongoDB/DocumentDB document catalog and change streams; depends on EH-661/663.
- [ ] **EH-679 / DB-8.8:** Snowflake, BigQuery, DuckDB and Iceberg federation; depends on EH-666.
- [ ] **EH-680 / DB-8.9:** Debezium Kafka-to-`ChangeEnvelope` bridge; depends on EH-667.

## Platform and app admission

- [ ] **EH-682 / DB-7.11, DB-4.5, DB-4.7:** Build shared CloudNativePG and MariaDB platform with per-app roles, PITR and restore drill.
- [ ] **EH-683 / DB-7.12, DB-6.5:** Admit three apps including MariaDB with rollback and retire each ingest connector only after parity; depends on EH-682/672/673/668.
- [ ] **EH-696 / Immich I0–I6:** Generated API client, MCP tools, registration, incremental per-user ingest, naming, approved links and operations. Use pilot phases (`plans/database/PILOT-IMMICH.md`).
- [ ] **EH-714 / Ghostfolio:** Attach shared Postgres, map activities to finance-v1 and prove one-way import; depends on EH-672/682/698.

## Native hosting and bursty engine

- [ ] **EH-684 / DB-9.0–9.2:** After EH-290, add point fast path and hot/cold union with compaction.
- [ ] **EH-685 / DB-9.3:** Benchmark redb/fjall/RocksDB on captured workload and decide by ADR; depends on EH-684.
- [ ] **EH-686 / DB-9.4:** Separate pgrx companion spike and go/no-go ADR; depends on EH-660.
- [ ] **EH-687 / Gramps P0–P5:** Baseline, real-Postgres control, unmodified EG replay, pilot fixes, restores and OBDA understanding; depends on EH-692/689. Use pilot phases (`plans/database/PILOT-GRAMPS.md`).
- [ ] **EH-688 / DB-1.1–1.21:** Fix Postgres features only when captured app traffic proves need; depends on EH-687.
- [ ] **EH-689 / DB-2.1–2.4:** Differential corpora and reviewed deviation list; depends on EH-681.
- [ ] **EH-690 / DB-3.1–3.4:** Cross-store atomicity ADR, crash and concurrency proof; depends on EH-687.
- [ ] **EH-691 / DB-4.2–4.3, DB-4.6:** pg_dump/restore, replication and operational tooling; depends on EH-687.
- [ ] **EH-697 / Gramps P6:** Separate operator-authorized production cutover, replica, nightly XML and 30-day SQLite fallback; depends on EH-687/690/691.
- [ ] **EH-717 / DB-9.5:** Classify every wire before planner; depends on EH-684.
- [ ] **EH-718 / DB-9.6:** Visible sync/async/ephemeral durability and measured loss; depends on EH-290.
- [ ] **EH-719 / DB-9.7:** Recoverable hot data structures and atomic primitives; depends on EH-718.
- [ ] **EH-720 / DB-9.8:** One-admission, one-commit RESP/pgwire batches; depends on EH-717/718.
- [ ] **EH-721 / DB-9.9:** Schema-versioned SQL plan cache bound to DepClock; depends on EH-717.
- [ ] **EH-722 / DB-9.10:** Publish R820 benchmark matrix and gate every comparative claim; depends on EH-719/720/721.

## Completion record for each row

Fill in the row's merged commit, exact-head test run, conformance/quality reports, operational drill where relevant, cross-repository consumer, old-path deletion and ledger status change. `ACCEPTED` requires all applicable gates in [test-spec.md](test-spec.md); `CUTOVER` and `DONE` additionally require the rollout and ledger reconciliation evidence. Preserve failed runs and corrections rather than rewriting history.
