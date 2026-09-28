# Test and acceptance specification

**State:** PROPOSED. Evidence must cite the exact merged commit, fixture/source version, command, environment and result. A passing isolated branch is implementation evidence, not acceptance. See [requirements](spec.md), [design](plan.md) and [tasks](tasks.md).

| Gate | Positive proof | Negative and failure proof | Ledger |
|---|---|---|---|
| Registry and identity | One owner-scoped source drives SQL, named OBDA, CDC and mirror registration; secret references resolve only at service boundary. | Wrong tenant, expired/rotated secret, rebinding attempt, private/outbound address blocked, missing grant and stale source version fail before mutation or network connection. | EH-660, EH-661, EH-692, EH-695 |
| Catalog and mapping | Repeat snapshots produce stable hashes; keys, views, enums and nested JSON shapes match native catalog; approved R2RML answers known SPARQL/UQL/REASON questions. | Bounded sample budget exceeded, schema drift, inferred false FK, model-only proposal, unapproved mapping, deleted column and cross-app exposure do not leak or auto-approve. | EH-662–EH-664, EH-693 |
| SQL and OBDA | Compare pushed-down and non-pushed results with native source for nulls, collation, time zones, numeric/array/JSON types, pagination, joins and aggregates; EXPLAIN identifies route and fallbacks. | Unsupported dialect expression falls back visibly; tenant predicate cannot be dropped; malformed mapping/hostile SQL cannot escape allowed table/owner scope. | EH-665, EH-666, EH-670, EH-681 |
| CDC and acceleration | Initial snapshot plus replay equals current source after restart and source failover; graph/search/vector/CEP consumers and lake copy converge once by source position. | Duplicate/out-of-order event, dropped key, keyless update, DDL mid-stream, WAL/binlog retention pressure, unavailable sink and stale copy leave a diagnosable lag or typed error; read-your-writes never returns stale success. | EH-667–EH-670, EH-680 |
| Write-back and mirrors | Approved idempotent API/direct write reserves EH-658 audit; consume the kernel mirror cursor/reconcile receipt and restore SQL tables into real Postgres. | Missing approval/audit, duplicate key, ambiguous external response, empty-default mirror, down target and retry do not create duplicate or unlogged effects. Mirror is never selected as a read authority. | EH-671; kernel dependency EH-692 |
| Dialect matrix | For each DB-8.1–8.9 engine/version, catalog, type map, supported pushdown and CDC or explicit no-CDC capability pass; source query equals EG result. | Disconnected source, schema/version change, incompatible type, unsupported capture and native-source discrepancy fail its row rather than being counted green. | EH-672–EH-681 |
| Platform and app admissions | PITR and scheduled restore drill; per-app roles; three attached apps including MariaDB; connector ingest retired only after parity; consume the finance-owned Ghostfolio import receipt. | One app migration failure rolls back without affecting others; revoked role, stale replica, extension mismatch and duplicate Ghostfolio import are caught. | EH-682, EH-683; finance dependency EH-714 |
| Native Gramps admission | P0 XML digest; P1 real-Postgres control and statement corpus; P2 unmodified EG failure capture; P3 differential replay and export digest; P4 three restore paths and kill-9; P5 approved OBDA answer and `schema_context`. | Every suspected feature has a captured failing/passing test before implementation; SQLSTATE, app migrations, crash anomalies, dump/restore mismatch and cross-store partial commit are explicit failures. P6 requires separate operator approval, 30-day stability and failover evidence. | EH-686–EH-689, EH-697; kernel dependencies EH-684/EH-685/EH-690/EH-691 |
| Immich and entity resolution | API fixture/live smoke, full four-user ingest followed by no-op replay and one incremental upload; approved links answer cross-app relationship question. | Unnamed faces remain unlinked, low-score matches remain proposals, cross-user media/GPS access denied, API schema digest drift and missing connector registration fail release. | EH-694, EH-696 |
| Kernel dependency: bursty storage | Consume the `durable-graph-kernel` exact-head native point/KV, durability, batch, recovery and DepClock proof before admitting a dependent app. | Missing authz/HMAC, stale cached plan or partial batch ack blocks app admission; this row does not implement a second kernel. | Kernel-owned EH-684/EH-717–EH-721 |
| Kernel dependency: benchmarks | Consume the same-hardware raw benchmark receipt from `durable-graph-kernel`. | A missing target, changed class, unreported gap or unrepeatable run blocks a comparative claim. | Kernel-owned EH-722 |

## Reproducible contributor environment

Use a current Rust toolchain from `rust-toolchain.toml` if present, Python from `pyproject.toml`, Docker with Compose, and the repository lockfiles. Run commands from the EG repository root. Build the same feature set named by each implementation PR; record `git rev-parse HEAD`, `rustc --version`, `python3 --version`, `docker compose version`, container image digests and test command. Sample data must be synthetic, deterministic and safe to publish. Never use a real family tree, photo library, account or credential as a test fixture.

For a base SQL adapter fixture, save the following as a temporary `compose.yaml` outside the repository and run `docker compose -p eg-data-plane -f compose.yaml up -d --wait`. Use isolated test credentials and teardown with `docker compose -p eg-data-plane -f compose.yaml down -v`. CI must pin image digests and versions in its committed matrix before it can claim a green dialect row; the versions here are a reproducible local starting point.

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: egtest
      POSTGRES_PASSWORD: egtest-local-only
      POSTGRES_DB: egtest
    ports: ["127.0.0.1:15432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U egtest -d egtest"]
      interval: 2s
      retries: 30
  mariadb:
    image: mariadb:11.4
    environment:
      MARIADB_ROOT_PASSWORD: egtest-local-only
      MARIADB_USER: egtest
      MARIADB_PASSWORD: egtest-local-only
      MARIADB_DATABASE: egtest
    ports: ["127.0.0.1:13306:3306"]
    healthcheck:
      test: ["CMD-SHELL", "mariadb-admin ping -h localhost -u root -pegtest-local-only"]
      interval: 2s
      retries: 30
```

Create a SQLite fixture with Python's `sqlite3` module using a normal table, a `WITHOUT ROWID` table, JSON text, nulls, Unicode and a foreign key. For each adapter add a committed schema/data fixture covering primary and composite keys, no-key tables, generated IDs, nullable columns, decimal/timezone/array or document types where supported, DDL change and a known capture sequence. The conformance harness must itself create/reset these fixtures. A test must not depend on a developer's running application.

## Required acceptance artifacts

Each dialect entry needs a machine-readable capability declaration, image/driver version, source schema fixture, EG result, native result, canonical comparison rule, pushed-down SQL/EXPLAIN and capture replay result or explicit no-capture reason. Compare column type, value, SQLSTATE and row order when ordered; normalize only documented nondeterminism. Cover null ordering, numeric precision/overflow, integer division, implicit casts, LIKE escaping, timestamps/time zones, JSON path, `DISTINCT ON`, functional-dependence grouping and boolean text output. Generate separate reports for Postgres regression queries, SQLancer-style TLP/NoREC cases and captured synthetic app traffic. A known difference is a reviewed named deviation with owner and failing fixture, never a self-updating baseline.

For native hosting, the synthetic Gramps-like fixture must include person/family/event relations, JSON object payloads, an inferred `reference` relation, indexes and a sample import/export digest. P0–P5 artifacts are: baseline digest/counts; real-Postgres control digest and driver log; EG failure corpus with SQLSTATE; post-fix differential report; independent replica/dump/EG-backup restore digests plus kill-at-commit report; approved R2RML, live SPARQL answer and `schema_context` result. P6 adds the explicit decision, rehearsed failover, independent export and 30-day observation. If the app's real traffic is used privately to find a gap, publish a minimized synthetic reproducer before accepting the fix.

For Immich, commit a synthetic OpenAPI fixture digest and generator output test, read/write MCP fixtures, per-user ownership fixtures, a no-op replay and one-change incremental fixture. A connector registration test must enumerate every required release catalog and reject a missing site. Entity-resolution fixtures must include true match, same-name false match, unnamed face, missing birth date, cross-user image and unapproved candidate; no `owl:sameAs` becomes active before approval.

For CDC, fail at each snapshot-to-stream boundary, before and after cursor/effect commit, and during source failover. Check equal final snapshot, once-only derived effect, visible lag, bounded retention and mapping suspension on DDL drift. For mirrors, stop a sink, advance source, restart, reconcile a deliberately corrupted target and restore SQL tables to real Postgres. For native transactions, inject process termination at each table/graph commit boundary and compare post-restart state. For bursty durability, repeat abrupt termination under all three classes, measure actual async acknowledged-loss interval against ≤100 ms, and show that sync never acknowledges a lost mutation. Report p99, throughput, memory and restart recovery for each class rather than mixing them.

The benchmark report must name host CPU/RAM/storage, kernel, compiler, container digests, dataset size, load/concurrency, warm-up, run length, durability class, raw outputs and confidence interval. The target axes are: YCSB A–F beats Postgres in async class; RESP p99 within 2× Redis in ephemeral class; pgbench and HammerDB TPC-C report their gap; LDBC SNB Interactive reports graph performance; and a converged graph+vector+SQL+time-series query beats the same query assembled from Postgres with relevant extensions. A claimed win must use the same hardware and comparable durability and disclose failures. No claim is allowed for an unrun row.

## Program gates

At the exact proposed merge head run scoped Rust/Python tests, per-dialect container conformance, `pre-commit run --config .config/pre-commit.yaml --all-files`, `bash scripts/ci_parity.sh`, `python3 scripts/check_status_page.py` and the hosted CI matrix. Capture the CCCC, KISS, jscpd and Dupehound reports from their real configured gates. Record route traces, replay positions, before/after dependency and LOC/symbol counts, old-path deletion, and a restore drill. If a gate cannot run, keep the item `IMPLEMENTED-UNVALIDATED` with the limitation. A green documentation check cannot turn an unimplemented row into `ACCEPTED`.
