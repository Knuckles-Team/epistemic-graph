# Test and acceptance specification

**State:** PROPOSED. Evidence must cite the exact merged commit, fixture/source version, command, environment and result. A passing isolated branch is implementation evidence, not acceptance. See [requirements](spec.md), [design](plan.md) and [tasks](tasks.md).

| Gate | Positive proof | Negative and failure proof | Requirement IDs |
|---|---|---|---|
| Registry and identity | One owner-scoped source drives SQL, named OBDA, CDC and mirror registration; secret references resolve only at service boundary. | Wrong tenant, expired/rotated secret, rebinding attempt, private/outbound address blocked, missing grant and stale source version fail before mutation or network connection. | EG-UNIFIED-DATA-PLANE-R001, EG-UNIFIED-DATA-PLANE-R002, EG-DURABLE-KERNEL-R036, EG-UNIFIED-DATA-PLANE-R031 |
| Catalog and mapping | Repeat snapshots produce stable hashes; keys, views, enums and nested JSON shapes match native catalog; approved R2RML answers known SPARQL/UQL/REASON questions. | Bounded sample budget exceeded, schema drift, inferred false FK, model-only proposal, unapproved mapping, deleted column and cross-app exposure do not leak or auto-approve. | EG-UNIFIED-DATA-PLANE-R003–EG-UNIFIED-DATA-PLANE-R005, EG-UNIFIED-DATA-PLANE-R029 |
| SQL and OBDA | Compare pushed-down and non-pushed results with native source for nulls, collation, time zones, numeric/array/JSON types, pagination, joins and aggregates; EXPLAIN identifies route and fallbacks. | Unsupported dialect expression falls back visibly; tenant predicate cannot be dropped; malformed mapping/hostile SQL cannot escape allowed table/owner scope. | EG-UNIFIED-DATA-PLANE-R006, EG-UNIFIED-DATA-PLANE-R007, EG-UNIFIED-DATA-PLANE-R011, EG-UNIFIED-DATA-PLANE-R022 |
| CDC and acceleration | Initial snapshot plus replay equals current source after restart and source failover; graph/search/vector/CEP consumers and lake copy converge once by source position. | Duplicate/out-of-order event, dropped key, keyless update, DDL mid-stream, WAL/binlog retention pressure, unavailable sink and stale copy leave a diagnosable lag or typed error; read-your-writes never returns stale success. | EG-UNIFIED-DATA-PLANE-R008–EG-UNIFIED-DATA-PLANE-R011, EG-UNIFIED-DATA-PLANE-R021 |
| Write-back and mirrors | Approved idempotent API/direct write reserves EG-DURABLE-KERNEL-R031 audit; consume the kernel mirror cursor/reconcile receipt and restore SQL tables into real Postgres. | Missing approval/audit, duplicate key, ambiguous external response, empty-default mirror, down target and retry do not create duplicate or unlogged effects. Mirror is never selected as a read authority. | EG-UNIFIED-DATA-PLANE-R012; kernel dependency EG-DURABLE-KERNEL-R036 |
| Dialect matrix | For each adapter's covered engine/version, catalog, type map, supported pushdown and CDC or explicit no-CDC capability pass. Source query equals EG result. | Disconnected source, schema/version change, incompatible type, unsupported capture and native-source discrepancy fail its row rather than being counted green. | EG-UNIFIED-DATA-PLANE-R013–EG-UNIFIED-DATA-PLANE-R022 |
| Platform and app admissions | PITR and scheduled restore drill. Per-app roles. Three attached apps including MariaDB. Connector ingest retired only after parity. Consume the finance-owned Ghostfolio import receipt. | One app migration failure rolls back without affecting others. Revoked role, stale replica, extension mismatch and duplicate Ghostfolio import are caught. | EG-UNIFIED-DATA-PLANE-R023, EG-UNIFIED-DATA-PLANE-R024. Finance dependency EG-FINANCE-PRIMITIVES-R012 |
| Native Gramps admission | P0 XML digest. P1 real-Postgres control and statement corpus. P2 unmodified EG failure capture. P3 differential replay and export digest. P4 three restore paths and kill-9. P5 approved OBDA answer and `schema_context`. | Every suspected feature has a captured failing/passing test before implementation. SQLSTATE, app migrations, crash anomalies, dump/restore mismatch and cross-store partial commit are explicit failures. P6 requires separate operator approval, 30-day stability and failover evidence. | EG-UNIFIED-DATA-PLANE-R025–EG-UNIFIED-DATA-PLANE-R028, EG-UNIFIED-DATA-PLANE-R033. Kernel dependencies EG-DURABLE-KERNEL-R032/EG-DURABLE-KERNEL-R033/EG-DURABLE-KERNEL-R034/EG-DURABLE-KERNEL-R035 |
| Immich and entity resolution | API fixture/live smoke, full four-user ingest followed by no-op replay and one incremental upload; approved links answer cross-app relationship question. | Unnamed faces remain unlinked, low-score matches remain proposals, cross-user media/GPS access denied, API schema digest drift and missing connector registration fail release. | EG-UNIFIED-DATA-PLANE-R030, EG-UNIFIED-DATA-PLANE-R032 |
| Kernel dependency: bursty storage | Consume the `durable-graph-kernel` exact-head native point/KV, durability, batch, recovery and DepClock proof before admitting a dependent app. | Missing authz/HMAC, stale cached plan or partial batch ack blocks app admission; this row does not implement a second kernel. | Kernel-owned EG-DURABLE-KERNEL-R032/EG-DURABLE-KERNEL-R037–EG-DURABLE-KERNEL-R041 |
| Kernel dependency: benchmarks | Consume the same-hardware raw benchmark receipt from `durable-graph-kernel`. | A missing target, changed class, unreported gap or unrepeatable run blocks a comparative claim. | Kernel-owned EG-DURABLE-KERNEL-R042 |
| Virtual-graph metadata | A registered API, MCP and SQL source each yield one hashed `MetadataContract`. An approved mapping answers a live read. A hot mapping copies only its declared subset. | Credential-bearing reference, undiscovered field, unapproved mapping, missing hot-subset predicate and stale watermark each refuse or fall back to live reads. | EG-UNIFIED-DATA-PLANE-R037, EG-UNIFIED-DATA-PLANE-R038 |

## Reproducible contributor environment

Use a current Rust toolchain from `rust-toolchain.toml` if present, Python from `pyproject.toml`, Docker with Compose, and the repository lockfiles. Run commands from the EG repository root. Build the same feature set named by each implementation PR; record `git rev-parse HEAD`, `rustc --version`, `python3 --version`, `docker compose version`, container image digests and test command. Sample data must be synthetic, deterministic and safe to publish. Never use a real family tree, photo library, account or credential as a test fixture.

For a base SQL adapter fixture, save the following as a temporary `compose.yaml` outside the repository and run `docker compose -p eg-data-plane -f compose.yaml up -d --wait`. Use isolated test credentials and teardown with `docker compose -p eg-data-plane -f compose.yaml down -v`. CI must pin image digests and versions in its committed matrix before it can claim a green dialect row. The versions here are a reproducible local starting point.

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

Authenticated Iceberg catalog (R039): a valid token lists and loads its own tables. Missing and cross-tenant tokens are refused on every route. A partitioned fixture prunes one manifest by a partition predicate, and a pinned external reader loads one table.

### EG-UNIFIED-DATA-PLANE-R040 tests

- `EG-UNIFIED-DATA-PLANE-R040.1`: `crates/eg-types/src/embedding_promotion.rs::tests::checks_round_trip`.
- `EG-UNIFIED-DATA-PLANE-R040.2`: `crates/eg-types/src/embedding_promotion.rs::tests::one_regression_blocks_promotion`.
- `EG-UNIFIED-DATA-PLANE-R040.3`: `crates/eg-ann/src/version.rs::tests::tag_requires_passing_verdict`.
- `EG-UNIFIED-DATA-PLANE-R040.4`: `crates/eg-ann/src/version.rs::tests::blocked_promotion_tags_nothing`.
- `EG-UNIFIED-DATA-PLANE-R040.5`: `crates/eg-types/src/embedding_promotion.rs::tests::verdict_is_stored_with_modality_entries`.
- `EG-UNIFIED-DATA-PLANE-R040.6`: `tests/embedding/test_promotion_gate_served.py::test_regressed_modality_blocks_promotion`.

<!-- spec-definition-patch:owner-batch-20261012-unified-data-plane -->
## Planned owner acceptance — 2026-10-12

These bullets define future acceptance targets. They are not test evidence or receipts. Exercise each row's actual producer/consumer boundary and retain existing covered behavior.

- **`EG-UNIFIED-DATA-PLANE-R026.4`**: EG:64; source SHA-256 `c55bc629b1bfefc9d0346b75b8bb95e5a231b891e7e34297df17b960d6c9773c`.
  - Read a mapped table through a served external client using an approved, pinned mapping. Bind schema/graph revisions and row digest.
  - Attempt INSERT, UPDATE, DELETE, and DDL through read-only and mutation-granted principals. Refuse each attempt and preserve graph revision.
  - Submit an authorized graph `MutationBatch`. Confirm the projection read reflects its committed revision.
  - Refuse stale/unapproved mappings and unauthorized graph mutations.
  - Hold application-level SQL/schema conformance until the R026 owner provides the approved target mapping and captured statement corpus.
  - Preserve EG-DURABLE-KERNEL-R035, EG-DURABLE-KERNEL-R043, EG-FEDERATED-QUERY-R080, EG-UNIFIED-DATA-PLANE-R026, and EG-FEDERATED-QUERY-R080.1 through R080.6.
<!-- /spec-definition-patch -->

## EG-UNIFIED-DATA-PLANE-R038.3 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-UNIFIED-DATA-PLANE-R038.3.1` | `crates/eg-types/src/metadata_acceleration.rs::tests::snapshot_semantics_round_trip`: A serde round trip keeps every field and rejects an unknown category or isolation. |

| Behavior | `EG-UNIFIED-DATA-PLANE-R038.3.2` | `crates/eg-plan/src/federation_opt/tests.rs::fragments_share_snapshot`: Two fragments of one query read one snapshot while a concurrent writer commits. |

| Wiring | `EG-UNIFIED-DATA-PLANE-R038.3.3` | `crates/eg-plan/src/federation_opt/tests.rs::foreign_scan_opens_snapshot`: A served query over a transactional source reaches the snapshot path. |

| Refusal | `EG-UNIFIED-DATA-PLANE-R038.3.4` | `crates/eg-plan/src/federation_opt/tests.rs::snapshot_refusals`: Each case returns its code and runs no source statement. |

| Evidence | `EG-UNIFIED-DATA-PLANE-R038.3.5` | `crates/eg-plan/src/federation_opt/tests.rs::provenance_names_snapshot`: The answer provenance names one snapshot token per source. |

| Hot-path proof | `EG-UNIFIED-DATA-PLANE-R038.3.6` | `tests/federation/test_oltp_snapshot_served.py::test_fragments_share_one_snapshot`: The answer matches the snapshot oracle and the engine row count for that source stays unchanged. |


Hot-path proof for `EG-UNIFIED-DATA-PLANE-R038.3`: `tests/federation/test_oltp_snapshot_served.py::test_fragments_share_one_snapshot`.

## EG-UNIFIED-DATA-PLANE-R038.4 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-UNIFIED-DATA-PLANE-R038.4.1` | `crates/eg-lake/src/tests/metastore_model.rs::metastore_spec_round_trips`: A serde round trip keeps every field and rejects an unknown format. |

| Behavior | `EG-UNIFIED-DATA-PLANE-R038.4.2` | `crates/eg-lake/src/tests/metastore_scan.rs::partition_and_stripe_pruning`: A pushed predicate reads only the matching partitions and stripes. |

| Wiring | `EG-UNIFIED-DATA-PLANE-R038.4.3` | `crates/eg-lake/src/tests/metastore_scan.rs::served_scan_reaches_pruning`: A served query over a metastore table reaches the pruned scan. |

| Refusal | `EG-UNIFIED-DATA-PLANE-R038.4.4` | `crates/eg-lake/src/tests/metastore_scan.rs::metastore_refusals`: Each case returns its code and reads no data file. |

| Evidence | `EG-UNIFIED-DATA-PLANE-R038.4.5` | `crates/eg-lake/src/tests/metastore_scan.rs::receipt_counts_match`: The receipt counts match the files the scan opened. |

| Hot-path proof | `EG-UNIFIED-DATA-PLANE-R038.4.6` | `tests/federation/test_metastore_orc_avro_served.py::test_partition_pruned_scan`: The answer matches the full-scan oracle and the receipt shows pruning. |


Hot-path proof for `EG-UNIFIED-DATA-PLANE-R038.4`: `tests/federation/test_metastore_orc_avro_served.py::test_partition_pruned_scan`.

## EG-UNIFIED-DATA-PLANE-R038.5 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-UNIFIED-DATA-PLANE-R038.5.1` | `crates/eg-plan/src/federation_opt/tests.rs::search_capability_round_trips`: A serde round trip keeps every field and rejects an unknown score kind. |

| Behavior | `EG-UNIFIED-DATA-PLANE-R038.5.2` | `crates/eg-plan/src/federation_opt/tests.rs::relevance_pushdown_replenishes`: A pushed query returns k results with remote scores in their channel. |

| Wiring | `EG-UNIFIED-DATA-PLANE-R038.5.3` | `crates/eg-plan/src/federation_opt/tests.rs::served_search_reaches_fusion`: A served query reaches the remote index and the shared fusion. |

| Refusal | `EG-UNIFIED-DATA-PLANE-R038.5.4` | `crates/eg-plan/src/federation_opt/tests.rs::search_refusals`: Each case returns its code or pages, and no result carries an invented score. |

| Evidence | `EG-UNIFIED-DATA-PLANE-R038.5.5` | `crates/eg-plan/src/federation_opt/tests.rs::search_receipt`: The receipt matches the remote request. |

| Hot-path proof | `EG-UNIFIED-DATA-PLANE-R038.5.6` | `tests/federation/test_search_index_pushdown_served.py::test_relevance_pushed_and_k_replenished`: The answer holds k results, and the engine stores no index documents. |


Hot-path proof for `EG-UNIFIED-DATA-PLANE-R038.5`: `tests/federation/test_search_index_pushdown_served.py::test_relevance_pushed_and_k_replenished`.

## EG-UNIFIED-DATA-PLANE-R038.6 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-UNIFIED-DATA-PLANE-R038.6.1` | `crates/eg-types/src/virtual_graph_view.rs::tests::graph_view_round_trips`: A serde round trip keeps every field and rejects an unmapped label. |

| Behavior | `EG-UNIFIED-DATA-PLANE-R038.6.2` | `crates/eg-query/src/cypher/tests/virtual_view.rs::pattern_pushes_join`: One pattern renders one pushed SQL statement per same-source join. |

| Wiring | `EG-UNIFIED-DATA-PLANE-R038.6.3` | `crates/eg-query/src/cypher/tests/virtual_view.rs::served_read_resolves_view`: A served openCypher read over a mapped source reaches the view resolution. |

| Refusal | `EG-UNIFIED-DATA-PLANE-R038.6.4` | `crates/eg-query/src/cypher/tests/virtual_view.rs::view_refusals`: Each case returns its code and runs no source statement. |

| Evidence | `EG-UNIFIED-DATA-PLANE-R038.6.5` | `crates/eg-query/src/cypher/tests/virtual_view.rs::explain_names_mapping`: EXPLAIN names the mapping digest of each fragment. |

| Hot-path proof | `EG-UNIFIED-DATA-PLANE-R038.6.6` | `tests/federation/test_graph_language_virtual_served.py::test_cypher_and_gremlin_match_sparql_over_sql_source`: The three answers are equal, and the engine row count for that source stays unchanged. |


Hot-path proof for `EG-UNIFIED-DATA-PLANE-R038.6`: `tests/federation/test_graph_language_virtual_served.py::test_cypher_and_gremlin_match_sparql_over_sql_source`.

## EG-UNIFIED-DATA-PLANE-R022.4 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Evidence | `EG-UNIFIED-DATA-PLANE-R022.4` | `crates/eg-types/src/dialect_conformance.rs::tests::matrix_covers_every_source_category`: A unit test fails when a category lacks a row. It reports `unproven` for a category without a passing proof. |

## EG-UNIFIED-DATA-PLANE-R038.7 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-UNIFIED-DATA-PLANE-R038.7.1` | `crates/eg-plan/src/federation_opt/tests.rs::web_result_round_trips`: A serde round trip keeps every field and rejects a result without a URL. |

| Behavior | `EG-UNIFIED-DATA-PLANE-R038.7.2` | `crates/eg-plan/src/federation_opt/tests.rs::web_search_pushdown_and_ttl`: A repeated pattern within the time-to-live sends one connector query. |

| Wiring | `EG-UNIFIED-DATA-PLANE-R038.7.3` | `crates/eg-plan/src/federation_opt/tests.rs::served_web_search_reaches_cache`: A served pattern over web results reaches the connector operation and the shared cache. |

| Refusal | `EG-UNIFIED-DATA-PLANE-R038.7.4` | `crates/eg-plan/src/federation_opt/tests.rs::web_search_refusals`: Each case returns its code, and no expired row serves a read. |

| Evidence | `EG-UNIFIED-DATA-PLANE-R038.7.5` | `crates/eg-plan/src/federation_opt/tests.rs::web_result_provenance`: Each row names its engine, rank and retrieval time, and a cached row says so. |

| Hot-path proof | `EG-UNIFIED-DATA-PLANE-R038.7.6` | `tests/federation/test_web_metasearch_served.py::test_pattern_pushes_to_connector_with_ttl_cache`: The connector receives two queries, the rows are typed, and the graph store holds no result rows. |


Hot-path proof for `EG-UNIFIED-DATA-PLANE-R038.7`: `tests/federation/test_web_metasearch_served.py::test_pattern_pushes_to_connector_with_ttl_cache`.
