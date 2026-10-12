# Test specification and acceptance evidence

All tests run from the repository root with local fixtures, mock servers, or isolated ephemeral stores by default. Live foreign services are an optional integration profile with explicit provisioning; a cloud contributor can execute parser, optimizer, security, differential and contract gates without private infrastructure. For each accepted slice, record the exact commit, command, exit status, fixture version and relevant assertion count in a reviewed evidence entry.

| Case | Requirements | Fixture and action | Expected result |
|---|---|---|---|
| Q-01 grammar exhaustiveness | FQR-01/02 | Every `OpKind`/`PredKind` plus 512 generated valid programs; parse → canonical print → parse. Run generated EBNF/dispatch check. | Same typed plan; no uncovered production variant. Unsupported feature gives `UQL_FEATURE_NOT_IN_BUILD` or `UQL_UNSUPPORTED`, with span. |
| Q-02 typed bindings | FQR-01/10 | Values with quotes, Unicode, JSONPath, arrays; unbound, mistyped and unused params; injection strings. | One bind result per declared type; no text interpolation; exact typed refusal for each invalid case. |
| Q-03 read-only/snapshot | FQR-02 | All UQL op variants, `LET`/`JOIN`, concurrent write between plan and execution. | No mutation-capable method; single authorized snapshot; budgets refuse at boundary without partial rows. |
| Q-03a reconciliation | EG-FEDERATED-QUERY-R007 | Interrupt a semantic-index source scan before complete-snapshot proof, then replay a complete scan. | The partial scan creates no tombstone; the complete scan tombstones exactly the absent sources. The focused `missing_sources_become_tombstones_only_after_complete_snapshot_proof` regression passes repeatedly. |
| Q-03b graph-free SQL | FQR-02 | DDL, DML, `COPY`, external-table statement and multi-statement payload through direct graph-free SQL and observability search. | Read-only guard refuses each mutation before execution; ordinary SELECT succeeds. |
| Q-04 proof/trace | FQR-02/08/10 | SPARQL/rule derivation, `WITH PROOF`, nested `LET`/`JOIN`, source with secret-like label. | Proof premises verify against generation; channels survive wire; trace has remote/local rows and no secret/URL/DSN. |
| F-01 result equivalence | FQR-03/06 | SQL, HTTP, RemoteEngine, SPARQL and OBDA mocks with duplicate rows, repeated predicate values, null/type edges, filter/order/limit/aggregate/join cases. Run optimized and naive full fetch. | Equal multisets, or equal ordered rows where ordered; no first-page truncation. Mock records lower or equal transfer where pushdown is declared. |
| F-02 budget and key source | FQR-03/04 | Set each request, row, key and wall limit one below needed work. Request full scan of `RequiresKeys`. | `FEDERATION_BUDGET_EXCEEDED:<dimension>` or `FEDERATION_SOURCE_REQUIRES_KEYS`. No partial success. |
| F-03 outbound policy | FQR-04 | Local/private/CGNAT/6to4/Teredo, redirect, DNS rebind, host placeholder, SQL injection, ungranted source, cross-owner key. | Refused before request or redirected hop; zero cross-tenant rows, credential bytes or network access. |
| F-04 capability fallback | FQR-03/05 | Source reports `Inexact` filter, lacks top-N, errors on batch, or has unknown stats; retry under rate limit. | Exact local residual; bounded fallback; trace names capability and estimate provenance. |
| F-05 cache/freshness | FQR-05 | Same canonical fragment across owners; watermark advance, grant revoke, missing watermark, schema change. | Only same-owner/current-watermark hit; all other cases miss or refuse. |
| F-06 OBDA/SERVICE/Iceberg | FQR-06 | Two predicate maps sharing rows, nonunique subject, `a`/`aa` template suffix, multivalued RDF predicate, partitioned Iceberg, SERVICE bind variables. | Differential answer equality; unsupported ORDER/aggregation falls back; manifest and request count prove pruning. |
| S-01 filtered search | FQR-07 | BM25 and ANN fixture with 10 permitted matches below unfiltered top-k; two-hop traversal and parallel edges. | Exactly k permitted IDs where available, stable edge identity; otherwise typed shortfall, not silent 7/10. |
| S-02 durable generation | FQR-07/08 | Crash during backfill, restart after activation, delete/replay, skipped reasoning outbox event, revoked tenant. | Atomic previous/new generation, correct scan fallback, rebuild on gap, no forbidden row. |
| S-03 bounded reopen | FQR-07 | Reopen graphs at increasing document counts with persisted index state and a deliberately incomplete state. | Complete state reopens without per-document rebuild; incomplete state returns typed partial-materialization until repair, then correct reads. |
| R-01 ontology corpus | FQR-08 | Shipped core schema plus minimal disjointness and deliberately inconsistent corpus. | Bounded compose with no stack overflow; EL/RL and tableau agree on consistency; unsatisfiable classes named; proof premises verifiable. |
| N-01 series and stats | FQR-09 | Two series at same timestamp, f64 values, constant/alternating/large windows, reference recompute. | No row collision or f32 truncation; flat window variance/z-score exactly zero; O(1) update measured independent of window length. |
| N-02 advanced kernels | FQR-09 | Exact small Shapley oracle, seeded sampled CI, DAG/cycle impact, barrier sigma→0, motif brute-force, high-tail literal. | Exact results where specified; seeded repeatability and CI coverage; high-tail relative error ≤1e-12; budget refusal is typed. |
| C-01 generated client | FQR-10 | Regenerate and check Rust method/error/receipt contracts; invoke UQL through Python generated sender. | Exact shape/code parity and served behavior; generated files show no manual drift. |
| F-07 ontology source selection | FQR-11 | Ontology with labels, a subclass mapping, two sources, an unapproved mapping and a disconnected class pair. | Path and mapping per class with premises; subclass mapping serves the superclass; unapproved class uncovered; disconnected pair answers no path. |
| F-08 operation-bound sources | FQR-04/11 | Recording MCP and GraphQL mocks with and without key-filter capability; budget one below need; ungranted source. | Equal rows with and without pushdown; residual always applied; typed budget and outbound refusals. |

## Quality and release gates

### EG-FEDERATED-QUERY-R045 / EG-FEDERATED-QUERY-R052 focused differential tests

| ID | Positive proof | Negative and boundary proof |
|---|---|---|
| EG-FEDERATED-QUERY-R045 | Mock SQL/HTTP/RemoteEngine rows include duplicate names, nulls, numeric/date values and a foreign-only filter. Exact push and inexact push plus local residual equal full-fetch ordered/multiset oracles; projection retains hidden residual columns until evaluated. | Unsupported filter, ambiguous schema, changed type, inexact push missing a matching row and projection dropping an authorization/order column refuse or fall back; never silently return partial rows. |
| EG-FEDERATED-QUERY-R052 | A recording second EG instance receives canonical `Method::Uql` with only authorized keys and a sound LIMIT, returns the same ordered result as no push, and reports lower transferred row count. | Residual filter/top-N interaction, pagination, missing UQL capability, invalid signature, ungranted key, redirect/private target and exhausted request/row budget refuse or use a safe bounded fallback; no secret appears in trace. |

Run changed-file CCCC, Dupehound, KISS and differential jscpd from `.config/pre-commit.yaml`; the configured CCCC threshold is no new function above cyclomatic 10 or cognitive 15 and no regression. Differential jscpd must show zero newly introduced clone pairs. KISS and Dupehound use their checked-in configuration, with zero attributable violations. Run `cargo fmt --all -- --check`, targeted `cargo test --locked` for each changed crate, full-feature Clippy with `-D warnings`, generated-contract checks, and the repository's hosted CI matrix for the final PR head. `bash scripts/ci_parity.sh` is a local CI preview. Record any unavailable live-service profile separately; it cannot be replaced by a fabricated pass. Every new public source kind needs a mock conformance suite before a live environment is considered.

## Evidence record format

For each accepted stable ID: `ID | public merged SHA | test command/profile | expected vs actual | quality gate | client/contract digest | reviewer/date`. A task checkbox, source branch, historical log or local-only build never substitutes for exact-head evidence. If a test is omitted, state the risk and named replacement before the ID can be accepted.

## EG-FEDERATED-QUERY-R056 focused acceptance

| Requirement | Proving test | Coverage and result |
|---|---|---|
| EG-FEDERATED-QUERY-R056 | `crates/eg-compute/src/mining/causal_impact.rs::tests::two_sided_p_keeps_relative_accuracy_in_the_far_tail` | Passed against exact production modules and real libm: independent literal probabilities for z = ±3, ±6, ±8, ±10, ±20 at relative error < 1e-12. Z = 0 and +infinity boundaries. All five causal-impact tests passed. |

Canonical crate command: `cargo test -p eg-compute --lib mining::causal_impact::tests`.
The 2026-10-11 bounded local run instead compiled the exact module with `rustc
--test --edition=2021`, importing production numeric modules by path and linking
the cached real libm. This is pure-module acceptance, not a full-crate or hosted
release result. See the owning plan's audit record for revision and limits.

### 2026-10-11 — NL cache source guard (EG-FEDERATED-QUERY-R058.3.1)

Base: `f154c5b4a115713ab8ff3e1d9643e2295c4ebe43`. Extract the exact `nl.rs`
test module into a standalone Rust test crate. Keep the original `nl.rs`
alongside it to preserve `include_str!` resolution. The old guard fails
(exit 101, one failed test). All three repaired tests pass (exit 0).
The real NL source remains uncached. The guard rejects injected `ResultCache`
references and direct or generic cache-aware response calls. Missing
production/test boundaries and the uncached response path fail closed.

The probe uses `rustc --edition=2021 --test --cfg 'feature="nl-query"' probe.rs`.
It excludes production compilation and proves the source guard only. Native
facade execution was not repeated in this repair; the separate native failure
and resource limits remain outside this evidence. Rust formatting, diff
whitespace, and public-spec checks pass. No production API or grammar changes.

Column vectorization (R077): a bound text column answers a nearest-neighbour query and EXPLAIN names the binding. An unbound column and a non-text column are refused. One row write queues exactly one re-embed.

## EG-FEDERATED-QUERY-R078 acceptance

| Scenario | Requirement | Fixture |
|---|---|---|
| Out-of-tenant isolation | R078.6 | Two tenants hold nodes, edges and vectors. A query as tenant A returns only A rows. |
| Unverified caller | R078.4 | A statement without a verified tenant returns `PLAN_SCOPE_UNVERIFIED` and reads nothing. |

## EG-FEDERATED-QUERY-R079 and R080 acceptance

These are required acceptance cases, not claims of completed tests. All changed requirements remain SPECIFIED until production evidence satisfies them.

| Scenario | Requirement | Fixture |
|---|---|---|
| Publication model | R079.1 | Round-trip version, tenant, schema, snapshot, locations and digests; reject missing binding and unsupported format. |
| Manifest writing compatibility | R079.2, R079.6 | Authoritative served graph append, compaction and copy-on-write deletion produce v2 manifests. A pinned independent reader queries their Parquet data through the authenticated catalog. Check counts, paths, sizes, partition/schema IDs and sequence metadata. |
| Schema evolution and time travel | R079.2, R079.3, R079.6 | Authoritative graph schema changes add, drop, rename and reorder. The external reader resolves current and retained snapshots, stable field IDs and expected rows. Identify the real graph handler, publisher and REST routes. |
| Atomic publication failure | R079.2, R079.4 | Fail artifact durability before metadata visibility. The previous snapshot remains readable. The catalog advertises no incomplete snapshot. Retry cannot expose duplicate committed state. |
| Unapproved drop and invalid publication | R079.4 | Unapproved drop, unknown column, absent graph binding and unsupported manifest features return their typed codes. Visible versions remain unchanged. |
| Correlated publication evidence | R079.5 | Match committed graph version and LSN to schema/snapshot IDs, artifact digests, outcome and time-travel resolution. Record reader version and feature matrix. |
| Projection stays read-only | R080.2, R080.4, R080.6 | Reject create, data commit, metadata/schema commit, rename, drop and unknown mutation through the projection. Assert HTTP 405, `PROJECTION_READ_ONLY`, no graph change and no publication call. |
| Boundary preservation | R079.3, R080.6 | Graph write becomes externally readable through the engine publisher without a separate copy job. No external-source adapter effect or credentials are invoked by manifest publication; requested source effects require the existing SDK-governed contract. |

### EG-FEDERATED-QUERY-R081 tests

- `EG-FEDERATED-QUERY-R081.1`: `crates/eg-types/src/retrieval_verdict.rs::tests::verdict_round_trips_and_rejects_unknown`.
- `EG-FEDERATED-QUERY-R081.2`: `crates/eg-plan/src/retrieval_verdict.rs::tests::repair_order_ends_at_round_limit`.
- `EG-FEDERATED-QUERY-R081.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_verdict_for_rule_plan`.
- `EG-FEDERATED-QUERY-R081.4`: `crates/eg-plan/src/retrieval_verdict.rs::tests::strict_insufficient_abstains`.
- `EG-FEDERATED-QUERY-R081.5`: `crates/eg-plan/src/retrieval_verdict.rs::tests::verdict_premise_lists_all_fields`.
- `EG-FEDERATED-QUERY-R081.6`: `tests/retrieval/test_sufficiency_served.py::test_insufficient_returns_boost_k`.

### EG-FEDERATED-QUERY-R082 tests

- `EG-FEDERATED-QUERY-R082.1`: `crates/eg-types/src/retrieval_stop.rs::tests::table_and_verdict_round_trip`.
- `EG-FEDERATED-QUERY-R082.2`: `crates/eg-plan/src/retrieval_stop.rs::tests::repeat_and_margin_stops`.
- `EG-FEDERATED-QUERY-R082.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_stop_verdict_with_history`.
- `EG-FEDERATED-QUERY-R082.4`: `crates/eg-plan/src/retrieval_stop.rs::tests::uncalibrated_round_refuses`.
- `EG-FEDERATED-QUERY-R082.5`: `crates/eg-plan/src/retrieval_stop.rs::tests::stop_premise_lists_margin_and_reason`.
- `EG-FEDERATED-QUERY-R082.6`: `tests/retrieval/test_stop_rule_served.py::test_repeat_with_margin_stops`.

### EG-FEDERATED-QUERY-R083 tests

- `EG-FEDERATED-QUERY-R083.1`: `crates/eg-types/src/retrieval_arbitration.rs::tests::verdict_carries_both_candidates`.
- `EG-FEDERATED-QUERY-R083.2`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::margin_picks_or_abstains`.
- `EG-FEDERATED-QUERY-R083.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_arbitration_for_prior_candidate`.
- `EG-FEDERATED-QUERY-R083.4`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::abstain_returns_both_candidates`.
- `EG-FEDERATED-QUERY-R083.5`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::arbitration_premise_lists_both_classes`.
- `EG-FEDERATED-QUERY-R083.6`: `tests/retrieval/test_arbitration_served.py::test_small_gap_abstains_with_both`.

## R081.7 through R081.9 served acceptance

These cases specify future evidence. Every new requirement remains SPECIFIED until real served tests and the activation benchmark pass.

| Scenario | Requirements | Positive and negative acceptance |
|---|---|---|
| Typed contracts | R081.7.1, R081.8.1, R081.9.1 | Round-trip revisions, IDs, budgets and receipts. Refuse missing provenance, invalid limits and non-finite scores. |
| Dual-score golden cases | R081.7.2, R081.7.4, R081.7.6 | With fixture thresholds s=0.7 and r=0.6, rescue (0.2,0.9) and reject (0.2,0.1). Retain dual-high (0.9,0.9). For w=0.3 and residual threshold 0.4, reject (0.9,0.1), whose fused score is 0.34. |
| Adaptive count and activation | R081.7.3, R081.7.5, R081.7.6 | Two corpus queries return different accepted counts. Compare the fixed-count baseline with identical scoring calls. Selection adds zero inference. Refuse missing or mismatched benchmark approval. |
| Empty and constant scores | R081.7.2, R081.7.4 | Empty accepted sets return insufficiency. Constant-score normalization follows its declared rule or refuses. Never pad with dual-low evidence. |
| Feedback and perspectives | R081.8.2, R081.8.3, R081.8.6 | Successive served rounds produce diverse queries and exact reinforce/missing/suppress tiers. Corpus feedback raises useful term weights and lowers misleading ones. |
| Feedback refusal and critique | R081.8.4, R081.8.5, R081.8.6 | Refuse forged or stale feedback. Record critique and its effect; hostile instructions cannot change measured scores, scope or exhausted budgets. |
| Hierarchy descent | R081.9.2, R081.9.3, R081.9.6 | Assert summary filtering, community refinement and entity search in order. Compare selected paths and final IDs to an independent corpus oracle. |
| Hierarchy refusals and costs | R081.9.4, R081.9.5, R081.9.6 | Refuse cycles, stale summaries and missing entities. Assert exact visited-node and scoring counters, plus recorded measured cost and bounded stopping. |
| Scope and evidence | R081.7.5, R081.8.5, R081.9.5 | Two-tenant fixtures prove every stage applies scope before truncation. Independently read receipts and trace; no forbidden entity, summary or feedback is consumed. |

Fixture thresholds above are deterministic test values, not production defaults or claimed paper parameters. The benchmark defines production thresholds before activation. Use actual served operators and pinned production inference artifacts; helper-only results and mocked terminal responses do not establish acceptance.

### R081.8 / R082 uncalibrated continuation acceptance

`tests/retrieval/test_feedback_served.py::test_uncalibrated_continuation_precedence` covers R081.8.1, R081.8.3 through R081.8.6 and R082.1 through R082.6. Start an authenticated served query against a deterministic corpus and pinned production inference artifacts. Obtain EG continuations from actual completed rounds; do not inject terminal responses or replace production operators. Omit the calibration row for the next round. Retain the same tenant, corpus revision and policy.

| Completed round / remaining limit | Required served result |
|---|---|
| New accepted documents, positive quality gain, all limits available | `stop=false`, `continue_uncalibrated`, `ROUND_NOT_CALIBRATED`; one bounded next continuation. |
| Zero new accepted document IDs, limits available | `stop=true`, `no_new_evidence`, `ROUND_NOT_CALIBRATED`. |
| New documents with quality gain at the declared saturation threshold for the declared window | `stop=true`, `quality_saturated`, `ROUND_NOT_CALIBRATED`. |
| Round limit reached and progress also saturated | `stop=true`, `round_limit`, `RETRIEVAL_BUDGET_EXCEEDED`, `ROUND_NOT_CALIBRATED`. |
| Each query, term, token or inference limit exhausted; saturation also present | `stop=true`, `budget_exhausted`, exact exhausted dimension, both refusal codes. |
| Forged, stale or cross-tenant saturation evidence | `RETRIEVAL_FEEDBACK_INVALID`; no continuation or successful confidence claim. |

For every missing-calibration case, assert absent margin/threshold and preserved R081 insufficiency. Read the decision trace and dispatch counters independently; terminal and invalid continuations issue zero subsequent retrieval, generation or assessment calls. Repeating a terminal continuation cannot reset budgets. A calibrated control still stops only when the repeated answer exceeds its margin threshold. Equality continues when no earlier terminal condition applies. These are acceptance requirements, not executed runtime evidence.

## Retrieval policy extensions: served acceptance

Acceptance state: **NOT RUN** for R039.1, R039.2, R039.3, R039.4, R040.1, R040.2, R040.3, R040.4, R077.6 and R081.9.7.
All test paths below are planned; this definitions batch adds no runtime test or implementation.

The harness must start an authenticated engine with disposable persistent storage and two independent tenant clients.
It must pin corpus, policy, model artifacts, preprocessing, tokenizer, graph and index generations.
It must invoke real transport, authorization, production dispatch, planner, model bindings and index reads.
It must not mock those components or inject finished rankings, feedback, topic scores or receipts.
A deterministic hash embedder cannot establish semantic or cross-modal acceptance.
The evidence reader must use a separate authorized client and read the actual trace and persisted records.

| Planned test | Positive acceptance | Negative acceptance |
|---|---|---|
| `tests/retrieval/test_multimodal_served.py` | Issue all sixteen text/image/audio/video direction pairs through UnifiedQuery. Read expected permitted matches and shared space identity. Verify actual model invocations and maintained index reads. | Delete one authorized target embedding and assert exact missing_embedding shortfall. Delete query binding and refuse before rank. Reject same-width incompatible spaces, unavailable modalities and cross-tenant inputs. |
| `tests/retrieval/test_constraints_served.py` | Load a real relationship fixture where the best similarity violates a required edge. Require permitted lower-scored results and exact replenishment. Compare optimized and unoptimized results with a separate exhaustive graph oracle. | Contradictory predicates produce zero eligible results and typed shortfall. Unsupported operators and stale graph revisions refuse. Prohibited candidates remain absent from text, vector and topic branches. |
| `tests/retrieval/test_section_tree_served.py` | Ingest a three-level section tree without vectors. Invoke approved production reasoning and read complete ordered paths, spans and pruning evidence. Assert zero embedding/index calls. | Reject cycles, multiple roots, dangling parents, invalid spans and stale summaries. Exhaust each limit separately and together. Refuse before further work; preserve completed incomplete paths and tenant isolation. |
| `tests/retrieval/test_chunk_encoding_served.py` | Governed ingestion must encode the same long document with local-reset and absolute-offset policies. Capture actual token ranges and independent model calls. Recompute mean and final normalization independently, then query the single published vector through UnifiedQuery. | Reject overlap at least size, unsupported offsets, non-finite or mismatched vectors and failed chunks. Empty input cannot publish a vector. Policy-generation mismatch must refuse; failed replacement must retain the prior complete generation. |
| `tests/retrieval/test_column_advice_served.py` | Create two equally distributed text columns. Issue column-specific text-search queries, then select from column_vector_advice through Sql. Reverse observed demand and assert reversed recommendations. Independently verify recorded workload contributions. | Reject unembeddable types before recommendation scoring. Cover no observations, stale schema, denied tables and unavailable encoders. Assert zero new bindings, embedding calls and write-side effects. |
| `tests/retrieval/test_topic_fusion_served.py` | Bind two production topic backends sequentially to the same corpus and space. Query at weights 0, 0.5 and 1. Independently reproduce normalized scores and stable ordering from receipts. | Reject mismatched dimensions, model revision, preprocessing, space tuple and corpus generation. Reject invalid responsibilities, zero vectors and missing backends. Hidden-topic data must never affect ranking or evidence. |
| `tests/retrieval/test_mixed_traversal_topk_served.py` — EG-FEDERATED-QUERY-R039.4 | Reuse the S-01 two-hop traversal and parallel-edge fixture. Include ten permitted matches below unfiltered top-k. Run authenticated mixed traversal-plus-vector queries against that reference corpus and require stable edge identity. Include a query whose first k similarity candidates fail the traversal filter but later candidates survive. The independent oracle must confirm exactly k permitted results when available. Otherwise, require an explicit typed shortfall naming the permitted match count. | Fail acceptance for a silent undercount or premature truncation when enough permitted matches exist. Reject a wrong shortfall count or an unauthorized result or count. Test fewer than k surviving candidates and zero survivors. |
| `tests/retrieval/test_filtered_scan_benchmark_served.py` — EG-FEDERATED-QUERY-R040.4 | Benchmark filtered ANN and BM25 latency before and after the filter-aware scan change. Use the same fixed corpus and query workload. Run the independent S-01 filtered-search correctness oracle alongside both measurements. Read actual plans and candidate traces to prove pre-filtered candidate sets or bitmap filters reach index scans. | Fail acceptance if either search mode lacks a measured before/after result or correctness proof. Reject a changed corpus or workload presented as a comparison. Reject a plan that scans the full unfiltered index and discards non-matching rows afterward. |

The residual cases above retain the complete original R039 and R040 acceptance, independently of the new policy extensions.
Their served entry is UnifiedQuery through `src/server/handlers/query/planning.rs::run_unified_with` and `crates/eg-plan/src/exec.rs::execute`.
The mixed query must reach `crates/eg-plan/src/exec.rs::traverse_op` and `crates/eg-plan/src/exec/vector_rank.rs::rank_top`.
The filtered scan benchmark must reach that vector path and `crates/eg-plan/src/exec.rs::rank_text` with real maintained indexes.
Record corpus digest, query workload, source revisions, index generations, benchmark environment, measured latency and oracle results.
The original R040 contract requires the comparison; it supplies no numerical latency target or claimed speedup.
Both residual cases remain NOT RUN. Existing parent receipts do not establish acceptance for the new leaf IDs.

For chunk numerical comparison, use absolute tolerance 1e-6 and relative tolerance 1e-5 for f32 aggregate outputs.
Pin the backend numerical mode and record any stricter tolerance required by its contract.
Exact token boundaries, counts, identities and generations permit no tolerance.
The fixture must include partial final chunks and nonzero overlap.
Different position modes require independently checked position inputs; an assumed difference in ranking is not proof.

For constraints, test required and forbidden directed edges plus typed entity predicates.
Set k above the number of surviving candidates and verify typed constraint-exhaustion shortfall.
For multimodal shortfall, include both missing vectors and unauthorized vectors.
Unauthorized items must contribute neither identifiers nor counts to the response.

For section traversal, rerun the existing community journey in `tests/retrieval/test_hierarchy_served.py`.
Document-section selection must not alter community stages or entity paths.
Rerun `tests/retrieval/test_feedback_served.py::test_uncalibrated_continuation_precedence`.
Invalid feedback, round exhaustion, other budget exhaustion, no new evidence and saturation must retain their existing precedence.
Neither section selection nor new fusion may claim sufficiency from an uncalibrated terminal result.

Restart before independent readback of published encoding generations, section revisions and retained receipts.
Record exact source revision, enabled features, test command, policy revisions, measured work and result.
Unavailable production bindings leave acceptance NOT RUN or failed, never passed through helper-only substitution.
