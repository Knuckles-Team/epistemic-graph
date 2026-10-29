# Test specification and acceptance evidence

All tests run from the repository root with local fixtures, mock servers, or isolated ephemeral stores by default. Live foreign services are an optional integration profile with explicit provisioning. A cloud contributor can execute parser, optimizer, security, differential and contract gates without private infrastructure. For each accepted slice, record the exact commit, command, and exit status. Also record the fixture version and relevant assertion count in a reviewed evidence entry.

| Case | Requirements | Fixture and action | Expected result |
|---|---|---|---|
| Q-01 grammar exhaustiveness | FQR-01/02 | Every `OpKind`/`PredKind` plus 512 generated valid programs; parse → canonical print → parse. Run generated EBNF/dispatch check. | Same typed plan; no uncovered production variant. Unsupported feature gives `UQL_FEATURE_NOT_IN_BUILD` or `UQL_UNSUPPORTED`, with span. |
| Q-02 typed bindings | FQR-01/10 | Values with quotes, Unicode, JSONPath, arrays; unbound, mistyped and unused params; injection strings. | One bind result per declared type; no text interpolation; exact typed refusal for each invalid case. |
| Q-03 read-only/snapshot | FQR-02 | All UQL op variants, `LET`/`JOIN`, concurrent write between plan and execution. | No mutation-capable method; single authorized snapshot; budgets refuse at boundary without partial rows. |
| Q-03a reconciliation | MEDULLA-FEDERATED-QUERY-R007 | Interrupt a semantic-index source scan before complete-snapshot proof, then replay a complete scan. | The partial scan creates no tombstone; the complete scan tombstones exactly the absent sources. The focused `missing_sources_become_tombstones_only_after_complete_snapshot_proof` regression passes repeatedly. |
| Q-03b graph-free SQL | FQR-02 | DDL, DML, `COPY`, external-table statement and multi-statement payload through direct graph-free SQL and observability search. | Read-only guard refuses each mutation before execution; ordinary SELECT succeeds. |
| Q-04 proof/trace | FQR-02/08/10 | SPARQL/rule derivation, `WITH PROOF`, nested `LET`/`JOIN`, source with secret-like label. | Proof premises verify against generation; channels survive wire; trace has remote/local rows and no secret/URL/DSN. |
| F-01 result equivalence | FQR-03/06 | SQL, HTTP, RemoteEngine, SPARQL and OBDA mocks cover duplicate rows, repeated predicates, nulls, types and filter/order/limit/aggregate/join cases. Run optimized and naive full fetch. | Equal multisets, or equal ordered rows where ordered. No first-page truncation. Mock records lower or equal transfer when pushdown is declared. |
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

### MEDULLA-FEDERATED-QUERY-R045 / MEDULLA-FEDERATED-QUERY-R052 focused differential tests

| ID | Positive proof | Negative and boundary proof |
|---|---|---|
| MEDULLA-FEDERATED-QUERY-R045 | Mock SQL/HTTP/RemoteEngine rows include duplicate names, nulls, numeric/date values and a foreign-only filter. Exact push and inexact push plus local residual equal full-fetch ordered/multiset oracles. Projection retains hidden residual columns until evaluated. | Unsupported filters, ambiguous schemas, and changed types refuse or fall back. An inexact push missing a matching row also refuses or falls back. Projection dropping an authorization or ordering column refuses or falls back. The system never silently returns partial rows. |
| MEDULLA-FEDERATED-QUERY-R052 | A recording second Medulla instance receives canonical `Method::Uql` with only authorized keys and a sound LIMIT, returns the same ordered result as no push, and reports lower transferred row count. | Residual filter/top-N interaction, pagination, missing UQL capability, invalid signature, ungranted key, redirect/private target and exhausted request/row budget refuse or use a safe bounded fallback; no secret appears in trace. |

Run changed-file CCCC, Dupehound, KISS and differential jscpd from `.config/pre-commit.yaml`; the configured CCCC threshold is no new function above cyclomatic 10 or cognitive 15 and no regression. Differential jscpd must show zero newly introduced clone pairs. KISS and Dupehound use their checked-in configuration, with zero attributable violations. Run `cargo fmt --all -- --check`, targeted `cargo test --locked` for each changed crate, full-feature Clippy with `-D warnings`, generated-contract checks, and the repository's hosted CI matrix for the final PR head. `bash scripts/ci_parity.sh` is a local CI preview. Record any unavailable live-service profile separately; it cannot be replaced by a fabricated pass. Every new public source kind needs a mock conformance suite before a live environment is considered.

## Evidence record format

For each accepted stable ID: `ID | public merged SHA | test command/profile | expected vs actual | quality gate | client/contract digest | reviewer/date`. A task checkbox, source branch, historical log or local-only build never substitutes for exact-head evidence. If a test is omitted, state the risk and named replacement before the ID can be accepted.

## MEDULLA-FEDERATED-QUERY-R056 focused acceptance

| Requirement | Proving test | Coverage and result |
|---|---|---|
| MEDULLA-FEDERATED-QUERY-R056 | `crates/eg-compute/src/mining/causal_impact.rs::tests::two_sided_p_keeps_relative_accuracy_in_the_far_tail` | Passed against exact production modules and real libm: independent literal probabilities for z = ±3, ±6, ±8, ±10, ±20 at relative error < 1e-12. Z = 0 and +infinity boundaries. All five causal-impact tests passed. |

Canonical crate command: `cargo test -p eg-compute --lib mining::causal_impact::tests`.
The 2026-10-11 bounded local run compiled the exact module with `rustc
--test --edition=2021`. It imported production numeric modules by path and linked
the cached real libm. This is pure-module acceptance, not a full-crate or hosted
release result. See the owning plan's audit record for revision and limits.

### 2026-10-11 — NL cache source guard (MEDULLA-FEDERATED-QUERY-R058.3.1)

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

## MEDULLA-FEDERATED-QUERY-R078 acceptance

| Scenario | Requirement | Fixture |
|---|---|---|
| Out-of-tenant isolation | R078.6 | Two tenants hold nodes, edges and vectors. A query as tenant A returns only A rows. |
| Unverified caller | R078.4 | A statement without a verified tenant returns `PLAN_SCOPE_UNVERIFIED` and reads nothing. |

## MEDULLA-FEDERATED-QUERY-R079 and R080 acceptance

These are required acceptance cases, not claims of completed tests.

| Scenario | Requirement | Fixture |
|---|---|---|
| Publication model | R079.1 | Round-trip version, tenant, schema, snapshot, locations and digests; reject missing binding and unsupported format. |
| Manifest writing compatibility | R079.2, R079.6 | Authoritative served graph append, compaction and copy-on-write deletion produce v2 manifests. A pinned independent reader queries their Parquet data through the authenticated catalog. Check counts, paths, sizes, partition/schema IDs and sequence metadata. |
| Schema evolution and time travel | R079.2, R079.3, R079.6 | Authoritative graph schema changes add, drop, rename and reorder. The external reader resolves current and retained snapshots, stable field IDs and expected rows. Identify the real graph handler, publisher and REST routes. |
| Atomic publication failure | R079.2, R079.4 | Fail artifact durability before metadata visibility. The previous snapshot remains readable. The catalog advertises no incomplete snapshot. Retry cannot expose duplicate committed state. |
| Unapproved drop and invalid publication | R079.4 | Unapproved drop, unknown column, absent graph binding and unsupported manifest features return their typed codes. Visible versions remain unchanged. |
| Correlated publication evidence | R079.5 | Match committed graph version and LSN to schema/snapshot IDs, artifact digests, outcome and time-travel resolution. Record reader version and feature matrix. |
| Projection stays read-only | R080.2, R080.4, R080.6 | Reject create, data commit, metadata/schema commit, rename, drop and unknown mutation through the projection. Assert HTTP 405, `PROJECTION_READ_ONLY`, no graph change and no publication call. |
| Boundary preservation | R079.3, R080.6 | Graph write becomes externally readable through the engine publisher without a separate copy job. No external-source adapter effect or credentials are invoked by manifest publication; requested source effects require the existing Graft SDK-governed contract. |

### MEDULLA-FEDERATED-QUERY-R081 tests

- `MEDULLA-FEDERATED-QUERY-R081.1`: `crates/eg-types/src/retrieval_verdict.rs::tests::verdict_round_trips_and_rejects_unknown`.
- `MEDULLA-FEDERATED-QUERY-R081.2`: `crates/eg-plan/src/retrieval_verdict.rs::tests::repair_order_ends_at_round_limit`.
- `MEDULLA-FEDERATED-QUERY-R081.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_verdict_for_rule_plan`.
- `MEDULLA-FEDERATED-QUERY-R081.4`: `crates/eg-plan/src/retrieval_verdict.rs::tests::strict_insufficient_abstains`.
- `MEDULLA-FEDERATED-QUERY-R081.5`: `crates/eg-plan/src/retrieval_verdict.rs::tests::verdict_premise_lists_all_fields`.
- `MEDULLA-FEDERATED-QUERY-R081.6`: `tests/retrieval/test_sufficiency_served.py::test_insufficient_returns_boost_k`.

### MEDULLA-FEDERATED-QUERY-R082 tests

- `MEDULLA-FEDERATED-QUERY-R082.1`: `crates/eg-types/src/retrieval_stop.rs::tests::table_and_verdict_round_trip`.
- `MEDULLA-FEDERATED-QUERY-R082.2`: `crates/eg-plan/src/retrieval_stop.rs::tests::repeat_and_margin_stops`.
- `MEDULLA-FEDERATED-QUERY-R082.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_stop_verdict_with_history`.
- `MEDULLA-FEDERATED-QUERY-R082.4`: `crates/eg-plan/src/retrieval_stop.rs::tests::uncalibrated_round_refuses`.
- `MEDULLA-FEDERATED-QUERY-R082.5`: `crates/eg-plan/src/retrieval_stop.rs::tests::stop_premise_lists_margin_and_reason`.
- `MEDULLA-FEDERATED-QUERY-R082.6`: `tests/retrieval/test_stop_rule_served.py::test_repeat_with_margin_stops`.

### MEDULLA-FEDERATED-QUERY-R083 tests

- `MEDULLA-FEDERATED-QUERY-R083.1`: `crates/eg-types/src/retrieval_arbitration.rs::tests::verdict_carries_both_candidates`.
- `MEDULLA-FEDERATED-QUERY-R083.2`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::margin_picks_or_abstains`.
- `MEDULLA-FEDERATED-QUERY-R083.3`: `crates/eg-plan/src/exec.rs::tests::execute_returns_arbitration_for_prior_candidate`.
- `MEDULLA-FEDERATED-QUERY-R083.4`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::abstain_returns_both_candidates`.
- `MEDULLA-FEDERATED-QUERY-R083.5`: `crates/eg-plan/src/retrieval_arbitration.rs::tests::arbitration_premise_lists_both_classes`.
- `MEDULLA-FEDERATED-QUERY-R083.6`: `tests/retrieval/test_arbitration_served.py::test_small_gap_abstains_with_both`.

## R081.7 through R081.9 served acceptance

These cases specify future evidence. The real served tests and activation benchmark must satisfy their acceptance criteria.

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

`tests/retrieval/test_feedback_served.py::test_uncalibrated_continuation_precedence` covers R081.8.1, R081.8.3 through R081.8.6 and R082.1 through R082.6. Start an authenticated served query against a deterministic corpus and pinned production inference artifacts. Obtain Medulla continuations from actual completed rounds; do not inject terminal responses or replace production operators. Omit the calibration row for the next round. Retain the same tenant, corpus revision and policy.

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

These acceptance scenarios have not been executed.
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
| `tests/retrieval/test_mixed_traversal_topk_served.py` — MEDULLA-FEDERATED-QUERY-R039.4 | Reuse the S-01 two-hop traversal and parallel-edge fixture. Include ten permitted matches below unfiltered top-k. Run authenticated mixed traversal-plus-vector queries against that reference corpus and require stable edge identity. Include a query whose first k similarity candidates fail the traversal filter but later candidates survive. The independent oracle must confirm exactly k permitted results when available. Otherwise, require an explicit typed shortfall naming the permitted match count. | Fail acceptance for a silent undercount or premature truncation when enough permitted matches exist. Reject a wrong shortfall count or an unauthorized result or count. Test fewer than k surviving candidates and zero survivors. |
| `tests/retrieval/test_filtered_scan_benchmark_served.py` — MEDULLA-FEDERATED-QUERY-R040.4 | Benchmark filtered ANN and BM25 latency before and after the filter-aware scan change. Use the same fixed corpus and query workload. Run the independent S-01 filtered-search correctness oracle alongside both measurements. Read actual plans and candidate traces to prove pre-filtered candidate sets or bitmap filters reach index scans. | Fail acceptance if either search mode lacks a measured before/after result or correctness proof. Reject a changed corpus or workload presented as a comparison. Reject a plan that scans the full unfiltered index and discards non-matching rows afterward. |

The residual cases above retain the complete original R039 and R040 acceptance, independently of the new policy extensions.
Their served entry is UnifiedQuery through `src/server/handlers/query/planning.rs::run_unified_with` and `crates/eg-plan/src/exec.rs::execute`.
The mixed query must reach `crates/eg-plan/src/exec.rs::traverse_op` and `crates/eg-plan/src/exec/vector_rank.rs::rank_top`.
The filtered scan benchmark must reach that vector path and `crates/eg-plan/src/exec.rs::rank_text` with real maintained indexes.
Record corpus digest, query workload, source revisions, index generations, benchmark environment, measured latency and oracle results.
The original R040 contract requires the comparison; it supplies no numerical latency target or claimed speedup.
These residual acceptance scenarios have not been executed. Existing parent receipts do not establish acceptance for the new leaf IDs.

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
Unavailable production bindings prevent served acceptance. Helper-only checks do not establish served acceptance.

## R084 planned ShEx acceptance

These are proposed tests in tests/shex_served.rs. None supplies execution evidence in this definition batch.
Each child has a focused case. The rollup requires every child and the served journey.

| Requirement | Planned proving test |
|---|---|
| MEDULLA-FEDERATED-QUERY-R084 | `served_shex_contract_journey` |
| MEDULLA-FEDERATED-QUERY-R084.1 | `wire_report_preserves_shape_map` |
| MEDULLA-FEDERATED-QUERY-R084.2 | `inline_and_live_data_selection` |
| MEDULLA-FEDERATED-QUERY-R084.3 | `bounded_constraints_report_results` |
| MEDULLA-FEDERATED-QUERY-R084.4 | `semantic_limits_remain_visible` |
| MEDULLA-FEDERATED-QUERY-R084.5 | `real_dispatch_reaches_shex_validator` |
| MEDULLA-FEDERATED-QUERY-R084.6 | `parse_and_dispatch_failures_are_distinct` |
| MEDULLA-FEDERATED-QUERY-R084.7 | `live_validation_obeys_read_authority` |
| MEDULLA-FEDERATED-QUERY-R084.8 | `served_shex_contract_journey` |

Start a real authenticated engine with the shex handler enabled and disposable durable storage.
Send actual ShexValidate requests through the existing native protocol.
Keep transport, authorization, dispatch, data export and eg-shex validation real.
Observe handle_shex_validate, validate and shex_report_wire without substituting their results.
Record source SHA, binary digest, features, request identity, graph scope and terminal result.

Use a ShExJ schema with two focus nodes, one conforming and one nonconforming.
Assert result order, lexical node identities, labels, per-node reasons and the aggregate.
Repeat duplicate pairs and an empty map. Empty-map success must carry no whole-graph validation claim.
Check START and unknown-label outcomes separately from malformed JSON.
Submit malformed Turtle and an explicitly unsupported expression type.
Exercise a real live-export error or retain its missing proof as an open acceptance item.
A build without the handler must fail dispatch, not return a synthetic conforming report.
Use existing approved build infrastructure for that case; no feature change is requested here.

For live selection, write known authorized graph data and send empty and whitespace-only data_graph.
Compare with a matching inline Turtle request, then a different inline graph.
Prove inline data does not import rows or change stored schema.
Use a second graph and hidden rows to test the authority projection through dispatch.
Read graph and schema state independently before and after successful and refused requests.
A denied request must not disclose hidden triples through a reason or result.

Keep constraint tests bounded: node kinds, datatypes, a value set, facets, cardinality and closed/extra behavior.
Distinguish sequential EachOf from the first-success OneOf implementation.
Include shared-predicate, inverse and recursion-boundary fixtures as explicit semantic-limit evidence.
Record both expected semantics and actual results. Do not rewrite the oracle to hide a discrepancy.
Those cases cannot certify unsupported full-language behavior.
Crate parser tests cannot stand in for served ShExJ syntax, authorization or report projection.

## R085 planned evidence

The following are proving targets, not execution receipts. Runtime delivery requires the real producer and the linked Graft SDK consumer.

- **MEDULLA-FEDERATED-QUERY-R085:** All child tests and linked consumer evidence. Each child must pass; these definitions do not establish runtime delivery.
- **MEDULLA-FEDERATED-QUERY-R085.1:** `crates/eg-types/src/source_query.rs::tests::approval_records_round_trip` (planned). Round trips preserve every binding. Invalid references, parameter types, nonfinite values, absent snapshot identity and zero/unbounded limits refuse.
- **MEDULLA-FEDERATED-QUERY-R085.2:** `src/server/handlers/federation/source_query_tests.rs::approval_requires_authority_and_native_commit` (planned). A successful response follows durable issuance. Generic lease writes cannot forge approval. Issuance uses existing storage and credentials; no external SQL executes during approval.
- **MEDULLA-FEDERATED-QUERY-R085.3:** `src/server/handlers/federation/source_query_tests.rs::changed_or_revoked_approval_refuses` (planned). Changing any binding, authority, source or revision refuses. Revoked, expired, consumed, unavailable or missing leases refuse. Verification grants no SQL effect and creates no bearer credential.
- **MEDULLA-FEDERATED-QUERY-R085.4:** `crates/eg-plan/src/federation_tests.rs::approved_source_query_has_no_write_effects` (planned). Native parameter binding preserves values and query meaning. Supported projections, predicates, ordering and limits match the query oracle. Unknown dialects, functions or effects refuse without issuing approval.
- **MEDULLA-FEDERATED-QUERY-R085.5:** `tests/source_query_approval_served.rs::real_dispatch_issues_and_verifies_approval` (planned). Real authenticated dispatch reaches source validation, native issuance and fresh verification. No parallel planner, store or client-side approval authority appears. Current source has neither method.
- **MEDULLA-FEDERATED-QUERY-R085.6:** `tests/source_query_approval_served.rs::bounded_stream_preserves_approval` (planned). Oversized rows, exceeded budgets or failed revalidation stop execution and close the driver. Exactly one explicit completion or typed failure stops an execution. Missing terminal completion never means success.
- **MEDULLA-FEDERATED-QUERY-R085.7:** `tests/source_query_approval_served.rs::failure_after_three_chunks_is_terminal` (planned). Failure after three chunks stays failure with an incomplete execution identity. No successful terminal event or failed-chunk checkpoint follows. Earlier accepted pages retain their actual durable receipts.
- **MEDULLA-FEDERATED-QUERY-R085.8:** `tests/source_query_approval_served.rs::approval_to_connector_query_journey` (planned). Keep row widths, driver settings and all limits fixed while increasing cardinality by two orders of magnitude. Declare the baseline, memory envelope and tolerance before runs. Every peak must fit that fixed envelope; whole-result buffering fails. Record exact revisions, features, commands, rows/bytes, queue high-water marks, terminal outcomes and checkpoint agreement.

<!-- spec-definition-patch:owner-batch-20261012-federated-query-and-reasoning -->
## Source-backed acceptance targets and explicit holds — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`MEDULLA-FEDERATED-QUERY-R070.1` / AU:71. Source `0f1b775fd66c41586bdcdf0047a9ccbc52e38aa1bc21a4e8234e9c641e45c68e`:** Definition acceptance is blocked until the owner publishes the producer fields above. Serve an authorized query over an unindexed corpus. Return exact lexical hits, bounded file spans, verification result, source revision and measured cost. An over-ceiling query must return typed abstention before reading beyond the ceiling. Repeat against an indexed-only fixture and prove it is not accepted as this contract. Do not claim a served test or implementation now.

- **`MEDULLA-FEDERATED-QUERY-R006.1` / EG:11. Source `2c2ffcf18d947730bc60a935fed942e1f82e9e8547f4509fe2941a34672fb8f3`:** After owner decisions are recorded, acceptance uses two source facts with sentence and axiom origins. It exercises matching and nonmatching temporal intervals. Read back the derived fact with rule digest, graph generation and complete origin refs. Missing/stale origins, unsupported rule versions, conflicting validity windows and unauthorized writes must refuse without changing graph revision. Until then no producer test is specified as executable.

- **`MEDULLA-FEDERATED-QUERY-R040.5` / EG:20. Source `07df52b453f0f1b7be7aec7bf790f087c3fc5e13500915011382a9fcd8d9d41a`:** Blocked until the owner binds admission, relation, retention and benchmark semantics. The benchmark compares fixed query-selectivity workloads at multiple corpus sizes. It reports rows/bytes/visits and demonstrates the declared cost bound. Read back new knowledge; prove prior records and model revision remain unchanged. Missing admission authority, relation target or bound refuses. No runnable acceptance is asserted before the missing policy is supplied.

- **`MEDULLA-FEDERATED-QUERY-R040.6` / EG:21. Source `c321db3aa79c29d891456f2bee98f545611b8d1be7c93d299b939c3d73a40279`:** After the owner fixes those decisions, acceptance ingests two ordered sessions. It builds and independently reads back an episode with transcript/revision/span provenance. It searches for the episode using hybrid vector+keyword search. Validate event time against storage time. Refuse or report partial for duplicate/conflicting chronology, stale transcript revision or unauthorized cross-session reference. Apply the owner-approved rule. No executable test exists until taxonomy and consumer are fixed.

- **`MEDULLA-FEDERATED-QUERY-R083.7` / EG:22:** Extend existing R083 behavior/model/refusal acceptance with the support-margin inputs and invariant that unsupported candidates lose. Add a served counterexample with a higher-confidence unsupported candidate and an evidence-supported lower-confidence candidate. Retain current two-candidate, class, abstention and evidence obligations. Producer/consumer path: Planned MERISTEM-RETRIEVAL-R006 prior probe -> served cross-modal query -> eg_plan::execute -> arbitrate -> ArbitrationVerdict -> harness. R083.3/.6 identify the producer path. This audit does not claim it is implemented. Source review `544b7b0054e8c7cd9a76aedd0d665589d555a0bcf20d1f00679442574936af36`. Exercise the served Medulla producer and named consumer where that owner contract exists. Assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`MEDULLA-FEDERATED-QUERY-R081.10` / EG:25. Source `fb8d54d8554b8158da64f7c9e8bf02ea846e5b411768a0381c2682b2e6302dac`:** Once defined, serve a fixture where all three clocks disagree and another where one clock is absent. Assert the selected basis and its provenance, and trigger exactly the configured bounded fallback only below the declared sufficiency threshold. enough first-pass context must issue no second pass. Stale or conflicting clock provenance and exhausted query budget must return a typed partial/refusal with no unbounded retry. No executable acceptance is currently authorized.

- **`MEDULLA-FEDERATED-QUERY-R028.1` / EG:29. Source `2eb8aadb1b6ccc2d5de77a4d1964fc04a4e944bccf9bcc1793e262f146af7583`:** Create one plain row and one row with resolved evidence/policy frames. Compare both with the same structured KnowledgeSet producer. Verify row id/score/projection are unchanged and both frame states are row-local. Independently omit provenance, policy and snapshot binding. Each case must return the typed refusal without a partial row. Keep R045 foreign-row pushdown behavior unchanged. Tests are acceptance targets only. No UQL implementation or served receipt is claimed.

- **`MEDULLA-FEDERATED-QUERY-R005.1` / EG:31. Source `973de1c3b38646a438d057299d8070070f0ef7a2056bb30acff6d1a3814df513`:** For asymmetry, `(a,p,b)` alone is the clean control. Adding `(b,p,a)` creates a violation. It cites the declared asymmetric axiom, both edge ids and evidence refs at pinned graph/ontology revisions. A missing reverse edge is not a violation. Serve positive/control pairs for temporal conflict, cardinality, self-loop and ontology defect. Combine two independent defects and require both diagnostics. Refuse missing kind-specific fields, unknown kinds and stale ontology revisions. Read back graph state to prove validation made no mutation. Retraction remains denied absent explicit authorization.

- **`MEDULLA-FEDERATED-QUERY-R078.7` / EG:59. Source `fd5990a7c9f30e1e1aa0eed9aa08efef4e2ebc8f5f34387598f1c9e577c458dc`:** With category mappings supplied, serve identical company-context queries as two tenants with differing clearances. Verify the authorized tenant receives each mapped category as source-bound references. Exclude denied rows before any source read and copy no source record. Join native time-series values only by the declared series/time key and snapshot. Missing/ambiguous mapping, unverified tenant, stale source revision or cross-tenant join refuses. Current scope-planning acceptance remains covered by R078.1-.6. This new composed view stays held until mappings are exact.

- **`MEDULLA-FEDERATED-QUERY-R081.9.7.7` / GRAPHOS:24. Source `b2e5de8386b3ca72b59a81ca5fa3193eaf7736af86aca472a720b9e4511961c0`:** Serve all three granularities over a nested authorized section tree. Compare selected path/span ids with an independent tree oracle. Instrument external model, embedding and vector calls and require zero calls on every positive query. Malformed or stale section records and exhausted depth/visit/token limits refuse before extra dispatch. Tenant isolation and existing community traversal outputs remain unchanged. No runtime proof is claimed.

- **`MEDULLA-FEDERATED-QUERY-R081.9.7.8` / GRAPHOS:25. Source `6797cff839c16c3b9c3877d6bbf48bf8953eae8696087abc99f2b57147daed93`:** Once the owner supplies TOC generation and versioned positive finite `max_lines` and `max_bytes`, index a multisection fixture offline. Read a bounded middle span and verify exact line numbers and source digest. Refuse stale document/TOC revision, invalid or too-wide range, a response above the bound, and whole-file requests. Verify current Arbora space/section links still resolve. Until producer and caps are named, no served acceptance is claimed.

- **`MEDULLA-FEDERATED-QUERY-R040.2.7` / GRAPHOS:28. Source `f7654cd07d4d95f32f09fec742c4172b35c689749b3228aef9424830b1f7cc1b`:** Serve the same corpus/backend with two candidates whose assignments differ. Independently recompute both topic components and final scores. Change only one candidate’s responsibility vector and prove only the expected candidate contribution/order changes. Refuse missing/duplicate document assignment, stale document or corpus revision, backend mismatch and embedding-space mismatch before truncation. Verify hard exclusions still hold and `run_unified_with` records the assignment/backend revision in the existing topic receipt. No implementation or served receipt is claimed.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run. Names describe acceptance obligations, not evidence.

- **`MEDULLA-FEDERATED-QUERY-R081.8.100` (AU:77. Source `213700ec5b9e8648657ec47d0c587483a978db10f4661475b467be451d3bb2e1`):** Same-retriever feedback reuse succeeds. Retriever-version changes require validation. Failed, stale or missing transfer receipts refuse retrieval. Verify the receipt binds the strategy digest and target retriever. Keep feedback budgets unchanged. Rejected critiques must not change the active strategy. Producer/consumer boundary: Authenticated crossmodal -> `eg-plan::exec` R081.8 feedback -> selected retriever. Preserve existing R081.8.1-.6 tiering, feedback and refusal behavior. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only. No implementation or receipt is asserted.

- **`MEDULLA-FEDERATED-QUERY-R053.100` (EG:37. Source `803f4e4ffbcd939af750e23451469835555beaa37e14ce555ffd0a59c0a3ac48`):** Build-time test enters the Cypher query-construction path. Check generated property keys for plain, reserved, spaced, Unicode and embedded-backtick names. Parse safe output against the Cypher grammar. An unquoted or malformed generated key fails the build. Keep values parameterized, preserve ForeignSourceSpec registration and Spark batch-only capability, and add no served API or runtime receipt. Gate evidence binds renderer source revision, fixture digest and test result.

- **`MEDULLA-FEDERATED-QUERY-R032.100` (AU:43. Source `327ced0e0605f34366c662017d82ac892a481c5ca5f40bba2f3693ba6a5f7649`):** Graph fixtures cover ancestors and descendants across three layers. Include differing historical failure counts, a cycle boundary and missing-history data. Verify both directions and deterministic layer/history contribution. Refuse stale or malformed history, and compare existing R032 propagation outputs unchanged. Producer/consumer boundary: Medulla owns graph impact query/risk decision; Meristem requests and consumes the result. Existing numeric risk helpers are not this two-direction historical impact contract. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only. No implementation or receipt is asserted.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run. They describe required future acceptance, not evidence.

- **`MEDULLA-FEDERATED-QUERY-R086.100` / EG:14. Source `7223250e7f899a187aad0b152e597c6174b772c49779ca2bdd12d3ba68f4f5ef`:** Acceptance is blocked until model, evidence-class, authorization and Meristem receipt decisions are published. A served ordered three-source fixture includes an unseen-at-fit entity. Return candidates with tuple order, graph/model/policy revisions and evidence class. Leave graph revision unchanged. Changing source order must change request digest. Refuse unary tuples, unresolved/unauthorized sources, stale revisions and malformed relation constraints. Confirm `GraphLearnPredict` binary results and write authority are unchanged. No producer test is currently defined as runnable. Parent `MEDULLA-FEDERATED-QUERY-R086` is linked to this sole required child. Both remain SPECIFIED.

- **`MEDULLA-FEDERATED-QUERY-R081.101` / AU:126. Source `deca151180a749cb3784d9946da0de17205eca89c92e5eb4f275859e1d4be165`:** Build state once, then rerank under two query contexts and reversed candidate order. Assert the state digest stays identical and per-document scores remain aligned. Assert no decoder/model-generation call. Compare measured interaction cost with the same full-token baseline. Refuse stale document/encoder revisions, malformed segment coverage or any query-time state rewrite. Producer/consumer: Medulla retrieval owns document-state construction and PlanCtx reranking. Query embeddings/context remain query inputs and do not alter the reusable state. Preserve existing FQR R081 tiering and selection bounds. Parent `MEDULLA-FEDERATED-QUERY-R081` remains intact.

- **`MEDULLA-FEDERATED-QUERY-R081.102` / GRAPHOS:27. Source `baac22c9fb42470957b5b91499c6e42e1bb2410d24446d15f6f364638c88138e`:** Distill and read back an artifact meeting held-out quality threshold. Serve queries and assert zero model-service calls plus scorer-digest binding. Below-threshold, missing held-out evidence, stale corpus/index or teacher revision all refuse activation. Verify the prior scorer remains active. Producer/consumer: Medulla owns retrieval learning/ranking and the stored scorer. Meristem may execute evaluation. Arbora catalog ranking is separate. Use the existing authenticated UnifiedQuery -> run_unified_with planning route. Do not invent a public operation name. Parent `MEDULLA-FEDERATED-QUERY-R081` remains intact.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mapping-eg41-20261012 -->
## Mapped retrieval-selection acceptance — 2026-10-12

No case listed here has run.

- **`MEDULLA-FEDERATED-QUERY-R081.100` / EG:41. Source `bceeed106b5db2933cf5161d58b4203fc7afe9d714503a31d68323992906c23b`:** Compare served retrieval on a tiny fixture with an independent exhaustive nonnegative-sparse oracle. Include two highly correlated documents and an alternative diverse pair. Assert exact selection, coefficients, residual and deterministic order. Refuse embedding-space mismatch, invalid policy and non-convergence. Verify existing tenant and result-count filters still apply. Producer/consumer: Medulla retrieval selection owns the joint objective and set result. The existing served UnifiedQuery/retrieval path consumes it. Preserve R081.1-.9 selection and tenant bounds. No new caller-controlled score authority. Parent `R081` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — viewport query

No test in this section has run.

- `MEDULLA-FEDERATED-QUERY-R081.103`: serve scatter and line variants over a real indexed source of at least 1,000,000,000 points. Verify viewport-derived bin caps and `min(requested_byte_ceiling, formula_cap)` encoded-byte behavior at minimum/maximum sizes, source/cardinality/index revision and line gaps. Missing, invalid and too-small requested ceilings refuse before scanning. Pan/zoom changes query digest. Exact-point and aggregate-bin selections remain revision-bound. Stale index/source, denied selection, invalid/exceeded cap and any full-source transfer fail acceptance.
- Pan/zoom and exact/aggregate selection prove viewport-bound tokens. Missing index, stale revision, denied point/bin and invalid/exceeded caps refuse. Verify no full-source payload or silently dropped bins.
<!-- /spec-definition-patch -->

## MEDULLA-FEDERATED-QUERY-R058.4 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-FEDERATED-QUERY-R058.4.1` | A serde round trip keeps every field and rejects `max_steps = 5`. |
| Behavior | `MEDULLA-FEDERATED-QUERY-R058.4.2` | Unit tests cover linking, each dialect choice and one corrected step. |
| Wiring | `MEDULLA-FEDERATED-QUERY-R058.4.3` | A dispatch test observes one schema read per request. |
| Refusal | `MEDULLA-FEDERATED-QUERY-R058.4.4` | Each case returns its code and no rows. |
| Evidence | `MEDULLA-FEDERATED-QUERY-R058.4.5` | A test reads the step list and the digest. |
| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R058.4.6` | The served test passes, and the measured overhead meets the budget. |

Snapshot grounding acceptance for `MEDULLA-FEDERATED-QUERY-R058.4.7.1` uses the `eg-plan` NL context suite. It covers full-snapshot discovery beyond 4096 nodes, attribution property repairs, named-source authority, JOIN scope and mixed fusion scope. Feature-enabled tests check source scope replacement and spatial/tensor layers. Served tests must show that each caller sees only names from its owner-authorized registry.

`MEDULLA-FEDERATED-QUERY-R058.4.7.2` remains open. Acceptance must cover the native identifier schemas named in that row. The parent `MEDULLA-FEDERATED-QUERY-R058.4.7` remains open until both children complete.

Ontology-name acceptance for `MEDULLA-FEDERATED-QUERY-R058.4.8` uses the mapped-property and ambiguity tests in the same suite.

`MEDULLA-FEDERATED-QUERY-R058.4.9` remains open. Its current negative test refuses an inline HTTP adapter before any source can run. Completing it requires positive tests for a schema-bound adapter and cross-owner refusal.

Hot-path proof for `MEDULLA-FEDERATED-QUERY-R058.4`: `tests/nl_query_bounded_steps.rs::second_step_corrects_parse_error`.

## MEDULLA-FEDERATED-QUERY-R088 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-FEDERATED-QUERY-R088.1` | A serde round trip keeps every field and the default row shape. |
| Behavior | `MEDULLA-FEDERATED-QUERY-R088.2` | A unit test shows that hidden rows do not displace visible rows. |
| Wiring | `MEDULLA-FEDERATED-QUERY-R088.3` | A dispatch test observes one snapshot for rows and summaries. |
| Refusal | `MEDULLA-FEDERATED-QUERY-R088.4` | The test asserts the code and no partial rows. |
| Evidence | `MEDULLA-FEDERATED-QUERY-R088.5` | A test reads the header fields. |
| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R088.6` | The served test passes, and the measured cost meets the budget. |

Hot-path proof for `MEDULLA-FEDERATED-QUERY-R088`: `tests/uql_governed_summary.rs::governed_rows_carry_summary`.

## MEDULLA-FEDERATED-QUERY-R089 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-FEDERATED-QUERY-R089.1` | A serde round trip keeps every field and rejects an unknown failure mode. |
| Behavior | `MEDULLA-FEDERATED-QUERY-R089.2` | Unit tests cover each failure mode. |
| Wiring | `MEDULLA-FEDERATED-QUERY-R089.3` | A dispatch test reads the policy node and returns the verdict. |
| Refusal | `MEDULLA-FEDERATED-QUERY-R089.4` | Tests assert the code and a failed verdict with rows. |
| Evidence | `MEDULLA-FEDERATED-QUERY-R089.5` | A test reads the three fields. |
| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R089.6` | The served test passes, and the measured cost meets the budget. |
| Behavior | `MEDULLA-FEDERATED-QUERY-R089.7` | `crates/eg-plan/src/retrieval_verdict.rs::tests::drift_mode_reads_monitor_and_names_adapter` sets `drift` and names the adapter and shard generations. |

Hot-path proof for `MEDULLA-FEDERATED-QUERY-R089`: `tests/discover_quality_verdict.rs::sparse_index_fails_closed`.

## MEDULLA-FEDERATED-QUERY-R090 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-FEDERATED-QUERY-R090.1` | A serde round trip keeps every field and rejects `hops = 3`. |
| Behavior | `MEDULLA-FEDERATED-QUERY-R090.2` | Unit tests cover fusion, decay, the neutral date and the frontier cap. |
| Wiring | `MEDULLA-FEDERATED-QUERY-R090.3` | A dispatch test observes one planner call and batched neighbor reads. |
| Refusal | `MEDULLA-FEDERATED-QUERY-R090.4` | Each case returns its code and no rows. |
| Evidence | `MEDULLA-FEDERATED-QUERY-R090.5` | A test reads each field. |
| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R090.6` | The served test passes, and the benchmark receipt meets the budget. |
| Wiring | `MEDULLA-FEDERATED-QUERY-R090.7` | `tests/discover_planning_served.rs::deep_arms_use_scoped_adapter` observes one adapter application per arm and refresh provenance. |

Hot-path proof for `MEDULLA-FEDERATED-QUERY-R090`: `tests/discover_planning_served.rs::deep_recency_multihop`.

## MEDULLA-FEDERATED-QUERY-R073.2 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Evidence | `MEDULLA-FEDERATED-QUERY-R073.2` | `crates/eg-plan/src/federation_opt/tests.rs::protocol_matrix_bounds_pushdown`: A unit test fails when a declared protocol lacks a row. It refuses a pushdown kind outside the row. |

## MEDULLA-FEDERATED-QUERY-R091 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `MEDULLA-FEDERATED-QUERY-R091.1` | `crates/eg-plan/src/federation_opt/plan_evolution.rs::tests::observation_store_bounds`: A serde round trip keeps every field and rejects an unknown kind; the store evicts at its bound. |

| Behavior | `MEDULLA-FEDERATED-QUERY-R091.2` | `crates/eg-plan/src/federation_opt/plan_evolution.rs::tests::keep_or_revert_by_measurement`: A faster equal candidate is kept, and a slower or unequal candidate is reverted. |

| Wiring | `MEDULLA-FEDERATED-QUERY-R091.3` | `crates/eg-plan/src/federation_opt/plan_evolution.rs::tests::served_path_records_and_reads`: A served query records one observation, and a later query uses the kept plan. |

| Refusal | `MEDULLA-FEDERATED-QUERY-R091.4` | `crates/eg-plan/src/federation_opt/plan_evolution.rs::tests::evolution_refusals`: Each case returns its code, and the active plan stays the baseline. |

| Evidence | `MEDULLA-FEDERATED-QUERY-R091.5` | `crates/eg-plan/src/federation_opt/plan_evolution.rs::tests::receipt_names_measurements`: A served read finds the receipt linked to the shape. |

| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R091.6` | `tests/federation/test_plan_evolution_served.py::test_kept_plan_measurably_faster_and_mismatch_reverts`: The kept plan improves p50 by 10% or more, the unequal candidate reverts, and the overhead meets the budget. |


Hot-path proof for `MEDULLA-FEDERATED-QUERY-R091`: `tests/federation/test_plan_evolution_served.py::test_kept_plan_measurably_faster_and_mismatch_reverts`.

## MEDULLA-FEDERATED-QUERY-R653.7 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Citation | `MEDULLA-FEDERATED-QUERY-R653.7` | `tests/retrieval/test_web_citation_served.py::test_hybrid_hit_cites_source_url`: A served fused hit cites the URL, the retrieval time and the query. |

## Context engine and governance query rows

| Row | Proving test |
|---|---|
| `MEDULLA-FEDERATED-QUERY-R703` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R703.1` | `crates/eg-plan/tests/access_check.rs::request_round_trips` |
| `MEDULLA-FEDERATED-QUERY-R703.2` | `crates/eg-plan/tests/access_check.rs::timeout_drops_hits` |
| `MEDULLA-FEDERATED-QUERY-R703.3` | `tests/retrieval/test_access_check_served.py::test_denied_hit_never_enters_bundle` |
| `MEDULLA-FEDERATED-QUERY-R703.4` | `tests/retrieval/test_access_check_served.py::test_missing_port_fails_closed` |
| `MEDULLA-FEDERATED-QUERY-R703.5` | `tests/retrieval/test_access_check_served.py::test_drop_reasons_read_back` |
| `MEDULLA-FEDERATED-QUERY-R703.6` | `cargo bench -p eg-plan --bench access_check` |
| `MEDULLA-FEDERATED-QUERY-R704` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R704.1` | `crates/eg-plan/tests/knowledge_scope.rs::scope_round_trips` |
| `MEDULLA-FEDERATED-QUERY-R704.2` | `crates/eg-plan/tests/knowledge_scope.rs::scope_prefilters_before_truncation` |
| `MEDULLA-FEDERATED-QUERY-R704.3` | `tests/retrieval/test_knowledge_scope_served.py::test_agent_sees_only_scoped_sources` |
| `MEDULLA-FEDERATED-QUERY-R704.4` | `tests/retrieval/test_knowledge_scope_served.py::test_scope_refusals` |
| `MEDULLA-FEDERATED-QUERY-R704.5` | `tests/retrieval/test_knowledge_scope_served.py::test_scope_digest_recorded` |
| `MEDULLA-FEDERATED-QUERY-R704.6` | `cargo bench -p eg-plan --bench knowledge_scope` |
| `MEDULLA-FEDERATED-QUERY-R705` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R705.1` | `crates/eg-plan/tests/feedback_boost.rs::feedback_round_trips` |
| `MEDULLA-FEDERATED-QUERY-R705.2` | `crates/eg-plan/tests/feedback_boost.rs::boost_decays_and_clamps` |
| `MEDULLA-FEDERATED-QUERY-R705.3` | `tests/retrieval/test_feedback_boost_served.py::test_upvote_raises_rank` |
| `MEDULLA-FEDERATED-QUERY-R705.4` | `tests/retrieval/test_feedback_boost_served.py::test_feedback_refusals` |
| `MEDULLA-FEDERATED-QUERY-R705.5` | `tests/retrieval/test_feedback_boost_served.py::test_boost_listed_per_hit` |
| `MEDULLA-FEDERATED-QUERY-R705.6` | `cargo bench -p eg-plan --bench feedback_boost` |
| `MEDULLA-FEDERATED-QUERY-R706` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R706.1` | `crates/eg-plan/tests/policy_mask.rs::policies_round_trip` |
| `MEDULLA-FEDERATED-QUERY-R706.2` | `crates/eg-plan/tests/policy_mask.rs::one_rewrite_serves_every_language` |
| `MEDULLA-FEDERATED-QUERY-R706.3` | `tests/test_policy_mask_served.py::test_masked_column_over_sql_and_uql` |
| `MEDULLA-FEDERATED-QUERY-R706.4` | `tests/test_policy_mask_served.py::test_mask_refusals` |
| `MEDULLA-FEDERATED-QUERY-R706.5` | `tests/test_policy_mask_served.py::test_explain_lists_policies` |
| `MEDULLA-FEDERATED-QUERY-R706.6` | `cargo bench -p eg-plan --bench policy_mask` |
| `MEDULLA-FEDERATED-QUERY-R707` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R707.1` | `crates/eg-rdf/tests/sparql_update.rs::update_forms_parse` |
| `MEDULLA-FEDERATED-QUERY-R707.2` | `crates/eg-rdf/tests/sparql_update.rs::multi_operation_update_is_atomic` |
| `MEDULLA-FEDERATED-QUERY-R707.3` | `tests/test_sparql_update_served.py::test_insert_then_select` |
| `MEDULLA-FEDERATED-QUERY-R707.4` | `tests/test_sparql_update_served.py::test_update_refusals` |
| `MEDULLA-FEDERATED-QUERY-R707.5` | `tests/test_sparql_update_served.py::test_update_receipt_reads_back` |
| `MEDULLA-FEDERATED-QUERY-R707.6` | `cargo bench -p eg-rdf --bench sparql_update` |
| `MEDULLA-FEDERATED-QUERY-R708` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R708.1` | `crates/eg-plan/tests/stored_query.rs::stored_query_round_trips` |
| `MEDULLA-FEDERATED-QUERY-R708.2` | `crates/eg-plan/tests/stored_query.rs::on_change_refreshes` |
| `MEDULLA-FEDERATED-QUERY-R708.3` | `tests/test_stored_query_served.py::test_put_then_call` |
| `MEDULLA-FEDERATED-QUERY-R708.4` | `tests/test_stored_query_served.py::test_stored_query_refusals` |
| `MEDULLA-FEDERATED-QUERY-R708.5` | `tests/test_stored_query_served.py::test_staleness_reported` |
| `MEDULLA-FEDERATED-QUERY-R708.6` | `cargo bench -p eg-plan --bench stored_query` |
| `MEDULLA-FEDERATED-QUERY-R709` | See each child. |
| `MEDULLA-FEDERATED-QUERY-R709.1` | `crates/eg-rdf/tests/geosparql.rs::functions_registered` |
| `MEDULLA-FEDERATED-QUERY-R709.2` | `crates/eg-rdf/tests/geosparql.rs::topology_matches_kernel` |
| `MEDULLA-FEDERATED-QUERY-R709.3` | `tests/test_geosparql_served.py::test_within_query` |
| `MEDULLA-FEDERATED-QUERY-R709.4` | `tests/test_geosparql_served.py::test_geo_refusals` |
| `MEDULLA-FEDERATED-QUERY-R709.5` | `tests/test_geosparql_served.py::test_explain_names_spatial_index` |
| `MEDULLA-FEDERATED-QUERY-R709.6` | `cargo bench -p eg-rdf --bench geosparql` |

## Shared query workspace

| Row | Proving test |
|---|---|
| `MEDULLA-FEDERATED-QUERY-R708.7` | `tests/test_stored_query_served.py::test_workspace_share_schedule_and_lint` |

## Agent-memory substrate claims (2026-10-22)

| Case | Requirement | Proving test |
|---|---|---|
| One UQL plan across graph, vector, SQL, SPARQL and epistemic stages (rollup) | `MEDULLA-FEDERATED-QUERY-R720` | Rollup: satisfied when the test of every child passes. |
| Epistemic row fields | `MEDULLA-FEDERATED-QUERY-R720.1` | `crates/eg-plan/src/rowset.rs::tests::epistemic_fields_pass_through` |
| Confidence, bitemporal and time-decay stages | `MEDULLA-FEDERATED-QUERY-R720.2` | `crates/eg-plan/tests/uql_epistemic_stages.rs::confidence_asof_decay` |
| Embedding-similarity clustering stage | `MEDULLA-FEDERATED-QUERY-R720.3` | `crates/eg-plan/tests/uql_epistemic_stages.rs::cluster_by_embedding_is_deterministic` |
| Wiring | `MEDULLA-FEDERATED-QUERY-R720.4` | `tests/uql_single_plan_all_modalities.rs::served_program_runs_one_plan` |
| Refusal | `MEDULLA-FEDERATED-QUERY-R720.5` | `crates/eg-plan/tests/uql_epistemic_stages.rs::decay_and_cluster_refusals` |
| Hot-path proof | `MEDULLA-FEDERATED-QUERY-R720.6` | `tests/uql_single_plan_all_modalities.rs::five_stage_kinds_one_plan` |
| Modalities are views over one RowSet algebra in one store (rollup) | `MEDULLA-FEDERATED-QUERY-R721` | Rollup: satisfied when the test of every child passes. |
| Each modality implements the modality contract | `MEDULLA-FEDERATED-QUERY-R721.1` | `crates/eg-modality/tests/rowset_views.rs::seven_modalities_implement_contract` |
| Wiring and store authority | `MEDULLA-FEDERATED-QUERY-R721.2` | `tests/test_store_registry_modalities.py::test_every_modality_has_one_redb_authority` |
| Refusal | `MEDULLA-FEDERATED-QUERY-R721.3` | `crates/eg-plan/tests/uql_epistemic_stages.rs::unknown_modality_refuses` |
| Hot-path proof: cross-modal queries with default configuration | `MEDULLA-FEDERATED-QUERY-R721.4` | `tests/advanced_crossmodal_roundtrip.rs::fresh_engine_answers_cross_modal_query` |

- `MEDULLA-FEDERATED-QUERY-R722`: See each child.
- `MEDULLA-FEDERATED-QUERY-R722.1`: `crates/eg-types/tests/pack_source.rs::access_kinds_map_to_foreign_source_kinds`.
- `MEDULLA-FEDERATED-QUERY-R722.2`: `crates/eg-plan/tests/pack_source.rs::held_serves_none_approved_serves_rows_and_changed_contract_returns_to_held`; `src/server/handlers/admin/connector_pack/tests/source_approval.rs::approve_source_requires_current_head_and_contract_digest` proves the admitted approval CAS; `tests/federated/test_pack_source_served.py::test_import_registers_through_the_one_catalog` proves the served catalog import.
- `MEDULLA-FEDERATED-QUERY-R722.4`: `tests/federated/test_pack_source_served.py::test_secret_guard_held_and_unavailable_refuse_with_named_codes`.
- `MEDULLA-FEDERATED-QUERY-R722.5`: `tests/federated/test_pack_source_served.py::test_rows_carry_contract_provenance_and_nothing_is_copied`.
- `MEDULLA-FEDERATED-QUERY-R722.6`: `tests/federated/test_pack_source_served.py::test_uql_joins_native_fact_to_live_connector_rows_with_zero_copy`.

## MEDULLA-FEDERATED-QUERY-R058.5 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Behavior | `MEDULLA-FEDERATED-QUERY-R058.5` | `nl_regressions::planner_sends_configured_extra_body_and_honours_read_timeout` sees the extra body at a stub server and a timeout inside 3 s. |
| Default | `MEDULLA-FEDERATED-QUERY-R058.5` | `nl_regressions::planner_request_defaults_disable_thinking` sees `enable_thinking=false` in the body. |
| Refusal | `MEDULLA-FEDERATED-QUERY-R058.5` | `nl_regressions::planner_refuses_invalid_request_config_with_typed_errors` returns each `NlConfigError` variant. |
| Served refusal | `MEDULLA-FEDERATED-QUERY-R058.5` | `nl_config_refusal_tests::invalid_planner_config_is_refused_with_code_and_key` sees `ENGINE_UNAVAILABLE` and the bad key in the detail. |
| Raw value | `MEDULLA-FEDERATED-QUERY-R058.5` | `nl::tests::unparseable_env_values_are_refused_with_source_and_raw_value` sees the env name or config key and the raw text in the detail. |

### R722.1 import-held registration slice

- `source::tests::access_kinds_map_to_foreign_source_kinds` checks the closed source kinds.
- `sources::tests::imported_contract_is_held_and_identifies_its_authoritative_body` checks source claims and absent declarations.
- `sources::tests::publisher_approval_and_literal_credentials_refuse` checks admission authority and credential references.
- `ontology::source_claims_persist_held_and_old_or_other_tenant_members_stay_unbound` exercises signed import, tenant isolation, unchanged import and store reopen.

These tests do not prove live source execution or completion of R722.2 through R722.6.

### R722.2 admitted source approval contract

- `src/server/handlers/admin/connector_pack/tests/source_approval.rs::approve_source_requires_current_head_and_contract_digest` sends the existing `ConnectorPackOp::ApproveSource` / `approve_source` operation with a read-only admitted context. It proves the `agent:pack-control` action, tenant/current-head/body-digest CAS, and the approved `ConnectorPackSourceApprovalResult` batch evidence.
- The same test retries an exact approved target and gets the idempotent result. It then imports a changed contract and proves the new current member is held. The old approval cannot be reused. It also refuses an absent registration, stale `PackHeadRef`, and mismatched current member digest without changing the head.
- `tests/federated/test_pack_source_served.py::test_import_registers_through_the_one_catalog` remains the served catalog proof. Arbora's R042.2 policy id/reason is recorded by Arbora; EG's approval result carries only the engine-owned registration/head/digest and durable batch evidence.

This contract does not add a human approval step, a new capability scope, a publisher-supplied approval bit, a second source registry, or live driver execution. No acceptance claim is made until native execution and the served test pass.

### R722.3 owner-bound execution seam (tracked, pending)

The following matrix names the required evidence for the first runtime binding.
The tests are planned and remain open; the existing R722.1/R722.2 tests do not
prove any of these rows.

| Case | Requirement | Planned proving test | Required observation |
|---|---|---|---|
| Registry identity and precedence | R722.3 | `crates/eg-plan/src/federation_registry_tests.rs::eg722_bound_driver_resolves_naively_and_retains_spec`; `crates/eg-plan/src/federation_opt/remote.rs::bound_driver_tests::bound_driver_precedes_spec_and_preserves_identity` | The bound driver retains the registered source kind, operation and fingerprint. Naive and optimized resolution select the same bound source; the spec fallback is never used. |
| Registration and context binding | R722.3/R042/R085 | `src/server/foreign_catalog.rs::binding_requires_verified_owner_tenant_and_current_registration` | Verified context supplies tenant, owner, actor and delegation. Wrong tenant/owner, missing approval, stale head, changed body or contract digest refuse before a driver call. Revoked authority and changed plan/query digest also refuse. |
| Four-phase lifecycle | R722.3/R043.7; SDK-SOURCE-INGEST-R004.7-.10 invariants mapped by the SDK owner | `tests/federated/test_pack_source_served.py::test_approved_source_open_chunk_complete_abort_is_bounded` | `ForeignReadOpen` issues one opaque bound handle. Chunks use monotone sequence and finite row/byte/deadline caps. Complete or Abort is the only terminal transition. Replay, expiry, cancellation and over-limit cases return named errors. Existing R004.7-.10 alone do not prove a generic pack receiver. |
| Governed source call | R043.7/R722.3 | `tests/federated/test_pack_source_served.py::test_approved_source_reaches_fleet_call_once` | The served path calls `FleetSourceExecutor` through `fleet.call` with read effect and never opens a direct URL or second transport. |
| Provenance and zero copy | R722.5 | `tests/federated/test_pack_source_served.py::test_bound_source_rows_keep_provenance_without_source_storage` | Live rows carry connector, contract, mapping and fetch-time provenance. A store audit finds no source-row writes for a non-materialised source. |
| Refusal and partial availability | R722.4 | `tests/federated/test_pack_source_served.py::test_bound_source_refusals_and_unavailable_gap` | Held/unbound, forbidden destination and stale/revoked cases return stable typed errors. Source-down cases return an explicit source gap. Native rows remain correct. No silent empty result occurs. The path never falls back. |
| Cross-source hot path | R722.6 | `tests/federated/test_pack_source_served.py::test_uql_dag_joins_native_and_live_bound_rows` | Served UQL/DAG joins native facts and live connector rows. It preserves dependency concurrency and cancellation. It produces equal answers for naive and optimized resolution. |

No row above is an acceptance result yet. The named native and served tests must
run against the same reviewed source tree and record positive and refusal
evidence. R722.2 approval evidence remains separate from this runtime proof.

### R722.3 EG native leaf evidence (tracked, pending served integration)

These tests bind the current EG DTO, query-pin and mapped-cell implementation to
the R722.3 contract. They record evidence for the EG leaf only. They do not
mark the cross-owner rows above complete. They do not replace the served
`FleetSourceExecutor`/SDK tests.

| Case | Requirement | Proving test | Evidence boundary |
|---|---|---|---|
| Lifecycle DTO and bounded limits | R722.3 | `crates/eg-types/src/foreign_read.rs::tests::lifecycle_dtos_round_trip_with_digest_and_optional_provenance`; `::tests::bounded_limits_reject_zero_and_over_cap`; `src/server/foreign_read.rs::tests::foreign_read_rows_match_sdk_msgpack_golden_fixture` | MessagePack fields, mapped cells and provenance round-trip. Zero and over-cap limits refuse. Rust `rmp_serde` bytes and SHA match the connector SDK golden fixture. |
| Historical ExplainPlan compatibility | R722.3 | `crates/eg-types/src/protocol/support_01.rs::explain_plan_result_tests::historical_response_without_plan_digest_still_decodes` | A response without the additive optional digest decodes as `None`; a foreign client must fail closed. |
| Canonical plan/query binding pin | R722.3 | `src/server/foreign_read.rs::tests::canonical_plan_digest_is_the_query_binding_pin`; `src/server/handlers/query/planning.rs::foreign_query_digest_tests::uql_query_digest_binds_text_and_typed_parameters`; `src/server/handlers/query/uql_statement.rs::served_tests::explain_returns_server_owned_foreign_query_pins`; `src/server/handlers/query/uql_statement.rs::served_tests::prepare_returns_pins_for_the_exact_executable_text`; `tests/advanced_crossmodal_roundtrip.rs::explain_plan_surfaces_the_optimizer_rewrite` | The server helper and ExplainPlan use identical named-MessagePack bytes. Authenticated UQL `prepare=true` returns server-owned plan/query pins for the exact executable statement. This still does not prove a served host exchange or source approval. |
| Owner/sequence/expiry refusal | R722.3/R085 | `src/server/foreign_read.rs::tests::owner_digest_and_sequence_are_checked_before_page_acceptance`; adjacent abort/expiry tests in the same module | Wrong owner, digest or sequence refuse before page acceptance. Expired or terminal handles do likewise. Served paths must revalidate durable approval. |
| Existing mapped-cell projection | R722.3/R722.5 | `src/server/foreign_read.rs::tests::mapped_cells_use_existing_foreign_rows_residual_and_projection` | Existing `ForeignRows`/`ColumnPlan` keeps the approved `contract_iri` mapping and provenance through residual filtering and projection. Live connector rows and zero-copy store audit remain open. |

No row in this leaf promotes R722.3--R722.6. Record positive tests and refusal
cases against the same frozen source archive before composing this evidence.
