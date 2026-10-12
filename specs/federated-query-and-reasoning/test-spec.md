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
| `tests/retrieval/test_mixed_traversal_topk_served.py` — EG-FEDERATED-QUERY-R039.4 | Reuse the S-01 two-hop traversal and parallel-edge fixture. Include ten permitted matches below unfiltered top-k. Run authenticated mixed traversal-plus-vector queries against that reference corpus and require stable edge identity. Include a query whose first k similarity candidates fail the traversal filter but later candidates survive. The independent oracle must confirm exactly k permitted results when available. Otherwise, require an explicit typed shortfall naming the permitted match count. | Fail acceptance for a silent undercount or premature truncation when enough permitted matches exist. Reject a wrong shortfall count or an unauthorized result or count. Test fewer than k surviving candidates and zero survivors. |
| `tests/retrieval/test_filtered_scan_benchmark_served.py` — EG-FEDERATED-QUERY-R040.4 | Benchmark filtered ANN and BM25 latency before and after the filter-aware scan change. Use the same fixed corpus and query workload. Run the independent S-01 filtered-search correctness oracle alongside both measurements. Read actual plans and candidate traces to prove pre-filtered candidate sets or bitmap filters reach index scans. | Fail acceptance if either search mode lacks a measured before/after result or correctness proof. Reject a changed corpus or workload presented as a comparison. Reject a plan that scans the full unfiltered index and discards non-matching rows afterward. |

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
| EG-FEDERATED-QUERY-R084 | `served_shex_contract_journey` |
| EG-FEDERATED-QUERY-R084.1 | `wire_report_preserves_shape_map` |
| EG-FEDERATED-QUERY-R084.2 | `inline_and_live_data_selection` |
| EG-FEDERATED-QUERY-R084.3 | `bounded_constraints_report_results` |
| EG-FEDERATED-QUERY-R084.4 | `semantic_limits_remain_visible` |
| EG-FEDERATED-QUERY-R084.5 | `real_dispatch_reaches_shex_validator` |
| EG-FEDERATED-QUERY-R084.6 | `parse_and_dispatch_failures_are_distinct` |
| EG-FEDERATED-QUERY-R084.7 | `live_validation_obeys_read_authority` |
| EG-FEDERATED-QUERY-R084.8 | `served_shex_contract_journey` |

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

The following are proving targets, not execution receipts. Runtime delivery requires the real producer and the linked SDK consumer.

- **EG-FEDERATED-QUERY-R085:** All child tests and linked consumer evidence. Each child must pass; these definitions do not establish runtime delivery.
- **EG-FEDERATED-QUERY-R085.1:** `crates/eg-types/src/source_query.rs::tests::approval_records_round_trip` (planned). Round trips preserve every binding. Invalid references, parameter types, nonfinite values, absent snapshot identity and zero/unbounded limits refuse.
- **EG-FEDERATED-QUERY-R085.2:** `src/server/handlers/federation/source_query_tests.rs::approval_requires_authority_and_native_commit` (planned). A successful response follows durable issuance. Generic lease writes cannot forge approval. Issuance uses existing storage and credentials; no external SQL executes during approval.
- **EG-FEDERATED-QUERY-R085.3:** `src/server/handlers/federation/source_query_tests.rs::changed_or_revoked_approval_refuses` (planned). Changing any binding, authority, source or revision refuses. Revoked, expired, consumed, unavailable or missing leases refuse. Verification grants no SQL effect and creates no bearer credential.
- **EG-FEDERATED-QUERY-R085.4:** `crates/eg-plan/src/federation_tests.rs::approved_source_query_has_no_write_effects` (planned). Native parameter binding preserves values and query meaning. Supported projections, predicates, ordering and limits match the query oracle. Unknown dialects, functions or effects refuse without issuing approval.
- **EG-FEDERATED-QUERY-R085.5:** `tests/source_query_approval_served.rs::real_dispatch_issues_and_verifies_approval` (planned). Real authenticated dispatch reaches source validation, native issuance and fresh verification. No parallel planner, store or client-side approval authority appears. Current source has neither method.
- **EG-FEDERATED-QUERY-R085.6:** `tests/source_query_approval_served.rs::bounded_stream_preserves_approval` (planned). Oversized rows, exceeded budgets or failed revalidation stop execution and close the driver. Exactly one explicit completion or typed failure stops an execution. Missing terminal completion never means success.
- **EG-FEDERATED-QUERY-R085.7:** `tests/source_query_approval_served.rs::failure_after_three_chunks_is_terminal` (planned). Failure after three chunks stays failure with an incomplete execution identity. No successful terminal event or failed-chunk checkpoint follows. Earlier accepted pages retain their actual durable receipts.
- **EG-FEDERATED-QUERY-R085.8:** `tests/source_query_approval_served.rs::approval_to_connector_query_journey` (planned). Keep row widths, driver settings and all limits fixed while increasing cardinality by two orders of magnitude. Declare the baseline, memory envelope and tolerance before runs. Every peak must fit that fixed envelope; whole-result buffering fails. Record exact revisions, features, commands, rows/bytes, queue high-water marks, terminal outcomes and checkpoint agreement.

<!-- spec-definition-patch:owner-batch-20261012-federated-query-and-reasoning -->
## Source-backed acceptance targets and explicit holds — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-FEDERATED-QUERY-R070.1` / AU:71; source `0f1b775fd66c41586bdcdf0047a9ccbc52e38aa1bc21a4e8234e9c641e45c68e`:** Definition acceptance is blocked until the owner publishes the producer fields above. Once published, serve an authorized query over an unindexed corpus and return exact lexical hits, bounded file spans, verification result, source revision and measured cost; an over-ceiling query must return typed abstention before reading beyond the ceiling. Repeat against an indexed-only fixture and prove it is not accepted as this contract. Do not claim a served test or implementation now.

- **`EG-FEDERATED-QUERY-R006.1` / EG:11; source `2c2ffcf18d947730bc60a935fed942e1f82e9e8547f4509fe2941a34672fb8f3`:** After those owner decisions are recorded, acceptance must use two source facts with sentence and axiom origins, exercise both matching and nonmatching temporal intervals, and read back the derived fact with rule digest, graph generation and complete origin refs. Missing/stale origins, unsupported rule versions, conflicting validity windows and unauthorized writes must refuse without changing graph revision. Until then no producer test is specified as executable.

- **`EG-FEDERATED-QUERY-R040.5` / EG:20; source `07df52b453f0f1b7be7aec7bf790f087c3fc5e13500915011382a9fcd8d9d41a`:** Blocked until the owner binds admission, relation, retention and benchmark semantics. The eventual benchmark must compare fixed query-selectivity workloads at multiple corpus sizes, report rows/bytes/visits and demonstrate the declared cost bound while reading back the new knowledge and proving prior records and model revision unchanged. Missing admission authority, relation target or bound refuses; no runnable acceptance is asserted before the missing policy is supplied.

- **`EG-FEDERATED-QUERY-R040.6` / EG:21; source `c321db3aa79c29d891456f2bee98f545611b8d1be7c93d299b939c3d73a40279`:** After the owner fixes those decisions, acceptance must ingest two ordered sessions, build and independently read back an episode with source transcript/revision/span provenance, retrieve it via hybrid vector+keyword search and validate event time against storage time. A duplicate, conflicting chronology, stale transcript revision or unauthorized cross-session reference must be refused or reported as partial according to the owner-approved rule. No executable test exists until taxonomy and consumer are fixed.

- **`EG-FEDERATED-QUERY-R083.7` / EG:22:** Extend existing R083 behavior/model/refusal acceptance with the support-margin inputs and invariant that unsupported candidates lose. Add a served counterexample with a higher-confidence unsupported candidate and an evidence-supported lower-confidence candidate. Retain current two-candidate, class, abstention and evidence obligations. Producer/consumer path: Planned AU-RETRIEVAL-R006 prior probe -> served cross-modal query -> eg_plan::execute -> arbitrate -> ArbitrationVerdict -> harness. R083.3/.6 identify the producer path; this audit does not claim it is implemented. Source review `544b7b0054e8c7cd9a76aedd0d665589d555a0bcf20d1f00679442574936af36`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-FEDERATED-QUERY-R081.10` / EG:25; source `fb8d54d8554b8158da64f7c9e8bf02ea846e5b411768a0381c2682b2e6302dac`:** Once defined, serve a fixture where all three clocks disagree and another where one clock is absent; assert the selected basis and its provenance, and trigger exactly the configured bounded fallback only below the declared sufficiency threshold. Sufficient first-pass context must issue no second pass; stale or conflicting clock provenance and exhausted query budget must return a typed partial/refusal with no unbounded retry. No executable acceptance is currently authorized.

- **`EG-FEDERATED-QUERY-R028.1` / EG:29; source `2eb8aadb1b6ccc2d5de77a4d1964fc04a4e944bccf9bcc1793e262f146af7583`:** Construct one plain row and one row with resolved evidence/policy frames and compare the result against the same structured KnowledgeSet producer. Verify row id/score/projection are unchanged and both frame states are row-local. Independently remove provenance, policy, and snapshot binding; each case must return the typed refusal without a partial row. Keep R045 foreign-row pushdown behavior unchanged. Tests are acceptance targets only; no UQL implementation or served receipt is claimed.

- **`EG-FEDERATED-QUERY-R005.1` / EG:31; source `973de1c3b38646a438d057299d8070070f0ef7a2056bb30acff6d1a3814df513`:** For asymmetry, `(a,p,b)` alone is the clean control; adding `(b,p,a)` as the sole change creates one violation citing the declared asymmetric axiom, both observed edge ids and both evidence refs at pinned graph/ontology revisions. A missing reverse edge is not a violation. Also serve positive/control pairs for temporal conflict, cardinality, self-loop and ontology defect, combine two independent defects to require both diagnostics, and refuse missing kind-specific fields, unknown kinds and stale ontology revisions. Read back graph state to prove validation made no mutation; retraction remains denied absent explicit authorization.

- **`EG-FEDERATED-QUERY-R078.7` / EG:59; source `fd5990a7c9f30e1e1aa0eed9aa08efef4e2ebc8f5f34387598f1c9e577c458dc`:** With category mappings supplied, serve identical company-context queries as two tenants with differing clearances. Verify the authorized tenant receives each mapped category as source-bound references, denied rows are excluded before any source read, no source record is copied, and native time-series values join only by the declared series/time key and snapshot. Missing/ambiguous mapping, unverified tenant, stale source revision or cross-tenant join refuses. Current scope-planning acceptance remains covered by R078.1-.6; this new composed view stays held until mappings are exact.

- **`EG-FEDERATED-QUERY-R081.9.7.7` / GRAPHOS:24; source `b2e5de8386b3ca72b59a81ca5fa3193eaf7736af86aca472a720b9e4511961c0`:** Serve each of the three granularities over a nested authorized section tree and compare selected path/span ids with an independent tree oracle. Instrument external model, embedding and vector calls and require zero calls on every positive query. Malformed or stale section records and exhausted depth/visit/token limits refuse before extra dispatch; tenant isolation and existing community traversal outputs remain unchanged. No runtime proof is claimed.

- **`EG-FEDERATED-QUERY-R081.9.7.8` / GRAPHOS:25; source `6797cff839c16c3b9c3877d6bbf48bf8953eae8696087abc99f2b57147daed93`:** Once the owner supplies TOC generation and versioned positive finite `max_lines` and `max_bytes`, index a multisection fixture offline, then read a bounded middle span and verify exact line numbers and source digest. Refuse stale document/TOC revision, invalid or too-wide range, a response above the bound, and whole-file requests. Verify current GRAPHOS space/section links still resolve. Until producer and caps are named, no served acceptance is claimed.

- **`EG-FEDERATED-QUERY-R040.2.7` / GRAPHOS:28; source `f7654cd07d4d95f32f09fec742c4172b35c689749b3228aef9424830b1f7cc1b`:** Serve the same corpus/backend with two candidates whose assignments differ; independently recompute both topic components and final scores. Change only one candidate’s responsibility vector and prove only the expected candidate contribution/order changes. Refuse missing/duplicate document assignment, stale document or corpus revision, backend mismatch and embedding-space mismatch before truncation. Verify hard exclusions still hold and `run_unified_with` records the assignment/backend revision in the existing topic receipt. No implementation or served receipt is claimed.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run; names describe acceptance obligations, not evidence.

- **`EG-FEDERATED-QUERY-R081.8.100` (AU:77; source `213700ec5b9e8648657ec47d0c587483a978db10f4661475b467be451d3bb2e1`):** Served feedback fixtures accept same-retriever reuse, require validation after retriever version changes, and refuse a failed, stale or missing transfer receipt before retrieval. Verify the strategy digest and target retriever are in the receipt, unchanged feedback budgets remain enforced, and no rejected critique changes active strategy. Producer/consumer boundary: Authenticated crossmodal -> `eg-plan::exec` R081.8 feedback -> selected retriever. Preserve existing R081.8.1-.6 tiering, feedback and refusal behavior. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-FEDERATED-QUERY-R053.100` (EG:37; source `803f4e4ffbcd939af750e23451469835555beaa37e14ce555ffd0a59c0a3ac48`):** Build-time test enters the actual Cypher query-construction path and checks generated property keys for plain, reserved, spaced, Unicode and embedded-backtick names. Parse safe output against the Cypher grammar; an unquoted or malformed generated key fails the build. Keep values parameterized, preserve ForeignSourceSpec registration and Spark batch-only capability, and add no served API or runtime receipt. Gate evidence binds renderer source revision, fixture digest and test result.

- **`EG-FEDERATED-QUERY-R032.100` (AU:43; source `327ced0e0605f34366c662017d82ac892a481c5ca5f40bba2f3693ba6a5f7649`):** Served graph fixtures include ancestors and descendants across several layers, different historical failure counts, a cycle boundary and missing-history data. Verify both directions and deterministic layer/history contribution; refuse stale or malformed history, and compare existing R032 propagation outputs unchanged. Producer/consumer boundary: EG owns graph impact query/risk decision; AU requests or displays the result. Existing numeric risk helpers are not this two-direction historical impact contract. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run; they describe required future acceptance, not evidence.

- **`EG-FEDERATED-QUERY-R086.100` / EG:14; source `7223250e7f899a187aad0b152e597c6174b772c49779ca2bdd12d3ba68f4f5ef`:** Acceptance is blocked until model, evidence-class, authorization and AU receipt decisions are published. Then a served ordered three-source fixture must include an unseen-at-fit entity, return candidates with tuple order, graph/model/policy revisions and evidence class, leave graph revision unchanged, and change request digest when source order changes. Refuse unary tuples, unresolved/unauthorized sources, stale revisions and malformed relation constraints; confirm `GraphLearnPredict` binary results and write authority are unchanged. No producer test is currently defined as runnable. Parent `EG-FEDERATED-QUERY-R086` is linked to this sole required child; both remain SPECIFIED.

- **`EG-FEDERATED-QUERY-R081.101` / AU:126; source `deca151180a749cb3784d9946da0de17205eca89c92e5eb4f275859e1d4be165`:** Build a state once, rerank under two query contexts and reversed candidate order, and assert identical state digest with correctly aligned per-document scores. Assert no decoder/model-generation call. Compare measured interaction cost to the same full-token baseline and refuse stale document/encoder revision, malformed segment coverage or a query-time state rewrite. Producer/consumer: EG retrieval owns document-state construction and PlanCtx reranking; query embeddings/context remain query inputs and do not alter the reusable state. Preserve existing FQR R081 tiering and selection bounds. Parent `EG-FEDERATED-QUERY-R081` remains intact.

- **`EG-FEDERATED-QUERY-R081.102` / GRAPHOS:27; source `baac22c9fb42470957b5b91499c6e42e1bb2410d24446d15f6f364638c88138e`:** Distill and read back an artifact whose held-out quality meets threshold, then run served queries and assert zero model-service calls and scorer digest binding. Below-threshold, missing held-out evidence, stale corpus/index or teacher revision all refuse activation; verify the prior scorer remains active. Producer/consumer: EG owns retrieval learning/ranking and the stored scorer; AU may execute evaluation; GraphOS catalog ranking is not this document retrieval contract. Use the existing authenticated UnifiedQuery -> run_unified_with planning route; do not invent a public operation name. Parent `EG-FEDERATED-QUERY-R081` remains intact.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mapping-eg41-20261012 -->
## Mapped retrieval-selection acceptance — 2026-10-12

No case listed here has run.

- **`EG-FEDERATED-QUERY-R081.100` / EG:41; source `bceeed106b5db2933cf5161d58b4203fc7afe9d714503a31d68323992906c23b`:** Through served retrieval, compare a tiny fixture against an independent exhaustive nonnegative-sparse oracle, including two highly correlated documents and an alternative diverse pair. Assert exact selection, coefficients, residual and deterministic order. Refuse embedding-space mismatch, invalid policy and non-convergence; verify existing tenant and result-count filters still apply. Producer/consumer: EG retrieval selection owns the joint objective and set result; the existing served UnifiedQuery/retrieval path consumes it. Preserve R081.1-.9 selection and tenant bounds; no new caller-controlled score authority. Parent `R081` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — viewport query

No test in this section has run.

- `EG-FEDERATED-QUERY-R081.103`: serve scatter and line variants over a real indexed source of at least 1,000,000,000 points; verify viewport-derived bin caps and `min(requested_byte_ceiling, formula_cap)` encoded-byte behavior at minimum/maximum sizes, source/cardinality/index revision and line gaps. Missing, invalid and too-small requested ceilings refuse before scanning. Pan/zoom changes query digest; exact-point and aggregate-bin selections remain revision-bound. Stale index/source, denied selection, invalid/exceeded cap and any full-source transfer fail acceptance.
- Pan/zoom and exact/aggregate selection prove viewport-bound tokens. Missing index, stale revision, denied point/bin and invalid/exceeded caps refuse; verify no full-source payload or silently dropped bins.
<!-- /spec-definition-patch -->
