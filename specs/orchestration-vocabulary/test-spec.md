# EG-ORCH-VOCAB — Test spec

Positive: each term resolves and the reasoner stays consistent. Each Q1-Q7 query returns fixture rows. Negative: plant one invalid fixture per shape. Cover `:precedes` cycle and `swarm:feeds` cycle. Cover slot with non-kind, member without capability, rule without class, and agent without model. Integration: committed assembly is queryable through Q5 and Q7. Tests live in `crates/eg-core/tests/orchestration_vocabulary.rs`. Fixtures live in `crates/eg-core/tests/fixtures/orchestration_vocabulary/`.

Assembly-kind acceptance: a verifier-constrained slot excludes an executor-only candidate, accepts a visible verifier, and yields typed abstention without one.

Harness run-trace acceptance: a valid trace per termination reason round-trips through the client and `AddNode`. The engine refuses a negative counter, an unknown reason, and a peak above budget without `context_overflow`. The engine refuses a measured fact without evaluation-run evidence. A newer measurement supersedes an older one, and the older one stays readable. Competency question: for model M on task class C, `harness_best_config` returns the configuration with the lowest cost per solved task. It returns its trace ids and abstains below the sample floor.
Spec-folder mapping: a fixture folder through `IndexRepository` yields one node of each kind. An unknown file is refused and stored as a plain document. Every node names its deciding rule.

### EG-ORCH-VOCAB-R013 tests

- `EG-ORCH-VOCAB-R013.1`: `crates/eg-types/src/outcome_bundle.rs::tests::step_record_round_trips`.
- `EG-ORCH-VOCAB-R013.2`: `crates/eg-types/src/outcome_bundle.rs::tests::step_digest_is_canonical`.
- `EG-ORCH-VOCAB-R013.3`: `src/server/handlers/graph_ops/gateway_graph.rs::tests::add_run_trace_checks_step_digests`.
- `EG-ORCH-VOCAB-R013.4`: `crates/eg-types/src/outcome_bundle.rs::tests::bad_digest_and_gap_refused`.
- `EG-ORCH-VOCAB-R013.5`: `crates/eg-types/src/outcome_bundle.rs::tests::stored_steps_read_in_order`.
- `EG-ORCH-VOCAB-R013.6`: `tests/orchestration/test_run_trace_steps_served.py::test_first_divergence_is_step_two`.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-vocabulary -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, no test or receipt has run. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-ORCH-VOCAB-R009.18` / AU:26; source `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`:** Planned served acceptance `run_observations_change_model_and_width_with_trace`. Exercise run_observations_change_model_and_width_with_trace through real AU routing/continuation and served EG Decide. Use identical budgets and controlled latency/cost/error windows. Demonstrate policy-directed model/width changes and budget-forced refusal/stop. Read back exact before/after trace facts. Reject stale windows and invalid rates/denominators. Keep unknown values unknown. Show width narrowing releases capacity. Reject mid-run widening. Host-only AIMD or model-only allocator tests do not pass. Owner wiring and holds are defined in `requirements.md`. No test, served readback, receipt, implementation, or closure is claimed.

- **`EG-ORCH-VOCAB-R013.7` / AU:115:** Source-pinned `CodeWalkthroughV1` manifest and exact AddNode/UQL/artifact readback/refusal cases as named in requirements.md. AU producer schema/receipt and WebUI artifact-consumer owner IDs remain pending; no producer test is credited.

- **`EG-ORCH-VOCAB-R003.8` / EG:12:** Require successful-run promotion. Test pinned similarity policy and exact signature/schema/topology eligibility. Test Jaccard score and stable tie break. Refuse stale or malformed handler metadata. Test served `graph.assemble` reuse with R036 certificate. Exact future test names and paths are in `requirements.md`; no implementation evidence is claimed.

- **`EG-ORCH-VOCAB-R012.7` / EG:46; source `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`:** Planned served acceptance `repository_policy_is_synthesized_guarded_and_used`. Exercise repository_policy_is_synthesized_guarded_and_used with real IndexRepository input. Inspect typed candidates/spans. Review the exact digest. Activate through owner path. The served decision SHALL prove active-rule use. A conflicting rule names the invariant and preserves prior active revision. Test unparsed prose, missing review, changed source digest, and invariant-generation race. Classification and direct helper-only tests do not pass. Owner wiring and holds are defined in `requirements.md`. No test, served readback, receipt, implementation, or closure is claimed.

- **`EG-ORCH-VOCAB-R004.6` / EG:51; source `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`:** Planned served acceptance `capability_gap_and_enhancement_are_queryable`. Exercise capability_gap_and_enhancement_are_queryable through served mutation and generated UnifiedQuery. Cause a served unmet-capability abstention. Confirm it does not by itself create a durable registry gap. Admit a CapabilityGap that cites the abstention. Admit a distinct DesiredEnhancement from a reviewed request through the governed mutation path. Query two task classes and independently read typed fields, edges, and provenance before and after restart. Test event deduplication, revision conflict, invalid taxonomy, exact/descendant matching, and denied-tenant pagination. Confirm capability implementation state did not change. In-memory fixtures alone do not pass. Owner wiring and holds are defined in `requirements.md`. No test, served readback, receipt, implementation, or closure is claimed.

- **`EG-ORCH-VOCAB-R012.8` / EG:61; source `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`:** Planned served acceptance `reviewed_normative_source_activates_without_graph_overwrite`. Exercise reviewed_normative_source_activates_without_graph_overwrite through served ingestion/review/activation. Dispatch a decision that records active policy revision. Compare source bytes/Git revision before and after graph mutation/reasoning. A graph-originated normative write has no source effect. Missing/rejected review, modified blob, changed rules, or raced revisions preserve prior policy. Permitted source changes require governed writeback, reread, renewed review, then activation. Direct fixture activation does not pass. Owner wiring and holds are defined in `requirements.md`. No test, served readback, receipt, implementation, or closure is claimed.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run; names describe acceptance obligations, not evidence.

- **`EG-ORCH-VOCAB-R005.100` (AU:33; source `1a32741b0a97f0c31cd1a3df4ea31d6eee0069c62c3487c0d8057b1ced4b6cf4`):** Served case: a secret undeclared nested field and declared sibling enter the workflow. Worker receives only declared projection. Coordinator can still synthesize typed outputs. Unknown paths, stale schema, and unauthorized fields refuse before dispatch. Preserve R020 result and schema refusal. EG declares projection/shield policy. AU `_dispatch_parallel_batch`/processor applies projection before tools. DTO extra-field rejection is not projection. Require exact revision, IO digests, caller, and independent readback as specified. Planned only.

- **`EG-ORCH-VOCAB-R009.100` (AU:60; source `5d53cbdda932456b27c6649360c3051d4ab09f2db42018550b4ee81c785748b4`):** Served read test exports a parent with multiple agents and tool calls. Retrieve all correlated records across pages. Verify stable digest, redaction, and completeness. Detect missing child, duplicate span, unauthorized read, and truncated pagination. EG read tests do not assert MCP transport delivery. AU `default_runtime_capabilities/run_agent` exports. GraphOS transports MCP. EG returns correlated readback. Preserve R008/R024 trace ownership. Require exact revision, IO digests, caller, and independent readback as specified. Planned only.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — cross-owner harness analytics

No test in this section has run; these are future acceptance requirements.

- `EG-ORCH-VOCAB-R009.101`: Serve index/search across every adapter in the pinned manifest. Read traces against receipts. Test restart/replay idempotency, source coverage, and cursor paging. Cost summaries use R010.101 units/profile and preserve unknown state. Refuse conflicting identities, stale cursors, and cross-tenant requests.
- `EG-ORCH-VOCAB-R010.100`: Serve `EvolutionTraceMetricsV1` joining two revisioned traces and outcome metrics to proposal/prompt/progress revision. Independently read metric/progress digests and window. Missing traces are partial/unknown. Refuse stale, mismatched, and cross-tenant refs. AU producer and GraphOS adapter evidence are separate.
- `EG-ORCH-VOCAB-R010.101`: Reconcile four token categories and grouped costs against an independent ledger. Test split-trace invariance, half-even boundaries, unknown prices, mixed currencies, and final FX rounding. Measure p95 ≤500 ms on 1,000,000 indexed traces and 32 readers through GraphOS `handle_runs_metrics`.
<!-- /spec-definition-patch -->


### Additional future acceptance for EG-ORCH-VOCAB-R004.6

`tests/orchestration-vocabulary/test_authority_binding_served.py::test_authority_bound_gap_admission_and_restart` is a planned test, not an existing receipt. Exercise every schema and refusal rule in plan.md through generated served clients. Independently read admitted authority, producer receipts, exact revisions and output after restart. Test identical replay and conflicting source events, stale expected revisions, missing/changed authority, denied tenant and unknown references. Verify refusal stores nothing. Preserve every existing acceptance clause above verbatim.
Test unknown-equal IRIs, forbidden/unlisted extensions and descendant proof pinning. Request-local abstention must create no durable gap; separately admit and read the gap and reviewed enhancement.

Test equal admitted endpoints in exact and descendant modes. Test distinct candidate-child/requested-ancestor endpoints only in descendant mode. Refuse reversed, disconnected, mismatched and unknown-equal endpoints. Independently read both endpoints and match mode from resolver output and durable admission receipt after restart.
