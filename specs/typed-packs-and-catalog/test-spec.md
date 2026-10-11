# Test and quality specification

## Evidence rule

Tests may run in an isolated ephemeral EG/SDK environment using synthetic packs and local engine state. No private service, inventory or live tenant is a prerequisite. Record exact commit, command, fixture version, count, exit status and relevant artifact digest. A skipped or unavailable gate remains unverified. Every refusal asserts unchanged head, members, holders, records, body liveness and outbox, not only the error string.

| Test ID | Requirements | Fixture/action | Expected result and evidence |
|---|---|---|---|
| TP-01 | PACK-01, 05, 10 | Replay `contract/fixtures/connector_pack_digest_vectors.json` in Rust and SDK; permute archive offsets/order and package version; change one body byte. | Semantic digest stable under repackaging, changes on content, server pins follow declared contract, and generated fixtures/receipt match committed bytes. |
| TP-02 | PACK-01, 06 | Plant one failing index/archive case for G1–G6: unsorted/duplicate URI, wrong scheme/kind, two servers, each size edge, missing/corrupt archive, overflow/overlap/gap, wrong digest, forbidden or duplicate component ID. | Named code; no write; mutation deletion of each validation guard makes its test fail. |
| TP-03 | PACK-06 | Plant G7–G12: BOM, invalid UTF-8, duplicate JSON key, too deep/nodes, NaN, missing tool input schema, YAML alias/tag/front-matter mismatch, malformed native IRI, foreign capability claim, invalid currency/cost/latency/model values. | Invalid bodies refused; foreign absolute capability IRI accepted as unresolved claim with warning; empty description accepted with fallback warning. |
| TP-04 | PACK-06, 08 | Plant G13–G21/K1–K7: `@base`, remote `owl:imports`, overlong IRI, inconsistent ontology, derivation budget exhaustion, `SERVICE SILENT`, per-file blank-node collision, conflicting shape owner, SHACL violation, bad/cyclic ref, invalid draft, unbound importer, >50% withdrawal, 300 violations and stale schema source. | Exact refusal/warning and bounded runtime; 256 maximum violations with exhaustion flag; no remote fetch or partial mutation. The planted G14 and malformed G15 cases must be red before the fix and green after. |
| TP-05 | PACK-03, 04 | Import same pack twice. Lose response and retry same operation identity. Retry with different bytes. Race two expected heads. Change only package version. Change server contract version. Remove and restore one tool. | No-op creates no revision. Replay returns same receipt. Changed bytes conflict. One racing head refuses. Contract change cascades pins, package-only change does not. Withdrawal/republication preserves ID. |
| TP-06 | PACK-03, 11 | Kill child after body store and before owner commit, during owner commit, after commit and before response. Cancel timed-out import and immediately Bind same tenant. | Orphan bodies are reclaimed after grace. Owner state is all-or-nothing. Receipt replays. Lock is released and next operation is not blocked. |
| TP-07 | PACK-02, 04, 11 | Authorized and unauthorized `Status`, `Content`, Bind/Unbind/Retire/Reproject, direct `mcp:` Publish, cross-tenant and graph reads, clustered writer. | Authz actions and stable codes match descriptors. No cross-tenant bytes/metadata. Reserved ID and LocalOnly refusals. Historic pin may resolve withdrawn revision but new selection cannot. |
| TP-08 | PACK-07, 08 | Crash projection between graph attach and visibility ack, inject graph grant denial, process stale outbox event after newer head, retry failed event, attach pack snapshot to another graph. | At most one visible revision; stale worker refuses; denial is durable failed status; later events are not collateral-dead-lettered; static attachment reports staleness. |
| TP-09 | PACK-08 | Attach two compatible sources, conflicting shape owner, duplicate shared class, source bound overflow, `IcvConfigure` compatibility and attach→detach→attach same bytes; compare `GraphSchemaClasses`. | Composed policy/digest and class list deterministic; only real conflict refuses; cache cannot suppress repeated attach; no retroactive data scan is implied. |
| TP-10 | PACK-09, 10 | Run generated-contract fixpoint and committed-artifact comparison. Replay method-body signing vectors in Python/Go/JS. Every OpKind/PredKind printer/parser round trip, 512 property cases and fuzz target. | Receipt and generated files match by content. Every public client signs and decodes same typed contract. No variant silently falls to Dynamic. |
| TP-11 | PACK-09, 12 | Synthetic connector from SDK manifest/tool probe through pack import, visible projection, fleet catalog join and body read. Restart and restore. | Content and liveness have distinct authorities. No duplicate catalog row. Served result matches local typed result after restart. |
| TP-12 | PACK-12 | Full Rust nextest suite including integration binaries, root Python suite, Go and JS client suites, planted pack tests and release workflow replica at the exact head. | Zero failures, zero signal-killed hidden tests and no omitted root suite. If a test binary dies by signal, rerun that binary under process-isolated nextest and account for every test. |
| TP-13 | PACK-08, 10 | Migrate a representative ontology object, schema pack DTO and schema-drift candidate through EG typed/generated APIs; compare prior read/write behavior and provenance before deleting duplicate types. | One EG schema authority, no lost semantics, no caller-owned SHACL rendering and deterministic drift gate refusal/acceptance. |

## Quality and release gates

The repository pins CCCC 1.6.0, KISS 0.4.12, Dupehound 0.1.2 (`threshold 0.85`, at least 40 tokens), and jscpd 5.0.16 (at least 50 tokens and 5 lines, mild mode) in `pyproject.toml`. Use the repository's scanner source manifest and release workflow rather than arbitrary file subsets. `release.yml` runs scanner install and validation. `scripts/check_dupehound_census.py`, `scripts/validate_cccc_census.py`, `scripts/check_kiss_census.py`, and jscpd differential are the configured proofs. Keep generated contract artifacts in their explicit generated exclusions only; source duplicate pairs need resolution or reviewed explanation. Apply KISS by extending typed owner modules and a shared validator instead of a second registry, parser or cache. Run Rust fmt/Clippy, Python lint/type/test, `gen_contract --check`, contract generated-artifact test, architecture/pin-resolution checks and release gate replica where applicable. The current gate's exact command should be copied into the evidence, not replaced with an invented score.

## Acceptance matrix

PACK-01: TP-01/02; PACK-02: TP-07/10; PACK-03: TP-05/06; PACK-04: TP-05/07; PACK-05: TP-01/04; PACK-06: TP-02/03/04; PACK-07: TP-08; PACK-08: TP-04/09/13; PACK-09: TP-10/11; PACK-10: TP-01/10/13; PACK-11: TP-06/07/08; PACK-12: TP-11/12 and all quality gates. A requirement is `ACCEPTED` only when its mapped tests and applicable gates pass against a merged exact head.

## Boundary and verification notes (EG-TYPED-PACKS-R020, R052, R060, R063, R082)

These five requirements are boundary or verify-against-existing-contract rows, not net-new pack behavior; each is covered by evidence that already exists rather than a new fixture.

| ID | Note |
|---|---|
| EG-TYPED-PACKS-R020 | The tenant-profile administrative capability defect is owned and tested by the dependency that defines tenant profiles (see the ruled-decision table in `tasks.md`); no pack-write code path in this repository depends on it, so this spec's own release gate (TP-01..TP-13, scanner census) is the full proof that no corresponding change was made here. |
| EG-TYPED-PACKS-R052 | Head-state projection for the first Agent Library outbox consumer waits on the general outbox reject/re-drive primitive (PACK-07, P7); per this row's own text, no pack-specific stand-in is shipped while that primitive is pending, so there is no placeholder code for this gate to find in this repository today. |
| EG-TYPED-PACKS-R060 | Manifest-read refusal for an incompatible owner layout precedes any import path relying on it; this ordering is exercised by the same incompatible-layout refusal coverage TP-06 already asserts for owner-transaction recovery (lock release, no partial commit), so no import path may be added here ahead of that refusal. |
| EG-TYPED-PACKS-R063 | Before any shared schema version bump, the affected agent-layer row types must be enumerated explicitly; TP-10's generated-contract fixpoint and committed-artifact comparison is the existing check this enumeration must pass, so a bump without an explicit per-type list fails TP-10 rather than passing silently. |
| EG-TYPED-PACKS-R082 | TP-13 (migrate a representative ontology object, schema pack DTO and schema-drift candidate through EG typed/generated APIs, compare prior read/write behavior) is this row's EG-side proof; the hand-written-DTO modules it names (`models/knowledge_graph`, `models/schema_definition`, `models/evidence_bundle`, `models/knowledge_pack`, `models/codemap`, `models/graph`, `models/knowledge_base`, `schema_pack`, `schema_packs`) are owned by `agent-utilities`, so their migration and the `AU-SEMANTIC-R005` `kg/domain_packs` follow-on are cross-repo children delivered against that repository, consuming EG's generated types as the destination-copies-first side of the move. |

## Cross-cutting import-safety defect regression mapping (EG-TYPED-PACKS-R021)

Every cross-cutting pack-import-safety defect is a named rule (G1-G21) in `epistemic_graph/testing/synthetic/packs/catalogue.py`'s `MUTATIONS` (TP-02/03/04 above), and each rule's every variant below has its own regression test case: one parametrized run of `tests/test_connector_pack_planted.py::test_planted_defect_is_refused_by_its_rule`, one per `MUTATIONS` entry, with the test id computed by that module's own `_case_id()` (a mutation function's `__name__`, or a case object's `f"{rule}_{variant}"` -- the two mutation sources in `MUTATIONS` use each form; `_case_id`'s docstring names both, and `pytest --collect-only tests/test_connector_pack_planted.py` lists the exact ids). The one documented exception is the tenant-profile administrative capability defect (EG-TYPED-PACKS-R020): it is fixed in, and tested by, the dependency that defines tenant profiles, not by this gate.

| Rule | Variants, each with its own regression case in `MUTATIONS` |
|---|---|
| G1 | connector_with_slash, invalid_connector, name_over_256_bytes, schema_version, scheme_mismatch, skill_file_traversal, two_servers, unknown_kind, unsorted_uris |
| G2 | archive_over_16mib, body_over_2mib, entries_over_1024, index_over_1mib, iri_list_over_64, references_over_64, schema_over_1mib |
| G3 | archive_length, archive_not_uploaded, archive_sha256 |
| G4 | offset_overflow, overlapping_sections, uncovered_bytes |
| G5 | flipped_body_byte, forged_pack_digest |
| G6 | decide_owned_kind, duplicate_component_id |
| G7 | invalid_utf8, utf8_bom |
| G8 | array_input_schema, duplicate_keys, json_depth_65, nan_literal |
| G9 | front_matter_over_16kib, missing_front_matter, name_mismatch, unknown_skill_type, yaml_alias |
| G10 | missing_input_schema |
| G11 | empty_description |
| G12 | cost_over_bound, currency_not_iso_4217, foreign_namespace_iri, malformed_iri, max_output_over_context, mistyped_native_iri, modality_in_provides, p50_over_p95, tool_mode_on_prompt |
| G13 | base_directive, iri_over_4kib, relative_iri, remote_import, shapes_base_directive, unparseable |
| G14 | derivation_budget, unsatisfiable_class |
| G15 | duplicate_shape_iri, malformed_path, scoped_blank_node_labels, service_silent |
| G16 | instance_without_label |
| G17 | missing_reference, reference_cycle, reference_kind_mismatch |
| G18 | control_character_summary |
| G19 | unconfigured_importer |
| G20 | empty_pack, withdraw_sixty_percent |
| G21 | three_hundred_violations |

A requirement is `ACCEPTED` only when every row's regression test passes against a merged exact head, exactly as the acceptance matrix above requires for PACK-06/08.

| Repair | Requirement | Regression | Acceptance |
| --- | --- | --- | --- |
| TP-PARSE-IDENTITY | EG-TYPED-PACKS-R093.1 | Native public and bounded Turtle readers parse the same explicit and anonymous blank nodes independently, then union the results. | Six subjects stay distinct across three parses; same-document references remain equal; composition retains explicit source labels; existing bounded refusal tests pass. |
| TP-REPROJECT-IDENTITY | EG-TYPED-PACKS-R093.2 | Served reproject combines independent connector-pack members with reused blank labels. | Isolated member identities and repeatable composed digests; remains open until the served regression executes. |

## Capability annotation evidence

| Requirement | Native regression | Result |
| --- | --- | --- |
| EG-TYPED-PACKS-R042.1 | `src/server/handlers/admin/connector_pack/import/capability_tests.rs::both_capability_fields_refuse_relative_and_invalid_scheme_claims` and `foreign_terms_warn_and_stay_only_in_the_declared_partition`: both typed fields accept native terms and retain exact foreign claims with warnings. Native typos, roots, wrong kinds, invalid schemes, and malformed typed lists refuse. | Eight production-module tests passed on 2026-10-11 with Rust 1.96.0 and cached `eg_types` and `serde_json` dependencies. Restoring the old scheme check makes the invalid-scheme test fail at `provides: 1vendor:search`. |
| EG-TYPED-PACKS-R042.2 | `src/server/handlers/admin/connector_pack/tests/admin.rs::a_rejected_import_mutates_nothing_in_the_catalog`. `src/server/handlers/admin/connector_pack/tests/content_model.rs::capability_annotations_remain_declared_after_import`. | Both signed, served native regressions pass. Exact source, commands and results are in `../decision-engine/evidence/gr1080-native-20261011/final-native-results.json`. This focused slice does not verify the parent. |

The native harness loads the unchanged production module path with `#[path]`; it uses no behavior substitutes. To repeat from the repository root with compatible cached Rust dependencies:

```sh
work=$(mktemp -d)
printf '#[path = "%s/src/server/handlers/admin/connector_pack/import/annotations.rs"]\nmod annotations;\n' "$PWD" > "$work/native.rs"
rustc --test --edition=2021 "$work/native.rs" \
  -L "dependency=$CACHED_RUST_DEPS" \
  --extern "eg_types=$(find "$CACHED_RUST_DEPS" -name 'libeg_types-*.rlib' -print -quit)" \
  --extern "serde_json=$(find "$CACHED_RUST_DEPS" -name 'libserde_json-*.rlib' -print -quit)" \
  -o "$work/native-tests"
"$work/native-tests" --nocapture
```

The full served test suite and hosted CI were not run for this native slice.

R042.2 uses signed imports for four G12 refusals, two per capability field.
The admin fixture compares head, members, component revisions, import records, holders, body liveness, and outbox rows.
The fixture checks blob counts after archive upload to detect rejected body admission.
The positive fixture compares both native and declared fields and exact foreign warning receipts.
R093.2 extends `an_admin_forced_reproject_redrives_a_stale_projection_without_losing_later_work`.
The fixture reads authoritative persisted schema after signed Reproject and checks two distinct blank identities.
A fresh request key repeats Reproject and preserves the composed digest, source catalogue, and connector head.
Scoped formatting and diff checks do not execute these native tests.
The R042.2 fixtures and R093.2 forced-reprojection fixture pass native execution;
their exact evidence is in the shared fixture-repair receipt linked above.
Test bindings record the existing acceptance fixtures, independently of execution results.

## Modality regression scope (EG-TYPED-PACKS-R043)

The following tests cover existing native validation and digest behavior. They do not establish complete PACK-09 acceptance.

| Source and proving test | Required observation |
|---|---|
| `src/server/handlers/admin/connector_pack/import/modality_tests.rs::modalities_accept_native_terms_for_tools_prompts_and_skills` | Both fields accept native terms for all three entry kinds and round-trip without loss. |
| `src/server/handlers/admin/connector_pack/import/modality_tests.rs::modalities_refuse_foreign_roots_typos_and_wrong_kinds` | Both fields refuse foreign, root, typo, wrong-kind, relative, and empty IRIs with the typed code. |
| `src/server/handlers/admin/connector_pack/import/modality_tests.rs::modality_lists_enforce_wire_types_and_both_size_edges` | Zero and 64 elements pass; 65 elements, null, scalars, and non-string elements refuse. |
| `src/server/handlers/admin/connector_pack/import/modality_tests.rs::modality_digest_replay_is_canonical_and_direction_sensitive` | Rust replays committed full/empty vectors. Reordering and duplication preserve identity; content and direction changes do not. |
| `tests/test_connector_pack_client.py::test_modality_annotations_replay_golden_vector_and_affect_identity` | Python replays the full vector. Both fields change tool, prompt, and skill pack identity. |

TP-10, TP-11, and applicable quality gates remain required on the merged exact head.
These partial regressions intentionally carry no acceptance binding that would mark R043 VERIFIED.

## Harness configuration evidence

- `EG-TYPED-PACKS-R048.1`: `crates/eg-types/src/agent_component/facts.rs::tests::context_window_zero_is_refused`.
- `EG-TYPED-PACKS-R048.2`: `crates/eg-types/src/agent_component/facts.rs::tests::measured_fact_requires_run_ref`.
- `EG-TYPED-PACKS-R048.3`: `crates/eg-types/src/agent_component.rs::tests::absent_shell_proficiency_keeps_digest`.
- `EG-TYPED-PACKS-R048.4`: `crates/eg-types/src/agent_component.rs::tests::measured_model_facts_change_digest`.
- `EG-TYPED-PACKS-R048.5`: `tests/typed_packs/test_model_harness_facts.py::test_model_profile_round_trip`.
- `EG-TYPED-PACKS-R066.1`: `crates/eg-core/tests/harness_shapes.rs::model_fact_shapes_reject_missing_run`.
- `EG-TYPED-PACKS-R066.2`: `crates/eg-core/tests/swarm_topology_shapes.rs::shell_centric_must_be_boolean`.
- `EG-TYPED-PACKS-R066.3`: `crates/eg-core/tests/swarm_topology_shapes.rs::shell_centric_inherits_or_is_absent`.
- `EG-TYPED-PACKS-R045.1`: `crates/eg-types/src/connector_pack/annotations.rs::tests::new_fields_roundtrip`.
- `EG-TYPED-PACKS-R045.2`: `crates/eg-types/src/connector_pack/annotations.rs::tests::absent_facts_take_safe_defaults`.
- `EG-TYPED-PACKS-R045.3`: `crates/eg-types/src/connector_pack/digest.rs::tests::absent_new_fields_keep_golden_vectors`.
- `EG-TYPED-PACKS-R045.4`: `src/server/handlers/admin/connector_pack/import/annotations.rs::tests::unknown_side_effect_is_refused`.
- `EG-TYPED-PACKS-R045.5`: `src/server/handlers/admin/connector_pack/import/facts.rs::tests::mcp_hint_read_only_is_unproven`.
- `EG-TYPED-PACKS-R045.6`: `src/server/handlers/decide/stat_tests/tool_subset_tests.rs::missing_schema_tokens_abstains`.
- `EG-TYPED-PACKS-R045.7`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::mcp_hint_read_only_premise_is_unproven`.
- `EG-TYPED-PACKS-R045.8`: `crates/eg-capabilities/src/contract/python/surfaces/tests.rs::pack_annotations_expose_harness_tool_facts`.
- `EG-TYPED-PACKS-R045.9`: `tests/harness_config_tool_facts_served.rs::two_tool_pack_subset_respects_budget`.

## Measured model producer tests (R048.3, R048.4)

`absent_shell_proficiency_keeps_digest` checks fixed v3 and v4 component digest
vectors and decodes omitted and explicit-null fields. `measured_model_facts_change_digest`
checks each measured value, evaluation ID and evaluation digest independently.
`measured_model_facts_refuse_malformed_references` checks decode and publication
refusal. `measured_model_facts_project_ontology_names` calls the existing
`component_atoms` projector and checks all enum values and absent fields.

These are producer unit tests. R048.5 still requires signed pack import and served
readback evidence. The generated lifecycle records test bindings, not execution
results. Native types/RDF test execution and contract generation are required
before integration of the producer fields.

Skill package rendering: a changed referenced node changes the digest on the next listing. A dangling term is refused and the row stays listed.
