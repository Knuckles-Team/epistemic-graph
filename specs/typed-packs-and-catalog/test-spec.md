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
| TP-08 | PACK-07, 08 | Crash projection between graph attach and visibility ack. Inject graph grant denial. Process a stale outbox event after a newer head. Retry a failed event. Attach a pack snapshot to another graph. | At most one revision is visible. A stale worker refuses. Denial becomes durable failed status. Later events are not collateral-dead-lettered. Static attachment reports staleness. |
| TP-09 | PACK-08 | Attach two compatible sources, conflicting shape owner, duplicate shared class, source bound overflow, `IcvConfigure` compatibility and attach→detach→attach same bytes; compare `GraphSchemaClasses`. | Composed policy/digest and class list deterministic; only real conflict refuses; cache cannot suppress repeated attach; no retroactive data scan is implied. |
| TP-10 | PACK-09, 10 | Run generated-contract fixpoint and committed-artifact comparison. Replay method-body signing vectors in Python/Go/JS. Every OpKind/PredKind printer/parser round trip, 512 property cases and fuzz target. | Receipt and generated files match by content. Every public client signs and decodes same typed contract. No variant silently falls to Dynamic. |
| TP-11 | PACK-09, 12 | Synthetic connector from SDK manifest/tool probe through pack import, visible projection, fleet catalog join and body read. Restart and restore. | Content and liveness have distinct authorities. No duplicate catalog row. Served result matches local typed result after restart. |
| TP-12 | PACK-12 | Full Rust nextest suite including integration binaries, root Python suite, Go and JS client suites, planted pack tests and release workflow replica at the exact head. | Zero failures, zero signal-killed hidden tests and no omitted root suite. If a test binary dies by signal, rerun that binary under process-isolated nextest and account for every test. |
| TP-13 | PACK-08, 10 | Migrate a representative ontology object, schema pack DTO and schema-drift candidate through EG typed/generated APIs; compare prior read/write behavior and provenance before deleting duplicate types. | One EG schema authority, no lost semantics, no caller-owned SHACL rendering and deterministic drift gate refusal/acceptance. |

## Quality and release gates

The repository pins CCCC 1.6.0, KISS 0.4.12, Dupehound 0.1.2 (`threshold 0.85`, at least 40 tokens), and jscpd 5.0.16 (at least 50 tokens and 5 lines, mild mode) in `pyproject.toml`. Use the repository's scanner source manifest and release workflow rather than arbitrary file subsets. `release.yml` runs scanner install and validation. `scripts/check_dupehound_census.py`, `scripts/validate_cccc_census.py`, `scripts/check_kiss_census.py`, and jscpd differential are the configured proofs. Keep generated contract artifacts in their explicit generated exclusions only; source duplicate pairs need resolution or reviewed explanation. Apply KISS by extending typed owner modules and a shared validator instead of a second registry, parser or cache. Run Rust fmt/Clippy, Python lint/type/test, `gen_contract --check`, contract generated-artifact test, architecture/pin-resolution checks and release gate replica where applicable. The current gate's exact command should be copied into the evidence, not replaced with an invented score.

## Acceptance matrix

PACK-01: TP-01/02; PACK-02: TP-07/10; PACK-03: TP-05/06; PACK-04: TP-05/07; PACK-05: TP-01/04; PACK-06: TP-02/03/04; PACK-07: TP-08; PACK-08: TP-04/09/13; PACK-09: TP-10/11; PACK-10: TP-01/10/13; PACK-11: TP-06/07/08; PACK-12: TP-11/12 and all quality gates. Qualification requires the mapped tests and applicable gates to pass against an exact merged head.

## Boundary and verification notes (EG-TYPED-PACKS-R020, R052, R060, R063, R082)

These five requirements are boundary or verify-against-existing-contract rows. They add no net-new pack behavior. Existing evidence covers each row; no new fixture is required.

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

Every row's regression test must pass against an exact merged head, as PACK-06/08 requires.

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
- `EG-TYPED-PACKS-R066.2`: `crates/eg-shacl/tests/swarm_topology_shapes.rs::shell_centric_must_be_boolean`.
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

## R071.1 and R097.2 crosswalk acceptance

All cases below are required future evidence.

| Scenario | Requirements | Required evidence |
|---|---|---|
| Typed records and domain kinds | R071.1.1, R097.2.1 | Round-trip revisions, origins, relations and coverage. Reject malformed IRIs, unknown kinds and foreign coverage entries. |
| Deterministic generation | R071.1.2, R097.2.2 | Regenerate RDF twice. Compare declared mappings and exact unmapped sets against independently enumerated source classes. |
| Real production path | R071.1.3, R097.2.3 | Signed import reaches production validation and schema attachment. Generated client inspection reads the committed revision and digest. |
| Refusal before activation | R071.1.4, R097.2.4 | Reject unknown and out-of-scope sources, conflicts, category mismatch, stale revision, forged evidence and every bound. Read pack status and active schema independently afterward. |
| Evidence ownership | R071.1.5, R097.2.5 | Correlate tenant, graph, pack, vocabulary and schema digests. Inline reports cannot claim committed activation. |
| Core metadata served proof | R071.1.6 | Real server returns the exact golden source/target/relation tuples below, not merely three results. A rejected Person-to-Event mapping preserves the prior schema. |
| Domain served proof | R097.2.6 | Import medical, technology and labor fixtures with mapped and unmapped classes. Query exact coverage, restart, and read it again. |

Proving integration owner: `tests/test_ontology_crosswalk_served.py`. Use generated client operations and independent schema/pack readback. Mocked routes, inline-only inspection and direct helper calls cannot close these rows.

### Golden tuples and source-resolution refusals

For R071.1.2 and R071.1.6, compare these literal tuples after served import and inspection. Preserve each IRI's scheme. A count-only assertion is insufficient.

| Source class IRI | Target class IRI | Declared relation |
|---|---|---|
| `http://knuckles.team/kg#Person` | `http://schema.org/Person` | `SeeAlso` |
| `http://knuckles.team/kg#Event` | `http://schema.org/Event` | `SeeAlso` |
| `http://knuckles.team/kg#Document` | `http://schema.org/DigitalDocument` | `SeeAlso` |

The fixture pins source revision `core-crosswalk-fixture-v1` and target revision `schemaorg-crosswalk-fixture-v1`, with their document digests. These are fixture labels, not claimed upstream releases. The report must return both labels and digests unchanged. EG Document includes digital and physical documents. Its reference does not license `SkosExactMatch`, `owl:sameAs` or a new equivalence axiom. Existing core axioms remain unchanged.

For directional acceptance, the fixture declares Document and DigitalDocument in both source and target vocabularies. DigitalDocument is narrower than Document in each approved fixture. Pin both revisions and assert these exact tuples:

| Source class IRI | Target class IRI | Declared relation |
|---|---|---|
| `urn:eg:crosswalk-fixture:source:DigitalDocument` | `urn:eg:crosswalk-fixture:target:Document` | `SkosBroadMatch` |
| `urn:eg:crosswalk-fixture:source:Document` | `urn:eg:crosswalk-fixture:target:DigitalDocument` | `SkosNarrowMatch` |

These asymmetric fixtures must detect swapped endpoints or relations. The broadMatch target is broader than its source; the narrowMatch target is narrower. Compare emitted predicates and served records separately. A submitted reversed relation must return `CROSSWALK_CONFLICT` against the pinned fixture declaration.

For R071.1.4 and R097.2.4, use valid targets in every source-refusal case:

| Source fixture | Exact refusal | Independent readback |
|---|---|---|
| Undeclared class, or a property IRI used as a class | `CROSSWALK_SOURCE_UNKNOWN` | Pack head, receipts and schema digest remain unchanged. |
| Class declared only by a later source revision | `CROSSWALK_SOURCE_UNKNOWN` | The pinned earlier revision cannot resolve it. |
| Known class in an unselected pack or unauthorized source scope | `CROSSWALK_SOURCE_OUT_OF_SCOPE` | No crosswalk or unmapped entry is published for that class. |

Each refusal identifies the source IRI, requested source revision and selected scope. Exercise these through real import and schema attachment. Query pack status, GraphSchemaClasses and OntologyInspect afterward to prove unchanged committed state. Scope refusal must not disclose foreign class metadata.


## Prompt and residual acceptance matrix

Every test below is proposed. This definition adds no runtime test binding or execution receipt.
Retain all original PACK and TP acceptance above, including every original R001/R002/R094 obligation.

| ID | Positive proof | Negative or boundary proof | Proposed proving test |
|---|---|---|---|
| EG-TYPED-PACKS-R001.1 | Round-trip every original index and entry field. | Unknown kinds and malformed index fields refuse. | `tests/test_connector_pack_contract.py::test_pack_index_entry_contract` |
| EG-TYPED-PACKS-R001.2 | Round-trip every section and annotation field without changing existing semantics. | Invalid section bounds and malformed typed annotations refuse. | `tests/test_connector_pack_contract.py::test_pack_section_annotation_contract` |
| EG-TYPED-PACKS-R001.3 | Preserve ConnectorPackOp and AgentComponent.Content operation and result shapes. | Missing operations and incompatible result shapes fail qualification. | `tests/test_connector_pack_contract.py::test_pack_content_operation_contract` |
| EG-TYPED-PACKS-R001.4 | Every published connector_pack_digest_vectors.json sample matches byte-for-byte. | Changing a framed input or expected digest fails. | `tests/test_connector_pack_contract.py::test_original_framed_digest_vectors` |
| EG-TYPED-PACKS-R001.5 | The complete generated-client inventory shares the original wire shapes and golden bytes. | A stale client, omitted client or changed field fails. | `tests/test_connector_pack_contract.py::test_all_clients_share_pack_contract` |
| EG-TYPED-PACKS-R001.6 | A typed object round-trips all identity and revision fields. | Changing a schema or body pin cannot preserve the old definition identity. | `crates/eg-types/src/agent_component.rs::tests::prompt_blueprint_identity` |
| EG-TYPED-PACKS-R001.7 | Nested valid objects retain every admitted field and extension value. | Missing task, wrong field types, nonfinite numbers and oversized objects refuse. | `crates/eg-types/src/agent_component/facts.rs::tests::prompt_blueprint_schema_refusals` |
| EG-TYPED-PACKS-R001.8 | Repeated identical inputs produce the same bytes and render digest. | A changed variable changes the result; invalid input produces no render result. | `crates/eg-compute/src/assemble/prompt.rs::tests::prompt_render_vectors` |
| EG-TYPED-PACKS-R001.9 | Generated clients encode and decode every declared request, result and typed refusal. | Missing render provenance or an untyped fallback fails. | `tests/test_prompt_blueprint_contract.py::test_generated_prompt_contract` |
| EG-TYPED-PACKS-R002.1 | Staged bytes use their verified SHA-256 address. | A digest mismatch refuses; staging alone exposes no component. | `tests/test_connector_pack_storage.py::test_body_staging` |
| EG-TYPED-PACKS-R002.2 | An unchanged import keeps exact holder/refcount values. | Repeated identical import cannot increment or decrement a holder. | `tests/test_connector_pack_storage.py::test_unchanged_holder` |
| EG-TYPED-PACKS-R002.3 | A revised component binds its new body and preserves historical revision holders. | A changed revision cannot orphan a body held by history. | `tests/test_connector_pack_storage.py::test_revised_holder` |
| EG-TYPED-PACKS-R002.4 | A withdrawn component retains body access through a valid historical pin. | Withdrawal cannot reclaim a held historical body. | `tests/test_connector_pack_storage.py::test_withdrawn_holder` |
| EG-TYPED-PACKS-R002.5 | A fault before owner commit leaves an orphan reclaimed only after its grace window. | An unexpired orphan or held body survives reconciliation and restart. | `tests/test_connector_pack_storage.py::test_orphan_grace_fault` |
| EG-TYPED-PACKS-R002.6 | AgentComponent.Content returns the exact body of its pinned component revision. | Corrupt bytes and denied tenants refuse without leaking body data. | `tests/test_connector_pack_storage.py::test_verified_body_read` |
| EG-TYPED-PACKS-R002.7 | A successful write has one revision and live holder with its exact body digest. | A fault before owner commit leaves only an unreferenced body subject to the existing grace rule. | `tests/test_prompt_blueprints_served.py::test_prompt_owner_commit` |
| EG-TYPED-PACKS-R002.8 | Independent graph queries read the exact prompt revision and dependency links. | Replaying an old event cannot regress the visible revision. | `tests/test_prompt_blueprints_served.py::test_prompt_graph_projection` |
| EG-TYPED-PACKS-R002.9 | A real generated-client request reaches the renderer and returns the pinned revision and expected bytes. | Invalid requests leave graph, body holders, head and render-result publication unchanged. | `tests/test_prompt_blueprints_served.py::test_prompt_render_route` |
| EG-TYPED-PACKS-R002.10 | A valid served journey renders the pinned version and causes one observed model request. | Each invalid case yields a typed refusal and zero requests at the real model transport boundary. | `tests/test_prompt_blueprints_served.py::test_invalid_prompt_prevents_dispatch` |
| EG-TYPED-PACKS-R002.11 | Current and historical revisions, graph links and verified body bytes survive restart. | A refused stale update leaves the previous durable head intact. | `tests/test_prompt_blueprints_served.py::test_prompt_restart_readback` |
| EG-TYPED-PACKS-R094.1 | A generated Python client reads exact verified pack body bytes. | A refusal-only dispatch arm fails this acceptance. | `tests/test_connector_pack_content.py::test_stable_content_returns_body` |
| EG-TYPED-PACKS-R094.2 | Export preserves nested extension values, nulls, arrays and exact numeric values. | An omitted field, changed revision or lost numeric precision fails. | `tests/test_prompt_blueprints_served.py::test_lossless_blueprint_export` |
| EG-TYPED-PACKS-R094.3 | Changing one field creates one new revision and preserves every other field. | Concurrent writers cannot overwrite a newer head; reserved pack identities require pack admission. | `tests/test_prompt_blueprints_served.py::test_modified_blueprint_import` |
| EG-TYPED-PACKS-R094.4 | Replay produces no extra revision, holder or outbox event. | A conflicting retry preserves the prior committed result. | `tests/test_prompt_blueprints_served.py::test_blueprint_retry_identity` |
| EG-TYPED-PACKS-R094.5 | The new revision contains the one intended change and all unchanged values. | Stale import and denied-tenant cases preserve both historical and current readback. | `tests/test_prompt_blueprints_served.py::test_blueprint_roundtrip_restart` |
| EG-TYPED-PACKS-R094.6 | Authorized graph adoption uses the normal publication boundary and records the source revision. | A proposal alone causes no active-head change, model dispatch or external write. | `tests/test_prompt_blueprints_served.py::test_prompt_proposal_has_no_authority` |
| EG-TYPED-PACKS-R094.7 | A real authorized producer journey writes only the approved source and returns a bound receipt. | Absent producers leave this acceptance unfulfilled; graph persistence cannot substitute. | `tests/test_prompt_blueprints_served.py::test_approved_source_effect_dependency` |

### Deterministic fixtures and real served journey

Use synthetic, versioned prompt fixtures with a pinned manifest digest and independently authored expected values.
Include nested custom fields, arrays, Unicode, null and absent fields, empty collections and the exact integer 9007199254740993.
Include an exact decimal 0.10000000000000000001; no binary64 conversion may change it.
The fixture schema explicitly admits both numbers. A narrower schema must refuse rather than round them.
The render fixture requires task="summarize", topic="alpha" and a declared audience variable with value "engineers".
Its canonical rendered bytes must be exactly the following UTF-8 sequence, without a trailing newline:

```json
{"audience":"engineers","task":"summarize","topic":"alpha"}
```

Repeat rendering with object keys reordered. The bytes and digest must remain equal.
Change audience to "reviewers"; only that value and the render digest change.
Test exact and exceeded body, schema, depth, node and output bounds from the admitted profile.

Start a real EG server with disposable durable stores and the required declared features.
Use generated public clients with authenticated identities; do not call private store helpers as the served proof.
Publish a schema and prompt, wait for observable graph readiness, then read them through a second client.
Render the exact prompt revision and observe the named production handler and renderer.
Export that revision, change one nested field, import with its expected base, and read the new revision.
Restart the server and compare graph identity, historical revisions, complete payload, schema pins and body digests.
Retry after a lost reply and assert one committed revision and receipt.

Exercise malformed schemas, invalid payloads, unsupported revisions, missing variables and substituted digest pins separately.
Race two updates from the same base; one succeeds and the stale update refuses.
Use a different actor and tenant for every public read and write boundary.
Assert unchanged head, graph visibility, holders and committed outbox on each rejected mutation.
Uncommitted staged bodies follow the existing orphan grace rule; they must never become readable prompt revisions.

The dispatch proof traverses the actual AU consumer and its model transport against a disposable recording endpoint.
A valid control request produces one observed model call with the pinned rendered bytes.
Every invalid schema, body, revision and scope case produces zero calls and a typed refusal.
No helper-only counter, mocked dispatcher or missing consumer can satisfy R002.10.
Record exact EG and consumer heads, generated schema revisions, request IDs, production call sites and readback digests.
Publish sanitized assertions; exclude credentials, private prompt content and raw request logs.

R094.7 also requires the actual approved SDK/GraphOS source-effect producer journey.
Use a disposable repository and prove exact-path/base-revision approval, conflict refusal and receipt-bound readback.
Use [SDK R004.5 through R004.12](https://github.com/Knuckles-Team/agent-connector-sdk/tree/0c6c53dd63fe2a8f9b2ac3568b6d0e8f2c602734/specs/SDK-GOVERNED-WRITEBACK) and [GraphOS R003.4 through R003.10](https://github.com/Knuckles-Team/graph-os/tree/86737b45aaceb2eebe29b3a98ada5a55f8b71955/specs/fleet-catalog-and-tools).
Their merged definitions identify the exact contracts; this amendment has not run the source-effect journey.

Pass the exact exported bytes through RepositorySourceEffectRequest inside GraphOS RepositoryWriteBackParams.
Serve `fleet.write_back.preview`, then prepare and confirm `fleet.write_back.commit` with the identical full request and trusted binding.
Keep the original SDK effect key, independent recorded approval and commit-operation plan association.
Observe the actual SDK conditional write, separate provider read at its immutable revision, and a fresh durable graph receipt read.
Match complete payload values and exact written bytes/digest/size, approved path, base, actor, tenant, mode and receipt identity.
A returned applied flag or request-key echo is insufficient evidence.

For the original blueprint compatibility obligation, pin a manifest of every prompt blueprint at the selected source revision.
Record the complete file inventory and actual count; the original input described more than sixty prompts.
Load, modify one declared field, export and reimport every entry, preserving all remaining custom fields and exact numeric values.
Check declared JSON formatting and final-newline policy when producing repository bytes; those exact bytes enter the approved SDK payload.
An omitted file or parse/schema failure fails corpus qualification. Lost extension fields and rounded values also fail.
Repository writes remain separately approved, single-file effects; corpus coverage cannot grant a batch write permission.

Repeat absent/mismatched approval, stale base, path escape, changed bytes, wrong actor/tenant and unbound receipt cases through both surfaces.
Each pre-effect refusal must produce zero provider mutations; a later readback failure must preserve uncertainty without an additional write.
Race identical requests and restart GraphOS plus the connector. Recover the same proven receipt or reconcile before any retry.
Record exact EG/SDK/GraphOS implementation heads separately from the merged definition revisions.
No new runtime receipt or lifecycle promotion follows from these contract links.

R002.10 still requires a separately named AU owning requirement and production dispatch call site.
Its existing valid/invalid model-transport acceptance is unchanged. Repository persistence evidence cannot substitute for that consumer proof.

<!-- spec-definition-patch:owner-batch-20261012-typed-packs-and-catalog -->
## Owner acceptance targets — 2026-10-12

All tests below are planned and unexecuted; they create no implementation or served receipts.

- **`EG-TYPED-PACKS-R100.7`:** Matching exact-package passing validation evidence enables the catalog package. Require `run_set_complete=true` and canonical sorted unique run refs. Missing evidence, wrong tenant, candidate/package/graph/policy digest mismatch, incomplete run set, stale source or failed verdict refuses. The refusal leaves no publishable package. Re-run current dangling-ontology refusal. Hold positive AU receipt production and its pass criterion until AU defines the exact operation and rubric. Source record SHA-256: `96ff38761f9e21d52f608ba593c902aff4c93ca7b1ed13088ad39efd3e968a3a`.
- **`EG-TYPED-PACKS-R097.2.7`:** Import a pinned schema with every supported term kind and mapped/unmapped examples. Independently read full coverage through `OntologyInspect` after commit/restart. Read class activation through `GraphSchemaClasses`. Verify the exact sorted `Mapped`/`Unmapped{NoDeclaredMapping}` report and provenance. Verify unmapped terms are not adopted, order/digest are deterministic, partial packs are admitted, and R071.1 reverse links remain unchanged. Each malformed, duplicate, unsupported-kind, kind-mismatch, stale or forged case refuses with pack head and active schema unchanged.
- **`EG-TYPED-PACKS-R100.8`:** Export/import a workflow containing named graphs, unknown predicate, typed and language-tagged literals and ontology references; SHA-256 digest of canonical N-Quads and RDF graph isomorphism must survive. Tampered digest, invalid RDF, unresolved references and unsupported serializer refuse without writes. Equal/older export refusal and newer export admission require a resolved stored-version baseline/order contract; positive end-to-end consumer remains held. Source record SHA-256: `620b1ae05d8e35696c7522f207672ef6f374525004d5488fe62fb4b27fc6cf5a`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run; names describe acceptance obligations, not evidence.

- **`EG-TYPED-PACKS-R069.100` (AU:69; source `8ca81dc16448fefc73440f2eb9ea07b2e433f7e38075563f86800071029846e9`):** Exercise five outcomes. (1) Exact repeated full identity/profile is fully idempotent. (2) An identical blob at another branch/ref or normalized path writes a distinct source binding and provenance. This holds even when parsed content is reused. (3) A new commit with cosmetic-only content writes source revision/provenance but leaves semantic graph revision unchanged. (4) A parser/normalization-profile revision reparses the same raw blob and records the new profile binding. Graph mutation occurs only if the recomputed semantic fingerprint changes. (5) Structural token changes update both source binding and semantic graph revision. In every case, read back source-binding revision/digest separately from semantic graph revision. Unsupported language uses raw digest and is not classified cosmetic. A source identity plus profile key, not raw blob digest alone, determines full no-op. Producer/consumer boundary: RepositorySnapshotProvider -> SDK transport -> generated IndexRepository -> EG parser/index mutation. R079 embedding dedupe is not structural-mutation suppression. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_repository_fingerprint_identity_served.py::test_source_identity_profile_drives_mutation_decision`, `tests/typed_packs/test_repository_fingerprint_identity_served.py::test_unsupported_language_uses_raw_digest_only`. No runtime/test receipt is asserted.

- **`EG-TYPED-PACKS-R100.100` (AU:90; source `1b717c4a284896dbf8c7b4fb1f00ebba80fa6a75e930a020c5bc01706634f582`):** A served export/edit/reimport test changes one frontmatter value and one step. Assert all non-target byte ranges, custom fields, comments, spacing, ordering and CRLF/LF style remain identical. Malformed anchors, overlapping patches, stale source digest and unknown edit spans refuse without writes. A prompt-blueprint JSON roundtrip is not accepted as evidence. Producer/consumer boundary: EG owns skill parse/admission and R100 rendering; SDK owns source transport/edit write-back. Do not substitute a prompt blueprint or whole-document normalization path. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_skill_lossless_edit_served.py::test_frontmatter_and_step_edits_preserve_unmodified_bytes`, `tests/typed_packs/test_skill_lossless_edit_served.py::test_stale_or_ambiguous_edit_refuses_before_write`. No runtime/test receipt is asserted.

- **`EG-TYPED-PACKS-R100.101` (AU:98; source `531cb0ba1accd9e88833249fb253a9f5f7a3c1a911f653b3c9a5bfc42341f978`):** Serve compatible upgrade, breaking upgrade without migration (refused before effects), and breaking upgrade with exact declared migration. Changing a consumer contract, package digest or migration receipt invalidates admission. Keep historical package readable and read back the admitted new version only after validation. Producer/consumer boundary: EG skill package read/admission -> GraphOS invoke certification and consumer code; GraphOS owns its invocation gate. Do not equate existing version/dependency pins with compatibility. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_skill_compatibility_admission_served.py::test_breaking_upgrade_requires_exact_consumer_migration`, `tests/typed_packs/test_skill_compatibility_admission_served.py::test_changed_consumer_contract_invalidates_admission`. No runtime/test receipt is asserted.

- **`EG-TYPED-PACKS-R001.100` (AU:52; source `e03b25156a4f84d27c4afba63d0cd9e526b549b5905994a53664d193416d048c`):** Round-trip nested schemas and version metadata through generated clients and canonical digest vectors. Test additive optional-field evolution, required-field removal, incompatible type change, stale base digest and unknown nested fields. Verify SDK authority remains separate and no tool dispatch occurs from pack admission. Producer/consumer boundary: EG ConnectorPack typed authority -> generated public pack APIs; SDK retains tool compatibility and execution authority. Preserve R001 index/entry/section/annotation and digest contract. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_connector_nested_schema_served.py::test_nested_schema_evolution_preserves_digest`, `tests/typed_packs/test_connector_nested_schema_served.py::test_incompatible_nested_schema_refuses_before_dispatch`. No runtime/test receipt is asserted.

- **`EG-TYPED-PACKS-R072.100` (AU:97; source `f73767d1a3fdba29a702254ed1db2c9904158b6136f6f4b38a3c9a0d3e4fcb03`):** Served registry tests register and read a workflow. Prove no eager AU engine initialization and one initialization on first invocation. Invoke by registry id. Refuse unknown/withdrawn/stale ids with no run. Read back registry and invocation receipt independently. Producer/consumer boundary: EG typed registry and SourceIngest R002 own durable registration; AU owns lazy client initialization/execution. Preserve GRAPHOS-HOST-R022 workflow-store semantics. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_workflow_registry_served.py::test_registration_is_lazy_and_invokes_exact_revision`, `tests/typed_packs/test_workflow_registry_served.py::test_unknown_withdrawn_or_stale_registry_id_refuses`. Owner prerequisite: AU-CONTROL-R037.5 fills only a requirement gap. It does not define lazy initialization timing, the first-use operation or the execution receipt. No exact AU companion requirement ID is identified. Keep that consumer as an explicit hold until its owner publishes the operation and receipt. No invocation evidence is claimed. No runtime/test receipt is asserted.

- **`EG-TYPED-PACKS-R071.100` (EG:8; source `525e39d1d194ac8a820bbd741a1c261ceab0c7d564981c33c731c06c1eb641d8`):** Cross-repository fixtures admit a new unique path. They refuse duplicate canonical paths, alias collisions, incompatible parent assignments and stale vocabulary revisions. Read back canonical path and unchanged published IRIs. Retain R071 generated ontology authority and R001.4 digest vectors. Producer/consumer boundary: EG vocabulary validation owns canonical identity; repository producers submit proposed paths. Preserve existing published IRIs and federated query vocabulary. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_concept_path_admission_served.py::test_owner_approved_path_grammar_is_shared`, `tests/typed_packs/test_concept_path_admission_served.py::test_duplicate_or_conflicting_paths_refuse_without_iri_changes`. Owner decision hold: the canonical segment grammar is not approved for separators/escaping, Unicode normalization, case, or whitespace. Keep admission SPECIFIED until the vocabulary owner publishes one versioned grammar shared by producers; preserve existing published IRIs. No runtime/test receipt is asserted.
<!-- /spec-definition-patch -->

## Connector packs as native RDF (R101, R102)

- `EG-TYPED-PACKS-R101`: Rollup closes when every child `.1` to `.8` passes its proving test.
- `EG-TYPED-PACKS-R101.1`: `crates/eg-core/tests/connector_pack_vocabulary.rs::contract_terms_resolve_with_domain_and_range`.
- `EG-TYPED-PACKS-R101.2`: `crates/eg-shacl/tests/connector_pack_shapes.rs::each_contract_shape_accepts_valid_and_refuses_invalid`.
- `EG-TYPED-PACKS-R101.3`: `src/server/handlers/admin/connector_pack/import/contract_tests.rs::contract_entry_validates_and_projects_triples`.
- `EG-TYPED-PACKS-R101.4`: `src/server/persistence/connector_pack/contract_index_tests.rs::mapping_resolution_reads_contract_triples`.
- `EG-TYPED-PACKS-R101.5`: `src/server/handlers/admin/connector_pack/tests/contract_wiring.rs::import_and_ingest_reach_contract_index`.
- `EG-TYPED-PACKS-R101.6`: `src/server/handlers/admin/connector_pack/import/contract_tests.rs::contract_refusals_leave_state_unchanged`.
- `EG-TYPED-PACKS-R101.7`: `src/server/handlers/source_ingestion/contract_receipt_tests.rs::receipt_binds_contract_digest_and_governed_by`.
- `EG-TYPED-PACKS-R101.8`: `tests/typed_packs/test_ingest_contract_rdf_served.py::test_contract_import_governs_ingest`.
- `EG-TYPED-PACKS-R102`: Rollup closes when every child `.1` to `.7` passes its proving test.
- `EG-TYPED-PACKS-R102.1`: `crates/eg-shacl/tests/connector_pack_shapes.rs::surface_shapes_accept_valid_and_refuse_invalid`.
- `EG-TYPED-PACKS-R102.2`: `src/server/connector_pack_projection/surface_tests.rs::surface_nodes_follow_pack_head`.
- `EG-TYPED-PACKS-R102.3`: `src/server/handlers/admin/connector_pack/import/contract_tests.rs::core_class_redeclaration_is_refused`.
- `EG-TYPED-PACKS-R102.4`: `src/server/connector_pack_projection/surface_tests.rs::outbox_consumer_writes_surface_nodes`.
- `EG-TYPED-PACKS-R102.5`: `src/server/handlers/admin/connector_pack/import/contract_tests.rs::surface_refusals_leave_state_unchanged`.
- `EG-TYPED-PACKS-R102.6`: `src/server/connector_pack_projection/surface_tests.rs::surface_nodes_carry_pack_digest`.
- `EG-TYPED-PACKS-R102.7`: `tests/typed_packs/test_connector_surface_rdf_served.py::test_surface_query_follows_pack_head`.
- `EG-TYPED-PACKS-R103`: Rollup closes when every child `.1` to `.6` passes its proving test.
- `EG-TYPED-PACKS-R103.1`: `crates/eg-types/src/graph_schema.rs::tests::ontology_import_ops_round_trip`.
- `EG-TYPED-PACKS-R103.2`: `src/server/graph_schema/ontology_import_tests.rs::import_is_idempotent_by_digest_and_revises`.
- `EG-TYPED-PACKS-R103.3`: `src/server/graph_schema/ontology_import_tests.rs::gateway_routes_ontology_import`.
- `EG-TYPED-PACKS-R103.4`: `src/server/graph_schema/ontology_import_tests.rs::ontology_import_refusals_leave_schema_unchanged`.
- `EG-TYPED-PACKS-R103.5`: `src/server/graph_schema/ontology_import_tests.rs::revision_records_digest_importer_and_classes`.
- `EG-TYPED-PACKS-R103.6`: `tests/typed_packs/test_runtime_ontology_import_served.py::test_import_revise_withdraw_end_to_end`.

## EG-TYPED-PACKS-R104 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `EG-TYPED-PACKS-R104.1` | `crates/eg-types/src/connector_pack/seed_graph.rs::tests::seed_import_round_trips`: A serde round trip keeps every field and rejects an unknown part key. |

| Behavior | `EG-TYPED-PACKS-R104.2` | `src/server/handlers/admin/connector_pack/seed_tests.rs::changed_part_reimports_alone`: A second import with one changed part replaces that part and leaves the other two byte-identical. |

| Wiring | `EG-TYPED-PACKS-R104.3` | `src/server/handlers/admin/connector_pack/seed_tests.rs::served_import_projects_seed`: A served import with a seed body reaches `project_head` and writes the seed triples. |

| Refusal | `EG-TYPED-PACKS-R104.4` | `src/server/handlers/admin/connector_pack/seed_tests.rs::seed_refusals_write_nothing`: Each case returns its code and the graph revision stays unchanged. |

| Evidence | `EG-TYPED-PACKS-R104.5` | `src/server/handlers/admin/connector_pack/seed_tests.rs::receipt_names_part_outcomes`: A served SPARQL read finds the receipt and the `seed:fromSeed` link of one catalog object. |

| Hot-path proof | `EG-TYPED-PACKS-R104.6` | `tests/connector_pack/test_seed_graph_served.py::test_seed_idempotent_and_partial_reimport`: The repeat returns `unchanged`, the change replaces only `catalog`, and the query sees the new objects. |


Hot-path proof for `EG-TYPED-PACKS-R104`: `tests/connector_pack/test_seed_graph_served.py::test_seed_idempotent_and_partial_reimport`.
