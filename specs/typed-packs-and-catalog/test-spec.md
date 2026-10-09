# Test and quality specification

## Evidence rule

Tests may run in an isolated ephemeral EG/SDK environment using synthetic packs and local engine state; no private service, inventory or live tenant is a prerequisite. Record exact commit, command, fixture version, count, exit status and relevant artifact digest. A skipped or unavailable gate remains unverified. Every refusal asserts unchanged head, members, holders, records, body liveness and outbox, not only the error string.

| Test ID | Requirements | Fixture/action | Expected result and evidence |
|---|---|---|---|
| TP-01 | PACK-01, 05, 10 | Replay `contract/fixtures/connector_pack_digest_vectors.json` in Rust and SDK; permute archive offsets/order and package version; change one body byte. | Semantic digest stable under repackaging, changes on content, server pins follow declared contract, and generated fixtures/receipt match committed bytes. |
| TP-02 | PACK-01, 06 | Plant one failing index/archive case for G1–G6: unsorted/duplicate URI, wrong scheme/kind, two servers, each size edge, missing/corrupt archive, overflow/overlap/gap, wrong digest, forbidden or duplicate component ID. | Named code; no write; mutation deletion of each validation guard makes its test fail. |
| TP-03 | PACK-06 | Plant G7–G12: BOM, invalid UTF-8, duplicate JSON key, too deep/nodes, NaN, missing tool input schema, YAML alias/tag/front-matter mismatch, malformed native IRI, foreign capability claim, invalid currency/cost/latency/model values. | Invalid bodies refused; foreign absolute capability IRI accepted as unresolved claim with warning; empty description accepted with fallback warning. |
| TP-04 | PACK-06, 08 | Plant G13–G21/K1–K7: `@base`, remote `owl:imports`, overlong IRI, inconsistent ontology, derivation budget exhaustion, `SERVICE SILENT`, per-file blank-node collision, conflicting shape owner, SHACL violation, bad/cyclic ref, invalid draft, unbound importer, >50% withdrawal, 300 violations and stale schema source. | Exact refusal/warning and bounded runtime; 256 maximum violations with exhaustion flag; no remote fetch or partial mutation. The planted G14 and malformed G15 cases must be red before the fix and green after. |
| TP-05 | PACK-03, 04 | Import same pack twice; lose response and retry same operation identity; retry with different bytes; race two expected heads; change only package version; change server contract version; remove and restore one tool. | No-op creates no revision; replay returns same receipt; changed bytes conflict; one racing head refuses; contract change cascades pins, package-only change does not; withdrawal/republication preserves ID. |
| TP-06 | PACK-03, 11 | Kill child after body store and before owner commit, during owner commit, after commit and before response; cancel timed-out import and immediately Bind same tenant. | Orphan bodies are reclaimed after grace; owner state is all-or-nothing; receipt replays; lock is released and next operation is not blocked. |
| TP-07 | PACK-02, 04, 11 | Authorized and unauthorized `Status`, `Content`, Bind/Unbind/Retire/Reproject, direct `mcp:` Publish, cross-tenant and graph reads, clustered writer. | Authz actions and stable codes match descriptors; no cross-tenant bytes/metadata; reserved ID and LocalOnly refusals; historic pin may resolve withdrawn revision but new selection cannot. |
| TP-08 | PACK-07, 08 | Crash projection between graph attach and visibility ack, inject graph grant denial, process stale outbox event after newer head, retry failed event, attach pack snapshot to another graph. | At most one visible revision; stale worker refuses; denial is durable failed status; later events are not collateral-dead-lettered; static attachment reports staleness. |
| TP-09 | PACK-08 | Attach two compatible sources, conflicting shape owner, duplicate shared class, source bound overflow, `IcvConfigure` compatibility and attach→detach→attach same bytes; compare `GraphSchemaClasses`. | Composed policy/digest and class list deterministic; only real conflict refuses; cache cannot suppress repeated attach; no retroactive data scan is implied. |
| TP-10 | PACK-09, 10 | Run generated-contract fixpoint and committed-artifact comparison; replay method-body signing vectors in Python/Go/JS; every OpKind/PredKind printer/parser round trip, 512 property cases and fuzz target. | Receipt and generated files match by content; every public client signs and decodes same typed contract; no variant silently falls to Dynamic. |
| TP-11 | PACK-09, 12 | Synthetic connector from SDK manifest/tool probe through pack import, visible projection, fleet catalog join and body read; restart and restore. | Content and liveness have distinct authorities; no duplicate catalog row; served result matches local typed result after restart. |
| TP-12 | PACK-12 | Full Rust nextest suite including integration binaries, root Python suite, Go and JS client suites, planted pack tests and release workflow replica at the exact head. | Zero failures, zero signal-killed hidden tests and no omitted root suite. If a test binary dies by signal, rerun that binary under process-isolated nextest and account for every test. |
| TP-13 | PACK-08, 10 | Migrate a representative ontology object, schema pack DTO and schema-drift candidate through EG typed/generated APIs; compare prior read/write behavior and provenance before deleting duplicate types. | One EG schema authority, no lost semantics, no caller-owned SHACL rendering and deterministic drift gate refusal/acceptance. |

## Quality and release gates

The repository pins CCCC 1.6.0, KISS 0.4.12, Dupehound 0.1.2 (`threshold 0.85`, at least 40 tokens), and jscpd 5.0.16 (at least 50 tokens and 5 lines, mild mode) in `pyproject.toml`; use the repository's scanner source manifest and release workflow rather than arbitrary file subsets. `release.yml` runs scanner install and validation; `scripts/check_dupehound_census.py`, `scripts/validate_cccc_census.py`, `scripts/check_kiss_census.py`, and jscpd differential are the configured proofs. Keep generated contract artifacts in their explicit generated exclusions only; source duplicate pairs need resolution or reviewed explanation. Apply KISS by extending typed owner modules and a shared validator instead of a second registry, parser or cache. Run Rust fmt/Clippy, Python lint/type/test, `gen_contract --check`, contract generated-artifact test, architecture/pin-resolution checks and release gate replica where applicable. The current gate's exact command should be copied into the evidence, not replaced with an invented score.

## Acceptance matrix

PACK-01: TP-01/02; PACK-02: TP-07/10; PACK-03: TP-05/06; PACK-04: TP-05/07; PACK-05: TP-01/04; PACK-06: TP-02/03/04; PACK-07: TP-08; PACK-08: TP-04/09/13; PACK-09: TP-10/11; PACK-10: TP-01/10/13; PACK-11: TP-06/07/08; PACK-12: TP-11/12 and all quality gates. A requirement is `ACCEPTED` only when its mapped tests and applicable gates pass against a merged exact head.

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
