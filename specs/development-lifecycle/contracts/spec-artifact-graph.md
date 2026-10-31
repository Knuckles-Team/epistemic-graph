# Seam contract: spec artifact graph

Governing spec: [spec.md](../spec.md).

## Provider

Repository: medulla. Rows: `MEDULLA-SDLC-R014.1`, `MEDULLA-SDLC-R014.4`, `MEDULLA-SDLC-R014.6`, `MEDULLA-SDLC-R014.7`, `MEDULLA-SDLC-R005.7`.

## Consumer

Repository: arbora. Rows: `ARBORA-OPS-R896.1` (artifact reads), `ARBORA-OPS-R896.3` (start a run) and `ARBORA-OPS-R896.4` (observe a run).

## Wire shape

Arbora serves four read questions through the intent tools `ask` and `find`. Each maps to one registered competency query of `MEDULLA-SDLC-R014.6`. Arbora holds no per-question operation.

| Question | Request | Answer |
|---|---|---|
| Next task | `{repo, phase}` | `{task_id, row_id, plan_section, contracts[], code_items[], digests[]}` |
| Task context | `{repo, task_id}` | `{row_text, plan_sections[], contracts[], data_model?, neighbourhood[]}` |
| Plan section | `{repo, spec_id, section}` | `{heading, body_digest, rows[], tasks[]}` |
| Contract | `{repo, seam}` | `{provider, consumer, wire_shape, refusal_codes[], types[], vectors[]}` |

Every answer carries the record digests it came from. The caller's tenant and visibility bound each answer.

## Refusal codes

- `COMPETENCY_SUBJECT_UNKNOWN`: the phase, goal or task does not exist.
- `CONTRACT_ROW_UNRESOLVED`: a contract names a row that no spec holds.
- `REQUIREMENT_UNRESOLVED`: an ID resolves to no row.
- `TASK_CONTEXT_PLAN_MISSING`: the row has no covering plan section.
- `SPEC_ARTIFACT_UNPARSEABLE`: an artifact line matches no known form.

## Authoritative code types

- `crates/medulla-types/src/lifecycle.rs`: `SpecStatusAnswer`, `SpecRowState`, and the task and plan record types.
- `crates/medulla-core/ontology/sdd-spec-v1.ttl`: the classes and properties of the pack.

## Test vectors

- `tests/sdlc/test_spec_competency_served.py::test_next_unblocked_task_for_phase` holds the next-task vector.
- `tests/sdlc/test_task_dispatch_served.py::test_context_holds_row_plan_contracts_and_neighbourhood` holds the task-context vector.
- `tests/sdlc/test_seam_graph_served.py::test_concept_resolves_to_owning_contract_and_both_repositories` holds the contract vector.
