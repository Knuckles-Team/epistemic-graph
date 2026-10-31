# Verification and acceptance contract

Tests run against disposable fixtures per connector; no live credential is part of the PR gate. Each `.3` row asserts that both its questions return a non-empty answer with provenance fields (source, record id, as-of), or a typed abstention naming the unconnected source when a linked connector has no data. Negative proof: a question whose required source is absent must abstain, never return an empty success.

## Vocabulary acceptance

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-AUD-001 | file rollups MEDULLA-CQ-CONN-R068 to R082 | Contract | Load each child row's ontology through the `CoreSpec` registry. | The module declares the added class or property with the stated domain and range; the module still loads. | PENDING |
| `ontology_audit_software::Repository_declared` | MEDULLA-CQ-CONN-R069.2 | Contract | Load registered `core:software@1` and `core:sdlc_lifecycle@1` sources | The software module declares `:Repository` with its `:Collection` parent; a lifecycle-module declaration fails | PENDING |
| `ontology_audit_software::Branch_declared` | MEDULLA-CQ-CONN-R069.3 | Contract | Load registered `core:software@1` and `core:sdlc_lifecycle@1` sources | The software module declares `:Branch` with its BFO parent; a lifecycle-module declaration fails | PENDING |
| T-AUD-002 | children with a SHACL shape | Contract | Validate a fixture node that violates the shape (for example `filledQty` above `orderQty`) | Validation reports the shape's message | PENDING |
| T-AUD-003 | MEDULLA-CQ-CONN-R083 | Docs | Parse the Coverage cells for erpnext, salesforce and twenty after the `MEDULLA-SUPPLY-VOCAB-R003` terms land | `commercial_connector_prerequisites` checks all three Coverage rows and both Vocabulary cells. Producers in MEDULLA-SUPPLY-VOCAB-R003.7 through R003.17 resolve. Missing producers, unrelated-row-only edits and duplicate incompatible declarations fail; unavailable live fields cause typed abstention | PENDING |

A term added to the wrong module must fail T-AUD-001 for the module named in the row. T-AUD-001 also asserts the exact full IRI and range; T-AUD-002 requires both valid and invalid fixtures.

## Noncommercial mapping acceptance

Each test below binds to the existing acceptance child and its producer list in `requirements.md`.
Setup must load producer modules through the CoreSpec registry and use disposable documentation fixtures.
The positive fixture must match the exact Coverage row and both question Vocabulary cells.
It must resolve every required full IRI from the specified producer module.
Negative fixtures must remove each producer term and substitute a same-local-name IRI from another module.
Separate negative fixtures must alter only an unrelated row or retain one incorrect target cell.
Each negative fixture must fail; unrelated edits must never satisfy the target connector's acceptance.
Decomposition lint must pass after each accepted mapping update.
These tests remain PENDING; this documentation change supplies no implementation or runtime evidence.

| Test name | Requirement | Expected primary vocabulary | Evidence |
|---|---|---|---|
| `connector_audit_reassignment::ciso_assistant` | `MEDULLA-CQ-CONN-R011.3` | `government-v1.ttl` | PENDING |
| `connector_audit_reassignment::dockerhub` | `MEDULLA-CQ-CONN-R015.3` | `infrastructure-v1.ttl` | PENDING |
| `connector_audit_reassignment::egeria` | `MEDULLA-CQ-CONN-R017.3` | `core-foundation-v1.ttl` | PENDING |
| `connector_audit_reassignment::github` | `MEDULLA-CQ-CONN-R023.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::gitlab` | `MEDULLA-CQ-CONN-R024.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::gramps` | `MEDULLA-CQ-CONN-R025.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::home_assistant` | `MEDULLA-CQ-CONN-R026.3` | `infrastructure-v1.ttl` | PENDING |
| `connector_audit_reassignment::langfuse` | `MEDULLA-CQ-CONN-R031.3` | `orchestration-v1.ttl` | PENDING |
| `connector_audit_reassignment::legal_peripherals` | `MEDULLA-CQ-CONN-R033.3` | `core-foundation-v1.ttl` | PENDING |
| `connector_audit_reassignment::nextcloud` | `MEDULLA-CQ-CONN-R040.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::onetrust` | `MEDULLA-CQ-CONN-R043.3` | `government-v1.ttl` | PENDING |
| `connector_audit_reassignment::paperless_ngx` | `MEDULLA-CQ-CONN-R046.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::repository_manager` | `MEDULLA-CQ-CONN-R052.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::servicenow` | `MEDULLA-CQ-CONN-R057.3` | `archimate-v1.ttl` | PENDING |
| `connector_audit_reassignment::stirlingpdf` | `MEDULLA-CQ-CONN-R059.3` | `action-v1.ttl` | PENDING |

## Connector-pack competency questions (R084)

- `MEDULLA-CQ-CONN-R084`: Rollup closes when every child `.1` to `.6` passes its proving test.
- `MEDULLA-CQ-CONN-R084.1`: `crates/medulla-core/tests/connector_pack_competency.rs::finance_algorithm_selection_answers`.
- `MEDULLA-CQ-CONN-R084.2`: `crates/medulla-core/tests/connector_pack_competency.rs::positions_and_applicable_risk_analyses_answer`.
- `MEDULLA-CQ-CONN-R084.3`: `crates/medulla-core/tests/connector_pack_competency.rs::capability_to_surface_and_contract_answers`.
- `MEDULLA-CQ-CONN-R084.4`: `crates/medulla-core/tests/connector_pack_competency.rs::record_governance_answers`.
- `MEDULLA-CQ-CONN-R084.5`: `crates/medulla-core/tests/connector_pack_competency.rs::finance_method_terms_and_shape`.
- `MEDULLA-CQ-CONN-R084.6`: `tests/typed_packs/test_connector_pack_competency_served.py::test_pack_questions_end_to_end`.

- `MEDULLA-CQ-CONN-R085`: See each child.
- `MEDULLA-CQ-CONN-R085.1`: `crates/medulla-core/tests/connector_auto_onboard.rs::fixture_parses_and_is_linked_to_the_parent`.
- `MEDULLA-CQ-CONN-R085.2`: `crates/medulla-core/tests/connector_auto_onboard.rs::parent_term_question_returns_native_and_live_connector_rows`.
- `MEDULLA-CQ-CONN-R085.3`: `tests/typed_packs/test_connector_auto_onboard_served.py::test_check_uses_production_import_and_scan`.
- `MEDULLA-CQ-CONN-R085.4`: `crates/medulla-core/tests/connector_auto_onboard.rs::orphan_and_held_variants_abstain_by_name`.
- `MEDULLA-CQ-CONN-R085.5`: `tests/typed_packs/test_connector_auto_onboard_served.py::test_answer_cites_digest_revision_fetch_time_and_load_set`.
- `MEDULLA-CQ-CONN-R085.6`: `tests/typed_packs/test_connector_auto_onboard_served.py::test_onboarded_connector_answers_a_cross_ontology_live_question`.
