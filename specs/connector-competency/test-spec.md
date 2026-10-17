# Verification and acceptance contract

Tests run against disposable fixtures per connector; no live credential is part of the PR gate. Each `.3` row asserts that both its questions return a non-empty answer with provenance fields (source, record id, as-of), or a typed abstention naming the unconnected source when a linked connector has no data. Negative proof: a question whose required source is absent must abstain, never return an empty success.

## Vocabulary acceptance

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-AUD-001 | file rollups EG-CQ-CONN-R068 to R082 | Contract | Load each ontology module named in the child row through the `CoreSpec` registry | The added class or property is declared with the stated domain and range; the module still loads | PENDING |
| T-AUD-002 | children with a SHACL shape | Contract | Validate a fixture node that violates the shape (for example `filledQty` above `orderQty`) | Validation reports the shape's message | PENDING |
| T-AUD-003 | EG-CQ-CONN-R083 | Docs | Parse the Coverage cells for erpnext, salesforce and twenty after the `EG-SUPPLY-VOCAB-R003` terms land | `commercial_connector_prerequisites` checks all three Coverage rows and both Vocabulary cells. Producers in EG-SUPPLY-VOCAB-R003.7 through R003.17 resolve. Missing producers, unrelated-row-only edits and duplicate incompatible declarations fail; unavailable live fields cause typed abstention | PENDING |

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
| `connector_audit_reassignment::ciso_assistant` | `EG-CQ-CONN-R011.3` | `government-v1.ttl` | PENDING |
| `connector_audit_reassignment::dockerhub` | `EG-CQ-CONN-R015.3` | `infrastructure-v1.ttl` | PENDING |
| `connector_audit_reassignment::egeria` | `EG-CQ-CONN-R017.3` | `core-foundation-v1.ttl` | PENDING |
| `connector_audit_reassignment::github` | `EG-CQ-CONN-R023.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::gitlab` | `EG-CQ-CONN-R024.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::gramps` | `EG-CQ-CONN-R025.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::home_assistant` | `EG-CQ-CONN-R026.3` | `infrastructure-v1.ttl` | PENDING |
| `connector_audit_reassignment::langfuse` | `EG-CQ-CONN-R031.3` | `orchestration-v1.ttl` | PENDING |
| `connector_audit_reassignment::legal_peripherals` | `EG-CQ-CONN-R033.3` | `core-foundation-v1.ttl` | PENDING |
| `connector_audit_reassignment::nextcloud` | `EG-CQ-CONN-R040.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::onetrust` | `EG-CQ-CONN-R043.3` | `government-v1.ttl` | PENDING |
| `connector_audit_reassignment::paperless_ngx` | `EG-CQ-CONN-R046.3` | `personal-v1.ttl` | PENDING |
| `connector_audit_reassignment::repository_manager` | `EG-CQ-CONN-R052.3` | `sdlc_lifecycle-v1.ttl` | PENDING |
| `connector_audit_reassignment::servicenow` | `EG-CQ-CONN-R057.3` | `archimate-v1.ttl` | PENDING |
| `connector_audit_reassignment::stirlingpdf` | `EG-CQ-CONN-R059.3` | `action-v1.ttl` | PENDING |

## Connector-pack competency questions (R084)

- `EG-CQ-CONN-R084`: Rollup closes when every child `.1` to `.6` passes its proving test.
- `EG-CQ-CONN-R084.1`: `crates/eg-core/tests/connector_pack_competency.rs::finance_algorithm_selection_answers`.
- `EG-CQ-CONN-R084.2`: `crates/eg-core/tests/connector_pack_competency.rs::positions_and_applicable_risk_analyses_answer`.
- `EG-CQ-CONN-R084.3`: `crates/eg-core/tests/connector_pack_competency.rs::capability_to_surface_and_contract_answers`.
- `EG-CQ-CONN-R084.4`: `crates/eg-core/tests/connector_pack_competency.rs::record_governance_answers`.
- `EG-CQ-CONN-R084.5`: `crates/eg-core/tests/connector_pack_competency.rs::finance_method_terms_and_shape`.
- `EG-CQ-CONN-R084.6`: `tests/typed_packs/test_connector_pack_competency_served.py::test_pack_questions_end_to_end`.
