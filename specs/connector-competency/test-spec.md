# Verification and acceptance contract

Tests run against disposable fixtures per connector; no live credential is part of the PR gate. Each `.3` row asserts that both its questions return a non-empty answer with provenance fields (source, record id, as-of), or a typed abstention naming the unconnected source when a linked connector has no data. Negative proof: a question whose required source is absent must abstain, never return an empty success.

## Vocabulary audit rows (folded from EG-CONN-AUDIT, 2026-10-11)

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

## Historical retirement validation

| Historical requirement | Check | Evidence |
|---|---|---|
| `EG-CONN-AUDIT-R001` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.9` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.10` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.11` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.12` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.13` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.14` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.15` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.16` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.17` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.18` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.19` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.20` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.21` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.22` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.23` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.24` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.25` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R001.26` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.9` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.10` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.11` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.12` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.13` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.14` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.15` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.16` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.17` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.18` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.19` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.20` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.21` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.22` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R002.23` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R003.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R004.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R005.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.9` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.10` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R006.11` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R007.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R008.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R009.9` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R010` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R010.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R010.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R010.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R011.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R012.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R013` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R013.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R013.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R013.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R013.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R014.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R015` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R015.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.1` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.2` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.3` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.4` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.5` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.6` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.7` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.8` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.9` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.10` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.11` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.12` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.13` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.14` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.15` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.16` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.17` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
| `EG-CONN-AUDIT-R017.18` | Exact replacement mapping, unique tombstone, generated RETIRED state, unchanged active rows. | Historical lineage validation in plan.md. |
