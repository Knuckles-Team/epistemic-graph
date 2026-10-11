# Verification and acceptance contract

Tests run against disposable fixtures per connector; no live credential is part of the PR gate. Each `.3` row asserts that both its questions return a non-empty answer with provenance fields (source, record id, as-of), or a typed abstention naming the unconnected source when a linked connector has no data. Negative proof: a question whose required source is absent must abstain, never return an empty success.

## Vocabulary audit rows (folded from EG-CONN-AUDIT, 2026-10-11)

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-AUD-001 | file rollups EG-CQ-CONN-R068 to R082 | Contract | Load each ontology module named in the child row through the `CoreSpec` registry | The added class or property is declared with the stated domain and range; the module still loads | PENDING |
| T-AUD-002 | children with a SHACL shape | Contract | Validate a fixture node that violates the shape (for example `filledQty` above `orderQty`) | Validation reports the shape's message | PENDING |
| T-AUD-003 | EG-CQ-CONN-R083 | Docs | Parse the Coverage cells for erpnext, salesforce and twenty after the `EG-SUPPLY-VOCAB-R003` terms land | `commercial_connector_prerequisites` checks all three Coverage rows and both Vocabulary cells. Producers in EG-SUPPLY-VOCAB-R003.7 through R003.17 resolve. Missing producers, unrelated-row-only edits and duplicate incompatible declarations fail; unavailable live fields cause typed abstention | PENDING |

A term added to the wrong module must fail T-AUD-001 for the module named in the row. T-AUD-001 also asserts the exact full IRI and range; T-AUD-002 requires both valid and invalid fixtures.
