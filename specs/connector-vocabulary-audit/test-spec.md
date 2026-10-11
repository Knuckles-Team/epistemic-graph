# EG-CONN-AUDIT-001 — Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | file rollups R001 to R016 | Contract | Load each ontology module named in the child row through the `CoreSpec` registry | The added class or property is declared with the stated domain and range; the module still loads | PENDING |
| T-002 | children with a SHACL shape | Contract | Validate a fixture node that violates the shape (for example `filledQty` above `orderQty`) | Validation reports the shape's message | PENDING |
| T-003 | commerce-v1 setup row | Contract | Load `commerce` v1 and the regenerated catalog | Module present in the catalog; no duplicate `:Contact` across modules | PENDING |
| T-004 | R017 Reassignments | Docs | Run `pipelines-hook spec-decomposition` after the parent spec applies the edit | Clean; each reassigned connector's Coverage row names the new file | PENDING |

## Negative and boundary cases

A term added to the wrong module must fail T-001 for the module named in the row. A reassignment applied before its producer term rows land must fail T-004 (term absent from the target file).
