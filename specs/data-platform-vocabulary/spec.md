# EG-DATAPLAT-VOCAB-001 — Data-platform vocabulary

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: EG-DATAPLAT-VOCAB-R009.1 to .5 (objectstore-mcp, vector-mcp, openbao-mcp, clarity-api, data-science-mcp).
Public provenance: gap found by `specs/connector-competency` (EG-CQ-CONN-001), rows R012, R014, R016, R029, R041, R044, R058, R065.

## Purpose and user stories

As the engine, I need one typed vocabulary for the data platform stores (sql, documentdb, objectstore, vector, kafka, openbao, clarity, data-science) so a competency question over them can be answered through each connector's access contract. Several of these are themselves the contract kinds through which other data is reached, so the vocabulary describes a store as a source and how its contents map to domain concepts.

## Requirements and acceptance

- FR-001: `core:data-platform@1` defines the classes and properties in [plan.md](plan.md) and is registered next to `core:virtual-graph@1` (R001, R002, R003, R004).
- FR-002: Secret metadata only. No term, shape or mapping can carry a secret value; a violating triple and a value-reading mapping are refused (R005).
- FR-003: Every store is virtual-first; a `materialised` mapping must state a reason (R006, R007).
- FR-004: Schema discovery is engine-side (unified-data-plane R003, R004, R038.2); this spec adds no discovery path (R007.3).
- FR-005: Package-local ontologies align to the `dp:` terms (R008).
- FR-006: Operations the competency questions assume that do not exist are recorded as cross-repo rows (R009).
- SC-001: every row in [requirements.md](requirements.md) is LANDED with its named test.

## Scope and interfaces

Vocabulary, shapes, mapping fixtures and validator rule in epistemic-graph. Mechanisms cited, not re-specified: `specs/unified-data-plane` (R003, R004, R005, R005.1, R006, R013, R019, R019.1, R037, R038, R038.2), `specs/connector-competency`, `specs/federated-query-and-reasoning`. Non-goals: discovery, pushdown, CDC, connector code, any secret value.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md Vocabulary | T-001, T-002 | PENDING |
| FR-002 | plan.md Decisions 11 | T-003, T-004, T-009 | PENDING |
| FR-003 | plan.md Decisions 4 | T-005 | PENDING |
| FR-004 | plan.md Decisions 3 | T-006 | PENDING |
| FR-005 | plan.md Architecture | T-007 | PENDING |
| FR-006 | plan.md Decisions 5-9 | T-008 | PENDING |

## Open questions

- None. Items 5-9 in plan.md Decisions are BLOCKED on named connector operations, owned by the rows in R009.
