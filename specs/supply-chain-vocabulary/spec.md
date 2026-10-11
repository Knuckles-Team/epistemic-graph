# EG-SUPPLY-VOCAB — Supply-chain and service-impact vocabulary

Owner: epistemic-graph. Cross-repo IDs: agent-packages connectors (`erpnext-agent`, `uptime-kuma-agent`, `container-manager-mcp`, `salesforce-agent`, `fan-manager`: class resolution and access contracts), `EG-CQ-XSRC-001` (consumer), `EG-UNIFIED-DATA-PLANE-R030` and `EG-FEDERATED-QUERY-R072` (mechanism).
Public provenance: gap found while reading `specs/cross-source-competency` (`plan.md` MISSING list); decisions are recorded in [plan.md](plan.md#decisions).

## Purpose and user stories

`EG-CQ-XSRC-001` answers "How does the supply chain affect our services?" but needs about fifteen ontology terms that do not exist in `crates/eg-core/ontology/`. This spec designs that vocabulary end to end so each term has an owner file, a domain and range, a SHACL shape, a source that can really supply it, and a place in the impact traversal.

- As an operator I ask which suppliers, purchased items or software components stand behind a service, and which are vulnerable, late or over a limit.
- As a customer-facing owner I ask which open incidents or changes touch a service that a customer contract covers.
- As the engine I choose sources from the ontology's declarations and fetch from them at query time; no source is copied.

## Requirements and acceptance

- FR-001: The physical chain is expressible as supplier to item to asset to host to service, using only terms declared in `supply_chain-v1.ttl` plus existing `infra:` and `trm` terms.
- FR-002: The software chain is expressible as component version to image to workload to service, with publisher and vulnerability attached.
- FR-003: Customer, contract, covered service and the incident or change that touches a service are expressible without a duplicate of `:Change` or `:Incident`.
- FR-004: Host temperature and capacity limits are expressible as a normalized `infra:HostLimit` row, and "over limit" is a query condition, not a stored fact.
- FR-005: Service to business capability uses one declared property, `:supportsCapability`, mapped from each source's native relation.
- FR-006: Every concept has a recorded source table: which connector operation supplies it (CONFIRMED from the tool list) or that no connected source can, in which case the report abstains with a typed reason.
- FR-007: Entities from different sources join only across an approved `owl:sameAs` (`EG-UNIFIED-DATA-PLANE-R030`); this spec supplies the blocking keys per concept.
- FR-008: EG-SUPPLY-VOCAB-R003.13 through R003.17 require commercial registry, namespace and Opportunity date acceptance.
- SC-001: Each new `.ttl` parses, each shape accepts a valid fixture and refuses each fault, and the class-only impact query in plan.md parses against the ontology.

## Scope and interfaces

Owned: new terms and shapes in `supply_chain-v1.ttl`, `commercial-v1.ttl`, `infrastructure-v1.ttl` (host limits), their mappings as `vg:VirtualMapping` fixtures, blocking keys, and the impact query. Not owned: authority declaration (`EG-CQ-XSRC-R001`), federated planning, the report model (`EG-CQ-XSRC-R006`), entity-resolution scoring (`R030`), connector internals (agent-packages).

Non-goals: ingesting any source's data; a new decision or planning mechanism; ArchiMate model authoring.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md Vocabulary (physical) | T-001 | PENDING |
| FR-002 | plan.md Vocabulary (software) | T-002 | PENDING |
| FR-003 | plan.md Vocabulary (commercial), Decisions D3 | T-003 | PENDING |
| FR-004 | plan.md Vocabulary (host limits) | T-004 | PENDING |
| FR-005 | plan.md Decisions D1 | T-005 | PENDING |
| FR-006 | plan.md Decisions D2, D4 | T-006 | PENDING |
| FR-007 | plan.md Entity resolution | T-007 | PENDING |
| FR-008 | plan.md Amendments | T-COM-013 through T-COM-017 | PENDING |
| SC-001 | plan.md Worked example | T-008 | PENDING |

## Open questions

- None blocking the ontology. Source gaps, including the unverified Item-to-Asset link, are recorded as BLOCKED in plan.md D4 with the owning repository.
