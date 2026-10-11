# EG-CQ-XSRC — Cross-source competency: supply chain to services

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: graph-os (served intent tool, connected-source list), agent-packages connectors (operation contracts for erpnext, uptime, container-manager).
Public provenance: program exit gate for the Graph OS refactor.

## Purpose and user stories

This is the exit gate. An operator asks one question and gets one report that joins supply-chain data with service data. The ontology, not the query, says which connected source is authoritative for each concept. This is the Stardog-style capability: SPARQL, OWL, RDF and SHACL over virtual graphs on connectors.

Exit-gate question, verbatim: **"How does the supply chain affect our services?"**

Generalization questions, verbatim:

- "Which services are exposed by a vulnerability in an image we deploy?"
- "Which business capabilities depend on a host that is over its temperature or capacity limit?"
- "Which open incidents or changes touch a service a customer contract depends on?"

A question is **answered** when one query over the ontology returns a correct, provenance-carrying answer from real connected data. When a source the ontology names is not connected, the answer is a typed abstention that names the source and the concept, never a silent gap or a fallback.

## Requirements and acceptance

- FR-001: The ontology declares, per concept, the authoritative connected source (`EG-CQ-XSRC-R001`), with SHACL.
- FR-002: Supply-chain and service concepts exist and are bound to sources (`R002`, `R003`).
- FR-003: A relation links a supply-chain entity to a service, and records in different sources resolve to one node only through an approved link (`R004`).
- FR-004: The query names classes only; the planner derives the sources from the authority declarations (`R005`).
- FR-005: The report gives impact per service, the items behind it, sources used with the reason, freshness, and typed abstention (`R006`).
- FR-006: The question is answered end to end, and changing only an authority declaration changes the sources used (`R007`).
- FR-007: Three further questions reuse the same mechanism (`R008`).
- SC-001: The test of `R007.4` shows byte-identical query text and different sources after an ontology-only change.

## Scope and interfaces

Owned here: the authority vocabulary, the missing supply-chain and commercial terms, the linking relations, the report model and the acceptance tests. Not owned here: the federated planner, pushdown, virtual-graph execution, entity-resolution scoring and decision-engine routing. These are cited as the mechanism: `EG-FEDERATED-QUERY-R072`, `R073`, `R046`, `R069`, `R044`; `EG-UNIFIED-DATA-PLANE-R005`, `R006`, `R030`, `R037`, `R038`; `EG-DECISION-ENGINE-R126` for capability coverage. The served tool belongs to graph-os; connector operation contracts belong to agent-packages.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md, Authority declaration | T-001 | PENDING |
| FR-002 | plan.md, Concepts and sources | T-002 | PENDING |
| FR-003 | plan.md, Linking | T-003 | PENDING |
| FR-004 | plan.md, Federated query | T-004 | PENDING |
| FR-005 | plan.md, Report | T-005 | PENDING |
| FR-006 | plan.md, Acceptance | T-006 | PENDING |
| FR-007 | plan.md, Generalization | T-007 | PENDING |

## Open questions

- Whether the authority is per class or per class and property facet (`R003.1` assumes facets; to be decided at `R001.1`).
- Which relation carries service to business capability: `:supports` (`core-foundation-v1.ttl`) or an ArchiMate relationship (`R008.2.3`).
- Which connected sources exist on the day of the live run is unknown from this repository.

## Virtual-first: the engine reaches source data, it does not copy it

What is ingested from a connected source is its **ontology and access contract**. The data stays in the source. The engine fetches it at query time through an ontology-driven virtual graph, selected by the ontology's source declarations. The contract kinds are A2A, MCP, API (REST), GraphQL and virtual-graph contracts over relational or document stores. A question is answered by federating over those contracts. Copying a source into the engine is an optimisation that needs its own stated reason (latency, availability, history). It is never the default and never a precondition for answering.
