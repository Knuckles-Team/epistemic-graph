# EG-CQ-CONN-001 — Connector competency questions

**Owner:** epistemic-graph (EG) · **State:** proposed

## Outcome and boundary

Exit gate: the deployed engine holds rich, related data from every live connector, tracked per connector. For each of the 66 live-deployed connectors this spec states the use cases the ontology must answer: one single-source question, one cross-source question linking at least one other live connector, and an acceptance test.

A competency question is answered when one query over the ontology returns a correct, provenance-carrying answer from real connected data, or a typed abstention naming the unconnected source. Questions, vocabulary assignment and the coverage table are in [design.md](design.md); rows are in [requirements.md](requirements.md). Cross-source composition is not new functionality: it is delivered by `specs/federated-query-and-reasoning` and `specs/unified-data-plane`.

## Requirements and acceptance

Rows `EG-CQ-CONN-R001` to `R066` are rollups, one per connector; each has children `.1` (single-source question), `.2` (cross-source question) and `.3` (acceptance test). Cross-repo prerequisites: connector SDK mappings (owning repo: the connector's package in agent-packages) and graph-os surface for federated query.

## Traceability

See [test-spec.md](test-spec.md).
