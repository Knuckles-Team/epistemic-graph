# EG-CQ-CONN-001 — Design and implementation plan

Status: PROPOSED. Owner: epistemic-graph. The governing behavior and requirement
IDs are in [spec.md](spec.md) and [requirements.md](requirements.md). The complete
question and vocabulary inventory remains in [design.md](design.md).

## Architecture and reuse

The engine answers connector competency questions through its existing
[federated query contract](../federated-query-and-reasoning/spec.md) and
[unified data plane](../unified-data-plane/spec.md). Reuse their ontology-driven
source selection, approved virtual mappings, query planning, and provenance
interfaces; do not introduce a separate connector-specific query planner.

The ontology describes source access contracts. Records remain at their source
and are fetched at query time through MCP, A2A, API, GraphQL, or virtual-graph
contracts. Materialisation is an explicit optimisation with a stated reason,
never a prerequisite for the competency questions. The R067 children define
acceptance for each contract kind and declared materialisation.

## Ownership and interfaces

EG owns the ontology vocabulary, query behavior, provenance, typed abstentions,
and engine-side verification. Connector transports and mappings belong to the
[connector SDK and package owners](https://github.com/Knuckles-Team/agent-connector-sdk/tree/main/specs).
The [graph-os serving owner](https://github.com/Knuckles-Team/graph-os/tree/main/specs)
provides the integrated query surface. Cross-repository prerequisites remain
explicit dependencies, not engine implementations or evidence of delivery.

Each connector rollup has a single-source question, a cross-source question,
and an acceptance test. Fixtures preserve the connector's declared response
shape and stable record identities. Results carry source, record ID, and as-of
metadata; unavailable required sources produce typed abstentions naming the
source, rather than a successful empty answer. Cross-source joins must use the
existing approved mapping and identity-resolution contracts.

## Implementation sequence and proof

1. Inspect the proposed vocabulary assignment in [design.md](design.md) against
   the actual ontology and connector contracts. Resolve every MISSING entry
   before implementing its question; filename-based assignments are proposals.
2. Add or extend vocabulary and source mappings using the existing legal owner.
   Preserve all 66 connector rows and their stable requirement IDs.
3. Implement recorded, disposable fixtures for each question without live
   credentials or a network dependency in the PR gate.
4. Execute both question paths and the negative unavailable-source path through
   the existing engine interfaces, following [test-spec.md](test-spec.md).
5. Verify R067 contract-kind coverage and the served integration surface.
   Update [tasks.md](tasks.md) and generated status only with exact-revision
   evidence; no proposal or cross-repository dependency counts as landed.

## Quality gates and risks

Run the public-spec check and generated spec-status check for documentation.
For implementation, run focused owner tests, language-native checks, and the
repository's applicable architecture, duplication, and release gates. Missing
connector operations, incomplete vocabulary, and unapproved cross-source
identity links remain explicit acceptance blockers. Do not replace those gaps
with copied source data or an independently implemented planner.
