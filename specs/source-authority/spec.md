# EG-SRC-AUTH — Source authority: the ontology says which connected source answers a concept

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: graph-os (served intent tool prints the "sources used and why" block, connected-source list), agent-packages connectors (operation contracts that a pack's mapping proposals cite). Consumers in this repository: `EG-CQ-XSRC-001` (`R001`, `R005`), `EG-CQ-ORCH-001` (`.3` children of `R001` to `R007`), `EG-CQ-CONN-001` (cross-source children).
Public provenance: gap found while designing the Stardog-style requirement "the ontology specifies which connected data sources are used to answer a question".

## Purpose and user stories

The virtual-graph vocabulary (`crates/eg-core/ontology/virtual_graph-v1.ttl`) can say a source maps a class (`vg:mapsOntologyClass`) and that an operator approved the mapping (`vg:approved`). It cannot say that a connected source is the **authority** for a concept, how it ranks against another source that maps the same concept, how fresh its answer must be, or whether it may be copied. Without that statement source selection (`EG-FEDERATED-QUERY-R072`) has nothing to choose with, and the three competency specs each carry a "source-authority mapping" child that cannot be written.

As an operator I declare, in the ontology, "for `:Incident`, ServiceNow is authoritative, rank 1; PagerDuty is a rank 2 fallback", approve it, and ask a question that names only classes. The engine picks the sources, fetches from them at query time, and prints which sources it used and why. If the authority is not connected, I get a typed abstention naming the source and the concept, not a silent gap.

## Requirements and acceptance

- FR-001: A `vg:SourceAuthority` row states source, concept (class, optionally one property facet), rank, scope, freshness expectation, contract kind and materialisation policy with reason (`R001`).
- FR-002: SHACL refuses a malformed row, an authority over an unmapped class, a contract kind that contradicts the connection, and a non-virtual policy without a reason (`R002`).
- FR-003: Only an operator-approved row has effect. A connector pack may propose, never assert (`R003`).
- FR-004: `select_sources` reads effective authority rows and names the winning source per class or facet, with alternates (`R004`).
- FR-005: Tie-breaking is a total, documented order; an unresolvable tie is a typed abstention, never an arbitrary pick (`R005`).
- FR-006: An unconnected, unapproved, stale or ambiguous authority yields a typed abstention, or an explicit declared fallback (`R006`).
- FR-007: Every answer carries "sources used and why" built from the selection premises (`R007`).
- FR-008: Rows copy data only under a declared, reasoned acceleration policy; the default is virtual (`R008`).
- SC-001: Changing only an authority row changes the sources used for an equal query text (`R009`).

## Scope and interfaces

Owned here: the authority vocabulary and shapes, the approval path for authority, the authority-aware extension of `eg-rdf::source_selection`, the abstention and audit types. Not owned: the federated planner and pushdown (`EG-FEDERATED-QUERY-R046`, `R069`, `R073`), virtual-graph execution (`EG-UNIFIED-DATA-PLANE-R005`, `R006`), acceleration (`R038`), pack import (`EG-TYPED-PACKS`), decision-engine routing (`EG-DECISION-ENGINE`). Non-goals: ingesting source data as a default; a second catalog of connectors.

Reconciliation: `EG-CQ-XSRC-R001.1` to `R001.4` and `R005.1` already plan a class-level subset of this vocabulary under the same term names. This spec adopts those names (`vg:SourceAuthority`, `vg:authoritativeSource`, `vg:authorityFor`, `vg:authorityRank`, `vg:allowFallback`) and becomes the single owner of the vocabulary. Those rows should be re-pointed to `EG-SRC-AUTH-R001`, `R002`, `R004` when this spec lands, so no term is defined twice.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md, Vocabulary | T-001 | PENDING |
| FR-002 | plan.md, Vocabulary (SHACL) | T-002 | PENDING |
| FR-003 | plan.md, Decisions D3 | T-003 | PENDING |
| FR-004 | plan.md, Wiring | T-004 | PENDING |
| FR-005 | plan.md, Decisions D4 | T-005 | PENDING |
| FR-006 | plan.md, Decisions D5 | T-006 | PENDING |
| FR-007 | plan.md, Architecture | T-007 | PENDING |
| FR-008 | plan.md, Vocabulary | T-008 | PENDING |
| SC-001 | plan.md, Worked example | T-009 | PENDING |

## Open questions

None. Every caveat is resolved in plan.md Decisions (D1 DECIDED, D2 CONFIRMED, D3 DECIDED). Dependencies on other owners are BLOCKED rows named in plan.md (`governed.source-authority` kind, `governance:approve-source-authority` scope).
