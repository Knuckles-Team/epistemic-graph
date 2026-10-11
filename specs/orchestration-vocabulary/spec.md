# EG-ORCH-VOCAB-001 — Orchestration vocabulary

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: none required (consumer is graph-os intent tools via `EG-CQ-ORCH-R009`).
Public provenance: closes the MISSING-term lists in `specs/orchestration-competency` (`EG-CQ-ORCH-001`); spec-only change.

## Purpose and user stories

The competency spec found that seven orchestration questions cannot be asked of the graph because the ontology lacks the terms. This spec designs the complete vocabulary, says which existing mechanism writes each term and how `graph.assemble()` (`EG-DECISION-ENGINE-R036`) reads it. It adds no planner, router, guardrail engine or topology chooser, and no second catalog: packs and the Agent Library stay the content authority (`EG-TYPED-PACKS-R072`).

## Requirements and acceptance

- FR-001: Every missing term listed in `EG-CQ-ORCH-001` exists in the ontology file named in `design.md` (Vocabulary), with domain, range and SHACL constraints stated there.
- FR-002: Each term has exactly one writer among existing mechanisms (typed packs projection, the Agent Library outbox, the template/topology projection, or the assemble commit) and at least one reader named in `design.md` (Wiring).
- FR-003: The worked example (deploy a container) answers the seven part-queries from one fixture and one committed assembly.
- FR-004: Every caveat from the gap brief is recorded as CONFIRMED, DECIDED or BLOCKED in `design.md` (Decisions), with a Corrections list for `EG-CQ-ORCH-001`.
- SC-001: `requirements.md` rollups R001..R008 each close when their children pass their named tests.

## Scope and interfaces

Owned: the ontology terms, shapes, projection call sites and query tests in `requirements.md`. Cited, not re-specified: `specs/decision-engine` (R036, R044, R046, R106, R111, R126, R127), `specs/typed-packs-and-catalog` (R006, R013, R072), `specs/federated-query-and-reasoning`. Non-goals: solver changes, new wire methods, graph-os tool code.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| EG-ORCH-VOCAB-R001..R008 | design.md Vocabulary, Wiring, Worked example | `crates/eg-core/tests/orchestration_vocabulary.rs` | PENDING |

## Open questions

- BLOCKED (owner: epistemic-graph, after `EG-DECISION-ENGINE-R106` lands the template projection): whether `AgentGraphEdge` can mark a loop-back edge; today it carries only `from`, `to`, `condition`. R006.5 adds the marker as a projection rule if absent.
