# EG-CQ-ORCH-001 — Orchestration competency questions

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: graph-os (intent-surface probes, rollup R009), agent-utilities (none required).
Public provenance: none; spec-only change.

## Purpose and user stories

Exit gate: "How can I execute task XYZ?" is answered when it decomposes into seven separately answerable questions, each answered by one query over the ontology from real connected data, and a composed answer carries all seven parts with provenance, served through the engine's assembly (`graph.assemble()` / AgentAssemble, `EG-DECISION-ENGINE-R036`). It is not a second planner.

Questions (verbatim):

1. "What steps do I need to follow to complete task XYZ?"
2. "How many agents and what type of agents are needed to complete the task?"
3. "Can we use an agent template we already have, or an external A2A agent we have added to our Agent Library, based on their capabilities?"
4. "What standards, best practices, or guardrails should I follow?"
5. "What skills, tools, prompts, and LLM models does each agent need to complete each subtask?"
6. "What workflow do the agents need to follow?"
7. "What does the agent graph shape (DAG) look like?"

Composed: "How can I execute task XYZ?" as one response carrying all seven parts with provenance (rollup R008).
Probes through the intent surface: rollup R009, owned by graph-os.

A question is **answered** when one query over the ontology returns a correct, provenance-carrying answer from real connected data. When the source is not connected, the answer is a typed abstention naming the missing source or capability, never an empty success.

## Requirements and acceptance

- FR-001: Each of questions 1-7 has one rollup row and an end-to-end acceptance test asserting a non-empty, provenance-carrying answer or the typed abstention.
- FR-002: The composed answer is produced by the assemble handler; no parallel planner, router, guardrail engine or topology chooser is added.
- FR-003: Each probe in R009 is an acceptance test through the graph-os intent tools, recorded with its previously observed result.
- SC-001: All 8 plus 1 rollups have passing acceptance children on a fixture graph.

## Scope and interfaces

Owned: ontology gaps, SHACL shapes, source-authority mappings, fixtures and acceptance tests listed in `requirements.md`. Mechanisms are cited, not re-specified: `specs/decision-engine`, `specs/federated-query-and-reasoning`, `specs/unified-data-plane`, `specs/typed-packs-and-catalog`. Non-goals: a new planner; changes to the solver; graph-os tool implementation (owned by graph-os, rows marked cross-repo).

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| EG-CQ-ORCH-R001..R009 | design.md Q1..Q9 | each rollup's acceptance child | PENDING |

## Open questions

- Whether the AgentTemplate class belongs in `catalog-v1.ttl` (currently term-free) or `orchestration-v1.ttl`.
- Whether step order reuses `swarm:` edges or adds a neutral `eg:precedes`.
