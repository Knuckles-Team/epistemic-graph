# EG-SDLC — Graph-driven development lifecycle

Owner: epistemic-graph (`EG`). Sole owner of the lifecycle records and their derivation rules.
Component: Development lifecycle
Depends-on: EG-SCM-R002, EG-SCM-R008, EG-ORCH-VOCAB-R012
References: [EG-SCM](../source-changesets/spec.md), [EG-REPO-INGEST](../repository-index-and-ingestion/spec.md), [EG-ORCH-VOCAB](../orchestration-vocabulary/spec.md), [EG-UNIFIED-DATA-PLANE](../unified-data-plane/spec.md)

Cross-repo IDs: `ARBORA-OPS-R853` serves the lifecycle operations. `PIPE-GATES-R004` and
`PIPE-CONNSPEC-R009` publish gate verdicts and read the derived status. `AU-SCM-R002` applies
graph-native edits inside a run. Each owner files its own row.

## Purpose and user stories

The engine holds the whole software lifecycle as typed records in one graph. Nine record kinds
exist. They are requirement row, code symbol, changeset, test, gate verdict, defect, run trace,
release and deployment. Typed edges link them. Named rules derive most edges from
the changeset log, so no person or agent maintains the links by hand.

- A developer agent asks which requirements a symbol implements. The engine answers from derived edges.
- An operator asks which deployment runs the code for a requirement. The engine walks the chain.
- A gate failure opens a defect. The engine links the defect to the culprit changeset and symbols.
- A spec page reads requirement state from the engine. No repository keeps a hand-run status file as truth.

## Requirements and acceptance

Every requirement ID lives in [requirements.md](requirements.md). The IDs are `EG-SDLC-R001` to
`EG-SDLC-R006`. Architecture and live wiring live in [plan.md](plan.md). The test contract lives
in [test-spec.md](test-spec.md).

- FR-001: The engine stores the nine lifecycle record kinds and the closed edge set (R001).
- FR-002: The engine stores releases and deployments and links them to changesets (R002).
- FR-003: The engine accepts defects from five intake paths and removes duplicates by signature (R003).
- FR-004: The engine decides defect triage as a replayable decision record (R004).
- FR-005: The engine derives each requirement state from the graph (R005).
- FR-006: UQL answers the lifecycle competency questions (R006).
- SC-001: A rebuild of every derived edge from the changeset log reproduces the edge set byte for byte.
- SC-002: The derived requirement states equal the output of the shared status generator on the same revision.

## Why a new spec

The search covered `git grep -n -iE "defect|triage|release record|deployment record|lifecycle|spec status" -- specs`.
It read EG-SCM, EG-REPO-INGEST, EG-ORCH-VOCAB, EG-UNIFIED-DATA-PLANE, EG-DURABLE-KERNEL and EG-DECISION-ENGINE.

- EG-SCM owns changesets, trees, gate verdicts and bisect. It does not own defects, releases or deployments.
- EG-REPO-INGEST-R009 links hydrated entities of an ingested repository. It holds no derivation rules.
- EG-ORCH-VOCAB-R012 maps spec files to typed nodes. It does not derive requirement state.
- EG-UNIFIED-DATA-PLANE-R760 owns operational incidents. An incident is not a code defect.

No spec owns the lifecycle chain as one record set with derivation rules. This spec owns that
chain. Each linked record stays with its owner.

## Scope and interfaces

In scope: the lifecycle vocabulary, the derivation rules, and release and deployment records.
Also in scope: defect records and triage, derived requirement state and the lifecycle queries.

Non-goals: changeset storage (EG-SCM), served operations (arbora), gate execution (pipelines),
views (the UIs) and incident correlation (EG-UNIFIED-DATA-PLANE-R760).

## Traceability

| Requirement | Design section | Test ID |
|---|---|---|
| FR-001 | plan.md "Record kinds and edges" | T-001 |
| FR-002 | plan.md "Releases and deployments" | T-002 |
| FR-003 | plan.md "Defect intake" | T-003 |
| FR-004 | plan.md "Defect triage" | T-004 |
| FR-005 | plan.md "Derived requirement state" | T-005 |
| FR-006 | plan.md "Lifecycle queries" | T-006 |

## Open questions

- The first repository to read its state from the engine is an operator choice. A small connector repository is the default.
