# MEDULLA-SDLC — Graph-native development lifecycle

Owner: Medulla (`EG`). Sole owner of the lifecycle records and their derivation rules.
Component: Development lifecycle
Depends-on: MEDULLA-SCM-R002, MEDULLA-SCM-R008, MEDULLA-ORCH-VOCAB-R012
References: [MEDULLA-SCM](../source-changesets/spec.md), [MEDULLA-REPO-INGEST](../repository-index-and-ingestion/spec.md), [MEDULLA-ORCH-VOCAB](../orchestration-vocabulary/spec.md), [MEDULLA-UNIFIED-DATA-PLANE](../unified-data-plane/spec.md)

Cross-repo IDs: `ARBORA-OPS-R853` and `ARBORA-OPS-R896` serve the lifecycle and artifact operations. `PIPE-GATES-R004` and
`PIPE-CONNSPEC-R009` publish gate verdicts and read the derived status. `PIPE-RELEASE-R025` computes release plans. `MERISTEM-SCM-R002` applies
graph-native edits inside a run. Each owner files its own row.
Public provenance: operator decision on graph-driven development and release planning.

## User Scenarios & Testing

### User Story 1 - Lifecycle records and derived state (Priority: P1)

A developer agent or an operator asks which requirements a symbol implements. They also ask which deployment runs a requirement and what state a row has. The engine holds the whole software lifecycle as typed records in one graph. Named rules derive the edges from the changeset log, so nobody maintains links by hand.

**Acceptance Scenarios**:

1. **Given** a changeset with a `Spec:` trailer, **When** the agent reads the row, **Then** the engine returns its edges and state.
2. **Given** a failed gate verdict, **When** the intake runs, **Then** the engine opens one defect linked to the culprit changeset.

### User Story 2 - Spec artifacts as a queryable graph (Priority: P1)

A planner asks what to build next. The engine ingests specs, rows, plans, tasks and contracts as records. It links them to code and commits. It answers competency questions with record digests.

**Acceptance Scenarios**:

1. **Given** a repository with spec artifacts, **When** one artifact file changes, **Then** only that file re-ingests within 1 s.
2. **Given** an ingested repo, **When** the planner asks for a phase task, **Then** the answer names the task. It lists row, plan, contracts and code.

### User Story 3 - Automated implementation and research (Priority: P2)

An operator starts an orchestrated implementation run. The engine picks unblocked tasks, assembles each context and hands each task to an agent run. Plan unknowns trigger cited research.

**Acceptance Scenarios**:

1. **Given** a plan with dependent tasks, **When** the engine dispatches, **Then** tasks run in dependency layers. Parallel tasks run together.
2. **Given** a run result, **When** the change fails a gate, **Then** the task stays open. The failure feeds the evolution loop.

### User Story 4 - Goals, modes and reviewed proposals (Priority: P2)

An owner records a goal and a project mode. The evolution loop proposes typed changes toward the goal. Proposals land only as pull requests through the gates.

**Acceptance Scenarios**:

1. **Given** a codebase in mode `reference`, **When** the loop proposes a change, **Then** the engine refuses it.
2. **Given** an approved proposal, **When** it lands, **Then** it lands as a pull request through the gates. It never lands as a direct write.

### User Story 5 - Landing gates and scenarios (Priority: P3)

A maintainer lands work only when the full gates pass and the phase scenario runs.

**Acceptance Scenarios**:

1. **Given** a discovered test without a verdict, **When** the landing gate runs, **Then** it fails with `SUITE_INCOMPLETE`.

### Edge Cases

- A direct write of a derived edge refuses, and the graph stays unchanged.
- A task line that matches no known form refuses and names the file and line.
- A promotion to `evolving` without operator approval refuses.
- A proposal with no evidence refuses.
- A dependency cycle among tasks refuses and dispatches nothing.
- A status read at an unknown revision refuses.

## Requirements

- **FR-001**: The engine stores the nine lifecycle record kinds and the closed edge set. Rows: `MEDULLA-SDLC-R001`.
- **FR-002**: The engine stores releases and deployments and links them to changesets. Rows: `MEDULLA-SDLC-R002`.
- **FR-003**: The engine accepts defects from five intake paths and removes duplicates by signature. Rows: `MEDULLA-SDLC-R003`.
- **FR-004**: The engine decides defect triage as a replayable decision record. Rows: `MEDULLA-SDLC-R004`.
- **FR-005**: The engine derives each requirement and task state from the graph. Rows: `MEDULLA-SDLC-R005`, `MEDULLA-SDLC-R005.7`.
- **FR-006**: UQL answers the lifecycle competency questions. Rows: `MEDULLA-SDLC-R006`.
- **FR-007**: The engine ingests spec artifacts deterministically, incrementally and as first-class records, including plans and contracts. Rows: `MEDULLA-SDLC-R014`, `MEDULLA-SDLC-R012.2`.
- **FR-008**: The engine answers the six artifact competency questions. Rows: `MEDULLA-SDLC-R014.6`.
- **FR-009**: The engine dispatches unblocked tasks to agent runs and accepts results only through gates. Rows: `MEDULLA-SDLC-R015`.
- **FR-010**: Plan unknowns trigger cited research runs. Rows: `MEDULLA-SDLC-R016`.
- **FR-011**: Goals and typed proposals drive evolution and land only as pull requests. Rows: `MEDULLA-SDLC-R017`.
- **FR-012**: Every codebase has a project mode, and promotion to `evolving` needs operator approval. Rows: `MEDULLA-SDLC-R012.8`.
- **FR-013**: Release bundle plans ingest as records. Rows: `MEDULLA-SDLC-R018`.
- **SC-001**: A rebuild of every derived edge from the changeset log reproduces the edge set byte for byte. Rows: `MEDULLA-SDLC-R001.5`.
- **SC-002**: The derived requirement states equal the output of the shared status generator on the same revision. Rows: `MEDULLA-SDLC-R005.5`.
- **SC-003**: A one-file spec change re-ingests in under 1 s, and a full spec set in under 30 s. Rows: `MEDULLA-SDLC-R014.2`.
- **SC-004**: No proposal reaches a spec file except through a merged pull request. Rows: `MEDULLA-SDLC-R017.4`.

## Assumptions

- The first repository to read its state from the engine is an operator choice. A small connector repository is the default.
- The `Depends on:` convention and the task-line form of the spec standard stay stable.
- Research connectors for web, scholarly and archived web search stay reachable through arbora.

## Why a new spec

The search covered `git grep -n -iE "defect|triage|release record|deployment record|lifecycle|spec status" -- specs`.
It read MEDULLA-SCM, MEDULLA-REPO-INGEST, MEDULLA-ORCH-VOCAB, MEDULLA-UNIFIED-DATA-PLANE, MEDULLA-DURABLE-KERNEL and MEDULLA-DECISION-ENGINE.

- MEDULLA-SCM owns changesets, trees, gate verdicts and bisect. It does not own defects, releases or deployments.
- MEDULLA-REPO-INGEST-R009 links hydrated entities of an ingested repository. It holds no derivation rules.
- MEDULLA-ORCH-VOCAB-R012 maps spec files to typed nodes. It does not derive requirement state.
- MEDULLA-UNIFIED-DATA-PLANE-R760 owns operational incidents. An incident is not a code defect.

A later search read MEDULLA-DECISION-ENGINE-R134.111, R705, R654, R811, R817, R805 and R179. They own the decisions. This spec owns the records they read and the task-dispatch inputs.

No spec owns the lifecycle chain as one record set with derivation rules. This spec owns that
chain. Each linked record stays with its owner.

## Scope and interfaces

In scope: spec artifact records, task dispatch and research. Also in scope: evolution proposals, project mode and release graph records. The lifecycle vocabulary, the derivation rules, and release and deployment records stay in scope.
Also in scope: defect records and triage, derived requirement state and the lifecycle queries.

Non-goals: changeset storage (MEDULLA-SCM), served operations (arbora), gate execution (pipelines),
views (the UIs) and incident correlation (MEDULLA-UNIFIED-DATA-PLANE-R760).

## Traceability

| Label | Rows | Design section | Test ID | Evidence |
|---|---|---|---|---|
| FR-001 | `MEDULLA-SDLC-R001` | plan.md "Record kinds and edges" | T-001 |  |
| FR-002 | `MEDULLA-SDLC-R002` | plan.md "Releases and deployments" | T-002 |  |
| FR-003 | `MEDULLA-SDLC-R003` | plan.md "Defect intake" | T-003 |  |
| FR-004 | `MEDULLA-SDLC-R004` | plan.md "Defect triage" | T-004 |  |
| FR-005 | `MEDULLA-SDLC-R005` | plan.md "Derived requirement state" | T-005 |  |
| FR-006 | `MEDULLA-SDLC-R006` | plan.md "Lifecycle queries" | T-006 |  |
| FR-007 | `MEDULLA-SDLC-R014` | plan.md "Spec artifact graph" | T-007 |  |
| FR-008 | `MEDULLA-SDLC-R014.6` | plan.md "Spec artifact graph" | T-008 |  |
| FR-009 | `MEDULLA-SDLC-R015` | plan.md "Task dispatch, research and evolution" | T-009 |  |
| FR-010 | `MEDULLA-SDLC-R016` | plan.md "Task dispatch, research and evolution" | T-010 |  |
| FR-011 | `MEDULLA-SDLC-R017` | plan.md "Task dispatch, research and evolution" | T-011 |  |
| FR-012 | `MEDULLA-SDLC-R012.8` | plan.md "Project mode" | T-012 |  |
| FR-013 | `MEDULLA-SDLC-R018` | plan.md "Spec artifact graph" | T-013 |  |

A question that blocks a row carries the marker `[NEEDS CLARIFICATION: <question>]` in `spec.md`. No marker is open.

