# [ID] — [Feature name]

Owner: [repository name]. Cross-repo IDs: [IDs or none].
Public provenance: [GitHub issue, PR, or decision URL if available. Keep all required detail below].

## Delivery mapping

Component: [owned component]

[Declare Depends-on, Extends, References, and Code lines using the shared spec standard.
Leave missing mappings unknown. Use public source links for external prerequisites.]

## User Scenarios & Testing

### User Story 1 - [Title] (Priority: P1)

[Describe the user, the outcome, and the reason in plain language.]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [observable outcome].

### User Story 2 - [Title] (Priority: P2)

[Describe the next story. Each story must work and be testable on its own.]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [observable outcome].

### Edge Cases

- [Boundary condition, error path, or denial, and the behavior it must show.]

## Requirements

- **FR-001**: [Observable behavior, including errors, authorization, and boundaries.] Rows: `[ID]-R001`.

## Success Criteria

- **SC-001**: [Measurable, independently checkable success condition.] Rows: `[ID]-R001.1`.

## Assumptions

- [An assumption that bounds the scope. Delete it when a row replaces it.]

## Why a new spec

[Record the search of existing specs and its result. Name the specs that were read. State why none of them owns this capability.]

## Scope and interfaces

[Name owned behavior, public contracts, dependencies, and explicit non-goals.]

## Traceability

| Label | Rows | Design section | Test ID | Evidence |
|---|---|---|---|---|
| FR-001 | `[ID]-R001` | [plan.md anchor] | T-001 |  |

A question that blocks a row carries the marker `[NEEDS CLARIFICATION: <question>]` in `spec.md`.
The marker must be resolved and deleted before the row lands. Do not write a status line: the generator computes the state.
