# [ID] — Requirements quality checklist

Fill this before landing. Check an item only when the spec text satisfies it; an unchecked item blocks landing. Copy the file to `specs/<family>/checklists/requirements.md`.

## Content quality

- [ ] No implementation detail leaks into the user stories
- [ ] Every story states a user, an outcome and a reason
- [ ] Every mandatory section is complete

## Requirement completeness

- [ ] No `[NEEDS CLARIFICATION: ...]` marker remains
- [ ] Each row is testable and unambiguous
- [ ] Each `SC-` criterion is measurable and technology-agnostic
- [ ] Acceptance scenarios cover each story
- [ ] Edge cases are listed
- [ ] Scope is bounded and dependencies are named by row ID
- [ ] Assumptions are listed

## Readiness

- [ ] Every row has a task line and a bound test or a test-spec entry
- [ ] Each story can be delivered and proven on its own
