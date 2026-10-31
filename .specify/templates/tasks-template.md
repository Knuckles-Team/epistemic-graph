# [ID] — Implementation tasks

Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md). Rows: [requirements.md](requirements.md).

Each task line reads `- [ ] T001 [P] [US1] <ID>-R012.4 description`. The task number and the row ID are required.
`[P]` marks a task that may run in parallel. `[US1]` names the user story. Order tasks producer first.

- [ ] T001 [ID]-R001 Inventory the existing component, owner, caller, data path, and reuse candidates.
- [ ] T002 [US1] [ID]-R001 Implement the smallest complete vertical slice and remove superseded paths.
- [ ] T003 [P] [US1] [ID]-R001.1 Prove live wiring and negative behavior against the test spec.
- [ ] T004 [ID]-R001.1 Run focused tests and the applicable quality scanners.
