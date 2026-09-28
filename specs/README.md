# epistemic-graph specifications

This tracked `specs/` directory is the build contract for **EG** owned work. The workspace program
draft and ledger live at `plans/refactor/`; its distilled cross-repository specifications live at
`plans/specs/<stable-id>/` in the workspace. A public contribution must include the complete
repository-owned spec here so a contributor can work from this repository alone. The program ledger
remains the authority for delivery state until a verified landing is recorded.

## Structure

Create `specs/<stable-id>/` with `spec.md` (user outcome, requirements, acceptance), `plan.md`
(architecture, reuse, interfaces, live wiring, decisions), `test-spec.md` (positive, negative,
integration, quality and release proof), and `tasks.md` (ordered implementation and verification).
Start from [`_template/`](_template/). Keep status and evidence explicit; a planned or tested item
is not a landed item. Put durable evidence links in the spec directory, never local scratch output.
This follows GitHub Spec Kit's specify/plan/tasks flow with an explicit test contract. The tracked [constitution](../.specify/memory/constitution.md) records this repository's governing principles.

Each spec must name its stable ID and sole owner. Cross-repository work uses the same ID in the
central program and links the other owners' specs by ID and GitHub URL; each repository documents
only the contracts and acceptance it owns. Designs must inventory existing code and reuse the legal
owner and live wiring before proposing new components. Include CCCC, jscpd, dupehound, KISS,
language-native and repo release gates where applicable, with exact pass evidence and a
no-duplicate-authority check.

## Graph OS owner map

| Prefix | Repository specs | Responsibility |
|---|---|---|
| `EG` | [epistemic-graph](https://github.com/Knuckles-Team/epistemic-graph/tree/main/specs) | Rust database, graph compute, ontology, governed ingestion, durable records and clients |
| `SDK` | [agent-connector-sdk](https://github.com/Knuckles-Team/agent-connector-sdk/tree/main/specs) | connector control, transport, manifests, certification, governed effects |
| `AU` | [agent-utilities](https://github.com/Knuckles-Team/agent-utilities/tree/main/specs) | agent orchestration and control workflows |
| `GRAPHOS` | [graph-os](https://github.com/Knuckles-Team/graph-os/tree/main/specs) | serving composition, fleet, gateway, A2A, deployment operations |
| `WEBUI` | [agent-webui](https://github.com/Knuckles-Team/agent-webui/tree/main/specs) | browser presentation and interaction |
| `RM` | [repository-manager](https://github.com/Knuckles-Team/repository-manager/tree/main/specs) | repository discovery, worktrees, source control and execution |

## Contributions

Read this repository's `AGENTS.md` and any contribution guide. Propose `spec.md` first, resolve
architecture and test details in `plan.md` and `test-spec.md`, then implement `tasks.md` in a
dedicated branch or worktree. Link the PR to spec IDs and update evidence and status only when the
corresponding gates actually pass. Use the [universal-skills spec-generator](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/spec-generator),
[spec-verifier](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/spec-verifier),
and [task-planner](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development/task-planner)
with the [SDD full lifecycle](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development-workflows/sdd-full-lifecycle) workflow.

## Local specifications

- [`EG-T4-GREEN`](EG-T4-GREEN/spec.md) — Train 4 post-merge green repair and PR #3 evidence; IN REVIEW.
