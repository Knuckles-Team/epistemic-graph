# epistemic-graph specifications

These specs follow the ecosystem [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md): one owning spec per capability, extend an existing spec before creating one, audits and reviews land in the owning spec, one ID and title form, one file set. The `spec-standard` gate enforces it.

This tracked `specs/` directory is the public build contract for **EG** owned work. Every
spec must contain the behavior, architecture, interfaces, tests, quality gates, and acceptance
criteria needed to implement it from this repository. External program notes may inform a draft,
but no private document or local workspace path is required to build or verify a public spec.
Delivery status is recorded here with exact merged revision and test evidence.

## Structure

Create `specs/<stable-id>/` with `spec.md` (user outcome, requirements, acceptance), `plan.md`
(architecture, reuse, interfaces, live wiring, decisions), `test-spec.md` (positive, negative,
integration, quality and release proof), `tasks.md` (ordered implementation and verification),
`requirements.md` (the definition of every requirement ID the spec owns), plus `status.json`
(machine-readable delivery, acceptance, and public receipts, including one `requirements` array
entry per requirement ID with its own `delivery_state` and evidence).
Start from [`_template/`](_template/). Keep status and evidence explicit; a planned or tested item
is not a landed item. Put durable evidence links in the spec directory, never local scratch output.
This follows GitHub Spec Kit's specify/plan/tasks flow with an explicit test contract. The tracked [constitution](../.specify/memory/constitution.md) records this repository's governing principles.

Each spec must name its stable ID and sole owner. Cross-repository work links the other owners'
public specs by stable ID and GitHub URL; each repository documents the complete contracts and
acceptance it owns. Designs must inventory existing code and reuse the legal
owner and live wiring before proposing new components. Include CCCC, jscpd, dupehound, KISS,
language-native and repo release gates where applicable, with exact pass evidence and a
no-duplicate-authority check.

## States

States are `SPECIFIED`, `LANDED`, and `VERIFIED`. The generator sets them. See the [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md#states).

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
and the [graph-os-development](https://github.com/Knuckles-Team/graph-os/blob/main/graph_os/skills/graph-os-development/SKILL.md)
bootstrap skill, together with the [SDD full lifecycle](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development-workflows/sdd-full-lifecycle) workflow.

## Local specifications

- [`EG-BRANCH-MERGE`](branch-merge/spec.md) — agent branching, three-way merge, and epistemic conflict resolution.
- [`EG-UNIFIED-DATA-PLANE`](unified-data-plane/spec.md) — EG attached-source, dialect and app-admission owner contract
- [`EG-DECISION-ENGINE`](decision-engine/spec.md) — governed decision and calibration engine contract.
- [`EG-DURABLE-KERNEL`](durable-graph-kernel/spec.md) — durable graph state and recovery contract.
- [`EG-IDENTITY`](engine-identity-and-scope/spec.md) — issuer, identity, scope, and audit contract.
- [`EG-CQ-XSRC`](cross-source-competency/spec.md) — cross-source competency: supply chain to services, sources chosen by the ontology
- [`EG-FEDERATED-QUERY`](federated-query-and-reasoning/spec.md) — federated query and reasoning contract.
- [`EG-FINANCE-PRIMITIVES`](finance-primitives/spec.md) — native financial data and calculation primitives.
- [`EG-CQ-CONN`](connector-competency/spec.md) — per-connector competency questions.
- [`EG-CQ-ORCH`](orchestration-competency/spec.md) — ontology-driven orchestration competency questions.
- [`EG-INGEST-WORKER`](ingest-worker/spec.md) — durable ingest queue policy, lanes, and metrics.
- [`EG-CONTRACT`](public-engine-contract-and-release/spec.md) — public engine contract and release proof.
- [`EG-REPO-INGEST`](repository-index-and-ingestion/spec.md) — repository indexing and governed ingestion.
- [`EG-TYPED-PACKS`](typed-packs-and-catalog/spec.md) — typed packs, catalog, and admission contract.
- [`EG-SRC-AUTH`](source-authority/spec.md) — source authority: which connected source answers a concept
- [`EG-DATAPLAT-VOCAB`](data-platform-vocabulary/spec.md) — data-platform store vocabulary (sql, documentdb, objectstore, vector, kafka, openbao, clarity, data-science)
- [`EG-ORCH-VOCAB`](orchestration-vocabulary/spec.md) — orchestration vocabulary, typed agent roles and slot-kind constraints
- [`EG-PLACEMENT`](placement-and-worker-policy/spec.md) — placement decisions, engine-owned worker policy and association-mining bounds
- [`EG-MEDIA-VOCAB`](media-and-personal-vocabulary/spec.md) — media and personal-content vocabulary and per-connector virtual-graph mappings.
- [`EG-SUPPLY-VOCAB`](supply-chain-vocabulary/spec.md) — supply-chain, commercial and host-limit vocabulary for the cross-source competency
