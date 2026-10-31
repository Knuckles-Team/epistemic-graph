# Medulla specifications

These specs follow the ecosystem [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md): one owning spec per capability, extend an existing spec before creating one, audits and reviews land in the owning spec, one ID and title form, one file set. The `spec-standard` gate enforces it.

This tracked `specs/` directory is the public build contract for **Medulla** owned work. Every
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
Start from the templates in `.specify/templates/`. Keep status and evidence explicit; a planned or tested item
is not a landed item. Put durable evidence links in the spec directory, never local scratch output.
This follows a specify, plan and tasks flow with an explicit test contract. The tracked [constitution](../.specify/memory/constitution.md) records this repository's governing principles.

Each spec must name its stable ID and sole owner. Cross-repository work links the other owners'
public specs by stable ID and GitHub URL. Each repository documents the complete contracts and
acceptance it owns. Designs must inventory existing code and reuse the legal
owner and live wiring before proposing new components. Include CCCC, jscpd, dupehound, KISS,
language-native and repo release gates where applicable, with exact pass evidence and a
no-duplicate-authority check.

## States

States are `SPECIFIED`, `LANDED`, and `VERIFIED`. The generator sets them. See the [spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md#states).

## Ecosystem owner map

The [ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md) standard routes every feature. A part that medulla does not own goes to its owner's specs as a row. The medulla row links that row by ID.

| Prefix | Repository specs | Owns |
|---|---|---|
| `EG` | [Medulla](https://github.com/Knuckles-Team/medulla/tree/main/specs) | Facts, records, vocabulary, storage, query, retrieval, ingest, reasoning, decisions and choices, including the server executable |
| `SCION` | [Scion SDK](https://github.com/Knuckles-Team/scion-sdk/tree/main/specs) | Connector contracts, transports and the medulla client adapter |
| `MERISTEM` | [Meristem](https://github.com/Knuckles-Team/meristem/tree/main/specs) | The in-run harness; it applies a decision it receives |
| `ARBORA` | [arbora](https://github.com/Knuckles-Team/arbora/tree/main/specs) | Served operations, admission, auth, approvals and the operation registry |
| `OVERSTORY`, `TUIG`, `AXIL` | Overstory, Tuig and Axil Desk | What a person sees |
| `CONN-<REPO>` | Each connector repository | One external system's calls and mappings |
| `PIPE` | [pipelines](https://github.com/Knuckles-Team/pipelines/tree/main/specs) | Gates, pages, release rules and the spec standard |

## Medulla subsystem map

Medulla is one engine with one server executable. Every spec belongs to one subsystem. Every requirement plugs into a production entry point of its subsystem.

| Subsystem | Specs | Owns | Production entry points |
|---|---|---|---|
| Durable kernel | `MEDULLA-DURABLE-KERNEL`, `MEDULLA-OBJECT-STORAGE-BACKEND`, `MEDULLA-ENGINE-PARITY` | Storage, transactions, group commit, shards, outbox and change capture, leases, scheduler config, wire listeners | Mutation gateway, shard writer, dispatch, wire listeners |
| Identity and placement | `MEDULLA-IDENTITY`, `MEDULLA-PLACEMENT` | Issuers, scopes, visibility and sharing, worker admission, placement | Request boundary, worker admission |
| Ingest | `MEDULLA-INGEST-WORKER`, `MEDULLA-REPO-INGEST`, `MEDULLA-SCM`, `MEDULLA-BRANCH-MERGE` | Ingest lanes and queues, repository indexing, changesets, merges | Ingest worker loop, `SourceIngest`, repository index |
| Sources | `MEDULLA-UNIFIED-DATA-PLANE`, `MEDULLA-SRC-AUTH` | Attached sources read as virtual graphs, source authority and abstention | Virtual SPARQL, `UnifiedQuery` |
| Query and reasoning | `MEDULLA-FEDERATED-QUERY` | UQL, SPARQL, natural language, retrieval, expansion, reasoning | `UnifiedQuery`, `Discover`, SPARQL handlers |
| Ontology and packs | `MEDULLA-TYPED-PACKS`, `MEDULLA-DATAPLAT-VOCAB`, `MEDULLA-MEDIA-VOCAB`, `MEDULLA-SUPPLY-VOCAB`, `MEDULLA-ORCH-VOCAB` | Typed packs, the catalog, ontology modules and shapes | `ConnectorPack.Import`, schema sources |
| Decisions and self-evolution | `MEDULLA-DECISION-ENGINE`, `MEDULLA-SDLC` | The decision pipeline, fitted heads, knobs, the revision lifecycle, lifecycle records | `Decide`, `AgentAssemble`, `DecisionCommit`, `DecisionLog`, decision jobs |
| Finance | `MEDULLA-FINANCE-PRIMITIVES` | Market data primitives, kernels, backtests, risk and signals | Finance numeric kernels, series operators in UQL |
| Graph-native development | `MEDULLA-SDLC`, `MEDULLA-SCM`, `MEDULLA-REPO-INGEST`, `MEDULLA-BRANCH-MERGE` | The ecosystem's code, specs, changes, gates and releases as graph records | `SubmitChangeset`, `ReadTree`, `SourceIngest`, `UnifiedQuery`, served through arbora MCP |
| Competency | `MEDULLA-CQ-CONN`, `MEDULLA-CQ-XSRC`, `MEDULLA-CQ-ORCH` | The questions the ontology must answer | The served query path only |
| Contract | `MEDULLA-CONTRACT` | The public engine contract, gates and release proof | The server executable and client |

## Cross-spec rules

1. **Integration-first proof.** The decision-engine proof standard applies to every spec (`decision-engine/test-spec.md`, "Integration proof standard"). A behaviour row is VERIFIED only by a test through a production entry point. A competency row is VERIFIED only when the served query path answers the question from engine-produced facts. A hand-written fixture graph does not prove it.
2. **Reachability.** The reachability gate (MEDULLA-CONTRACT-R055) covers every crate the server depends on. A crate that the server does not depend on must be wired in or deleted.
3. **Both sides declared.** A cross-spec contract has a provider row and a consumer row. Each names the other by ID. One spec never edits another spec's rows; it adds a consumer row in its own spec.
4. **One owner per concept.** A type, store, method or rule has one owning row. Other specs reference that row.
5. **One engine.** Medulla keeps no synced second database. An external store is a connector source read as a virtual graph. A copy is allowed only as a watermark-bound cache.
6. **Packs roll up to the core.** Every pack class extends a core class through `rdfs:subClassOf`. A pack never imports the core ontology.
7. **Knobs.** Every tunable value is a named knob in the knob registry (MEDULLA-DECISION-ENGINE-R179), with a default, bounds and a unit. Any subsystem may declare knobs. Safety and gate knobs are `Fixed`.
8. **One revision lifecycle.** Fitted heads, programs, prompts, skills, harness components, adoptions and knob sets share the MEDULLA-DECISION-ENGINE-R805 lifecycle.
9. **Graph-native development.** Every repository, spec row, change and gate verdict is a graph record (MEDULLA-SDLC-R012). A row is implemented, proved and landed through served operations, and git is an export mirror.

## Cross-spec decisions (2026-10-30)

The 2026-10-30 review graded every open row in every spec. Each spec's `evidence/cross-spec-review-20261030/` holds its register. Each owner below wins; the other rows become references or retire.

| Concept | Owner | Other rows |
|---|---|---|
| Scheduler config, worker registry, leases | `MEDULLA-DURABLE-KERNEL-R074`, `R076`, `R075` | `MEDULLA-INGEST-WORKER-R604.1`, `R604.2`, `R001.10` reference them |
| Worker admission and grants | `MEDULLA-PLACEMENT-R002`, served through `MEDULLA-DURABLE-KERNEL-R075` | `MEDULLA-INGEST-WORKER` consumes |
| Group commit, partitions, change capture, SQL compatibility matrix | `MEDULLA-DURABLE-KERNEL` (`R040`, `R099`, `R002`, `R035.1`) | `MEDULLA-ENGINE-PARITY-R001`, `R003`, `R009`, `R015` extend them |
| Bitemporal stamps | `MEDULLA-DURABLE-KERNEL-R078`–`R081` | `R609` keeps the types only |
| Governed edit ledger | `MEDULLA-DURABLE-KERNEL-R027.1` | `MEDULLA-TYPED-PACKS-R651` references it |
| Visibility, sharing and markings | `MEDULLA-IDENTITY-R610` (one sharing enum) | `MEDULLA-PLACEMENT-R023`, `MEDULLA-TYPED-PACKS-R652` reference it |
| Re-embedding | One executor, `semantic_index/worker.rs` (`MEDULLA-FEDERATED-QUERY-R039.2`) | `MEDULLA-REPO-INGEST-R006.2`, `MEDULLA-FEDERATED-QUERY-R040.5`, `R077.3` reference it |
| Query expansion | `MEDULLA-FEDERATED-QUERY-R651` | `R090`, `R058.4` consume it |
| Retrieval failure verdict | `MEDULLA-FEDERATED-QUERY-R089` | `R655.1`–`R655.3` retire |
| Dead letter | `MEDULLA-REPO-INGEST-R002.650` | ingest rows reference it |
| Source authority vocabulary and abstention | `MEDULLA-SRC-AUTH-R001`, `R002`, `R006` | `MEDULLA-CQ-XSRC-R001`, `R006.3` reference them |
| Workflow, agent and prompt types | `MEDULLA-ORCH-VOCAB-R800.1` | `R616.1`, `MEDULLA-CQ-ORCH-R700.1` retire |
| Revision subjects (harness components, adoptions, teams) | `MEDULLA-DECISION-ENGINE-R805` | `MEDULLA-ORCH-VOCAB-R801`, `R802`, `R003.8` become subject kinds |
| Order revision chain | `MEDULLA-FINANCE-PRIMITIVES-R015.1` | `R751` retires |
| Finance kernels | `medulla-numeric`, with the finance aliases deleted in the same release | `R750` retires |
| Misfiled durable-kernel families | Finance rows (`R057.1`, `R092`, `R093`) move to finance. Reasoners (`R097`, `R098`) move to query. Changeset review (`R087`) moves to `MEDULLA-SCM`. Corrections and step verdicts (`R088`, `R089`) move to decisions. Schema import (`R090`) moves to packs. The policy source (`R091`) moves to `MEDULLA-SRC-AUTH`. The inbox (`R095`) moves to arbora. The CI gate (`R011`) moves to pipelines. | Each moves with its tests |
| Second databases | Retire the mirror sinks (`MEDULLA-UNIFIED-DATA-PLANE-R002.3`, `MEDULLA-DURABLE-KERNEL-R024` mirror driver) and the companion extension (`R025`). The shared relational stack (`R023`) moves to the service stacks. WAL tailing (`R015`) becomes a connector change source. Row copy (`R038`) stays a watermark-bound cache. | `MEDULLA-ENGINE-PARITY-R018` columnar copy retires |
| Pack imports | `MEDULLA-TYPED-PACKS-R105` refuses a core import; a codemod rewrites the 40 modules that import the core | `R105.1`, `R105.2` drop the import edge |
| Competency acceptance | One table-driven served test per competency spec | The per-connector `.3` acceptance rows collapse into it |
| Unwired crates | `medulla-durable` and `medulla-enrich` join the server dependency graph | Their specs bind through served paths |

## Contributions

Read this repository's `AGENTS.md` and any contribution guide. Propose `spec.md` first, resolve
architecture and test details in `plan.md` and `test-spec.md`, then implement `tasks.md` in a
dedicated branch or worktree. Link the PR to spec IDs and update evidence and status only when the
corresponding gates actually pass. Use the [universal-skills speckit-specify](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/skills/speckit-specify),
[speckit-analyze](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/skills/speckit-analyze),
and [speckit-tasks](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/skills/speckit-tasks)
and this repository's `medulla-development`
bootstrap skill, together with the [SDD full lifecycle](https://github.com/Knuckles-Team/universal-skills/tree/main/universal_skills/development-workflows/sdd-full-lifecycle) workflow.

## Local specifications

- [`MEDULLA-SCM`](source-changesets/spec.md) — source changesets as the source of truth for repository files.
- [`MEDULLA-ENGINE-PARITY`](engine-parity/spec.md) — relational engine parity inside one library: write scaling, PostgreSQL wire front end, relational features and log services.
- [`MEDULLA-SDLC`](development-lifecycle/spec.md) — graph-native development lifecycle: the ecosystem is developed inside the engine, with lifecycle records, derived edges, defects and derived requirement state.
- [`MEDULLA-BRANCH-MERGE`](branch-merge/spec.md) — agent branching, three-way merge, and epistemic conflict resolution.
- [`MEDULLA-OBJECT-STORAGE-BACKEND`](object-storage-backend/spec.md) — object-storage-native durable store and comparative cost gate.
- [`MEDULLA-UNIFIED-DATA-PLANE`](unified-data-plane/spec.md) — Medulla attached-source, dialect and app-admission owner contract
- [`MEDULLA-DECISION-ENGINE`](decision-engine/spec.md) — governed decision and calibration engine contract.
- [`MEDULLA-DURABLE-KERNEL`](durable-graph-kernel/spec.md) — durable graph state and recovery contract.
- [`MEDULLA-IDENTITY`](engine-identity-and-scope/spec.md) — issuer, identity, scope, and audit contract.
- [`MEDULLA-CQ-XSRC`](cross-source-competency/spec.md) — cross-source competency: supply chain to services, sources chosen by the ontology
- [`MEDULLA-FEDERATED-QUERY`](federated-query-and-reasoning/spec.md) — federated query and reasoning contract.
- [`MEDULLA-FINANCE-PRIMITIVES`](finance-primitives/spec.md) — native financial data and calculation primitives.
- [`MEDULLA-CQ-CONN`](connector-competency/spec.md) — per-connector competency questions.
- [`MEDULLA-CQ-ORCH`](orchestration-competency/spec.md) — ontology-driven orchestration competency questions.
- [`MEDULLA-INGEST-WORKER`](ingest-worker/spec.md) — durable ingest queue policy, lanes, and metrics.
- [`MEDULLA-CONTRACT`](public-engine-contract-and-release/spec.md) — public engine contract and release proof.
- [`MEDULLA-REPO-INGEST`](repository-index-and-ingestion/spec.md) — repository indexing and governed ingestion.
- [`MEDULLA-TYPED-PACKS`](typed-packs-and-catalog/spec.md) — typed packs, catalog, and admission contract.
- [`MEDULLA-SRC-AUTH`](source-authority/spec.md) — source authority: which connected source answers a concept
- [`MEDULLA-DATAPLAT-VOCAB`](data-platform-vocabulary/spec.md) — data-platform store vocabulary (sql, documentdb, objectstore, vector, kafka, openbao, clarity, data-science)
- [`MEDULLA-ORCH-VOCAB`](orchestration-vocabulary/spec.md) — orchestration vocabulary, typed agent roles and slot-kind constraints
- [`MEDULLA-PLACEMENT`](placement-and-worker-policy/spec.md) — placement decisions, engine-owned worker policy and association-mining bounds
- [`MEDULLA-MEDIA-VOCAB`](media-and-personal-vocabulary/spec.md) — media and personal-content vocabulary and per-connector virtual-graph mappings.
- [`MEDULLA-SUPPLY-VOCAB`](supply-chain-vocabulary/spec.md) — supply-chain, commercial and host-limit vocabulary for the cross-source competency
