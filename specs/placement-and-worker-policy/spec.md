# EG-PLACEMENT — Placement, worker policy and analytics bound

Owner: epistemic-graph. Cross-repo IDs: GRAPHOS-DEPLOY-R017 (graph-os), EG-DECISION-ENGINE-R036, R046, R111, EG-DURABLE-KERNEL-R074 to R077, R082.3, EG-INGEST-WORKER-R001.
Public provenance: closes three engine-side design gaps so each keeps one decision authority.

## Purpose and user stories

- An operator asks "where should this run?" and gets one engine decision (target, evidence, why-not, or typed abstention) plus an assembled, certificate-backed step graph; graph-os adds no planner.
- An operator runs ingest workers that register and obey the engine; no worker-side policy can disagree with the engine's.
- A caller submits an association-mining job and gets a deterministic, documented bound instead of an unexplained refusal.

## Requirements and acceptance

- FR-001: A `Placement` Decide question over in_process/compose/helm/swarm returns a typed record or typed abstention. It never takes a lease (R001).
- FR-002: `AgentAssemble` returns an ordered deployment step graph from a published template (R001.12, R001.13).
- FR-003: Autosize, lane floors, widths, routing and queue-depth limit are served engine settings; the worker holds none (R002).
- FR-004: `MineAssociate` refuses more than 31 distinct items with its own typed refusal, and the kernel cannot overflow (R003).
- SC-001: The seven overlapping ingest-worker rows are reworded and a check finds no duplicate policy constant.

## Scope and interfaces

Owned: `QuestionKind::Placement`, placement vocabulary, `stat_placement.rs`, proposed edits to durable-kernel R074-R076 and ingest-worker rows, and the submit.rs bound. Not owned: manifests and fleet-tool components (graph-os), the worker runner (connector SDK), R111.1 (decision-engine spec).

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md A | T-001, T-002 | PENDING |
| FR-002 | plan.md A | T-003 | PENDING |
| FR-003 | plan.md B | T-004 | PENDING |
| FR-004 | plan.md C | T-005 | PENDING |

## Open questions

- BLOCKED (graph-os, typed packs): the production deployment template and fleet-tool components do not exist in any Agent Library yet (plan.md D4).

R002.2 supplies the pure R076.1 scheduler config contract. FR-003 remains pending until the served R076.2 setting and admission/queue consumers are wired and tested.

<!-- spec-definition-patch:owner-batch-20261012-placement-and-worker-policy -->
## Owner definitions — 2026-10-12

### `EG-PLACEMENT-R001.15` — EG:9

EG's proposed pure operation is `evaluate_capacity_adjustment(proposal, capacity_premise) -> CapacityAdjustmentVerdictV1`, a read-only feasibility decision over a GraphOS-generated capacity adjustment proposal. The proposal binds tenant, resource class/unit, unsigned integer current/proposed capacity and bounds, measured load/headroom, UTC millisecond window, premise/policy revisions and SHA-256 digest. Reuse R001.6's capacity premise; refuse invalid digest/measurements, stale or cross-tenant evidence, inconsistent units/window, mismatch between proposal and graph-observed current capacity/headroom, any target outside limits, or an increase larger than observed headroom using the named capacity refusal codes in requirements.md; `allow` carries no reason code and each refusal carries exactly one. GraphOS retains mode, pure AIMD, host actuation and limits; EG never mutates host capacity. Preserve `GRAPHOS-CAPACITY-R001.1`, `EG-DECISION-ENGINE-R112`, `GRAPHOS-CAPACITY-R001/R002`, `EG-PLACEMENT-R001.6`.

**Hold:** no reviewed owner requirement identifies the EG Decision method/QuestionKind, GraphOS proposal/apply/readback operations, or applied-state receipt schema. The typed proposal/verdict is a candidate interface only; this is not a closed-loop definition until those exact owner links are resolved. Source hash: `cb17ea03a7344584fed7a9f22a78c1e97c58919616433e7026481606fd0bdccb`.
<!-- /spec-definition-patch -->
