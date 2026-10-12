# EG-PLACEMENT - Design and implementation plan

Governing spec: [spec.md](spec.md). Rows: [requirements.md](requirements.md).

All three gaps keep ONE decision authority: the engine decides, callers (graph-os, the ingest worker, the submit path) ask and obey. Nothing here adds a planner outside `specs/decision-engine`.

## Problem and evidence

- A. graph-os `GRAPHOS-DEPLOY-R017` (branch `land/train-1010h`, `specs/portable-deployment/requirements.md:27`) requires "a deployment-placement question kind under `EG-DECISION-ENGINE-R046`/`R111`" and says assemble is "a refusal stub". In code: `QuestionKind` (`crates/eg-types/src/decision/statistical.rs:56-95`) has no placement or topology kind. `R111.1` (`specs/decision-engine/requirements.md:163`) says the standalone topology question is not built and its proving test name `a_standalone_topology_decide_question_returns_a_typed_decision_record` appears nowhere in `src`. Topology is reachable only through `AgentAssemble` (`src/server/handlers/decide/assemble.rs:57`, `topology_read.rs:44`).
- B. `specs/ingest-worker` (R001.2, .3, .10, .11, .15) asks the worker/SDK to autosize (0.36 of cores, 3 GiB), hold a hydration floor and route partitions, while `EG-DURABLE-KERNEL-R074`-`R077` (`specs/durable-graph-kernel/requirements.md:109-112`) give the engine the registry, admission, floors and shard-writer width. No code for either exists yet (`git grep AdmitWorkerTask|SchedulerConfig|WorkerRecord` finds nothing in `src`/`crates`), so the overlap is still only in specs and is cheap to remove now. The only live autosize knob is `EG_ANALYTICS_WORKERS` (`src/server/handlers/jobs/executor_loop.rs:22-25`, default 1, cap 32).
- C. `EG-DURABLE-KERNEL-R082.3` says the cap's existence "was not determinable". It is: `src/server/handlers/jobs/submit.rs:289` refuses `distinct_items > 31` (inside the same merged check as the threshold tests, one message at `:293`).

## Architecture

```
caller (graph-os / worker / submit)        engine (single authority)
 question + typed body ------Decide-----> QuestionKind::Placement (stat_placement.rs)
                                           |- candidates: PlacementTarget (declared options)
                                           |- hard constraints -> eliminated + why-not
                                           |- capacity headroom premise (capacity_premise.rs, no lease)
                                           |- DecisionPolicy caps (request may only tighten)
                                           '- cheapest feasible -> Advisory; none -> Abstained
 task + capabilities -------AgentAssemble-> graph.assemble() over published deployment template
                                           -> Solved graph (ordered fleet-tool steps) | Abstained
 worker Register{cores,mem} -> WorkerRecord (R074) -> AdmitWorkerTask (R075) -> WorkerGrant{slots,lane}
                                           SchedulerConfig (R076) + shard width (R077) decide everything
 submit MineAssociate -> submit.rs MAX_ASSOCIATION_DISTINCT_ITEMS=31 (u32 mask width of rule enumeration)
```

### A. Placement question

- **Question**: `QuestionKind::Placement`, candidates `CandidateSource::Declared` with option ids `placement:in_process|compose|helm|swarm` (same shape as `IngestionLane`, `statistical/ingestion_lane.rs:24-47`, and `ToolSubset`, `stat_tool_subset.rs`). It is a flat choice among deployment targets, not a template/slot assembly, so it does not go through `TopologyRequirements` (`decision/topology.rs:110`), which sizes widths of a swarm template. It reuses the topology path's pieces: the capacity headroom premise (`capacity_premise.rs:headroom`, R112), the policy-tightening rule (`TopologyCaps::loosened_field`, `topology.rs:~60`), and the standalone-typed-question dispatch R111.1 builds first.
- **What is placed**: `PlacementSubject{workload_iri, cpu_millis, memory_bytes, replicas, needs_persistent_volume}`.
- **Constraints**: `PlacementConstraints{requires_ha, forbidden_targets, max_relative_cost, capacity_cells, priority}`; capacity from the ledger at the stated `LeasePriority`; policy from the pinned `DecisionPolicy` (request can only tighten).
- **Target facts** are data, not code: an admin-attached schema source (like `swarm-topology`, `topology_schema.rs`) states per target `placement:supportsHa`, `placement:supportsVolumes`, `placement:maxReplicas`, `placement:relativeCost`. The engine hardcodes none of them. Only the four-member target registry is closed (it must match graph-os's supported artifacts; systemd is intentionally absent, GRAPHOS-DEPLOY-R017.6).
- **Answer**: chosen target (`Advisory` top score, `resolution_kind` Constraint when one candidate survives, Optimization when cost chooses), evidence class from premises (capacity headroom = Observation, target facts = their schema authority), why-not for each other target via `DecisionRecord.why_not`/`eliminated` (`record.rs:267-270`), and typed abstention via `StatisticalOutcome::Abstained{reasons: AbstainReason}` (`outcome.rs:107`) when no target is feasible or capacity facts are unavailable (`CAPACITY_UNAVAILABLE`, `topology_read`/`capacity_premise.rs:30`). No lease is taken (R046); acquisition stays `AcquireCapacity`.
- **Step graph**: separate call, `AgentAssemble` over a published deployment graph template whose slots are fleet-tool components (render-artifact, apply, verify). Chosen target selects the template by IRI (`AssemblyRequest.templates`).

### B. Single arrangement

The worker holds NO policy. Sequence: `register(cores, mem_bytes, lanes_supported)` -> engine returns `WorkerRecord` and the engine-computed `WorkerGrant{slots, reserved_lane}`; each claim goes through `AdmitWorkerTask`; claim order and lane are derived by the engine from `lane_for(kind)` and `partition_key(envelope)` (engine-side pure functions, ingest-worker R001.2/.3, now explicitly engine-called). Autosize constants (360 per-mille of cores, 3 GiB per worker), lane floors and the queue-depth limit become fields of `SchedulerConfig` (R076) with those defaults. The worker's `--workers` becomes a request ceiling the engine may lower, never raise. A one-worker minimum never overrides a zero request ceiling, insufficient available memory or zero writer width; those cases return a typed capacity refusal and no allocation. Admission is a deterministic served constraint (R075), not a `Decide` question: `QuestionKind::IngestionLane` (fast/medium/slow) is a different concept (derivation cost lanes) and must not be conflated with worker lanes general/hydration.

### C. Association bound

`rules_for_itemset` enumerates antecedent masks `for mask in 1u32..((1u32 << k) - 1)` (`crates/eg-compute/src/mining/association.rs:402`), and `rule_parts`/`rule_for_mask` take `mask: u32` (`:349`, `:364`). `k` is an itemset length, which cannot exceed the number of distinct items, so 31 distinct items guarantees `1u32 << k` cannot overflow (`1u32 << 32` overflows). The Apriori/Eclat/FP-Growth itemset engines themselves have no bound (`association.rs:124`, `:265`); `generate_rules` is `pub`, so a direct caller with k >= 32 is unprotected.

## Decisions

| # | Item | Outcome | Evidence and reasoning |
|---|---|---|---|
| D1 | `AgentAssemble` is a refusal stub? | CONFIRMED NOT a stub | `src/server/handlers/decide/assemble.rs:28-55` reads the library snapshot, runs `eg_compute::assemble`, returns `AssemblyResult`; routed at `src/server/dispatch/router/decision_plane.rs:64`. It refuses only when built without the `decide` feature (`assemble.rs:134-140`; `decide` is in `full`, `Cargo.toml:1278`, which `default` enables, `Cargo.toml:168`). The graph-os "stub" claim is therefore stale or describes a build without `decide`; propose reword (R001.14). |
| D2 | What does the topology decision path return today? | CONFIRMED | Only inside assembly: `TopologyPlan` (`topology/plan.rs:120`) when `request.requirements.topology` is set (`topology_read.rs:44-47`), else `None`. Standalone Decide topology question: absent (R111.1 open; test name not in tree). |
| D3 | Placement ID/shape | DECIDED | New `QuestionKind::Placement` with declared candidates, modelled on `IngestionLane`/`ToolSubset`; alternative (encode targets as swarm templates in `TopologyRequirements`) rejected: targets are not slots/widths, and it would stretch the swarm model. Depends on R111.1 only for the shared standalone-dispatch pattern, not for its types. |
| D4 | Assemble prerequisites | BLOCKED (owner: graph-os + typed packs) | Handler exists, but a `Solved` deployment graph needs published components: fleet-tool components and a deployment graph template in the tenant Agent Library. None exists for deployment; graph-os publishes them as a typed pack (`specs/typed-packs-and-catalog`). EG row R001.12 proves assembly with a fixture template. |
| D5 | Ingest-worker overlap | DECIDED | Engine owns admission, floors, widths, routing, autosize constants and depth limit; worker asks/obeys. Seven rows reworded below. Alternative (keep worker autosize as a hint) kept only as a request ceiling. |
| D6 | Why 31 | CONFIRMED | `u32` mask in `association.rs:402`, `:349`, `:364`; `1u32 << 32` overflows. Cap check `submit.rs:289`. |
| D7 | The bound | DECIDED | Keep 31 as named `MAX_ASSOCIATION_DISTINCT_ITEMS = u32::BITS - 1` in `submit.rs` (byte-identical parity with the old worker, R082.4, and rule enumeration cost is already 2^k, so a wider mask gains nothing: 2^63 is infeasible). Alternative (u64 masks, cap 63) rejected. Add a defence-in-depth guard so `generate_rules` cannot overflow for k >= 32. |
| D8 | Opaque-reference rule of R082.3 | DECIDED | Not a refusal: the engine itself maps every label to an opaque ref (`submit.rs:266-278`, `native_opaque_ref`) before counting, so an `eg:` prefix check on input would reject valid labels for no gain. The cap is measured after hashing. Tests cover deterministic references and distinct outputs for bounded fixtures; they do not prove that hashing is injective. R082.3's non-opaque-item test is replaced by a test that a plain label is accepted and stored only as an opaque ref. |

## Worked example

Question: "Where should the `graphos-core` workload (2000 mCPU, 4 GiB, 2 replicas, needs HA, no volume) run, among in_process, compose, helm, swarm?"

```
Decide{kind: placement, candidates: [placement:in_process, :compose, :helm, :swarm],
       params: workload_iri, cpu_millis=2000, memory_bytes=4GiB, replicas=2,
       requires_ha=true, capacity_cells=[cell-a], priority=Normal}
 -> eliminated: in_process (no HA), compose (single host: supportsHa=false)
 -> feasible: helm (cost 3), swarm (cost 2); capacity headroom cell-a covers both
 -> Advisory[ swarm 1.0 (Optimization), helm 0.0 ]   evidence_class: Observation
    why_not: in_process "placement:supportsHa=false", compose "placement:supportsHa=false",
             helm "costlier than swarm (3 > 2)"
Then AgentAssemble{task: deploy-graphos-core, templates:[deploy-swarm-template]}
 -> Solved graph: render-artifact -> apply -> verify (engine order, certificate)
```
If cell-a headroom is below demand for every feasible target: `Abstained[CAPACITY_INFEASIBLE lease:cell-a:cpu]` and no lease taken.

## Out of scope

Rendering or applying manifests (graph-os `deploy/`), acquiring capacity, systemd, runtime auto-migration between targets, and the worker runner itself (SDK).

### B producer implementation boundary

R002.2 is implemented by the pure `eg_types::scheduler_config` contract (R076.1). R076.2 owns its later served integration. Neither the autosize grant function nor the existing analytics pool has been switched by this producer slice.

R002.3 now provides pure `worker_registry::granted_slots` with `WorkerSizingInput` for the optional request ceiling, current available memory and engine-selected write classification. It consumes the real `WorkerRecord` and `SchedulerConfig`; the typed `WorkerSlotGrant` reports zero on refusal. CPU sizing alone may round up to one; memory, request ceiling, pool, disabled resources and writer width remain hard bounds. No reservations, served registration or R075 admission are claimed.

## Capability-declared heavy compute routing (R004)

Data flow: job submit wire method -> ready queue -> `claim_next` (`crates/eg-jobs/src/store.rs:736`) -> capability anchor filter (`store.rs:773`) -> claim on the capable host.

Types: `HostCapability`, `RoutingFact`. Attach point: `store.rs:773`. Production caller: `crates/eg-quantum-jobs/src/job.rs:275`, which passes no capabilities today; the Wiring row passes them. Failure behavior: `no_capable_host` leaves the job queued.

Reused: the capability anchor selection in `select_ready_for_worker`. New: capability declaration from graph topology facts.

Decision: hosts declare capabilities and the graph stores them. Rejected alternative: a fixed host list in configuration, because it drifts from the real topology.

Cross-repository: `GRAPHOS-` rows that submit jobs only pass the job class.

<!-- spec-definition-patch:owner-batch-20261012-placement-and-worker-policy -->
## Owner implementation plan — 2026-10-12

### `EG-PLACEMENT-R001.15` — EG:9

- Define pure `evaluate_capacity_adjustment(proposal, capacity_premise) -> CapacityAdjustmentVerdictV1`.
- Return a verdict for a typed capacity proposal. Bind it to tenant, resource class, and unit.
- Use unsigned integers for current/proposed capacity and minimum/maximum bounds. Include measured load and available headroom.
- Record the UTC millisecond window, premise revision, policy revision, and canonical proposal digest.
- EG validates against fresh R112 capacity premises and R001.6 placement feasibility.
- Refuse stale, cross-tenant, or malformed proposals. Refuse inconsistent units, invalid windows, and out-of-bound targets.
- Refuse increases above observed headroom. Return the applicable `CapacityRefusalCode` without host or graph mutation.
- GraphOS owns mode, AIMD proposal generation, limits, and actuation.
- GraphOS exposes an applied receipt for independent readback. The receipt binds proposal digest, requested/applied amount, resource/host, policy revision, and resulting state revision.
- Preserve `GRAPHOS-CAPACITY-R001.1`, `EG-DECISION-ENGINE-R112`, `GRAPHOS-CAPACITY-R001/R002`, and `EG-PLACEMENT-R001.6`. Source hash: `cb17ea03a7344584fed7a9f22a78c1e97c58919616433e7026481606fd0bdccb`.
- **Hold:** Reviewed evidence names no EG decision method or QuestionKind for this proposal. It names no GraphOS apply/readback operation or receipt schema.
- Resolve those exact owner API and receipt bindings before this candidate represents a wired closed loop.
<!-- /spec-definition-patch -->
