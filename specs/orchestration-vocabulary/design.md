# EG-ORCH-VOCAB-001 design

Prefixes: `:` = `http://knuckles.team/kg#`, `swarm:` = `http://knuckles.team/kg/swarm#`, `sh:` SHACL. Evidence below was read from `crates/eg-core/ontology/` and `crates/eg-types/src/` on main.

## Problem and evidence

`specs/orchestration-competency` lists terms as MISSING. Re-checked against the files:

- `grep -rn "hasStep\|handsOffTo\|AgentKind\|ModelProfile\|BestPractice\|AgentTemplate\|AgentLibrary\|needsSkill\|feeds" crates/eg-core/ontology` finds nothing except `:precedes` (see Corrections): the terms are genuinely absent.
- `catalog-v1.ttl` is a generated aggregate: it holds one `owl:Ontology` header and an `owl:imports` list, no class or property.
- The concepts exist in Rust but not in the ontology: `AgentTemplate` (`crates/eg-types/src/agent_template.rs`), `AgentLibraryEntry` (`agent_library.rs`), `AgentComponentKind::{ModelProfile, A2aAgentCard, GuardrailRule}` (`agent_component.rs`), `AgentGraphEdge` (`agent_graph.rs`). So the graph cannot be asked what the engine already stores.

## Architecture

```
question -> ontology terms (this spec) -> candidate source -> contract fetch -> answer
Q1 steps      :TaskStep :hasStep :precedes     task decomposition   in-graph (L0/L3)
Q2 staffing   swarm:Slot swarm:requiresAgentKind :AgentKind        template facts (L3/L2)
Q3 reuse      :AgentTemplate :AgentLibrary :A2AAgentCard           Agent Library store (L2)
Q4 guardrails :BestPractice :appliesToTaskClass                    GuardrailRule components
Q5 bindings   :ModelProfile :needsSkill :stepAssignedAgent         component catalog (L1)
Q6/Q7 flow    :handsOffTo swarm:feeds swarm:loopsBackTo            template + assembled graph
```

Writers (all existing): the Agent Library outbox projection (`EG-TYPED-PACKS-R006`; `src/server/connector_pack_projection.rs`, `src/server/persistence/agent_library/`) projects components, templates and library entries; the template projection (`EG-DECISION-ENGINE-R106`) projects topology facts; the assemble commit (`DecisionCommit` after `handle_agent_assemble`, `src/server/handlers/decide/assemble.rs`) projects the chosen assignment. Readers: the Q1-Q7 queries (federated query over the composed schema), `capability_coverage_query` (R126), `guardrail_entailment_query` (R127), and `graph.assemble()` itself for step expansion. External agent cards are fetched at query time through their A2A contract; only the card's declaration (name, skills, declared capabilities) is held, as a claim. Nothing here copies a source's data.

## Vocabulary

All new object properties carry `rdfs:label` and `rdfs:comment`. Domain "-" means none is declared, because a module must not constrain a shared property.

| Term | Kind | Domain | Range | File |
|---|---|---|---|---|
| `:TaskStep` | class, `rdfs:subClassOf bfo:0000001` | | | `orchestration-v1.ttl` |
| `:hasStep` | object property | - | `:TaskStep` | `orchestration-v1.ttl` |
| `:precedes` (exists, reused) | object property | `bfo:0000001` | `bfo:0000001` | `core-foundation-v1.ttl:549` |
| `:handsOffTo` | object property, sub-property of `:precedes` | `:TaskStep` | `:TaskStep` | `orchestration-v1.ttl` |
| `:needsSkill` | object property | `:TaskStep` | `:Skill` | `orchestration-v1.ttl` |
| `:stepAssignedAgent` | object property | `:TaskStep` | - (shape: `:Agent` or `:A2AAgentCard`) | `orchestration-v1.ttl` |
| `:AgentKind` | class; seven named individuals `:agentKindSupervisor`, `Worker`, `Verifier`, `Aggregator`, `Planner`, `Researcher`, `Executor` | | | `orchestration-v1.ttl` |
| `:hasAgentKind` | object property | `:Agent` | `:AgentKind` | `orchestration-v1.ttl` |
| `swarm:requiresAgentKind` | object property | `swarm:Slot` | `:AgentKind` | `swarm_topology-v1.ttl` (+ `owl:imports` orchestration) |
| `:AgentTemplate` | class, `rdfs:subClassOf :Agent` | | | `orchestration-v1.ttl` |
| `:AgentLibrary` | class | | | `orchestration-v1.ttl` |
| `:memberOfLibrary` | object property | - | `:AgentLibrary` | `orchestration-v1.ttl` |
| `:instanceOfTemplate` | object property | `:Agent` | `:AgentTemplate` | `orchestration-v1.ttl` |
| `:ModelProfile` | class, `rdfs:subClassOf :Model` | | | `orchestration-v1.ttl` |
| `:BestPractice` | class, `rdfs:subClassOf :DevelopmentStandard` | | | `capability-v1.ttl` |
| `:appliesToTaskClass` | object property | `:DevelopmentStandard` | `owl:Class` | `capability-v1.ttl` |
| `swarm:feeds` | object property | `swarm:Slot` | `swarm:Slot` | `swarm_topology-v1.ttl` |
| `swarm:loopsBackTo` | object property | `swarm:Slot` | `swarm:Slot` | `swarm_topology-v1.ttl` |

Reused unchanged: `:Agent`, `:Skill`, `:Tool`, `:Prompt`, `:Model`, `:hasSkill`, `:hasPrompt`, `:usesTool`, `:usesModel`, `:contextWindow`, `:A2AAgentCard`, `:exposesSkill`, `:registeredIn`, `:requiresCapability`, `:providesCapability`, `:DevelopmentStandard`, `swarm:TopologyTemplate`, `swarm:Slot`, `swarm:hasSlot`, `swarm:role`, `swarm:minWidth`, `swarm:maxWidth`.

Why `:AgentTemplate` is an `:Agent`: the Rust type instantiates to an ordinary `AgentLibraryEntry` and "is admitted, delegated, pinned and traversed exactly like a hand-published agent" (`agent_template.rs`). Subclassing lets every existing agent property apply. It is not `swarm:TopologyTemplate`, which is an agent-graph shape with topology facts; the two names must not be confused (`AgentTemplate` = parameterized agent; `TopologyTemplate` = parameterized team shape).

SHACL (new file `crates/eg-core/ontology/orchestration-v1.shapes.ttl`, same style as `swarm_topology-v1.shapes.ttl`: SHACL Core plus aggregate-free `sh:sparql`, absence checks by `OPTIONAL ... FILTER(!BOUND(...))`; property paths are not aggregates):

- `:TaskStepShape` (target `:TaskStep`): `sh:sparql` selecting `$this` where `$this :precedes+ $this`; exactly one owner: `sh:path [sh:inversePath :hasStep]` `sh:minCount 1; sh:maxCount 1`.
- `:LibraryMemberShape`: every subject of `:memberOfLibrary` has `:providesCapability` (`sh:minCount 1`), so coverage cannot find a member that covers nothing.
- `:RuleScopeShape`: target every `:BestPractice` emitted by the guardrail-rule projection, including nodes missing the scope property; require exactly one `:appliesToTaskClass`, an IRI. This target detects the missing-class negative case.
- `:StepBindingShape`: `:stepAssignedAgent` value is `sh:or (sh:class :Agent, sh:class :A2AAgentCard)`; an `:Agent` assignee has `:usesModel` and `:hasPrompt` exactly once.
- In `swarm_topology-v1.shapes.ttl`: `swarm:SlotShape` gains `swarm:requiresAgentKind` (`sh:maxCount 1; sh:class :AgentKind`); new `swarm:FeedsAcyclicShape` (target `swarm:TopologyTemplate`): `SELECT $this WHERE { $this swarm:hasSlot ?s . ?s swarm:feeds+ ?s }` plus a check that both ends of every `swarm:feeds` are `swarm:hasSlot` of `$this`.

Loops: `CritiqueLoop`, `Debate` and `Council` are cyclic by design. Forward flow uses `swarm:feeds` (must be acyclic); the single permitted back-edge uses `swarm:loopsBackTo` (not in the acyclicity path). So "is the graph acyclic" is answered on `feeds` alone and loop structure stays visible.

## Wiring

| Term | Written by | Read by |
|---|---|---|
| `:TaskStep`, `:hasStep`, `:precedes`, `:needsSkill`, `:requiresCapability` on steps | task decomposition facts from process/SDD sources (L0, virtual) or authored with the task; no new writer | Q1; `handle_agent_assemble` step expansion (R007.1) |
| `:handsOffTo` | same decomposition source; or projected from `AgentGraphEdge` of a stored workflow | Q6 |
| `:AgentKind`, `:hasAgentKind` | pack annotation on the component (typed annotations, `EG-TYPED-PACKS-R006`), projected by the outbox consumer | Q2, Q3 |
| `swarm:requiresAgentKind`, `swarm:feeds`, `swarm:loopsBackTo` | template projection (`EG-DECISION-ENGINE-R106`) from `TopologyFacts` and `AgentGraphEdge`; assemble commit for chosen graphs | Q2, Q7; R046/R111 topology decision |
| `:AgentTemplate`, `:AgentLibrary`, `:memberOfLibrary`, `:instanceOfTemplate` | Agent Library outbox projection from `AgentTemplateDraft`, `AgentLibraryEntry`, `TemplateInstanceRef` | Q3; `capability_coverage_query` (R126.2.1 resolves candidates from the same store) |
| `:A2AAgentCard` library member | same projection, from `a2a_agent_card`-kind `AgentComponent`s; capabilities from `declared_capabilities` at claim strength | Q3; assemble candidate elimination (`eliminate.rs`, `a2a_requires_observation` policy) |
| `:BestPractice`, `:appliesToTaskClass` | projection of `guardrail_rule` components (`applies_to_class`, `source_generation`; R127.2.1) | Q4; `guardrail_entailment_query` (R127.2.2) |
| `:ModelProfile`, `:contextWindow` | projection of `model_profile` components | Q5; assemble `context_window` check |
| `:stepAssignedAgent` | assemble commit (R007.2) | Q5, Q8 |

Scopes: only those the engine contract already lists (tenant-visible Agent Library reads; no new scope). Visibility filtering is unchanged: a record unprojected or invisible is excluded from reads (`EG-TYPED-PACKS-R009`).

How `graph.assemble()` consumes it: (1) the request names a task IRI; if the task has `:hasStep` steps, each step becomes one typed task with its required capabilities, so the 0-1 program (R014) covers per-step capability; (2) candidates are the visible library entries, instantiated templates and A2A cards, as today; (3) topology comes from `TopologyFacts`/slots, whose `swarm:requiresAgentKind` restricts which kind may fill a slot (R007.4); (4) the committed `AgentGraphDraft` is projected back as `:stepAssignedAgent` and `swarm:feeds`. The solver gains no term list; kinds and edges arrive as data.

External A2A agent to library member: a card is published as an `AgentComponent` of kind `a2a_agent_card` into the Agent Library store (already the case, R126.2.1). The projection types it `:A2AAgentCard`, links `:memberOfLibrary`, `:registeredIn` its registry, and `:providesCapability` per declared capability. It is usable by assembly as a claim: coverage lists it with claim evidence class, and when policy `a2a_requires_observation` is on it is not selectable until an observation exists. Task execution is fetched at run time through the A2A contract; the card body is not copied beyond its declaration.

## Decisions

1. AgentTemplate placement: DECIDED `orchestration-v1.ttl`. Reason: it is an agent (subclass of `:Agent`, which lives there with `:hasSkill`/`:usesModel`), and `catalog-v1.ttl` is a generated import-only aggregate (read: header plus `owl:imports`, no term), so a term there would be overwritten on regeneration. Alternative: swarm vocabulary, rejected because a template is not a topology fact (`swarm:TopologyTemplate` already is that).
2. Step order: DECIDED reuse core `:precedes` for task steps and add `swarm:feeds` for slots; no `eg:precedes`. Reason: `:precedes` exists (`core-foundation-v1.ttl:549`, domain/range `bfo:0000001`, inverse `:succeeds`). The swarm vocabulary has no ordering edge between slots (its properties: `admits`, `class`, `hasSlot`, `role`, `hasStopRule`, `lease`, `nodeId`, `nodeKind`, `minWidth`, `maxWidth`, `maxRounds`, `k`, `n`, `resourceClass`, `amount`; `swarm:feeds` is absent), and steps and slots are different layers (what to do versus who does it), so the swarm edge cannot carry step order. Alternative: use swarm edges for steps, rejected because a step has no slot until assembly.
3. `swarm:` name verification: CONFIRMED, see table below.
4. A2A cards as library members: DECIDED as described under Wiring (projection types the card, claim-strength capabilities, observation policy). Alternative, a separate external-agent registry, rejected as a second authority (`EG-TYPED-PACKS-R072`).
5. Hand-off with conditions: DECIDED `:handsOffTo` direct only. Conditional edges stay on `AgentGraphEdge.condition` (a pinned `Predicate` component) and are not duplicated into the ontology. Alternative: reified `:HandOff`, rejected for now as a second copy of the predicate pin.
6. `:stepAssignedAgent` instead of reusing `:assignedAgent`: DECIDED. `sdd-v1.ttl:32` `:assignedAgent` has range `:A2AAgentCard`, so using it with an `:Agent` would infer the agent to be a card.
7. Agent kinds as individuals, not subclasses: DECIDED, so a slot constraint is a value, tenants add kinds under their own GraphSchema key (as the swarm header says tenants extend), and no class explosion.

### Verification of `swarm:` names cited by the competency spec (`swarm_topology-v1.ttl`, `swarm_topology-v1.shapes.ttl`)

| Name | Actual type | Domain / range declared | Competency usage |
|---|---|---|---|
| `swarm:DecomposableTask` | `owl:Class`, subclass of `swarm:TaskShape` (plus admits Pipeline, SupervisorWorkers, Hierarchy) | n/a | A task class (a shape), not a task instance's type in general: a concrete task IRI is an instance of, or subclass of, it. Usable. |
| `swarm:Topology` | `owl:Class` (superclass of Single, Pipeline, FanOutJoin, SupervisorWorkers, PeerTeam, Hierarchy) | n/a | Correct. |
| `swarm:Slot` | `owl:Class` | n/a | Correct. |
| `swarm:hasSlot` | `owl:ObjectProperty` | none declared (shape: subject is a template, max 6 slots) | Correct as a template-to-slot link. |
| `swarm:minWidth`, `swarm:maxWidth` | `owl:DatatypeProperty` | range `xsd:nonNegativeInteger`; no domain (shape: on `swarm:Slot`, 1..16) | Correct, on slots. |
| `swarm:TopologyTemplate` | `owl:Class` | n/a | Correct. |
| `swarm:nodeKind` | `owl:DatatypeProperty`, range `xsd:string` | none; the shape reads it on the template (`$this swarm:nodeKind "fanout"/"join"`), not on a slot | Competency lists it under slot facts: wrong subject. |

### Corrections to `specs/orchestration-competency` (not edited there)

1. Q1 lists `:precedes` as MISSING; it exists in `core-foundation-v1.ttl:549`. Only `:TaskStep` and `:hasStep` are missing; the `eg:precedes` in the Q1 sketch should be `:precedes`.
2. Q1 sketch uses `:childWorkItemOf` between steps and tasks; it has domain `:WorkItem` and range `:Loop` (`orchestration-v1.ttl:131`), so it is not a step-to-task link. Use `:hasStep`.
3. Q2: `swarm:nodeKind` is a template-level string, not a slot property; slot role is `swarm:role`, whose values are role classes (`swarm:Parent`, ...). The `?slot swarm:role ?role` sketch is correct.
4. Q3 and R003.1 place `eg:AgentTemplate` in `catalog-v1.ttl`; see Decision 1: `orchestration-v1.ttl`. Also the `eg:` prefix does not exist in these ontologies; terms use `:` (`http://knuckles.team/kg#`) and `swarm:`.
5. Q4 sketch `?task :mustFollow ?rule`: `:mustFollow` has domain `:Team` and range `:DevelopmentStandard` (`capability-v1.ttl:302`), so it is not task-to-rule. Rules reach a task by `:appliesToTaskClass` plus subclass entailment (R127.1).
6. Q4/R004.1 puts `BestPractice` in `capability-v1.ttl`: agreed, as a subclass of `:DevelopmentStandard`, which is declared there.
7. Q5 sketch uses `:assignedAgent`; its range is `:A2AAgentCard` (`sdd-v1.ttl:32`). Use `:stepAssignedAgent`. Also `ModelProfile` is a component kind in Rust; in the ontology it subclasses `:Model`, which already carries `:contextWindow`.
8. Q6 and R006.1 name `eg:hands_off_to`/`eg:handsOffTo`; the term is `:handsOffTo`. Q7's `eg:feeds` is `swarm:feeds`.

## Worked example

Task: "deploy a container with container-manager" (competency probe R009.2, previously a 1-step plan). Fixture IRIs are illustrative; the fixture file is `crates/eg-core/tests/fixtures/orchestration_vocabulary/deploy_container.ttl`.

```
@prefix : <http://knuckles.team/kg#> .
@prefix swarm: <http://knuckles.team/kg/swarm#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

:DeployContainerTask a swarm:SequentialDependency ; :hasStep :s1, :s2, :s3, :s4 .
:s1 a :TaskStep ; rdfs:label "resolve target context" ; :handsOffTo :s2 ; :precedes :s2 ; :requiresCapability :ContainerOrchestrationCapability .
:s2 a :TaskStep ; rdfs:label "verify image" ; :precedes :s3 ; :needsSkill :skillImageVerify .
:s3 a :TaskStep ; rdfs:label "run container" ; :precedes :s4 ; :needsSkill :skillContainerLifecycle ; :stepAssignedAgent :agentContainerOps .
:s4 a :TaskStep ; rdfs:label "verify health" ; :stepAssignedAgent :agentVerifier .
:tmplPipeline a swarm:TopologyTemplate ; swarm:class swarm:Pipeline ; swarm:hasSlot :slotA, :slotB .
:slotA swarm:role swarm:Child ; swarm:requiresAgentKind :agentKindExecutor ; swarm:minWidth 1 ; swarm:maxWidth 1 ; swarm:feeds :slotB .
:slotB swarm:role swarm:Verifier ; swarm:requiresAgentKind :agentKindVerifier ; swarm:minWidth 1 ; swarm:maxWidth 1 .
:agentContainerOps a :AgentTemplate ; :memberOfLibrary :libMain ; :hasAgentKind :agentKindExecutor ; :providesCapability :ContainerOrchestrationCapability ;
                   :hasSkill :skillContainerLifecycle ; :usesModel :profileLocal ; :hasPrompt :promptOps .
:agentVerifier a :Agent ; :hasAgentKind :agentKindVerifier ; :hasSkill :skillHealthVerify ; :usesModel :profileLocal ; :hasPrompt :promptVerify .
:libMain a :AgentLibrary .
:promptOps a :Prompt .
:promptVerify a :Prompt .
:skillContainerLifecycle a :Skill .
:skillImageVerify a :Skill .
:skillHealthVerify a :Skill .
:slotA a swarm:Slot .
:slotB a swarm:Slot .
:profileLocal a :ModelProfile ; :contextWindow 32768 .
:ruleNoLatest a :BestPractice ; :appliesToTaskClass swarm:SequentialDependency .   # from a guardrail_rule component
:cardRemoteOps a :A2AAgentCard ; :memberOfLibrary :libMain ; :providesCapability :ContainerOrchestrationCapability .  # claim
```

Questions and answers:

- Q1: `SELECT ?s ?next WHERE { :DeployContainerTask :hasStep ?s . OPTIONAL { ?s :precedes ?next } }` returns s1->s2, s2->s3, s3->s4, s4->none, each with provenance.
- Q2: `SELECT ?slot ?role ?kind ?min ?max WHERE { :tmplPipeline swarm:hasSlot ?slot . ?slot swarm:role ?role ; swarm:requiresAgentKind ?kind ; swarm:minWidth ?min ; swarm:maxWidth ?max }` returns two slots, 2 agents (min 2, max 2), kinds executor and verifier.
- Q3: R126 `capability_coverage_query` for the task returns, for `ContainerOrchestrationCapability`, candidates `:agentContainerOps` (template, library evidence) and `:cardRemoteOps` (a2a, claim).
- Q4: R127 returns `:ruleNoLatest` with path `DeployContainerTask -> SequentialDependency` and its source generation.
- Q5: `SELECT ?s ?agent ?skill ?model ?prompt WHERE { ?s :stepAssignedAgent ?agent . ?agent :hasSkill ?skill ; :usesModel ?model ; :hasPrompt ?prompt }` returns the s3 row with `:profileLocal` (context 32768).
- Q6: `SELECT ?s ?t WHERE { ?s :handsOffTo ?t }` returns s1->s2; Q7: `SELECT ?a ?b WHERE { :tmplPipeline swarm:hasSlot ?a, ?b . ?a swarm:feeds ?b }` returns slotA->slotB, and `FeedsAcyclicShape` conforms.
- Q8: `graph.assemble()` expands the four steps into four typed tasks (R007.1), selects the pipeline topology with slot kinds as constraints, binds `:agentContainerOps` (observed, preferred over the claim-only card), commits, and the projection writes `:stepAssignedAgent` and `swarm:feeds` (R007.2) so Q5 and Q7 read the assembled graph. If no library member covers a capability the answer is a typed abstention naming it (R010).

## Out of scope

Solver or certificate changes; new wire methods or scopes; a task-decomposition engine (steps come from sources or authoring); conditional hand-off modelling beyond `AgentGraphEdge.condition`; generated-client work (`gen_contract` needs the build host); graph-os tool code.
