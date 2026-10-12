# EG-ORCH-VOCAB — Implementation plan

## Dependencies and unresolved owner contracts

- `EG-REPO-INGEST-R009` defines repository/source relationships; `AU-HARNESS-R024` supplies connected execution spans. Neither row currently defines the AU walkthrough producer input/output binding or the WebUI interactive artifact consumer. Those exact owner requirement IDs and served receipts remain pending coordinator assignment; `EG-ORCH-VOCAB-R013.7` is SPECIFIED and cannot be treated as end-to-end closed.
- `AU-HARNESS-R019.5-.7` are the existing study-gap producer/consumer prerequisites for `EG-CQ-ORCH-R010.7`; they must carry the same repository and revision as `EG-CQ-ORCH-R010.1-.6`. If the AU rows lack a stable proposal id/revision, that missing producer field remains a hold.
- `WEBUI-APPS-R004.7` and `.8` are the R010.100/.101 consumers. AU progress and SDK manifest/sync producer IDs remain unestablished. GraphOS served-adapter IDs also remain unestablished. These are explicit holds.
- Existing `EG-DECISION-ENGINE-R036`, `R135.2`, `R135.4`, and `EG-TYPED-PACKS-R006` are prerequisites for `EG-ORCH-VOCAB-R003.8`; they define the assembly authority, program store, and Agent Library projection.

The architecture and vocabulary decisions are specified in [design.md](plan.md). EG reuses the existing orchestration competency,
decision-engine and typed-pack contracts. EG introduces no second planner
and no execution runtime.

## Implementation sequence

1. Add the declared ontology terms and SHACL constraints to the existing schema
   sources. Validate missing rule scope and malformed agent descriptions with
   refusal fixtures before changing readers.
2. Write the agent-kind and required-kind projections from admitted records.
   Preserve the source record and provenance for each projection.
3. Extend the assembly reader to filter slots by the required agent kind and
   return a typed abstention when no compatible agent is available.
4. Exercise the standalone Turtle example, projection queries and negative
   fixtures defined in [test-spec.md](test-spec.md). Keep served/client acceptance
   dependencies open until the corresponding runtime evidence exists.

Each child follows [requirements.md](requirements.md) and the generated delivery
record. Regenerate that record after implementation commits; specification-only
changes do not establish that a runtime capability has been delivered.

## Design

Prefixes: `:` = `http://knuckles.team/kg#`, `swarm:` = `http://knuckles.team/kg/swarm#`, `sh:` SHACL. Evidence below was read from `crates/eg-core/ontology/` and `crates/eg-types/src/` on main.

### Problem and evidence

`specs/orchestration-competency` lists terms as MISSING. Re-checked against the files:

- `grep -rn "hasStep\|handsOffTo\|AgentKind\|ModelProfile\|BestPractice\|AgentTemplate\|AgentLibrary\|needsSkill\|feeds" crates/eg-core/ontology` finds nothing except `:precedes` (see Corrections): the terms are genuinely absent.
- `catalog-v1.ttl` is a generated aggregate: it holds one `owl:Ontology` header and an `owl:imports` list, no class or property.
- The concepts exist in Rust but not in the ontology: `AgentTemplate` (`crates/eg-types/src/agent_template.rs`), `AgentLibraryEntry` (`agent_library.rs`), `AgentComponentKind::{ModelProfile, A2aAgentCard, GuardrailRule}` (`agent_component.rs`), `AgentGraphEdge` (`agent_graph.rs`). So the graph cannot be asked what the engine already stores.

### Architecture

```
question -> ontology terms (this spec) -> candidate source -> contract fetch -> answer
Q1 steps      :TaskStep :hasStep :precedes     task decomposition   in-graph (L0/L3)
Q2 staffing   swarm:Slot swarm:requiresAgentKind :AgentKind        template facts (L3/L2)
Q3 reuse      :AgentTemplate :AgentLibrary :A2AAgentCard           Agent Library store (L2)
Q4 guardrails :BestPractice :appliesToTaskClass                    GuardrailRule components
Q5 bindings   :ModelProfile :needsSkill :stepAssignedAgent         component catalog (L1)
Q6/Q7 flow    :handsOffTo swarm:feeds swarm:loopsBackTo            template + assembled graph
```

Writers (all existing): the Agent Library outbox projection (`EG-TYPED-PACKS-R006`; `src/server/connector_pack_projection.rs`, `src/server/persistence/agent_library/`) projects components, templates and library entries. The template projection (`EG-DECISION-ENGINE-R106`) projects topology facts. The assemble commit (`DecisionCommit` after `handle_agent_assemble`, `src/server/handlers/decide/assemble.rs`) projects the chosen assignment. Readers: the Q1-Q7 queries (federated query over the composed schema), `capability_coverage_query` (R126), `guardrail_entailment_query` (R127), and `graph.assemble()` itself for step expansion. External agent cards are fetched at query time through their A2A contract. Only the card's declaration (name, skills, declared capabilities) is held, as a claim. Nothing here copies a source's data.

### Vocabulary

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

Why `:AgentTemplate` is an `:Agent`: Rust instantiates it as an ordinary `AgentLibraryEntry` (`agent_template.rs`). The entry is admitted, delegated, pinned and traversed like a hand-published agent. Subclassing lets every existing agent property apply. It is not `swarm:TopologyTemplate`. That term means an agent-graph shape with topology facts. Do not confuse the names: `AgentTemplate` is a parameterized agent. `TopologyTemplate` is a parameterized team shape.

SHACL (new file `crates/eg-core/ontology/orchestration-v1.shapes.ttl`, same style as `swarm_topology-v1.shapes.ttl`: SHACL Core plus aggregate-free `sh:sparql`, absence checks by `OPTIONAL ... FILTER(!BOUND(...))`; property paths are not aggregates):

- `:TaskStepShape` (target `:TaskStep`): `sh:sparql` selecting `$this` where `$this :precedes+ $this`; exactly one owner: `sh:path [sh:inversePath :hasStep]` `sh:minCount 1; sh:maxCount 1`.
- `:LibraryMemberShape`: every subject of `:memberOfLibrary` has `:providesCapability` (`sh:minCount 1`), so coverage cannot find a member that covers nothing.
- `:RuleScopeShape`: target every `:BestPractice` emitted by the guardrail-rule projection, including nodes missing the scope property; require exactly one `:appliesToTaskClass`, an IRI. This target detects the missing-class negative case.
- `:StepBindingShape`: `:stepAssignedAgent` value is `sh:or (sh:class :Agent, sh:class :A2AAgentCard)`; an `:Agent` assignee has `:usesModel` and `:hasPrompt` exactly once.
- In `swarm_topology-v1.shapes.ttl`: `swarm:SlotShape` gains `swarm:requiresAgentKind` (`sh:maxCount 1; sh:class :AgentKind`); new `swarm:FeedsAcyclicShape` (target `swarm:TopologyTemplate`): `SELECT $this WHERE { $this swarm:hasSlot ?s . ?s swarm:feeds+ ?s }` plus a condition requiring both ends of every `swarm:feeds` are `swarm:hasSlot` of `$this`.

Loops: `CritiqueLoop`, `Debate` and `Council` are cyclic by design. Forward flow uses `swarm:feeds` (must be acyclic); the single permitted back-edge uses `swarm:loopsBackTo` (not in the acyclicity path). So "is the graph acyclic" is answered on `feeds` alone and loop structure stays visible.

### Wiring

| Term | Written by | Read by |
|---|---|---|
| `:TaskStep`, `:hasStep`, `:precedes`, `:needsSkill`, `:requiresCapability` on steps | task decomposition facts from process/SDD sources (L0, virtual) or authored with the task. No new writer | Q1. `handle_agent_assemble` step expansion (R007.1) |
| `:handsOffTo` | same decomposition source; or projected from `AgentGraphEdge` of a stored workflow | Q6 |
| `:AgentKind`, `:hasAgentKind` | pack annotation on the component (typed annotations, `EG-TYPED-PACKS-R006`), projected by the outbox consumer | Q2, Q3 |
| `swarm:requiresAgentKind`, `swarm:feeds`, `swarm:loopsBackTo` | template projection (`EG-DECISION-ENGINE-R106`) from `TopologyFacts` and `AgentGraphEdge`; assemble commit for chosen graphs | Q2, Q7; R046/R111 topology decision |
| `:AgentTemplate`, `:AgentLibrary`, `:memberOfLibrary`, `:instanceOfTemplate` | Agent Library outbox projection from `AgentTemplateDraft`, `AgentLibraryEntry`, `TemplateInstanceRef` | Q3; `capability_coverage_query` (R126.2.1 resolves candidates from the same store) |
| `:A2AAgentCard` library member | same projection, from `a2a_agent_card`-kind `AgentComponent`s; capabilities from `declared_capabilities` at claim strength | Q3; assemble candidate elimination (`eliminate.rs`, `a2a_requires_observation` policy) |
| `:BestPractice`, `:appliesToTaskClass` | projection of `guardrail_rule` components (`applies_to_class`, `source_generation`; R127.2.1) | Q4; `guardrail_entailment_query` (R127.2.2) |
| `:ModelProfile`, `:contextWindow` | projection of `model_profile` components | Q5; assemble `context_window` validation |
| `:stepAssignedAgent` | assemble commit (R007.2) | Q5, Q8 |

Scopes: only those the engine contract already lists (tenant-visible Agent Library reads; no new scope). Visibility filtering is unchanged: a record unprojected or invisible is excluded from reads (`EG-TYPED-PACKS-R009`).

How `graph.assemble()` consumes it: the request names a task IRI. Each `:hasStep` becomes one typed task with required capabilities. The R014 0-1 program then covers per-step capability. Candidates include visible library entries, instantiated templates, and A2A cards. Topology comes from `TopologyFacts` and slots. `swarm:requiresAgentKind` limits which kind fills each slot (R007.4). The committed `AgentGraphDraft` projects back as `:stepAssignedAgent` and `swarm:feeds`. The solver gains no term list. Kinds and edges arrive as data.

External A2A agent to library member: a card is published as an `AgentComponent` of kind `a2a_agent_card` into the Agent Library store (R126.2.1 does this). The projection types it `:A2AAgentCard` and links `:memberOfLibrary`. It also links `:registeredIn` its registry and `:providesCapability` per declared capability. Assembly may use it as a claim. Coverage lists it with claim evidence class. When policy `a2a_requires_observation` is on it is not selectable until an observation exists. The A2A contract fetches task execution at run time. The engine copies nothing of the card body beyond its declaration.

### Decisions

1. AgentTemplate placement: DECIDED `orchestration-v1.ttl`. Reason: it is an agent, a subclass of `:Agent`, which lives there with `:hasSkill` and `:usesModel`. Also, `catalog-v1.ttl` is a generated import-only aggregate (read: header plus `owl:imports`, no term), so regeneration overwrites a term placed there. Alternative: swarm vocabulary, rejected because a template is not a topology fact (`swarm:TopologyTemplate` already is that).
2. Step order: DECIDED. Reuse core `:precedes` for task steps and add `swarm:feeds` for slots. Do not add `eg:precedes`. Core `:precedes` exists (`core-foundation-v1.ttl:549`). Its domain and range are `bfo:0000001`; its inverse is `:succeeds`. The swarm vocabulary has no slot-order edge. Its listed properties are `admits`, `class`, `hasSlot`, `role`, `hasStopRule`, `lease`, `nodeId`, `nodeKind`, `minWidth`, `maxWidth`, `maxRounds`, `k`, `n`, `resourceClass`, and `amount`. `swarm:feeds` is absent there. Steps and slots are different layers: what to do versus who does it. Therefore, a swarm edge cannot carry step order. Alternative rejected: use swarm edges for steps. A step has no slot until assembly.
3. `swarm:` name verification: CONFIRMED, see table below.
4. A2A cards as library members: DECIDED as described under Wiring (projection types the card, claim-strength capabilities, observation policy). Alternative, a separate external-agent registry, rejected as a second authority (`EG-TYPED-PACKS-R072`).
5. Hand-off with conditions: DECIDED `:handsOffTo` direct only. Conditional edges stay on `AgentGraphEdge.condition` (a pinned `Predicate` component) and are not duplicated into the ontology. Alternative: reified `:HandOff`, rejected for now as a second copy of the predicate pin.
6. `:stepAssignedAgent` instead of reusing `:assignedAgent`: DECIDED. `sdd-v1.ttl:32` `:assignedAgent` has range `:A2AAgentCard`, so using it with an `:Agent` infers the agent to be a card.
7. Agent kinds are individuals, not subclasses: DECIDED. A slot constraint is a value. Tenants add kinds under their own GraphSchema key, as the swarm header allows. This avoids class explosion.

#### Verification of `swarm:` names cited by the competency spec (`swarm_topology-v1.ttl`, `swarm_topology-v1.shapes.ttl`)

| Name | Actual type | Domain / range declared | Competency usage |
|---|---|---|---|
| `swarm:DecomposableTask` | `owl:Class`, subclass of `swarm:TaskShape` (plus admits Pipeline, SupervisorWorkers, Hierarchy) | n/a | A task IRI is an instance of or subclass of this shape. It is not a generic task-instance type. Usable. |
| `swarm:Topology` | `owl:Class` (superclass of Single, Pipeline, FanOutJoin, SupervisorWorkers, PeerTeam, Hierarchy) | n/a | Correct. |
| `swarm:Slot` | `owl:Class` | n/a | Correct. |
| `swarm:hasSlot` | `owl:ObjectProperty` | none declared (shape: subject is a template, max 6 slots) | Correct as a template-to-slot link. |
| `swarm:minWidth`, `swarm:maxWidth` | `owl:DatatypeProperty` | range `xsd:nonNegativeInteger`; no domain (shape: on `swarm:Slot`, 1..16) | Correct, on slots. |
| `swarm:TopologyTemplate` | `owl:Class` | n/a | Correct. |
| `swarm:nodeKind` | `owl:DatatypeProperty`, range `xsd:string` | none. The shape reads it on the template (`$this swarm:nodeKind "fanout"/"join"`), not on a slot | Competency lists it under slot facts: wrong subject. |

#### Corrections to `specs/orchestration-competency` (not edited there)

1. Q1 lists `:precedes` as MISSING; it exists in `core-foundation-v1.ttl:549`. Only `:TaskStep` and `:hasStep` are missing; the `eg:precedes` in the Q1 sketch should be `:precedes`.
2. Q1 sketch uses `:childWorkItemOf` between steps and tasks; it has domain `:WorkItem` and range `:Loop` (`orchestration-v1.ttl:131`), so it is not a step-to-task link. Use `:hasStep`.
3. Q2: `swarm:nodeKind` is a template-level string, not a slot property; slot role is `swarm:role`, whose values are role classes (`swarm:Parent`, ...). The `?slot swarm:role ?role` sketch is correct.
4. Q3 and R003.1 place `eg:AgentTemplate` in `catalog-v1.ttl`; see Decision 1: `orchestration-v1.ttl`. Also the `eg:` prefix does not exist in these ontologies; terms use `:` (`http://knuckles.team/kg#`) and `swarm:`.
5. Q4 sketch `?task :mustFollow ?rule`: `:mustFollow` has domain `:Team` and range `:DevelopmentStandard` (`capability-v1.ttl:302`), so it is not task-to-rule. Rules reach a task by `:appliesToTaskClass` plus subclass entailment (R127.1).
6. Q4/R004.1 puts `BestPractice` in `capability-v1.ttl`: agreed, as a subclass of `:DevelopmentStandard`, which is declared there.
7. Q5 sketch uses `:assignedAgent`; its range is `:A2AAgentCard` (`sdd-v1.ttl:32`). Use `:stepAssignedAgent`. Also `ModelProfile` is a component kind in Rust; in the ontology it subclasses `:Model`, which already carries `:contextWindow`.
8. Q6 and R006.1 name `eg:hands_off_to`/`eg:handsOffTo`; the term is `:handsOffTo`. Q7's `eg:feeds` is `swarm:feeds`.

### Worked example

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
- Q8: `graph.assemble()` expands the four steps into four typed tasks (R007.1). It selects pipeline topology using slot kinds as constraints. It binds observed `:agentContainerOps`, preferred over the claim-only card. It commits the graph. Projection writes `:stepAssignedAgent` and `swarm:feeds` (R007.2). Q5 and Q7 then read the assembled graph. If no library member covers a capability, the answer is a typed abstention naming it (R010).

### Out of scope

Solver or certificate changes. New wire methods or scopes. A task-decomposition engine (steps come from sources or authoring). Conditional hand-off modelling beyond `AgentGraphEdge.condition`. Generated-client work (`gen_contract` needs the build host). Graph-os tool code.

## Foundational class implementation and validation

EG-ORCH-VOCAB-R001.1 and EG-ORCH-VOCAB-R004.1 declare the foundational classes. Native
parser/reasoner acceptance lives in `crates/eg-rdf/tests/orchestration_vocabulary.rs`:
`eg-rdf` owns the OWL API and already depends on `eg-core`.
Placing these tests in eg-core inverts the dependency layering. Tests cover the shipped foundation, orchestration and capability documents.
They include a positive step-order fixture, unrelated-superclass negatives
and a planted disjointness contradiction.
Step properties, shapes, rule scope and consumer wiring remain open.
Parsed-source corpus counts are refreshed. Parser-only checks do not establish native reasoner consistency.
Compilation of `eg-types` terminated by SIGKILL before these tests executed.
Full-corpus native validation also remains unrun.
Test markers bind the existing acceptance tests to their owning rows.
The generated lifecycle records test existence, independently of execution results.

## Harness run-trace vocabulary and measured-fact loop

This spec owns the run-trace vocabulary. `:RunTrace` is declared in `orchestration-v1.ttl:653`, and the neighbouring `:tokenCost`, `:evaluationScore`, `:ModelProfile` and `:contextWindow` rows already extend it here. A second owner for run-trace terms splits one class across two specs.

### Data flow

Ingestion: agent-utilities run end → generated client RunTrace method (`EG-CONTRACT-R051.1`) → `Method::AddNode` (`crates/eg-types/src/protocol/method/method_00.rs:6`) → `AddNode` arm (`src/server/handlers/graph_ops/gateway_graph.rs:62`) → shape check (R009.15) → graph store → Cypher read (`crates/eg-query/src/cypher/exec.rs:4574` already reads `RunTrace` nodes by id).

Measured fact: evaluation run → `measured_fact_envelope` (R011.7) → `ApplyChangeEnvelopes` → `dispatch_change_envelopes` (`src/server/dispatch/change_envelope.rs:36`) → `ChangeEnvelope::validate` (`crates/eg-types/src/change_envelope.rs:248`, evidence refusal R011.8) → `:MeasuredModelFact` node → `Decide` for `HarnessConfig`.

### Types and fields

On `:RunTrace`: `terminationReason`, `peakContextTokens`, `contextBudgetTokens`, `elisionCount`, `summarizationCount`, `turnCount`, `toolCallCount`, `costUsd`, `runModel`, `runTaskClass`, and `appliedHarnessConfig` (a link to the committed `:Decision`). On the new `:MeasuredModelFact`: `factOfModel`, `measuredProperty`, the value, `evidencedByRun`, `measuredSupersedes`. The names match the cross-repository brief exactly.

### Attach point and failure behaviour

No dedicated run-trace wire method exists. `git grep -n "IngestRunTrace"` finds nothing, and `RunTrace` appears in the server only as a node label (`src/server/persistence/provenance_anchor.rs:55`). A client therefore writes a `RunTrace` node through `AddNode`. The only new server code is the shape check in R009.15. The code read for this plan did not show SHACL validation on `AddNode`, so R009.15 states the check as new work.

A shape violation returns a typed refusal that names the shape and stores nothing. A measured fact without an evaluation-run evidence record returns `MeasuredFactWithoutEvidence`. A missing measured value never produces a guess; the `Decide` abstention belongs to the `EG-TYPED-PACKS` rows for the measured model facts.

### Reuse and new work

Reused: `:RunTrace`, `:tokenCost`, `:evaluationScore`, `:ModelProfile`, `:contextWindow`, the shapes companion, `AddNode`, `ApplyChangeEnvelopes`, `EvidenceRecord` (`change_envelope.rs:158`) and the Cypher executor. New: eleven properties, one class pair (`:EvaluationRun`, `:MeasuredModelFact`), four shapes, the `AddNode` shape check, the envelope builder, and six queries.

### Queries

Each query groups by `:runModel` and `:runTaskClass`. A solved trace has `final_answer` and `:evaluationScore` 1.0. Success rate and cost per solved task use solved traces. Overflow rate counts `context_overflow`. The question asks which harness configuration has lowest cost per solved task for model M on task class C. `harness_best_config` (R010.6) answers. It returns trace IDs as evidence. It abstains below 20 traces per configuration. That default is a named starting value; measurement may replace it.

### Decisions

- Reuse `ApplyChangeEnvelopes` for the governed write. Rejected: `GraphSchemaOp::AttachApproved` (`crates/eg-types/src/graph_schema.rs:79`), because it attaches schema sources, not facts.
- Keep `:costUsd` beside `:tokenCost`. Rejected: reusing `:tokenCost`, because tokens and dollars differ in unit.
- Supersede by edge, not by deletion. Rejected: overwrite, because it loses the evidence of the earlier measurement.
- Write traces through `AddNode`. Reject a new `IngestRunTrace` wire method. The brief forbids a second store. The contract spec also bans new wire methods here.

### Cross-repository rows

Depends on the `EG-DECISION-ENGINE` row for `HarnessConfig` (the committed decision that `:appliedHarnessConfig` links) and the `EG-TYPED-PACKS` rows for the measured model facts and `shellCentric`. Unblocks the `AU-HARNESS` row that writes the run trace and the `AU-HARNESS` evaluation runner row that writes the measured facts.

## Spec-folder contents as typed nodes (R012)

Data flow: `IndexRepository` wire method (`crates/eg-types/src/protocol/method/method_03.rs:317`) reaches the source-ingest handler (`src/server/dispatch/router/source_ingest.rs:24`). The handler calls new `classify_spec_file` (`crates/eg-core/src/spec_folder.rs`). It writes typed nodes. Cypher then shows task, goal, document, memory, and policy nodes.

Types: `SpecNodeKind`, `SpecFolderRefusal::UnknownKind`, and the property `:classifiedByRule`. The ontology file `crates/eg-core/ontology/sdd-v1.ttl` already declares `:Specification`, `:Requirement` and `:ImplementationPlan` (`sdd-v1.ttl:201-216`); only four classes are new.

Attach point: the non-code branch of the `IndexRepository` handler, called by the dispatch router. Today that branch stores plain documents. A file that matches no rule keeps that behavior and returns the typed refusal.

Failure behavior: an unclassifiable file never aborts the batch. The batch outcome lists the refusal per file, as the typed per-file outcomes in `crates/eg-types/src/ingestion_wire.rs:36` already do.

Reused: the `IndexRepository` batch, per-file outcomes, and the software ontology. New: one module, four classes and one property.

Decision: classify by path and front matter in the engine. Rejected: classification in each client, because clients would drift and the graph would hold different kinds for the same file.

Cross-repository rows: the `AU-INGEST` rows that send a spec folder to `IndexRepository` depend on this contract name `SpecNodeKind`.

## EG-ORCH-VOCAB-R013 Run trace step record with request, argument and result digests

Data flow: run write -> `Method::AddNode` (`crates/eg-types/src/protocol/method/method_00.rs:6`) -> `AddNode` arm (`src/server/handlers/graph_ops/gateway_graph.rs:62`) -> digest validation -> ordered `StepRecord` nodes -> replay read.
Types: `StepRecord`, `StepKind`, the `ModelRequest` receipt kind, `step_digest`.
Attach point: The `AddNode` arm at `src/server/handlers/graph_ops/gateway_graph.rs:62`, after the shape check of `EG-ORCH-VOCAB-R009`. The comparison loop stays in the harness.
Failure: A bad digest returns `STEP_DIGEST_INVALID`. A gap in the index returns `STEP_ORDER_INVALID`.
Consumers: AU-HARNESS (R024)
Moves from agent-utilities: agent_utilities/observability/replay_engine.py:81 (span digest recipe)
Existing code: `crates/eg-types/src/outcome_bundle.rs:130`; `crates/eg-types/src/outcome_bundle.rs:668`; `crates/eg-types/src/authority/replay.rs:191` (a different record). Reachable today: no.
Existing-code search: the engine stores run trace nodes and one `RunEvent` digest. It holds no per-step digests and no replay compare. Search terms: `ModelRequest`, `request_digest`, `args_digest`, `result_digest`, `RunTrace`.
Reuses: `ReceiptNodeKind` and the `AddNode` path. New: the step record and the digest recipe.
Decision: The engine owns the record shape and digest recipe so any replay agrees with the store. Rejected alternative: each harness defines its own digest, because a replay never matches a record written by another version.
Cross-repository: `AU-HARNESS-R024` writes the steps and runs the replay compare. Its replay code at `replay_engine.py:81` keeps the loop and takes the digest recipe from here.

References
- No paper is cited.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-vocabulary -->
## Reviewed residual closure plan — owner batch 2026-10-12

### `EG-ORCH-VOCAB-R009.18` — AU:26

- Existing EG parent/rollup: `EG-ORCH-VOCAB-R009`; preserve its existing obligations.
- Original objective: Platform monitors latency, token cost, and error frequency per run. It adjusts model selection and parallelism within declared budgets. It records each adjustment on the trace.
- Original intake source record: AU:26. Its exact source array and SHA-256 remain in the canonical R009.18 requirement.
- Contract definition: R009.18 in `requirements.md` specifies the complete revisioned observation window, fields, validation, joint decision, trace, and capacity behavior. This plan preserves that contract without restating its long field list.
- Production wiring and owner boundary: Feed the proposed AU RunAdjustmentObservation from the existing routing and continuation paths to Method::Decide. Reuse R136 model adequacy/stage allocation and the existing RunTrace ingestion/readback under R009.17. Persist both adjustments and abstentions before application; GraphOS remains capacity authority.
- Planned acceptance: `run_observations_change_model_and_width_with_trace` exercises AU routing/continuation and served EG Decide. It compares controlled latency, cost, and error windows under identical budgets. Full positive, refusal, trace, and capacity cases remain in `requirements.md` and `test-spec.md`.
- Preserve existing coverage: `AU-CONTROL-R006`, `AU-CONTROL-R019`, `AU-HARNESS-R007`, `EG-ORCH-VOCAB-R009.8`, `EG-ORCH-VOCAB-R009.9`, `GRAPHOS-CAPACITY-R001.1`.
- Source record: AU:26 SHA-256 and exact owner references are retained in `requirements.md`.
- Explicit owner holds: AU-CONTROL-R006.30 is unmerged. Policy thresholds, predictive cost inputs, and tie breaks remain open until the owner publishes a versioned policy. No tuning defaults or implementation claims are supplied.
- Unmerged consumer dependencies: AU-CONTROL-R006.30 (draft/unmerged).
- Proposed owner-review choices, not source-established facts: The joint carrier and policy-required fields make missing decisions explicit. A self-referential command to adjust appropriately is not a substitute for that published policy..
- Keep status SPECIFIED with empty `landed_in`/`verified_by` until separately reviewed implementation and served evidence exist; no receipt is inferred.

### `EG-ORCH-VOCAB-R013.7` — AU:115

- Existing EG operations: `Method::UnifiedQuery` reads revision-pinned repository relationships. `Method::AddNode` stores ordered `RunTrace` records. UQL and the content-addressed artifact read path independently confirm stored refs. No new graph writer or EG model executor is introduced.
- EG output schema `CodeWalkthroughV1`: bind run, repository, revision, graph snapshot, query digest, and ordered section manifest. Source refs include node/revision/digest/path/line range. The schema also includes interaction edges, artifact ref/digest, and ordered trace-step/span note refs. AddNode refuses stale/cross-tenant refs, uncited sections, invalid artifact refs, invalid span/run links, or non-contiguous notes before storage.
- EG test targets: `crates/eg-types/src/outcome_bundle.rs::tests::code_walkthrough_manifest_round_trips`; `src/server/handlers/graph_ops/gateway_graph.rs::tests::add_run_trace_rejects_stale_walkthrough_source_or_artifact`; served `tests/orchestration/test_code_walkthrough_served.py::test_walkthrough_trace_and_artifact_read_back`.
- Depends on `EG-REPO-INGEST-R009` and `AU-HARNESS-R024`. AU still needs an exact producer schema/receipt row; WebUI still needs an exact artifact-consumer/interaction row. No matching owner IDs were found; coordinator assignment and served receipts remain a closure hold.
- Original source AU:115 SHA-256 `445704f78ae07d1c4b62f42dd23797823d91bc470f04a9dc779e3f6777fffbf0`. Status remains SPECIFIED.

### `EG-ORCH-VOCAB-R003.8` — EG:12

- Reuse R135 `CompiledProgram`/`lookup_program`, `AgentLibraryStore`, R003.6 outbox projection and `graph.assemble()`; successful terminal trace is required for promotion.
- Canonical signature binds task class, sorted capability IRIs, input/output schema digests, and topology digest. Similarity uses a caller-pinned policy with explicit weights and minimum score. Capability overlap uses Jaccard. Other features name deterministic score functions and source revisions. Candidates need compatible signatures, schemas, and topology before scoring. Missing/stale inputs exclude candidates. Stable program IRI breaks ties.
- Project typed handler metadata with ID/version, input/output schema digests, and effect set. `graph.assemble()` uses it as compatibility/effect constraints. The R036 solver still runs. EG does not invoke handlers.
- Wire through `program_store.rs`, `stat_decide.rs`, `candidates.rs`, `assemble.rs`, `persistence/agent_library.rs`, and R003.6 projection. Test type round trip and stale/incompatible-handler refusal. Test served promotion and similar-task reuse at the exact requirement names.
- Depends on `EG-DECISION-ENGINE-R135.2`, `.4`, `R036`, `EG-TYPED-PACKS-R006`, and `EG-ORCH-VOCAB-R003.5/.6`. Original EG:12 SHA-256 `517823a931bb93e173b301a9a31fbe6bd75c14687233c72278a59ced1940673a`. Status remains SPECIFIED.

### `EG-ORCH-VOCAB-R012.7` — EG:46

- Existing EG parent/rollup: `EG-ORCH-VOCAB-R012`; preserve its existing obligations.
- Original objective: Repository constitution and policy files become graph policy nodes for reasoning. A policy that contradicts a stored invariant is refused.
- Contract definition: `requirements.md` R012.7 defines source bindings and typed GuardrailRule candidates. It also defines versioned grammar, inactive ambiguity, review/invariant checks, atomic activation, and reasoning evidence.
- Production wiring: Extend the existing source_ingest router and SourceIngest path after `classify_spec_file`. Reuse GuardrailRule storage and `check_rule_against_invariants`. Link activation to the R012.7 decision-policy path under EG-IDENTITY-R008.4. R129.5 alone does not establish repository-policy guarding.
- Planned acceptance: `repository_policy_is_synthesized_guarded_and_used` exercises real IndexRepository ingestion, exact-digest owner review, activation, and served decision use. Full conflict, stale, unsupported, and race cases remain in `requirements.md` and `test-spec.md`.
- Preserve existing coverage: `EG-ORCH-VOCAB-R012.1`, `EG-ORCH-VOCAB-R012.2`, `EG-ORCH-VOCAB-R012.3`, `EG-ORCH-VOCAB-R012.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`, `EG-DECISION-ENGINE-R129.3`, `EG-DECISION-ENGINE-R129.4`, `EG-DECISION-ENGINE-R129.5`, `EG-IDENTITY-R008.4`.
- Source record: EG:46 SHA-256 `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`. Owner reference IDs and paths are in the canonical R012.7 requirement.
- Explicit owner holds: The supported declarative grammar/synthesis profile and concrete review receipt producer must be published and bound. Unstructured text synthesis may produce candidates, but cannot silently fill those authority gaps.
- Unmerged consumer dependencies: none.
- Proposed owner-review choice, not source-established fact: Atomic publication covers the active graph policy set only. It does not imply atomic cross-repository filesystem effects.
- Keep status SPECIFIED with empty `landed_in`/`verified_by` until separately reviewed implementation and served evidence exist; no receipt is inferred.

### `EG-ORCH-VOCAB-R004.6` — EG:51

- Existing EG parent/rollup: `EG-ORCH-VOCAB-R004`; preserve its existing obligations.
- Original objective: The capability registry models functional gaps and desired enhancements as typed domain-taxonomy nodes. Queries filter them by task class.
- Contract definition: `requirements.md` R004.6 defines distinct CapabilityGap and DesiredEnhancement records. It binds tenant, revision, capability, task class, domain, source evidence, owner, and disposition.
- Production wiring and owner boundary: Extend the existing orchestration ontology/shapes and governed AddNode/ChangeEnvelope path. Reuse the existing task-class/domain taxonomy and Method::UnifiedQuery rather than introducing a second capability registry. Keep the EG:7 owner-export manifest question separate from these persisted domain nodes.
- Planned acceptance: `capability_gap_and_enhancement_are_queryable` uses served mutation and generated UnifiedQuery. It tests task classes, restart/readback, deduplication, revision conflict, invalid taxonomy, matching modes, tenant isolation, and unchanged capability state.
- Preserve existing coverage: `EG-ORCH-VOCAB-R004`, `EG-DECISION-ENGINE-R010`.
- Source record: EG:51 SHA-256 `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`. Owner reference IDs and paths are in the canonical R004.6 requirement.
- Explicit owner holds: The authoritative domain taxonomy/task-class resolver and admitted mutation producer must be bound to existing owner contracts. Do not fabricate a new owner role-export ID to cover the unrelated EG:7 hold.
- Unmerged consumer dependencies: none.
- Proposed owner-review choices, not source-established facts: Distinct gap/enhancement types and explicit query match mode are proposed schema additions..
- Keep status SPECIFIED with empty `landed_in`/`verified_by` until separately reviewed implementation and served evidence exist; no receipt is inferred.

### `EG-ORCH-VOCAB-R012.8` — EG:61

- Existing EG parent/rollup: `EG-ORCH-VOCAB-R012`; preserve its existing obligations.
- Original objective: Epistemic state and active policy live in the graph. Normative rules and constitutions live in versioned files. Graph writes never overwrite those files. Normative changes require review.
- Contract definition: `requirements.md` R012.8 defines the normative-source binding, review digest, generation/revision checks, governed writeback, reread, and activation conditions.
- Production wiring and owner boundary: Link the R012.2 policy classification and EG-IDENTITY-R008.4 active graph policy to R012.7 activation. Bind review/approval evidence to GRAPHOS-OPS-R050.2/.4 and the concrete SDK governed source effect. EG has no normative filesystem writer.
- Planned acceptance: `reviewed_normative_source_activates_without_graph_overwrite` exercises served ingestion, owner review, activation, and decision use. It confirms unchanged source bytes after graph operations and refusal without reviewed governed writeback. Full mismatch, stale, and race cases remain in `requirements.md` and `test-spec.md`.
- Preserve existing coverage: `EG-IDENTITY-R008.4`, `EG-ORCH-VOCAB-R012.2`, `GRAPHOS-OPS-R050.2`, `GRAPHOS-OPS-R050.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`.
- Source record: EG:61 SHA-256 `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`. Owner reference IDs and paths are in the canonical R012.8 requirement.
- Explicit owner holds: The concrete producer of the review receipt and normative-file source-effect binding must be owner-confirmed. R050 references establish review/ops boundaries, not proof that the proposed full carrier already exists.
- Unmerged consumer dependencies: none.
- Proposed owner-review choice, not source-established fact: Reviewed files remain normative source. EG owns active policy, not file-only runtime authority.
- Keep status SPECIFIED with empty `landed_in`/`verified_by` until separately reviewed implementation and served evidence exist; no receipt is inferred.

<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition implementation plan — 2026-10-12

### `EG-ORCH-VOCAB-R005.100` / AU:33

- Extend existing parent `R005` without removing its existing behavior.
- Specify: R005.100 extends the orchestration step contract. It binds nested input schema, source-slot/path mappings, declared projection, shield policy, and payload digest before AU dispatch. The AU consumer applies projection before tool dispatch. DTO extra-field rejection is not projection.
- Producer/consumer boundary: EG declares typed step projection/shield policy; AU `_dispatch_parallel_batch`/processor applies projection before tool dispatch. DTO extra-field rejection is not data projection.
- Source record SHA-256: `1a32741b0a97f0c31cd1a3df4ea31d6eee0069c62c3487c0d8057b1ced4b6cf4`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.

### `EG-ORCH-VOCAB-R009.100` / AU:60

- Extend existing parent `R009` without removing its existing behavior.
- Specify: R009.100 defines trace/run, span/child-agent, and tool-call IDs. It binds time range, source revision, and redaction profile. Child spans correlate to the parent run. Consumers receive complete paginated snapshots, completeness metadata, and stable digest. AU exports spans. GraphOS owns MCP transport. EG exposes correlated readback only.
- Producer/consumer boundary: AU default_runtime_capabilities/run_agent exports. GraphOS MCP transports the export. EG offers correlated readback. Preserve R008/R024 trace ownership.
- Source record SHA-256: `5d53cbdda932456b27c6649360c3051d4ab09f2db42018550b4ee81c785748b4`.
- Keep status SPECIFIED until the EG served producer and each separately owned consumer are implemented, served-tested and independently read back.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-plan -->
## Cross-owner producer definition plan — 2026-10-12

- **`EG-ORCH-VOCAB-R009.101` / WEBUI:3:** Add `IndexHarnessSessionBatchV1` and `SearchHarnessSessionsV1` over durable RunTrace/AddNode records and existing UQL, spanning every adapter in the pinned supported-harness manifest. Bind tenant, manifest, SDK sync receipt, session and indexed/source revisions; return source coverage, watermark and snapshot cursor. Optional cost summaries reuse R010.101 accounting profile/unit/unknown semantics. Replay is idempotent; conflicting content, stale cursor and cross-tenant reads refuse. SDK source sync and the GraphOS query-adapter requirement ID remain pending owner dependencies. Source SHA-256 `38936b5f2136263c40c29fe639c984243fed8be6dbb9a6d836f40d7b7672116e`.
- **`EG-ORCH-VOCAB-R010.100` / AU:103:** Add `EvolutionTraceMetricsV1` with revisioned proposal/prompt/progress records, ordered RunTrace refs, revisioned metrics, observation window and explicit coverage. Missing known traces are partial/unknown, never zero. Existing ChangeEnvelope/AddNode writes traces; UQL independently reads them; GraphOS `runs.get`/`runs.metrics` serves the view, with exact adapter ID pending. AU progress producer ID/schema remains pending. Source SHA-256 `68bae14976bd3581bc33ee65b153b5ad60f807fb894af09577b9debd19be7557`.
- **`EG-ORCH-VOCAB-R010.101` / WEBUI:4:** Extend RunTrace with project/agent IDs and four disjoint token counters. `RunTraceUsageQueryV1` returns grouped totals/costs with accounting, price, currency revisions, watermark, and unknown semantics. Aggregate exact rational rates. Round once per output group/category using pinned currency exponent and half-even. Preserve p95 ≤500 ms for the 1,000,000-row/32-reader dataset through GraphOS `handle_runs_metrics`. Preserve cost-per-solved-task semantics.
- Keep all three requirements SPECIFIED until EG operations, GraphOS adapters, and WebUI consumers pass contract checks. Require independent readbacks for each owner.
<!-- /spec-definition-patch -->
