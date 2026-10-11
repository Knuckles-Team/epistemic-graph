# EG-CQ-ORCH-001 design

## Layer model

| Layer | Contents | Read by |
|---|---|---|
| L0 | Sources and ontologies | Q1, Q4, Q8 |
| L1 | Components: tools, skills, prompts, model profiles | Q3, Q4, Q5, Q8 |
| L2 | Agents and the Agent Library | Q2, Q3, Q5, Q8 |
| L3 | Agent graphs | Q1, Q2, Q6, Q7, Q8 |
| L4 | Harness port | Q6, Q8 |
| L5 | Runs, outcomes and feedback | Q1, Q2, Q6, Q8 |

Composition is the engine's decision functionality: `graph.assemble()` (`EG-DECISION-ENGINE-R036`) lowers typed tasks and capabilities to a bounded 0-1 program (R014) and returns a certified assembly or a typed abstention. Q2, Q3, Q4, Q7 reuse its sibling queries (R046/R111 topology, R126 coverage, R127 guardrails). Cross-source answers use federated query and foreign sources; the Agent Library comes from typed packs.

## Q1. What steps do I need to follow to complete task XYZ?

- **Layers read:** L0 (task, process and standard ontologies), L3 (stored agent graphs and workflows), L5 (past runs and outcomes)
- **Question:** "What steps do I need to follow to complete task XYZ?"
- **Decomposition:**
  - Which task class is XYZ (native task IRI, or typed abstention if unknown)?
  - Which subtask decomposition does the class entail?
  - In what partial order do the steps run?
  - Which past runs of the same class succeeded (L5 evidence)?
- **Ontology terms:** found: swarm:DecomposableTask, swarm:TaskShape (swarm_topology-v1.ttl); :Goal, :Workflow, :realizesProcess, :childWorkItemOf (orchestration-v1.ttl); :ImplementationPlan, :planForFeature (sdd-v1.ttl). MISSING: ordered-step class (TaskStep), :hasStep, :precedes (step order).
- **Sources:** Process models (ARIS/ArchiMate/process-intelligence mirrors), SDD plans, and run history from L5; each declared an authority for :realizesProcess / :ImplementationPlan through the source-authority mapping (MISSING statement, see child .3).
- **Mechanism:** EG-DECISION-ENGINE-R036 (graph.assemble composes tasks), R003 (is_a derivation records), EG-FEDERATED-QUERY-R006 (proof-carrying results).
- **Query sketch:** `SELECT ?step ?order ?src WHERE { ?task a swarm:DecomposableTask ; :childWorkItemOf* ?root . ?step :childWorkItemOf ?task . OPTIONAL { ?step eg:precedes ?next } # MISSING eg:precedes }`
- **Answer shape:** StepsAnswer{task_iri, steps[{step_iri, label, precedes[], provenance{source, derivation_path, evidence_class}}], resolution_kind}; abstention AbstainReason::UnknownTaskClass.

## Q2. How many agents and what type of agents are needed to complete the task?

- **Layers read:** L3 (topology slots), L2 (agent kinds), L5 (observed width/outcomes)
- **Question:** "How many agents and what type of agents are needed to complete the task?"
- **Decomposition:**
  - How many slots does the chosen topology template have (min/max width)?
  - What role does each slot play (supervisor, worker, verifier, aggregator)?
  - What agent kind fills each role?
- **Ontology terms:** found: swarm:Topology, swarm:TopologyTemplate, swarm:Slot, swarm:SlotRole, swarm:hasSlot, swarm:role, swarm:minWidth, swarm:maxWidth, swarm:nodeKind, swarm:Aggregator, swarm:Verifier (swarm_topology-v1.ttl); :Agent, :Team, :memberOfTeam, :intelligenceLevel (orchestration-v1.ttl). MISSING: agent-kind taxonomy (AgentKind), slot-to-agent-kind link.
- **Sources:** Agent Library catalog projection (L2) and swarm topology facts; no external source needed.
- **Mechanism:** EG-DECISION-ENGINE-R046 and R111 (topology decision), R014 (composition as bounded 0-1 optimization), R036.
- **Query sketch:** `SELECT ?slot ?role ?kind WHERE { ?t a swarm:TopologyTemplate ; swarm:hasSlot ?slot . ?slot swarm:role ?role . OPTIONAL { ?slot eg:fillsAgentKind ?kind } # MISSING eg:fillsAgentKind }`
- **Answer shape:** StaffingAnswer{topology_iri, agent_count{min,max,chosen}, slots[{slot, role, agent_kind}], certificate_digest, provenance}; abstention AbstainReason::NoLegalTopology.

## Q3. Can we use an agent template we already have, or an external A2A agent we have added to our Agent Library, based on their capabilities?

- **Layers read:** L2 (agents, templates, A2A cards in the Agent Library), L1 (capabilities of components)
- **Question:** "Can we use an agent template we already have, or an external A2A agent we have added to our Agent Library, based on their capabilities?"
- **Decomposition:**
  - Which capabilities does each subtask require?
  - Which Agent Library templates provide them?
  - Which registered A2A cards provide them?
  - Which gaps remain uncovered?
- **Ontology terms:** found: :Agent, :A2AAgentCard, :exposesSkill, :registeredIn, :ResourceRegistry, :a2aDelegatesTo (a2a-v1.ttl); :requiresCapability, :providesCapability, :canServe, :swappableWith (capability-v1.ttl). MISSING: AgentTemplate class, AgentLibrary class (catalog-v1.ttl declares no terms; library is storage per EG-TYPED-PACKS).
- **Sources:** Agent Library catalog (EG-TYPED-PACKS) for templates; registered A2A cards for external agents; each is the authority for :providesCapability on its own components.
- **Mechanism:** EG-DECISION-ENGINE-R126 and children (capability coverage over Agent Library candidates and A2A cards), R044 (A2A routing), EG-TYPED-PACKS-R013 (capability IRI validation), R006 (pack projection).
- **Query sketch:** `SELECT ?cap ?cand ?kind WHERE { ?task :requiresCapability ?cap . ?cand :providesCapability ?cap . OPTIONAL { ?cand a eg:AgentTemplate } # MISSING OPTIONAL { ?cand a :A2AAgentCard } }`
- **Answer shape:** CoverageAnswer{per_capability[{capability, candidates[{id, kind: template|a2a, evidence_class}]}], uncovered[], provenance}; typed abstention names the missing capability (R010).

## Q4. What standards, best practices, or guardrails should I follow?

- **Layers read:** L0 (standards, shapes, policies), L1 (component-level constraints)
- **Question:** "What standards, best practices, or guardrails should I follow?"
- **Decomposition:**
  - Which task class governs?
  - Which standards and rules are entailed for that class?
  - Which SHACL shapes enforce them?
- **Ontology terms:** found: :DevelopmentStandard, :mustFollow, :mustFollowChain, :SecureTask (capability-v1.ttl); :ComplianceConstraint, :constrainedBy, :DesignGuideline, :guidelineFor (sdd-v1.ttl); :PolicyShape, :CapabilityShape, :ToolShape (governance-core-v1.shapes.ttl); swarm:StopRule. MISSING: BestPractice class, appliesToTaskClass on a rule.
- **Sources:** Governance and standards sources (Egeria glossary/governance, CISO assistant compliance, repository standards); each is the authority for :DevelopmentStandard.
- **Mechanism:** EG-DECISION-ENGINE-R127 and children (guardrail entailment with entailment path and source generation), EG-FEDERATED-QUERY-R005 (VALIDATE SHAPE), R006.
- **Query sketch:** `SELECT ?rule ?path WHERE { ?task :mustFollow ?rule } # entailed via :mustFollowChain; eg:appliesToTaskClass MISSING`
- **Answer shape:** GuardrailAnswer{task_iri, rules[{rule_id, kind: standard|best_practice|guardrail, entailment_path, source_generation, shape_iri}], provenance}; empty rule set is a typed answer, not an error.

## Q5. What skills, tools, prompts, and LLM models does each agent need to complete each subtask?

- **Layers read:** L1 (skills, tools, prompts, model profiles), L2 (which agent holds them)
- **Question:** "What skills, tools, prompts, and LLM models does each agent need to complete each subtask?"
- **Decomposition:**
  - Which agent fills each subtask?
  - Which skills and tools provide the subtask capability?
  - Which prompt applies?
  - Which model profile satisfies context and cost limits?
- **Ontology terms:** found: :Skill, :Tool, :Prompt, :Model, :hasSkill, :hasPrompt, :usesTool, :usesModel, :orchestrationUsesTool, :contextWindow, :systemPrompt (orchestration-v1.ttl); :providesCapability (capability-v1.ttl); :assignedAgent (sdd-v1.ttl). MISSING: ModelProfile class, subtask-needs-skill property, per-subtask assignment node.
- **Sources:** Component catalog (EG-TYPED-PACKS): skills, tools, prompts imported as packs; model profile facts from annotations (EG-TYPED-PACKS-R006 typed annotations).
- **Mechanism:** EG-DECISION-ENGINE-R036 (assembly binds components), R126 (coverage), R014 (knapsack/cardinality constraints), R002 (evidence class).
- **Query sketch:** `SELECT ?agent ?sub ?skill ?tool ?prompt ?model WHERE { ?agent :hasSkill ?skill ; :usesTool ?tool ; :hasPrompt ?prompt ; :usesModel ?model . ?sub :assignedAgent ?agent }`
- **Answer shape:** BindingAnswer{rows[{subtask, agent, skills[], tools[], prompts[], model_profile, provenance}], gaps[]}; abstention names the unbound slot.

## Q6. What workflow do the agents need to follow?

- **Layers read:** L3 (workflows and agent graphs), L4 (harness hooks and extension points), L5 (past runs)
- **Question:** "What workflow do the agents need to follow?"
- **Decomposition:**
  - Which workflow realizes the task class?
  - What hand-offs and control-flow does it declare?
  - Which harness hooks govern execution?
- **Ontology terms:** found: :Workflow, :composes, :orchestrates, :delegatesTo, :SkillWorkflowProposal, :DurableRun, :SagaCoordination, :StatechartInstance (orchestration-v1.ttl); :Harness, :HarnessHook, :atHook, :hookOrder (harness-v1.ttl). MISSING: handoff condition property, workflow-step ordering (shared with Q1).
- **Sources:** Stored workflows (L3) and harness definitions (L4); run history (L5) as supporting evidence.
- **Mechanism:** EG-DECISION-ENGINE-R036 (assembled graph carries workflow), R044 (routing to an existing graph), plus the durable-run kernel already in orchestration-v1.
- **Query sketch:** `SELECT ?wf ?member ?next WHERE { ?wf a :Workflow ; :composes ?member . OPTIONAL { ?member eg:hands_off_to ?next } # MISSING }`
- **Answer shape:** WorkflowAnswer{workflow_iri, steps[], handoffs[], hooks[], provenance}; abstention AbstainReason::NoWorkflowForClass.

## Q7. What does the agent graph shape (DAG) look like?

- **Layers read:** L3 (agent graphs and topology templates)
- **Question:** "What does the agent graph shape (DAG) look like?"
- **Decomposition:**
  - Which topology kind applies?
  - Which nodes and slots exist?
  - Which edges connect them?
  - Is the graph acyclic?
- **Ontology terms:** found: swarm:Topology and subclasses (Single, Pipeline, FanOutJoin, SupervisorWorkers, PeerTeam, Hierarchy), swarm:TopologyTemplate, swarm:Slot, swarm:hasSlot, swarm:nodeId, swarm:nodeKind (swarm_topology-v1.ttl); swarm_topology-v1.shapes.ttl TemplateTopologyShape. MISSING: slot-to-slot edge property, acyclicity SHACL shape.
- **Sources:** Swarm topology facts and assembled graphs persisted in L3; no external source.
- **Mechanism:** EG-DECISION-ENGINE-R046, R111 and R111.1 (topology as typed Decide question), R036.
- **Query sketch:** `SELECT ?a ?b WHERE { ?t swarm:hasSlot ?a, ?b . ?a eg:feeds ?b } # eg:feeds MISSING`
- **Answer shape:** DagAnswer{topology_kind, nodes[], edges[], acyclic: bool, serialization (adjacency list), provenance}.

## Q8. How can I execute task XYZ? (composed answer)

- **Layers read:** L0 through L5 (all layers, composed)
- **Question:** "How can I execute task XYZ? (composed answer)"
- **Decomposition:**
  - Are all seven part-answers present?
  - Is each part carrying its own provenance?
  - Is a missing part a typed abstention instead of an omitted field?
- **Ontology terms:** found: :derivedFrom (capability-v1.ttl, orchestration-v1.ttl), :RunTrace, :producedBy (orchestration-v1.ttl). MISSING: none new; the composed answer is a wire DTO, not an ontology class.
- **Sources:** All sources feeding rollups 1-7.
- **Mechanism:** EG-DECISION-ENGINE-R036 graph.assemble() / AgentAssemble is the sole composition path. This spec adds no planner; it adds an answer envelope over assemble() output and the coverage and guardrail queries (R126, R127).
- **Query sketch:** `n/a: the envelope is built by the assemble handler from the seven part queries.`
- **Answer shape:** ExecutionAnswer{task_iri, parts{steps, staffing, coverage, guardrails, bindings, workflow, dag}, each part{answer|abstain, provenance, evidence_class}, assembled_by: assemble, certificate_digest}.

## Q9. Probe questions through the intent surface

- **Layers read:** the graph-os intent tools front L0-L5; each probe lands on the rollup above it (node counts: L0; Agent Library: L2; tool discovery: L1; delegate: L3-L5).
- **Mechanism:** graph-os registry serves the intent tools; EG supplies the answer and provenance. Owner: graph-os (cross-repo).
- **Answer shape:** non-empty answer, provenance field present, correct against a fixture; typed abstention only when the source is genuinely absent.
- **Probes and previously observed results:**
  - "How many nodes does the graph hold, by label?": previously correct SQL, empty answer.
  - "How can I deploy a container with container-manager?": previously 1-step plan.
  - "Which agents are in the Agent Library?": previously routed.
  - "Why does graph-os use the intent surface?": previously pending.
  - "Find a tool that lists Docker containers.": previously pending.
  - "Delegate: list running containers.": previously pending.
