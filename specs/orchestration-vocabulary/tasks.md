# EG-ORCH-VOCAB-001 — Implementation tasks

Status: PROPOSED. Governing spec: [spec.md](spec.md). Design: [design.md](design.md).

Producer first: terms, then shapes, then projection call sites, then query tests. Rows within a rollup are independent 5-minute units unless the Verification names another.

- [ ] EG-ORCH-VOCAB-R001 Task steps (rollup)
- [ ] EG-ORCH-VOCAB-R001.1 Add class `:TaskStep`
- [ ] EG-ORCH-VOCAB-R001.2 Add property `:hasStep`
- [ ] EG-ORCH-VOCAB-R001.3 Add step-order acyclicity shape
- [ ] EG-ORCH-VOCAB-R001.4 Add Q1 fixture and query test
- [ ] EG-ORCH-VOCAB-R002 Agent kinds and staffing (rollup)
- [ ] EG-ORCH-VOCAB-R002.1 Add class `:AgentKind` and seed members
- [ ] EG-ORCH-VOCAB-R002.2 Add property `:hasAgentKind`
- [ ] EG-ORCH-VOCAB-R002.3 Add property `swarm:requiresAgentKind` and the import
- [ ] EG-ORCH-VOCAB-R002.4 Add slot-kind shape
- [ ] EG-ORCH-VOCAB-R002.5 Add Q2 query test
- [ ] EG-ORCH-VOCAB-R003 Agent Library, templates and A2A members (rollup)
- [ ] EG-ORCH-VOCAB-R003.1 Add class `:AgentTemplate`
- [ ] EG-ORCH-VOCAB-R003.2 Add class `:AgentLibrary`
- [ ] EG-ORCH-VOCAB-R003.3 Add property `:memberOfLibrary`
- [ ] EG-ORCH-VOCAB-R003.4 Add property `:instanceOfTemplate`
- [ ] EG-ORCH-VOCAB-R003.5 Add library-member shape
- [ ] EG-ORCH-VOCAB-R003.6 Wire the Agent Library projection to emit membership
- [ ] EG-ORCH-VOCAB-R003.7 Add Q3 query test
- [ ] EG-ORCH-VOCAB-R004 Best practices and guardrail scope (rollup)
- [ ] EG-ORCH-VOCAB-R004.1 Add class `:BestPractice`
- [ ] EG-ORCH-VOCAB-R004.2 Add property `:appliesToTaskClass`
- [ ] EG-ORCH-VOCAB-R004.3 Add rule-scope shape
- [ ] EG-ORCH-VOCAB-R004.4 Project `GuardrailRule` components
- [ ] EG-ORCH-VOCAB-R004.5 Add Q4 query test
- [ ] EG-ORCH-VOCAB-R005 Per-subtask bindings (rollup)
- [ ] EG-ORCH-VOCAB-R005.1 Add class `:ModelProfile`
- [ ] EG-ORCH-VOCAB-R005.2 Add property `:needsSkill`
- [ ] EG-ORCH-VOCAB-R005.3 Add property `:stepAssignedAgent`
- [ ] EG-ORCH-VOCAB-R005.4 Add binding shape
- [ ] EG-ORCH-VOCAB-R005.5 Project `model_profile` components
- [ ] EG-ORCH-VOCAB-R005.6 Add Q5 query test
- [ ] EG-ORCH-VOCAB-R006 Hand-off and agent-graph DAG (rollup)
- [ ] EG-ORCH-VOCAB-R006.1 Add property `:handsOffTo`
- [ ] EG-ORCH-VOCAB-R006.2 Add property `swarm:feeds`
- [ ] EG-ORCH-VOCAB-R006.3 Add property `swarm:loopsBackTo`
- [ ] EG-ORCH-VOCAB-R006.4 Add DAG acyclicity shape
- [ ] EG-ORCH-VOCAB-R006.5 Project template topology facts to `swarm:feeds`
- [ ] EG-ORCH-VOCAB-R006.6 Add Q6 and Q7 query tests
- [ ] EG-ORCH-VOCAB-R007 Assembly consumption (rollup)
- [ ] EG-ORCH-VOCAB-R007.1 Expand a task's steps into assembly tasks
- [ ] EG-ORCH-VOCAB-R007.2 Project the assembled graph back
- [ ] EG-ORCH-VOCAB-R007.3 Add the composed read
- [ ] EG-ORCH-VOCAB-R007.4 Enforce slot agent-kind constraints before assembly selection
- [ ] EG-ORCH-VOCAB-R008 Catalog registration and ontology gates (rollup)
- [ ] EG-ORCH-VOCAB-R008.1 Register `orchestration-v1.shapes.ttl` with the shape loader
- [ ] EG-ORCH-VOCAB-R008.2 Regenerate ontology artifacts
