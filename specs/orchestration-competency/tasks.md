# EG-CQ-ORCH — Implementation tasks

Governing spec: [spec.md](spec.md). Design: [design.md](plan.md).

Producer first: ontology gaps, shapes and authority mappings before fixtures and acceptance tests. R009 children are owned by graph-os.

- [ ] EG-CQ-ORCH-R001.1 Q1 Ontology gap: ordered step
- [ ] EG-CQ-ORCH-R001.2 Q1 SHACL shape
- [ ] EG-CQ-ORCH-R001.3 Q1 Source-authority mapping
- [ ] EG-CQ-ORCH-R001.4 Q1 Query fixture
- [ ] EG-CQ-ORCH-R001.5 Q1 Acceptance test
- [ ] EG-CQ-ORCH-R002.1 Q2 Ontology gap: agent kind
- [ ] EG-CQ-ORCH-R002.2 Q2 SHACL shape
- [ ] EG-CQ-ORCH-R002.3 Q2 Source-authority mapping
- [ ] EG-CQ-ORCH-R002.4 Q2 Query fixture
- [ ] EG-CQ-ORCH-R002.5 Q2 Acceptance test
- [ ] EG-CQ-ORCH-R003.1 Q3 Ontology gap: agent template
- [ ] EG-CQ-ORCH-R003.2 Q3 SHACL shape
- [ ] EG-CQ-ORCH-R003.3 Q3 Source-authority mapping
- [ ] EG-CQ-ORCH-R003.4 Q3 Query fixture
- [ ] EG-CQ-ORCH-R003.5 Q3 Acceptance test
- [ ] EG-CQ-ORCH-R004.1 Q4 Ontology gap: best practice and rule scope
- [ ] EG-CQ-ORCH-R004.2 Q4 SHACL shape
- [ ] EG-CQ-ORCH-R004.3 Q4 Source-authority mapping
- [ ] EG-CQ-ORCH-R004.4 Q4 Query fixture
- [ ] EG-CQ-ORCH-R004.5 Q4 Acceptance test
- [ ] EG-CQ-ORCH-R005.1 Q5 Ontology gap: model profile
- [ ] EG-CQ-ORCH-R005.2 Q5 SHACL shape
- [ ] EG-CQ-ORCH-R005.3 Q5 Source-authority mapping
- [ ] EG-CQ-ORCH-R005.4 Q5 Query fixture
- [ ] EG-CQ-ORCH-R005.5 Q5 Acceptance test
- [ ] EG-CQ-ORCH-R006.1 Q6 Ontology gap: handoff
- [ ] EG-CQ-ORCH-R006.2 Q6 SHACL shape
- [ ] EG-CQ-ORCH-R006.3 Q6 Source-authority mapping
- [ ] EG-CQ-ORCH-R006.4 Q6 Query fixture
- [ ] EG-CQ-ORCH-R006.5 Q6 Acceptance test
- [ ] EG-CQ-ORCH-R007.1 Q7 Ontology gap: slot edge
- [ ] EG-CQ-ORCH-R007.2 Q7 SHACL shape
- [ ] EG-CQ-ORCH-R007.3 Q7 Source-authority mapping
- [ ] EG-CQ-ORCH-R007.4 Q7 Query fixture
- [ ] EG-CQ-ORCH-R007.5 Q7 Acceptance test
- [ ] EG-CQ-ORCH-R008.1 Composed Envelope type
- [ ] EG-CQ-ORCH-R008.2 Composed Assemble wiring
- [ ] EG-CQ-ORCH-R008.3 Composed Typed abstention per part
- [ ] EG-CQ-ORCH-R008.4 Composed Query fixture
- [ ] EG-CQ-ORCH-R008.5 Composed Acceptance test
- [ ] EG-CQ-ORCH-R009.1 Probe (graph-os): How many nodes does the graph hold, by label?
- [ ] EG-CQ-ORCH-R009.2 Probe (graph-os): How can I deploy a container with container-manager?
- [ ] EG-CQ-ORCH-R009.3 Probe (graph-os): Which agents are in the Agent Library?
- [ ] EG-CQ-ORCH-R009.4 Probe (graph-os): Why does graph-os use the intent surface?
- [ ] EG-CQ-ORCH-R009.5 Probe (graph-os): Find a tool that lists Docker containers.
- [ ] EG-CQ-ORCH-R009.6 Probe (graph-os): Delegate: list running containers.
- [ ] EG-CQ-ORCH-R010.1 Add the architecture view types
- [ ] EG-CQ-ORCH-R010.2 Derive containers from code and diagrams
- [ ] EG-CQ-ORCH-R010.3 Serve the view through the query path
- [ ] EG-CQ-ORCH-R010.4 Abstain when no source supports a container
- [ ] EG-CQ-ORCH-R010.5 Cite the support of every container
- [ ] EG-CQ-ORCH-R010.6 Prove the view through the served operation

<!-- spec-definition-patch:owner-batch-20261012-orchestration-competency -->
## Reviewed residual definition tasks — owner batch 2026-10-12

- [ ] **EG-CQ-ORCH-R010.7:** Gate plan-producing Decide responses on a revision-matched architecture view and AU study-gap record. Require each feature's attach point and wiring edge. Reject missing or stale inputs before returning a plan.
- [ ] **EG-CQ-ORCH-R010.7 acceptance:** Run `unwired_feature_refuses_before_task_plan`, `stale_architecture_or_study_gap_refuses`, and `test_plan_requires_pinned_architecture_and_attach_points`. Assert named refusals and no plan. Confirm AU proposal id/revision before cross-owner closure. Status remains SPECIFIED.
<!-- /spec-definition-patch -->
