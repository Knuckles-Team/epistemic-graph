# EG-INGEST-WORKER implementation tasks

Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md).

- [ ] **T1 - R001:** Ingest worker capability is served by the engine queue and run by the SDK runner (rollup).
- [ ] **T2 - R001.1:** Typed ingest job envelope with refusal tests.
- [ ] **T3 - R001.2:** Typed job kinds and lane derivation.
- [ ] **T4 - R001.3:** Partition key function.
- [ ] **T5 - R001.4:** Enqueue maps onto the served task queue with deterministic ids.
- [ ] **T6 - R001.5:** Prerequisite: durable task queue (this repository, EG-DURABLE-KERNEL-R071, R072, R073).
- [ ] **T7 - R001.6:** Prerequisite: worker registry, reserved admission and scheduler floors (this repository, EG-DURABLE-KERNEL-R074, R075, R076).
- [ ] **T8 - R001.7:** Typed retry policy with validation.
- [ ] **T9 - R001.8:** Retry delay function with jitter.
- [ ] **T10 - R001.9:** Failure routing: retry then dead letter.
- [ ] **T11 - R001.10:** Hydration-reserved lane served as a distinct queue.
- [ ] **T12 - R001.11:** Backpressure read and producer refusal.
- [ ] **T13 - R001.12:** Lease renewal for long jobs.
- [ ] **T14 - R001.13:** Ingest queue metrics.
- [ ] **T15 - R001.14:** Cross-repo (connector SDK repository): ingest worker runner.
- [ ] **T16 - R001.15:** Cross-repo (connector SDK repository): CLI entry point and autosize.
- [ ] **T17 - R001.16:** Cross-repo (graph-os): host and supervise the worker.
- [ ] **T18 - R001.17:** Parity test against the deleted module.

Order: R001.5 and R001.6 prerequisites, then R001.1-R001.3, R001.7-R001.8, R001.4, R001.9-R001.13, then SDK and graph-os rows.
- [ ] **T-R002 - EG-INGEST-WORKER-R002:** Ontology induction from text stays a draft until it passes checks (rollup).
- [ ] **T-R002 - EG-INGEST-WORKER-R002.1:** `OntologyProposal` typed model
- [ ] **T-R002 - EG-INGEST-WORKER-R002.2:** Coverage and redundancy checks as replaceable defaults
- [ ] **T-R002 - EG-INGEST-WORKER-R002.3:** Attach at the ingest job body
- [ ] **T-R002 - EG-INGEST-WORKER-R002.4:** Draft retention refusal
- [ ] **T-R002 - EG-INGEST-WORKER-R002.5:** Check results in the job trace
- [ ] **T-R002 - EG-INGEST-WORKER-R002.6:** Claim-to-admission proof
- [ ] **T-R003 - EG-INGEST-WORKER-R003.1:** `KnowledgeAtom` typed model
- [ ] **T-R003 - EG-INGEST-WORKER-R003.2:** Decompose and select as replaceable defaults
- [ ] **T-R003 - EG-INGEST-WORKER-R003.3:** Attach at the ingest job body and context query
- [ ] **T-R003 - EG-INGEST-WORKER-R003.4:** Missing provenance refusal
- [ ] **T-R003 - EG-INGEST-WORKER-R003.5:** Atom counts in the trace
- [ ] **T-R003 - EG-INGEST-WORKER-R003.6:** Ingest-to-context proof
