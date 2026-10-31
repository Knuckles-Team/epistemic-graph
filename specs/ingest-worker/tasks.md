# MEDULLA-INGEST-WORKER implementation tasks

Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md).

- [ ] **T1 - R001:** Ingest worker capability is served by the engine queue and run by the Graft SDK runner (rollup).
- [ ] **T2 - R001.1:** Typed ingest job envelope with refusal tests.
- [ ] **T3 - R001.2:** Typed job kinds and lane derivation.
- [ ] **T4 - R001.3:** Partition key function.
- [ ] **T5 - R001.4:** Enqueue maps onto the served task queue with deterministic ids.
- [ ] **T6 - R001.5:** Prerequisite: durable task queue (this repository, MEDULLA-DURABLE-KERNEL-R071, R072, R073).
- [ ] **T7 - R001.6:** Prerequisite: worker registry, reserved admission and scheduler floors (this repository, MEDULLA-DURABLE-KERNEL-R074, R075, R076).
- [ ] **T8 - R001.7:** Typed retry policy with validation.
- [ ] **T9 - R001.8:** Retry delay function with jitter.
- [ ] **T10 - R001.9:** Failure routing: retry then dead letter.
- [ ] **T11 - R001.10:** Hydration-reserved lane served as a distinct queue.
- [ ] **T12 - R001.11:** Backpressure read and producer refusal.
- [ ] **T13 - R001.12:** Lease renewal for long jobs.
- [ ] **T14 - R001.13:** Ingest queue metrics.
- [ ] **T15 - R001.14:** Cross-repo (connector Graft SDK repository): ingest worker runner.
- [ ] **T16 - R001.15:** Cross-repo (connector Graft SDK repository): CLI entry point and autosize.
- [ ] **T17 - R001.16:** Cross-repo (Arbora): host and supervise the worker.
- [ ] **T18 - R001.17:** Parity test against the deleted module.

Order: R001.5 and R001.6 prerequisites, then R001.1-R001.3, R001.7-R001.8, R001.4, R001.9-R001.13, then Graft SDK and Arbora rows.
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002:** Ontology induction from text stays a draft until it passes checks (rollup).
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.1:** `OntologyProposal` typed model
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.2:** Coverage and redundancy checks as replaceable defaults
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.3:** Attach at the ingest job body
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.4:** Draft retention refusal
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.5:** Check results in the job trace
- [ ] **T-R002 - MEDULLA-INGEST-WORKER-R002.6:** Claim-to-admission proof
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.1:** `KnowledgeAtom` typed model
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.2:** Decompose and select as replaceable defaults
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.3:** Attach at the ingest job body and context query
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.4:** Missing provenance refusal
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.5:** Atom counts in the trace
- [ ] **T-R003 - MEDULLA-INGEST-WORKER-R003.6:** Ingest-to-context proof
- [ ] MEDULLA-INGEST-WORKER-R003.7: Model for document normalization.
- [ ] MEDULLA-INGEST-WORKER-R003.8: Behavior for document normalization.
- [ ] MEDULLA-INGEST-WORKER-R003.9: Resolve mappings once before document validation; preserve authorized replay.
- [ ] MEDULLA-INGEST-WORKER-R003.10: Refusal for document normalization.
- [ ] MEDULLA-INGEST-WORKER-R003.11: Return typed accepted or quarantine outcomes from finish and replay.
- [ ] MEDULLA-INGEST-WORKER-R003.12: Prove first-batch quarantine, replay, checkpoint preservation and read denial.
- [ ] MEDULLA-INGEST-WORKER-R003.13: Model for transcript ingestion.
- [ ] MEDULLA-INGEST-WORKER-R003.14: Scope transcript identity by tenant, connector, stream, session and message.
- [ ] MEDULLA-INGEST-WORKER-R003.15: Wiring for transcript ingestion.
- [ ] MEDULLA-INGEST-WORKER-R003.16: Refusal for transcript ingestion.
- [ ] MEDULLA-INGEST-WORKER-R003.17: Evidence for transcript ingestion.
- [ ] MEDULLA-INGEST-WORKER-R003.18: Prove source isolation, idempotence and same-tenant unauthorized read denial.

- [ ] **MEDULLA-INGEST-WORKER-R902:** Source permission and group sync (rollup).
- [ ] **MEDULLA-INGEST-WORKER-R902.1:** Model.
- [ ] **MEDULLA-INGEST-WORKER-R902.2:** Behavior.
- [ ] **MEDULLA-INGEST-WORKER-R902.3:** Wiring.
- [ ] **MEDULLA-INGEST-WORKER-R902.4:** Refusal.
- [ ] **MEDULLA-INGEST-WORKER-R902.5:** Evidence.
- [ ] **MEDULLA-INGEST-WORKER-R902.6:** Hot-path proof.
- [ ] **MEDULLA-INGEST-WORKER-R903:** Source deletion reconciliation (rollup).
- [ ] **MEDULLA-INGEST-WORKER-R903.1:** Model.
- [ ] **MEDULLA-INGEST-WORKER-R903.2:** Behavior.
- [ ] **MEDULLA-INGEST-WORKER-R903.3:** Wiring.
- [ ] **MEDULLA-INGEST-WORKER-R903.4:** Refusal.
- [ ] **MEDULLA-INGEST-WORKER-R903.5:** Evidence.
- [ ] **MEDULLA-INGEST-WORKER-R903.6:** Hot-path proof.
- [ ] **MEDULLA-INGEST-WORKER-R904:** Source container hierarchy (rollup).
- [ ] **MEDULLA-INGEST-WORKER-R904.1:** Model.
- [ ] **MEDULLA-INGEST-WORKER-R904.2:** Behavior.
- [ ] **MEDULLA-INGEST-WORKER-R904.3:** Wiring.
- [ ] **MEDULLA-INGEST-WORKER-R904.4:** Refusal.
- [ ] **MEDULLA-INGEST-WORKER-R904.5:** Evidence.
- [ ] **MEDULLA-INGEST-WORKER-R904.6:** Hot-path proof.

## Stream content versions (R910)

- [ ] **MEDULLA-INGEST-WORKER-R910 (rollup):** Version source content per stream, not per batch.
- [ ] **MEDULLA-INGEST-WORKER-R910.1 (Model):** Derive the content version object id from the stream.
- [ ] **MEDULLA-INGEST-WORKER-R910.2 (Behavior):** Chain each batch to the prior stream digest; count equal content as unchanged.
- [ ] **MEDULLA-INGEST-WORKER-R910.3 (Wiring):** Stamp the stream id in `admit_records` and read the chain in the content check.
- [ ] **MEDULLA-INGEST-WORKER-R910.4 (Refusal):** Refuse a true stream-head mismatch with `STALE_CONTENT_VERSION`.
- [ ] **MEDULLA-INGEST-WORKER-R910.5 (Evidence):** Record the digest chain in the receipt.
- [ ] **MEDULLA-INGEST-WORKER-R910.6 (Hot-path proof):** Add the served two-batch test and the benchmark.
