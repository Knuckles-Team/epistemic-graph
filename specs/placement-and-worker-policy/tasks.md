# EG-PLACEMENT implementation tasks

Status: PROPOSED. Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md).

- [ ] **T1 - R001:** Deployment-placement question and assembled step graph go through the engine decision functionality (rollup).
- [ ] **T2 - R001.1:** Prerequisite (decision-engine): standalone topology Decide question.
- [ ] **T3 - R001.2:** Add the `Placement` question kind.
- [ ] **T4 - R001.3:** Target registry.
- [ ] **T5 - R001.4:** Typed request body with refusals.
- [ ] **T6 - R001.5:** Add the placement vocabulary and shape.
- [ ] **T7 - R001.6:** Pure feasibility function.
- [ ] **T8 - R001.7:** Wire the kind into Decide.
- [ ] **T9 - R001.8:** Capacity premise reuse, no lease.
- [ ] **T10 - R001.9:** Why-not for every non-chosen target.
- [ ] **T11 - R001.10:** Typed abstention.
- [ ] **T12 - R001.11:** Served contract test through the real dispatch.
- [ ] **T13 - R001.12:** Assembled step graph fixture.
- [ ] **T14 - R001.13:** Assemble abstains rather than invents.
- [ ] **T15 - R001.14:** Cross-repo (graph-os): correct R017 wording.
- [ ] **T16 - R002:** Ingest-worker policy has one authority: the engine decides, the worker asks and obeys (rollup).
- [ ] **T17 - R002.1:** Register carries capacity.
- [ ] **T18 - R002.2:** Autosize and depth limit live in `SchedulerConfig`.
- [ ] **T19 - R002.3:** Pure grant function.
- [ ] **T20 - R002.4:** Admission returns a grant.
- [ ] **T21 - R002.5:** Routing is engine-side.
- [ ] **T22 - R002.6:** Reword `EG-INGEST-WORKER-R001.2`.
- [ ] **T23 - R002.7:** Reword `EG-INGEST-WORKER-R001.3`.
- [ ] **T24 - R002.8:** Reword `EG-INGEST-WORKER-R001.6`.
- [ ] **T25 - R002.9:** Reword `EG-INGEST-WORKER-R001.10`.
- [ ] **T26 - R002.10:** Reword `EG-INGEST-WORKER-R001.11`.
- [ ] **T27 - R002.11:** Reword `EG-INGEST-WORKER-R001.14`.
- [ ] **T28 - R002.12:** Reword `EG-INGEST-WORKER-R001.15`.
- [ ] **T29 - R002.13:** Cross-repo (connector SDK repository): no policy constants in the worker.
- [ ] **T30 - R002.14:** Fold the analytics pool knob into the same config.
- [ ] **T31 - R002.15:** Parity check that no duplicate policy remains.
- [ ] **T32 - R003:** Analytics payload bound is decided and defended (rollup).
- [ ] **T33 - R003.1:** Name the constant.
- [ ] **T34 - R003.2:** Separate refusal for cardinality.
- [ ] **T35 - R003.3:** Accept at the cap, refuse above.
- [ ] **T36 - R003.4:** Defence in depth in the kernel.
- [ ] **T37 - R003.5:** Plain labels are accepted and stored opaque.
- [ ] **T38 - R003.6:** Cap is counted after hashing and equals the raw distinct count.

Order: R003 (independent, smallest), R002.1-R002.5 with the durable-kernel rows, then R002 rewordings; R001.1 (decision-engine R111.1) before R001.2-R001.11; R001.12-R001.14 last.
