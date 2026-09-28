# Tasks

Status key: `[ ]` queued, `[-]` in progress, `[x]` verified at cited main revision. All tasks below remain unchecked until an implementation PR supplies exact evidence.

- [ ] **K-01 (KG-01, KG-02):** Pin the current kernel revision, map every durable write entry point, write the cross-store atomicity ADR, and add all-or-none crash/retry tests (T-KG-01/02).
- [ ] **K-02 (KG-03, KG-04):** Validate and cache owner bindings at one transaction boundary, retain golden layout lineage/refusal cases, implement bounded scrub and typed status (T-KG-03/04).
- [ ] **K-03 (KG-05, KG-06):** Make head-only outbox attempts and reject/rewind semantics observable; add cursor-based CDC/mirror replay, table mirroring, digest reconciliation and empty-default refusal (T-KG-05/06).
- [ ] **K-04 (KG-07):** Enumerate all served read/write paths, thread immutable verified scope and build cross-principal/tenant denial fixtures including cache/UDF/foreign/TSDB (T-KG-07).
- [ ] **K-05 (KG-08, KG-09):** Add durable audit reservation/outcome reconciliation and reserved two-person lease kinds; generate any changed protocol/client contracts (T-KG-08/09).
- [ ] **K-06 (KG-10):** Capture failure traces automatically, restore full multi-group workload, prove repeated loaded failover and independent progress (T-KG-10).
- [ ] **K-07 (KG-11, KG-12):** Add explicit durability classes and loss-window metrics; build recoverable hot structures and linearizable typed primitives (T-KG-11).
- [ ] **K-08 (KG-13, KG-14):** Route point/structure operations before SQL planning, implement one-admission pipeline batches and schema/policy-safe plan cache (T-KG-12).
- [ ] **K-09 (KG-15):** Decide store engine by ADR, add hot/cold/tombstone parity and replication-out tooling, run differential/restore and published benchmark matrix (T-KG-13).
- [ ] **K-10 (all):** Run configured CCCC, Dupehound, jscpd, KISS, rust architecture lint, clippy, focused tests and cloud CI at exact head; record positive and negative evidence, then update delivery and acceptance state separately.
