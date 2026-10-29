# MEDULLA-OBJECT-STORAGE-BACKEND tasks

Ordered one-bite lanes. Compile slot: Medulla only, one cargo lane host-wide at a time.

- [ ] MEDULLA-OBJECT-STORAGE-BACKEND-R001 — typed backend spec (`crates/eg-types` wire type; `crates/eg-storage` binding dispatch; lineage registry admission).
- [ ] MEDULLA-OBJECT-STORAGE-BACKEND-R002 — object-store read path (segment fetch by manifest; `GraphView` integration; digest verification).
- [ ] MEDULLA-OBJECT-STORAGE-BACKEND-R003 — object-store commit path (immutable segment write; CAS manifest publish; durability-class ack; stale-CAS refusal).
- [ ] MEDULLA-OBJECT-STORAGE-BACKEND-R004 — shared-root multi-node access (cold open with bounded re-fetch; placement-grant gate; stale-cache discard).
- [ ] MEDULLA-OBJECT-STORAGE-BACKEND-R005 — comparative cost gate (stamped JSONL harness; matched-run protocol; bounded regression guard; claim-ineligibility rule).
