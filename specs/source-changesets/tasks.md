# MEDULLA-SCM tasks

Ordered one-bite lanes. Compile slot: Medulla only, one cargo lane host-wide at a time.

- [ ] MEDULLA-SCM-R001 — content-addressed blob store (blob CAS binding, sealed create-only guard, digest readback).
- [ ] MEDULLA-SCM-R002 — changeset record (`crates/eg-types/src/source_changeset.rs`; chain append; first-parent delta).
- [ ] MEDULLA-SCM-R003 — lock-free acceptance (disjoint path landing; server rebase; `SOURCE_CONFLICT{paths}` refusal).
- [ ] MEDULLA-SCM-R004 — tree materialization by query (`crates/eg-compute/src/source_tree.rs`; snapshot checkpoints; prefix and depth).
- [ ] MEDULLA-SCM-R005 — derived history indexes (`crates/eg-compute/src/source_history.rs`; path history; blame; skip lists; coupling).
- [ ] MEDULLA-SCM-R006 — code graph stays live (incremental re-index on accept; fingerprints, symbols, tests, Leiden subgraph).
- [ ] MEDULLA-SCM-R007 — path-scoped access control (path-prefix predicate on the read-RLS gate; read and write bits).
- [ ] MEDULLA-SCM-R008 — gate verdict records (`GateVerdict` record; sealed create-only storage; landing-policy read).
- [ ] MEDULLA-SCM-R009 — bisect and revert decisions (chain bisect over verdicts; recorded decision; replay).
- [ ] MEDULLA-SCM-R010 — git import and export fidelity (`crates/eg-compute/src/source_git_objects.rs`; tree-hash parity; round-trip test).
- [ ] MEDULLA-SCM-R011 — simulation harness (`tests/simulation/source_changesets.rs`; concurrent writers; crash and replay; fuzzing).
- [ ] MEDULLA-SCM-R012 — run provenance (agent principal binding; run trace id; typed refusal).

- [ ] **MEDULLA-SCM-R002.1:** Structured trailers on a changeset.
- [ ] **MEDULLA-SCM-R013:** Graph-native code edits (rollup).
- [ ] **MEDULLA-SCM-R013.1:** Model.
- [ ] **MEDULLA-SCM-R013.2:** Projection.
- [ ] **MEDULLA-SCM-R013.3:** Round-trip check.
- [ ] **MEDULLA-SCM-R013.4:** Wiring.
- [ ] **MEDULLA-SCM-R013.5:** Refusal.
- [ ] **MEDULLA-SCM-R013.6:** Evidence.
- [ ] **MEDULLA-SCM-R013.7:** Hot-path proof.
