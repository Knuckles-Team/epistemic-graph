# EG-SCM tasks

Ordered one-bite lanes. Compile slot: EG only, one cargo lane host-wide at a time.

- [ ] EG-SCM-R001 — content-addressed blob store (blob CAS binding, sealed create-only guard, digest readback).
- [ ] EG-SCM-R002 — changeset record (`crates/eg-types/src/source_changeset.rs`; chain append; first-parent delta).
- [ ] EG-SCM-R003 — lock-free acceptance (disjoint path landing; server rebase; `SOURCE_CONFLICT{paths}` refusal).
- [ ] EG-SCM-R004 — tree materialization by query (`crates/eg-compute/src/source_tree.rs`; snapshot checkpoints; prefix and depth).
- [ ] EG-SCM-R005 — derived history indexes (`crates/eg-compute/src/source_history.rs`; path history; blame; skip lists; coupling).
- [ ] EG-SCM-R006 — code graph stays live (incremental re-index on accept; fingerprints, symbols, tests, Leiden subgraph).
- [ ] EG-SCM-R007 — path-scoped access control (path-prefix predicate on the read-RLS gate; read and write bits).
- [ ] EG-SCM-R008 — gate verdict records (`GateVerdict` record; sealed create-only storage; landing-policy read).
- [ ] EG-SCM-R009 — bisect and revert decisions (chain bisect over verdicts; recorded decision; replay).
- [ ] EG-SCM-R010 — git import and export fidelity (`crates/eg-compute/src/source_git_objects.rs`; tree-hash parity; round-trip test).
- [ ] EG-SCM-R011 — simulation harness (`tests/simulation/source_changesets.rs`; concurrent writers; crash and replay; fuzzing).
- [ ] EG-SCM-R012 — run provenance (agent principal binding; run trace id; typed refusal).

- [ ] **EG-SCM-R002.1:** Structured trailers on a changeset.
- [ ] **EG-SCM-R013:** Graph-native code edits (rollup).
- [ ] **EG-SCM-R013.1:** Model.
- [ ] **EG-SCM-R013.2:** Projection.
- [ ] **EG-SCM-R013.3:** Round-trip check.
- [ ] **EG-SCM-R013.4:** Wiring.
- [ ] **EG-SCM-R013.5:** Refusal.
- [ ] **EG-SCM-R013.6:** Evidence.
- [ ] **EG-SCM-R013.7:** Hot-path proof.
