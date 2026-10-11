# EG-ORCH-VOCAB — Test spec

Positive: each term resolves and the reasoner stays consistent; each Q1-Q7 query returns the fixture rows. Negative: one planted-invalid fixture per shape (cycle in `:precedes`, cycle in `swarm:feeds`, slot with a non-kind, member without capability, rule without class, agent without model). Integration: committed assembly is queryable through Q5 and Q7. All tests live in `crates/eg-core/tests/orchestration_vocabulary.rs` against `crates/eg-core/tests/fixtures/orchestration_vocabulary/`.

Assembly-kind acceptance: a verifier-constrained slot excludes an executor-only candidate, accepts a visible verifier, and yields typed abstention without one.

Harness run-trace acceptance: a valid trace per termination reason round-trips through the client and `AddNode`. The engine refuses a negative counter, an unknown reason, and a peak above budget without `context_overflow`. The engine refuses a measured fact without evaluation-run evidence. A newer measurement supersedes an older one, and the older one stays readable. Competency question: for model M on task class C, `harness_best_config` returns the configuration with the lowest cost per solved task. It returns its trace ids and abstains below the sample floor.
Spec-folder mapping: a fixture folder through `IndexRepository` yields one node of each kind. An unknown file is refused and stored as a plain document. Every node names its deciding rule.
