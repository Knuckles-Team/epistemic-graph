# EG-ORCH-VOCAB-001 — Test spec

Positive: each term resolves and the reasoner stays consistent; each Q1-Q7 query returns the fixture rows. Negative: one planted-invalid fixture per shape (cycle in `:precedes`, cycle in `swarm:feeds`, slot with a non-kind, member without capability, rule without class, agent without model). Integration: committed assembly is queryable through Q5 and Q7. All tests live in `crates/eg-core/tests/orchestration_vocabulary.rs` against `crates/eg-core/tests/fixtures/orchestration_vocabulary/`.
