# EG-DATAPLAT-VOCAB — Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Unit | load `data_platform-v1.ttl` | all shared, store and mapping IRIs present with domain/range | PENDING |
| T-002 | FR-001 | Unit | validate a `document` connection and an `ftp` one | `document` accepted, `ftp` rejected | PENDING |
| T-003 | FR-002 | Unit | `dp:SecretPath` with a `value` triple, then with `token` | both fail `sh:closed` validation | PENDING |
| T-004 | FR-002 | Unit | `dp:StoreMapping` with `dp:viaOperation` `openbao_mcp_kv:kv2_get` | refused with a typed error | PENDING |
| T-005 | FR-003 | Unit | `materialised` mapping without reason; `virtual` without reason | first rejected, second accepted | PENDING |
| T-006 | FR-004 | Unit | scan vocabulary and fixtures for discovery operations | none declared | PENDING |
| T-007 | FR-005 | Unit | resolve each alignment row | every package term maps to a `dp:` term | PENDING |
| T-008 | FR-006 | Review | R009 rows exist for objectstore, vector, openbao, clarity, data-science | each names an owner repo and a fixture test | PENDING |

| T-009 | FR-002 | Unit | one policy grants read on path A and deny on path B. Then add an invalid permission | each grant retains its policy/path association. Invalid permission rejected | PENDING |

## Negative and boundary cases

Negative proof: an absent source yields a typed abstention, never an empty success. A secret value is never representable. A negative `dp:lag`, a zero `dp:dimensions` and a `presenceRatio` of 2 are rejected.

## Quality and release proof

Focused: `cargo test -p eg-core --test data_platform_vocab` (run by the implementing lane, not this spec PR). Spec gates: `pipelines-hook spec-status --write` and `spec-decomposition`.
