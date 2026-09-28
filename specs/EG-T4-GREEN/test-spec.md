# EG-T4-GREEN — Test specification

Status: IN REVIEW. Governing [spec](spec.md). Evidence: [evidence.md](evidence.md).

| ID | Requirement | Level | Input / setup | Expected observation |
|---|---|---|---|---|
| T-001 | FR-001 | CI build | Full and workspace crate matrix at PR head | Compile and tests pass |
| T-002 | FR-001 | CI build | Slim server and Raft cluster features | Referenced Shard methods compile, no missing imports |
| T-003 | FR-002 | Contract | Declared code plus detail | Code stays declared; detail survives response conversion |
| T-004 | FR-002 | Negative | Undeclared free text from each touched handler | Classified under a declared method code; no invented public code |
| T-005 | FR-003 | Security | Node, tenant, audience and policy mismatch envelopes | Documented distinct code; node detail redacted |
| T-006 | FR-003 | Security | Other invalid identity | `AUTHENTICATION_REQUIRED`, no input echo |
| T-007 | FR-004 | Static/unit | Registry census, candidate file, ConnectorPack, nonce, metrics, dispatch fixtures | Each matches the live owner/contract |
| T-008 | FR-005 | Package/client | Exact EG wheel plus Python, Go and JS client suites | All pass against same protocol revision |
| T-009 | FR-005 | Scanner | CCCC, KISS, dupehound, jscpd, decomposition | Zero actionable findings under accepted gate terms |
| T-010 | SC-001 | Release | Complete hosted check rollup and evidence review | Required checks pass; unresolved advisory failures dispositioned; exact receipts and merged revision recorded in `evidence.md` |

A local unit pass is not a package, served, or hosted acceptance claim. Attach exact command, revision, environment and result to `evidence.md` before marking any test passed.
