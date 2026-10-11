# EG-SRC-AUTH-001 — Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md). No live credential is part of the PR gate; all fixtures are disposable and recorded.

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Contract | Load the core virtual-graph source | All authority terms present | PENDING |
| T-002 | FR-002 | Shape | One fixture per fault: no source, no class, no rank, wrong contract kind, mapping of another class or source, facet not projected, accelerated without reason, equal-rank tie | Valid accepted; each fault refused with its message | PENDING |
| T-003 | FR-003 | Negative and integration | Pack source with `vg:approved true`; operator source; approval lease with changed digest; proposer equal to approver | Pack refused; operator attaches; mismatch and same-identity approvals refused | PENDING |
| T-004 | FR-004 | Unit and integration | Two-source fixture; superclass and subclass rows; facet rows; live read of attached sources | Winner, alternates and premises as specified; live equals fixture | PENDING |
| T-005 | FR-005 | Unit | Each ordering pair; tie across two schema sources | Documented order; `Ambiguous` listing both rows, name swap changes nothing | PENDING |
| T-006 | FR-006 | Unit | Unconnected, unapproved, stale, mismatched, ambiguous; with and without `allowFallback`; no authority row | Typed verdict naming source and concept, or declared fallback recorded | PENDING |
| T-007 | FR-007 | Unit and report | Verdicts to audit; audit to report `sources_used` | Audit equals premises; none added | PENDING |
| T-008 | FR-008 | Unit | Virtual row; accelerated row with reason | Live mode; accelerated mode with reason in the audit | PENDING |
| T-009 | SC-001 | End to end | Recorded responses; swap two ranks; same query text | Different `sources_used`, equal query text, non-empty provenance | PENDING |

Negative cases that must exist: an origin-approved rank-1 declaration with a connected, approved rank-2 alternate must abstain when rank 1 is unconnected or its mapping is unapproved and `allowFallback` is false; the same fixtures may select rank 2 only when rank 1 explicitly enables fallback, with `FallbackFrom` recorded. Also test a caller who cannot see the registered source; a connector self-claim; an equal-rank tie; a copy served for a virtual row.
