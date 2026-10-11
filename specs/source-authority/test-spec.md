# EG-SRC-AUTH — Test specification

Governing spec: [spec.md](spec.md). No live credential is part of the PR gate; all fixtures are disposable and recorded.

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Contract | Load the core virtual-graph source | All authority terms present | PENDING |
| T-002 | FR-002 | Shape | One fixture per fault: no source, no class, no rank, wrong contract kind, mapping of another class or source, facet not projected, accelerated without reason, equal-rank tie | Valid accepted; each fault refused with its message | PENDING |
| T-003 | FR-003 | Negative and integration | Pack source with `vg:approved true`. Operator source. Approval lease with changed digest. Proposer equal to approver | Pack refused. Operator attaches. Mismatch and same-identity approvals refused | PENDING |
| T-004 | FR-004 | Unit and integration | Two-source fixture. Superclass and subclass rows. Facet rows. Live read of attached sources | Winner, alternates and premises as specified. Live equals fixture | PENDING |
| T-005 | FR-005 | Unit | Each ordering pair; tie across two schema sources | Documented order; `Ambiguous` listing both rows, name swap changes nothing | PENDING |
| T-006 | FR-006 | Unit | Unconnected, unapproved, stale, mismatched, ambiguous. With and without `allowFallback`. No authority row | Typed verdict naming source and concept, or declared fallback recorded | PENDING |
| T-007 | FR-007 | Unit and report | Verdicts to audit; audit to report `sources_used` | Audit equals premises; none added | PENDING |
| T-008 | FR-008 | Unit | Virtual row; accelerated row with reason | Live mode; accelerated mode with reason in the audit | PENDING |
| T-009 | SC-001 | End to end | Recorded responses; swap two ranks; same query text | Different `sources_used`, equal query text, non-empty provenance | PENDING |

Negative cases that must exist: an origin-approved rank-1 declaration with a connected, approved rank-2 alternate must abstain when rank 1 is unconnected or its mapping is unapproved and `allowFallback` is false; the same fixtures may select rank 2 only when rank 1 explicitly enables fallback, with `FallbackFrom` recorded. Also test a caller who cannot see the registered source; a connector self-claim; an equal-rank tie; a copy served for a virtual row.

### EG-SRC-AUTH-R010 tests

- `EG-SRC-AUTH-R010.1`: `crates/eg-rdf/src/source_selection.rs::tests::source_role_round_trips`.
- `EG-SRC-AUTH-R010.2`: `crates/eg-rdf/src/source_selection.rs::tests::live_truth_beats_historical_on_conflict`.
- `EG-SRC-AUTH-R010.3`: `crates/eg-plan/src/federation.rs::tests::plan_applies_source_role`.
- `EG-SRC-AUTH-R010.4`: `crates/eg-rdf/src/source_selection.rs::tests::inspiration_only_abstains_role_insufficient`.
- `EG-SRC-AUTH-R010.5`: `crates/eg-rdf/src/source_selection.rs::tests::role_premise_names_both_sources`.
- `EG-SRC-AUTH-R010.6`: `tests/federated/test_source_role_served.py::test_live_truth_wins_conflict`.

### EG-SRC-AUTH-R011 tests

- `EG-SRC-AUTH-R011.1`: `crates/eg-rdf/src/source_selection.rs::tests::trust_mark_round_trips`.
- `EG-SRC-AUTH-R011.2`: `crates/eg-rdf/src/source_selection.rs::tests::derived_content_inherits_untrusted`.
- `EG-SRC-AUTH-R011.3`: `src/server/handlers/graph_ops/gateway_graph.rs::tests::add_node_stamps_trust_mark`.
- `EG-SRC-AUTH-R011.4`: `crates/eg-rdf/src/source_selection.rs::tests::downgrade_without_approval_refused`.
- `EG-SRC-AUTH-R011.5`: `crates/eg-rdf/src/source_selection.rs::tests::mark_carries_rule_and_generation`.
- `EG-SRC-AUTH-R011.6`: `tests/federated/test_trust_mark_served.py::test_tool_result_is_untrusted_and_sticky`.
