# Seam contract: proposal review

Governing spec: [spec.md](../spec.md).

## Provider

Repository: medulla. Rows: `MEDULLA-SDLC-R017.2`, `MEDULLA-SDLC-R017.3`, `MEDULLA-SDLC-R017.4`, `MEDULLA-SDLC-R017.6`.

## Consumer

Repository: arbora, row `ARBORA-OPS-R896.5` (goal, proposal list and decision operations). Repository: overstory, row `OVERSTORY-DECIDE-R009`. The spec-delivery page of pipelines reads the same list.

## Wire shape

| Method | Request | Answer |
|---|---|---|
| `ProposalList` | `{repo, state?, kind?}` | `[{id, kind, target, evidence[], state}]` |
| `ProposalDecide` | `{id, decision, reviewer, reason}` | `{id, state, pull_request?}` |
| `GoalPut` | `{repo, statement, targets[]}` | `{goal_id}` |

`kind` is one of twelve values. Row kinds: add, update, split, prune and retire. Plan kind: edit plan section. Task kinds: add, reorder and split. Other kinds: add contract, resolve clarification and tune knob. `state` is `open`, `approved`, `rejected` or `landed`. A proposal reaches `landed` only after its pull request merges.

## Refusal codes

- `PROPOSAL_KIND_UNKNOWN`: the kind is not in the closed set.
- `PROPOSAL_EVIDENCE_REQUIRED`: the proposal has no evidence.
- `PROPOSAL_DIRECT_WRITE_REFUSED`: a caller tried to write a spec file directly.
- `MODE_REFERENCE_NO_PROPOSALS`: the codebase is in mode `reference`.
- `MODE_APPROVAL_REQUIRED`: a move to `evolving` has no operator approval.
- `GOAL_TARGET_REQUIRED`: a goal names no target.

## Authoritative code types

- `crates/medulla-types/src/lifecycle.rs`: `ProposalKind`, `Proposal`, `Goal` and `ProjectMode`.

## Test vectors

- `crates/medulla-types/tests/lifecycle_records.rs::proposal_kind_set_is_closed` holds the kind vector.
- `tests/sdlc/test_proposals_served.py::test_review_list_and_decision_round_trip` holds the list and decision vectors.
