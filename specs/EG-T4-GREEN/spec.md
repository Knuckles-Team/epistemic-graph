# EG-T4-GREEN — Restore green Train 4 integration

Status: LANDED (PR #3 merged at `a7f4dd74e7b2d01832fc9c3359046255d0e606d4`; acceptance pending). Owner: `epistemic-graph`. Related obligation IDs: EH-592 (wheel/package), EH-655 (jscpd), EH-656 (quality hooks). Public implementation and review: [PR #3](https://github.com/Knuckles-Team/epistemic-graph/pull/3). This spec contains the required behavior and acceptance contract.

## Purpose

As an EG contributor, I need Train 4's merged engine to compile and preserve declared error semantics across its supported feature sets, so downstream consumers can rely on one published contract and release qualification can proceed.

## Requirements and acceptance

- FR-001: Supported full, slim server, raft, and workspace crate configurations compile at the exact candidate revision. Feature gates and referenced Raft methods must agree.
- FR-002: `Response::err` keeps strict declared error codes. Each producer emits a declared code before detail; converting a response back to text preserves refusal detail without bypassing the contract.
- FR-003: Request verification preserves the documented node and tenant/audience/policy mismatch codes, redacts node identity, and rejects all other unclassified auth failures.
- FR-004: Tests and static gates reflect actual Train 4 table registry, candidate file role, served ConnectorPack operations, nonce rules, metrics label ordering, and dispatch ownership.
- FR-005: Quality gates for EH-655/EH-656 and the EH-592 wheel/package consumer proof pass on the same reviewed source lineage; status is not advanced based on source-only tests.
- SC-001: All required hosted checks pass on the final PR head and no critical/high scanner or release blocker remains. A failed advisory quality check requires a documented disposition in `evidence.md` against this repository's [quality gate terms](../../docs/quality-gate-terms.md).

## Scope and boundaries

This spec covers the focused PR #3 green repair and its direct release evidence. Other Train 4 obligations require separate owner specs and evidence. TxnUql/TxnUnifiedQuery lifecycle refusals remain an explicit open contract decision in PR #3.

## Traceability

| Requirement | Design | Tests | Current evidence |
|---|---|---|---|
| FR-001 | `plan.md` feature ownership | T-001, T-002 | Hosted checks pending |
| FR-002–003 | `plan.md` error boundary | T-003–T-006 | PR #3 source and partial tests |
| FR-004 | `plan.md` exact pins | T-007 | Partial local pass |
| FR-005, SC-001 | `plan.md` release path | T-008–T-010 | Open; see `evidence.md` |
