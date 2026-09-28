# Implementation plan

## Sequence

1. **Freeze shared contracts.** Audit current `eg-types` decision, connector-pack and generated Python/wire surfaces. Keep one component schema bump and one method inventory update. Add missing typed errors and versioned vectors without changing old record bytes. Gate: A1–A2.
2. **Close exact-solve and commit gaps.** Reuse `eg-compute/solve`, `decision/derivation`, server decide handlers and Agent Library transaction. Make certificate verification independent, unknown-fact policy explicit, no-good-cut bounded, and commit a re-derived CAS operation. Gate: A3–A5 and B.
3. **Qualify pack fact admission.** Consume validated component revisions only. Preserve claim provenance, reject decision-owned pack kinds, exclude withdrawn entries and refuse stale projection facts. Gate: E.
4. **Complete statistical boundary.** Reuse `eg-numeric/decision`, decision jobs, and record v2; make visible-only features, exact propensity logging, independent-outcome admission, calibration, replay and receipt-bound promotion testable. Gate: C–D.
5. **Add question adapters.** Implement typed retrieval, routing, enrichment, entity-resolution, schema mapping, topology and informational recommendation questions through the same ladder. No adapter owns a second policy or decision store. Gate: D and question-specific negative tests.
6. **Publish evidence.** Run quality and hosted checks on the exact branch head, land on main, rerun or cite hosted main checks, and update each requirement's delivery and acceptance states. A branch green status is not a main acceptance verdict.

## Parallelism and dependencies

The contract bump precedes decision handler integration. Exact solver and pack fact-admission work may proceed independently once DTO ownership is frozen. Statistical jobs consume the committed record and visibility rules; question adapters consume the stable decision method. Shared edits to `agent_component.rs`, method descriptors, access classification and generated contract files are one integration lane to avoid competing schema versions.

## Review rules

Review each changed module for reuse of existing transactions, outbox, Blob CAS, generated DTOs and visibility filters. Require a negative proof for every new authority boundary, persistence mutation and candidate source. Check CCCC, Dupehound, KISS and zero-new-pair jscpd before landing; resolve findings in the owning module rather than adding a duplicate helper. Hosted baseline checks build disposable local fixtures. Record live-deployment evidence separately.
