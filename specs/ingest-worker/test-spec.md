# EG-INGEST-WORKER - Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Integration | Enqueue one job twice, claim, kill the worker before ack | One task, redelivered after lease expiry, completed once | PENDING |
| T-002 | FR-002 | Integration | One and two worker pools with queued hydration and general jobs | Single worker serves general work. Reserved worker takes hydration first | PENDING |
| T-003 | FR-003 | Integration | Job that always fails | Delays grow with jitter, one dead-letter record after max_attempts | PENDING |
| T-004 | FR-004 | Integration | Fill queue to limit | INGEST_QUEUE_FULL, depth unchanged, gauges correct | PENDING |

## Negative and boundary cases

Blank job_id or target, unknown kind, wrong tenant claim, duplicate delivery after ack, unreachable engine at startup (worker exits non-zero before claiming).

## Quality and release proof

Run cargo test for the touched crates in CI, the SDK unit tests, and the spec-status and spec-decomposition hooks.

## EG-INGEST-WORKER-R002 acceptance

| ID | Requirement | Type | Fixture | Expected | Status |
|---|---|---|---|---|---|
| T-R002 | R002.6 | Integration | One passing and one failing document | Admitted and Draft states | PENDING |

## EG-INGEST-WORKER-R003 acceptance

| ID | Requirement | Type | Fixture | Expected | Status |
|---|---|---|---|---|---|
| T-R003 | R003.6 | Integration | One document and one query | Context from atoms with provenance keys | PENDING |
| T-R003-N | R003.4 | Unit | An atom without provenance | `ATOM_PROVENANCE_MISSING`, none stored | PENDING |

## Document and transcript producer acceptance

The following tests define required acceptance; they do not record passing results.

- `EG-INGEST-WORKER-R003.7`: `crates/eg-types/src/source_document.rs::tests::document_receipt_round_trips`.
- `EG-INGEST-WORKER-R003.8`: `src/server/handlers/source_ingestion.rs::tests::complete_document_is_idempotent`.
- `EG-INGEST-WORKER-R003.9`: `src/server/handlers/source_ingestion.rs::tests::prepare_validates_document_after_resolution`.
- `EG-INGEST-WORKER-R003.10`: `src/server/handlers/source_ingestion.rs::tests::invalid_document_quarantines_without_partial_graph`.
- `EG-INGEST-WORKER-R003.11`: `src/server/handlers/source_ingestion.rs::tests::quarantine_finish_and_replay_preserve_outcome`.
- `EG-INGEST-WORKER-R003.12`: `tests/ingest/test_document_normalization_served.py::test_document_batch_is_atomic`.
- `EG-INGEST-WORKER-R003.13`: `crates/eg-types/src/source_transcript.rs::tests::transcript_round_trips`.
- `EG-INGEST-WORKER-R003.14`: `src/server/handlers/source_ingestion/transcript.rs::tests::source_sequence_and_identity_are_preserved`.
- `EG-INGEST-WORKER-R003.15`: `src/server/handlers/source_ingestion/transcript.rs::tests::prepare_routes_declared_transcript_format`.
- `EG-INGEST-WORKER-R003.16`: `src/server/handlers/source_ingestion/transcript.rs::tests::unknown_format_and_foreign_concept_are_refused`.
- `EG-INGEST-WORKER-R003.17`: `src/server/handlers/source_ingestion/transcript.rs::tests::receipt_preserves_message_provenance`.
- `EG-INGEST-WORKER-R003.18`: `tests/ingest/test_transcript_served.py::test_thread_messages_are_shared_within_scope`.

R003.9 and R003.15 must resolve trusted mappings once and pass that snapshot into validation before admission.
Their fixtures must preserve authorized committed replay when the current mapping revision changes.
R003.12 also requires these served tests in `tests/ingest/test_document_normalization_served.py`:

- `test_first_batch_quarantine_and_replay`: durable quarantine preserves prior checkpoint `None`; replay creates no duplicate receipt or acceptance.
- `test_quarantine_preserves_prior_checkpoint`: quarantine retains prior acceptance; replay after later success must not reset its checkpoint.
- `test_failed_quarantine_commit_is_not_reported_durable`: commit failure returns refusal without a durable quarantine claim.
- `test_quarantine_reads_require_policy`: same-tenant unauthorized callers cannot read quarantine receipts, provenance, raw references or raw content.

R003.14 and R003.18 must reuse session/message IDs across two connectors and two streams without collisions.
Same-source replay must remain idempotent; conflicting same-source content must return `DuplicateMessageConflict`.
R003.18 must deny transcript text and provenance to same-tenant unauthorized callers, while allowing a second authorized agent.
Forged tenant or policy identities must never broaden access or commit a projection.
