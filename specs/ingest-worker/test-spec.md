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
- `EG-INGEST-WORKER-R003.9`: `src/server/handlers/source_ingestion.rs::tests::prepare_validates_document_before_mapping`.
- `EG-INGEST-WORKER-R003.10`: `src/server/handlers/source_ingestion.rs::tests::invalid_document_quarantines_without_partial_graph`.
- `EG-INGEST-WORKER-R003.11`: `crates/eg-types/src/source_document.rs::tests::receipt_links_normalization_evidence`.
- `EG-INGEST-WORKER-R003.12`: `tests/ingest/test_document_normalization_served.py::test_document_batch_is_atomic`.
- `EG-INGEST-WORKER-R003.13`: `crates/eg-types/src/source_transcript.rs::tests::transcript_round_trips`.
- `EG-INGEST-WORKER-R003.14`: `src/server/handlers/source_ingestion/transcript.rs::tests::source_sequence_and_identity_are_preserved`.
- `EG-INGEST-WORKER-R003.15`: `src/server/handlers/source_ingestion/transcript.rs::tests::prepare_routes_declared_transcript_format`.
- `EG-INGEST-WORKER-R003.16`: `src/server/handlers/source_ingestion/transcript.rs::tests::unknown_format_and_foreign_concept_are_refused`.
- `EG-INGEST-WORKER-R003.17`: `src/server/handlers/source_ingestion/transcript.rs::tests::receipt_preserves_message_provenance`.
- `EG-INGEST-WORKER-R003.18`: `tests/ingest/test_transcript_served.py::test_thread_messages_are_shared_within_scope`.
