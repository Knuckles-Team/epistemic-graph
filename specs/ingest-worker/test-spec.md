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
