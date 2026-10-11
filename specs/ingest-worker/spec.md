# EG-INGEST-WORKER — Durable ingest worker

Status: PROPOSED. Owner: epistemic-graph (queue, policy, metrics); runner owned by the connector SDK repository; host by graph-os. Cross-repo IDs: EG-DURABLE-KERNEL-R071 to R077, EG-REPO-INGEST-R002, R006.
Public provenance: replaces the agent-utilities `kg-ingest-worker` console script (module ingest_worker.py, 427 lines), deleted by commit b02e39950, which claimed an engine-served durable-job path replaces it. This spec makes that claim true.

## Purpose and user stories

An operator runs one or many ingest worker processes that drain queued ingestion jobs (documents, codebases, hydration of skills and capabilities) into the graph, survive worker crashes, and expose backlog so producers slow down.

## Requirements and acceptance

- FR-001: Jobs are enqueued once (deterministic id), claimed with a lease, acked after processing, and redelivered if a worker dies (at-least-once, idempotent claim).
- FR-002: Hydration jobs use a reserved lane that is polled first by reserved workers, with at least one non-reserved worker always serving general work.
- FR-003: Failures retry with jittered backoff and dead-letter after the ceiling.
- FR-004: Queue depth is readable, producers are refused with INGEST_QUEUE_FULL above the limit, and metrics are exported.
- SC-001: Killing a worker mid-job results in exactly one completed job and no lost job.

## Scope and interfaces

Owned here: job model, lanes, partition key, enqueue, retry and dead-letter policy, depth read, metrics. Not owned: durable queue primitives (EG-DURABLE-KERNEL-R071 to R077), source effects and the process (SDK), hosting (graph-os). Out of scope: the in-process Python worker pool, Kafka (the engine queue replaces it), the analytics worker.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md Architecture | T-001 | PENDING |
| FR-002 | plan.md Architecture | T-002 | PENDING |
| FR-003 | plan.md Failure | T-003 | PENDING |
| FR-004 | plan.md Backpressure | T-004 | PENDING |

## Open questions

- The exact name of the connector SDK repository package that hosts the runner was not verified from this repository.
