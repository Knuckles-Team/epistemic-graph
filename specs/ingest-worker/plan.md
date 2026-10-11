# EG-INGEST-WORKER - Design and implementation plan

Governing spec: [spec.md](spec.md).

## Functional description (what the deleted worker did)

The agent-utilities `kg-ingest-worker` was a standalone process joining the Kafka consumer group `kg-ingest` on topic `kg_tasks`. The host engine used the same pool in Kafka mode.
- Queue: keyed messages on `kg_tasks`; hydration task types (skill_workflows, capability_hydration) went to a separate `kg_tasks_hydration` topic and consumer group `kg-ingest-hydration`.
- Job kinds: task type read from the WorkItem metadata (document or codebase, plus the two hydration types). Target path resolved from metadata.
- Routing: partition key tenant, then repo or corpus, then task type, giving per-key ordering without global serialization.
- Claiming: Kafka was notification only. Each consumer atomically claimed the deterministic WorkItem (`ClaimWorkItem`, lease 60 s). A negative claim skipped the duplicate. Missing target marked the task failed.
- Batching and sizing: each thread owned one consumer. Threads autosized from CPU (0.36 of cores) and memory (3 GiB per worker). One poll per message, no message batching.
- Checkpointing: offsets committed synchronously only after processing or durable failed marking (at-least-once); max poll interval 1 h to avoid mid-task rebalance.
- Backpressure: Kafka consumer lag, plus jittered admission-denied re-offer delay.
- Failure: any exception marked the task failed and the loop continued. Retry, backoff and dead letter lived in the engine's task code (`_fail_or_retry_task`).
- Reserved lane: at least one and at most count-1 workers polled the hydration topic first (50 ms peek), then the general topic.
- Startup: forced client role, minted an identity session from the process token, verified the engine exposed native ClaimWorkItem before joining, exited 2 otherwise; SIGINT and SIGTERM drained.
- Metrics: logs only; lag came from Kafka.

## Existing system and reuse

Found in this repository: work-item methods (ClaimWorkItem, SubmitWorkItem, RenewWorkItemLease, CommitWorkItemResult, DeferWorkItem, CancelWorkItem) in the coordination domain; SourceIngest dead_letter and enrichment admission (EG-REPO-INGEST-R002, R006); the broker's consumer QoS acks. Not found: a served task queue (landing via EG-DURABLE-KERNEL-R071 to R073), a worker registry (R074), and any ingest-specific lane, depth read or metrics. No Kafka path is wanted.

## Architecture and ownership decision

The engine owns state and policy: queue, leases, retry, dead letter, depth, metrics. The running process is owned by the connector SDK runner, with graph-os as host only.
- Not the engine: job bodies parse files, call LLMs and touch source systems and credentials. The engine is a store and must not run source effects or hold their secrets.
- Not graph-os: it is the door and supervisor. Embedding job logic there recreates the monolith and ties worker scale to the front door.
- The SDK already owns source effects and the halve-and-resend retry against EG batch refusals (EG-REPO-INGEST-R008), so the worker is a client of the engine in the same shape.
- Native fast stages (parse, symbols, stats) stay in-engine per EG-REPO-INGEST-R006; this worker drains only the deferred, source-effect and enrichment jobs.

## Wiring

enqueue_ingest_job is called by SourceIngest when it defers enrichment and by SDK producers. Run_ingest_worker claims through the generated client methods (EG-DURABLE-KERNEL-R073). The identity scopes already in the engine contract apply, and any new scope is a named dependency row. Config keys: queue depth limit, retry policy, lease seconds, worker count, engine endpoint.

## Failure and Backpressure

Failed outcome: redeliver with jittered delay until the ceiling, then one dead-letter record. Depth above limit: producers get INGEST_QUEUE_FULL; workers are unaffected.

## Parity table

| Old entry point | New entry point | Status |
|---|---|---|
| kg-ingest-worker console script | ingest-worker command (R001.15) | to build |
| claim_task_envelope | TaskQueueClaim plus ClaimWorkItem (R001.5, R001.12) | exists once R071 to R073 land |
| run_ingest_consumer_loop | run_ingest_worker (R001.14) | to build |
| start_ingest_consumer_pool (autosize) | autosize in R001.15 | to build |
| hydration topic and reserved floor | R001.10 with R074 to R076 | to build |
| partition_key_for | partition_key (R001.3) | to build |
| Kafka offset commit | TaskQueueAck (R001.14) | to build |
| Kafka lag | IngestQueueDepth and metrics (R001.11, R001.13) | to build |
| Kafka transport and consumer groups | none | intentionally dropped: engine queue replaces it |
| in-process host pool | none | intentionally dropped: workers are always clients |

## Out of scope

Kafka compatibility, the analytics worker, the job bodies' parsing and LLM logic (SDK existing runners).
