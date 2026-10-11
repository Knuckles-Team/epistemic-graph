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

## EG-INGEST-WORKER-R002 ontology induction with admission tests

Data flow: served ingest queue claim → runner → `ingest_document` body → induction → `score_proposal` (creates the call site in `crates/eg-capabilities/src/ingest_queue.rs`) → `Admitted` or `Draft` → instance population.
Types: `OntologyProposal` in `crates/eg-types/src/ontology_proposal.rs`.
Attach point: the job body after induction. The production caller is the runner of R001.14.
Failure: a failing score keeps the draft and populates nothing.
Reuses the queue and ack path of R001.4. New: the proposal and its scores.
Decision: admit only after scoring. Rejected: admitting at once and repairing later, which pollutes instances.
Cross-repository dependency: `SDK-SOURCE-INGEST-R010` specifies the planned `run_ingest_worker` producer.
That SDK row remains `SPECIFIED`; it does not establish a live worker runtime.

References
- arXiv:2511.11017 — Peshevski et al., "AI Agent-Driven Framework for Automated Product Knowledge Graph Construction in E-Commerce" (2025)

## EG-INGEST-WORKER-R003 knowledge atoms

Data flow: claim → `ingest_document` body (`crates/eg-capabilities/src/ingest_queue.rs`, created by R002.3) → `decompose` → atom store; query → `select_atoms` → context.
Types: `KnowledgeAtom{atom_id, kind, text, provenance_key, tenant}` in `crates/eg-types/src/knowledge_atom.rs`.
Attach point: the document job body and the context query path. Production caller: the runner of R001.14. No production caller exists today.
Failure: an atom without provenance is refused and not stored.
Existing code: `crates/eg-document/src/contract.rs:141` (chunk validation). Reachable today: no. Search terms with no hit: knowledge atom, memory atom, provenance_key, assemble_context, ContextPack.
Reuses document chunks. New: the atom type, decomposition and selection.
Decision: select atoms per query. Rejected: sending whole documents as context, which spends budget on irrelevant text.
Cross-repository dependency: `SDK-SOURCE-INGEST-R010` specifies the planned `run_ingest_worker` producer.
That SDK row remains `SPECIFIED`; it does not establish a live worker runtime.

References
- arXiv:2606.12400 — Diao et al., "Doc-to-Atom: Learning to Compile and Compose Memory Atoms" (2026)
- arXiv:2606.19847 — Yao et al., "AtomMem" (2026)

## Document and transcript producers within R003

Planned data flow: SDK worker -> served `SourceIngest` -> authorize -> resolve trusted mappings -> validate -> reconcile/admit/map -> one envelope.
The authorized committed-replay shortcut precedes mapping resolution and preserves its recorded outcome.
The production route calls `prepare` at `src/server/dispatch/graph_pipeline/native_routes.rs:408`.
It commits the envelope at `src/server/dispatch/graph_pipeline/native_routes.rs:428`, then calls `finish` at line 429.

Existing code: `SourceIngestionRequest` at `crates/eg-types/src/source_ingestion.rs:265` validates the batch envelope.
`prepare` at `src/server/handlers/source_ingestion.rs:155` authorizes requests and prepares the existing atomic projection.
`resolve_mappings` at `src/server/handlers/source_ingestion.rs:385` resolves trusted mappings, rather than accepting caller-defined ontology authority.
`DocumentData` at `crates/eg-document/src/document.rs:225` already holds content addresses, layout and chunk lineage.
`NativeDocumentRuntime::decode_text` at `crates/eg-document/src/runtime.rs:79` decodes native text.
Its existing production caller is `DecodedIngest::into_document` at `src/server/handlers/modality/ingest.rs:151`.
The source pipeline must reuse document validation without invoking a second modality commit.
Reachable today: yes for generic source ingestion and native document decoding; no for the new normalization and transcript validators.

New code must add typed terminal outcomes, transcript records and two validators after trusted mapping resolution.
Both validators must consume the same resolved mapping snapshot before reconciliation and source admission.
Validators must not resolve mappings independently.
Searches for `TranscriptThread`, `TranscriptMessage`, `validate_transcript` and normalization quarantine found no equivalent source-ingestion validator.
The graph must remain authoritative for accepted records, provenance, mapping revisions and source checkpoints.
`prepare` must return a prepared accepted or quarantine outcome for the existing one-envelope commit route.
Normalization failure must not return the ordinary error path, which skips commit.
A quarantine envelope must store only restricted evidence; it must not change the source marker, checkpoint or accepted projections.
`finish` at `src/server/handlers/source_ingestion.rs:1683` must return quarantine only after its envelope commits.
The accepted variant must retain `SourceIngestionReceipt`; the quarantine variant must carry an optional prior accepted checkpoint.
First-batch quarantine must preserve `None`; it must not fabricate acceptance of the submitted checkpoint.
`prepared_replay` at `src/server/handlers/source_ingestion.rs:1791` must restore the stored outcome after verifying authority.
Quarantine replay must preserve its original receipt without resetting a checkpoint advanced by later accepted ingestion.
A failed quarantine commit must return refusal, never a durable quarantine claim.
Raw content must retain its existing content-addressed admission boundary.
The format registry must come from the single resolved connector mapping revision.
The engine must bind tenant and policy identities from verified server authority, never from source payloads.
Transcript thread identity must include tenant, connector, stream and session ID; message identity must add message ID.
The source-scoped convention at `src/server/handlers/source_ingestion.rs:2187` must encode those record keys without ambiguity.
Read authorization must deny same-tenant unauthorized access to transcript text, provenance, quarantine receipts and raw-content references.
Raw-content retrieval must enforce the stored policy; knowledge of a reference must grant no access.
Transcript mappings must reuse existing message and concept vocabulary where applicable; they must not redefine media transcript segments.

Decision: validate the complete source batch before committing its graph projection.
Rejected alternative: commit individual messages or atoms during normalization, which exposes partial knowledge after failure.
The named defaults `complete_document`, `content_identity`, `source_sequence` and `source_identity` must be replaceable through versioned policy.

Cross-repository dependency: `SDK-SOURCE-INGEST-R010` specifies `run_ingest_worker`; its delivery state remains `SPECIFIED`.
This planned worker must capture sources, perform source effects, parse external formats and submit normalized records.
`GRAPHOS-` owns the hosted entry and worker supervision; `AU-` emits harness events only in this ingestion boundary.
EG must validate, store and select knowledge; no connector credential or source-effect loop moves into EG.
R001 already owns queues and retries; R003.1 through R003.6 already own atom decomposition and selection.
Decision rows R133 through R138 own run traces, plans and budgets; they do not own source transcript ingestion.

### Source evidence mapping

Queue authority: plans commit `e3e7260d41006035c5499b46686101c3f77e5302`, `spec-audit/writer-queue-merged/epistemic-graph.tsv`.
Code and deduplication base: EG commit `2e44b8af87ec9681ab0f45d13b37fab38d45c7c2`.

| Queue line | Source paths | Citation | Requirement mapping |
|---|---|---|---|
| 47 | `prompts/conversations/old/paper_ingestion.txt`; `open-source-libraries/LeanRAG/README.md` | None in source queue | Extend R003 with R003.7 through R003.12: normalized documents, atomic publication and quarantine. |
| 48 | `prompts/plans/om_plan.md`; `prompts/plans/temporal_update.md` | None in source queue | Extend R003 with R003.13 through R003.18: timestamped threads, messages, concept links and format refusal. |

These inputs add no paper citation. Existing R003 research references inspire atom design only; they do not prove served acceptance.
