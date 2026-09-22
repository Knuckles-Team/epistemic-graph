# Ingestion API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.ingestion.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 14 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AddEmbedding`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `embedding` | array of number (float) | yes |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AddEmbedding`, `contract/schemas/result.ingestion.json#/methods/AddEmbedding`.

## `Asr`

self-routes before dispatch_graph_op like Quantum/Viz; direct non-durable whisper-rs transcription, commits no asr.result.v1 (that governed commit is future worker/AU-orchestration work, W03/W06)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `asr:transcribe` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `AsrOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `TranscribeFile` | `AsrTranscription` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Asr`, `contract/schemas/result.ingestion.json#/methods/Asr`.

## `Discover`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:semantic` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `k` | integer (uint) | yes |  |
| `keywords` | array of string | yes |  |
| `query_embedding` | array of number (float) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `DiscoverHit` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Discover`, `contract/schemas/result.ingestion.json#/methods/Discover`.

## `IndexRepository`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:parse` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `files_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `IndexResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/IndexRepository`, `contract/schemas/result.ingestion.json#/methods/IndexRepository`.

## `ObserveScreen`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:vision` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `obs_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ScreenObservationResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ObserveScreen`, `contract/schemas/result.ingestion.json#/methods/ObserveScreen`.

## `ParseFile`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:parse` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `file_path` | string | yes |  |
| `source` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ParseResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ParseFile`, `contract/schemas/result.ingestion.json#/methods/ParseFile`.

## `ParseFiles`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:parse` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `files_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `ParseResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ParseFiles`, `contract/schemas/result.ingestion.json#/methods/ParseFiles`.

## `Quantum`

self-routes before dispatch_graph_op like AnalyticsJob/Statechart, never reaches the graph tamper-evident audit chain; R5 override audit instead rides the response's PlannerDecision.audit trail into the agent-utilities :ToolCall/:QuantumJob provenance

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `quantum:run` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `QuantumOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `expectation` | `QuantumExpectationResult` | Json |  |
| `optimize_qaoa` | `QuantumQaoaResult` | Json |  |
| `rank` | `QuantumRankResult` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Quantum`, `contract/schemas/result.ingestion.json#/methods/Quantum`.

## `SemanticIndex`

RF-019's S1-S6 tiered ingestion queue. Runtime-conditional like the four agent layers, but over SIX authz actions rather than two: binding lifecycle is semantic:binding-write, S1 admission semantic:source-admit, subscribe/claim/release semantic:stage-claim, stage completion semantic:stage-complete, and the reads semantic:binding-read / semantic:stage-read. The row names the binding-write leg; SemanticIndexOp::authz_action is the authority for each operation

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `semantic:binding-write` |
| Mutates | `true` |
| Durability domain | `SemanticIndexRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `SemanticIndexOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `admit_binding` | `SemanticMutationReceipt` | Raw |  |
| `admit_source_page` | `SemanticSqlSourcePageAdmission` | Raw |  |
| `admit_source_reconcile` | `SemanticSqlSourceReconciliationAdmission` | Raw |  |
| `admit_source_record` | `SemanticMutationReceipt` | Raw |  |
| `admit_source_replacement` | `SemanticMutationReceipt` | Raw |  |
| `binding` | one of: `SemanticBinding` \| null | Raw |  |
| `claim_stage_leases` | `SemanticStageLeasePage` | Raw |  |
| `complete_generation_stage` | `SemanticMutationReceipt` | Raw |  |
| `complete_sql_source_stage` | `SemanticMutationReceipt` | Raw |  |
| `complete_stage` | `SemanticMutationReceipt` | Raw |  |
| `drop_binding` | `SemanticMutationReceipt` | Raw |  |
| `list_bindings` | `SemanticBindingPage` | Raw |  |
| `live_generation` | integer \| null | Raw |  |
| `refresh_binding` | `SemanticMutationReceipt` | Raw |  |
| `release_stage_lease` | boolean | Raw |  |
| `replay_completed_sql_source_stage` | array \| null | Raw |  |
| `sql_source_manifest` | one of: `SemanticSqlSourceManifest` \| null | Raw |  |
| `stage_status` | `SemanticOutboxStatus` | Raw |  |
| `subscribe_stage_consumer` | boolean | Raw |  |
| `transition_binding` | `SemanticMutationReceipt` | Raw |  |
| `validate_stage_lease` | `SemanticStageIntent` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SemanticIndex`, `contract/schemas/result.ingestion.json#/methods/SemanticIndex`.

## `SemanticSearch`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:semantic` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `n_results` | integer (uint) | yes |  |
| `query_embedding` | array of number (float) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SemanticSearch`, `contract/schemas/result.ingestion.json#/methods/SemanticSearch`.

## `ServedModality`

runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `modality:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `ServedModalityOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `authority` | `ServedModalityAuthority` | Raw |  |
| `capabilities` | `ServedModalityCapabilities` | Json |  |
| `collect_tombstones` | `ServedModalityTombstoneCollection` | Json |  |
| `delete` | `ServedModalityApplyOutcome` | Raw |  |
| `events` | array of `ServedModalityEvent` | Raw |  |
| `ingest` | `ServedModalityApplyOutcome` | Raw |  |
| `ingest_stream` | array of `ServedModalityApplyOutcome` | Raw |  |
| `move_to_cold` | `ServedModalityApplyOutcome` | Raw |  |
| `native_query` | any | Raw | {'reason': 'requested-modality', 'summary': 'the server-declared served-modality body selected by the request'} |
| `query` | any | Raw | {'reason': 'requested-modality', 'summary': 'the server-declared served-modality body selected by the request'} |
| `restore` | `ServedModalityApplyOutcome` | Raw |  |
| `stats` | `ServedModalityStats` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ServedModality`, `contract/schemas/result.ingestion.json#/methods/ServedModality`.

## `SourceIngest`

RF-ADR-009 native ingestion authority: tenant-bound typed Connector Manifest mapping resolution and idempotent raw-CAS admission precede one atomic ChangeEnvelope commit for mapped graph material, provenance, cursor and receipt; unknown mapping, tenant or authority fails closed; exact MCP catalog generation and digest binding makes the generated consumer contract replay-safe

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `source:ingest` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `SourceIngestionRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SourceIngestionReceipt` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SourceIngest`, `contract/schemas/result.ingestion.json#/methods/SourceIngest`.

## `SourceIngestStatus`

read-only authoritative source-partition checkpoint and receipt identity for restart/failover CAS recovery; callers must not substitute local checkpoint authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `source:ingest` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `connector` | string | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SourceIngestStatus` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SourceIngestStatus`, `contract/schemas/result.ingestion.json#/methods/SourceIngestStatus`.

## `Viz`

pure compute: resolves a fresh per-request ColumnStore and returns rendered bytes, no durable write (D-VZ-1 lanes V4/V6)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `viz:render` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `VizOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `CapabilityMatrix` | `VizCapabilityMatrix` | Raw |  |
| `Render` | `VizRenderResponse` | Raw |  |
| `RenderProvenance` | one of: `VizProvenanceRecord` \| null | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Viz`, `contract/schemas/result.ingestion.json#/methods/Viz`.
