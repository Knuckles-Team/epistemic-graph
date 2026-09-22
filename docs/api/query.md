# Query API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.query.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 29 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `CausalCounterfactual`

EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `actual` | object | yes |  |
| `do_values` | object | yes |  |
| `variables` | array of `StructuralEquationWire` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CausalCounterfactualResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CausalCounterfactual`, `contract/schemas/result.query.json#/methods/CausalCounterfactual`.

## `CausalEstimate`

EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `do_values` | object | yes |  |
| `mode` | `CausalQueryModeWire` (enum: `Intervene`, `Observe`) | yes |  |
| `variables` | array of `StructuralEquationWire` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CausalEstimateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CausalEstimate`, `contract/schemas/result.query.json#/methods/CausalEstimate`.

## `CypherQuery`

runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:cypher` |
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
| `mode` | `CypherMode` (enum: `read`, `write`) | yes | Exact requested execution authority; no implicit or inferred mode. |
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CypherQuery`, `contract/schemas/result.query.json#/methods/CypherQuery`.

## `Decide`

RF-ADR-010 DL-2. Evaluate-only: scores library or RLS-filtered graph candidates under a pinned feature schema and head and answers a batch of records; it commits none of them

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `query:decide` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `DecideRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DecisionBatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Decide`, `contract/schemas/result.query.json#/methods/Decide`.

## `EpistemicStatus`

L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EpistemicStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EpistemicStatus`, `contract/schemas/result.query.json#/methods/EpistemicStatus`.

## `ExplainBelief`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `disclosure_level` | one of: `DisclosureLevelWire` (enum: `Full`, `Skeleton`, `ExistenceOnly`) \| null | no |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainBeliefResponse` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainBelief`, `contract/schemas/result.query.json#/methods/ExplainBelief`.

## `ExplainEvidence`

CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainEvidenceResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainEvidence`, `contract/schemas/result.query.json#/methods/ExplainEvidence`.

## `ExplainPlan`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainPlanResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainPlan`, `contract/schemas/result.query.json#/methods/ExplainPlan`.

## `ExplainPolicy`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainPolicyResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainPolicy`, `contract/schemas/result.query.json#/methods/ExplainPolicy`.

## `ExplainProvenance`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EvidenceBundle` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainProvenance`, `contract/schemas/result.query.json#/methods/ExplainProvenance`.

## `ExplainProvenanceByIds`

CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EvidenceBundle` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainProvenanceByIds`, `contract/schemas/result.query.json#/methods/ExplainProvenanceByIds`.

## `GetChangeCursor`

Typed source cursors are tenant/graph/partition scoped

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
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
| `partition` | string | no |  |
| `source` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ChangeCursor` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChangeCursor`, `contract/schemas/result.query.json#/methods/GetChangeCursor`.

## `GetChangeEnvelope`

Verified tenant-scoped reconciliation read

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
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
| `envelope_id` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ChangeEnvelopeRecord` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChangeEnvelope`, `contract/schemas/result.query.json#/methods/GetChangeEnvelope`.

## `GetContentVersion`

Typed content versions are never compared lexically

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
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
| `object_id` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ContentVersion` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetContentVersion`, `contract/schemas/result.query.json#/methods/GetContentVersion`.

## `GetContextView`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:read` |
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
| `agent_id` | string | yes |  |
| `max_tokens` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ContextView` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetContextView`, `contract/schemas/result.query.json#/methods/GetContextView`.

## `GraphQl`

runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:graphql` |
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
| `query` | string | yes |  |
| `variables` | any | no | Optional GraphQL `$variables` — a JSON object bound at execution (CONCEPT:EG-KG.query.fragments-variables-directives variables, wired through the wire path as an EG-064 follow-up). The handler binds these via `execute_with_variables` (`@skip`/`@include` + `$var` args). `None` is encoded explicitly and means an empty binding. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphQl`, `contract/schemas/result.query.json#/methods/GraphQl`.

## `KnowledgeStream`

one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:stream` |
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
| `request` | `KnowledgeStreamRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KnowledgeStreamBatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KnowledgeStream`, `contract/schemas/result.query.json#/methods/KnowledgeStream`.

## `MaterializationStatus`

read-only status from the durable per-graph incremental reasoning authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `MaterializationStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MaterializationStatus`, `contract/schemas/result.query.json#/methods/MaterializationStatus`.

## `NlQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:nl` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `graph` | string | no |  |
| `text` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/NlQuery`, `contract/schemas/result.query.json#/methods/NlQuery`.

## `RankByProvenance`

EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `candidates` | array of `RetrievalCandidateWire` | yes |  |
| `weights` | `RankWeightsWire` | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RankByProvenanceResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RankByProvenance`, `contract/schemas/result.query.json#/methods/RankByProvenance`.

## `RecomputeMaterialization`

fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `reasoning:write` |
| Mutates | `true` |
| Durability domain | `ReasoningProjection` |
| Idempotent | `false` |
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
| `derived_id` | string | yes |  |
| `expected_source_graph_version` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RecomputeMaterializationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RecomputeMaterialization`, `contract/schemas/result.query.json#/methods/RecomputeMaterialization`.

## `ResolveConflict`

EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `node_ids` | array of string | yes |  |
| `semantics` | string | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResolveConflictResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ResolveConflict`, `contract/schemas/result.query.json#/methods/ResolveConflict`.

## `Sql`

runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:sql` |
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
| `params_msgpack` | array of integer (uint8) | no |  |
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Sql`, `contract/schemas/result.query.json#/methods/Sql`.

## `StaleMaterializations`

bulk opaque stale references from the durable per-graph incremental reasoning authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `StaleMaterializationsResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StaleMaterializations`, `contract/schemas/result.query.json#/methods/StaleMaterializations`.

## `TxnUnifiedQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnUnifiedQuery`, `contract/schemas/result.query.json#/methods/TxnUnifiedQuery`.

## `TxnUnifiedQueryText`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `text` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnUnifiedQueryText`, `contract/schemas/result.query.json#/methods/TxnUnifiedQueryText`.

## `UnifiedQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:unified` |
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
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnifiedQuery`, `contract/schemas/result.query.json#/methods/UnifiedQuery`.

## `UnifiedQueryText`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:unified` |
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
| `text` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnifiedQueryText`, `contract/schemas/result.query.json#/methods/UnifiedQueryText`.

## `WhatChanged`

L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
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
| `tx_from` | integer (uint64) | yes |  |
| `tx_to` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WhatChangedResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/WhatChanged`, `contract/schemas/result.query.json#/methods/WhatChanged`.
