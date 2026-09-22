# Coordination API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.coordination.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 38 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AcquireCapacity`

atomic multi-dimensional capacity admission with epoch/fence ownership

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:lease` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CapacityAcquireRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityAcquireResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AcquireCapacity`, `contract/schemas/result.coordination.json#/methods/AcquireCapacity`.

## `AnalyticsJob`

runtime-conditional: Status is a read; Submit/Cancel/Resume commit through the native jobs.redb MutationBatch gateway

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `jobs:write` |
| Mutates | `true` |
| Durability domain | `JobsRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `ANALYTICS_JOB_SCOPE_INCARNATION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `JobOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `Cancel` | `AnalyticsJobRecord` | Json |  |
| `Resume` | `AnalyticsJobRecord` | Json |  |
| `Status` | `AnalyticsJobRecord` | Json |  |
| `Submit` | `AnalyticsJobRecord` | Json |  |
| `WorkerCancel` | `AnalyticsJobRecord` | Json |  |
| `WorkerCheckpoint` | `AnalyticsJobRecord` | Json |  |
| `WorkerClaim` | one of: `WorkerJobClaim` \| null | Json |  |
| `WorkerFail` | `AnalyticsJobRecord` | Json |  |
| `WorkerPublish` | `AnalyticsJobRecord` | Json |  |
| `WorkerRenew` | `JobWorkerLease` | Json |  |
| `WorkerStage` | `AnalyticsJobRecord` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AnalyticsJob`, `contract/schemas/result.coordination.json#/methods/AnalyticsJob`.

## `CancelWorkItem`

pending cancellation never steals an active lease

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `idempotency_key` | string | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `reason_ref` | string \| null | no | Opaque reference to a redacted cancellation reason. The engine never persists a caller-supplied reason body in the control-plane node. |
| `tenant` | string | yes |  |
| `work_item_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemTransition2` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CancelWorkItem`, `contract/schemas/result.coordination.json#/methods/CancelWorkItem`.

## `CapacityStatus`

exact tenant-scoped native capacity status

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:read` |
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
| `request` | `CapacityStatusRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CapacityStatus`, `contract/schemas/result.coordination.json#/methods/CapacityStatus`.

## `CasWorkItemMetadata`

BUG-111: atomic single-field CAS on non-authority scheduling metadata (checkpoint_id/metadata/prio_bucket); status/lease/tenant are fenced but never written

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CasWorkItemMetadataRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CasWorkItemMetadataResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CasWorkItemMetadata`, `contract/schemas/result.coordination.json#/methods/CasWorkItemMetadata`.

## `ClaimNext`

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
| `label` | string | yes |  |
| `updates_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClaimNext`, `contract/schemas/result.coordination.json#/methods/ClaimNext`.

## `ClaimWorkItem`

engine-native tenant/fair WorkItem lease claim

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:claim` |
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
| `request` | `ClaimWorkItemRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClaimWorkItemResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClaimWorkItem`, `contract/schemas/result.coordination.json#/methods/ClaimWorkItem`.

## `CleanupDevelopmentLane`

distinct cleanup WorkItem fence releases retained disk and exclusivity; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:cleanup` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneCleanupCompleteRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneCleanupCompleteResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CleanupDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/CleanupDevelopmentLane`.

## `CommitWorkItemResult`

terminal result references and outbox commit atomically

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `error_ref` | string \| null | no |  |
| `fencing_token` | integer (uint64) | yes |  |
| `idempotency_key` | string | yes |  |
| `lease_epoch` | integer (uint64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `outcome` | string | yes |  |
| `outcome_extension` | one of: `TerminalOutcomeExtension` \| null | no | Optional RF-020/GOC-20 terminal receipts. When present, the mutation compiler lowers the bound receipt nodes to AddNode operations and one run-event outbox intent in this same batch.  Boxed because it dominates the size of this variant and, through it, of EVERY `Method`. `Option<Box<T>>` and `Option<T>` serialize identically, so the wire form -- and the frozen contract -- are unchanged; only the in-memory layout moves. |
| `result_ref` | string \| null | no |  |
| `retryable` | boolean | no |  |
| `tenant` | string | yes |  |
| `work_item_id` | string | yes |  |
| `worker_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemTransition` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CommitWorkItemResult`, `contract/schemas/result.coordination.json#/methods/CommitWorkItemResult`.

## `DecisionEval`

runtime-conditional like AnalyticsJob: status is a read; submit commits a native MutationBatch in jobs.redb carrying the evaluation job row and its receipt. Local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `admin:decision-eval` |
| Mutates | `true` |
| Durability domain | `JobsRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `ANALYTICS_JOB_SCOPE_INCARNATION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `DecisionEvalOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `status` | one of: `DecisionJobRecord` \| null | Raw |  |
| `submit` | `DecisionJobRecord` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecisionEval`, `contract/schemas/result.coordination.json#/methods/DecisionEval`.

## `DecisionFit`

runtime-conditional like AnalyticsJob: status is a read; submit commits a native MutationBatch in jobs.redb carrying the decision job row and its receipt, while the fitted draft is an engine-held Blob CAS body. Local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `admin:decision-fit` |
| Mutates | `true` |
| Durability domain | `JobsRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `ANALYTICS_JOB_SCOPE_INCARNATION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `DecisionFitOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `status` | one of: `DecisionJobRecord` \| null | Raw |  |
| `submit` | `DecisionJobRecord` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecisionFit`, `contract/schemas/result.coordination.json#/methods/DecisionFit`.

## `DeferWorkItem`

fenced lease release schedules retry without consuming an attempt

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `fencing_token` | integer (uint64) | yes |  |
| `idempotency_key` | string | yes |  |
| `lease_epoch` | integer (uint64) | yes |  |
| `next_retry_at_ms` | integer (uint64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `reason_ref` | string \| null | no | Opaque reference only; no free-form reason body is retained. |
| `tenant` | string | yes |  |
| `work_item_id` | string | yes |  |
| `worker_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemDeferral` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DeferWorkItem`, `contract/schemas/result.coordination.json#/methods/DeferWorkItem`.

## `DevelopmentLaneStatus`

bounded tenant-scoped status with maintained counters

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:read` |
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
| `request` | `DevelopmentLaneStatusRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DevelopmentLaneStatus`, `contract/schemas/result.coordination.json#/methods/DevelopmentLaneStatus`.

## `FinishDevelopmentLane`

terminal lifecycle releases active count but retains cleanup charges and identity; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneFinishRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneFinishResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinishDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/FinishDevelopmentLane`.

## `KgDelegate`

authenticated Agent Library pinned delegation lowered to native WorkItem admission

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:delegate` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `KgDelegateRequest` | yes | Boxed: a pinned delegation request carries the whole Agent Library entry reference and would otherwise set the size of EVERY `Method`. `Box` is transparent to serde, so the wire form is unchanged. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KgDelegateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KgDelegate`, `contract/schemas/result.coordination.json#/methods/KgDelegate`.

## `MintWorkItemClaimCapability`

opaque native capability is retained in a private ledger and never projected

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:claim-capability` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `WorkItemClaimCapabilityMintRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemClaimCapabilityResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MintWorkItemClaimCapability`, `contract/schemas/result.coordination.json#/methods/MintWorkItemClaimCapability`.

## `ObserveDevelopmentLane`

monotonic retained-footprint observation replaces the prior native charge; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneObserveRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneObserveResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ObserveDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/ObserveDevelopmentLane`.

## `QueryDevelopmentLane`

linearizable exact lane hold/tombstone read

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:read` |
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
| `request` | `DevelopmentLaneQueryRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneQueryResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/QueryDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/QueryDevelopmentLane`.

## `QueryWorkItemReservation`

linearizable exact native authority read

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:read` |
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
| `request` | `ResourceReservationStatusRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceReservationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/QueryWorkItemReservation`, `contract/schemas/result.coordination.json#/methods/QueryWorkItemReservation`.

## `ReclaimExpiredCapacity`

bounded expiry reclaim with native aggregate accounting

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:lease` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CapacityReclaimRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityReclaimResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReclaimExpiredCapacity`, `contract/schemas/result.coordination.json#/methods/ReclaimExpiredCapacity`.

## `ReclaimWorkItemResources`

controller-only expiry/supersession reclaim with retained tombstone

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `ResourceReservationRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceReservationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReclaimWorkItemResources`, `contract/schemas/result.coordination.json#/methods/ReclaimWorkItemResources`.

## `ReconcileCapacity`

bounded native cells/leases reconciliation page

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:read` |
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
| `request` | `CapacityStatusRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReconcileCapacity`, `contract/schemas/result.coordination.json#/methods/ReconcileCapacity`.

## `ReleaseCapacity`

bounded all-or-nothing lease release

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:lease` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CapacityLeaseMutationRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityMutationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReleaseCapacity`, `contract/schemas/result.coordination.json#/methods/ReleaseCapacity`.

## `ReleaseWorkItemResources`

controller-only lifecycle release with retained tombstone

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `ResourceReservationRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceReservationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReleaseWorkItemResources`, `contract/schemas/result.coordination.json#/methods/ReleaseWorkItemResources`.

## `RenewCapacity`

bounded all-or-nothing lease renewal

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:lease` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CapacityLeaseMutationRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityMutationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RenewCapacity`, `contract/schemas/result.coordination.json#/methods/RenewCapacity`.

## `RenewDevelopmentLane`

in-place O(1) hold renewal bound to the current WorkItem lease; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneRenewRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneRenewResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RenewDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/RenewDevelopmentLane`.

## `RenewWorkItemLease`

lease epoch and fencing token are validated atomically

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `fencing_token` | integer (uint64) | yes |  |
| `lease_epoch` | integer (uint64) | yes |  |
| `lease_ms` | integer (uint64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `tenant` | string | yes |  |
| `work_item_id` | string | yes |  |
| `worker_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemLeaseRenewal` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RenewWorkItemLease`, `contract/schemas/result.coordination.json#/methods/RenewWorkItemLease`.

## `ReserveDevelopmentLane`

controller-only atomic branch/worktree uniqueness and multi-scope quota hold; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneReserveRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReserveDevelopmentLane`, `contract/schemas/result.coordination.json#/methods/ReserveDevelopmentLane`.

## `ReserveWorkItemResources`

controller-only atomic host admission and WorkItem fence validation

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:reserve` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `ResourceReservationRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceReservationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReserveWorkItemResources`, `contract/schemas/result.coordination.json#/methods/ReserveWorkItemResources`.

## `ResourceReservationStatus`

bounded linearizable reconciliation read

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:read` |
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
| `request` | `ResourceReservationStatusRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceReservationStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ResourceReservationStatus`, `contract/schemas/result.coordination.json#/methods/ResourceReservationStatus`.

## `ResourceStatsPage`

bounded ACL-filtered keyset page; summary suppresses detail arrays

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `service:control` |
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
| `cursor` | string \| null | no |  |
| `limit` | integer (uint) | no |  |
| `summary` | boolean | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceSnapshot` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ResourceStatsPage`, `contract/schemas/result.coordination.json#/methods/ResourceStatsPage`.

## `Statechart`

runtime-conditional: GetState/List are reads; Define/Instantiate/SendEvent commit to the native statecharts.redb store (CONCEPT:INT-P2-2)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `statechart:write` |
| Mutates | `true` |
| Durability domain | `StatechartRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `INSTANCE_MUTATION_INCARNATION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `StatechartOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `Define` | `StatechartDefinitionId` | Json |  |
| `GetState` | `StatechartInstance` | Json |  |
| `Instantiate` | `StatechartInstance` | Json |  |
| `List` | `StatechartInstanceList` | Json |  |
| `SendEvent` | `StatechartEventOutcome` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Statechart`, `contract/schemas/result.coordination.json#/methods/Statechart`.

## `SubmitWorkItem`

native tenant-scoped WorkItem command-log admission and outbox commit

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:submit` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `SubmitWorkItemRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SubmitWorkItemResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SubmitWorkItem`, `contract/schemas/result.coordination.json#/methods/SubmitWorkItem`.

## `SubmitWorkItems`

bounded all-or-nothing WorkItem admission batch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:submit` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `SubmitWorkItemsRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SubmitWorkItemsResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SubmitWorkItems`, `contract/schemas/result.coordination.json#/methods/SubmitWorkItems`.

## `UpdateCapacityCell`

controller epoch CAS for resource dimension/capacity policy

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `capacity:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `CapacityCellUpdateRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CapacityCellUpdateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UpdateCapacityCell`, `contract/schemas/result.coordination.json#/methods/UpdateCapacityCell`.

## `UpdateDevelopmentLaneQuota`

controller/admin-only monotonic server-owned quota policy with numeric expected_policy_revision CAS; now_ms is authority-normalized

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `lane:quota` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `DevelopmentLaneQuotaUpdateRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DevelopmentLaneQuotaUpdateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UpdateDevelopmentLaneQuota`, `contract/schemas/result.coordination.json#/methods/UpdateDevelopmentLaneQuota`.

## `UpdateResourceHost`

controller-only monotonic host telemetry update

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `resource:host` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
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
| `request` | `ResourceHostUpdateRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResourceHostUpdateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UpdateResourceHost`, `contract/schemas/result.coordination.json#/methods/UpdateResourceHost`.

## `VerifyWorkItemClaimCapability`

linearizable live-lease check precedes private capability lookup

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `work:claim-capability` |
| Mutates | `false` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `WorkItemClaimCapabilityVerifyRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WorkItemClaimCapabilityResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/VerifyWorkItemClaimCapability`, `contract/schemas/result.coordination.json#/methods/VerifyWorkItemClaimCapability`.
