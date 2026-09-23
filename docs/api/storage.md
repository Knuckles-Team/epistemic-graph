# Storage API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.storage.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 39 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AgentAssemble`

RF-ADR-010 A1. Reads ONE tenant-bound agent_library.redb snapshot and proves an agent graph against it; commits nothing. The record it answers with is durable only if the caller then sends DecisionCommit

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:assemble-read` |
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
| `request` | `AssemblyRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AssemblyResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AgentAssemble`, `contract/schemas/result.storage.json#/methods/AgentAssemble`.

## `AgentComponent`

RF-ADR-008 layer 1. Runtime-conditional like AgentLibrary/AgentGraph: Current/History/Status/Search are authenticated tenant-bound read snapshots; Publish/Retire atomically commit native component revisions, action provenance, replay receipts and outbox through ControlRedb into the SAME agent_library.redb owner

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:component-write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `AgentComponentOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `content` | `AgentComponentContentResult` | Raw |  |
| `current` | one of: `AgentComponentEntry` \| null | Raw |  |
| `history` | array of `AgentComponentEntry` | Raw |  |
| `publish` | `AgentComponentCommittedResult` | Raw |  |
| `retire` | `AgentComponentCommittedResult` | Raw |  |
| `search` | `AgentComponentSearchPage` | Raw |  |
| `status` | one of: `AgentComponentCommittedResult` \| null | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AgentComponent`, `contract/schemas/result.storage.json#/methods/AgentComponent`.

## `AgentGraph`

RF-ADR-008. Runtime-conditional exactly like AgentLibrary: Current/History/Status are authenticated tenant-bound read snapshots; Publish/Retire atomically commit native graph revisions, action provenance, replay receipts, and outbox through ControlRedb into the SAME agent_library.redb owner

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:graph-write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `AgentGraphOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `current` | one of: `AgentGraphEntry` \| null | Raw |  |
| `history` | array of `AgentGraphEntry` | Raw |  |
| `publish` | `AgentGraphCommittedResult` | Raw |  |
| `retire` | `AgentGraphCommittedResult` | Raw |  |
| `status` | one of: `AgentGraphCommittedResult` \| null | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AgentGraph`, `contract/schemas/result.storage.json#/methods/AgentGraph`.

## `AgentLibrary`

runtime-conditional: Current/History/Status are authenticated tenant-bound read snapshots; Publish/Retire atomically commit native revisions, action provenance, replay receipts, and outbox through ControlRedb

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:library-write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `AgentLibraryOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `current` | one of: `AgentLibraryEntry` \| null | Raw |  |
| `history` | array of `AgentLibraryEntry` | Raw |  |
| `publish` | `AgentLibraryWriteResult` | Raw |  |
| `retire` | `AgentLibraryWriteResult` | Raw |  |
| `status` | one of: `AgentLibraryWriteResult` \| null | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AgentLibrary`, `contract/schemas/result.storage.json#/methods/AgentLibrary`.

## `AgentTemplate`

RF-ADR-008 item C. Runtime-conditional like the three layers beside it: Current/History/Status/Instantiate are authenticated tenant-bound read snapshots (Instantiate binds parameters and returns a draft, committing nothing); Publish/Retire atomically commit native template revisions, action provenance, replay receipts and outbox through ControlRedb into the SAME agent_library.redb owner. Its own authz action because publishing a parameterized FAMILY of agents is a distinct privilege from publishing one

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:template-write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `AgentTemplateOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `current` | one of: `AgentTemplateEntry` \| null | Raw |  |
| `history` | array of `AgentTemplateEntry` | Raw |  |
| `instantiate` | `AgentLibraryEntryDraft` | Raw |  |
| `publish` | `AgentTemplateCommittedResult` | Raw |  |
| `retire` | `AgentTemplateCommittedResult` | Raw |  |
| `status` | one of: `AgentTemplateCommittedResult` \| null | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AgentTemplate`, `contract/schemas/result.storage.json#/methods/AgentTemplate`.

## `ApplyLedger`

state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ledger:write` |
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
| `transactions` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ApplyLedger`, `contract/schemas/result.storage.json#/methods/ApplyLedger`.

## `Backup`

reads a consistent snapshot out to a bundle; does not mutate the live graph

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:backup` |
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
| `destination` | string | yes |  |
| `label` | string \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `BackupReceipt` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Backup`, `contract/schemas/result.storage.json#/methods/Backup`.

## `BlobBegin`

multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:write` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `chunk_size` | integer (uint32) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobBegin`, `contract/schemas/result.storage.json#/methods/BlobBegin`.

## `BlobChunkGet`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:read` |
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
| `cursor` | integer (uint64) | yes |  |
| `idx` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'caller-bytes', 'summary': 'opaque bytes the caller wrote or a caller-supplied program produced'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobChunkGet`, `contract/schemas/result.storage.json#/methods/BlobChunkGet`.

## `BlobChunkPut`

durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:write` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cursor` | integer (uint64) | yes |  |
| `data` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobChunkPut`, `contract/schemas/result.storage.json#/methods/BlobChunkPut`.

## `BlobCommit`

multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:write` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cursor` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobCommit`, `contract/schemas/result.storage.json#/methods/BlobCommit`.

## `BlobFetchBegin`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:read` |
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
| `digest` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobFetchBegin`, `contract/schemas/result.storage.json#/methods/BlobFetchBegin`.

## `BlobFetchEnd`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:read` |
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
| `cursor` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobFetchEnd`, `contract/schemas/result.storage.json#/methods/BlobFetchEnd`.

## `BlobGc`

durable via blob.redb

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:admin` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobGc`, `contract/schemas/result.storage.json#/methods/BlobGc`.

## `BlobRef`

X6 (fix/eg-blob-cas-hardening-20260917): holder-scoped named reference (digest, owner scope), not a bare counter -- a retry or replay of the same reference is one holder row, never a second count (holders.rs's own module doc and handle_blob_ref_op's doc comment). Idempotent as of the 2.27.x blob CAS hardening; durable via blob.redb.

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:write` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
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
| `digest` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobRef`, `contract/schemas/result.storage.json#/methods/BlobRef`.

## `BlobUnref`

X6: releasing an already-released holder returns changed: false rather than erroring or underflowing (HolderOutcome's own doc comment). Idempotent as of the 2.27.x blob CAS hardening; durable via blob.redb.

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `blob:write` |
| Mutates | `true` |
| Durability domain | `BlobRedb` |
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
| `digest` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BlobUnref`, `contract/schemas/result.storage.json#/methods/BlobUnref`.

## `ClearLedger`

state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ledger:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClearLedger`, `contract/schemas/result.storage.json#/methods/ClearLedger`.

## `ConnectorPack`

RF-ADR-009 A2 plus RF-021 durable catalog reconciliation; runtime-conditional: status is an authenticated tenant-bound read snapshot exposing the exact MCP catalog generation/digest binding; import atomically commits resources/templates, component revisions, CAS body holders, provenance, receipt and outbox. Bind/unbind/retire/reproject/reconcile_bodies and a mass-withdrawal import need admin:connector-pack; local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:pack-control` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `ConnectorPackOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `bind` | `ConnectorPackBindingResult` | Raw |  |
| `import` | `PackImportResult` | Raw |  |
| `reconcile_bodies` | `PackBodyReconcileReport` | Raw |  |
| `reproject` | `PackImportReceipt` | Raw |  |
| `retire` | `PackRetireResult` | Raw |  |
| `status` | `ConnectorPackStatus` | Raw |  |
| `unbind` | `ConnectorPackBindingResult` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ConnectorPack`, `contract/schemas/result.storage.json#/methods/ConnectorPack`.

## `DecisionCommit`

native MutationBatch in agent_library.redb: one DecisionRecord component revision, its receipt and outbox in one WTX after re-derivation and a catalog compare-and-set; local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `agent:decision-write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `DecisionCommitRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DecisionCommitResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecisionCommit`, `contract/schemas/result.storage.json#/methods/DecisionCommit`.

## `ExportSqliteFile`

operator-provisioned transfer root; logical filenames only

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:sqlite-file` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `path` | string | yes |  |
| `tables` | array of string | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SqliteExportReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExportSqliteFile`, `contract/schemas/result.storage.json#/methods/ExportSqliteFile`.

## `FromMsgpack`

state-backed MutationBatch commits the imported authoritative image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:admin` |
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
| `msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FromMsgpack`, `contract/schemas/result.storage.json#/methods/FromMsgpack`.

## `ImportSqliteFile`

native SQL-catalog MutationBatch; logical transfer name is excluded from the durable receipt

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:sqlite-file` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `path` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SqliteImportReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ImportSqliteFile`, `contract/schemas/result.storage.json#/methods/ImportSqliteFile`.

## `KvCas`

durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `kv:write` |
| Mutates | `true` |
| Durability domain | `KvRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `expected` | array of integer (uint8) | no |  |
| `key` | string | yes |  |
| `namespace` | string | yes |  |
| `new` | array of integer (uint8) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KvCas`, `contract/schemas/result.storage.json#/methods/KvCas`.

## `KvDelete`

durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `kv:write` |
| Mutates | `true` |
| Durability domain | `KvRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `key` | string | yes |  |
| `namespace` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KvDelete`, `contract/schemas/result.storage.json#/methods/KvDelete`.

## `KvGet`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `kv:read` |
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
| `key` | string | yes |  |
| `namespace` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | RawOrNull | {'reason': 'caller-bytes', 'summary': 'opaque bytes the caller wrote or a caller-supplied program produced'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KvGet`, `contract/schemas/result.storage.json#/methods/KvGet`.

## `KvPut`

durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `kv:write` |
| Mutates | `true` |
| Durability domain | `KvRedb` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `key` | string | yes |  |
| `namespace` | string | yes |  |
| `value` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KvPut`, `contract/schemas/result.storage.json#/methods/KvPut`.

## `KvScan`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `kv:read` |
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
| `limit` | integer (uint) | yes |  |
| `namespace` | string | yes |  |
| `prefix` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'caller-bytes', 'summary': 'opaque bytes the caller wrote or a caller-supplied program produced'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KvScan`, `contract/schemas/result.storage.json#/methods/KvScan`.

## `Restore`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:backup` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `source` | string | yes |  |
| `target_shards` | integer (uint) | yes | Required current target layout. Setting this to a different value from the bundle proves restore-time migration rather than silently preserving K. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RestoreReceipt` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Restore`, `contract/schemas/result.storage.json#/methods/Restore`.

## `SqlSourceBatch`

native SQL-catalog MutationBatch: typed source rows, provider cursor, committed source epoch, terminal result, replay/idempotency and outbox in one WTX; self-routes before graph dispatch; local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:sql` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `batch` | `SqlSourceBatchRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SqlSourceBatchResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SqlSourceBatch`, `contract/schemas/result.storage.json#/methods/SqlSourceBatch`.

## `ToMsgpack`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:read` |
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
| `result` | array of integer (uint8) | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ToMsgpack`, `contract/schemas/result.storage.json#/methods/ToMsgpack`.

## `TsAppend`

graph ACL + placement policy precede the tenant/graph/series-scoped series.redb write

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:write` |
| Mutates | `true` |
| Durability domain | `SeriesRedb` |
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
| `bucket_ns` | integer (uint64) | yes | Bucket/time-partition width in nanoseconds (series-creation parameter). |
| `field_names` | array of string | no | Optional field names (series-creation metadata). |
| `n_fields` | integer (uint) | yes | Field count per point (1 for a scalar series, N for OHLCV…). Used only when the series is NEW; an existing series' stored schema wins. |
| `points_msgpack` | array of integer (uint8) | yes | MessagePack `Vec<(i64, Vec<f64>)>` — the batch of points (one round-trip). |
| `series_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsAppend`, `contract/schemas/result.storage.json#/methods/TsAppend`.

## `TsAsofJoin`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:read` |
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
| `left_ts_msgpack` | array of integer (uint8) | yes | MessagePack `Vec<i64>` — the left event timestamps (ns). |
| `series_id` | string | yes | The "right" series each left event is joined to by nearest-prior ts. |
| `tolerance` | integer (int64) | no | Optional tolerance (ns); a match older than this is dropped (`None` = unbounded). `-1` encodes `None` over the wire. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsAsofJoin`, `contract/schemas/result.storage.json#/methods/TsAsofJoin`.

## `TsDeleteSeries`

content-idempotent unlike TsAppend: re-deleting an already-gone series is a safe no-op (see SeriesStore::delete_series)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:write` |
| Mutates | `true` |
| Durability domain | `SeriesRedb` |
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
| `series_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsDeleteSeries`, `contract/schemas/result.storage.json#/methods/TsDeleteSeries`.

## `TsEvict`

content-idempotent unlike TsAppend: re-evicting an already-past cutoff is a safe no-op (see SeriesStore::evict_before)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:write` |
| Mutates | `true` |
| Durability domain | `SeriesRedb` |
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
| `cutoff` | integer (int64) | yes |  |
| `series_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsEvict`, `contract/schemas/result.storage.json#/methods/TsEvict`.

## `TsGapFill`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:read` |
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
| `from` | integer (int64) | yes |  |
| `series_id` | string | yes |  |
| `step` | integer (int64) | yes | Grid step (ns) for the LOCF densification. |
| `to` | integer (int64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsGapFill`, `contract/schemas/result.storage.json#/methods/TsGapFill`.

## `TsListSeries`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:read` |
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
| `result` | array of string | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsListSeries`, `contract/schemas/result.storage.json#/methods/TsListSeries`.

## `TsRange`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:read` |
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
| `from` | integer (int64) | yes | Inclusive lower / exclusive upper ts bound (ns). |
| `series_id` | string | yes |  |
| `to` | integer (int64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsRange`, `contract/schemas/result.storage.json#/methods/TsRange`.

## `TsWindow`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `timeseries:read` |
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
| `agg` | string | yes | Aggregate function: one of first/last/min/max/mean/sum/count. |
| `from` | integer (int64) | yes |  |
| `series_id` | string | yes |  |
| `to` | integer (int64) | yes |  |
| `width` | integer (int64) | yes | Window width (ns) for the bucketed aggregate. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TsWindow`, `contract/schemas/result.storage.json#/methods/TsWindow`.

## `WriteBack`

RF-ADR-009 D18, runtime-conditional: create/record operations append tenant-bound change-set and receipt rows through the existing Agent Library ControlRedb mutation kernel; get/receipts are authenticated snapshots. EG records authorization and source observations and never calls vendor APIs

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `connector:write-back` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `WriteBackOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `create` | `SourceChangeSet` | Raw |  |
| `get` | one of: `SourceChangeSet` \| null | Raw |  |
| `receipts` | `WriteBackReceiptPage` | Raw |  |
| `record_attempt` | `WriteBackReceipt` | Raw |  |
| `record_reconciliation` | `ReconciliationReceipt` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/WriteBack`, `contract/schemas/result.storage.json#/methods/WriteBack`.
