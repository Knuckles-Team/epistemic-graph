# Transactions API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.transactions.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 21 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `ApplyChangeEnvelope`

Engine-native object/material/governance/version/cursor/outbox commit; verified context is mandatory

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:write` |
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

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `envelope` | `ChangeEnvelope` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ChangeEnvelopeApplied` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ApplyChangeEnvelope`, `contract/schemas/result.transactions.json#/methods/ApplyChangeEnvelope`.

## `ApplyChangeEnvelopes`

Batch envelope coordinator: one coalesced graph transaction per shard-partition; same policy class as ApplyChangeEnvelope

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:write` |
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

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `envelopes` | array of `ChangeEnvelope` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ChangeEnvelopeBatch` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ApplyChangeEnvelopes`, `contract/schemas/result.transactions.json#/methods/ApplyChangeEnvelopes`.

## `ApplyMultisigMutation`

threshold validation translates into the graph MutationBatch gateway

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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
| `mutation_type` | string | yes |  |
| `query` | string | yes |  |
| `signatures` | array of string | yes |  |
| `threshold` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SparqlUpdateReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ApplyMultisigMutation`, `contract/schemas/result.transactions.json#/methods/ApplyMultisigMutation`.

## `BatchUpdate`

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
| `operations_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `BatchUpdateReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BatchUpdate`, `contract/schemas/result.transactions.json#/methods/BatchUpdate`.

## `BeginTxn`

encrypted Raft-native transaction staging authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:control` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no | Optional explicit target graph. An explicit `None` selects the request envelope's `graph`. |
| `isolation` | string \| null | no | Reserved isolation hint; only snapshot isolation is implemented. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BeginTxn`, `contract/schemas/result.transactions.json#/methods/BeginTxn`.

## `Commit`

named parent receipt plus atomic graph/cross-modal child batches

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:control` |
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
| `idempotency_key` | string \| null | no |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CommitOutcome` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Commit`, `contract/schemas/result.transactions.json#/methods/Commit`.

## `MultiGraphBatchUpdate`

durable parent coordinator with per-graph MutationBatch children

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
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
| `batches_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `MultiGraphBatchReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MultiGraphBatchUpdate`, `contract/schemas/result.transactions.json#/methods/MultiGraphBatchUpdate`.

## `MutationOutbox`

X10, runtime-conditional: status and dead_letters are reads; rewind resets one consumer's durable cursor through bounded eg-transaction transactions, so it is a saga rather than one atomic write. Local-only authority, refused in clustered mode

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `admin:outbox` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `RBAC_SCOPE_INCARNATION`, `CONSENSUS_TRANSACTION_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `MutationOutboxOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `dead_letters` | `OutboxDeadLetterPage` | Raw |  |
| `rewind` | `OutboxRewindReceipt` | Raw |  |
| `status` | `MutationOutboxStatusView` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MutationOutbox`, `contract/schemas/result.transactions.json#/methods/MutationOutbox`.

## `Rollback`

encrypted Raft-native transaction staging removal

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:control` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Rollback`, `contract/schemas/result.transactions.json#/methods/Rollback`.

## `TxnAddEdge`

encrypted Raft-native staging; Commit owns graph publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no |  |
| `properties_msgpack` | array of integer (uint8) | yes |  |
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnAddEdge`, `contract/schemas/result.transactions.json#/methods/TxnAddEdge`.

## `TxnAddEmbedding`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `embedding` | array of number (float) | yes |  |
| `graph` | string \| null | no |  |
| `node_id` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnAddEmbedding`, `contract/schemas/result.transactions.json#/methods/TxnAddEmbedding`.

## `TxnAddMeasurement`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no |  |
| `points` | array of integer (uint8) | yes | MessagePack `Vec<(i64, Vec<f64>)>` — the batch of points (one round-trip). |
| `series` | string | yes | Target series id the points belong to. |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnAddMeasurement`, `contract/schemas/result.transactions.json#/methods/TxnAddMeasurement`.

## `TxnAddNode`

encrypted Raft-native staging; Commit owns graph publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no | Optional target graph for THIS staged op (CONCEPT:EG-KG.txn.routes-cross-shard-txn — multi-graph txn). An explicit `None` selects the txn's default graph. A staged op naming a graph that resolves to a DIFFERENT Raft group makes the txn CROSS-SHARD, routed through 2PC at commit. |
| `node_id` | string | yes |  |
| `properties_msgpack` | array of integer (uint8) | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnAddNode`, `contract/schemas/result.transactions.json#/methods/TxnAddNode`.

## `TxnAxiom`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no |  |
| `turtle` | string | yes | OWL axioms as Turtle to stage into the txn. |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnAxiom`, `contract/schemas/result.transactions.json#/methods/TxnAxiom`.

## `TxnBlobRef`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `digest` | string | yes |  |
| `graph` | string \| null | no |  |
| `node_id` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnBlobRef`, `contract/schemas/result.transactions.json#/methods/TxnBlobRef`.

## `TxnCas`

encrypted Raft-native staging; Commit owns graph publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
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
| `conditions_msgpack` | array of integer (uint8) | yes |  |
| `graph` | string \| null | no |  |
| `node_id` | string | yes |  |
| `txn_id` | string | yes |  |
| `updates_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnCas`, `contract/schemas/result.transactions.json#/methods/TxnCas`.

## `TxnConstruct`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no |  |
| `sparql` | string | yes | SPARQL CONSTRUCT query whose triples are staged into the txn. |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnConstruct`, `contract/schemas/result.transactions.json#/methods/TxnConstruct`.

## `TxnMaterializeBelief`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `graph` | string \| null | no |  |
| `node_id` | string | yes | The node whose propagated belief is computed and written back. |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `BeliefMaterialization` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnMaterializeBelief`, `contract/schemas/result.transactions.json#/methods/TxnMaterializeBelief`.

## `TxnPlanWriteback`

encrypted Raft-native cross-modal staging

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `anchor_id` | string | yes | The edge SOURCE every materialized edge is anchored to. |
| `graph` | string \| null | no |  |
| `plan` | `Plan` | yes | The plan whose result `RowSet` is materialized as edges. |
| `relationship` | string | yes | The `relationship` property every materialized edge carries. |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnPlanWriteback`, `contract/schemas/result.transactions.json#/methods/TxnPlanWriteback`.

## `TxnRemoveEdge`

encrypted Raft-native staging; Commit owns graph publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
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
| `graph` | string \| null | no |  |
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnRemoveEdge`, `contract/schemas/result.transactions.json#/methods/TxnRemoveEdge`.

## `TxnRemoveNode`

encrypted Raft-native staging; Commit owns graph publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:write` |
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
| `graph` | string \| null | no |  |
| `node_id` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnRemoveNode`, `contract/schemas/result.transactions.json#/methods/TxnRemoveNode`.
