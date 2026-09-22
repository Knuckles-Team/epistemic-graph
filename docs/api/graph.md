# Graph API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.graph.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 64 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AddEdge`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:write` |
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
| `properties_msgpack` | array of integer (uint8) | yes |  |
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AddEdge`, `contract/schemas/result.graph.json#/methods/AddEdge`.

## `AddNode`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
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
| `node_id` | string | yes |  |
| `properties_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AddNode`, `contract/schemas/result.graph.json#/methods/AddNode`.

## `AddSceneObject`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `scene:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `parent` | string \| null | no |  |
| `pose_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AddSceneObject`, `contract/schemas/result.graph.json#/methods/AddSceneObject`.

## `AddTriples`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `rdf:write` |
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
| `ntriples` | string | no | N-Triples document (empty ⇒ use `turtle`). |
| `turtle` | string | no | Turtle document (empty ⇒ use `ntriples`). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `LoadReport` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AddTriples`, `contract/schemas/result.graph.json#/methods/AddTriples`.

## `AppendStep`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `action_msgpack` | array of integer (uint8) | yes |  |
| `next_state_ref` | string \| null | no |  |
| `reward` | number (double) | yes |  |
| `state_ref` | string \| null | no |  |
| `t` | integer (uint64) | yes |  |
| `traj_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AppendStep`, `contract/schemas/result.graph.json#/methods/AppendStep`.

## `ApplyMutation`

state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:write` |
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
| `event_type` | string | yes |  |
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SparqlUpdateReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ApplyMutation`, `contract/schemas/result.graph.json#/methods/ApplyMutation`.

## `BestTrajectory`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:read` |
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
| `gamma` | number (double) | yes |  |
| `traj_ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BestTrajectory`, `contract/schemas/result.graph.json#/methods/BestTrajectory`.

## `ClearGraph`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:admin` |
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

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClearGraph`, `contract/schemas/result.graph.json#/methods/ClearGraph`.

## `CompactNodesByType`

state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `node:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_type` | string | yes |  |
| `threshold` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CompactNodesResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CompactNodesByType`, `contract/schemas/result.graph.json#/methods/CompactNodesByType`.

## `CompareAndSetNodeFields`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
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
| `conditions_msgpack` | array of integer (uint8) | yes |  |
| `node_id` | string | yes |  |
| `updates_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CompareAndSetNodeFields`, `contract/schemas/result.graph.json#/methods/CompareAndSetNodeFields`.

## `Consolidate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `episodic_ids` | array of string | yes |  |
| `semantic_props_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Consolidate`, `contract/schemas/result.graph.json#/methods/Consolidate`.

## `CreateNodeIfAbsent`

atomic create returns true only to the inserting writer, so its result is not cross-request cacheable

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
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
| `node_id` | string | yes |  |
| `properties_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CreateNodeIfAbsent`, `contract/schemas/result.graph.json#/methods/CreateNodeIfAbsent`.

## `CreateSummaryNode`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `child_ids` | array of string | yes |  |
| `level` | integer (uint32) | yes |  |
| `props_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CreateSummaryNode`, `contract/schemas/result.graph.json#/methods/CreateSummaryNode`.

## `DecayMemories`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `half_life_ms` | integer (uint64) | yes |  |
| `ids` | array of string | yes |  |
| `now_ms` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecayMemories`, `contract/schemas/result.graph.json#/methods/DecayMemories`.

## `DecayNode`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `half_life_ms` | integer (uint64) | yes |  |
| `node_id` | string | yes |  |
| `now_ms` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecayNode`, `contract/schemas/result.graph.json#/methods/DecayNode`.

## `DecaySweep`

state-backed MutationBatch commits the resulting authoritative image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
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
| `floor` | number (double) | yes |  |
| `half_life_secs` | number (double) | yes |  |
| `prune` | boolean | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DecayStats` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DecaySweep`, `contract/schemas/result.graph.json#/methods/DecaySweep`.

## `DiffAgainst`

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

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `other_graph` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphDiff` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DiffAgainst`, `contract/schemas/result.graph.json#/methods/DiffAgainst`.

## `DiscountedReturn`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:read` |
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
| `gamma` | number (double) | yes |  |
| `traj_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DiscountedReturn`, `contract/schemas/result.graph.json#/methods/DiscountedReturn`.

## `DropNamedGraph`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `rdf:write` |
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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DropNamedGraph`, `contract/schemas/result.graph.json#/methods/DropNamedGraph`.

## `EdgeCount`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EdgeCount`, `contract/schemas/result.graph.json#/methods/EdgeCount`.

## `EvictBelow`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `delete` | boolean | yes |  |
| `ids` | array of string | yes |  |
| `threshold` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EvictBelow`, `contract/schemas/result.graph.json#/methods/EvictBelow`.

## `EvictLRU`

state-backed MutationBatch commits the resulting authoritative image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
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
| `max_nodes` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EvictLRU`, `contract/schemas/result.graph.json#/methods/EvictLRU`.

## `Fork`

returns the forked snapshot to the caller; never registers/persists it server-side

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
| `result` | any | Json | {'reason': 'caller-properties', 'summary': 'property maps the caller wrote, returned verbatim'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Fork`, `contract/schemas/result.graph.json#/methods/Fork`.

## `GetEdgeProperties`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Json | {'reason': 'caller-properties', 'summary': 'property maps the caller wrote, returned verbatim'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetEdgeProperties`, `contract/schemas/result.graph.json#/methods/GetEdgeProperties`.

## `GetEdgePropertiesBatch`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `edges` | array of array of any | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of `PropertyBlob` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetEdgePropertiesBatch`, `contract/schemas/result.graph.json#/methods/GetEdgePropertiesBatch`.

## `GetEdges`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | array of array of any | EdgeList |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetEdges`, `contract/schemas/result.graph.json#/methods/GetEdges`.

## `GetEdgesPage`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `after` | array \| null | no |  |
| `limit` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetEdgesPage`, `contract/schemas/result.graph.json#/methods/GetEdgesPage`.

## `GetNeighbors`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNeighbors`, `contract/schemas/result.graph.json#/methods/GetNeighbors`.

## `GetNeighborsBatch`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNeighborsBatch`, `contract/schemas/result.graph.json#/methods/GetNeighborsBatch`.

## `GetNodeProperties`

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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | RawOrNull | {'reason': 'caller-properties', 'summary': 'property maps the caller wrote, returned verbatim'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNodeProperties`, `contract/schemas/result.graph.json#/methods/GetNodeProperties`.

## `GetNodePropertiesBatch`

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
| `node_ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNodePropertiesBatch`, `contract/schemas/result.graph.json#/methods/GetNodePropertiesBatch`.

## `GetNodes`

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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | NodeList |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNodes`, `contract/schemas/result.graph.json#/methods/GetNodes`.

## `GetNodesByLabel`

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
| `after` | string \| null | no |  |
| `label` | string | yes |  |
| `limit` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | NodeList |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetNodesByLabel`, `contract/schemas/result.graph.json#/methods/GetNodesByLabel`.

## `GetPredecessors`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetPredecessors`, `contract/schemas/result.graph.json#/methods/GetPredecessors`.

## `GetSubgraph`

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
| `node_ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SubgraphResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetSubgraph`, `contract/schemas/result.graph.json#/methods/GetSubgraph`.

## `GetSuccessors`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetSuccessors`, `contract/schemas/result.graph.json#/methods/GetSuccessors`.

## `HasEdge`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/HasEdge`, `contract/schemas/result.graph.json#/methods/HasEdge`.

## `HasNode`

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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/HasNode`, `contract/schemas/result.graph.json#/methods/HasNode`.

## `HasNodesBatch`

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
| `node_ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of boolean | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/HasNodesBatch`, `contract/schemas/result.graph.json#/methods/HasNodesBatch`.

## `InDegree`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/InDegree`, `contract/schemas/result.graph.json#/methods/InDegree`.

## `InvalidateEdge`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:write` |
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
| `invalid_at` | integer (uint64) | yes |  |
| `relationship` | string | yes |  |
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |
| `tx_now` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/InvalidateEdge`, `contract/schemas/result.graph.json#/methods/InvalidateEdge`.

## `Maintain`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `delete` | boolean | yes |  |
| `evict_threshold` | number (double) | yes |  |
| `half_life_ms` | integer (uint64) | yes |  |
| `ids` | array of string | yes |  |
| `now_ms` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Maintain`, `contract/schemas/result.graph.json#/methods/Maintain`.

## `Metrics`

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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphMetrics` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Metrics`, `contract/schemas/result.graph.json#/methods/Metrics`.

## `NodeCount`

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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/NodeCount`, `contract/schemas/result.graph.json#/methods/NodeCount`.

## `NodeIds`

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

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/NodeIds`, `contract/schemas/result.graph.json#/methods/NodeIds`.

## `OutDegree`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:read` |
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
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/OutDegree`, `contract/schemas/result.graph.json#/methods/OutDegree`.

## `PruneByLifecycle`

state-backed MutationBatch commits the resulting authoritative image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
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
| `max_age_secs` | integer (uint64) | yes |  |
| `min_score` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PruneStats` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PruneByLifecycle`, `contract/schemas/result.graph.json#/methods/PruneByLifecycle`.

## `Reconcile`

state-backed MutationBatch commits the merged image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
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
| `graph_name` | string | yes |  |
| `msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Reconcile`, `contract/schemas/result.graph.json#/methods/Reconcile`.

## `Reinforce`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `node_id` | string | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `weight` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Reinforce`, `contract/schemas/result.graph.json#/methods/Reinforce`.

## `RemoveEdge`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:write` |
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
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RemoveEdge`, `contract/schemas/result.graph.json#/methods/RemoveEdge`.

## `RemoveNode`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:write` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RemoveNode`, `contract/schemas/result.graph.json#/methods/RemoveNode`.

## `RemoveTriples`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `rdf:write` |
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
| `ntriples` | string | no | N-Triples document (empty ⇒ use `turtle`). |
| `turtle` | string | no | Turtle document (empty ⇒ use `ntriples`). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RemoveTriples`, `contract/schemas/result.graph.json#/methods/RemoveTriples`.

## `Reparent`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `scene:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `new_parent` | string \| null | no |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Reparent`, `contract/schemas/result.graph.json#/methods/Reparent`.

## `SceneChildren`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `scene:read` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SceneChildren`, `contract/schemas/result.graph.json#/methods/SceneChildren`.

## `SetPose`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `scene:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_id` | string | yes |  |
| `pose_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SetPose`, `contract/schemas/result.graph.json#/methods/SetPose`.

## `StartTrajectory`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:write` |
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
| `props_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StartTrajectory`, `contract/schemas/result.graph.json#/methods/StartTrajectory`.

## `SummariesAtLevel`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:read` |
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
| `level` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SummariesAtLevel`, `contract/schemas/result.graph.json#/methods/SummariesAtLevel`.

## `SummaryChildren`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `memory:read` |
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
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SummaryChildren`, `contract/schemas/result.graph.json#/methods/SummaryChildren`.

## `SupersedeEdge`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `edge:write` |
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
| `prior_relationship` | string | yes |  |
| `prior_source` | string | yes |  |
| `prior_target` | string | yes |  |
| `properties_msgpack` | array of integer (uint8) | yes |  |
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |
| `tx_now` | integer (uint64) | yes |  |
| `valid_at` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SupersedeEdge`, `contract/schemas/result.graph.json#/methods/SupersedeEdge`.

## `TouchNodes`

state-backed MutationBatch commits the resulting authoritative image

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
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
| `node_ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TouchNodes`, `contract/schemas/result.graph.json#/methods/TouchNodes`.

## `UnionGetNeighbors`

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
| `graphs` | array of string | yes |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnionGetNeighbors`, `contract/schemas/result.graph.json#/methods/UnionGetNeighbors`.

## `UnionGetNodeProperties`

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
| `graphs` | array of string | yes |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | RawOrNull | {'reason': 'caller-properties', 'summary': 'property maps the caller wrote, returned verbatim'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnionGetNodeProperties`, `contract/schemas/result.graph.json#/methods/UnionGetNodeProperties`.

## `UnionGetNodesByLabel`

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
| `graphs` | array of string | yes |  |
| `label` | string | yes |  |
| `limit` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | NodeList |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnionGetNodesByLabel`, `contract/schemas/result.graph.json#/methods/UnionGetNodesByLabel`.

## `WorldTransform`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `scene:read` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ScenePose` \| null | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/WorldTransform`, `contract/schemas/result.graph.json#/methods/WorldTransform`.
