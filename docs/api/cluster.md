# Cluster API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.cluster.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 31 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `CancelRequest`

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
| `target_req_id` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CancelRequest`, `contract/schemas/result.cluster.json#/methods/CancelRequest`.

## `CatalogAssign`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `graph` | string | yes |  |
| `node` | integer \| null | no |  |
| `shard` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CatalogAssign`, `contract/schemas/result.cluster.json#/methods/CatalogAssign`.

## `CatalogList`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster-read` |
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
| `result` | `CatalogListing` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CatalogList`, `contract/schemas/result.cluster.json#/methods/CatalogList`.

## `CatalogReassign`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `graph` | string | yes |  |
| `shard` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CatalogReassign`, `contract/schemas/result.cluster.json#/methods/CatalogReassign`.

## `CatalogRemove`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `graph` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CatalogRemove`, `contract/schemas/result.cluster.json#/methods/CatalogRemove`.

## `ClusterMembers`

ADR-1/W1.1 engine-authoritative client topology; deliberately NOT admin:cluster-read -- ordinary service roles need it to re-resolve after a failover; answered from any node, not just the leader

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cluster:topology-read` |
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
| `result` | `ClusterDiscoverySnapshot` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClusterMembers`, `contract/schemas/result.cluster.json#/methods/ClusterMembers`.

## `CreateGraph`

native lifecycle MutationBatch before registry publication

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:admin` |
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
| `graph_name` | string | yes |  |
| `graph_type` | `GraphType` (enum: `Agent`, `Team`, `Global`, `Commons`) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphCreated` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CreateGraph`, `contract/schemas/result.cluster.json#/methods/CreateGraph`.

## `CreateMatView`

prepared/committed control-plane MutationBatch saga

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `matview:admin` |
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
| `algo` | `DistAlgo` | yes |  |
| `graphs` | array of string | yes |  |
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CreateMatView`, `contract/schemas/result.cluster.json#/methods/CreateMatView`.

## `DeleteGraph`

native lifecycle MutationBatch before registry eviction

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graph:admin` |
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
| `graph_name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphDeleted` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DeleteGraph`, `contract/schemas/result.cluster.json#/methods/DeleteGraph`.

## `FleetCatalog`

EH-345 fleet catalog, runtime-conditional: list/lookup are tenant- and principal-projected snapshot reads forced to __commons__ that join discovery records and operator overrides with connector-pack AgentComponents; record_discovery/set_override/clear_override self-translate into CreateNodeIfAbsent/CompareAndSetNodeFields against __commons__ (overrides need admin:fleet-catalog)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `registry:write` |
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
| `op` | `FleetCatalogOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `clear_override` | `FleetWriteReceipt` | Raw |  |
| `list` | `FleetCatalogPage` | Raw |  |
| `lookup` | `FleetCatalogLookup` | Raw |  |
| `record_discovery` | `FleetWriteReceipt` | Raw |  |
| `set_override` | `FleetWriteReceipt` | Raw |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FleetCatalog`, `contract/schemas/result.cluster.json#/methods/FleetCatalog`.

## `GetMatView`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `matview:read` |
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
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DistResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetMatView`, `contract/schemas/result.cluster.json#/methods/GetMatView`.

## `Health`

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
| `result` | `HealthReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Health`, `contract/schemas/result.cluster.json#/methods/Health`.

## `ListGraphs`

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
| `result` | array of `GraphListing` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ListGraphs`, `contract/schemas/result.cluster.json#/methods/ListGraphs`.

## `ListRegisteredServers`

typed, RLS-projected live :Server snapshot forced to __commons__; revision+digest-fenced keyset pages; authoritative on the single-node/full route (no cluster read barrier is claimed)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `registry:read` |
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
| `request` | `RegisteredServerListRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RegisteredServerListPage` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ListRegisteredServers`, `contract/schemas/result.cluster.json#/methods/ListRegisteredServers`.

## `Ping`

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
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Ping`, `contract/schemas/result.cluster.json#/methods/Ping`.

## `PlacementAdmin`

raft-replicated placement-catalog admin op (Assign/Move/AbortMove, the placement DECISION + PLAN->EXECUTE->CATALOG-UPDATE legs): MultiRaft::placement_assign / TenantManager::move_partition / abort_move commit through the DEFAULT group's own client_write / commit_placement, not this gateway's per-graph MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `op` | `PlacementAdminOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `abort_move` | boolean | Bool |  |
| `assign` | `PlacementEpoch` | Json |  |
| `move` | `PlacementMoveResult` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlacementAdmin`, `contract/schemas/result.cluster.json#/methods/PlacementAdmin`.

## `PlacementRoute`

engine-authoritative complete route; single-node returns authoritative unplaced group 0/epoch 0, while clustered routing requires a live MultiRaft control leader; GOC-15/BUG-030 narrowed off admin:cluster-read (2026-08-17) -- ordinary kg:read/kg:write routes their OWN tenant, handlers::placement::handle_route requires kg:admin for any other tenant's route

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cluster:placement-read` |
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
| `request` | `PlacementRouteRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PlacementRouteWire` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlacementRoute`, `contract/schemas/result.cluster.json#/methods/PlacementRoute`.

## `PlanMatViewDefine`

prepared/committed control-plane MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `matview:admin` |
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
| `graph` | string | yes |  |
| `name` | string | yes |  |
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlanMatViewDefine`, `contract/schemas/result.cluster.json#/methods/PlanMatViewDefine`.

## `PlanMatViewDrop`

prepared/committed control-plane MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `matview:admin` |
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
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlanMatViewDrop`, `contract/schemas/result.cluster.json#/methods/PlanMatViewDrop`.

## `PlanMatViewGet`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `matview:read` |
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
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlanMatViewGet`, `contract/schemas/result.cluster.json#/methods/PlanMatViewGet`.

## `PlanMatViewRefresh`

prepared/committed control-plane MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `matview:admin` |
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
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PlanMatViewRefresh`, `contract/schemas/result.cluster.json#/methods/PlanMatViewRefresh`.

## `RaftAddLearner`

leader-only openraft add_learner; attaches a non-voting replica without changing the voter set

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `addr` | string | yes |  |
| `group` | integer \| null | no |  |
| `node_id` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RaftAddLearner`, `contract/schemas/result.cluster.json#/methods/RaftAddLearner`.

## `RaftChangeMembership`

leader-only openraft change_membership; sets the group's exact voter set (the usual way to promote a learner added via RaftAddLearner)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `group` | integer \| null | no |  |
| `voters` | array of integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RaftChangeMembership`, `contract/schemas/result.cluster.json#/methods/RaftChangeMembership`.

## `RebalanceExecute`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `max_moves` | integer \| null | no |  |
| `tolerance` | number \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RebalanceExecution` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RebalanceExecute`, `contract/schemas/result.cluster.json#/methods/RebalanceExecute`.

## `RebalancePlan`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster-read` |
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
| `max_moves` | integer \| null | no |  |
| `tolerance` | number \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RebalancePlanReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RebalancePlan`, `contract/schemas/result.cluster.json#/methods/RebalancePlan`.

## `RefreshMatView`

prepared/committed control-plane MutationBatch saga

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `matview:admin` |
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
| `name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RefreshMatView`, `contract/schemas/result.cluster.json#/methods/RefreshMatView`.

## `RegisterForeignSource`

opaque prepared/committed control receipt; endpoint configuration is not duplicated in the ledger

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `federation:admin` |
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
| `name` | string | yes |  |
| `source` | `ForeignSourceSpec` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterForeignSource`, `contract/schemas/result.cluster.json#/methods/RegisterForeignSource`.

## `RegisterServer`

W2.5 fleet server push-registration/heartbeat: self-translates into Method::AddNode against __commons__ (dispatch.rs), writing a REAL :Server graph node, unlike the internal non-graph topology rows

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `registry:write` |
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
| `desired` | `ServerDesiredState` (enum: `enabled`, `disabled`) | no |  |
| `name` | string | yes |  |
| `resources_json` | string | no |  |
| `transport` | `ServerTransport` | no |  |
| `ttl_secs` | integer (uint64) | yes |  |
| `url` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterServer`, `contract/schemas/result.cluster.json#/methods/RegisterServer`.

## `RegisterUdf`

opaque prepared/committed control receipt; module bytes are not duplicated in the ledger

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `udf:admin` |
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
| `id` | string | yes |  |
| `wasm` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterUdf`, `contract/schemas/result.cluster.json#/methods/RegisterUdf`.

## `Reshard`

prepared/committed admin MutationBatch saga

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `admin:cluster` |
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
| `graph` | string | yes |  |
| `to_shard` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ShardReshardReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Reshard`, `contract/schemas/result.cluster.json#/methods/Reshard`.

## `Shutdown`

explicitly ephemeral process control; never acknowledges a user-data commit

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `service:admin` |
| Mutates | `true` |
| Durability domain | `VolatileControl` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NonceOnly` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Shutdown`, `contract/schemas/result.cluster.json#/methods/Shutdown`.
