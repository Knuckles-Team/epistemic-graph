# Messaging API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.messaging.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 42 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `BindQueue`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `exchange` | string | yes |  |
| `queue` | string | yes |  |
| `routing_key` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BindQueue`, `contract/schemas/result.messaging.json#/methods/BindQueue`.

## `BrokerAck`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:ack` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_id` | string | yes |  |
| `queue` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerAck`, `contract/schemas/result.messaging.json#/methods/BrokerAck`.

## `BrokerAckTag`

current-generation result must not be replay-cached across requests

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:ack` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `consumer` | string | yes |  |
| `delivery_tag` | integer (int64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerAckTag`, `contract/schemas/result.messaging.json#/methods/BrokerAckTag`.

## `BrokerConsume`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:consume` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `consumer` | string | yes |  |
| `group` | string | yes |  |
| `lease_ms` | integer (uint64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `prefetch` | integer (uint32) | yes |  |
| `queue` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerConsume`, `contract/schemas/result.messaging.json#/methods/BrokerConsume`.

## `BrokerNackTag`

current-generation result must not be replay-cached across requests

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:ack` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `consumer` | string | yes |  |
| `delivery_tag` | integer (int64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `requeue` | boolean | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerNackTag`, `contract/schemas/result.messaging.json#/methods/BrokerNackTag`.

## `BrokerReject`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:ack` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_id` | string | yes |  |
| `now_ms` | integer (uint64) | yes |  |
| `queue` | string | yes |  |
| `requeue` | boolean | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerReject`, `contract/schemas/result.messaging.json#/methods/BrokerReject`.

## `BrokerRenewTag`

current-generation result must not be replay-cached across requests

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:ack` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `consumer` | string | yes |  |
| `delivery_tag` | integer (int64) | yes |  |
| `lease_ms` | integer (uint64) | yes |  |
| `now_ms` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BrokerRenewTag`, `contract/schemas/result.messaging.json#/methods/BrokerRenewTag`.

## `CdcRead`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:read` |
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
| `from_seq` | integer (uint64) | yes |  |
| `graph` | string | yes |  |
| `limit` | integer (uint32) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CdcReadResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CdcRead`, `contract/schemas/result.messaging.json#/methods/CdcRead`.

## `CepPoll`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cep:read` |
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
| `sub_id` | integer (uint64) | yes |  |
| `timeout_ms` | integer (uint64) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `CepMatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CepPoll`, `contract/schemas/result.messaging.json#/methods/CepPoll`.

## `CepSubscribe`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cep:admin` |
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
| `buffer` | integer (uint32) | no |  |
| `pattern_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CepSubscribe`, `contract/schemas/result.messaging.json#/methods/CepSubscribe`.

## `CepUnsubscribe`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cep:admin` |
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
| `sub_id` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CepUnsubscribe`, `contract/schemas/result.messaging.json#/methods/CepUnsubscribe`.

## `CloseChannel`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:admin` |
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
| `channel_id` | string | yes |  |
| `summary_embedding` | array \| null | no | Optional embedding of the conversation summary. |
| `topic_metadata` | string \| null | no | Optional topic/metadata for the KG imprint. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ChannelDeparture` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CloseChannel`, `contract/schemas/result.messaging.json#/methods/CloseChannel`.

## `CreateChannel`

opaque prepared/committed session-control MutationBatch; message/member payloads stay out of the ledger

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:admin` |
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
| `channel_id` | string | yes |  |
| `channel_type` | `ChannelType` | yes |  |
| `creator` | string | yes |  |
| `initial_members` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ChannelCreated` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CreateChannel`, `contract/schemas/result.messaging.json#/methods/CreateChannel`.

## `DeclareExchange`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `exchange` | string | yes |  |
| `kind` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DeclareExchange`, `contract/schemas/result.messaging.json#/methods/DeclareExchange`.

## `DeclareQueue`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `dl_exchange` | string \| null | no | EG-276: exchange to republish dead-lettered messages to (`None` ⇒ drop). |
| `dl_routing_key` | string \| null | no | EG-276: routing key for dead-lettered messages (`None` ⇒ reuse original). |
| `max_delivery_count` | integer \| null | no | EG-276: max delivery attempts before a message is dead-lettered. |
| `max_priority` | integer \| null | no | EG-278: max priority band the queue honors (advisory ceiling). |
| `message_ttl_ms` | integer \| null | no | EG-277: default per-message TTL in ms applied when a publish omits one. |
| `queue` | string | yes |  |
| `queue_expiry_ms` | integer \| null | no | EG-277: queue-expiry hint in ms (unused-queue teardown; advisory). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DeclareQueue`, `contract/schemas/result.messaging.json#/methods/DeclareQueue`.

## `DeleteExchange`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `exchange` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DeleteExchange`, `contract/schemas/result.messaging.json#/methods/DeleteExchange`.

## `DropContinuousQuery`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:admin` |
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

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DropContinuousQuery`, `contract/schemas/result.messaging.json#/methods/DropContinuousQuery`.

## `DropTrigger`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:admin` |
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

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DropTrigger`, `contract/schemas/result.messaging.json#/methods/DropTrigger`.

## `FiredTriggers`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:read` |
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
| `from_seq` | integer (uint64) | yes |  |
| `graph` | string | yes |  |
| `limit` | integer (uint32) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `FiredTriggersResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FiredTriggers`, `contract/schemas/result.messaging.json#/methods/FiredTriggers`.

## `GetChannelMembers`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:read` |
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
| `channel_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Ids |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChannelMembers`, `contract/schemas/result.messaging.json#/methods/GetChannelMembers`.

## `GetChannelMessages`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:read` |
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
| `channel_id` | string | yes |  |
| `limit` | integer \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `ChannelMessage` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChannelMessages`, `contract/schemas/result.messaging.json#/methods/GetChannelMessages`.

## `JoinChannel`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:admin` |
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
| `agent_id` | string | yes |  |
| `channel_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/JoinChannel`, `contract/schemas/result.messaging.json#/methods/JoinChannel`.

## `LeaveChannel`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:admin` |
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
| `agent_id` | string | yes |  |
| `channel_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ChannelDeparture` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/LeaveChannel`, `contract/schemas/result.messaging.json#/methods/LeaveChannel`.

## `ListChannels`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:read` |
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
| `result` | array of `ChannelSummary` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ListChannels`, `contract/schemas/result.messaging.json#/methods/ListChannels`.

## `ListTriggers`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:read` |
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
| `graph` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `TriggerInfo` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ListTriggers`, `contract/schemas/result.messaging.json#/methods/ListTriggers`.

## `Publish`

PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:publish` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `exchange` | string | yes |  |
| `payload` | array of integer (uint8) | yes |  |
| `routing_key` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Publish`, `contract/schemas/result.messaging.json#/methods/Publish`.

## `PublishConfirmed`

PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:publish` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `delay_ms` | integer \| null | no |  |
| `exchange` | string | yes |  |
| `now_ms` | integer \| null | no |  |
| `payload` | array of integer (uint8) | yes |  |
| `priority` | integer (int64) | no |  |
| `routing_key` | string | yes |  |
| `ttl_ms` | integer \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ConfirmToken` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PublishConfirmed`, `contract/schemas/result.messaging.json#/methods/PublishConfirmed`.

## `PublishEx`

PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:publish` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `delay_ms` | integer \| null | no | EG-279: hold the message non-claimable for this many ms from `now_ms`. |
| `exchange` | string | yes |  |
| `now_ms` | integer \| null | no | Caller clock (ms since epoch) used to resolve `delay_ms`/`ttl_ms` to absolute etas — explicit so WAL replay is deterministic. |
| `payload` | array of integer (uint8) | yes |  |
| `priority` | integer (int64) | no | EG-278: priority band; higher is delivered first (default 0). |
| `routing_key` | string | yes |  |
| `ttl_ms` | integer \| null | no | EG-277: per-message TTL in ms from `now_ms` (falls back to queue TTL). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PublishEx`, `contract/schemas/result.messaging.json#/methods/PublishEx`.

## `PublishIdempotent`

PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:publish` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `delay_ms` | integer \| null | no |  |
| `exchange` | string | yes |  |
| `now_ms` | integer \| null | no |  |
| `payload` | array of integer (uint8) | yes |  |
| `priority` | integer (int64) | no |  |
| `producer_id` | string \| null | no | Stable publisher identity; `None`/empty ⇒ at-least-once (no dedup). |
| `routing_key` | string | yes |  |
| `seq` | integer (int64) | no | Per-producer monotonic sequence number (dedup key). |
| `ttl_ms` | integer \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `IdempotentPublish` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PublishIdempotent`, `contract/schemas/result.messaging.json#/methods/PublishIdempotent`.

## `ReadContinuousQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:read` |
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
| `result` | `ContinuousQueryResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ReadContinuousQuery`, `contract/schemas/result.messaging.json#/methods/ReadContinuousQuery`.

## `RegisterContinuousQuery`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:admin` |
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
| `spec_msgpack` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterContinuousQuery`, `contract/schemas/result.messaging.json#/methods/RegisterContinuousQuery`.

## `RegisterTrigger`

opaque prepared/committed session-control MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:admin` |
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
| `action_msgpack` | array of integer (uint8) | no |  |
| `graph` | string | yes |  |
| `label` | string | no |  |
| `name` | string | yes |  |
| `op` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterTrigger`, `contract/schemas/result.messaging.json#/methods/RegisterTrigger`.

## `SendMessage`

request-scoped opaque session-control receipt prevents acknowledgement-lost duplicate sends

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `channel:write` |
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
| `channel_id` | string | yes |  |
| `payload` | string | yes |  |
| `sender` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SendMessage`, `contract/schemas/result.messaging.json#/methods/SendMessage`.

## `StreamCommitOffset`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `group` | string | yes |  |
| `offset` | integer (int64) | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamCommitOffset`, `contract/schemas/result.messaging.json#/methods/StreamCommitOffset`.

## `StreamCommittedOffset`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:read` |
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
| `group` | string | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamCommittedOffset`, `contract/schemas/result.messaging.json#/methods/StreamCommittedOffset`.

## `StreamDeclare`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `max_age_ms` | integer \| null | no | Drop messages older than this many ms (`now_ms - ts`) on trim. |
| `max_messages` | integer \| null | no | Keep at most this many newest messages (older dropped on trim). |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamDeclare`, `contract/schemas/result.messaging.json#/methods/StreamDeclare`.

## `StreamPublish`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:write` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `now_ms` | integer (uint64) | yes | Caller clock (ms) stamped as the message `ts` for age-based retention. |
| `payload` | array of integer (uint8) | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamPublish`, `contract/schemas/result.messaging.json#/methods/StreamPublish`.

## `StreamRead`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:read` |
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
| `from_offset` | integer (int64) | yes |  |
| `max` | integer (uint64) | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamRead`, `contract/schemas/result.messaging.json#/methods/StreamRead`.

## `StreamTrim`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `stream:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `now_ms` | integer (uint64) | yes |  |
| `stream` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StreamTrim`, `contract/schemas/result.messaging.json#/methods/StreamTrim`.

## `SweepExpired`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `now_ms` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | integer (uint64) | Count |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SweepExpired`, `contract/schemas/result.messaging.json#/methods/SweepExpired`.

## `UnbindQueue`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `broker:admin` |
| Mutates | `true` |
| Durability domain | `Outbox` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `exchange` | string | yes |  |
| `queue` | string | yes |  |
| `routing_key` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnbindQueue`, `contract/schemas/result.messaging.json#/methods/UnbindQueue`.

## `Watch`

opens a push subscription; not a snapshot read nor a mutation

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `cdc:read` |
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
| `from_seq` | integer (uint64) | yes |  |
| `graph` | string | yes |  |
| `label` | string | no |  |
| `timeout_ms` | integer (uint64) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WatchBatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Watch`, `contract/schemas/result.messaging.json#/methods/Watch`.
