# Security API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.security.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 6 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AuditProveInclusion`

provenance anchoring: Merkle inclusion proof for one node against a prior PROVENANCE_ANCHOR audit-chain entry

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:audit` |
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
| `anchor_seq` | integer \| null | no |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `MerkleInclusionReport` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AuditProveInclusion`, `contract/schemas/result.security.json#/methods/AuditProveInclusion`.

## `AuditVerify`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:audit` |
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
| `result` | `AuditReport` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AuditVerify`, `contract/schemas/result.security.json#/methods/AuditVerify`.

## `GetIdentity`

identity read-back closing the RegisterIdentity blind-upsert gap: None means unregistered/unknown, Some(identity) with empty roles means registered-and-confirmed-empty -- gated security:admin like RegisterIdentity/RbacAdmin so it grants no caller new privilege

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `AgentIdentity` \| null | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetIdentity`, `contract/schemas/result.security.json#/methods/GetIdentity`.

## `GetLedger`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ledger:read` |
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
| `result` | `LedgerReadResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetLedger`, `contract/schemas/result.security.json#/methods/GetLedger`.

## `RbacAdmin`

runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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
| `op` | `RbacAdminOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `AddGrant` | string | String |  |
| `AddRole` | string | String |  |
| `List` | `RbacPolicyListing` | Json |  |
| `RemoveGrant` | `RbacGrantRemoval` | Json |  |
| `RemoveRole` | string | String |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RbacAdmin`, `contract/schemas/result.security.json#/methods/RbacAdmin`.

## `RegisterIdentity`

RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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
| `agent_id` | string | yes |  |
| `role` | `AgentRole` | yes |  |
| `roles` | array of string | yes | RBAC role names this agent holds (CONCEPT:EG-KG.compute.feature). |
| `signature` | string | yes |  |
| `teams` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | String |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RegisterIdentity`, `contract/schemas/result.security.json#/methods/RegisterIdentity`.
