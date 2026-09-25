# Security API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.security.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 9 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

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

## `GovernedChange`

EH-560 governed changes, runtime-conditional: get/list are reads (governance:read); propose needs the kind's EXACT proposer scope, approve the kind's EXACT approver scope from a direct principal sharing no identity with the proposer, bound to the exact candidate digest; consumption is engine-internal and single-use; every transition is hash-chain audited in the ledger that shares the rbac.redb policy WTX; generic IssueControlLease refuses the governed. prefix

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `governance:propose` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `actor` | one of: `GovernedActor` \| null | no |  |
| `op` | `GovernedChangeOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `approve` | `GovernedChange` | Json |  |
| `get` | one of: `GovernedChange` \| null | Json |  |
| `list` | array of `GovernedChange` | Json |  |
| `propose` | `GovernedChange` | Json |  |
| `revoke` | `GovernedChange` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GovernedChange`, `contract/schemas/result.security.json#/methods/GovernedChange`.

## `Identity`

IDM-01..04 engine-owned identity store, runtime-conditional: reads are identity:read/identity:authenticate; writes need the op's EXACT identity:* scope (never implied by kg:admin or *) from the boundary-stamped actor; every secret is hashed, verified or sealed at the request boundary and cleared before replication; the store and its full RBAC projection share one rbac.redb WTX with a hash-chained identity audit trail

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `identity:admin` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `op` | `IdentityOp` | yes |  |
| `stamp` | one of: `IdentityStamp` \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `IdentityReply` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Identity`, `contract/schemas/result.security.json#/methods/Identity`.

## `RbacAdmin`

runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata; IDM-03: each write appends a hash-chained identity-audit entry in that WTX, and the identity store's idm: namespace is refused

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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

## `RbacElevation`

EH-404 just-in-time elevation, runtime-conditional: list is a read (rbac:elevation-read); request/revoke need rbac:elevation, approve needs the EXACT rbac:approve-elevation scope from a direct (undelegated) principal sharing no identity with the requester; every transition is hash-chain audited in the elevation ledger that shares the rbac.redb policy WTX

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `rbac:elevation` |
| Mutates | `true` |
| Durability domain | `ControlRedb` |
| Idempotent | `false` |
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
| `actor` | one of: `ElevationActor` \| null | no |  |
| `op` | `RbacElevationOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `approve` | `ElevationLease` | Json |  |
| `list` | array of `ElevationLease` | Json |  |
| `request` | `ElevationLease` | Json |  |
| `revoke` | `ElevationLease` | Json |  |

> Multi-body result: the `op` request field selects which body above is returned.

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RbacElevation`, `contract/schemas/result.security.json#/methods/RbacElevation`.

## `RegisterIdentity`

RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX; IDM-03: each registration appends a hash-chained identity-audit entry in that same WTX, and a principal owned by the identity store is refused

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
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
