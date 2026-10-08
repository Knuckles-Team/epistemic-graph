# Security API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.security.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 11 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `AuditAppend`

tenant-bound operation audit: a reservation and its linked outcome with request-id/op idempotency and no raw params. Requires a declared audit class and a durable writer (AUDIT_CLASS_REQUIRED, AUDIT_CLASS_UNKNOWN, AUDIT_WRITER_UNAVAILABLE). Writes its own audit-chain entry directly (see src/redb_store/operation_audit.rs::operation_audit_append), not through the generic per-mutation audit_line() dispatch, like EdgeIndex above

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:audit-write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AUDIT_CLASS_REQUIRED`, `AUDIT_CLASS_UNKNOWN`, `AUDIT_IDEMPOTENCY_CONFLICT`, `AUDIT_RESERVATION_MISMATCH`, `AUDIT_RESERVATION_REQUIRED`, `AUDIT_WRITER_UNAVAILABLE`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `READ_ONLY`, `REDIRECTED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `audit_class` | string | no |  |
| `op` | string | yes |  |
| `params_sha256` | string | yes |  |
| `request_id` | string | yes |  |
| `status` | string | yes |  |
| `surface` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AuditAppendReceipt` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AuditAppend`, `contract/schemas/result.security.json#/methods/AuditAppend`.

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

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

## `AuditReadEvent`

read one privacy-safe operation event with a full-chain verification result

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `seq` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AuditEventProof` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AuditReadEvent`, `contract/schemas/result.security.json#/methods/AuditReadEvent`.

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AuditReport` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/AuditVerify`, `contract/schemas/result.security.json#/methods/AuditVerify`.

## `CheckAccess`

confused-deputy-safe executor re-check: will this principal's own request of read/write on the request graph be admitted now (the engine's isolation/RBAC decision). Answers only allowed yes/no for one principal on one graph the caller can itself read. Never the identity or the policy

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:check` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `access` | `AccessCheck` (enum: `read`, `write`) | yes |  |
| `agent_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AccessDecision` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CheckAccess`, `contract/schemas/result.security.json#/methods/CheckAccess`.

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `LedgerReadResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetLedger`, `contract/schemas/result.security.json#/methods/GetLedger`.

## `Identity`

the engine-owned identity store (users, credentials, sessions, tokens, MFA, API keys, roles, groups, identity providers), runtime-conditional: reads are identity:read/identity:authenticate; writes need the op's EXACT identity:* scope (never implied by kg:admin or *) from the boundary-stamped actor; every secret is hashed, verified or sealed at the request boundary and cleared before replication; the store and its full RBAC projection share one rbac.redb WTX with a hash-chained identity audit trail

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `IDENTITY_ALREADY_INITIALIZED`, `IDENTITY_BAD_CREDENTIAL`, `IDENTITY_BUILT_IN`, `IDENTITY_CLASS_VIOLATION`, `IDENTITY_COLLISION`, `IDENTITY_EPOCH_CONFLICT`, `IDENTITY_FORGED_STAMP`, `IDENTITY_FULL`, `IDENTITY_ILLEGAL_TRANSITION`, `IDENTITY_INVALID`, `IDENTITY_KIND_MISMATCH`, `IDENTITY_NOT_AUTHORIZED`, `IDENTITY_NOT_FOUND`, `IDENTITY_NOT_INITIALIZED`, `IDENTITY_PASSWORD_REUSED`, `IDENTITY_PRECONDITION_FAILED`, `IDENTITY_REPLAY`, `IDENTITY_STALE_CREDENTIAL`, `IDENTITY_STAMP_FAILED`, `IDENTITY_STORE_MANAGED`, `IDENTITY_STORE_NAMESPACE`, `IDENTITY_SYSTEM_BOOTSTRAP_PENDING`, `IDENTITY_TOKEN_SPENT`, `IDENTITY_UNKNOWN_SCOPE`, `IDENTITY_UNSTAMPED`, `IDENTITY_WEAK_PASSWORD`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `READ_ONLY`, `REDIRECTED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |
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

runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata; each write also appends a hash-chained entry to the identity store's own audit trail in that WTX, and the identity store's idm: namespace is refused

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `IDENTITY_STORE_NAMESPACE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `READ_ONLY`, `REDIRECTED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |
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

EH-404 just-in-time elevation, runtime-conditional: list is a read (rbac:elevation-read). Request/revoke need rbac:elevation, approve needs the EXACT rbac:approve-elevation scope from a direct (undelegated) principal sharing no identity with the requester. Every transition is hash-chain audited in the elevation ledger that shares the rbac.redb policy WTX

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ELEVATION_ACTOR_UNSTAMPED`, `ELEVATION_COLLISION`, `ELEVATION_CONFLICT`, `ELEVATION_DELEGATED_APPROVER`, `ELEVATION_DIGEST_MISMATCH`, `ELEVATION_INVALID`, `ELEVATION_LEDGER_FULL`, `ELEVATION_NOT_APPROVER`, `ELEVATION_NOT_FOUND`, `ELEVATION_NOT_PARTY`, `ELEVATION_RESERVED_GRAPH`, `ELEVATION_SELF_APPROVAL`, `ELEVATION_UNKNOWN_ACTOR`, `ELEVATION_WILDCARD`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `READ_ONLY`, `REDIRECTED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |
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

RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX; each registration also appends a hash-chained entry to the identity store's own audit trail in that same WTX, and a principal owned by the identity store is refused

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
| Error set | `ACCESS_DENIED`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_INTERNAL_METHOD`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `IDENTITY_STORE_MANAGED`, `IDENTITY_STORE_NAMESPACE`, `INTERNAL`, `INVALID_ARGUMENT`, `METHOD_NOT_YET_SERVED`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `POLICY_NATIVE_AUTHORITY_REQUIRED`, `READ_ONLY`, `REDIRECTED`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT` |
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
