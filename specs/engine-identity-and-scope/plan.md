# EG-IDENTITY — Implementation plan

1. Define a versioned issuer-entry schema and validation for duplicate issuer, invalid URL, key ambiguity and unsupported algorithm. Upgrade primary OIDC loading while preserving separate ancillary protocol validators. Add local signed-JWT fixtures for two issuers and key rotation.
2. Add typed full-replacement identity mutation in the ControlRedb write transaction. Couple the identity audit record and chain head to that transaction; add `AuditVerify` and bounded denial storage. Route existing `RegisterIdentity` and `RbacAdmin` through the same mutation path.
3. Add the durable mode singleton and epoch CAS. Implement the precondition table, verified gateway attestation and nonce, session-epoch invalidation, revocation/rotation receipt and restart recovery. Keep account/link/grant/data records intact.
4. Classify scopes in capability declarations, generate the registry and package digest, then update server grant evaluation to use the generated classification. Add consumer contract tests using only the public wheel catalog.
5. Run focused and full checks in [test-spec.md](test-spec.md), review exact source and generated-artifact diffs, and record a merged-head SHA plus hosted results in [tasks.md](tasks.md). Keep spec acceptance pending until consumer and release proof exists.

The first implementation slice should land issuer verification with negative tests, then transactional RBAC audit, then mode transitions, then registry. Each slice has a compatibility and rollback receipt. Do not enable a partially generated scope table or silently reinterpret an existing role set.

## Architecture

### Existing seams

| Component | Reuse |
|---|---|
| `JwtValidator::from_env_primary`, `decode_with`, `validate_claims` in `src/server/oidc.rs` | Evolve single primary issuer into an ordered validated issuer registry; retain network-free `from_parts` for tests. |
| `primary_oidc_validator`, `bind_verified_identity` in `src/server/auth.rs` | Keep envelope subject/tenant binding after cryptographic verification. |
| `identity_access.rs`, `IsolationLayer::try_register_agent` | Route typed identity mutations through one ControlRedb transaction. |
| `rbac_persist.rs` | Store complete role set, mode singleton and epoch under one owner; migration is versioned and restart-safe. |
| `src/audit.rs` | Reuse chain hash/verification pattern. Add identity audit rows to the RBAC control transaction, because graph audit currently covers a different store. |
| `domains/security.rs`, `gen_contract` | Emit the public scope registry and digest from the same capability declarations checked at dispatch. |

### Token processing order

Parse bounded JWT header and claims without trusting them. Resolve exact issuer to one configured entry. Resolve key ID from that issuer's JWKS; a cache miss may trigger a bounded refresh, then failure if still absent. Verify algorithm allowlist and signature, expiry, issuer and audience. Resolve principal kind and enforce issuer's allowed kinds. Bind token subject and tenant to the authenticated envelope and call context. Require token roles/scopes to be a subset of current durable grants. Only then dispatch a method. A key rotation overlap has explicit start/end; removed keys stop authorizing immediately after the configured refresh boundary.

### Control transaction and audit

`IdentityAdmin` loads current full role set and mode/epoch under a write transaction, checks principal and scope, computes a replacement set, writes identity state plus audit record, then commits. No response succeeds until durable commit. Denial records are bounded and rate-controlled; refusal paths contain no raw token, password, JWKS URL with credentials or role-target secrets. The identity audit chain has sequence, previous hash, record hash and canonical payload digest. A verification oracle reads from genesis to head and reports missing, reordered or altered records. Recovery after crash reads the last committed role/mode/audit head; it never infers a successful transition from a partially written external session-revocation task.

### Mode state machine

| From → to | Preconditions |
|---|---|
| `none → local` | Credentialed human admin is created/verified in the same activation flow. |
| `none → external` | Enabled IdP and linked human admin verified before mode commit. |
| `local → external` | At least one enabled IdP maps to a human admin; local fallback remains until post-commit validation. |
| `external → local` | Each retained admin has a verified local credential before external dependence is removed. |
| `local/external → none` | Exact acknowledgment, human `identity:admin`, fresh MFA attestation, engine and gateway loopback attestations, and a transition nonce bound to expected epoch. |

The transition response contains old/new mode, epoch and revocation receipt. Session revocation/issuer rotation are durable follow-up effects with retry state. Until completed, the old sessions are denied by epoch at EG. Concurrent transitions race on epoch. One commits, others return typed conflict. A failed prerequisite changes neither mode nor epoch.

### Scope registry

Capability descriptors declare exact scope and class. The generator validates uniqueness and coverage, emits the registry with methods/errors and publishes its digest in the wheel. Server authorization resolves only exact declared scopes. Delegation cannot convert service-only into a human grant; approval cannot be inferred from wildcard or broad graph admin. Finance, fleet and other domain scopes enter through their owning capabilities, and stale undeclared names are rejected during generation. A consumer may generate a local allowlist from this catalog but cannot claim a broader class.

### Foreign-read scope classification (R004)

QueryForeignColumns declares federation:read as an engine user scope.
Federation administration retains the federation:admin admin scope, and wildcard
scope classification is rejected. Registry membership grants no scope to a
principal. Foreign reads still require verified owner isolation and kg:read.

## Semantic owner registry identity

The R005 registry must key the canonical persistence root, tenant, and binding.
Distinct roots must never share a semantic owner; aliases of one root must share its handle.
The existing activation grant and verified-principal checks remain mandatory.
`semantic_owner_registry_is_scoped_to_persistence_root` must prove root isolation and canonical alias reuse.
Native facade acceptance remains pending.

## Policy-bound run scope (R008)

Flow: stored policy node → `scope_within_bound` → `ScopeExceedsPolicy` refusal at plan admission.
The attach point is the plan-admission path beside `policy_admin.rs`; the first child row locates it.
The change reuses the existing verified-principal binding and the audit append from R002.
Decision: the engine reads the bound from the stored node, not from the plan body. A rejected alternative is deriving scope from the plan, because a model-written plan could then widen its own bound.
Handoff: graph-os must check the same bound when work enters; agent-utilities only applies the engine's refusal.
