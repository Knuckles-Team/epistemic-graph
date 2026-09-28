# EG-IDENTITY-001 — Architecture and transitions

## Existing seams

| Component | Reuse |
|---|---|
| `JwtValidator::from_env_primary`, `decode_with`, `validate_claims` in `src/server/oidc.rs` | Evolve single primary issuer into an ordered validated issuer registry; retain network-free `from_parts` for tests. |
| `primary_oidc_validator`, `bind_verified_identity` in `src/server/auth.rs` | Keep envelope subject/tenant binding after cryptographic verification. |
| `identity_access.rs`, `IsolationLayer::try_register_agent` | Route typed identity mutations through one ControlRedb transaction. |
| `rbac_persist.rs` | Store complete role set, mode singleton and epoch under one owner; migration is versioned and restart-safe. |
| `src/audit.rs` | Reuse chain hash/verification pattern; add identity audit rows to the RBAC control transaction, because graph audit currently covers a different store. |
| `domains/security.rs`, `gen_contract` | Emit the public scope registry and digest from the same capability declarations checked at dispatch. |

## Token processing order

Parse bounded JWT header and claims without trusting them. Resolve exact issuer to one configured entry. Resolve key ID from that issuer's JWKS; a cache miss may trigger a bounded refresh, then failure if still absent. Verify algorithm allowlist and signature, expiry, issuer and audience. Resolve principal kind and enforce issuer's allowed kinds. Bind token subject and tenant to the authenticated envelope and call context. Require token roles/scopes to be a subset of current durable grants. Only then dispatch a method. A key rotation overlap has explicit start/end; removed keys stop authorizing immediately after the configured refresh boundary.

## Control transaction and audit

`IdentityAdmin` loads current full role set and mode/epoch under a write transaction, checks principal and scope, computes a replacement set, writes identity state plus audit record, then commits. No response succeeds until durable commit. Denial records are bounded and rate-controlled; refusal paths contain no raw token, password, JWKS URL with credentials or role-target secrets. The identity audit chain has sequence, previous hash, record hash and canonical payload digest. A verification oracle reads from genesis to head and reports missing, reordered or altered records. Recovery after crash reads the last committed role/mode/audit head; it never infers a successful transition from a partially written external session-revocation task.

## Mode state machine

| From → to | Preconditions |
|---|---|
| `none → local` | Credentialed human admin is created/verified in the same activation flow. |
| `none → external` | Enabled IdP and linked human admin verified before mode commit. |
| `local → external` | At least one enabled IdP maps to a human admin; local fallback remains until post-commit validation. |
| `external → local` | Each retained admin has a verified local credential before external dependence is removed. |
| `local/external → none` | Exact acknowledgment, human `identity:admin`, fresh MFA attestation, engine and gateway loopback attestations, and a transition nonce bound to expected epoch. |

The transition response contains old/new mode, epoch and revocation receipt. Session revocation/issuer rotation are durable follow-up effects with retry state; until completed, the old sessions are denied by epoch at EG. Concurrent transitions race on epoch; one commits, others return typed conflict. A failed prerequisite changes neither mode nor epoch.

## Scope registry

Capability descriptors declare exact scope and class. The generator validates uniqueness and coverage, emits the registry with methods/errors and publishes its digest in the wheel. Server authorization resolves only exact declared scopes. Delegation cannot convert service-only into a human grant; approval cannot be inferred from wildcard or broad graph admin. Finance, fleet and other domain scopes enter through their owning capabilities, and stale undeclared names are rejected during generation. A consumer may generate a local allowlist from this catalog but cannot claim a broader class.
