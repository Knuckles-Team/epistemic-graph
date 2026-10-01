# EG-IDENTITY-001 — Engine identity, mode and scope authority

**Owner:** epistemic-graph. **Delivery:** SPECIFIED. **Acceptance:** NOT_AUDITED.

## Outcome

An operator can configure several trusted identity issuers, modify engine roles, and transition identity modes without losing administrative access or allowing an unverified caller to widen authority. Clients can discover the exact engine scope registry from the public contract. A contributor can implement and test every requirement from this directory and a fresh public checkout.

## State legend

`SPECIFIED` means the design and tests are reviewable; `BUILDING` means source is in progress; `BUILT` means a branch artifact exists; `LANDED` requires the exact public main commit; `ACCEPTED` requires the negative, persistence, consumer and release tests for that commit. A historical `BUILT` label is not acceptance. EG-IDENTITY-R001, EG-IDENTITY-R002, EG-IDENTITY-R003 and EG-IDENTITY-R004 remain **NOT_AUDITED** here: the current default branch has partial OIDC, RBAC and audit wiring, while the multi-issuer store, mode transition and generated scope registry are not proven present.

## Requirements

| ID | Normative behavior | Refusal and acceptance boundary |
|---|---|---|
| ID-01 / EG-IDENTITY-R001 | The primary EG OIDC verifier reads an explicit, ordered set of issuer entries: exact `issuer`, JWKS source, required audience and allowed principal kinds. It verifies signature, issuer, audience, expiry and key ID before binding `sub` to the request-envelope principal and tenant. A rotated key may overlap only for a declared window. | Unknown/ambiguous issuer, duplicate issuer/key, stale or removed key, invalid audience, tenant, subject, role/scope or principal kind fails closed before any method dispatch. Auxiliary verifier settings for other protocols remain separate. |
| ID-02 / EG-IDENTITY-R002 | Every identity/RBAC mutation recomputes the full effective role set in one durable ControlRedb write transaction. Role removal takes effect before success is returned. An allow or denial audit record names actor, tenant, operation, target, old/new authorization digest, result and correlation ID without secrets. A bounded denial sample records rejected attempts. | Failed persistence rolls back role and audit together. `AuditVerify` detects reorder, deletion and tampering of identity records. No append-only partial role update or unaudited admin bypass. |
| ID-03 / EG-IDENTITY-R003 | A durable singleton identity mode is `none`, `local` or `external`, with monotonic epoch and compare-and-set transition. Stored mode wins after first boot; environment is a bootstrap input only. Transition checks the preconditions in [architecture.md](architecture.md) and revokes sessions/rotates issuer material after commit while retaining accounts, links, grants and data. | Stale epoch, absent surviving admin, invalid credential, forged gateway context, missing fresh MFA proof or wrong scope returns typed refusal with no mode/data change. A half-completed revocation cannot report success. |
| ID-04 / EG-IDENTITY-R004 | EG publishes one generated, versioned scope registry from capability declarations. Each scope has exact name and class `user`, `domain`, `service-only`, `approver` or `admin`; generated package catalog and server checks share the same digest. | Human roles cannot inherit service-only scopes. Approver requires direct designated human group membership; admin requires human kind. Wildcards and `kg:admin` do not imply `approver` or `identity:admin`. Unknown or unclassified scope fails generation. |

## Public interfaces and ownership

EG owns token verification, durable engine RBAC/mode state, capability scope declarations, method-level authorization and generated scope catalog. A serving gateway supplies a **signed, nonce-bound attestation** of loopback binding and fresh MFA when a mode transition requires facts EG cannot observe directly. EG verifies the attestation signature, audience, expiry, tenant, principal, nonce and one-time use before the transition; caller-supplied booleans are never trusted. The gateway may narrow access but cannot replace EG's durable role or scope authority. Client applications use the generated registry; they cannot manually extend the engine registry.

The `none → local` transition requires a credentialed human admin; `none → external` requires an enabled IdP and linked human admin; `local → external` requires an enabled admin mapping; `external → local` requires local credentials for every retained admin. Any `→ none` transition requires an explicitly acknowledged operation, `identity:admin`, fresh MFA attestation and loopback binding attestations for both engine and gateway. A mode change cannot delete accounts, policy or audit history.

## Design and quality rules

Reuse `src/server/oidc.rs` and `src/server/auth.rs` for JWT binding, `crates/eg-core/src/isolation/identity_admin.rs` and `src/server/dispatch/router/identity_access.rs` for RBAC mutations, `crates/eg-core/src/rbac_persist.rs` for durable control state, `src/audit.rs` for the existing hash-chain algorithm, and `crates/eg-capabilities/src/domains/security.rs` plus `gen_contract` for scope generation. Extend these authorities; do not add a second identity database or independent scope allowlist.

Tests cover both accepted and denied paths and exact restart behavior. Run CCCC, KISS, jscpd and Dupehound on changed code, with no new clone authority. Baseline PR tests use locally signed JWTs, local JWKS fixtures, disposable redb files and a test gateway attestation signer. No live IdP, private deployment or manually provisioned service is required. A public deployment smoke remains separate release evidence.

## Completion

Each requirement row receives a public merged-head SHA, focused positive/negative test result, package/consumer test and applicable release gate. Only then may its acceptance become ACCEPTED; a single passing CI run or current code-path presence cannot close the four obligations.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.
