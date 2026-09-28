# EG-IDENTITY-001 — Implementation plan

1. Define a versioned issuer-entry schema and validation for duplicate issuer, invalid URL, key ambiguity and unsupported algorithm. Upgrade primary OIDC loading while preserving separate ancillary protocol validators. Add local signed-JWT fixtures for two issuers and key rotation.
2. Add typed full-replacement identity mutation in the ControlRedb write transaction. Couple the identity audit record and chain head to that transaction; add `AuditVerify` and bounded denial storage. Route existing `RegisterIdentity` and `RbacAdmin` through the same mutation path.
3. Add the durable mode singleton and epoch CAS. Implement the precondition table, verified gateway attestation and nonce, session-epoch invalidation, revocation/rotation receipt and restart recovery. Keep account/link/grant/data records intact.
4. Classify scopes in capability declarations, generate the registry and package digest, then update server grant evaluation to use the generated classification. Add consumer contract tests using only the public wheel catalog.
5. Run focused and full checks in [test-spec.md](test-spec.md), review exact source and generated-artifact diffs, and record a merged-head SHA plus hosted results in [tasks.md](tasks.md). Keep spec acceptance pending until consumer and release proof exists.

The first implementation slice should land issuer verification with negative tests, then transactional RBAC audit, then mode transitions, then registry. Each slice has a compatibility and rollback receipt. Do not enable a partially generated scope table or silently reinterpret an existing role set.
