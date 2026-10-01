# EG-IDENTITY-001 — Work and evidence

| Task | IDs | Exit condition | State | Public evidence |
|---|---|---|---|---|
| I-1 | EG-IDENTITY-R001 | Multi-issuer verifier and envelope binding pass I-01/I-02, including rotation and cross-tenant denials. | SPECIFIED | PENDING |
| I-2 | EG-IDENTITY-R002 | Full role replacement and identity audit share one durable transaction; I-03/I-04 pass after restart. | SPECIFIED | PENDING |
| I-3 | EG-IDENTITY-R003 | Mode singleton, epoch CAS, attested preconditions and revocation recovery pass I-05/I-06. | SPECIFIED | PENDING |
| I-4 | EG-IDENTITY-R004 | Generated scope registry and installed-wheel consumer parity pass I-07/I-08. | SPECIFIED | PENDING |
| I-5 | EG-IDENTITY-R005 | A production semantic worker presents one verified identity across every queue class it processes. | SPECIFIED | PENDING |
| I-6 | all | Exact-head hosted, CCCC, KISS, jscpd, Dupehound, contract, consumer and release proof passes I-09. | SPECIFIED | PENDING |

For each task record public PR URL, exact merged SHA, test command and hosted run, tool versions, pass/fail, package digest and remaining unrun proof. Mark `LANDED` only at a verified public main ancestor and `ACCEPTED` only after tests, consumer and release receipts on that same commit. A historical branch-level `BUILT` description is an input to review, not completion evidence.

## Task list

- [ ] **I-1:** Define a versioned issuer-entry schema and validation for duplicate issuer, invalid URL, key ambiguity and unsupported algorithm; upgrade primary OIDC loading while preserving separate ancillary protocol validators; add local signed-JWT fixtures for two issuers and key rotation. Closes `EG-IDENTITY-R001`. Tests I-01, I-02.
- [ ] **I-2:** Add typed full-replacement identity mutation in one durable `ControlRedb` write transaction, couple the identity audit record and chain head to that transaction, add `AuditVerify` and bounded denial storage, and route existing `RegisterIdentity` and `RbacAdmin` through the same mutation path. Closes `EG-IDENTITY-R002`. Tests I-03, I-04.
- [ ] **I-3:** Add the durable mode singleton and epoch CAS: implement the precondition table, verified gateway attestation and nonce, session-epoch invalidation, revocation/rotation receipt and restart recovery, keeping account/link/grant/data records intact. Closes `EG-IDENTITY-R003`. Tests I-05, I-06.
- [ ] **I-4:** Classify scopes in capability declarations, generate the registry and package digest, update server grant evaluation to use the generated classification, and add consumer contract tests using only the public wheel catalog. Closes `EG-IDENTITY-R004`. Tests I-07, I-08.
- [ ] **I-5:** Verify a production semantic worker presents a single verified identity consistently across every queue class it processes, with an integration test asserting the same principal in its audit records for each class. Closes `EG-IDENTITY-R005`.
- [ ] **I-6:** Run the full checks in [test-spec.md](test-spec.md) — `cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check`, focused Rust tests for OIDC, isolation identity admin, RBAC persistence and audit, `pre-commit run --config .config/pre-commit.yaml --all-files` and `bash scripts/ci_parity.sh` — at the exact PR head; review generated-artifact diffs; record a merged-head SHA plus hosted results. Closes `EG-IDENTITY-R001`–`EG-IDENTITY-R005` (all). Test I-09.
