# EG-IDENTITY — Work and evidence

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

- [x] **I-1:** Define a versioned issuer-entry schema and validation for duplicate issuer, invalid URL, key ambiguity and unsupported algorithm; upgrade primary OIDC loading while preserving separate ancillary protocol validators; add local signed-JWT fixtures for two issuers and key rotation. Closes `EG-IDENTITY-R001`. Tests I-01, I-02.
- [x] **I-2:** Add typed full-replacement identity mutation in one durable `ControlRedb` write transaction, couple the identity audit record and chain head to that transaction, add `AuditVerify` and bounded denial storage, and route existing `RegisterIdentity` and `RbacAdmin` through the same mutation path. Closes `EG-IDENTITY-R002`. Tests I-03, I-04.
- [x] **I-3:** Add the durable mode singleton and epoch CAS: implement the precondition table, verified gateway attestation and nonce, session-epoch invalidation, revocation/rotation receipt and restart recovery, keeping account/link/grant/data records intact. Closes `EG-IDENTITY-R003`. Tests I-05, I-06.
- [x] **I-4:** Classify scopes in capability declarations, generate the registry and package digest, update server grant evaluation to use the generated classification, and add consumer contract tests using only the public wheel catalog. Closes `EG-IDENTITY-R004`. Tests I-07, I-08.
- [x] **I-5:** Verify a production semantic worker presents a single verified identity consistently across every queue class it processes, with an integration test asserting the same principal in its audit records for each class. Closes `EG-IDENTITY-R005`.
- [x] **I-6:** Run the full checks in [test-spec.md](test-spec.md) — `cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check`, focused Rust tests for OIDC, isolation identity admin, RBAC persistence and audit, `pre-commit run --config .config/pre-commit.yaml --all-files` and `bash scripts/ci_parity.sh` — at the exact PR head; review generated-artifact diffs; record a merged-head SHA plus hosted results. Closes `EG-IDENTITY-R001`–`EG-IDENTITY-R005` (all). Test I-09.
- [x] **I-7:** Add the commons Read grant to tenant role provisioning and add the isolation unit test `a_tenant_role_reads_but_never_writes_the_shared_commons`. Closes `EG-IDENTITY-R006`. Test I-10.
- [x] **I-8:** Backfill the commons Read grant for tenant roles that exist before this change. Provisioning runs only on `CreateGraph`, so an existing deployment needs one `RbacAdmin` `AddGrant` per tenant role, or a startup reconcile. Closes the live rollout of `EG-IDENTITY-R006`.
| I-9 | EG-IDENTITY-R007 | Scopes `identity:admin`, `loops:read` and `loops:control` are registered in the engine contract. | SPECIFIED | PENDING |
| I-11 | EG-IDENTITY-R008 | Plan scope is bounded by a stored policy node; I-11 passes including the refusal. | SPECIFIED | PENDING |
- [ ] **EG-IDENTITY-R023:** Principal-scoped knowledge segments with explicit sharing.
- [ ] **EG-IDENTITY-R023.1:** Model. Add `KnowledgeUnit{id, owner, version, supersedes, access_labels, share_scope}` in `crates/eg-types/src/identity/segment.rs`. Closed `share_scope` values are `private`, `placement` and `tenant`.
- [ ] **EG-IDENTITY-R023.2:** Behavior. Add `visible_units(principal, units)` returning owned units and units a matching scope shares. It keeps only the highest version of each lineage. Each rule is a named replaceable default.
- [ ] **EG-IDENTITY-R023.3:** Wiring. The behavior attaches in the served content-read handler, before the hits leave the retrieval function. It reuses the visibility pattern at `crates/eg-types/src/rbac_elevation/ledger.rs:268`. This row creates the call site in the served content-read handler that invokes retrieval.
- [ ] **EG-IDENTITY-R023.4:** Refusal. A read naming a unit outside the principal's scope must return `OutOfScope` naming the unit id. It must not reveal the unit's content.
- [ ] **EG-IDENTITY-R023.5:** Evidence. Each read must append an audit entry. The entry lists the principal and the returned unit ids. It also counts units hidden by scope or version.
- [ ] **EG-IDENTITY-R023.6:** Hot-path proof. The entry point is the served content-read wire method. The test `tests/identity/test_segment_retrieval_served.py::test_shared_unit_visible_only_in_scope` writes a private and a shared unit. It passes through the wired retrieval call and observes that only the permitted current version returns.
