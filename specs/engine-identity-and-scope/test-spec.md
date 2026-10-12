# EG-IDENTITY — Test specification

| Test | Requirement | Setup | Pass and refusal observations |
|---|---|---|---|
| I-01 | ID-01 | Network-free signed JWTs for issuers A/B, separate audiences and human/service kinds. | Both valid issuers bind exact principal and tenant; unknown issuer, bad signature/audience/expiry, subject/tenant mismatch and wrong kind are denied before method dispatch. |
| I-02 | ID-01 | Rotate key ID on issuer A while issuer B remains unchanged. | Declared overlap works only in window; removed/stale/ambiguous key fails after refresh; B is unaffected. |
| I-03 | ID-02 | Register and replace complete roles, then remove an admin role. | Effective grants reflect removal before success; a concurrent request cannot use removed role; state persists after restart. |
| I-04 | ID-02 | Force failure between proposed role write and audit append; tamper with one historical audit record. | One transaction rolls back both writes; audit verification detects deletion/reorder/alteration and reports bounded location without secrets. |
| I-05 | ID-03 | Exercise every allowed mode transition with two admins, local credentials, enabled IdP and a test-signed gateway attestation. | Mode/epoch advance once; accounts/grants persist; sessions on old epoch are denied; retry recovers revocation receipt after restart. |
| I-06 | ID-03 | Stale epoch, missing surviving admin, no fresh MFA, non-loopback attestations, forged/replayed nonce, wrong tenant/principal. | Typed denial/conflict, unchanged mode and epoch, no retained session accepted after a committed transition. |
| I-07 | ID-04 | Generate contract and install wheel in a clean environment. | Every declared scope has one class and digest; public catalog equals server registry and the consumer-derived allowlist. |
| I-08 | ID-04 | Try human grant of service-only scope, service account admin, wildcard approver, and an unknown name. | Generation or runtime refuses each; direct human approver membership works only for the designated group. |
| I-10 | ID-02 | Create a user whose explicit principal ID belongs to a `RegisterIdentity` principal or the System identity. | `IDENTITY_COLLISION`; the registered identity, its role set and its System role stay unchanged (`create_user_cannot_take_over_a_registered_principal`). |
| I-09 | all | Run complete PR/release quality checks at exact commit. | No new CCCC/KISS/jscpd/Dupehound violation; generated artifacts fresh; supported Rust/Python/consumer tests pass. |
| I-10 | ID-06 | Provision `tenant__alpha__default`, register a principal with role `tenant:alpha`, then check `__commons__` and `tenant__acme__default`. | Read on `__commons__` is allowed. Write on `__commons__` is denied. Read on the other tenant's graph is denied. Repeated provisioning keeps three grants. |

## Open acceptance gap: replicated tenant provisioning

**OPEN — clustered tenant provisioning and identity-store ordering.** The
inherited replicated `CreateGraph` path receives a principal fingerprint rather
than the original principal ID (`src/server/authority_context.rs`,
`VerifiedRequestContext::replicated_mutation`). The request boundary copies that
verified identity into the request, and lifecycle provisioning looks it up as an
identity ID. Local tenant provisioning tests do not establish correct clustered
provisioning; this work does not claim that inherited mismatch is fixed.

A narrow fail-closed guard now withholds managed auto-binding during replicated
creation when the fingerprint matches a managed ID. It skips the entire
provisioning call, including the unmanaged fallback; graph creation still
succeeds. Raw fingerprint-shaped self IDs are also withheld because the carrier
cannot distinguish them from another caller's fingerprint. Separate authorized
Identity grants are required. The new serialization/application regression source
covers caller `U` plus managed alias `H(U)`, raw self-ID `H`, local creation, and
policy persistence/reprojection using temporary Redb. These regressions are
unexecuted pending exact-source native qualification; they do not establish
cross-group ordering or close this acceptance gap.

Separately, the new managed tenant provisioning side effect mutates the identity
store and must have correct route ownership and ordering against Identity
mutations. `CreateGraph` follows the named graph route, while Identity mutations
use `__commons__` (`src/server/dispatch/consensus/routing.rs`). The local store
transaction and focused Identity routing regression do not establish ordering
between these routes.

Before clearing this acceptance gap, require a focused replicated
CreateGraph/Identity test or equivalent exact-source contract evidence proving
correct creator binding, durable grants, and ordering with identity revocation
across replica application and replay. Keep this gap OPEN until that evidence
exists; passing local mapping, persistence, or routing tests alone is insufficient.
This note does not authorize principal reverse lookup or a cross-group authority
design change, and does not promote the related acceptance criteria to passed.

## Reproducible gates

Use `cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check` and focused Rust tests for OIDC, isolation identity admin, RBAC persistence and audit. Run `pre-commit run --config .config/pre-commit.yaml --all-files` and `bash scripts/ci_parity.sh` on the exact PR head. Hosted CI must generate JWTs and attestations locally, use disposable redb files, and clean up all fixtures. If a named focused test moves, record the replacement before removing it from the gate. A live external IdP is an optional deployment qualification, not the baseline PR test substrate.

## Semantic owner registry validation

Published source: [cf7010728682663f735516ee276f423476d3690b](https://github.com/Knuckles-Team/epistemic-graph/commit/cf7010728682663f735516ee276f423476d3690b).
Review: [PR 445](https://github.com/Knuckles-Team/epistemic-graph/pull/445).

`semantic_owner_registry_is_scoped_to_persistence_root` checks distinct-root isolation and canonical path-alias reuse.
`dispatch_attributes_every_queue_class_to_the_one_verified_principal` retains its verified-principal and spoofed-consumer assertions.
Native execution of both tests remains pending.
Hosted facade execution must establish R005 acceptance; source inspection alone does not establish it.
| I-11 | R008 | A principal with a stored policy node allowing two tools; plans naming two tools and three tools. | The two-tool plan passes. The three-tool plan returns `ScopeExceedsPolicy` naming the third tool, and an audit record exists. |

### EG-IDENTITY-R023 tests

- `EG-IDENTITY-R023.1`: `crates/eg-types/src/identity/segment.rs::tests::unit_round_trips`.
- `EG-IDENTITY-R023.2`: `crates/eg-types/src/identity/segment.rs::tests::superseded_and_foreign_units_hidden`.
- `EG-IDENTITY-R023.3`: `tests/identity/test_segment_retrieval_served.py::test_retrieve_filters_by_principal`.
- `EG-IDENTITY-R023.4`: `crates/eg-types/src/identity/segment.rs::tests::out_of_scope_names_id_only`.
- `EG-IDENTITY-R023.5`: `crates/eg-types/src/identity/tests/segment_audit.rs::read_audit_counts_hidden`.
- `EG-IDENTITY-R023.6`: `tests/identity/test_segment_retrieval_served.py::test_shared_unit_visible_only_in_scope`.


## R023 typed metadata acceptance

All names below are planned tests; this definition batch does not run runtime acceptance.
Use a real authenticated server, generated client and disposable durable storage.
Do not mock transport, authorization, schema composition, mutation admission or persistence.

- `EG-IDENTITY-R023.7`: Planned: tests/identity/test_typed_metadata_served.py::typed_metadata_variants_round_trip.

- `EG-IDENTITY-R023.8`: Planned: tests/identity/test_typed_metadata_served.py::typed_metadata_persists_without_file_authority.

- `EG-IDENTITY-R023.9`: Planned: tests/identity/test_typed_metadata_served.py::all_metadata_write_routes_reach_admission.

- `EG-IDENTITY-R023.10`: Planned: tests/identity/test_typed_metadata_served.py::invalid_metadata_has_no_durable_effect.

- `EG-IDENTITY-R023.11`: Planned: tests/identity/test_typed_metadata_served.py::metadata_import_provenance_survives_restart.

- `EG-IDENTITY-R023.12`: Planned: tests/identity/test_typed_metadata_served.py::typed_metadata_served_journey.

Create each variant through served AddNode and ApplyMutation, including typed interaction links through AddEdge.
Read through GetNodeProperties and an independent authorized client; restart and repeat the reads.
Check the exact payload, owner, scope, schema digest, revision and source provenance.
Remove or change the import file and prove the committed graph remains authoritative.
Replay the same import and prove one mutation and one receipt; altered replay must refuse.
Exercise structural, batch, query and RDF writes into the managed metadata domain.
Every route must reject missing types, removed labels, malformed fields, unknown variants and stale schema digests.
Include dangling principals, client/principal kind confusion, disabled actors and foreign-tenant interaction endpoints.
Test schema detachment, caller-forged ownership, stale revisions and authorization revocation between admission and commit.
Compare denied existing and absent identities after removing request-local correlation IDs; neither response reveals stored metadata.
Check unchanged graph nodes, edges, revisions, import receipts and mutation outbox through independent authorized readback.
Security denial audit records may follow the existing policy; they must contain no metadata payload.
Inject persistence failure and restart. A batch must expose every accepted record and receipt, or none.
Confirm metadata cannot register an identity, grant a role, change a credential or widen sharing.
Record the source SHA, generated contract digest, binary digest and authenticated receipts for eventual served proof.
Run contract generation and the affected language, CCCC, KISS, jscpd and Dupehound checks when runtime code changes.
