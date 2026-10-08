# EG-IDENTITY-001 — Test specification

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
| I-09 | all | Run complete PR/release quality checks at exact commit. | No new CCCC/KISS/jscpd/Dupehound violation; generated artifacts fresh; supported Rust/Python/consumer tests pass. |

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
