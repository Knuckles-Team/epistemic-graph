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

## Principal-scoped knowledge segments (R023)

Data flow: content-read wire method -> the retrieval function -> `visible_units` (`crates/eg-types/src/identity/segment.rs`) -> permitted current units plus an audit entry.

Types: `KnowledgeUnit`, `share_scope`. Attach point: the retrieval function in the content-read path. Production caller: the content-read handler, created by the Wiring row. Failure behavior: out-of-scope reads return `OutOfScope` with the id only.

Reused: the visibility pattern at `crates/eg-types/src/rbac_elevation/ledger.rs:268` and the audit append from R002. New: version lineage and share scope on stored units.

Decision: access labels live in the unit itself, so every reader enforces them. Rejected alternative: a separate access table, because it can diverge from the unit it guards.


## Typed principal metadata within R023

### Model and authority

EG owns typed preferences, client descriptions and interaction networks as graph records.
The canonical identity store remains the sole principal, credential and grant authority.
`crates/eg-types/src/identity/model.rs:41` defines UserRecord and its stable principal_id.
`crates/eg-types/src/identity.rs:1` documents that authority and its RBAC projection.
Metadata references exact canonical principal IDs. It never infers identity from an alias, hash or display name.
A client description references its controlling principal. It does not register a principal or confer a credential.
Interaction links describe observed relationships. They never create membership, sharing grants or trust.
R023 sharing still requires an explicit authorized scope. Private scope is the default.

Extend the proposed `crates/eg-types/src/identity/segment.rs` model from R023.1.
Its metadata payload is a closed union: Preference, ClientMetadata or InteractionNetwork.
Common fields are node ID, owner principal, tenant, graph incarnation, revision, schema digest and provenance.
Preference contains a registered preference key and a value matching that key's declared type.
Value types are boolean, signed integer, bounded text and a registered enum choice.
ClientMetadata contains an opaque client ID, canonical controlling principal reference and a bounded display label.
InteractionNetwork contains typed member references and directed links with a registered relation kind and observed timestamp.
Each member reference distinguishes a canonical principal from a client metadata node.
Refuse dangling, wrong-kind and foreign-scope references before storing any node or link.
Bounds are 128 UTF-8 bytes per key, 1024 per label and 4096 per text value.
One payload has at most 64 members, 256 links and 64 KiB of encoded content.
Use checked revision counters, existing bounded containers and Digest256. Reject unknown fields and variants.
No payload field carries passwords, tokens, credential hashes, role grants or arbitrary authorization claims.

### Behavior and schema admission

Use an engine-managed graph schema source named identity_metadata through the existing R051 schema composition authority.
The source declares the three variants, reference kinds, required fields and bounds.
Declare a principal-metadata graph domain at graph provisioning; the declaration requires current administrative authority.
Every node and edge in that domain must satisfy the metadata contract, even without a caller-supplied type label.
Removing a label, using raw properties or changing the schema source cannot bypass admission.
Refuse source detachment or weakening while retained metadata depends on it.
Generic graphs retain their existing contracts; they cannot masquerade as the authoritative principal-metadata domain.
Resolve references and validate the complete candidate state before committing an atomic MutationBatch.
Updates require the expected revision and schema digest. Concurrent mismatches refuse without partial changes.
Current authorization and schema changes invalidate stale admission evidence before commit.
Metadata reads use the graph authority and R023 principal scope; metadata cannot widen either.

### Existing code and production wiring

The reviewed source is c0b952776129fd509b9f87b411a298593c2591ec.
Existing code: `src/server/auth.rs:901` binds the verified caller; `src/server/access.rs:702` checks graph access.
`src/server/handlers/graph_ops/gateway.rs:72` routes structural graph mutations through the existing commit gateway.
`src/server/handlers/graph_ops/gateway_graph.rs:151` handles ApplyMutation application.
`src/server/mutation_batch/compile.rs:126` defines compile_methods_with_outbox for the canonical write set.
`src/server/mutation_batch/commit/` owns durable commit and projection publication.
`src/server/handlers/graph_ops/nodes.rs:85` serves GetNodeProperties through the authorized read path.
`src/server/graph_schema/compose.rs` uses validate_and_compose for existing named schema sources.
`src/server/handlers/rdf/validation.rs:21` calls eg_shacl::validate through the served ShaclValidate boundary.
ShaclValidate is read-only inspection. It is not a mandatory metadata admission gate today.
Reachable today: no. The mandatory metadata domain, typed variants and common admission predicate remain proposed.

The new predicate must run before compilation and again against the admitted schema and authority at commit.
Structural writes enter through AddNode, AddEdge and ApplyMutation at the existing gateway.
Query and RDF writes use their existing gateway routes and must reach the same predicate.
Audit direct and opaque batch paths; a path lacking validated metadata evidence must refuse for this domain.
No alternate GraphCore mutation, direct store write or caller-selected schema may bypass that check.
Use existing generated method contracts. Register new fields and typed refusal codes before exposing them.
No new client metadata store, identity registry, schema composer or public write method is required.

### Refusal, migration and evidence

Check caller authority before resolving protected principal references or exposing their existence.
Denied existing and absent references return the same redacted error shape and no stored metadata.
Validate type, bounds, schema digest, ownership, reference scope and expected revision before any durable mutation.
A disabled or revoked actor cannot regain access through previously admitted metadata or an import receipt.
Refusal leaves graph records, edges, revisions, import receipts and mutation outbox rows unchanged.
Existing security denial auditing remains separate and must contain no metadata payload or protected reference details.

Imports from legacy files require an explicit versioned mapping into these same variants and admission rules.
Record source kind, logical source reference, source Digest256, mapping version and verified importing actor.
Record the schema digest, previous revision and commit receipt in the same admitted transaction.
The caller supplies candidate source evidence; EG checks the bytes and derives the stored digest.
The same import identity and content replay once. Reusing it with changed content refuses.
Invalid legacy records remain unimported; an import cannot retain an untyped fallback in the authoritative graph.
A bounded import batch commits all validated records or none. Large imports use explicit independent batches.
Readback after restart uses EG records, even when the source file disappears or changes.
An exported file is a snapshot, never write authority. Reimport requires fresh admission and an expected revision.
Migration never changes canonical identity records, grants, credentials or principal IDs.

### Acceptance and ownership

The six new children require the planned positive and negative cases in test-spec.md.
No helper-only test, schema declaration or generated client establishes served acceptance.
R051 and R098 remain schema prerequisites; their typed-packs files and crosswalk work are unchanged.
GraphOS may compose these engine results. AU applies them; clients cannot become metadata or identity authorities.


### 2026-10-13: preserve the R008.2 construction invariant

Keep `ScopeBound.policy_iri` private and expose only a borrowed `policy_iri()` accessor.
All public construction must pass through `ScopeBound::new`.
Reject blank policy IRIs before construction; callers cannot replace or clear the stored IRI.
Compile-fail examples must reject external struct literals and direct field mutation.
Existing subset checks and allowed-tool/graph fields keep their behavior.
