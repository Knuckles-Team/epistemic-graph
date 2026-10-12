# EG-TYPED-PACKS — Typed packs and catalog authority

| Field | Value |
|---|---|
| Stable spec ID | `typed-packs-and-catalog` |
| Owner | `epistemic-graph` |
| Scope | Typed connector and agent components, content packs, schema composition, fleet/catalog authority, generated contract receipts |

## Evidence

The generator owns delivery state in `status.json`.
Record exact source, commands, test results and served evidence for each obligation.
Existing records do not establish execution of newly defined scenarios.

## Outcome and user stories

1. A connector author can publish one deterministic snapshot of tools, skills, prompts, ontology, shapes, model profiles, A2A cards, and its manifest. Reimporting identical content is a no-op; a changed or withdrawn entry has a predictable lifecycle.
2. A tenant administrator can bind the one allowed importer, inspect the head and projection status, retire entries, attach a visible pack schema to a graph, and safely re-drive a failed projection.
3. A consumer can read a typed component and its engine-owned body. The consumer can trust the generated method/result contract and receipt. It sees only a projection that passed its readiness gate.
4. A contributor can add a pack kind or catalog method by extending the existing type/registry/handler path. The contributor proves wire, storage, authorization, replay, client, and release parity.

## Normative requirements

| ID | Contract | Stable obligations |
|---|---|---|
| PACK-01 | One `ConnectorPackIndex` identifies a connector, exactly one server entry, sorted unique typed entries and bounded archive sections. EG recomputes raw-body SHA-256 and domain-framed entry, annotation, reference and pack digests; mismatches are refused. Archive layout and package version do not change the semantic pack digest. Keep golden vectors at `contract/fixtures/connector_pack_digest_vectors.json`. | EG-TYPED-PACKS-R001, EG-TYPED-PACKS-R022–EG-TYPED-PACKS-R026 |
| PACK-02 | `ConnectorPack` uses typed `Status`, `Import`, `Bind`, `Unbind`, `Retire`, `Reproject`, `ReconcileBodies`, and catalog reads. `AgentComponent.Content` supplies a verified body through the existing component method. Every operation has a declared result, action, durability and routing policy; dynamic result fallbacks are forbidden. | EG-TYPED-PACKS-R001, EG-TYPED-PACKS-R007, EG-TYPED-PACKS-R018, EG-TYPED-PACKS-R067 |
| PACK-03 | Import checks verified tenant and importer binding, expected head, archive, validation and policy before writing. Engine bodies may be staged first. One Agent Library owner transaction commits components, memberships, body holders, head, import record, receipt and ordered outbox intents. A crash before commit leaves only reclaimable orphan bodies; a crash during commit exposes no partial head. A lost reply replays by operation identity; changed bytes under the same idempotency key are refused. | EG-TYPED-PACKS-R002, EG-TYPED-PACKS-R003, EG-TYPED-PACKS-R008, EG-TYPED-PACKS-R010, EG-TYPED-PACKS-R041 |
| PACK-04 | A pack is a content snapshot. Identical head digest returns `Unchanged` with no version bump. An unchanged entry and pins carry forward; changed content or a changed server pin creates a revision. Absence becomes reversible `Withdrawn`; explicit admin retirement is permanent. New candidate/search reads hide withdrawn and unready records, while historical pinned reads remain resolvable. Direct component writes to reserved pack IDs are refused. | EG-TYPED-PACKS-R014, EG-TYPED-PACKS-R016, EG-TYPED-PACKS-R041, EG-TYPED-PACKS-R049, EG-TYPED-PACKS-R050 |
| PACK-05 | Each pack entry's references resolve only to a sibling of the stated kind, without cycles. Validation and the atomic writer both resolve pins against staged rows before stored rows. The server pin binds declared contract version, name and instructions; package version is provenance only. `contract_pin` is an SDK claim, and EG must not pretend to recompute SDK normalization. | EG-TYPED-PACKS-R016, EG-TYPED-PACKS-R036, EG-TYPED-PACKS-R049, EG-TYPED-PACKS-R050, EG-TYPED-PACKS-R064, EG-TYPED-PACKS-R065 |
| PACK-06 | Validation applies rules G1–G21 in [architecture.md](plan.md), collecting at most 256 violations and reporting budget exhaustion. No rejected import changes the head, members, holders, body liveness, record, receipt or outbox. Bounded parsers, no remote ontology import, and step-limited reasoning are mandatory; the OWL consistency algorithm itself is owned by `federated-query-and-reasoning`. | EG-TYPED-PACKS-R003, EG-TYPED-PACKS-R022–EG-TYPED-PACKS-R033, EG-TYPED-PACKS-R034–EG-TYPED-PACKS-R040, EG-TYPED-PACKS-R086 |
| PACK-07 | An Agent Library outbox worker projects the current committed head into a reserved `pack__<connector>` graph. One graph-schema attach carries the pack record revision; a subsequent Agent Library transaction marks `visible_record_id` and acknowledges the outbox item. Read paths filter against visibility. A failed projection is visible, rejected for retry without losing later work, and can be reprojected. A stale worker cannot overwrite a newer source. | EG-TYPED-PACKS-R006, EG-TYPED-PACKS-R051, EG-TYPED-PACKS-R052, EG-TYPED-PACKS-R053, EG-TYPED-PACKS-R054–EG-TYPED-PACKS-R059, EG-TYPED-PACKS-R074 |
| PACK-08 | Graph schema is a bounded map of named sources. Each source carries shapes and ontology bytes, origin and revision; the composed digest and policy are derived. K1–K7 conflict/consistency rules in [architecture.md](plan.md) apply on attach, detach and pack projection. `IcvConfigure` maps to the operator source. `GraphSchema.List` and `GraphSchemaClasses` expose versioned source/class/property facts. Attach to another graph is a snapshot whose staleness is visible. | EG-TYPED-PACKS-R051, EG-TYPED-PACKS-R061, EG-TYPED-PACKS-R062, EG-TYPED-PACKS-R071, EG-TYPED-PACKS-R077 |
| PACK-09 | EG is the typed catalog authority: `RegisterServer`/ServerRegistry owns liveness; `ConnectorPack`/`AgentComponent` owns content and lifecycle. The SDK creates/imports packs; GraphOS joins typed reads for serving. No SQL shadow catalog or independent AU content authority may appear. Catalog reads carry tenant, visibility, contract and version provenance. Python model convergence is owned by `public-engine-contract-and-release`. | EG-TYPED-PACKS-R042–EG-TYPED-PACKS-R048, EG-TYPED-PACKS-R066, EG-TYPED-PACKS-R072 |
| PACK-10 | Pack methods consume the shared generated wire/client artifacts and receipt. Pack-specific OpKind/PredKind additions are exhaustively enumerated by canonical printer and parser checks. The Go/JS codec, Python model convergence and generated-client profile audit belong to `public-engine-contract-and-release`. | EG-TYPED-PACKS-R068, EG-TYPED-PACKS-R070, EG-TYPED-PACKS-R080 |
| PACK-11 | Served and embedded routes enforce identical tenant, graph and action authority for the methods they expose. A denied pack or component read never leaks importer identity, body bytes or cross-tenant metadata. Long import work observes deadline/cancellation and releases its pack lock. Projection uses a least-privilege grant tied to the outbox row and records failures. | EG-TYPED-PACKS-R012, EG-TYPED-PACKS-R038, EG-TYPED-PACKS-R074, EG-TYPED-PACKS-R084, EG-TYPED-PACKS-R087, EG-TYPED-PACKS-R088 |
| PACK-12 | Pack contracts are verified through planted invalid cases, mutation tests, crash/retry tests, tenant and cluster tests, SDK/client conformance, full Rust/Python suites, and quality/release gates. General Python CI and dispatch-decomposition gates are owned by `public-engine-contract-and-release`; their results are required here as consumer evidence. | EG-TYPED-PACKS-R005, EG-TYPED-PACKS-R075, EG-TYPED-PACKS-R076, EG-TYPED-PACKS-R086 |

## Entry and validation boundaries

The current wire schema constant is `CONNECTOR_PACK_SCHEMA_VERSION = 2`; a version change requires a coordinated generated contract and TCK bump, not an unreviewed rewrite. `Import` is a single operation, not a prepare/commit/abort protocol. Pack and the four agent-layer methods remain `LocalOnly` until a separately specified clustered writer exists. An existing incompatible Agent Library layout is refused with `AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED`; this spec does not promise an offline migration. Only `SKILL.md` is imported as skill content; supporting files require a new bounded contract. Source authorization uses the bound importer with an environment-configured initial default; binding and retirement require `admin:connector-pack`.

## Acceptance scenarios

| Scenario | Expected observable result |
|---|---|
| Identical pack is imported twice, including after a package-only version bump | Second call is `Unchanged`. Head, component revisions, receipt count and outbox remain unchanged. |
| A tool is removed, then restored with the same body | It becomes `Withdrawn`, disappears from new selection, remains available for historical pins, then re-publishes under the same ID. |
| A worker crashes after schema attach and before Agent Library acknowledgement. | Retry attaches the same record as a no-op and advances visibility once. No older record replaces the newer one. |
| A forged or oversized index, unsafe ontology import or unbound importer is submitted. | Return stable refusal with bounded violations and zero catalog mutation. |
| A client signs a generated method body or reads component content | The sender matches contract vectors. Authorized reader receives exact verified bytes, unauthorized reader receives stable denial without bytes. |

## Traceability and ownership

The IDs in the requirement table are stable work identifiers. The full behavior is defined here and in this directory. SDK package construction and connector certification belong in the SDK's own public specs. GraphOS serving and the browser belong in their own repositories. This spec defines the EG boundary those clients must call. Unit and contract proof do not require private documentation, a particular operator deployment, or a live external service. Specific deferred adjacent work is listed in [tasks.md](tasks.md). This prevents a broad deferred-work entry from being mistaken for completed pack behavior.

## Success criteria

Every PACK requirement has an exact merged commit, passing mapped tests, matching generated contract artifacts, and zero new unjustified duplication/complexity. A served-path result covers Import, Status, Content and projection. The visibility gate survives crash and retry. Unauthorized and malformed input never changes durable state. No public client must infer a wire result. Record source revision, command, test count, environment and scenario results in this spec bundle.
These records must establish every required served and durable outcome.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.


## Native prompt blueprints

R001, R002 and R094 also own typed versioned prompt objects, runtime rendering and native blueprint round trips.
Reuse component identity, graph projections, engine bodies and generated operations.
A prompt must pass its pinned schema before rendering or model dispatch.
Optimization emits proposals; existing policy controls adoption.
Repository-source writes require separate approved SDK execution under GraphOS authority.
R094.7 now references merged SDK-GOVERNED-WRITEBACK-R004.5 through R004.12 and GRAPHOS-FLEET-R003.4 through R003.10.
The exact producer revisions and contracts are pinned in plan.md; runtime acceptance still requires the actual served journey.
R002.10 retains its missing AU model-dispatch consumer requirement and call-site prerequisite.
Original pack types, digest vectors, clients, body holders, grace reconciliation and Stable content serving remain mandatory.

<!-- spec-definition-patch:owner-batch-20261012-typed-packs-and-catalog -->
## Owner definitions — 2026-10-12

### `EG-TYPED-PACKS-R100.7` — AU:31

The existing rendered-package path must gate catalog exposure on a `RecordedRunValidationV1`. Bind it to tenant, skill, candidate revision, exact rendered content digest, graph revision, and validation-policy reference `{policy_iri, policy_revision}`. Each sorted unique `run_refs` entry carries `{run_ref, package_content_digest, graph_revision, policy_revision, outcome, evidence_digest}`. `run_set_digest` hashes its canonical encoding. Require `run_set_complete=true`. Each run attests the same package and policy revisions. Missing, failed, partial, stale, mismatched, or cross-tenant evidence refuses publication before `skill_row` returns a publishable package. AU `record_outcome`/`SkillGraphPipeline` remains the run/candidate producer. EG `render_skill_package` and `skill_row` bind and enforce the receipt.

**Hold:** the reviewed AU rows do not define the validation operation, receipt producer fields, run-set selection, or pass criterion. AU must publish those semantics before a passing cross-owner journey can be accepted. Until then absence of a receipt refuses; this does not close the positive producer path. Preserve `AU-HARNESS-R022.2/.5/.6`, `AU-HARNESS-R004.1`, and `EG-TYPED-PACKS-R100.1/.5/.6`. Source hash: `96ff38761f9e21d52f608ba593c902aff4c93ca7b1ed13088ad39efd3e968a3a`.

### `EG-TYPED-PACKS-R097.2.7` — EG:35

External ontology ingestion is directional from the pinned external schema into EG. The import contract covers classes, object/datatype/annotation properties and named individuals. An enumeration is a class with named individuals. Map each declared term by a pinned, importer-authorized, kind-compatible `ExternalTermMappingV1` to a platform IRI. Retain source IRI and kind as provenance. Otherwise, return revision-bound coverage as `Unmapped{reason: NoDeclaredMapping}` and do not adopt the term. Sort coverage by external IRI and kind. Partial unmapped coverage does not invalidate a pack under R097.2. Malformed, duplicate, unknown-kind, incompatible, forged or stale mappings refuse before commit/activation. Validate in `ConnectorPack.Import::validate_and_commit`. Recheck in `apply_pack_source`. Return full per-term coverage on `OntologyInspect`. Use `GraphSchemaClasses` as independent readback for adopted classes and non-adoption of unmapped classes. Keep R071.1 as the reverse EG-class-to-external-vocabulary alignment; do not broaden it.

Preserve `EG-TYPED-PACKS-R051`, `EG-UNIFIED-DATA-PLANE-R005/R037`, `EG-TYPED-PACKS-R071.1.1-.6`, and `EG-TYPED-PACKS-R097.2.1-.6`. Source hash: `318723aee15c476039eca669a3230b206532c0c704b37efad5a7c49690bbb9c2`.

### `EG-TYPED-PACKS-R100.8` — EG:56

Add `ExportWorkflowRdfV1(workflow_iri, requested_workflow_version, stored_version_ref) -> WorkflowRdfDocumentV1` and `ImportWorkflowRdfV1(document) -> WorkflowImportReceiptV1{workflow_iri, committed_workflow_revision, graph_revision, dataset_digest, importer_id}` to the existing skill-package owner. The versioned `WorkflowRdfDocumentV1` contains workflow/skill IDs and workflow/stored-version/graph/ontology revisions. It also contains canonical N-Quads, dataset digest and serializer identity. `dataset_digest` is SHA-256 of the canonical UTF-8 N-Quads bytes selected by the document-pinned serializer. Export preserves every workflow triple, named graph, typed/language-tagged literal, ontology IRI and unknown workflow predicate. Import validates the canonical-byte dataset digest and restores an isomorphic graph through the existing graph write boundary. Keep the original strict-newer check at export.

**Hold:** source review does not identify the stored workflow version record to compare or its ordering grammar. It also does not identify the existing R100 consumer import operation. Do not shift comparison to import. Do not claim a complete round-trip until the owner decisions and exact attach points are identified. Preserve `EG-TYPED-PACKS-R051/R090/R100.1-.6`. Source hash: `620b1ae05d8e35696c7522f207672ef6f374525004d5488fe62fb4b27fc6cf5a`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R069

Preserve the existing `R069` obligation and all prior children; add `EG-TYPED-PACKS-R069.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R069.100` — AU:69

Define `FingerprintIdentityV1` with repository id, branch/ref id, normalized path and commit/source revision. Include blob/raw digest, language id, parser implementation/version, normalization-policy revision and grammar digest. The versioned profile registry lists each supported language and the trivia/comment forms removed from its canonical token stream. Preserve strings, literals, token order and all non-trivia tokens. Hash the canonical token stream with language/parser/profile versions. Unsupported languages use raw-content digest only. Never classify them as cosmetic. An identical complete source identity and profile is idempotent. The same blob at a new branch, path or source revision MUST create/update that source binding and provenance. Reuse parsed output only under identical parser/profile identity; a matching raw digest alone is never a full no-op. A changed parser or normalization profile forces reparse and fingerprint recomputation. Then update the binding even if semantic content remains equivalent. Control semantic graph mutation by the recomputed semantic fingerprint. Cosmetic-only and profile-only changes with equal canonical semantics update source provenance without changing semantic graph revision. Structural changes update both. SDK transports source identity/raw digest only. EG owns semantic classification.

Producer/consumer: RepositorySnapshotProvider -> SDK transport -> generated IndexRepository -> EG parser/index mutation; R079 embedding dedupe is not structural-mutation suppression.

Source record SHA-256: `8ca81dc16448fefc73440f2eb9ea07b2e433f7e38075563f86800071029846e9`. Verification remains planned until independently served by each named owner.

### Extension of R100

Preserve the existing `R100` obligation and all prior children; add `EG-TYPED-PACKS-R100.100`, `EG-TYPED-PACKS-R100.101`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R100.100` — AU:90

Define an edit representation bound to the original SKILL.md byte digest and source revision. Bind it to frontmatter field spans, step spans, line endings and unknown/custom-field spans. Apply edits as bounded source-range patches. Do not parse and reserialize the entire document. Preserve all untouched bytes, ordering, spacing, comments, line endings and unknown frontmatter fields. Emit a new content digest and source provenance. Continue to enforce R019 SKILL.md-only import and R029 restricted YAML validation.

Producer/consumer: EG owns skill parse/admission and R100 rendering; SDK owns source transport/edit write-back edge. No prompt blueprint or whole-document normalization path is substituted.

Source record SHA-256: `1b717c4a284896dbf8c7b4fb1f00ebba80fa6a75e930a020c5bc01706634f582`. Verification remains planned until independently served by each named owner.

#### `EG-TYPED-PACKS-R100.101` — AU:98

Define an admission record bound to prior/new skill content digests and consumer-declared input/output behavior contract revisions. Include the observed consumer cohort, compatibility verdict, migration id/digest and admission revision. Check compatibility against consuming-code behavior. Package version equality, contract-pin equality, endpoint certification or dependency digest alone do not establish compatibility. Refuse breaking changes before publication or invocation unless a declared migration binds the exact old/new digests and consumer contract. Require the migration receipt to pass.

Producer/consumer: EG skill package read/admission -> GraphOS invoke certification and consumer code; GraphOS owns its invocation gate. Do not equate existing version/dependency pins with compatibility.

Source record SHA-256: `531cb0ba1accd9e88833249fb253a9f5f7a3c1a911f653b3c9a5bfc42341f978`. Verification remains planned until independently served by each named owner.

### Extension of R001

Preserve the existing `R001` obligation and all prior children; add `EG-TYPED-PACKS-R001.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R001.100` — AU:52

Extend existing ConnectorPack prompt and tool definition entries. Include explicit schema version, nested field schema, required/optional status and compatibility revision. Incremental evolution records base digest, changed paths and compatibility verdict. Admit additive optional fields only when consumer policy says compatible. Removed or changed required fields refuse before publication. Pack index/entry digest rules remain canonical. SDK owns tool execution/schema compatibility authority. EG records and serves the versioned declaration and does not authorize SDK calls.

Producer/consumer: EG ConnectorPack typed authority -> generated public pack APIs; SDK retains tool compatibility and execution authority. Preserve R001 index/entry/section/annotation and digest contract.

Source record SHA-256: `e03b25156a4f84d27c4afba63d0cd9e526b549b5905994a53664d193416d048c`. Verification remains planned until independently served by each named owner.

### Extension of R072

Preserve the existing `R072` obligation and all prior children; add `EG-TYPED-PACKS-R072.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R072.100` — AU:97

Extend the typed fleet catalog with workflow registration identity, version/content digest, owner/tenant scope, schema and lifecycle state. Registration is persisted through the existing SourceIngest authority; invocation resolves by registry id and exact admitted revision rather than a caller-supplied body. Unknown, withdrawn, stale or unauthorized workflow ids refuse before execution. Engine/client initialization remains lazy at the AU first-use boundary and is not performed at registration or process startup.

Producer/consumer: EG typed registry and SourceIngest R002 own durable registration; AU owns lazy client initialization/execution; preserve GRAPHOS-HOST-R022 workflow-store semantics.

**Open owner prerequisite:** AU-CONTROL-R037.5 only fills a requirement gap; it does not define lazy initialization timing, the first-use operation, or the execution receipt. No exact AU companion requirement ID is identified. Keep that consumer as an explicit hold until its owner publishes the operation and receipt; no invocation evidence is claimed.
Source record SHA-256: `f73767d1a3fdba29a702254ed1db2c9904158b6136f6f4b38a3c9a0d3e4fcb03`. Verification remains planned until independently served by each named owner.

### Extension of R071

Preserve the existing `R071` obligation and all prior children; add `EG-TYPED-PACKS-R071.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R071.100` — EG:8

Define one versioned concept-path grammar and canonicalization rule shared by EG vocabulary producers. Include stable segment identity, parent path and canonical concept IRI. Resolve the entire path against the pinned vocabulary revision during admission. Check repository-wide uniqueness and parent/child consistency. Refuse duplicate aliases, paths that contradict an existing parent/child relationship, or two IRIs for the same canonical path. Return the conflicting ids. Keep existing published ontology IRIs unchanged and crosswalk them rather than rewriting them.

Producer/consumer: EG vocabulary validation owns canonical identity; repository producers submit proposed paths. Preserve existing published IRIs and federated query vocabulary.

**Open owner decision:** The canonical segment grammar is not approved for separators/escaping, Unicode normalization, case, or whitespace. Keep admission SPECIFIED until the vocabulary owner publishes one versioned grammar shared by producers; preserve existing published IRIs.
Source record SHA-256: `525e39d1d194ac8a820bbd741a1c261ceab0c7d564981c33c731c06c1eb641d8`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->
