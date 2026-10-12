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
3. A consumer can read a typed component and its engine-owned body, trust the generated method/result contract and receipt, and see only a projection that passed its readiness gate.
4. A contributor can add a pack kind or catalog method by extending the existing type/registry/handler path and proving wire, storage, authorization, replay, client, and release parity.

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
| A worker crashes after schema attach and before Agent Library acknowledgement | Retry attaches the same record as a no-op and advances visibility once; no older record replaces the newer one. |
| A forged or oversized index, unsafe ontology import or unbound importer is submitted | Stable refusal with bounded violations and zero catalog mutation. |
| A client signs a generated method body or reads component content | The sender matches contract vectors. Authorized reader receives exact verified bytes, unauthorized reader receives stable denial without bytes. |

## Traceability and ownership

The IDs in the requirement table are stable work identifiers; the full behavior is defined here and in this directory. SDK package construction and connector certification belong in the SDK's own public specs. GraphOS serving and the browser belong in their own repositories. This spec defines the EG boundary those clients must call; it does not require private documentation, a particular operator deployment, or a live external service for unit and contract proof. Specific deferred adjacent work is listed in [tasks.md](tasks.md) to prevent a broad deferred-work entry from being mistaken for completed pack behavior.

## Success criteria

Every PACK requirement has an exact merged commit, passing mapped tests, matching generated contract artifacts, zero new unjustified duplication/complexity, and a served-path result for Import, Status, Content and projection. The visibility gate survives crash and retry; unauthorized and malformed input never changes durable state; no public client must infer a wire result. Record source revision, command, test count, environment and scenario results in this spec bundle.
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
## Reviewed owner residuals — 2026-10-12

### `EG-TYPED-PACKS-R100.7` — AU:31

Original objective: Accumulated graph knowledge about a task domain is distilled into a versioned skill package that the platform publishes only after validation against recorded runs; unvalidated packages are refused.

EG-owned residual: A content digest/graph revision supplies package identity evidence, but neither rendering nor minimum rollout count validates that exact resulting package against recorded runs before publication. The required publication refusal remains absent.

Producer/consumer boundary: AU record_outcome/SkillGraphPipeline yields candidates; EG render_skill_package is called by fleet catalog skill_row and binds graph revision/content digest. No reviewed recorded-run validation receipt gates publication of that exact rendered package.

Preserve existing coverage: `AU-HARNESS-R022.2`, `AU-HARNESS-R022.5`, `AU-HARNESS-R022.6`, `AU-HARNESS-R004.1`, `EG-TYPED-PACKS-R100.1`, `EG-TYPED-PACKS-R100.5`, `EG-TYPED-PACKS-R100.6`.

Owner source hash: `96ff38761f9e21d52f608ba593c902aff4c93ca7b1ed13088ad39efd3e968a3a`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-TYPED-PACKS-R097.2.7` — EG:35

Original objective: External ontology and knowledge-graph schemas are ingested and normalized into the platform ontology through declared mappings; an unmapped term is reported and not silently adopted.

EG-owned residual: R097.2 covers selected pack classes in named domains and R071.1 crosswalks point EG classes to target vocabularies. They do not establish general external ontology/KG schema normalization into platform terms, including non-class terms. A valid pack may retain unmapped declared classes; that is not a blanket refusal of all unmapped external-term ingestion.

Producer/consumer boundary: Connector candidates -> ConnectorPack.Import validate_and_commit -> apply_pack_source -> OntologyInspect/GraphSchemaClasses. Reverse-direction external normalization is not implied by existing SeeAlso/SKOS links.

Preserve existing coverage: `EG-TYPED-PACKS-R051`, `EG-UNIFIED-DATA-PLANE-R005`, `EG-UNIFIED-DATA-PLANE-R037`, `EG-TYPED-PACKS-R071.1.1`, `EG-TYPED-PACKS-R071.1.2`, `EG-TYPED-PACKS-R071.1.3`, `EG-TYPED-PACKS-R071.1.4`, `EG-TYPED-PACKS-R071.1.5`, `EG-TYPED-PACKS-R071.1.6`, `EG-TYPED-PACKS-R097.2.1`, `EG-TYPED-PACKS-R097.2.2`, `EG-TYPED-PACKS-R097.2.3`, `EG-TYPED-PACKS-R097.2.4`, `EG-TYPED-PACKS-R097.2.5`, `EG-TYPED-PACKS-R097.2.6`.

Owner source hash: `318723aee15c476039eca669a3230b206532c0c704b37efad5a7c49690bbb9c2`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-TYPED-PACKS-R100.8` — EG:56

Original objective: Skill workflows are exported as versioned OWL/RDF documents that the engine imports back losslessly; an export whose version is not newer than the stored one is refused.

EG-owned residual: R100 rendering does not define lossless OWL/RDF workflow export/import. No strictly-newer version refusal at export is defined; do not silently move this unusual original clause to import.

Producer/consumer boundary: Fleet catalog skill_row -> render_skill_package -> package consumer; inverse workflow import/version admission remains unbound.

Preserve existing coverage: `EG-TYPED-PACKS-R051`, `EG-TYPED-PACKS-R090`, `EG-TYPED-PACKS-R100`, `EG-TYPED-PACKS-R100.1`, `EG-TYPED-PACKS-R100.2`, `EG-TYPED-PACKS-R100.3`, `EG-TYPED-PACKS-R100.4`, `EG-TYPED-PACKS-R100.5`, `EG-TYPED-PACKS-R100.6`.

Owner source hash: `620b1ae05d8e35696c7522f207672ef6f374525004d5488fe62fb4b27fc6cf5a`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R069

Preserve the existing `R069` obligation and all prior children; add `EG-TYPED-PACKS-R069.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R069.100` — AU:69

Define `fingerprint_profile` as language id, parser implementation/version, normalization-policy version and grammar digest. The versioned profile registry explicitly lists each supported language and the trivia/comment forms removed from its canonical token stream; strings, literals, token order and all non-trivia tokens are preserved. Hash the canonical token stream together with language/parser/profile versions. Unsupported languages use raw-content digest only and are never classified as cosmetic. On parser/profile revision change, force reparse and recompute; compare under the new profile before deciding whether semantic graph mutation is needed, while always writing the new source revision/raw digest/provenance. Identical raw digest is a full no-op. SDK transports raw revision/digest only; EG owns semantic classification.

Producer/consumer: RepositorySnapshotProvider -> SDK transport -> generated IndexRepository -> EG parser/index mutation; R079 embedding dedupe is not structural-mutation suppression.

Source record SHA-256: `8ca81dc16448fefc73440f2eb9ea07b2e433f7e38075563f86800071029846e9`. Verification remains planned until independently served by each named owner.

### Extension of R100

Preserve the existing `R100` obligation and all prior children; add `EG-TYPED-PACKS-R100.100`, `EG-TYPED-PACKS-R100.101`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R100.100` — AU:90

Define an edit representation bound to the original SKILL.md byte digest, source revision, frontmatter field spans, step spans, line endings and unknown/custom-field spans. Apply edits as bounded source-range patches; do not parse and reserialize the entire document. Preserve all untouched bytes, ordering, spacing, comments, line endings and unknown frontmatter fields; emit new content digest and source provenance. Continue to enforce R019 SKILL.md-only import and R029 restricted YAML validation.

Producer/consumer: EG owns skill parse/admission and R100 rendering; SDK owns source transport/edit write-back edge. No prompt blueprint or whole-document normalization path is substituted.

Source record SHA-256: `1b717c4a284896dbf8c7b4fb1f00ebba80fa6a75e930a020c5bc01706634f582`. Verification remains planned until independently served by each named owner.

#### `EG-TYPED-PACKS-R100.101` — AU:98

Define an admission record bound to prior/new skill content digests, consumer-declared input/output behavior contract revisions, observed consumer cohort, compatibility verdict, migration id/digest and admission revision. Compatibility is checked against consuming-code behavior, not package version equality, contract-pin equality, endpoint certification or dependency digest alone. A breaking change is refused before publication/invocation unless a declared migration is bound to the exact old/new digests and consumer contract and its migration receipt passes.

Producer/consumer: EG skill package read/admission -> GraphOS invoke certification and consumer code; GraphOS owns its invocation gate. Do not equate existing version/dependency pins with compatibility.

Source record SHA-256: `531cb0ba1accd9e88833249fb253a9f5f7a3c1a911f653b3c9a5bfc42341f978`. Verification remains planned until independently served by each named owner.

### Extension of R001

Preserve the existing `R001` obligation and all prior children; add `EG-TYPED-PACKS-R001.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R001.100` — AU:52

Extend existing ConnectorPack prompt and tool definition entries with explicit schema version, nested field schema, required/optional status and compatibility revision. Incremental evolution records base digest, changed paths and compatibility verdict; additive optional fields may be admitted only when consumer policy says compatible, while removed/changed required fields refuse before publication. Pack index/entry digest rules remain canonical. SDK owns tool execution/schema compatibility authority; EG records and serves the versioned declaration and does not authorize SDK calls.

Producer/consumer: EG ConnectorPack typed authority -> generated public pack APIs; SDK retains tool compatibility and execution authority. Preserve R001 index/entry/section/annotation and digest contract.

Source record SHA-256: `e03b25156a4f84d27c4afba63d0cd9e526b549b5905994a53664d193416d048c`. Verification remains planned until independently served by each named owner.

### Extension of R072

Preserve the existing `R072` obligation and all prior children; add `EG-TYPED-PACKS-R072.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R072.100` — AU:97

Extend the typed fleet catalog with workflow registration identity, version/content digest, owner/tenant scope, schema and lifecycle state. Registration is persisted through the existing SourceIngest authority; invocation resolves by registry id and exact admitted revision rather than a caller-supplied body. Unknown, withdrawn, stale or unauthorized workflow ids refuse before execution. Engine/client initialization remains lazy at the AU first-use boundary and is not performed at registration or process startup.

Producer/consumer: EG typed registry and SourceIngest R002 own durable registration; AU owns lazy client initialization/execution; preserve GRAPHOS-HOST-R022 workflow-store semantics.

Source record SHA-256: `f73767d1a3fdba29a702254ed1db2c9904158b6136f6f4b38a3c9a0d3e4fcb03`. Verification remains planned until independently served by each named owner.

### Extension of R071

Preserve the existing `R071` obligation and all prior children; add `EG-TYPED-PACKS-R071.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-TYPED-PACKS-R071.100` — EG:8

Define one versioned concept-path grammar and canonicalization rule shared by EG vocabulary producers, with stable segment identity, parent path and canonical concept IRI. Admission resolves the entire path against the pinned vocabulary revision and checks repository-wide uniqueness plus parent/child consistency. Duplicate aliases, a path that contradicts an existing parent/child relationship, or two IRIs for the same canonical path refuse with the conflicting ids; existing published ontology IRIs remain unchanged and are crosswalked rather than rewritten.

Producer/consumer: EG vocabulary validation owns canonical identity; repository producers submit proposed paths. Preserve existing published IRIs and federated query vocabulary.

Source record SHA-256: `525e39d1d194ac8a820bbd741a1c261ceab0c7d564981c33c731c06c1eb641d8`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->
