# Typed packs and catalog authority

| Field | Value |
|---|---|
| Stable spec ID | `typed-packs-and-catalog` |
| Owner | `epistemic-graph` |
| State | `PROPOSED` (specification only; implementation and acceptance require evidence) |
| Scope | Typed connector and agent components, content packs, schema composition, fleet/catalog authority, generated contract receipts |

## State legend

`PROPOSED` means this complete design is open for review. `READY` means contracts and tests have been reviewed. `IMPLEMENTED` means code exists on a branch. `LANDED` means an exact commit is merged into the owning repository's default branch. `ACCEPTED` means the exact landed commit passes the tests and release gates in [test-spec.md](test-spec.md), including served behavior where required. `DEFERRED` means a requirement is intentionally queued; `REJECTED` means a ruled alternative must not be built. A prior ledger label such as `BUILT`, `LANDED`, or `CLOSED` is historical classification, not proof of this spec's acceptance. Each requirement can have its own state and evidence. No requirement is marked `ACCEPTED` in this revision.

## Outcome and user stories

1. A connector author can publish one deterministic snapshot of tools, skills, prompts, ontology, shapes, model profiles, A2A cards, and its manifest. Reimporting identical content is a no-op; a changed or withdrawn entry has a predictable lifecycle.
2. A tenant administrator can bind the one allowed importer, inspect the head and projection status, retire entries, attach a visible pack schema to a graph, and safely re-drive a failed projection.
3. A consumer can read a typed component and its engine-owned body, trust the generated method/result contract and receipt, and see only a projection that passed its readiness gate.
4. A contributor can add a pack kind or catalog method by extending the existing type/registry/handler path and proving wire, storage, authorization, replay, client, and release parity.

## Normative requirements

| ID | Contract | Stable obligations |
|---|---|---|
| PACK-01 | One `ConnectorPackIndex` identifies a connector, exactly one server entry, sorted unique typed entries and bounded archive sections. EG recomputes raw-body SHA-256 and domain-framed entry, annotation, reference and pack digests; mismatches are refused. Archive layout and package version do not change the semantic pack digest. Keep golden vectors at `contract/fixtures/connector_pack_digest_vectors.json`. | EH-075, EH-106–EH-110 |
| PACK-02 | `ConnectorPack` uses typed `Status`, `Import`, `Bind`, `Unbind`, `Retire`, `Reproject`, `ReconcileBodies`, and catalog reads. `AgentComponent.Content` supplies a verified body through the existing component method. Every operation has a declared result, action, durability and routing policy; dynamic result fallbacks are forbidden. | EH-075, EH-082, EH-093, EH-237 |
| PACK-03 | Import checks verified tenant and importer binding, expected head, archive, validation and policy before writing. Engine bodies may be staged first. One Agent Library owner transaction commits components, memberships, body holders, head, import record, receipt and ordered outbox intents. A crash before commit leaves only reclaimable orphan bodies; a crash during commit exposes no partial head. A lost reply replays by operation identity; changed bytes under the same idempotency key are refused. | EH-077, EH-078, EH-083, EH-085, EH-127 |
| PACK-04 | A pack is a content snapshot. Identical head digest returns `Unchanged` with no version bump. An unchanged entry and pins carry forward; changed content or a changed server pin creates a revision. Absence becomes reversible `Withdrawn`; explicit admin retirement is permanent. New candidate/search reads hide withdrawn and unready records, while historical pinned reads remain resolvable. Direct component writes to reserved pack IDs are refused. | EH-089, EH-091, EH-127, EH-135, EH-136 |
| PACK-05 | Each pack entry's references resolve only to a sibling of the stated kind, without cycles. Validation and the atomic writer both resolve pins against staged rows before stored rows. The server pin binds declared contract version, name and instructions; package version is provenance only. `contract_pin` is an SDK claim, and EG must not pretend to recompute SDK normalization. | EH-091, EH-122, EH-135, EH-136, EH-198, EH-199 |
| PACK-06 | Validation applies rules G1–G21 in [architecture.md](architecture.md), collecting at most 256 violations and reporting budget exhaustion. No rejected import changes the head, members, holders, body liveness, record, receipt or outbox. Bounded parsers, no remote ontology import, and step-limited reasoning are mandatory. | EH-078, EH-106–EH-126, EH-587 |
| PACK-07 | An Agent Library outbox worker projects the current committed head into a reserved `pack__<connector>` graph. One graph-schema attach carries the pack record revision; a subsequent Agent Library transaction marks `visible_record_id` and acknowledges the outbox item. Read paths filter against visibility. A failed projection is visible, rejected for retry without losing later work, and can be reprojected. A stale worker cannot overwrite a newer source. | EH-081, EH-137, EH-145, EH-146, EH-148–EH-153, EH-370 |
| PACK-08 | Graph schema is a bounded map of named sources. Each source carries shapes and ontology bytes, origin and revision; the composed digest and policy are derived. K1–K7 conflict/consistency rules in [architecture.md](architecture.md) apply on attach, detach and pack projection. `IcvConfigure` maps to the operator source. `GraphSchema.List` and `GraphSchemaClasses` expose versioned source/class/property facts. Attach to another graph is a snapshot whose staleness is visible. | EH-137, EH-164, EH-166, EH-337, EH-389 |
| PACK-09 | EG is the typed catalog authority: `RegisterServer`/ServerRegistry owns liveness; `ConnectorPack`/`AgentComponent` owns content and lifecycle. The SDK creates/imports packs; GraphOS joins typed reads for serving. No SQL shadow catalog or independent AU content authority may appear. Catalog reads carry tenant, visibility, contract and version provenance. | EH-128–EH-134, EH-200, EH-345, EH-377 |
| PACK-10 | Method descriptors in `eg-capabilities` generate all wire/client artifacts and a receipt. The committed receipt, generated Rust digest, Python/Go/JS clients and method-body signing vectors must match the registry by content, not file size. New methods are exhaustively enumerated by canonical printer, parser and OpKind/PredKind checks. | EH-266, EH-324, EH-372, EH-377, EH-440, EH-643 |
| PACK-11 | Served and embedded routes enforce identical tenant, graph and action authority for the methods they expose. A denied pack or component read never leaks importer identity, body bytes or cross-tenant metadata. Long import work observes deadline/cancellation and releases its pack lock. Projection uses a least-privilege grant tied to the outbox row and records failures. | EH-087, EH-124, EH-370, EH-536, EH-591, EH-635 |
| PACK-12 | Pack contracts are verified through planted invalid cases, mutation tests, crash/retry tests, tenant and cluster tests, SDK/client conformance, full Rust/Python suites, and quality/release gates. Neither a generated artifact check alone nor a branch-local green result is acceptance. | EH-080, EH-371, EH-376, EH-430, EH-468, EH-587 |

## Entry and validation boundaries

The current wire schema constant is `CONNECTOR_PACK_SCHEMA_VERSION = 2`; a version change requires a coordinated generated contract and TCK bump, not an unreviewed rewrite. `Import` is a single operation, not a prepare/commit/abort protocol. Pack and the four agent-layer methods remain `LocalOnly` until a separately specified clustered writer exists. An existing incompatible Agent Library layout is refused with `AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED`; this spec does not promise an offline migration. Only `SKILL.md` is imported as skill content; supporting files require a new bounded contract. Source authorization uses the bound importer with an environment-configured initial default; binding and retirement require `admin:connector-pack`.

## Acceptance scenarios

| Scenario | Expected observable result |
|---|---|
| Identical pack is imported twice, including after a package-only version bump | Second call is `Unchanged`; head, component revisions, receipt count and outbox remain unchanged. |
| A tool is removed, then restored with the same body | It becomes `Withdrawn`, disappears from new selection, remains available for historical pins, then re-publishes under the same ID. |
| A worker crashes after schema attach and before Agent Library acknowledgement | Retry attaches the same record as a no-op and advances visibility once; no older record replaces the newer one. |
| A forged or oversized index, unsafe ontology import or unbound importer is submitted | Stable refusal with bounded violations and zero catalog mutation. |
| A client signs a generated method body or reads component content | The sender matches contract vectors; authorized reader receives exact verified bytes, unauthorized reader receives stable denial without bytes. |

## Traceability and ownership

The IDs in the requirement table are stable work identifiers; the full behavior is defined here and in this directory. SDK package construction and connector certification belong in the SDK's own public specs. GraphOS serving and the browser belong in their own repositories. This spec defines the EG boundary those clients must call; it does not require private documentation, a particular operator deployment, or a live external service for unit and contract proof. Specific deferred adjacent work is listed in [tasks.md](tasks.md) to prevent a broad program row from being mistaken for completed pack behavior.

## Success criteria

Every PACK requirement has an exact merged commit, passing mapped tests, matching generated contract artifacts, zero new unjustified duplication/complexity, and a served-path result for Import, Status, Content and projection. The visibility gate survives crash and retry; unauthorized and malformed input never changes durable state; no public client must infer a wire result. Acceptance evidence records revision, command, test count, environment and result in a future `evidence.md` before this state can change to `ACCEPTED`.
