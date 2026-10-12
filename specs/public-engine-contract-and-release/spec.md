# EG-CONTRACT — Public engine contract and reproducible clients

**Owner:** epistemic-graph. **Specification state:** PROPOSED. **Build state:** PARTIAL. **Acceptance:** PENDING.

## Purpose

A contributor must be able to change an engine method once and regenerate the same typed, signed, documented contract for Rust, Python, Go and JavaScript. An application must be able to discover callable methods and stable errors from a published EG package without importing private server internals. A release must prove that the published artifact behaves like the source revision.

## Delivery state legend

- **PROPOSED:** this reviewed contract describes work but gives no implementation credit.
- **QUEUED / BUILDING / BUILT:** implementation is assigned, in progress, or demonstrated on an unmerged branch. None implies a public artifact.
- **LANDED:** an exact commit containing the implementation is an ancestor of the public default branch, with a linked source test result.
- **ACCEPTED:** the same exact revision passes every required package, consumer, quality and release test in [test-spec.md](test-spec.md). Evidence is recorded in [tasks.md](tasks.md).
- **PARTIAL:** some requirements have landed source while other requirements or release evidence remain. Record each requirement separately; do not promote the whole spec from a passing subset.

The current repository includes a generated method catalog, error catalog, Python typed models, Go and JavaScript codec clients and a package contract receipt. Their presence establishes a partial source baseline only. No release acceptance is claimed here.

[PR #3](https://github.com/Knuckles-Team/epistemic-graph/pull/3) merged a source repair at [a7f4dd74e7b2](https://github.com/Knuckles-Team/epistemic-graph/commit/a7f4dd74e7b2d01832fc9c3359046255d0e606d4). This establishes source landing for that repair only. The EG-CONTRACT-R014 wheel and consumer proof, EG-CONTRACT-R022 duplication gate, EG-CONTRACT-R023 hooks, and exact-revision release acceptance remain open under this spec's test contract.

## Normative requirements

| ID | Required behavior | Verifiable result |
|---|---|---|
| EG-C-01 | Rust method declarations are the sole authority for method identity, request/result schemas, stability, policy, replay class, consumer profiles, `is_wire_callable` and `error_set`. | `contract/methods.json` and schemas regenerate byte for byte; every declared wire method is reachable through an intentional dispatch path. |
| EG-C-02 | Rust `ErrorCode` is the sole authority for stable error code, class, retryability and HTTP status hint. Every method error set contains its actual reachable errors, including transaction conflict rather than a generic internal error. | `contract/errors.json` regenerates byte for byte and induced failures return the declared code at every supported client surface. |
| EG-C-03 | The Python wheel contains the method, error and scope catalogs, schemas and a digest receipt, with a public loader that rejects a missing or mismatched file. | A clean wheel install provides the same digest and schema as the source revision without source-tree reads. |
| EG-C-04 | Python request and result decoding uses generated strict nested models with unknown fields refused. Handwritten convenience modules re-export generated types instead of defining competing wire shapes. | One class per wire shape and a typed round trip for every callable method. |
| EG-C-05 | Go and JavaScript sign the exact canonical method body produced by the Rust codec. They must not independently reimplement canonicalization. | Both replay all `contract/fixtures/method_body_vectors.json` fixtures and agree with the Python/Rust body digest. |
| EG-C-06 | Embedded WASM codec artifacts derive from a declared Rust source revision and deterministic toolchain recipe. A change in source or toolchain refreshes both copies and receipt; size is measured, not silently suppressed. | `scripts/build_method_codec_wasm.py --check` succeeds and Go/JS tests use the checked-in bytes. |
| EG-C-07 | A released artifact carries an exact source revision and catalog digest. The installed clients prove the same method/error behavior as source tests. | Rebuild, wheel completeness/privacy, consumer and package smoke evidence all identify the same commit. |
| EG-C-08 | A public PR gate uses vendored fixtures, temporary directories and provisioned local services where needed. No test requires a preexisting private deployment or a live operator host. Expensive soak and deployment tests run separately with documented provisioning. | A fresh checkout can run the blocking gate in CI, while skipped external integrations are reported as separate nonacceptance evidence. |
| EG-C-09 | CCCC, KISS, jscpd and Dupehound checks examine only relevant source and fixtures with committed configurations. A changed source tree introduces no new unsanctioned duplicate implementations or complexity regressions. | Differential scanner results and normal hosted checks pass on the exact release tree. |
| EG-C-10 | The installed Python client has a served Agent Library journey that publishes, reads and imports a pack, then runs a Decide request through the actual server and authorization boundary. | The hosted job starts a disposable engine and runs both authorized and denied cases against the installed wheel, with no source-tree imports or live service dependency. |

## Architecture and ownership

The generator in `crates/eg-capabilities/src/bin/gen_contract.rs` reads Rust method and error declarations and writes `contract/`. `scripts/generated_artifacts.py` and the contract receipt detect drift. `epistemic_graph/contract/` is the package projection. `epistemic_graph/generated/models.py` is the Python type projection. `clients/go/codec.go` and `clients/js/codec.mjs` execute the embedded Rust codec WASM. The server owns dispatch and policy; clients only encode, send and decode. The HTTP hint is presentation metadata, never the policy decision itself.

The build graph is: Rust declarations → generated catalogs/schemas/fixtures → Rust codec WASM → Go/JS embedding and vector tests → Python wheel with receipt → clean-install consumer tests → release gate. Each arrow must have a deterministic check. Do not create a second method registry, error classifier, Python model hierarchy or signed-body encoder.

External applications consume only the published public catalog and clients. They may project EG methods into their own HTTP/MCP surfaces, but authorization must be checked by that application's own principal-aware service layer; `is_wire_callable` describes an EG capability, not a grant.

## Stable obligation coverage

| Group | IDs and disposition in this spec |
|---|---|
| Client contract baseline | EG-CONTRACT-R001 strict models; EG-CONTRACT-R004 signed Go/JS vectors. Verify exact default-branch source and package evidence. |
| Consumer convergence and tree hygiene | EG-CONTRACT-R002 Agent Library served-Python substrate. EG-CONTRACT-R005 generated Python convergence. EG-CONTRACT-R006 codec WASM. EG-CONTRACT-R007 dispatch gate. EG-CONTRACT-R008 correct tree selection. EG-CONTRACT-R009 root Python CI suite. EG-CONTRACT-R014 error catalog. EG-CONTRACT-R016 generated client parity. EG-CONTRACT-R017 compiler dispatch census. EG-CONTRACT-R018 feature matrix. EG-CONTRACT-R019 current dispatch reachability. EG-CONTRACT-R020 component naming. EG-CONTRACT-R021 client lifecycle. EG-CONTRACT-R022 duplicate scan. EG-CONTRACT-R023 full tree hygiene. |
| Packaging and tooling compatibility | EG-CONTRACT-R015 contract packaging and `is_wire_callable`; EG-CONTRACT-R011 bounded CI fanout; EG-CONTRACT-R012 and EG-CONTRACT-R013 KISS parser/config compatibility. |
| Cross-cutting | EG-CONTRACT-R003 wheel privacy; EG-CONTRACT-R010 complexity scoring. |

Adjacent EG capabilities specify their own semantics. This spec governs their published method/error projection and release proof. This table groups the obligations tracked when this spec was last reviewed; [requirements.md](requirements.md) is the authoritative full set.

## Completion criteria

All EG-C requirements have linked implementation commits, focused tests and an exact revision release matrix in [tasks.md](tasks.md). The published wheel and both embedded client codecs share one contract digest. Unknown/invalid payloads and policy failures have typed error parity. Hosted gates pass without private environment dependencies. Only then set **ACCEPTED**.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.

## Current architecture contract

EG-CONTRACT-R052 owns the source-linked architecture map in [plan.md](plan.md#2026-10-12-current-architecture-boundaries).
The map preserves existing subsystem owners and distinguishes design, source wiring, generated delivery state, and execution evidence.
It changes no architecture, enabled feature, package recipe, or release version.

<!-- spec-definition-patch:owner-batch-20261012-public-engine-contract-and-release -->
## Reviewed owner residuals — 2026-10-12

### `EG-CONTRACT-R001.1` — AU:109

Original source objective: Agents use typed models as their primary data model, with construction, validation and serialization throughout the agent lifecycle.
Original intake fields (exact source record array): ["AU-HARNESS","Agents use typed models as their primary data model, with construction, validation and serialization throughout the agent lifecycle.","mechanism","(blank)","prompts/conversations/old/pydantic-native.txt","LOW","WRITE"]

**Contract definition:** EG SHALL export strict generated request, result, identity, and decision models, including their complete nested type closure, with a manifest containing the contract revision, schema digest, and exported type identifiers. Construction, deserialization, and serialization SHALL use the same pinned generated schema. Unknown nested fields, invalid enum values, incompatible revisions, and invalid scalar types SHALL fail with a field path before dispatch or persistence. AU owns AgentLifecycleState and its transition rules; its state composes these generated EG types with HarnessRequest, phase, tool requests/results, and RunOutcome. EG SHALL NOT define a second AU lifecycle state machine or permit an unvalidated dictionary to replace an admitted generated value. Rehydration SHALL validate the persisted schema revision and every nested generated value before another agent step can run.

**Production wiring:** Extend the existing EG contract generator and its generated-model manifest; consume those exports in AU HarnessRequest/run_agent and the proposed AU-owned AgentLifecycleState. Tests must cross the generated-client and actual AU execution/persistence boundaries.

**Planned acceptance:** Proposed integration case lifecycle_generated_types_survive_run_and_restart: the generated client enters the real AU run_agent path, transitions through tool request and result to RunOutcome, persists state, restarts, and resumes using the same typed values. Unknown nested fields, wrong scalar types, stale schema revisions, and malformed persisted values each refuse before the next step; AU separately refuses an illegal phase transition. Read back the persisted and resumed values independently and compare every field. A generated-model unit round trip alone does not pass.

Preserve existing coverage: `AU-HARNESS-R007`, `EG-CONTRACT-R001`, `AU-HARNESS-R008`.

Source record SHA-256: `c0d92285a141dcaac6817bcac25ffa438517afeba50be716211751f3a37a31c2`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/public-engine-contract-and-release/requirements.md:5` — `EG-CONTRACT-R001`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:12` — `AU-HARNESS-R007`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:13` — `AU-HARNESS-R008`

Open owner prerequisites:
- AU-HARNESS-R007.40 is unmerged. Lifecycle completion requires the AU-owned state, transitions, persistence, and real consumer integration; this EG export clause alone cannot close AU:109.

Unmerged consumer dependencies:
- `AU-HARNESS-R007.40` (draft_unmerged; observed main `50c313ab01871ff7e10aee2a9d0254fa55d71f07`): Local drafting payload; not a merged producer or implementation receipt.

Owner-review choices (not source-established facts):
- A schema/type manifest is a proposed export contract, not evidence of a current implemented manifest.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-CONTRACT-R001.2` — AU:121

Original source objective: Prompt JSON blueprints and Pydantic models round-trip through disk with stable formatting and no field lost; a round-trip difference fails the test.

**Contract definition:** EG SHALL bind each exported prompt blueprint to its blueprint/schema revision, generated-model revision, semantic digest, and canonical-byte digest. The versioned JSON formatting profile SHALL use UTF-8 without a BOM, sorted object keys, two-space indentation, LF line endings, one final newline, explicit declared default and null fields, preserved array order, and no NaN or Infinity. Parsing SHALL reject unknown fields and scalar coercion. The schema-defined number representation and Unicode scalar values SHALL survive without rounding or normalization. Export, SDK-owned disk write, disk read, strict model validation, and re-export SHALL preserve both the typed field values and the canonical bytes. A mismatch SHALL report the first differing JSON pointer/value or byte offset and fail acceptance. EG owns blueprint semantics and validation; existing SDK/GraphOS governed source-effect contracts own filesystem effects.

**Production wiring:** Use EG-TYPED-PACKS-R094.2/.3/.5/.7: the served agent admin export/import and durable persistence path, with SDK-GOVERNED-WRITEBACK-R004.5-.12 and GRAPHOS-FLEET-R003.4-.10 for effects. Reuse those contracts rather than adding an EG filesystem writer.

**Planned acceptance:** Proposed integration case blueprint_canonical_disk_roundtrip_has_no_difference: use the served R094 export and governed SDK disk write/read, then import and re-export after restart. Include nested defaults, nulls, ordered arrays, Unicode, and boundary numeric values admitted by the schema. Compare exact canonical bytes plus schema-typed values. Lost fields, unknown fields, coerced numbers, changed array order, and a changed final newline must each fail the difference oracle. Capture the source-effect receipt; a serializer-only test is insufficient.

Preserve existing coverage: `EG-CONTRACT-R001`, `EG-TYPED-PACKS-R094.2`, `EG-TYPED-PACKS-R094.5`, `EG-TYPED-PACKS-R094.7`, `EG-TYPED-PACKS-R094.3`.

Source record SHA-256: `98b7ad1186ec2a385b334ffa0ad1b28888157ed6b51c6ed4172eeb7b7ef6768e`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/public-engine-contract-and-release/requirements.md:5` — `EG-CONTRACT-R001`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:149` — `EG-TYPED-PACKS-R094.2`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:150` — `EG-TYPED-PACKS-R094.3`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:152` — `EG-TYPED-PACKS-R094.5`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:154` — `EG-TYPED-PACKS-R094.7`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:30` — `AU-CONTROL-R025`

Open owner prerequisites:
- AU-CONTROL-R025.40 remains unmerged. The formatter must pin an exact number encoder/profile version; no claim that arbitrary JSON parsers preserve numeric spelling is permitted.

Unmerged consumer dependencies:
- `AU-CONTROL-R025.40` (draft_unmerged; observed main `50c313ab01871ff7e10aee2a9d0254fa55d71f07`): Local drafting payload; not a merged producer or implementation receipt.

Owner-review choices (not source-established facts):
- The explicit formatting profile matches the AU draft proposal; it is a proposed cross-owner contract, not an existing landed receipt.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

<!-- /spec-definition-patch -->
